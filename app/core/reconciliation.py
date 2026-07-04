"""
app/core/reconciliation.py

APScheduler background job — re-enqueues documents that are stuck
at status='processing' because their Kafka publish failed at upload
time (circuit open, broker blip after all retries were exhausted).

─── Why this is a safety net, not the primary recovery path ──────
The primary path is the retry loop in produce_document_event (3
attempts, 0.5s/1.0s backoff). That handles transient blips in
milliseconds while the user is still waiting for the 202 response.

This job handles the genuinely stuck case:
  - Circuit was open for 30+ minutes (sustained broker outage)
  - Consumer crashed mid-processing and never updated status
    (Kafka redelivers, but the DB row stays at 'processing')
  - Any other edge case where the event was never delivered

─── Stuck threshold ──────────────────────────────────────────────
STUCK_AFTER_MINUTES = 10

Why 10 minutes? PDF processing (extraction, OCR, embedding) should
complete in under 2 minutes even for large files. 10 minutes is a
5x safety margin — generous enough to avoid false re-enqueues for
legitimately slow documents, tight enough that users don't wait
an unreasonable time before recovery kicks in.

Adjust down if your consumer is consistently fast; adjust up if
you have very large documents that take longer to process.

─── Idempotency ──────────────────────────────────────────────────
Re-enqueuing is safe because produce_document_event uses
enable.idempotence=True at the producer level. If the event was
actually delivered earlier and the consumer is just slow, the
consumer will receive the duplicate event. Your consumer should
therefore be idempotent — check if status is already 'completed'
before doing work:

    SELECT status FROM documents WHERE document_id = $1
    IF status = 'completed': skip (ack and move on)
    ELSE: process normally

─── APScheduler vs a real cron ───────────────────────────────────
Running inside the FastAPI process means:
  + Zero new infrastructure
  + Shares the same kafka_producer instance and DB pool
  + Starts/stops cleanly with the app lifespan
  - If the process restarts, the job restarts too (fine — it's
    stateless; it just re-queries the DB each run)
  - If you run multiple replicas, every replica runs the job.
    With small document volumes this is harmless (idempotent).
    At scale, move to a dedicated worker or use a DB advisory lock.
"""

import logging
from datetime import datetime, timezone

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from sqlalchemy import text

from app.db.database import database
from app.core.kafka_producer import kafka_producer, KafkaProduceError
from app.core.redis_client import redis_client

WORKER_HEARTBEAT_KEY = "worker:document_worker:heartbeat"

logger = logging.getLogger("reconciliation")

# How old a 'processing' document must be before we consider it stuck.
# See module docstring for rationale.
STUCK_AFTER_MINUTES = 10

# How often the job runs. Every 5 minutes means a stuck document is
# re-enqueued within 5–15 minutes of the original failure.
RUN_EVERY_MINUTES = 5


async def reenqueue_stuck_documents() -> None:
    """Find documents stuck at 'processing' and re-emit their Kafka events.

    This is the reconciliation job. It runs on the APScheduler interval
    and is the safety net for documents whose Kafka publish failed after
    all retries were exhausted at upload time.

    Steps:
      1. Query DB for documents stuck at 'processing' > STUCK_AFTER_MINUTES
      2. For each, call produce_document_event — same call as the upload handler
      3. Log success/failure per document; never raise — a single bad document
         must not abort the entire reconciliation run
    """
    logger.info("Reconciliation job started — checking for stuck documents")

    # Check worker heartbeat — warn loudly if the worker is down.
    # The worker sets this key every poll cycle with a 90s TTL.
    # A missing key means it hasn't run in over 90 seconds.
    try:
        heartbeat = await redis_client.redis.get(WORKER_HEARTBEAT_KEY)
        if heartbeat is None:
            logger.error(
                "WORKER DOWN — document_worker heartbeat missing. "
                "Stuck documents will keep re-enqueuing until the worker is restarted. "
                "Run: .venv/bin/python -m workers.document_worker"
            )
        else:
            logger.debug("Worker heartbeat OK")
    except Exception as e:
        logger.warning(f"Could not check worker heartbeat: {e}")

    try:
        async with database.get_session() as session:
            result = await session.execute(
                text("""
                    SELECT
                        document_id,
                        channel_id,
                        storage_path,
                        file_name,
                        uploaded_by
                    FROM documents
                    WHERE
                        status = 'processing'
                        AND created_at < NOW() - (:minutes * INTERVAL '1 minute')
                """),
                {"minutes": STUCK_AFTER_MINUTES},
            )
            rows = result.fetchall()
    except Exception as e:
        # DB failure — log and bail. The job will retry on the next interval.
        logger.error(f"Reconciliation job failed to query DB: {e}")
        return

    if not rows:
        logger.info("Reconciliation job: no stuck documents found")
        return

    logger.warning(
        f"Reconciliation job: found {len(rows)} stuck document(s) — re-enqueuing"
    )

    success_count = 0
    failure_count = 0

    for row in rows:
        document_id = row.document_id
        try:
            await kafka_producer.produce_document_event(
                {
                    "documentId": document_id,
                    "channelId": row.channel_id,
                    "storagePath": row.storage_path,
                    "fileName": row.file_name,
                    "uploadedBy": row.uploaded_by,
                    "isReconciliation": True,
                }
            )
            logger.info(
                f"Reconciliation: re-enqueued document {document_id}"
            )
            success_count += 1

        except KafkaProduceError as e:
            # Kafka is still down. Log and move on — this document will
            # be picked up on the next reconciliation run.
            logger.warning(
                f"Reconciliation: failed to re-enqueue document "
                f"{document_id}: {e} — will retry next run"
            )
            failure_count += 1

        except Exception as e:
            # Unexpected error for this specific document. Log at ERROR
            # (warrants investigation) but continue to the next document.
            logger.error(
                f"Reconciliation: unexpected error for document "
                f"{document_id}: {e}"
            )
            failure_count += 1

    logger.info(
        f"Reconciliation job complete — "
        f"re-enqueued={success_count} failed={failure_count}"
    )


def create_scheduler() -> AsyncIOScheduler:
    """Create and configure the APScheduler instance.

    Returns a configured but NOT yet started scheduler.
    Call scheduler.start() in the FastAPI lifespan startup,
    and scheduler.shutdown() in lifespan teardown.

    AsyncIOScheduler runs jobs as asyncio coroutines on the
    same event loop as FastAPI — no extra threads needed.
    """
    scheduler = AsyncIOScheduler()

    scheduler.add_job(
        reenqueue_stuck_documents,
        trigger="interval",
        minutes=RUN_EVERY_MINUTES,
        id="reconcile_stuck_documents",
        # Prevent job overlap: if a run takes longer than the interval
        # (shouldn't happen, but guards against a very slow DB), skip
        # the next scheduled run rather than stacking them.
        max_instances=1,
        # Run once immediately at startup so we catch anything stuck
        # from before the last restart, then switch to the interval.
        next_run_time=datetime.now(timezone.utc),
    )

    logger.info(
        f"Reconciliation scheduler configured — "
        f"interval={RUN_EVERY_MINUTES}min "
        f"stuck_threshold={STUCK_AFTER_MINUTES}min"
    )
    return scheduler
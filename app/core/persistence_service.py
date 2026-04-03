"""
Persistence Service — Kafka consumer → PostgreSQL.

Architecture:
  Same thread model as the delivery service: a dedicated
  thread runs the blocking Kafka consumer poll() loop,
  and hands messages to the async event loop for DB writes.

  ┌─────────────────────────────────────────────────────┐
  │            Kafka Topic: channel-messages             │
  │            (same topic, DIFFERENT consumer group)    │
  └────────────────────────┬────────────────────────────┘
                           │
  ┌────────────────────────▼────────────────────────────┐
  │      Consumer Thread (group: persistence-group)     │
  │                                                     │
  │   poll(1.0) → deserialize → hand to event loop      │
  └────────────────────────┬────────────────────────────┘
                           │
  ┌────────────────────────▼────────────────────────────┐
  │           Async Persistence Pipeline                │
  │                                                     │
  │  1. Batch messages (collect for up to 500ms)        │
  │  2. INSERT ... ON CONFLICT DO NOTHING (dedup)       │
  │  3. Commit Kafka offsets for the batch              │
  └─────────────────────────────────────────────────────┘

Why batch inserts?
  Single-row inserts to PostgreSQL cost ~5ms each (network
  round trip + WAL write + fsync). At 1000 messages/sec,
  that's 5 seconds of DB time per second — impossible.

  Batching 100 messages into one INSERT takes ~10ms total.
  That's 0.01 seconds per 100 messages. 100x more efficient.
  PostgreSQL loves bulk operations.

Dedup strategy: INSERT ... ON CONFLICT DO NOTHING
  message_id is the PRIMARY KEY. If Kafka re-delivers a
  message after a rebalance, the INSERT silently skips it.
  No error, no exception, no special handling needed.
  This is idempotent by design.

Retry strategy:
  If PostgreSQL is down, we DON'T commit the Kafka offset.
  The consumer will re-poll the same messages after recovery.
  We also retry with exponential backoff for transient errors
  (connection reset, timeout). After max retries, we log the
  failure and move on — the message stays in Kafka and can
  be reprocessed by a recovery tool.
"""

import json
import time
import logging
import asyncio
import threading
from datetime import datetime, timezone
from typing import Optional
import asyncpg
from confluent_kafka import Consumer, TopicPartition
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.config import settings
from app.db.database import database
from app.db.models import Message

logger = logging.getLogger("persistence")

MESSAGES_TOPIC = "channel-messages"
CONSUMER_GROUP = "persistence-group"

# ── Batching config ───────────────────────────────────────────
BATCH_SIZE = 100        # Max messages per batch insert
BATCH_TIMEOUT = 0.5     # Max seconds to wait before flushing
MAX_RETRIES = 3         # DB write retries per batch
RETRY_BACKOFF = 1.0     # Base backoff seconds (doubles each retry)


class PersistenceService:
    """Consumes from Kafka, writes to PostgreSQL."""

    def __init__(self):
        self._consumer: Optional[Consumer] = None
        self._consumer_thread: Optional[threading.Thread] = None
        self._running = False
        self._loop: Optional[asyncio.AbstractEventLoop] = None

        # ── Batch buffer ──────────────────────────────────────
        # Messages accumulate here until BATCH_SIZE is reached
        # or BATCH_TIMEOUT expires, whichever comes first.
        self._batch: list[tuple[dict, object]] = []  # (payload, kafka_msg)
        self._batch_lock = asyncio.Lock()
        self._last_flush = time.monotonic()

        # ── Stats ─────────────────────────────────────────────
        self._total_persisted = 0
        self._total_duplicates = 0
        self._total_errors = 0

    # ── Lifecycle ─────────────────────────────────────────────

    def start(self) -> None:
        """Initialize the Kafka consumer."""
        conf = {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": CONSUMER_GROUP,
            "auto.offset.reset": "earliest",
            # ── earliest, not latest ──────────────────────────
            # Unlike delivery (which only cares about NEW
            # messages for online users), persistence must
            # process ALL messages — including any that arrived
            # while the service was down. "earliest" means on
            # first start, read from the beginning.
            "enable.auto.commit": False,
            "session.timeout.ms": 30000,
            "heartbeat.interval.ms": 10000,
            "max.poll.interval.ms": 300000,
            "fetch.min.bytes": 1024,
            "fetch.wait.max.ms": 500,
        }
        self._consumer = Consumer(conf)
        self._consumer.subscribe(
            [MESSAGES_TOPIC],
            on_assign=self._on_assign,
            on_revoke=self._on_revoke,
        )
        logger.info(
            f"Persistence consumer initialized, "
            f"group={CONSUMER_GROUP}"
        )

    def _on_assign(self, consumer, partitions):
        parts = [f"P{p.partition}" for p in partitions]
        logger.info(f"Partitions assigned: {parts}")

    def _on_revoke(self, consumer, partitions):
        parts = [f"P{p.partition}" for p in partitions]
        logger.info(f"Partitions revoked: {parts}")
        # Flush pending batch before losing partitions
        if self._loop and self._batch:
            future = asyncio.run_coroutine_threadsafe(
                self._flush_batch(), self._loop
            )
            try:
                future.result(timeout=5.0)
            except Exception as e:
                logger.error(f"Flush on revoke failed: {e}")
        try:
            consumer.commit(asynchronous=False)
        except Exception as e:
            logger.error(f"Commit on revoke failed: {e}")

    def start_consumer_thread(self) -> None:
        """Launch the blocking consumer loop."""
        self._loop = asyncio.get_running_loop()
        self._running = True
        self._consumer_thread = threading.Thread(
            target=self._consume_loop,
            name="kafka-persistence-consumer",
            daemon=True,
        )
        self._consumer_thread.start()
        logger.info("Persistence consumer thread started")

    def _consume_loop(self) -> None:
        """Blocking loop in dedicated thread.

        Collects messages into batches. When a batch is full
        or the timeout expires, schedules a flush on the
        event loop.
        """
        while self._running:
            try:
                msg = self._consumer.poll(0.1)

                if msg is None:
                    # No message — check if we should flush
                    # a partial batch based on timeout
                    self._maybe_schedule_flush()
                    continue

                if msg.error():
                    logger.error(f"Consumer error: {msg.error()}")
                    continue

                try:
                    payload = json.loads(
                        msg.value().decode("utf-8")
                    )
                except (json.JSONDecodeError, UnicodeDecodeError) as e:
                    logger.error(
                        f"Bad message at "
                        f"{msg.partition()}/{msg.offset()}: {e}"
                    )
                    self._commit_single(msg)
                    continue

                # Skip messages missing required fields
                if not payload.get("channelId"):
                    logger.warning(
                        f"Skipping message without channelId "
                        f"at {msg.partition()}/{msg.offset()}"
                    )
                    self._commit_single(msg)
                    continue

                # Add to batch
                self._loop.call_soon_threadsafe(
                    self._add_to_batch, payload, msg
                )

            except Exception as e:
                logger.error(f"Consumer loop error: {e}")
                time.sleep(1.0)

        # Clean shutdown — flush remaining
        if self._loop and self._batch:
            future = asyncio.run_coroutine_threadsafe(
                self._flush_batch(), self._loop
            )
            try:
                future.result(timeout=10.0)
            except Exception:
                pass

        if self._consumer:
            try:
                self._consumer.commit(asynchronous=False)
            except Exception:
                pass
            self._consumer.close()
            logger.info("Persistence consumer closed")

    def _maybe_schedule_flush(self) -> None:
        """Check if partial batch should be flushed by timeout."""
        if not self._batch:
            return
        elapsed = time.monotonic() - self._last_flush
        if elapsed >= BATCH_TIMEOUT:
            self._loop.call_soon_threadsafe(
                self._loop.create_task,
                self._safe_flush(),
            )

    # ── Batch Management ──────────────────────────────────────

    def _add_to_batch(self, payload: dict, kafka_msg) -> None:
        """Add a message to the current batch.

        Called on the event loop via call_soon_threadsafe.
        When batch is full, triggers an immediate flush.
        """
        self._batch.append((payload, kafka_msg))

        if len(self._batch) >= BATCH_SIZE:
            self._loop.create_task(self._safe_flush())

    async def _safe_flush(self) -> None:
        """Wrapper that catches flush errors."""
        try:
            await self._flush_batch()
        except Exception as e:
            logger.error(
                f"Batch flush error: {e}", exc_info=True
            )

    async def _flush_batch(self) -> None:
        """Flush the current batch to PostgreSQL.

        Takes a snapshot of the batch, clears it, then writes.
        If the write fails, messages are NOT re-added to the
        batch — they stay in Kafka (offset not committed) and
        will be re-consumed after recovery.
        """
        async with self._batch_lock:
            if not self._batch:
                return

            # Snapshot and clear
            batch = self._batch[:]
            self._batch.clear()
            self._last_flush = time.monotonic()

        payloads = [p for p, _ in batch]
        kafka_msgs = [m for _, m in batch]

        logger.info(
            f"Flushing batch: {len(payloads)} messages"
        )

        # ── Write to PostgreSQL with retry ────────────────────
        success = await self._write_with_retry(payloads)

        if success:
            # Commit offsets for all messages in the batch.
            # We commit the highest offset per partition.
            self._commit_batch(kafka_msgs)
        else:
            # DB write failed after all retries.
            # DON'T commit offsets — Kafka will re-deliver.
            self._total_errors += len(payloads)
            logger.error(
                f"Batch of {len(payloads)} messages NOT "
                f"persisted — will be retried from Kafka"
            )

    # ── PostgreSQL Write ──────────────────────────────────────

    async def _write_with_retry(
        self, payloads: list[dict]
    ) -> bool:
        """Insert messages into PostgreSQL with retry.

        Uses INSERT ... ON CONFLICT DO NOTHING for idempotency.
        If a message was already persisted (duplicate Kafka
        delivery), the INSERT is silently skipped.

        Returns True if write succeeded, False if all retries
        exhausted.
        """
        for attempt in range(MAX_RETRIES):
            try:
                result = await self._batch_insert(payloads)
                inserted = result["inserted"]
                duplicates = result["duplicates"]

                self._total_persisted += inserted
                self._total_duplicates += duplicates

                if duplicates > 0:
                    logger.info(
                        f"Batch result: {inserted} inserted, "
                        f"{duplicates} duplicates skipped"
                    )

                return True

            except (SQLAlchemyError, asyncpg.PostgresError) as e:
                # Database-level error (constraint violation,
                # syntax error, etc). Usually not transient.
                logger.error(
                    f"PostgreSQL error (attempt "
                    f"{attempt + 1}/{MAX_RETRIES}): {e}"
                )
                if attempt < MAX_RETRIES - 1:
                    backoff = RETRY_BACKOFF * (2 ** attempt)
                    await asyncio.sleep(backoff)

            except (OSError, asyncio.TimeoutError) as e:
                # Connection-level error (network down, timeout).
                # Transient — retry makes sense.
                logger.error(
                    f"Connection error (attempt "
                    f"{attempt + 1}/{MAX_RETRIES}): {e}"
                )
                if attempt < MAX_RETRIES - 1:
                    backoff = RETRY_BACKOFF * (2 ** attempt)
                    logger.info(
                        f"Retrying in {backoff}s..."
                    )
                    await asyncio.sleep(backoff)

            except Exception as e:
                logger.error(
                    f"Unexpected error writing batch: {e}",
                    exc_info=True,
                )
                break  # Don't retry unknown errors

        return False

    async def _batch_insert(self, payloads: list[dict]) -> dict:
        """Execute the batch INSERT using SQLAlchemy.

        Uses PostgreSQL's INSERT ... ON CONFLICT DO NOTHING
        via SQLAlchemy's dialect-specific insert. This gives
        us dedup for free — if Kafka re-delivers a message
        that's already in the DB, the row is silently skipped.

        Returns count of inserted vs skipped rows.
        """
        rows = []
        for p in payloads:
            ts = p.get("timestamp", time.time())
            created_at = datetime.fromtimestamp(
                ts, tz=timezone.utc
            )
            rows.append({
                "message_id": p.get("messageId", ""),
                "channel_id": p.get("channelId", ""),
                "sender_id": p.get("senderId", ""),
                "content": p.get("content", ""),
                "created_at": created_at,
                "correlation_id": p.get("correlationId"),
                "client_request_id": p.get("clientRequestId"),
            })

        async with database.get_session() as session:
            async with session.begin():
                # PostgreSQL-specific INSERT ... ON CONFLICT
                stmt = (
                    pg_insert(Message)
                    .values(rows)
                    .on_conflict_do_nothing(
                        index_elements=["message_id"]
                    )
                )
                result = await session.execute(stmt)
                inserted = result.rowcount

        duplicates = len(rows) - inserted
        if duplicates < 0:
            duplicates = 0

        return {"inserted": inserted, "duplicates": duplicates}

    # ── Offset Management ─────────────────────────────────────

    def _commit_single(self, kafka_msg) -> None:
        """Commit offset for a single skipped message."""
        try:
            self._consumer.commit(
                offsets=[
                    TopicPartition(
                        kafka_msg.topic(),
                        kafka_msg.partition(),
                        kafka_msg.offset() + 1,
                    )
                ],
                asynchronous=False,
            )
        except Exception as e:
            logger.error(f"Offset commit failed: {e}")

    def _commit_batch(self, kafka_msgs: list) -> None:
        """Commit highest offset per partition for a batch.

        A batch might span multiple partitions. We need to
        commit the highest offset for EACH partition, not
        just the last message overall.

        Example batch:
          P0:offset=10, P1:offset=5, P0:offset=11, P1:offset=6
        We commit:
          P0:12, P1:7  (offset + 1 = next to read)
        """
        # Find highest offset per partition
        partition_offsets: dict[tuple[str, int], int] = {}
        for msg in kafka_msgs:
            key = (msg.topic(), msg.partition())
            current = partition_offsets.get(key, -1)
            if msg.offset() > current:
                partition_offsets[key] = msg.offset()

        offsets = [
            TopicPartition(topic, part, offset + 1)
            for (topic, part), offset in partition_offsets.items()
        ]

        try:
            self._consumer.commit(
                offsets=offsets, asynchronous=False
            )
        except Exception as e:
            logger.error(f"Batch offset commit failed: {e}")

    # ── Lifecycle ─────────────────────────────────────────────

    def shutdown(self) -> None:
        self._running = False
        if (
            self._consumer_thread
            and self._consumer_thread.is_alive()
        ):
            self._consumer_thread.join(timeout=10.0)
        logger.info("Persistence service shut down")

    def stats(self) -> dict:
        return {
            "running": self._running,
            "total_persisted": self._total_persisted,
            "total_duplicates": self._total_duplicates,
            "total_errors": self._total_errors,
            "batch_pending": len(self._batch),
        }


# ── Module singleton ──────────────────────────────────────────
persistence_service = PersistenceService()
"""
AI service consumer — main loop: poll channel-messages -> detect trigger ->
generate RAG answer -> publish reply, mirroring workers/document_worker.py's
poll -> process -> commit-offset pattern.

Offset commit discipline
------------------------
enable.auto.commit = False, same as document_worker. The offset for a
message is committed in the finally block after _handle_message runs,
regardless of outcome:
  - not a trigger (normal chat, AI's own reply, system sender) -> commit,
    nothing else to do
  - trigger matched, generation + publish succeed                -> commit
  - trigger matched, generation degrades (LLM down) but still
    publishes the degraded "unavailable" text                    -> commit
  - trigger matched, publish itself fails after all retries       -> commit
    anyway; redelivering the same question would just regenerate
    and attempt to publish again, likely hitting the same outage.
    A stuck Kafka producer is already visible via the circuit
    breaker's own logging/metrics — no separate reconciliation
    path exists for AI replies the way there is for documents.

Why not skip the commit on failure (like document_worker retries via
redelivery)? document_worker's failure states are persisted to a `documents`
row keyed by document_id, so redelivery is idempotent (checked via
get_status). Redelivering an AI trigger after a publish failure would risk
the model generating and (if the broker recovered) publishing a duplicate
answer instead of no answer — a worse outcome than a single dropped reply,
which a user can simply ask again.

Reply dedup
-----------
ai_service.rag.reply_dedup.ReplyDedupService gives redelivered/reprocessed
trigger messages (rebalance, crash-restart) a "has the AI already answered
this" check via a Redis SET NX claim keyed on the triggering message id,
checked in _handle_message right before publish. This closes the specific
gap described above without changing the offset-commit-always policy: a
message that's already been answered still gets its offset committed, it
just skips a second publish.

Rate limiting
-------------
ai_service.rag.rate_limiter.RateLimitService caps triggers per sender and
per channel (Redis fixed-window counters), checked right after trigger
detection — before embedding, retrieval, or a NIM call, so a rate-limited
request never spends that cost. A declined request is dropped silently
(logged, offset still committed), same posture as a dropped reply being
preferable to noisy output elsewhere in this module.

Intra-process concurrency
--------------------------
Instead of a fully-serial poll -> handle -> commit loop, the poll loop is a
single producer that feeds a bounded asyncio.Queue; WORKER_CONCURRENCY
worker tasks pull messages off that queue and run _handle_message
concurrently (see docs/ai_service_productionization.md §2). A full queue
blocks the poll loop's `put`, which is deliberate backpressure — it slows
Kafka consumption when workers can't keep up instead of spawning unbounded
in-flight tasks.

Because workers finish out of order, offsets can no longer be committed as
"whatever just finished" — that risks committing offset N+1 while offset N
is still in flight, and a crash then skips N's message on redelivery. Each
partition's OffsetWatermarkTracker (ai_service/offset_tracker.py) tracks the
lowest still-in-flight offset per partition and only reports a new
safe-to-commit watermark once the contiguous run of lower offsets has all
finished; only that watermark is ever committed.
"""

import asyncio
import json
import logging
import signal
import time

from confluent_kafka import Consumer, KafkaError, TopicPartition
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from ai_service.config import (
    DATABASE_URL,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_GROUP_ID,
    KAFKA_POLL_TIMEOUT_S,
    MESSAGES_TOPIC,
    WORKER_CONCURRENCY,
    AI_QUEUE_SIZE_MULTIPLIER,
)
from ai_service.offset_tracker import OffsetWatermarkTracker
from ai_service.pipeline import MessageParseError, detect, parse_event
from ai_service.rag.generator import RagGenerator
from ai_service.rag.publisher import AnswerPublishError, publish_answer
from ai_service.rag.rate_limiter import rate_limit_service
from ai_service.rag.reply_dedup import reply_dedup_service
from app.core.redis_client import redis_client
from workers.chunk_repository import ChunkRepository
from workers.embedder import Embedder

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("ai_service.consumer")


class AiServiceConsumer:
    def __init__(self) -> None:
        self._consumer: Consumer
        self._embedder = Embedder()
        self._chunk_repo: ChunkRepository
        self._generator: RagGenerator
        self._running = False
        self._db_engine = None
        self._offsets = OffsetWatermarkTracker()
        self._queue: asyncio.Queue = asyncio.Queue(
            maxsize=WORKER_CONCURRENCY * AI_QUEUE_SIZE_MULTIPLIER
        )

    async def start(self) -> None:
        await self._init_db()
        self._init_kafka()
        await redis_client.initialize()
        self._generator = RagGenerator(self._embedder, self._chunk_repo)
        logger.info("AiServiceConsumer started")

    async def _init_db(self) -> None:
        db_url = DATABASE_URL
        if db_url.startswith("postgresql://"):
            db_url = db_url.replace("postgresql://", "postgresql+asyncpg://", 1)

        self._db_engine = create_async_engine(db_url, pool_size=5, max_overflow=2, pool_recycle=3600)
        session_factory = async_sessionmaker(bind=self._db_engine, class_=AsyncSession, expire_on_commit=False)
        self._chunk_repo = ChunkRepository(session_factory)

    def _init_kafka(self) -> None:
        self._consumer = Consumer(
            {
                "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
                "group.id": KAFKA_GROUP_ID,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
                "session.timeout.ms": 10_000,
                "heartbeat.interval.ms": 3_000,
                # RAG generation (embed + retrieve + LLM call) can run well
                "max.poll.interval.ms": 300_000,
            }
        )
        self._consumer.subscribe([MESSAGES_TOPIC])
        logger.info(f"Subscribed to topic={MESSAGES_TOPIC} group={KAFKA_GROUP_ID}")

    async def shutdown(self) -> None:
        self._running = False
        if self._consumer:
            self._consumer.close()
            logger.info("Kafka consumer closed")
        await self._generator.close()
        if self._db_engine:
            await self._db_engine.dispose()
        await redis_client.close()

    async def run(self) -> None:
        self._running = True
        loop = asyncio.get_running_loop()

        workers = [
            asyncio.create_task(self._worker_loop())
            for _ in range(WORKER_CONCURRENCY)
        ]

        try:
            while self._running:
                msg = await loop.run_in_executor(None, self._consumer.poll, KAFKA_POLL_TIMEOUT_S)

                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        logger.error(f"Kafka consumer error: {msg.error()}")
                    continue

                self._offsets.track(msg.partition(), msg.offset())
                # Backpressure: blocks the poll loop itself when all workers
                # are busy and the queue is full, rather than piling up
                # unbounded in-flight tasks.
                await self._queue.put(msg)
        finally:
            for task in workers:
                task.cancel()
            await asyncio.gather(*workers, return_exceptions=True)

    async def _worker_loop(self) -> None:
        while True:
            msg = await self._queue.get()
            try:
                try:
                    await self._handle_message(msg)
                finally:
                    self._commit_watermark(msg)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception(
                    f"Unhandled error processing messageId partition={msg.partition()} "
                    f"offset={msg.offset()}"
                )
            finally:
                self._queue.task_done()

    def _commit_watermark(self, msg) -> None:
        new_offset = self._offsets.complete(msg.partition(), msg.offset())
        if new_offset is None:
            # A lower offset on this partition is still in flight; committing
            # now would risk skipping it on a future redelivery.
            return
        self._consumer.commit(
            offsets=[TopicPartition(msg.topic(), msg.partition(), new_offset)],
            asynchronous=False,
        )

    async def _handle_message(self, msg) -> None:
        t_start = time.monotonic()

        try:
            payload = json.loads(msg.value().decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            logger.warning(f"Unparseable Kafka message, skipping: {e}")
            return

        try:
            event = parse_event(payload)
        except MessageParseError as e:
            logger.warning(f"Malformed channel-messages payload, skipping: {e}")
            return

        result = detect(event)
        if not result.should_respond:
            # Overwhelming majority of traffic: ordinary chat, the AI's own
            # published replies, system senders. Not an error — logged at
            # debug to avoid flooding INFO with every non-trigger message.
            logger.debug(
                f"channel_id={event.channel_id} messageId={event.message_id} "
                f"not a trigger: {result.reason}"
            )
            return

        logger.info(
            f"channel_id={event.channel_id} messageId={event.message_id} "
            f"triggered, query={result.query[:80]!r}"
        )

        rate_result = await rate_limit_service.check(
            sender_id=event.sender_id, channel_id=event.channel_id
        )
        if not rate_result.allowed:
            # Declined, not an error: drop silently rather than publish a
            # "rate limited" reply — same posture as a dropped reply being
            # preferable to noisy/duplicate output elsewhere in this module.
            # Offset still commits normally in run()'s finally.
            logger.info(
                f"channel_id={event.channel_id} messageId={event.message_id} "
                f"rate limited (scope={rate_result.exceeded_scope}), dropping"
            )
            return

        answer = await self._generator.answer(event.channel_id, result.query)

        claimed = await reply_dedup_service.try_claim(event.message_id)
        if not claimed:
            # Already answered this trigger message — a crash/redelivery
            # reprocessed it. Skip publish to avoid a duplicate reply; the
            # offset commit in run()'s finally still proceeds normally.
            logger.info(
                f"channel_id={event.channel_id} messageId={event.message_id} "
                f"already answered, skipping duplicate publish"
            )
            return

        try:
            await publish_answer(
                channel_id=event.channel_id,
                reply_to_message_id=event.message_id,
                answer=answer,
            )
        except AnswerPublishError as e:
            # RagGenerator.answer() already degrades gracefully on LLM
            # failure (had_error=True, a fixed apology string); this is the
            # publish step itself failing after retries — nothing left to
            # retry here, see module docstring for why we don't hold the
            # offset back. Logged, not raised, so the consumer loop and its
            # offset commit continue undisturbed by a single bad publish.
            logger.error(
                f"channel_id={event.channel_id} messageId={event.message_id} "
                f"failed to publish AI reply: {e}"
            )
            return

        elapsed = (time.monotonic() - t_start) * 1000
        logger.info(
            f"channel_id={event.channel_id} messageId={event.message_id} "
            f"answered in {elapsed:.0f}ms had_error={answer.had_error}"
        )


async def main() -> None:
    consumer = AiServiceConsumer()
    await consumer.start()

    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def _handle_signal(sig):
        logger.info(f"Received {signal.Signals(sig).name} — shutting down")
        stop_event.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _handle_signal, sig)

    consumer_task = asyncio.create_task(consumer.run())
    await stop_event.wait()

    logger.info("Stopping AI service consumer...")
    await consumer.shutdown()
    consumer_task.cancel()
    try:
        await asyncio.wait_for(consumer_task, timeout=10.0)
    except (asyncio.CancelledError, asyncio.TimeoutError):
        pass

    logger.info("AI service consumer stopped cleanly")


if __name__ == "__main__":
    asyncio.run(main())

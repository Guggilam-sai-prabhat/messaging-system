"""
Kafka Producer — background poller pattern.

Previous approach: per-message thread pool for poll().
  Problem: 4 threads = max 4 concurrent produces. Acks
  can return out of order. Thread overhead under load.

New approach: ONE background asyncio task calls poll()
  continuously. produce_message() just calls produce()
  and awaits a future. The background poller fires
  callbacks in order as broker responses arrive.

  ┌──────────────────────────────────────────────────┐
  │  produce_message()    produce_message()    ...   │
  │       │                    │                     │
  │       ▼                    ▼                     │
  │   produce() + future   produce() + future        │
  │       │                    │                     │
  │       └────────┬───────────┘                     │
  │                ▼                                 │
  │     background_poller task (single loop)         │
  │       poll(0.1) → fires callbacks → resolves     │
  │       futures in ORDER                           │
  └──────────────────────────────────────────────────┘

─── Why async document processing? ───────────────────────────
PDF text extraction, OCR, and embedding generation are all
CPU/IO-bound operations that can take 5 to 30 seconds per file.
If we did that work inside the HTTP request handler, every
fastapi worker thread would be blocked for the duration —
meaning no other user request could be served by that worker.
Offloading via Kafka lets the HTTP layer return in milliseconds
while a separate consumer process handles the heavy lifting.

─── Why Kafka (and not Celery/Redis)? ────────────────────────
Kafka gives us three guarantees a simple task queue cannot:

  1. Durability   — Messages are written to disk and replicated.
                    A broker restart loses nothing.

  2. Replayability — Consumer offsets are explicit. If the worker
                    crashes mid-processing, the offset hasn't been
                    committed, so the event is automatically
                    redelivered on restart. With Celery/Redis the
                    job is gone the moment it's dequeued.

  3. Decoupling   — The producer (upload service) has zero
                    knowledge of the consumer. New consumers
                    (e.g. a thumbnail generator) can be added
                    without touching this file.

─── Producer config explained ────────────────────────────────
  acks=all
    The broker only sends an ack after ALL in-sync replicas have
    written the message. A leader failure mid-write can't lose
    the message because a replica already has it.

  retries=5
    librdkafka automatically retries transient errors such as a
    leader election in progress or a momentary network blip. The
    application never sees those errors.

  enable.idempotence=True
    The producer gets a unique producer ID and stamps each message
    with a monotonically increasing sequence number. The broker
    uses these to deduplicate retried messages. Without this, a
    retry after a network timeout where the original write
    actually succeeded would produce a duplicate message.

─── Failure scenarios ────────────────────────────────────────
  Kafka unavailable at publish time
    After 5 consecutive delivery failures the circuit breaker
    opens. produce_document_event raises KafkaCircuitOpenError.
    The upload handler catches it, logs a WARNING, and still
    returns 202. The document row sits in the DB with
    status='processing'. A reconciliation cron can:

      SELECT * FROM documents
      WHERE status = 'processing'
        AND created_at < NOW() - INTERVAL '10 minutes';

    …and re-enqueue those document IDs.

  DB insert succeeds, Kafka fails
    Same recovery path as above. We NEVER issue a compensating
    DELETE on the documents row — the row is safe and idempotent.
    Kafka failure is the only partial-failure mode here.
"""

import json
import logging
import asyncio
import time
from typing import Optional
from confluent_kafka import Producer, KafkaException

import threading
from app.config import settings
from app.core.circuit_breaker import CircuitBreaker
from app.core.metrics import ingest_metrics

logger = logging.getLogger("kafka.producer")

# Logger used exclusively by produce_document_event so that document
# processing events can be filtered / routed independently in log
# aggregation (e.g. a separate Datadog stream or alert rule).
document_logger = logging.getLogger("kafka.document")

MESSAGES_TOPIC = "channel-messages"

# Separate topic so document-processing consumers can scale
# independently from the real-time chat message consumers.
# Keeping topics segregated also lets you set different retention
# periods (chat: 7 days; documents: 30 days for reprocessing).
DOCUMENT_PROCESSING_TOPIC = "document-processing"


class KafkaProduceError(Exception):
    pass


class KafkaCircuitOpenError(KafkaProduceError):
    pass


class KafkaProducerService:

    def __init__(self):
        self._producer: Optional[Producer] = None
        self._circuit = CircuitBreaker(
            failure_threshold=5,
            recovery_timeout=30.0,
            name="kafka-producer",
        )
        self._poller_thread: Optional[threading.Thread] = None
        self._running = False
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    # ── Lifecycle ─────────────────────────────────────────────

    def start(self) -> None:
        """Initialize the producer. Call from lifespan startup."""
        conf = {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            # acks=all: broker only acks after every in-sync replica
            # has persisted the message. Prevents loss on leader failover.
            "acks": "all",
            # Assigns producer ID + per-message sequence numbers so the
            # broker can deduplicate retried messages automatically.
            "enable.idempotence": True,
            # librdkafka retries transient errors (leader election,
            # network blip) without surfacing them to the application.
            "retries": 5,
            "retry.backoff.ms": 100,
            "batch.size": 32768,
            "compression.type": "lz4",
        }
        self._producer = Producer(conf)
        self._running = True
        logger.info(
            f"Kafka producer started, "
            f"brokers={settings.kafka_bootstrap_servers}"
        )

    def start_poller(self) -> None:
        """Start the background poll thread.

        A dedicated thread — NOT an asyncio task — because
        poll() is a blocking C call. Running it in a thread
        means:
          1. The event loop is never blocked
          2. poll() can use longer timeouts (1s) for efficiency
          3. Callbacks fire the instant the broker responds
          4. call_soon_threadsafe correctly crosses the
             thread boundary to resolve futures
        """
        self._loop = asyncio.get_running_loop()
        self._poller_thread = threading.Thread(
            target=self._poll_loop,
            name="kafka-poller",
            daemon=True,
        )
        self._poller_thread.start()
        logger.info("Kafka background poller thread started")

    def _poll_loop(self) -> None:
        """Runs in a dedicated thread. Blocks on poll(0.5).

        poll(0.5) blocks for up to 500ms. When a broker
        response arrives, it fires the delivery callback
        immediately. The callback uses call_soon_threadsafe
        to resolve the future on the event loop.

        Because this is a real thread (not asyncio), there's
        zero event loop blocking. The future resolves on the
        very next event loop iteration after the callback.
        """
        while self._running:
            try:
                if self._producer:
                    self._producer.poll(0.05)  # 50ms block max
            except Exception as e:
                logger.error(f"Poller error: {e}")
                time.sleep(1.0)

    def shutdown(self, timeout: float = 10.0) -> None:
        """Stop poller thread, flush remaining messages, tear down."""
        self._running = False

        if self._poller_thread and self._poller_thread.is_alive():
            self._poller_thread.join(timeout=2.0)

        if not self._producer:
            return

        remaining = self._producer.flush(timeout=timeout)
        if remaining > 0:
            logger.error(
                f"Kafka shutdown: {remaining} messages NOT delivered"
            )
        else:
            logger.info("Kafka producer shut down cleanly")
        self._producer = None

    # ── Core produce methods ──────────────────────────────────

    async def produce_message(self, enriched_dict: dict) -> dict:
        """Produce a message and await delivery confirmation.

        This method is now simple:
          1. Check circuit breaker
          2. Call produce() (non-blocking, queues in librdkafka)
          3. Await the future (background poller resolves it)

        No threads, no executor, no polling here.
        """
        if not self._producer:
            raise KafkaProduceError("Kafka producer not initialized")

        if not self._circuit.allow_request():
            raise KafkaCircuitOpenError(
                f"Circuit open — Kafka unavailable "
                f"(cooldown {self._circuit._recovery_timeout}s)"
            )

        key = enriched_dict["channelId"].encode("utf-8")
        value = json.dumps(enriched_dict).encode("utf-8")

        loop = asyncio.get_running_loop()
        future = loop.create_future()
        t_start = time.monotonic()

        def _on_delivery(err, kafka_msg):
            """Fired by the background poller's poll() call."""
            if future.done():
                return
            if err is not None:
                loop.call_soon_threadsafe(
                    future.set_exception,
                    KafkaProduceError(
                        f"Delivery failed: {err.str()}"
                    ),
                )
            else:
                loop.call_soon_threadsafe(
                    future.set_result,
                    {
                        "topic": kafka_msg.topic(),
                        "partition": kafka_msg.partition(),
                        "offset": kafka_msg.offset(),
                    },
                )

        try:
            self._producer.produce(
                topic=MESSAGES_TOPIC,
                key=key,
                value=value,
                callback=_on_delivery,
            )
        except BufferError:
            self._circuit.record_failure()
            ingest_metrics.record_failed()
            raise KafkaProduceError(
                "Producer buffer full — broker may be unreachable"
            )
        except KafkaException as e:
            self._circuit.record_failure()
            ingest_metrics.record_failed()
            raise KafkaProduceError(f"Produce call failed: {e}")

        # ── Await delivery ────────────────────────────────────
        # The background poller will call poll(), which fires
        # _on_delivery, which resolves this future.
        # We just wait here. No threads needed.
        try:
            result = await asyncio.wait_for(future, timeout=10.0)
        except asyncio.TimeoutError:
            self._circuit.record_failure()
            ingest_metrics.record_failed()
            raise KafkaProduceError(
                "Delivery confirmation timed out (10s)"
            )
        except KafkaProduceError:
            self._circuit.record_failure()
            ingest_metrics.record_failed()
            raise

        latency_ms = (time.monotonic() - t_start) * 1000
        self._circuit.record_success()
        ingest_metrics.record_produced(latency_ms)

        return result

    async def produce_document_event(self, payload: dict) -> dict:
        """Produce a document-processing event and await delivery.

        Wraps _produce_document_event_once with a retry loop so that
        transient broker blips (leader election, network hiccup) are
        handled transparently before the upload handler ever sees an
        error.

        Retry policy:
          - Up to MAX_DOCUMENT_RETRIES attempts (3)
          - Exponential backoff: 0.5s, 1.0s between attempts
          - KafkaCircuitOpenError is NOT retried — the circuit is open
            because the broker has been unreachable for a sustained
            period. Retrying immediately just wastes time. Fall through
            so the upload handler logs a warning and returns 202; the
            APScheduler reconciliation job will re-enqueue later.
          - Any other KafkaProduceError IS retried — these are transient
            (buffer briefly full, single timeout, one bad broker reply).

        Why retry here and not in the upload handler?
            The upload handler should not know about Kafka internals.
            Keeping retry logic inside the producer keeps the handler
            simple: try → catch warning → return 202.
        """
        MAX_DOCUMENT_RETRIES = 3
        last_exc: Exception = KafkaProduceError("No attempts made")

        for attempt in range(MAX_DOCUMENT_RETRIES):
            try:
                return await self._produce_document_event_once(payload)
            except KafkaCircuitOpenError:
                # Circuit is open — broker is genuinely down for a
                # sustained period. Retrying won't help; stop immediately
                # and let the reconciliation job handle it later.
                document_logger.warning(
                    f"Circuit open on document event publish attempt "
                    f"{attempt + 1}/{MAX_DOCUMENT_RETRIES} "
                    f"| documentId={payload.get('documentId')}"
                )
                raise
            except KafkaProduceError as e:
                last_exc = e
                if attempt < MAX_DOCUMENT_RETRIES - 1:
                    backoff = 0.5 * (attempt + 1)  # 0.5s, 1.0s
                    document_logger.warning(
                        f"Document event publish failed "
                        f"(attempt {attempt + 1}/{MAX_DOCUMENT_RETRIES}), "
                        f"retrying in {backoff}s "
                        f"| documentId={payload.get('documentId')} "
                        f"| error={e}"
                    )
                    await asyncio.sleep(backoff)

        # All retries exhausted — raise the last error so the upload
        # handler can log a warning and return 202. The document row
        # is already in the DB; the reconciliation job will re-enqueue.
        document_logger.error(
            f"Document event publish failed after {MAX_DOCUMENT_RETRIES} "
            f"attempts | documentId={payload.get('documentId')}"
        )
        raise last_exc

    async def _produce_document_event_once(self, payload: dict) -> dict:
        """Single produce attempt for a document event. No retries.

        Called by produce_document_event which owns the retry loop.
        All circuit-breaker and delivery-callback logic lives here.
        """
        if not self._producer:
            raise KafkaProduceError("Kafka producer not initialized")

        if not self._circuit.allow_request():
            raise KafkaCircuitOpenError(
                f"Circuit open — Kafka unavailable "
                f"(cooldown {self._circuit._recovery_timeout}s)"
            )

        # Partition by channelId so all events for a channel land on
        # the same partition and are therefore processed in order.
        key = payload["channelId"].encode("utf-8")
        value = json.dumps(payload).encode("utf-8")

        loop = asyncio.get_running_loop()
        future = loop.create_future()
        t_start = time.monotonic()

        def _on_delivery(err, kafka_msg):
            """Delivery callback fired by the background poll thread.

            call_soon_threadsafe is required here because this callback
            runs on the poller thread, not the event loop thread. It
            schedules future resolution on the correct loop so there's
            no cross-thread mutation of the future object.
            """
            if future.done():
                return

            if err is not None:
                document_logger.error(
                    f"Document event delivery failed: {err.str()} "
                    f"| documentId={payload.get('documentId')} "
                    f"| channelId={payload.get('channelId')}"
                )
                loop.call_soon_threadsafe(
                    future.set_exception,
                    KafkaProduceError(
                        f"Delivery failed: {err.str()}"
                    ),
                )
            else:
                document_logger.info(
                    f"Document event delivered: "
                    f"topic={kafka_msg.topic()} "
                    f"partition={kafka_msg.partition()} "
                    f"offset={kafka_msg.offset()} "
                    f"| documentId={payload.get('documentId')}"
                )
                loop.call_soon_threadsafe(
                    future.set_result,
                    {
                        "topic": kafka_msg.topic(),
                        "partition": kafka_msg.partition(),
                        "offset": kafka_msg.offset(),
                    },
                )

        try:
            self._producer.produce(
                topic=DOCUMENT_PROCESSING_TOPIC,
                key=key,
                value=value,
                callback=_on_delivery,
            )
        except BufferError:
            self._circuit.record_failure()
            ingest_metrics.record_failed()
            raise KafkaProduceError(
                "Producer buffer full — broker may be unreachable"
            )
        except KafkaException as e:
            self._circuit.record_failure()
            ingest_metrics.record_failed()
            raise KafkaProduceError(f"Produce call failed: {e}")

        try:
            result = await asyncio.wait_for(future, timeout=10.0)
        except asyncio.TimeoutError:
            self._circuit.record_failure()
            ingest_metrics.record_failed()
            raise KafkaProduceError(
                "Delivery confirmation timed out (10s)"
            )
        except KafkaProduceError:
            self._circuit.record_failure()
            ingest_metrics.record_failed()
            raise

        latency_ms = (time.monotonic() - t_start) * 1000
        self._circuit.record_success()
        ingest_metrics.record_produced(latency_ms)

        return result

    def circuit_stats(self) -> dict:
        return self._circuit.stats()


kafka_producer = KafkaProducerService()
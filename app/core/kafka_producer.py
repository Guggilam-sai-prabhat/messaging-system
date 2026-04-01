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

MESSAGES_TOPIC = "channel-messages"


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
            "acks": "all",
            "enable.idempotence": True,
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

    # ── Core produce method ───────────────────────────────────

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

    def circuit_stats(self) -> dict:
        return self._circuit.stats()


kafka_producer = KafkaProducerService()
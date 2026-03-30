"""
Kafka Producer — async-safe wrapper with circuit breaker and metrics.

Key insight: confluent-kafka's delivery callbacks only fire during
poll(). poll(0) is non-blocking but may return before the broker
responds. We run poll(timeout) in a thread executor so it can
block waiting for the broker WITHOUT freezing the asyncio event loop.
"""

import json
import logging
import asyncio
import time
from typing import Optional
from concurrent.futures import ThreadPoolExecutor
from confluent_kafka import Producer, KafkaException

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
        # Dedicated thread pool for poll() calls.
        # 4 threads is enough — poll is mostly waiting on I/O,
        # not doing CPU work.
        self._poll_executor = ThreadPoolExecutor(
            max_workers=4, thread_name_prefix="kafka-poll"
        )

    # ── Lifecycle ─────────────────────────────────────────────

    def start(self) -> None:
        conf = {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "acks": "all",
            "enable.idempotence": True,
            "retries": 5,
            "retry.backoff.ms": 100,
            "linger.ms": 5,
            "batch.size": 32768,
            "compression.type": "lz4",
            "queue.buffering.max.ms": 1000,
        }
        self._producer = Producer(conf)
        logger.info(
            f"Kafka producer started, "
            f"brokers={settings.kafka_bootstrap_servers}"
        )

    def shutdown(self, timeout: float = 10.0) -> None:
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
        self._poll_executor.shutdown(wait=False)

    # ── Core produce method ───────────────────────────────────

    async def produce_message(self, enriched_dict: dict) -> dict:
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
            """Called by librdkafka during poll() — runs in the
            poll executor thread, so we use call_soon_threadsafe
            to resolve the future on the event loop."""
            if err is not None:
                if not future.done():
                    loop.call_soon_threadsafe(
                        future.set_exception,
                        KafkaProduceError(
                            f"Delivery failed: {err.str()}"
                        ),
                    )
            else:
                if not future.done():
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

        # ── Poll in a thread ──────────────────────────────────
        # This is the critical fix. poll(timeout) blocks the
        # CALLING thread until a callback fires or timeout
        # expires. By running it in an executor, the asyncio
        # event loop stays free to handle other WebSocket
        # connections.
        #
        # Flow:
        #   1. produce() queues message in librdkafka buffer
        #   2. librdkafka's background thread sends to broker
        #   3. broker responds
        #   4. poll(10) in executor thread processes the
        #      response and fires _on_delivery
        #   5. _on_delivery resolves the future via
        #      call_soon_threadsafe
        #   6. await future completes on the event loop
        def _blocking_poll():
            """Runs in thread pool — safe to block here.

            poll(1.0) blocks up to 1 second waiting for broker
            responses. It returns the number of callbacks fired
            in THAT call (often 0 if nothing arrived yet).
            We loop until the future is resolved or we hit the
            10s deadline.
            """
            deadline = time.monotonic() + 10.0
            while not future.done() and time.monotonic() < deadline:
                self._producer.poll(1.0)

        await loop.run_in_executor(self._poll_executor, _blocking_poll)

        # ── Check result ──────────────────────────────────────
        if not future.done():
            self._circuit.record_failure()
            ingest_metrics.record_failed()
            raise KafkaProduceError(
                "Delivery confirmation timed out (10s)"
            )

        try:
            result = future.result()
        except KafkaProduceError:
            self._circuit.record_failure()
            ingest_metrics.record_failed()
            raise

        # ── Success ───────────────────────────────────────────
        latency_ms = (time.monotonic() - t_start) * 1000
        self._circuit.record_success()
        ingest_metrics.record_produced(latency_ms)

        return result

    def circuit_stats(self) -> dict:
        return self._circuit.stats()


kafka_producer = KafkaProducerService()
"""
Kafka Producer — async-safe wrapper with circuit breaker and metrics.

Changes from v0.1:
  - Circuit breaker: fail fast when Kafka is consistently down
  - Metrics: track produce latency and failure rate
  - produce_message returns delivery metadata (partition, offset)
    for structured logging upstream
"""

import json
import logging
import asyncio
import time
from typing import Optional
from confluent_kafka import Producer, KafkaException

from app.config import settings
from app.core.circuit_breaker import CircuitBreaker
from app.core.metrics import ingest_metrics

logger = logging.getLogger("kafka.producer")

MESSAGES_TOPIC = "channel-messages"


class KafkaProduceError(Exception):
    """Raised when producing to Kafka fails."""
    pass


class KafkaCircuitOpenError(KafkaProduceError):
    """Raised when the circuit breaker is open.

    Distinct from KafkaProduceError so the WebSocket handler
    can tell the client WHY it's failing fast — "service
    temporarily unavailable" vs "your message failed."
    """
    pass


class KafkaProducerService:
    """Wraps confluent-kafka Producer with circuit breaker and metrics."""

    def __init__(self):
        self._producer: Optional[Producer] = None
        self._circuit = CircuitBreaker(
            failure_threshold=5,
            recovery_timeout=30.0,
            name="kafka-producer",
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

    # ── Core produce method ───────────────────────────────────

    async def produce_message(
        self, enriched_dict: dict
    ) -> dict:
        """Produce to Kafka with circuit breaker and metrics.

        Args:
            enriched_dict: camelCase dict from EnrichedMessage.to_kafka_dict()

        Returns:
            {"topic": str, "partition": int, "offset": int}

        Raises:
            KafkaCircuitOpenError: circuit breaker is open
            KafkaProduceError: delivery failed
        """
        if not self._producer:
            raise KafkaProduceError("Kafka producer not initialized")

        # ── Circuit breaker check ─────────────────────────────
        if not self._circuit.allow_request():
            raise KafkaCircuitOpenError(
                f"Circuit open — Kafka unavailable "
                f"(cooldown {self._circuit._recovery_timeout}s)"
            )

        # ── Serialize ─────────────────────────────────────────
        key = enriched_dict["channelId"].encode("utf-8")
        value = json.dumps(enriched_dict).encode("utf-8")

        # ── Produce with timing ───────────────────────────────
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        t_start = time.monotonic()

        def _on_delivery(err, kafka_msg):
            if err is not None:
                loop.call_soon_threadsafe(
                    future.set_exception,
                    KafkaProduceError(f"Delivery failed: {err.str()}"),
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

        self._producer.poll(0)

        # ── Await delivery ────────────────────────────────────
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

        # ── Success ───────────────────────────────────────────
        latency_ms = (time.monotonic() - t_start) * 1000
        self._circuit.record_success()
        ingest_metrics.record_produced(latency_ms)

        return result

    def circuit_stats(self) -> dict:
        return self._circuit.stats()


# ── Module-level singleton ────────────────────────────────────
kafka_producer = KafkaProducerService()
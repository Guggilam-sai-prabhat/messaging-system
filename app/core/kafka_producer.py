"""
Kafka Producer — async-safe wrapper around confluent-kafka.

Design decisions:
  1. confluent-kafka's Producer is thread-safe but not async-aware.
     We call poll(0) after each produce() to pump the delivery-callback
     queue without blocking the event loop.
  2. The delivery callback fires on the NEXT poll() call, not
     immediately — so we use an asyncio.Future to bridge the gap
     between the callback world and the await world.
  3. Startup/shutdown are explicit — the FastAPI lifespan controls
     when the producer is created and flushed.

             produce()      poll(0)       callback fires
  async code ───────→ librdkafka ───────→ _delivery_callback
       ↑                                        │
       └──── await future ←─────────────────────┘
"""

import json
import logging
import asyncio
from typing import Optional
from confluent_kafka import Producer, KafkaException

from app.config import settings

logger = logging.getLogger("kafka.producer")

# ── Topic constant ────────────────────────────────────────────
MESSAGES_TOPIC = "channel-messages"


class KafkaProducerService:
    """Wraps confluent-kafka Producer with async delivery confirmation.

    Usage:
        producer = KafkaProducerService()
        producer.start()                       # in lifespan startup
        await producer.produce_message(msg)    # in request handler
        producer.shutdown()                    # in lifespan shutdown
    """

    def __init__(self):
        self._producer: Optional[Producer] = None

    # ── Lifecycle ─────────────────────────────────────────────

    def start(self) -> None:
        """Initialize the underlying confluent-kafka Producer.

        Called once during FastAPI lifespan startup.
        Separating construction from __init__ lets us control
        exactly WHEN the broker connection happens.
        """
        conf = {
            # ── Connection ────────────────────────────────────
            # Comma-separated broker list. In production, list
            # 3+ brokers so the client can discover the cluster
            # even if one broker is down at startup.
            "bootstrap.servers": settings.kafka_bootstrap_servers,

            # ── Durability ────────────────────────────────────
            # "all" = wait for leader + all in-sync replicas.
            # For a chat app, losing a message is worse than
            # adding 5-10ms of latency.
            "acks": "all",

            # ── Exactly-once producing ────────────────────────
            # The producer tags each message with a sequence
            # number. If a retry re-delivers, the broker dedupes
            # it. REQUIRES acks=all and max.in.flight <= 5
            # (confluent-kafka enforces this automatically).
            "enable.idempotence": True,

            # ── Retries ───────────────────────────────────────
            # Transient failures (leader election, network blip)
            # are common. 5 retries with backoff covers most.
            # Idempotence makes retries safe — no duplicates.
            "retries": 5,
            "retry.backoff.ms": 100,

            # ── Batching ──────────────────────────────────────
            # linger.ms: wait up to 5ms to fill a batch.
            #   Low load → 5ms extra latency (acceptable for chat)
            #   High load → dramatically fewer network round-trips
            #
            # batch.size: max bytes per batch (32 KB).
            #   Whichever triggers first (linger or batch full)
            #   causes the send.
            "linger.ms": 5,
            "batch.size": 32768,  # 32 KB

            # ── Compression ───────────────────────────────────
            # lz4 gives good compression ratio with minimal CPU.
            # Chat messages are small text — compresses well.
            "compression.type": "lz4",

            # ── Safety net ────────────────────────────────────
            # If the internal buffer fills up (broker is slow),
            # produce() will block. This timeout controls how
            # long before it raises BufferError.
            "queue.buffering.max.ms": 1000,
        }

        self._producer = Producer(conf)
        logger.info(
            f"Kafka producer started, brokers={settings.kafka_bootstrap_servers}"
        )

    def shutdown(self, timeout: float = 10.0) -> None:
        """Flush remaining messages and tear down.

        flush() blocks until all buffered messages are delivered
        or the timeout expires. Any undelivered messages after
        timeout are lost — in practice this means the broker is
        truly unreachable, and we log an error.
        """
        if not self._producer:
            return

        remaining = self._producer.flush(timeout=timeout)
        if remaining > 0:
            logger.error(
                f"Kafka shutdown: {remaining} messages were NOT delivered"
            )
        else:
            logger.info("Kafka producer shut down cleanly")

        self._producer = None

    # ── Core produce method ───────────────────────────────────

    async def produce_message(self, enriched_dict: dict) -> None:
        """Produce a message to Kafka with async delivery confirmation.

        Args:
            enriched_dict: The camelCase dict from EnrichedMessage.to_log_dict()

        Raises:
            KafkaProduceError: if delivery fails or producer is down

        The flow:
          1. Serialize the message to JSON bytes
          2. Call produce() with channelId as the partition key
          3. poll(0) to pump the callback queue (non-blocking)
          4. Await the Future that the delivery callback resolves
        """
        if not self._producer:
            raise KafkaProduceError("Kafka producer is not initialized")

        # ── Serialize ─────────────────────────────────────────
        key = enriched_dict["channelId"].encode("utf-8")
        value = json.dumps(enriched_dict).encode("utf-8")

        # ── Bridge async ↔ callback ───────────────────────────
        # confluent-kafka uses a callback model; we wrap it in a
        # Future so the caller can `await` delivery confirmation.
        loop = asyncio.get_running_loop()
        future = loop.create_future()

        def _delivery_callback(err, kafka_msg):
            """Fires on the next poll() after broker responds.

            IMPORTANT: This callback runs in the asyncio thread
            (because poll() is called from the async context),
            so it's safe to resolve the Future directly.
            """
            if err is not None:
                # Don't set exception from callback — use
                # call_soon_threadsafe in case poll is called
                # from another thread in the future.
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

        # ── Produce ───────────────────────────────────────────
        try:
            self._producer.produce(
                topic=MESSAGES_TOPIC,
                key=key,
                value=value,
                callback=_delivery_callback,
            )
        except BufferError:
            # Internal librdkafka buffer is full — broker can't
            # keep up. This is a backpressure signal.
            raise KafkaProduceError(
                "Kafka producer buffer full — broker may be unreachable"
            )
        except KafkaException as e:
            raise KafkaProduceError(f"Kafka produce failed: {e}")

        # ── Pump callbacks ────────────────────────────────────
        # poll(0) is non-blocking. It processes any delivery
        # reports that have arrived from the broker. Without
        # this, callbacks would pile up until the next produce().
        self._producer.poll(0)

        # ── Await confirmation ────────────────────────────────
        # The future resolves when the delivery callback fires.
        # If the broker is slow, this is where we wait.
        try:
            result = await asyncio.wait_for(future, timeout=10.0)
            logger.debug(
                f"Delivered: topic={result['topic']} "
                f"partition={result['partition']} "
                f"offset={result['offset']}"
            )
        except asyncio.TimeoutError:
            # The broker hasn't acknowledged within 10s.
            # The message might still be delivered later,
            # but we can't block the WebSocket handler forever.
            raise KafkaProduceError(
                "Kafka delivery confirmation timed out (10s)"
            )


class KafkaProduceError(Exception):
    """Raised when producing to Kafka fails.

    Carries a message safe to log but NOT safe to send
    to the client (may contain broker addresses, etc.).
    """
    pass


# ── Module-level singleton ────────────────────────────────────
# Created at import time, but start() is called in lifespan.
# This pattern lets any module do:
#   from app.core.kafka_producer import kafka_producer
#   await kafka_producer.produce_message(...)
kafka_producer = KafkaProducerService()
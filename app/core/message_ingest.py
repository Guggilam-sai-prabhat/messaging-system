"""
Message Ingestion — validate, enrich, and produce to Kafka.

This module is the LEFT SIDE of the pipeline:

  Client → [Validate] → [Enrich] → [Produce to Kafka]

The ingest layer is now an ASYNC function of
(raw_message, sender_id) → EnrichedMessage, because
Kafka delivery confirmation is awaited.
"""

import logging
from pydantic import ValidationError

from app.models import IncomingMessage, EnrichedMessage
from app.core.connection_registry import ConnectionRegistry
from app.core.kafka_producer import kafka_producer, KafkaProduceError

logger = logging.getLogger("message.ingest")


class MessageValidationError(Exception):
    """Raised when a message fails validation.

    Carries a user-friendly error message that's safe to send
    back over the WebSocket (no internal details leaked).
    """
    def __init__(self, reason: str):
        self.reason = reason
        super().__init__(reason)


class MessageIngestService:
    """Validates, enriches, and produces messages to Kafka.

    Now async — the produce step awaits Kafka delivery confirmation.
    """

    def __init__(self, registry: ConnectionRegistry):
        self._registry = registry

    async def validate_and_enrich(
        self, raw: dict, sender_id: str
    ) -> EnrichedMessage:
        """Core ingestion pipeline: validate → check membership → enrich → produce.

        Args:
            raw: The raw JSON dict from the WebSocket message
            sender_id: From the authenticated connection (NOT from payload)

        Returns:
            EnrichedMessage that has been confirmed delivered to Kafka

        Raises:
            MessageValidationError: if validation fails at any step
            KafkaProduceError: if Kafka delivery fails (caller handles)
        """

        # ── Step 1: Schema validation ─────────────────────────
        try:
            incoming = IncomingMessage.model_validate(raw)
        except ValidationError as e:
            first_err = e.errors()[0]
            field = ".".join(str(loc) for loc in first_err["loc"])
            raise MessageValidationError(
                f"Invalid message: {field} — {first_err['msg']}"
            )

        # ── Step 2: Check message type ────────────────────────
        if incoming.type != "message.send":
            raise MessageValidationError(
                f"Expected type 'message.send', got '{incoming.type}'"
            )

        # ── Step 3: Channel membership check ──────────────────
        members = self._registry.get_channel_members(incoming.channel_id)
        if sender_id not in members:
            raise MessageValidationError(
                f"Not a member of channel '{incoming.channel_id}'"
            )

        # ── Step 4: Enrich ────────────────────────────────────
        enriched = EnrichedMessage(
            channel_id=incoming.channel_id,
            sender_id=sender_id,
            content=incoming.content,
        )

        # ── Step 5: Produce to Kafka ──────────────────────────
        # This replaces the old logger.info() placeholder.
        # We await delivery confirmation — if this succeeds,
        # the message is durably stored in Kafka.
        # If it fails, KafkaProduceError propagates to the
        # WebSocket handler, which sends an error to the client.
        await kafka_producer.produce_message(enriched.to_log_dict())

        logger.info(
            "Message produced to Kafka",
            extra={"message_data": enriched.to_log_dict()},
        )

        return enriched
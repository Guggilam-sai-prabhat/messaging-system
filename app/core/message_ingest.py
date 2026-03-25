"""
Message Ingestion — validate, enrich, and (for now) log.

This module is the LEFT SIDE of the pipeline:

  Client → [Validate] → [Enrich] → [Log/Produce]
                                       ↑
                                    YOU ARE HERE
                                    (Kafka comes next)

Separation rationale:
  The ingest layer is a PURE FUNCTION of (raw_message, sender_id) → EnrichedMessage.
  It has no knowledge of WebSockets, connections, or delivery.
  This means:
    1. It's trivially testable (no async, no mocks needed)
    2. It can be reused for REST-based message submission
    3. When Kafka arrives, we swap the log call for a produce call —
       the validation and enrichment logic stays identical
"""

import logging
from pydantic import ValidationError

from app.models import IncomingMessage, EnrichedMessage
from app.core.connection_registry import ConnectionRegistry

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
    """Validates, enriches, and logs messages.

    Why a class instead of free functions?
      - Holds a reference to the registry (needed for channel
        membership checks)
      - Easy to extend with rate limiting, content filtering, etc.
      - Injectable dependency — testable with a mock registry
    """

    def __init__(self, registry: ConnectionRegistry):
        self._registry = registry

    def validate_and_enrich(
        self, raw: dict, sender_id: str
    ) -> EnrichedMessage:
        """Core ingestion pipeline: validate → check membership → enrich.

        Args:
            raw: The raw JSON dict from the WebSocket message
            sender_id: From the authenticated connection (NOT from payload)

        Returns:
            EnrichedMessage ready for logging/producing

        Raises:
            MessageValidationError: if validation fails at any step
        """

        # ── Step 1: Schema validation ─────────────────────────
        # Pydantic catches: missing fields, wrong types, empty
        # strings, content too long, etc.
        try:
            incoming = IncomingMessage.model_validate(raw)
        except ValidationError as e:
            # Extract the first human-readable error
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
        # A user can only send to channels they've joined.
        # This prevents: spam to arbitrary channels, data leaks
        # to channels the sender shouldn't see.
        members = self._registry.get_channel_members(incoming.channel_id)
        if sender_id not in members:
            raise MessageValidationError(
                f"Not a member of channel '{incoming.channel_id}'"
            )

        # ── Step 4: Enrich ────────────────────────────────────
        # Server stamps messageId + timestamp + sender identity.
        # The sender_id comes from the CONNECTION, not the payload.
        # This is a critical security property — the client cannot
        # forge the sender field.
        enriched = EnrichedMessage(
            channel_id=incoming.channel_id,
            sender_id=sender_id,
            content=incoming.content,
        )

        # ── Step 5: Structured log (placeholder for Kafka) ───
        # In production, this becomes:
        #   await kafka_producer.send("messages", enriched.to_log_dict())
        #
        # The structured log lets us verify the exact payload shape
        # before we wire up Kafka. If the log looks right, the
        # Kafka message will look right.
        logger.info(
            "Message ingested",
            extra={"message_data": enriched.to_log_dict()},
        )

        return enriched
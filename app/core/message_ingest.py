"""
Message Ingestion — validate, enrich, produce, with full observability.

Every message gets a correlation_id at the moment it enters this
module. That ID appears in every log line, the Kafka payload,
the ack response, and the error response. To trace a message:

    jq 'select(.correlation_id == "abc-123")' logs/ingest.json

You'll see: received → validated → produced → ack_sent (or failed).
"""

import time
import uuid
import logging
from pydantic import ValidationError

from app.models import IncomingMessage, EnrichedMessage
from app.core.connection_registry import ConnectionRegistry
from app.core.kafka_producer import (
    kafka_producer,
    KafkaProduceError,
    KafkaCircuitOpenError,
)
from app.core.metrics import ingest_metrics
from app.core.structred_log import ingest_log

logger = logging.getLogger("message.ingest")


class MessageValidationError(Exception):
    """Carries a user-friendly reason safe for the WebSocket."""
    def __init__(self, reason: str, correlation_id: str = ""):
        self.reason = reason
        self.correlation_id = correlation_id
        super().__init__(reason)


class IngestResult:
    """Everything the WebSocket handler needs to build an ack.

    Bundles the enriched message with Kafka delivery metadata
    and timing info so the handler doesn't have to recompute.
    """
    def __init__(
        self,
        enriched: EnrichedMessage,
        kafka_partition: int,
        kafka_offset: int,
        pipeline_ms: float,
    ):
        self.enriched = enriched
        self.kafka_partition = kafka_partition
        self.kafka_offset = kafka_offset
        self.pipeline_ms = pipeline_ms


class MessageIngestService:
    """Validates, enriches, produces, and observes messages."""

    def __init__(self, registry: ConnectionRegistry):
        self._registry = registry

    async def validate_and_enrich(
        self, raw: dict, sender_id: str
    ) -> IngestResult:
        """Full pipeline: validate → enrich → produce → observe.

        Returns IngestResult with everything needed for the ack.

        Raises:
            MessageValidationError: client error (bad payload, not member)
            KafkaProduceError: infra error (broker down, timeout)
            KafkaCircuitOpenError: circuit breaker open (fail fast)
        """
        t_start = time.monotonic()
        correlation_id = str(uuid.uuid4())

        # ── Metrics: count every attempt ──────────────────────
        ingest_metrics.record_received()

        # ── Log: message arrived ──────────────────────────────
        ingest_log.message_received(
            correlation_id=correlation_id,
            user_id=sender_id,
            raw_type=raw.get("type", "<missing>"),
        )

        # ── Step 1: Schema validation ─────────────────────────
        try:
            incoming = IncomingMessage.model_validate(raw)
        except ValidationError as e:
            first_err = e.errors()[0]
            field = ".".join(str(loc) for loc in first_err["loc"])
            reason = f"Invalid message: {field} — {first_err['msg']}"

            self._log_failure(
                correlation_id, sender_id, None,
                stage="validation", error=reason, t_start=t_start,
            )
            raise MessageValidationError(reason, correlation_id)

        # ── Step 2: Type check ────────────────────────────────
        if incoming.type != "message.send":
            reason = f"Expected type 'message.send', got '{incoming.type}'"
            self._log_failure(
                correlation_id, sender_id, incoming.channel_id,
                stage="type_check", error=reason, t_start=t_start,
            )
            raise MessageValidationError(reason, correlation_id)

        # ── Step 3: Channel membership ────────────────────────
        members = self._registry.get_channel_members(incoming.channel_id)
        if sender_id not in members:
            reason = f"Not a member of channel '{incoming.channel_id}'"
            self._log_failure(
                correlation_id, sender_id, incoming.channel_id,
                stage="membership", error=reason, t_start=t_start,
            )
            raise MessageValidationError(reason, correlation_id)

        # ── Log: validation passed ────────────────────────────
        ingest_log.message_validated(
            correlation_id=correlation_id,
            user_id=sender_id,
            channel_id=incoming.channel_id,
            content_length=len(incoming.content),
            client_request_id=incoming.client_request_id,
        )

        # ── Step 4: Enrich ────────────────────────────────────
        enriched = EnrichedMessage(
            correlation_id=correlation_id,
            channel_id=incoming.channel_id,
            sender_id=sender_id,
            content=incoming.content,
            client_request_id=incoming.client_request_id,
        )

        # ── Step 5: Produce to Kafka ──────────────────────────
        try:
            result = await kafka_producer.produce_message(
                enriched.to_kafka_dict()
            )
        except (KafkaProduceError, KafkaCircuitOpenError) as e:
            self._log_failure(
                correlation_id, sender_id, incoming.channel_id,
                stage="kafka_produce", error=str(e), t_start=t_start,
            )
            raise

        # ── Log: produced successfully ────────────────────────
        pipeline_ms = (time.monotonic() - t_start) * 1000
        ingest_log.message_produced(
            correlation_id=correlation_id,
            user_id=sender_id,
            channel_id=enriched.channel_id,
            message_id=enriched.message_id,
            partition=result["partition"],
            offset=result["offset"],
            latency_ms=pipeline_ms,
        )

        return IngestResult(
            enriched=enriched,
            kafka_partition=result["partition"],
            kafka_offset=result["offset"],
            pipeline_ms=pipeline_ms,
        )

    def _log_failure(
        self,
        correlation_id: str,
        user_id: str,
        channel_id: str | None,
        stage: str,
        error: str,
        t_start: float,
    ) -> None:
        duration_ms = (time.monotonic() - t_start) * 1000
        ingest_metrics.record_failed()
        ingest_log.message_failed(
            correlation_id=correlation_id,
            user_id=user_id,
            channel_id=channel_id,
            stage=stage,
            error=error,
            duration_ms=duration_ms,
        )
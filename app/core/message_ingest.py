"""
Message Ingestion — validate, dedup, enrich, produce.

Pipeline:
  receive → validate → DEDUP CHECK → enrich → produce → DEDUP STORE → ack

The dedup step sits between validation and Kafka:
  - If client_request_id was seen before → return cached result
  - If new → produce to Kafka, then cache the result

This means a client can safely retry after a disconnect.
The second attempt returns the same message_id and timestamp
as the first, and Kafka only has one copy.
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
from app.core.dedup import dedup_service
from app.core.metrics import ingest_metrics
from app.core.structred_log import ingest_log

logger = logging.getLogger("message.ingest")


class MessageValidationError(Exception):
    def __init__(self, reason: str, correlation_id: str = ""):
        self.reason = reason
        self.correlation_id = correlation_id
        super().__init__(reason)


class IngestResult:
    """Everything the WebSocket handler needs to build an ack."""
    def __init__(
        self,
        enriched: EnrichedMessage,
        kafka_partition: int,
        kafka_offset: int,
        pipeline_ms: float,
        was_dedup: bool = False,
    ):
        self.enriched = enriched
        self.kafka_partition = kafka_partition
        self.kafka_offset = kafka_offset
        self.pipeline_ms = pipeline_ms
        self.was_dedup = was_dedup


class MessageIngestService:

    def __init__(self, registry: ConnectionRegistry):
        self._registry = registry

    async def validate_and_enrich(
        self, raw: dict, sender_id: str
    ) -> IngestResult:
        t_start = time.monotonic()
        correlation_id = str(uuid.uuid4())

        ingest_metrics.record_received()

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
        members = await self._registry.get_channel_members(
            incoming.channel_id
        )
        if sender_id not in members:
            reason = f"Not a member of channel '{incoming.channel_id}'"
            self._log_failure(
                correlation_id, sender_id, incoming.channel_id,
                stage="membership", error=reason, t_start=t_start,
            )
            raise MessageValidationError(reason, correlation_id)

        ingest_log.message_validated(
            correlation_id=correlation_id,
            user_id=sender_id,
            channel_id=incoming.channel_id,
            content_length=len(incoming.content),
            client_request_id=incoming.client_request_id,
        )

        # ── Step 4: Dedup check ───────────────────────────────
        dedup_result = await dedup_service.check(
            sender_id, incoming.client_request_id
        )

        if dedup_result.is_duplicate and dedup_result.cached_data:
            # Rebuild EnrichedMessage from cached data
            cached = dedup_result.cached_data
            enriched = EnrichedMessage(
                message_id=cached["messageId"],
                correlation_id=correlation_id,
                channel_id=cached["channelId"],
                sender_id=cached["senderId"],
                content=cached["content"],
                timestamp=cached["timestamp"],
                client_request_id=cached.get("clientRequestId"),
            )
            pipeline_ms = (time.monotonic() - t_start) * 1000

            logger.info(
                f"Dedup: returning cached message "
                f"id={enriched.message_id} for "
                f"client_request_id={incoming.client_request_id}"
            )

            return IngestResult(
                enriched=enriched,
                kafka_partition=-1,  # not produced this time
                kafka_offset=-1,
                pipeline_ms=pipeline_ms,
                was_dedup=True,
            )

        # ── Step 5: Enrich ────────────────────────────────────
        enriched = EnrichedMessage(
            correlation_id=correlation_id,
            channel_id=incoming.channel_id,
            sender_id=sender_id,
            content=incoming.content,
            client_request_id=incoming.client_request_id,
        )

        # ── Step 6: Produce to Kafka ──────────────────────────
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

        # ── Step 7: Store in dedup cache ──────────────────────
        # AFTER Kafka confirms. If Kafka fails, we don't cache,
        # so the client can retry and actually produce.
        await dedup_service.store(
            sender_id,
            incoming.client_request_id,
            enriched.to_kafka_dict(),
        )

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
            was_dedup=False,
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
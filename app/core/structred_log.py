"""
Structured logging — JSON log events for the ingestion pipeline.

Why structured logs instead of f-strings?
  1. Machine-parseable — ship to ELK/Datadog/CloudWatch without regex
  2. Consistent schema — every log for a message has correlation_id
  3. Greppable — `jq '.correlation_id == "abc123"'` gives you the
     full lifecycle of one message across all services

Log events:
  message.received   — raw message arrived on WebSocket
  message.validated   — passed schema + membership checks
  message.produced   — confirmed delivered to Kafka
  message.failed     — failed at any stage
  message.ack_sent   — ack sent back to client

Every event carries:
  - correlation_id: ties the full pipeline together
  - user_id: who sent it
  - channel_id: where it's going (when known)
  - event: the event name
  - duration_ms: time since pipeline started (when applicable)
"""

import json
import logging


class StructuredLogger:
    """Emits JSON-structured log lines for the ingest pipeline."""

    def __init__(self):
        self._logger = logging.getLogger("ingest.structured")

    def message_received(
        self, *, correlation_id: str, user_id: str, raw_type: str
    ) -> None:
        self._emit(
            event="message.received",
            correlation_id=correlation_id,
            user_id=user_id,
            raw_type=raw_type,
        )

    def message_validated(
        self,
        *,
        correlation_id: str,
        user_id: str,
        channel_id: str,
        content_length: int,
        client_request_id: str | None,
    ) -> None:
        self._emit(
            event="message.validated",
            correlation_id=correlation_id,
            user_id=user_id,
            channel_id=channel_id,
            content_length=content_length,
            client_request_id=client_request_id,
        )

    def message_produced(
        self,
        *,
        correlation_id: str,
        user_id: str,
        channel_id: str,
        message_id: str,
        partition: int,
        offset: int,
        latency_ms: float,
    ) -> None:
        self._emit(
            event="message.produced",
            correlation_id=correlation_id,
            user_id=user_id,
            channel_id=channel_id,
            message_id=message_id,
            kafka_partition=partition,
            kafka_offset=offset,
            produce_latency_ms=round(latency_ms, 2),
        )

    def message_failed(
        self,
        *,
        correlation_id: str,
        user_id: str,
        channel_id: str | None = None,
        stage: str,
        error: str,
        duration_ms: float | None = None,
    ) -> None:
        self._emit(
            level="error",
            event="message.failed",
            correlation_id=correlation_id,
            user_id=user_id,
            channel_id=channel_id,
            stage=stage,
            error=error,
            duration_ms=round(duration_ms, 2) if duration_ms else None,
        )

    def message_ack_sent(
        self,
        *,
        correlation_id: str,
        user_id: str,
        message_id: str,
        total_ms: float,
    ) -> None:
        self._emit(
            event="message.ack_sent",
            correlation_id=correlation_id,
            user_id=user_id,
            message_id=message_id,
            total_pipeline_ms=round(total_ms, 2),
        )

    def _emit(self, *, level: str = "info", **fields) -> None:
        # Remove None values for cleaner logs
        payload = {k: v for k, v in fields.items() if v is not None}
        log_fn = getattr(self._logger, level)
        log_fn(json.dumps(payload, default=str))


# ── Module-level singleton ────────────────────────────────────
ingest_log = StructuredLogger()
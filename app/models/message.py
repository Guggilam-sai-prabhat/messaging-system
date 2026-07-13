"""
Message data shapes — the contract between ingest and delivery.

IncomingMessage: what the client sends (minimal, untrusted)
EnrichedMessage: what the server produces (canonical, Kafka-ready)

Changes in v0.2:
  - client_request_id: optional dedup key from the client
  - correlation_id: server-generated trace ID for the full pipeline
"""

import time
import uuid
from typing import Optional
from pydantic import BaseModel, Field


class IncomingMessage(BaseModel):
    """What the client sends over WebSocket."""
    type: str = Field(
        ...,
        description="Must be 'message.send' for message ingestion",
    )
    channel_id: str = Field(
        ...,
        min_length=1,
        max_length=128,
        description="Target channel",
    )
    content: str = Field(
        ...,
        min_length=1,
        max_length=4096,
        description="Message body",
    )
    client_request_id: Optional[str] = Field(
        default=None,
        max_length=128,
        description=(
            "Client-generated idempotency key. If the client "
            "resends after a disconnect, same key = same message. "
            "Server uses this for application-level dedup."
        ),
    )


class EnrichedMessage(BaseModel):
    """Fully enriched message — the Kafka-ready schema."""
    message_id: str = Field(
        default_factory=lambda: str(uuid.uuid4()),
    )
    correlation_id: str = Field(
        default_factory=lambda: str(uuid.uuid4()),
        description=(
            "Trace ID for the full pipeline. Appears in every "
            "log line from receive → produce → ack. Use this "
            "to grep across services."
        ),
    )
    channel_id: str
    sender_id: str = Field(
        ...,
        description="From authenticated connection, NOT from payload",
    )
    content: str
    timestamp: float = Field(
        default_factory=time.time,
    )
    client_request_id: Optional[str] = Field(
        default=None,
        description="Echoed from IncomingMessage for dedup",
    )
    reply_to_message_id: Optional[str] = Field(
        default=None,
        description=(
            "messageId of the message this one is replying to. Set by the "
            "AI service when publishing a generated answer back onto "
            "channel-messages; absent for ordinary user messages."
        ),
    )

    def to_kafka_dict(self) -> dict:
        """Kafka payload with camelCase keys."""
        d = {
            "messageId": self.message_id,
            "correlationId": self.correlation_id,
            "channelId": self.channel_id,
            "senderId": self.sender_id,
            "content": self.content,
            "timestamp": self.timestamp,
        }
        if self.client_request_id:
            d["clientRequestId"] = self.client_request_id
        if self.reply_to_message_id:
            d["replyToMessageId"] = self.reply_to_message_id
        return d
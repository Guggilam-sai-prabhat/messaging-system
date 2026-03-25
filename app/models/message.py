"""
Message data shapes — the contract between ingest and delivery.

IncomingMessage: what the client sends (minimal, untrusted)
EnrichedMessage: what the server produces (canonical, Kafka-ready)
"""

import time
import uuid
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


class EnrichedMessage(BaseModel):
    """Fully enriched message — the Kafka-ready schema.

    Server-generated fields:
      - message_id: UUID4, globally unique without coordination
      - timestamp: server clock, not client clock
      - sender_id: from authenticated connection, never from payload
    """
    message_id: str = Field(
        default_factory=lambda: str(uuid.uuid4()),
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

    def to_log_dict(self) -> dict:
        """Structured log / Kafka payload with camelCase keys."""
        return {
            "messageId": self.message_id,
            "channelId": self.channel_id,
            "senderId": self.sender_id,
            "content": self.content,
            "timestamp": self.timestamp,
        }
"""
Event shapes — the contract between the Kafka consumer and everything downstream.

ChatMessageEvent: parsed from a channel-messages record (see
    app/models/message.py EnrichedMessage.to_kafka_dict for the producer side)
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class ChatMessageEvent:
    message_id: str
    correlation_id: str
    channel_id: str
    sender_id: str
    content: str
    timestamp: float
    reply_to_message_id: str | None = None

    @classmethod
    def from_kafka_dict(cls, payload: dict) -> "ChatMessageEvent":
        return cls(
            message_id=payload["messageId"],
            correlation_id=payload["correlationId"],
            channel_id=payload["channelId"],
            sender_id=payload["senderId"],
            content=payload["content"],
            timestamp=payload["timestamp"],
            reply_to_message_id=payload.get("replyToMessageId"),
        )

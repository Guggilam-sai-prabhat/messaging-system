from datetime import datetime
from sqlalchemy import String, Text, DateTime, Index, func
from sqlalchemy.orm import Mapped, mapped_column
from .base import Base


class Message(Base):
    __tablename__ = "messages"

    message_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    channel_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    sender_id: Mapped[str] = mapped_column(String(128), nullable=False)
    content: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    correlation_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    client_request_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    persisted_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )

    __table_args__ = (
        Index("idx_messages_channel_time", "channel_id", created_at.desc()),
        Index("idx_messages_sender_time", "sender_id", created_at.desc()),
        Index("idx_messages_persisted", persisted_at.desc()),
    )

    def __repr__(self) -> str:
        return f"<Message id={self.message_id[:8]}... channel={self.channel_id}>"

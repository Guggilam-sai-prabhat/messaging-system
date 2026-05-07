from datetime import datetime
from sqlalchemy import String, Text, Boolean, DateTime, Index, func
from sqlalchemy.orm import Mapped, mapped_column
from .base import Base


class Channel(Base):
    __tablename__ = "channels"

    channel_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    name: Mapped[str] = mapped_column(String(256), nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_by: Mapped[str] = mapped_column(String(128), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False,
    )
    is_deleted: Mapped[bool] = mapped_column(
        Boolean, default=False, server_default="false", nullable=False,
    )
    deleted_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True,
    )

    __table_args__ = (
        Index(
            "idx_channels_active", "is_deleted", created_at.desc(),
            postgresql_where=(is_deleted == False),  # noqa: E712
        ),
        Index("idx_channels_created_by", "created_by", created_at.desc()),
    )

    def __repr__(self) -> str:
        return f"<Channel id={self.channel_id[:8]}... name={self.name!r}>"

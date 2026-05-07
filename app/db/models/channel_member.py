from datetime import datetime
from sqlalchemy import String, DateTime, Index, CheckConstraint, func
from sqlalchemy.orm import Mapped, mapped_column
from .base import Base


class ChannelMember(Base):
    __tablename__ = "channel_members"

    channel_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    user_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    role: Mapped[str] = mapped_column(
        String(32), nullable=False, default="member", server_default="member",
    )
    joined_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False,
    )

    __table_args__ = (
        Index("idx_channel_members_user", "user_id"),
        CheckConstraint(
            "role IN ('owner', 'admin', 'member')",
            name="ck_channel_members_role",
        ),
    )

    def __repr__(self) -> str:
        return (
            f"<ChannelMember channel={self.channel_id[:8]}... "
            f"user={self.user_id[:8]}... role={self.role}>"
        )

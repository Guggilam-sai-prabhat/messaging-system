from datetime import datetime
from sqlalchemy import String, Boolean, DateTime, Index, func
from sqlalchemy.orm import Mapped, mapped_column
from .base import Base


class RefreshToken(Base):
    __tablename__ = "refresh_tokens"

    token_id: Mapped[str] = mapped_column(
        String(128),
        primary_key=True,
        comment="UUID — the token itself is a separate opaque string.",
    )
    user_id: Mapped[str] = mapped_column(
        String(128),
        nullable=False,
        index=True,
    )
    token_hash: Mapped[str] = mapped_column(
        String(256),
        unique=True,
        nullable=False,
        comment="SHA-256 of the actual token value. Never store the raw token.",
    )
    device_id: Mapped[str | None] = mapped_column(
        String(256),
        nullable=True,
        comment="Client-provided device identifier.",
    )
    user_agent: Mapped[str | None] = mapped_column(
        String(512),
        nullable=True,
    )

    is_revoked: Mapped[bool] = mapped_column(
        Boolean,
        default=False,
        server_default="false",
        nullable=False,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    expires_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )
    last_used_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    # All tokens in a rotation chain share a family_id.
    # A revoked token being reused signals theft — revoke the entire family.
    family_id: Mapped[str] = mapped_column(
        String(128),
        nullable=False,
    )

    __table_args__ = (
        Index(
            "idx_refresh_tokens_user_active",
            "user_id",
            "is_revoked",
            postgresql_where=(is_revoked == False),  # noqa: E712
        ),
        Index("idx_refresh_tokens_hash", "token_hash"),
        Index("idx_refresh_tokens_expires", "expires_at"),
    )

    def __repr__(self) -> str:
        return (
            f"<RefreshToken id={self.token_id[:8]}... "
            f"user={self.user_id[:8]}... "
            f"revoked={self.is_revoked}>"
        )

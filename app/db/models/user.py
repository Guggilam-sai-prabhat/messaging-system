from datetime import datetime
from sqlalchemy import String, Boolean, DateTime, Index, func
from sqlalchemy.orm import Mapped, mapped_column
from .base import Base


class User(Base):
    __tablename__ = "users"

    user_id: Mapped[str] = mapped_column(
        String(128),
        primary_key=True,
        comment="UUID generated at registration.",
    )
    email: Mapped[str] = mapped_column(
        String(320),
        unique=True,
        nullable=False,
        comment="RFC 5321 max email length is 320 chars.",
    )
    display_name: Mapped[str] = mapped_column(
        String(256),
        nullable=False,
        comment="Shown to other users. Not unique.",
    )
    password_hash: Mapped[str | None] = mapped_column(
        String(256),
        nullable=True,
        comment="bcrypt hash. Null for OAuth-only accounts.",
    )

    oauth_provider: Mapped[str | None] = mapped_column(
        String(64),
        nullable=True,
        comment="'google' or 'github'. Null for password-only.",
    )
    oauth_provider_id: Mapped[str | None] = mapped_column(
        String(256),
        nullable=True,
        comment="The user's ID from the OAuth provider.",
    )

    is_active: Mapped[bool] = mapped_column(
        Boolean,
        default=True,
        server_default="true",
        nullable=False,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    last_login_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment="Updated on each successful login.",
    )

    __table_args__ = (
        Index(
            "idx_users_oauth",
            "oauth_provider",
            "oauth_provider_id",
            unique=True,
            postgresql_where=(oauth_provider.isnot(None)),
        ),
        Index("idx_users_email", "email"),
    )

    def __repr__(self) -> str:
        return f"<User id={self.user_id[:8]}... email={self.email}>"

"""
SQLAlchemy models — complete schema.

Tables:
  users            — registered accounts (email/password + OAuth)
  refresh_tokens   — long-lived tokens for silent re-auth
  messages         — persisted message history
  channels         — channel metadata
  channel_members  — membership edges

All models share one Base so Alembic sees them in a single
autogenerate pass.
"""

from datetime import datetime
from sqlalchemy import (
    String,
    Text,
    Boolean,
    DateTime,
    Index,
    CheckConstraint,
    func,
)
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    mapped_column,
)


class Base(DeclarativeBase):
    pass


VALID_ROLES = {"owner", "admin", "member"}


# ─────────────────────────────────────────────────────────────
# Users
# ─────────────────────────────────────────────────────────────

class User(Base):
    """
    User account — supports both email/password and OAuth.

    Design decisions:

      password_hash is nullable:
        OAuth-only users have no password. If they later want
        to set one (account linking), they go through a "set
        password" flow. The service layer enforces that at least
        one auth method exists (password OR oauth provider).

      oauth_provider + oauth_provider_id:
        Stored as plain strings, not an enum. Adding a new
        provider (Apple, Microsoft) is just a new string value,
        no migration needed. The unique constraint on
        (oauth_provider, oauth_provider_id) prevents duplicate
        OAuth links.

      display_name vs username:
        display_name is what other users see ("Alice Johnson").
        We don't enforce uniqueness — real names collide.
        If you need @-mentions, add a unique `username` column
        later. YAGNI for now.

      is_active for soft-disable:
        Banned / deactivated users have is_active=False.
        The auth layer rejects their tokens without deleting
        their data. Reactivation is flipping a boolean.
    """
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

    # ── OAuth fields ──────────────────────────────────────────
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

    # ── Account state ─────────────────────────────────────────
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
        # OAuth dedup: one Google account can only link to one user.
        Index(
            "idx_users_oauth",
            "oauth_provider",
            "oauth_provider_id",
            unique=True,
            postgresql_where=(oauth_provider.isnot(None)),
        ),
        # Email lookups on login
        Index(
            "idx_users_email",
            "email",
        ),
    )

    def __repr__(self) -> str:
        return f"<User id={self.user_id[:8]}... email={self.email}>"


# ─────────────────────────────────────────────────────────────
# Refresh Tokens
# ─────────────────────────────────────────────────────────────

class RefreshToken(Base):
    """
    Persisted refresh tokens — enables revocation and rotation.

    Why store refresh tokens in the DB instead of just signing them?
    ───────────────────────────────────────────────────────────────
      Signed-only refresh tokens can't be revoked. If a user's
      device is stolen, you need to invalidate that device's
      refresh token immediately. With DB-backed tokens, you
      DELETE the row and the token is dead.

      The tradeoff: every token refresh hits the DB. But refresh
      happens once every 15 minutes (when the access token
      expires), not on every API call. The DB can handle this.

    Token rotation:
      When a refresh token is used, we issue a new one and
      invalidate the old one (is_revoked = True). If someone
      tries to use a revoked token, it means the token was
      stolen and replayed — we revoke ALL tokens for that user
      as a security measure.

    Device tracking:
      device_id + user_agent let the user see "where am I
      logged in?" and revoke specific sessions. Essential for
      a messaging app where users care about security.
    """
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
        comment="SHA-256 of the actual token value. We never "
                "store the raw token — same principle as passwords.",
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

    # ── Lifecycle ─────────────────────────────────────────────
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

    # ── Token family for rotation detection ───────────────────
    family_id: Mapped[str] = mapped_column(
        String(128),
        nullable=False,
        comment="All tokens in a rotation chain share a family_id. "
                "If a revoked token is reused, we revoke the entire "
                "family — signals token theft.",
    )

    __table_args__ = (
        # "Show me all active sessions for user X"
        Index(
            "idx_refresh_tokens_user_active",
            "user_id",
            "is_revoked",
            postgresql_where=(is_revoked == False),  # noqa: E712
        ),
        # Token lookup on refresh
        Index(
            "idx_refresh_tokens_hash",
            "token_hash",
        ),
        # Cleanup job: delete expired tokens
        Index(
            "idx_refresh_tokens_expires",
            "expires_at",
        ),
    )

    def __repr__(self) -> str:
        return (
            f"<RefreshToken id={self.token_id[:8]}... "
            f"user={self.user_id[:8]}... "
            f"revoked={self.is_revoked}>"
        )


# ─────────────────────────────────────────────────────────────
# Messages (unchanged)
# ─────────────────────────────────────────────────────────────

class Message(Base):
    __tablename__ = "messages"

    message_id: Mapped[str] = mapped_column(
        String(128), primary_key=True,
    )
    channel_id: Mapped[str] = mapped_column(
        String(128), nullable=False, index=True,
    )
    sender_id: Mapped[str] = mapped_column(
        String(128), nullable=False,
    )
    content: Mapped[str] = mapped_column(
        Text, nullable=False,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False,
    )
    correlation_id: Mapped[str | None] = mapped_column(
        String(128), nullable=True,
    )
    client_request_id: Mapped[str | None] = mapped_column(
        String(128), nullable=True,
    )
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


# ─────────────────────────────────────────────────────────────
# Channels
# ─────────────────────────────────────────────────────────────

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


# ─────────────────────────────────────────────────────────────
# Channel Members
# ─────────────────────────────────────────────────────────────

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
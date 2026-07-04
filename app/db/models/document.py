# models/document.py

from datetime import datetime
from sqlalchemy import (
    String,
    Text,
    BigInteger,
    DateTime,
    CheckConstraint,
    Index,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column
from .base import Base


VALID_DOC_STATUSES = {"processing", "ready", "embedding_failed", "failed"}


class Document(Base):
    """
    Uploaded file metadata.

    The actual file bytes live on the filesystem (or S3 later).
    This table tracks ownership, status, and the storage path
    so we never lose track of what's where.

    Why status lives here and not in Redis:
      Unlike channel membership (hot path, read on every message
      delivery), document status is read infrequently — once
      after upload to check progress, then on channel load to
      list attachments. No need for the Redis complexity.
      A simple SELECT with an index is fine.

    Why file_size_bytes is stored:
      We validate size on upload, but also need it later for
      display ("2.3 MB") and for storage quota enforcement
      if you add that. Cheaper to store it once than to stat
      the filesystem on every read.

    Why content_type is stored even though we only allow PDFs now:
      You'll support images and other file types within weeks.
      Adding a column later means a migration and backfill.
      Storing it now costs one string per row.
    """
    __tablename__ = "documents"

    document_id: Mapped[str] = mapped_column(
        String(128),
        primary_key=True,
        comment="UUID generated at upload time.",
    )
    channel_id: Mapped[str] = mapped_column(
        String(128),
        nullable=False,
        comment="Channel this document belongs to.",
    )
    file_name: Mapped[str] = mapped_column(
        String(512),
        nullable=False,
        comment="Original filename from the client. Sanitized on write.",
    )
    content_type: Mapped[str] = mapped_column(
        String(128),
        nullable=False,
        default="application/pdf",
        server_default="application/pdf",
        comment="MIME type. Currently only application/pdf.",
    )
    file_size_bytes: Mapped[int] = mapped_column(
        BigInteger,
        nullable=False,
        comment="Size in bytes at upload time.",
    )
    uploaded_by: Mapped[str] = mapped_column(
        String(128),
        nullable=False,
        comment="user_id of the uploader.",
    )
    status: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        default="processing",
        server_default="processing",
        comment="processing → ready → embedding_failed | processing → failed",
    )
    error_message: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
        comment="Why processing/embedding failed, if status = 'failed' or 'embedding_failed'.",
    )
    storage_path: Mapped[str] = mapped_column(
        String(1024),
        nullable=False,
        comment="Relative path: /uploads/{channelId}/{documentId}.pdf",
    )
    sha256_hash: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
        comment="SHA-256 of file contents. Used for dedup detection.",
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )

    __table_args__ = (
        # "Show all documents in this channel, newest first"
        # This is your primary read pattern — channel detail page
        # loads attachments sorted by upload time.
        Index(
            "idx_documents_channel_time",
            "channel_id",
            created_at.desc(),
        ),
        # "Show all uploads by this user" — admin/moderation view,
        # also useful for per-user quota enforcement.
        Index(
            "idx_documents_uploaded_by",
            "uploaded_by",
            created_at.desc(),
        ),
        # Duplicate detection: same file in the same channel.
        # UNIQUE on (channel_id, sha256_hash) means uploading the
        # exact same PDF twice to the same channel is rejected.
        # Different channels can have the same file — that's fine.
        Index(
            "idx_documents_channel_hash",
            "channel_id",
            "sha256_hash",
            unique=True,
        ),
        # Status filtering: "find all failed uploads for retry"
        # Partial index — only indexes non-ready docs, which is
        # a small subset. Keeps the index tiny.
        Index(
            "idx_documents_status",
            "status",
            postgresql_where=(status != "ready"),
        ),
        CheckConstraint(
            "status IN ('processing', 'ready', 'embedding_failed', 'failed')",
            name="ck_documents_status",
        ),
    )

    def __repr__(self) -> str:
        return (
            f"<Document id={self.document_id[:8]}... "
            f"channel={self.channel_id[:8]}... "
            f"status={self.status}>"
        )
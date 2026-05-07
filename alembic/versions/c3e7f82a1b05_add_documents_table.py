"""add documents table

Revision ID: c3e7f82a1b05
Revises: af16df0e9d19
Create Date: 2026-05-06 00:00:00.000000

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "c3e7f82a1b05"
down_revision: Union[str, None] = "af16df0e9d19"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "documents",
        sa.Column("document_id", sa.String(128), primary_key=True,
                  comment="UUID generated at upload time."),
        sa.Column("channel_id", sa.String(128), nullable=False,
                  comment="Channel this document belongs to."),
        sa.Column("file_name", sa.String(512), nullable=False,
                  comment="Original filename from the client. Sanitized on write."),
        sa.Column("content_type", sa.String(128), nullable=False,
                  server_default="application/pdf",
                  comment="MIME type. Currently only application/pdf."),
        sa.Column("file_size_bytes", sa.BigInteger(), nullable=False,
                  comment="Size in bytes at upload time."),
        sa.Column("uploaded_by", sa.String(128), nullable=False,
                  comment="user_id of the uploader."),
        sa.Column("status", sa.String(32), nullable=False,
                  server_default="processing",
                  comment="processing → ready | failed"),
        sa.Column("error_message", sa.Text(), nullable=True,
                  comment="Why processing failed, if status = 'failed'."),
        sa.Column("storage_path", sa.String(1024), nullable=False,
                  comment="Relative path: /uploads/{channelId}/{documentId}.pdf"),
        sa.Column("sha256_hash", sa.String(64), nullable=False,
                  comment="SHA-256 of file contents. Used for dedup detection."),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "status IN ('processing', 'ready', 'failed')",
            name="ck_documents_status",
        ),
    )

    # "Show all documents in this channel, newest first"
    op.create_index(
        "idx_documents_channel_time",
        "documents",
        ["channel_id", sa.text("created_at DESC")],
    )

    # "Show all uploads by this user"
    op.create_index(
        "idx_documents_uploaded_by",
        "documents",
        ["uploaded_by", sa.text("created_at DESC")],
    )

    # Duplicate detection: same file in the same channel
    op.create_index(
        "idx_documents_channel_hash",
        "documents",
        ["channel_id", "sha256_hash"],
        unique=True,
    )

    # Partial index: only non-ready docs (failed + processing are a small subset)
    op.create_index(
        "idx_documents_status",
        "documents",
        ["status"],
        postgresql_where=sa.text("status != 'ready'"),
    )


def downgrade() -> None:
    op.drop_index("idx_documents_status", table_name="documents")
    op.drop_index("idx_documents_channel_hash", table_name="documents")
    op.drop_index("idx_documents_uploaded_by", table_name="documents")
    op.drop_index("idx_documents_channel_time", table_name="documents")
    op.drop_table("documents")

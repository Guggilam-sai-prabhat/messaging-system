"""add document_chunks table with pgvector

Revision ID: d1e4f93b2c07
Revises: c3e7f82a1b05
Create Date: 2026-06-17 00:00:00.000000

Design notes
------------
- pgvector's VECTOR(1536) stores one OpenAI text-embedding-3-small embedding.
- HNSW index on (embedding) with cosine distance is the production-grade ANN
  index. It trades a small amount of recall (~98%) for query times that do not
  degrade linearly with table size (unlike the older IVFFlat index).
- A composite B-Tree index on (channel_id, document_id) supports the two most
  common filtered queries:
    1. "all chunks for this channel"  — used by semantic search
    2. "all chunks for this document" — used by delete-on-document-removal
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from pgvector.sqlalchemy import Vector

revision: str = "d1e4f93b2c07"
down_revision: Union[str, None] = "c3e7f82a1b05"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

EMBEDDING_DIM = 1536  # OpenAI text-embedding-3-small


def upgrade() -> None:
    # Enable the pgvector extension. IF NOT EXISTS makes this idempotent
    # so re-running the migration (e.g. in CI) never fails.
    op.execute("CREATE EXTENSION IF NOT EXISTS vector")

    op.create_table(
        "document_chunks",
        sa.Column(
            "id",
            sa.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
            comment="Surrogate PK. UUIDs are safe for distributed inserts without coordination.",
        ),
        sa.Column(
            "document_id",
            sa.String(128),
            sa.ForeignKey("documents.document_id", ondelete="CASCADE"),
            nullable=False,
            comment="Parent document. ON DELETE CASCADE keeps chunks in sync with document deletes.",
        ),
        sa.Column(
            "channel_id",
            sa.String(128),
            nullable=False,
            comment="Denormalised from documents. Allows channel-scoped search without a JOIN.",
        ),
        sa.Column(
            "chunk_index",
            sa.Integer(),
            nullable=False,
            comment="Zero-based position of this chunk within its document. Used to reconstruct reading order.",
        ),
        sa.Column(
            "content",
            sa.Text(),
            nullable=False,
            comment="The raw text of this chunk. Returned to the LLM as context.",
        ),
        sa.Column(
            "embedding",
            Vector(EMBEDDING_DIM),
            nullable=True,
            comment="NULL until the embedding worker processes this chunk.",
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.UniqueConstraint(
            "document_id",
            "chunk_index",
            name="uq_document_chunks_doc_chunk",
        ),
    )

    # ── Indexes ──────────────────────────────────────────────────────────────

    # B-Tree: filters before vector search — "only search THIS channel's chunks"
    # Also covers "all chunks for this document" (document_id is the leftmost
    # prefix when channel_id matches, so Postgres can use this for both).
    op.create_index(
        "idx_document_chunks_channel_doc",
        "document_chunks",
        ["channel_id", "document_id"],
    )

    # HNSW vector index with cosine distance.
    #
    # HNSW (Hierarchical Navigable Small World) builds a layered graph where
    # each node links to its nearest neighbours. At query time it navigates
    # the graph rather than scanning every row — O(log n) not O(n).
    #
    # m=16       — number of bi-directional links per node.
    #              Higher → better recall, more memory (rule of thumb: 8–64).
    # ef_construction=64 — search width during index build.
    #              Higher → better recall at build time, slower to build.
    #
    # These are conservative production defaults. You can tune after benchmarking.
    op.execute(
        """
        CREATE INDEX idx_document_chunks_embedding_hnsw
        ON document_chunks
        USING hnsw (embedding vector_cosine_ops)
        WITH (m = 16, ef_construction = 64)
        """
    )


def downgrade() -> None:
    op.drop_index("idx_document_chunks_embedding_hnsw", table_name="document_chunks")
    op.drop_index("idx_document_chunks_channel_doc", table_name="document_chunks")
    op.drop_table("document_chunks")
    # Do NOT drop the vector extension — other tables may use it.

"""change embedding vector dimension from 1536 to 768

Revision ID: e2a7f01b3d08
Revises: d1e4f93b2c07
Create Date: 2026-06-30 00:00:00.000000

Why 768 instead of 1536
-----------------------
We switched from OpenAI text-embedding-3-small (1536d, paid API) to
BAAI/bge-base-en-v1.5 (768d, free, runs locally via sentence-transformers).

MTEB retrieval benchmarks show bge-base-en-v1.5 performs within ~1% of
text-embedding-3-small while cutting vector storage in half and eliminating
API cost and rate-limiting entirely.

Migration procedure
-------------------
pgvector's Vector type is internally stored as a fixed-width float array.
Changing the dimension requires:
  1. Drop the HNSW index (cannot be altered in-place).
  2. ALTER COLUMN TYPE to Vector(768) — rewrites the column for all rows.
     Existing 1536-d embeddings are deleted (set to NULL) in the same step.
  3. Recreate the HNSW index with the new dimension.

Existing rows
-------------
Any existing embeddings are 1536-d and incompatible with the new model.
We NULL them out so they will be re-embedded when the worker reprocesses
those documents. The downgrade path restores the old dimension in the same
safe sequence.
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from pgvector.sqlalchemy import Vector

revision: str = "e2a7f01b3d08"
down_revision: Union[str, None] = "d1e4f93b2c07"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

OLD_DIM = 1536
NEW_DIM = 768


def upgrade() -> None:
    # Step 1 — drop HNSW index (incompatible with dimension change)
    op.drop_index("idx_document_chunks_embedding_hnsw", table_name="document_chunks")

    # Step 2 — change column type; NULL out existing embeddings
    op.execute(
        f"""
        ALTER TABLE document_chunks
        ALTER COLUMN embedding TYPE vector({NEW_DIM})
        USING NULL
        """
    )

    # Step 3 — recreate HNSW index for the new dimension
    op.execute(
        f"""
        CREATE INDEX idx_document_chunks_embedding_hnsw
        ON document_chunks
        USING hnsw (embedding vector_cosine_ops)
        WITH (m = 16, ef_construction = 64)
        """
    )


def downgrade() -> None:
    op.drop_index("idx_document_chunks_embedding_hnsw", table_name="document_chunks")

    op.execute(
        f"""
        ALTER TABLE document_chunks
        ALTER COLUMN embedding TYPE vector({OLD_DIM})
        USING NULL
        """
    )

    op.execute(
        f"""
        CREATE INDEX idx_document_chunks_embedding_hnsw
        ON document_chunks
        USING hnsw (embedding vector_cosine_ops)
        WITH (m = 16, ef_construction = 64)
        """
    )

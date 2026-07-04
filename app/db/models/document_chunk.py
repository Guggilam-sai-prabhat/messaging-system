from datetime import datetime
from uuid import UUID, uuid4

from pgvector.sqlalchemy import Vector
from sqlalchemy import (
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base

EMBEDDING_DIM = 768  # BAAI/bge-base-en-v1.5 (sentence-transformers, free/local)


class DocumentChunk(Base):
    """
    One text chunk of a document, with its vector embedding.

    channel_id is denormalised from documents so that semantic search
    can filter by channel without a JOIN — critical for tenant isolation.

    embedding is nullable until the embedding worker processes this chunk.
    The parent document is marked ready before chunks exist so the document
    is always accessible even if the embedding API is temporarily down.
    """

    __tablename__ = "document_chunks"

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
        server_default=func.gen_random_uuid(),
    )
    document_id: Mapped[str] = mapped_column(
        String(128),
        ForeignKey("documents.document_id", ondelete="CASCADE"),
        nullable=False,
    )
    channel_id: Mapped[str] = mapped_column(
        String(128),
        nullable=False,
    )
    chunk_index: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        comment="Zero-based position within the document. Preserves reading order.",
    )
    content: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        comment="Raw text of this chunk. Returned to the LLM as context.",
    )
    embedding: Mapped[list[float] | None] = mapped_column(
        Vector(EMBEDDING_DIM),
        nullable=True,
        comment="NULL until the embedding worker processes this chunk.",
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )

    __table_args__ = (
        UniqueConstraint(
            "document_id",
            "chunk_index",
            name="uq_document_chunks_doc_chunk",
        ),
        # B-Tree: channel filter before ANN search ("only search THIS channel")
        Index("idx_document_chunks_channel_doc", "channel_id", "document_id"),
        # HNSW vector index — approximate nearest-neighbour with cosine distance.
        # Created raw because SQLAlchemy has no built-in HNSW dialect support.
        # m=16, ef_construction=64 are conservative production defaults.
        Index(
            "idx_document_chunks_embedding_hnsw",
            "embedding",
            postgresql_using="hnsw",
            postgresql_ops={"embedding": "vector_cosine_ops"},
            postgresql_with={"m": 16, "ef_construction": 64},
        ),
    )

    def __repr__(self) -> str:
        return (
            f"<DocumentChunk doc={self.document_id[:8]}... "
            f"idx={self.chunk_index} "
            f"embedded={'yes' if self.embedding is not None else 'no'}>"
        )

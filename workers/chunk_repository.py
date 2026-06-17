"""
Repository for document_chunks using the SQLAlchemy ORM.
"""

import logging

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.db.models.document_chunk import DocumentChunk

logger = logging.getLogger("document.worker")


class ChunkRepository:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    # ── Write path ────────────────────────────────────────────────────────────

    async def insert_chunks(
        self,
        document_id: str,
        channel_id: str,
        chunks: list[dict],
    ) -> None:
        """
        Bulk-insert chunks with embeddings in a single transaction.

        `chunks`: [{"chunk_index": int, "content": str, "embedding": list[float]}]

        ON CONFLICT DO NOTHING makes this idempotent — safe on Kafka redelivery.
        pg_insert (PostgreSQL dialect) supports ON CONFLICT; the generic ORM
        insert does not, so we use it here deliberately.
        """
        if not chunks:
            return

        rows = [
            {
                "document_id": document_id,
                "channel_id": channel_id,
                "chunk_index": c["chunk_index"],
                "content": c["content"],
                "embedding": c["embedding"],
            }
            for c in chunks
        ]

        stmt = pg_insert(DocumentChunk).values(rows)
        stmt = stmt.on_conflict_do_nothing(
            index_elements=["document_id", "chunk_index"]
        )

        async with self._session_factory() as session:
            async with session.begin():
                await session.execute(stmt)

        logger.info(f"document_id={document_id} inserted {len(chunks)} chunks")

    # ── Read path — semantic search ───────────────────────────────────────────

    async def semantic_search(
        self,
        channel_id: str,
        query_embedding: list[float],
        limit: int = 10,
        min_score: float = 0.0,
    ) -> list[dict]:
        """
        Return up to `limit` chunks in `channel_id` closest to `query_embedding`.

        cosine_distance = embedding <=> query  (0 = identical, 2 = opposite)
        score           = 1 - cosine_distance  (1 = identical, -1 = opposite)

        The WHERE channel_id filter is resolved via the B-Tree index before
        the HNSW index is consulted, so the ANN search only touches this
        channel's vectors.

        Results are ordered by ascending distance (most similar first).
        """
        from pgvector.sqlalchemy import cosine_distance

        query_vec = query_embedding  # list[float] — pgvector accepts this natively

        score_col = (1 - cosine_distance(DocumentChunk.embedding, query_vec)).label("score")
        distance_col = cosine_distance(DocumentChunk.embedding, query_vec)

        stmt = (
            select(
                DocumentChunk.id.label("chunk_id"),
                DocumentChunk.document_id,
                DocumentChunk.chunk_index,
                DocumentChunk.content,
                score_col,
            )
            .where(DocumentChunk.channel_id == channel_id)
            .where(DocumentChunk.embedding.is_not(None))
            .where(score_col >= min_score)
            .order_by(distance_col.asc())
            .limit(limit)
        )

        async with self._session_factory() as session:
            result = await session.execute(stmt)
            rows = result.mappings().all()

        return [dict(row) for row in rows]

    # ── Utility ───────────────────────────────────────────────────────────────

    async def delete_by_document(self, document_id: str) -> int:
        """
        Delete all chunks for a document. Returns row count.

        The FK ON DELETE CASCADE handles this automatically when the parent
        documents row is deleted. This method exists for explicit programmatic
        deletes (e.g. reprocessing a document without deleting the metadata).
        """
        from sqlalchemy import delete

        stmt = delete(DocumentChunk).where(DocumentChunk.document_id == document_id)

        async with self._session_factory() as session:
            async with session.begin():
                result = await session.execute(stmt)

        deleted = result.rowcount
        logger.info(f"document_id={document_id} deleted {deleted} chunks")
        return deleted

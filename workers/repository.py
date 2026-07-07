import logging

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from workers.config import MAX_PAGES

logger = logging.getLogger("document.worker")


class DocumentRepository:
    """Thin async repository for document status updates."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def get_status(self, document_id: str) -> str | None:
        """Current status of a document, or None if the row doesn't exist."""
        async with self._session_factory() as session:
            result = await session.execute(
                text("SELECT status FROM documents WHERE document_id = :doc_id"),
                {"doc_id": document_id},
            )
            row = result.first()
            return row[0] if row else None

    async def mark_ready(
        self,
        document_id: str,
        extracted_text: str,
        page_count: int,
        truncated: bool,
    ) -> None:
        truncation_note = (
            f" (truncated at {MAX_PAGES} pages)" if truncated else ""
        )
        async with self._session_factory() as session:
            async with session.begin():
                await session.execute(
                    text(
                        """
                        UPDATE documents
                        SET
                            status        = 'ready',
                            error_message = NULL
                        WHERE document_id = :doc_id
                          AND status      = 'processing'
                        """
                    ),
                    {"doc_id": document_id},
                )
        logger.info(
            f"document_id={document_id} → ready "
            f"(pages={page_count}{truncation_note})"
        )

    async def mark_failed(self, document_id: str, reason: str) -> None:
        short_reason = reason[:500]
        async with self._session_factory() as session:
            async with session.begin():
                await session.execute(
                    text(
                        """
                        UPDATE documents
                        SET
                            status        = 'failed',
                            error_message = :reason
                        WHERE document_id = :doc_id
                          AND status      = 'processing'
                        """
                    ),
                    {"doc_id": document_id, "reason": short_reason},
                )
        logger.warning(
            f"document_id={document_id} → failed | reason={short_reason}"
        )

    async def mark_embedding_failed(self, document_id: str, reason: str) -> None:
        """Text extraction already succeeded (status='ready'); embedding/chunking
        did not. The document stays readable but has no searchable chunks."""
        short_reason = reason[:500]
        async with self._session_factory() as session:
            async with session.begin():
                await session.execute(
                    text(
                        """
                        UPDATE documents
                        SET
                            status        = 'embedding_failed',
                            error_message = :reason
                        WHERE document_id = :doc_id
                          AND status      = 'ready'
                        """
                    ),
                    {"doc_id": document_id, "reason": short_reason},
                )
        logger.warning(
            f"document_id={document_id} → embedding_failed | reason={short_reason}"
        )

"""
RAG orchestration: user question -> embedding -> pgvector retrieval ->
prompt construction -> NVIDIA NIM completion -> answer.

This is the module a Kafka consumer (or any future caller) invokes once
ai_service.pipeline.detect() has already confirmed `should_respond=True`
and produced a `query` and `channel_id`. Trigger detection, loop prevention,
and message parsing are NOT this module's concern — see pipeline.py.

Wiring, per [[project_semantic_retrieval]]: call ChunkRepository.semantic_search
directly (no standalone search endpoint), reusing the same Embedder and
ChunkRepository instances the document worker already uses.
"""

import logging

from ai_service.config import (
    RETRIEVAL_MAX_PER_DOCUMENT,
    RETRIEVAL_MIN_SCORE,
    RETRIEVAL_TOP_K,
)
from ai_service.rag.nvidia_client import NvidiaChatClient, NvidiaClientError
from ai_service.rag.prompt_builder import RetrievedChunk, build_prompt
from workers.chunk_repository import ChunkRepository
from workers.embedder import Embedder

logger = logging.getLogger("ai_service.rag")

# Returned verbatim when the LLM call itself fails (not when retrieval finds
# nothing — that case still calls the LLM so it can phrase the "insufficient
# information" response naturally per the system prompt). Kept as a distinct,
# static string so callers/tests can distinguish "model said no context" from
# "the AI service is down" without string-matching a variable LLM response.
GENERATION_UNAVAILABLE_MESSAGE = (
    "Sorry, I'm unable to generate a response right now — the AI service is "
    "temporarily unavailable. Please try again shortly."
)


class RagAnswer:
    def __init__(self, text: str, sources_used: list[RetrievedChunk], had_error: bool = False):
        self.text = text
        self.sources_used = sources_used
        self.had_error = had_error


class RagGenerator:
    """
    Holds the long-lived Embedder / ChunkRepository / NvidiaChatClient
    instances so a Kafka consumer can construct one RagGenerator at startup
    and call answer() per incoming request, rather than re-initializing the
    embedding model or HTTP client on every message.
    """

    def __init__(
        self,
        embedder: Embedder,
        chunk_repository: ChunkRepository,
        nvidia_client: NvidiaChatClient | None = None,
    ) -> None:
        self._embedder = embedder
        self._chunk_repository = chunk_repository
        self._nvidia_client = nvidia_client or NvidiaChatClient()

    async def close(self) -> None:
        await self._nvidia_client.close()

    async def answer(self, channel_id: str, query: str) -> RagAnswer:
        """
        Run one full RAG turn for `query`, scoped to `channel_id`.

        Never raises for "no results found" — that's a normal outcome
        surfaced through the LLM's own "insufficient information" phrasing.
        Only raises-turned-degraded-response on actual infra failure (NVIDIA
        API down, malformed response) — the caller gets a RagAnswer with
        had_error=True rather than an exception, so a single failed request
        can't crash the consumer loop.
        """
        query_embedding = await self._embedder.embed_query(query)

        rows = await self._chunk_repository.semantic_search(
            channel_id=channel_id,
            query_embedding=query_embedding,
            limit=RETRIEVAL_TOP_K,
            min_score=RETRIEVAL_MIN_SCORE,
            max_per_document=RETRIEVAL_MAX_PER_DOCUMENT,
        )

        chunks = [
            RetrievedChunk(
                content=row["content"],
                score=row["score"],
                document_id=row["document_id"],
                chunk_index=row["chunk_index"],
            )
            for row in rows
        ]

        logger.info(
            f"channel_id={channel_id} retrieved {len(chunks)} chunks "
            f"for query={query[:80]!r}"
        )

        prompt = build_prompt(query, chunks)
        if prompt.context_truncated:
            logger.warning(
                f"channel_id={channel_id} context truncated to fit budget — "
                f"{len(prompt.sources_used)}/{len(chunks)} retrieved chunks used"
            )

        try:
            answer_text = await self._nvidia_client.chat_completion(
                system_prompt=prompt.system_prompt,
                user_prompt=prompt.user_prompt,
            )
        except NvidiaClientError as e:
            logger.error(f"channel_id={channel_id} generation failed: {e}")
            return RagAnswer(
                text=GENERATION_UNAVAILABLE_MESSAGE,
                sources_used=prompt.sources_used,
                had_error=True,
            )

        return RagAnswer(text=answer_text, sources_used=prompt.sources_used)

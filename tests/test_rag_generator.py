"""
Tests for ai_service/rag/generator.py — RAG orchestration
(embed -> retrieve -> prompt -> generate), with Embedder, ChunkRepository,
and NvidiaChatClient all faked so no real model/DB/API is touched.
"""

import pytest
from sqlalchemy.exc import SQLAlchemyError

from ai_service.rag.generator import (
    GENERATION_UNAVAILABLE_MESSAGE,
    RETRIEVAL_UNAVAILABLE_MESSAGE,
    RagGenerator,
)
from ai_service.rag.nvidia_client import NvidiaClientError


class FakeEmbedder:
    async def embed_query(self, query: str) -> list[float]:
        return [0.1, 0.2, 0.3]


class FakeChunkRepository:
    def __init__(self, rows: list[dict] | None = None, error: Exception | None = None):
        self._rows = rows if rows is not None else []
        self._error = error
        self.last_call_kwargs: dict | None = None

    async def semantic_search(self, **kwargs) -> list[dict]:
        self.last_call_kwargs = kwargs
        if self._error:
            raise self._error
        return self._rows


class FakeNvidiaClient:
    def __init__(self, response: str | None = None, error: Exception | None = None):
        self._response = response
        self._error = error
        self.last_call_kwargs: dict | None = None

    async def chat_completion(self, **kwargs) -> str:
        self.last_call_kwargs = kwargs
        if self._error:
            raise self._error
        return self._response

    async def close(self) -> None:
        pass


def make_row(content="Chapter 3 covers HNSW indexes.", score=0.75, document_id="doc-1", chunk_index=0):
    return {
        "content": content,
        "score": score,
        "document_id": document_id,
        "chunk_index": chunk_index,
    }


@pytest.mark.asyncio
async def test_answer_scopes_retrieval_to_channel_id():
    repo = FakeChunkRepository([make_row()])
    nvidia = FakeNvidiaClient(response="Chapter 3 explains HNSW indexing [Source 1].")
    generator = RagGenerator(FakeEmbedder(), repo, nvidia)

    result = await generator.answer("chan-42", "Explain chapter 3")

    assert repo.last_call_kwargs["channel_id"] == "chan-42"
    assert result.text == "Chapter 3 explains HNSW indexing [Source 1]."
    assert result.had_error is False
    assert len(result.sources_used) == 1


@pytest.mark.asyncio
async def test_answer_passes_retrieved_chunks_into_prompt():
    repo = FakeChunkRepository([make_row(content="unique-marker-xyz")])
    nvidia = FakeNvidiaClient(response="ok")
    generator = RagGenerator(FakeEmbedder(), repo, nvidia)

    await generator.answer("chan-1", "some question")

    assert "unique-marker-xyz" in nvidia.last_call_kwargs["user_prompt"]


@pytest.mark.asyncio
async def test_answer_with_no_retrieved_chunks_still_calls_llm_for_insufficient_info_phrasing():
    repo = FakeChunkRepository([])  # no chunks above min_score
    nvidia = FakeNvidiaClient(response="I don't have enough information in this channel's documents.")
    generator = RagGenerator(FakeEmbedder(), repo, nvidia)

    result = await generator.answer("chan-1", "unrelated question")

    assert result.sources_used == []
    assert "don't have enough information" in result.text
    assert result.had_error is False


@pytest.mark.asyncio
async def test_answer_degrades_gracefully_when_nvidia_call_fails():
    repo = FakeChunkRepository([make_row()])
    nvidia = FakeNvidiaClient(error=NvidiaClientError("API down"))
    generator = RagGenerator(FakeEmbedder(), repo, nvidia)

    result = await generator.answer("chan-1", "some question")

    assert result.had_error is True
    assert result.text == GENERATION_UNAVAILABLE_MESSAGE
    # Retrieval still succeeded — sources_used reflects what was found even
    # though generation failed, useful for logging/debugging the failure.
    assert len(result.sources_used) == 1


@pytest.mark.asyncio
async def test_answer_degrades_gracefully_when_pgvector_retrieval_fails():
    repo = FakeChunkRepository(error=SQLAlchemyError("connection refused"))
    nvidia = FakeNvidiaClient(response="should not be called")
    generator = RagGenerator(FakeEmbedder(), repo, nvidia)

    result = await generator.answer("chan-1", "some question")

    assert result.had_error is True
    assert result.text == RETRIEVAL_UNAVAILABLE_MESSAGE
    assert result.sources_used == []
    # NIM is never reached — no point spending a generation call on a
    # request that already can't include retrieved context.
    assert nvidia.last_call_kwargs is None

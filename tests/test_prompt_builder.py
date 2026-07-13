"""Tests for ai_service/rag/prompt_builder.py — RAG prompt assembly."""

import pytest

from ai_service.rag import prompt_builder
from ai_service.rag.prompt_builder import RetrievedChunk, build_prompt


def make_chunk(content="some content", score=0.8, document_id="doc-1", chunk_index=0):
    return RetrievedChunk(
        content=content, score=score, document_id=document_id, chunk_index=chunk_index
    )


def test_build_prompt_includes_question_and_context():
    chunks = [make_chunk(content="Chapter 3 discusses vector indexes.")]
    result = build_prompt("Explain chapter 3", chunks)

    assert "Explain chapter 3" in result.user_prompt
    assert "Chapter 3 discusses vector indexes." in result.user_prompt
    assert "[Source 1]" in result.user_prompt


def test_build_prompt_labels_sources_in_rank_order():
    chunks = [make_chunk(content="first", score=0.9), make_chunk(content="second", score=0.7)]
    result = build_prompt("q", chunks)

    first_pos = result.user_prompt.index("[Source 1]")
    second_pos = result.user_prompt.index("[Source 2]")
    assert first_pos < second_pos
    assert result.user_prompt.index("first") < result.user_prompt.index("second")


def test_build_prompt_empty_context_says_so_explicitly():
    result = build_prompt("Explain chapter 3", [])

    assert result.sources_used == []
    assert result.context_truncated is False
    assert "No relevant context was found" in result.user_prompt


def test_system_prompt_instructs_against_hallucination():
    result = build_prompt("q", [make_chunk()])

    assert "ONLY the context" in result.system_prompt
    assert "enough information" in result.system_prompt


def test_build_prompt_respects_context_char_budget(monkeypatch):
    monkeypatch.setattr(prompt_builder, "RETRIEVAL_CONTEXT_CHAR_BUDGET", 50)

    chunks = [
        make_chunk(content="a" * 30, score=0.9),
        make_chunk(content="b" * 30, score=0.8),
        make_chunk(content="c" * 30, score=0.7),
    ]
    result = build_prompt("q", chunks)

    # First chunk (30 chars) fits; second would push past 50, so it's dropped.
    assert len(result.sources_used) == 1
    assert result.sources_used[0].content == "a" * 30
    assert result.context_truncated is True


def test_build_prompt_always_includes_at_least_one_chunk_even_if_oversized(monkeypatch):
    # A single chunk larger than the budget must still be included —
    # dropping everything would be worse than one chunk over budget.
    monkeypatch.setattr(prompt_builder, "RETRIEVAL_CONTEXT_CHAR_BUDGET", 10)

    chunks = [make_chunk(content="x" * 100, score=0.9)]
    result = build_prompt("q", chunks)

    assert len(result.sources_used) == 1
    assert result.context_truncated is False


def test_build_prompt_no_truncation_when_all_chunks_fit():
    chunks = [make_chunk(content="short one"), make_chunk(content="short two")]
    result = build_prompt("q", chunks)

    assert len(result.sources_used) == 2
    assert result.context_truncated is False

"""
Tests for workers/chunker.py

Each test targets a specific requirement or edge case from the spec:
- chunk size: 500–1000 tokens (≈ 375–750 words; default 600)
- overlap: 10–20% (default 15% = 90 words)
- preserve paragraph boundaries
- never split mid-sentence
- handle giant paragraphs, tables, malformed text
- chunk metadata: chunk_index, word_count, source_paragraph
"""

import pytest

from workers.chunker import (
    DEFAULT_CHUNK_WORDS,
    DEFAULT_OVERLAP_WORDS,
    TextChunk,
    split_text,
)


# ── Helpers ───────────────────────────────────────────────────────────────────


def make_paragraph(n_words: int, prefix: str = "word") -> str:
    """Create a paragraph of `n_words` distinct words."""
    return " ".join(f"{prefix}{i}" for i in range(n_words))


def make_sentences(n: int, words_per_sentence: int = 15) -> str:
    """Create `n` sentences, each `words_per_sentence` words, joined by spaces."""
    sentences = []
    for i in range(n):
        body = " ".join(f"word{i}_{j}" for j in range(words_per_sentence - 1))
        sentences.append(f"{body} end{i}.")
    return " ".join(sentences)


# ── Basic contract ────────────────────────────────────────────────────────────


def test_empty_string_returns_empty_list():
    assert split_text("") == []


def test_whitespace_only_returns_empty_list():
    assert split_text("   \n\n   ") == []


def test_single_short_paragraph_produces_one_chunk():
    text = make_paragraph(50)
    chunks = split_text(text)
    assert len(chunks) == 1
    assert chunks[0].chunk_index == 0


def test_chunk_index_is_sequential():
    # Enough text to force multiple chunks.
    text = "\n\n".join(make_paragraph(100) for _ in range(10))
    chunks = split_text(text)
    assert [c.chunk_index for c in chunks] == list(range(len(chunks)))


def test_word_count_matches_content():
    text = "\n\n".join(make_paragraph(200) for _ in range(4))
    chunks = split_text(text)
    for chunk in chunks:
        assert chunk.word_count == len(chunk.content.split())


# ── Chunk size bounds ─────────────────────────────────────────────────────────


def test_default_chunk_size_is_within_token_range():
    """600 words ≈ 800 tokens — within the 500–1000 token spec."""
    assert 375 <= DEFAULT_CHUNK_WORDS <= 750


def test_no_chunk_grossly_exceeds_target():
    """
    No chunk should exceed 1.5× the target (accounting for the overlap seed
    that seeds the next buffer plus one more paragraph before a flush).
    The hard upper bound is chunk_words + overlap_words + one extra paragraph.
    """
    text = "\n\n".join(make_paragraph(100) for _ in range(20))
    chunks = split_text(text)
    hard_limit = DEFAULT_CHUNK_WORDS + DEFAULT_OVERLAP_WORDS + 110  # +10 buffer
    for chunk in chunks:
        assert chunk.word_count <= hard_limit, (
            f"Chunk {chunk.chunk_index} has {chunk.word_count} words, "
            f"exceeds hard limit of {hard_limit}"
        )


# ── Overlap ───────────────────────────────────────────────────────────────────


def test_overlap_percentage_is_within_spec():
    """Default overlap should be 10–20% of chunk size."""
    ratio = DEFAULT_OVERLAP_WORDS / DEFAULT_CHUNK_WORDS
    assert 0.10 <= ratio <= 0.20


def test_overlap_words_appear_in_consecutive_chunks():
    """
    The last overlap_words of chunk N should appear at the start of chunk N+1.
    We verify that at least some words are shared between adjacent chunks.
    """
    text = "\n\n".join(make_paragraph(200, prefix=f"p{i}") for i in range(6))
    chunks = split_text(text)

    assert len(chunks) >= 2, "Need at least 2 chunks for overlap test"

    for i in range(len(chunks) - 1):
        words_a = chunks[i].content.split()
        words_b = chunks[i + 1].content.split()
        tail_a = set(words_a[-DEFAULT_OVERLAP_WORDS:])
        head_b = set(words_b[:DEFAULT_OVERLAP_WORDS])
        shared = tail_a & head_b
        assert shared, (
            f"No overlap between chunk {i} and chunk {i+1}. "
            f"tail_a={list(tail_a)[:5]} head_b={list(head_b)[:5]}"
        )


# ── Paragraph boundary preservation ──────────────────────────────────────────


def test_paragraph_boundary_used_as_split_point():
    """
    When paragraphs are small enough, a chunk boundary should align with a
    paragraph boundary — i.e. no paragraph is split across two chunks
    unless the paragraph itself is giant.
    """
    para = make_paragraph(100)
    # 7 paragraphs × 100 words = 700 words > DEFAULT_CHUNK_WORDS (600)
    # The first chunk should end on a paragraph boundary (after ~600 words).
    paragraphs = [para] * 7
    text = "\n\n".join(paragraphs)
    chunks = split_text(text)

    # Every chunk should be a clean join of whole paragraphs (no mid-para cut).
    # We verify: each chunk content is divisible by 100 words (the para size).
    for chunk in chunks:
        wc = chunk.word_count
        # Allow for the overlap window (up to 90 words from the previous chunk).
        wc_without_overlap = wc - DEFAULT_OVERLAP_WORDS
        assert wc_without_overlap % 100 == 0 or wc % 100 == 0, (
            f"Chunk {chunk.chunk_index} word_count={wc} is not a multiple of "
            f"the paragraph size (100), suggesting a mid-paragraph split."
        )


# ── Giant paragraph handling ──────────────────────────────────────────────────


def test_giant_paragraph_is_split():
    """A paragraph larger than 1.5× chunk_words must produce multiple chunks."""
    giant = make_sentences(60, words_per_sentence=15)  # ~900 words
    chunks = split_text(giant)
    assert len(chunks) >= 2


def test_giant_paragraph_all_content_preserved():
    """No words should be silently dropped when splitting a giant paragraph."""
    giant = make_sentences(50, words_per_sentence=14)
    chunks = split_text(giant)

    # Collect all non-overlap words. Because of overlap we may count some words
    # twice — instead we verify the full original text is a substring of the
    # concatenated chunk contents (order-insensitive word set check).
    original_words = set(giant.split())
    recovered_words = set(w for c in chunks for w in c.content.split())
    assert original_words <= recovered_words


# ── Table handling ────────────────────────────────────────────────────────────


def test_table_block_is_atomic_chunk():
    """A markdown table should be emitted as a single chunk, never split."""
    table = (
        "| Name | Age | Role |\n"
        "|------|-----|------|\n"
        "| Alice | 30 | Engineer |\n"
        "| Bob | 25 | Designer |\n"
        "| Carol | 28 | Manager |\n"
    )
    prose = make_paragraph(400)
    text = prose + "\n\n" + table
    chunks = split_text(text)

    # Find the chunk containing the table header.
    table_chunks = [c for c in chunks if "| Name |" in c.content]
    assert len(table_chunks) == 1, "Table must be contained in exactly one chunk"


def test_table_chunk_contains_all_rows():
    table = (
        "| Product | Price |\n"
        "|---------|-------|\n"
        "| Widget | $10 |\n"
        "| Gadget | $20 |\n"
    )
    chunks = split_text(table)
    assert len(chunks) == 1
    assert "Widget" in chunks[0].content
    assert "Gadget" in chunks[0].content


# ── Malformed text handling ───────────────────────────────────────────────────


def test_garbage_lines_are_dropped():
    """Lines with > 30% garbage characters should be stripped."""
    garbage = "word1 word2\n\x00\x01\x02\x03\x04\x05\x06\x07\x08\nword3 word4"
    chunks = split_text(garbage)
    assert chunks  # must not crash
    full_content = " ".join(c.content for c in chunks)
    # Null bytes and control chars must not appear in output.
    assert "\x00" not in full_content
    assert "\x01" not in full_content


def test_replacement_characters_are_handled():
    """U+FFFD replacement chars should not crash the chunker."""
    text = "Good text here.\n\n" + "word " * 10 + "bad��� text\n\n" + "More good text. " * 20
    chunks = split_text(text)
    assert chunks  # must not crash


def test_single_very_long_word_does_not_crash():
    """A word longer than any reasonable chunk size must not cause an infinite loop."""
    long_word = "a" * 10_000
    text = f"Normal text before. {long_word} Normal text after."
    chunks = split_text(text)
    assert chunks


# ── Metadata correctness ──────────────────────────────────────────────────────


def test_source_paragraph_is_non_negative():
    text = "\n\n".join(make_paragraph(150) for _ in range(6))
    chunks = split_text(text)
    for chunk in chunks:
        assert chunk.source_paragraph >= 0


def test_source_paragraph_is_non_decreasing():
    """source_paragraph should never decrease — chunks are ordered by document position."""
    text = "\n\n".join(make_paragraph(150) for _ in range(8))
    chunks = split_text(text)
    for a, b in zip(chunks, chunks[1:]):
        assert b.source_paragraph >= a.source_paragraph


def test_chunk_content_is_non_empty():
    text = "\n\n".join(make_paragraph(200) for _ in range(5))
    chunks = split_text(text)
    for chunk in chunks:
        assert chunk.content.strip()


# ── Custom parameters ─────────────────────────────────────────────────────────


def test_custom_chunk_size_is_respected():
    """With a small chunk_words, we should get more chunks."""
    text = make_paragraph(500)
    chunks_small = split_text(text, chunk_words=100, overlap_words=10)
    chunks_default = split_text(text)
    assert len(chunks_small) > len(chunks_default)


def test_zero_overlap_produces_no_shared_words():
    """With overlap=0, consecutive chunks should share no words at their boundary."""
    # Use a clean set of uniquely-named words to make this deterministic.
    text = "\n\n".join(make_paragraph(200, prefix=f"para{i}") for i in range(4))
    chunks = split_text(text, chunk_words=200, overlap_words=0)

    assert len(chunks) >= 2
    for i in range(len(chunks) - 1):
        words_a = set(chunks[i].content.split())
        words_b = set(chunks[i + 1].content.split())
        shared = words_a & words_b
        assert not shared, (
            f"With overlap=0, chunks {i} and {i+1} share words: {list(shared)[:5]}"
        )

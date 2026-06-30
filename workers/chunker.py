"""
Paragraph-aware, sentence-respecting text chunker for RAG embedding.

Design goals
------------
1. Never split mid-sentence — a sentence fragment is semantically broken and
   produces a poor embedding. The cosine similarity against a coherent query
   will be lower than it should be.

2. Prefer paragraph boundaries as split points — paragraphs are the natural
   unit of meaning in prose. Splitting inside a paragraph breaks the flow of
   an argument or explanation.

3. Overlap between chunks — if a key sentence lands at a boundary, it appears
   in both the trailing chunk and the leading chunk. Either chunk can be
   retrieved; neither misses the sentence entirely.

4. Handle edge cases without crashing — giant paragraphs, tables, malformed
   text, and near-empty inputs must all produce valid (possibly non-ideal)
   output rather than raising exceptions.

Chunk size decision (800 tokens / ~600 words)
---------------------------------------------
- text-embedding-3-small: 8191 token input limit. 800 tokens per chunk
  keeps us well inside the limit even after tokenisation overhead.
- GPT-4o context: 128k tokens. Top-5 retrieval at 800 tokens = 4000 tokens
  of context, leaving ample room for conversation history + system prompt.
- Too small (< 200 tokens): vectors have too little signal. Cosine distances
  cluster near 0.5 — everything looks equidistant.
- Too large (> 1500 tokens): semantic dilution. The embedding must represent
  multiple ideas; precision drops.

Tokenisation approximation
---------------------------
We approximate 1 token ≈ 0.75 words (1.33 words/token) for English prose,
which matches OpenAI's published ratio. Character counting is not used —
CJK characters count as one word but many tokens.

Overlap (15% = ~120 words)
---------------------------
120-word overlap means a sentence that straddles a boundary appears in full
in BOTH the preceding and following chunk. 10–20% is the documented range;
15% is the midpoint default.
"""

import re
from dataclasses import dataclass

# ── Constants ─────────────────────────────────────────────────────────────────

# Target chunk size in words. 600 words ≈ 800 tokens for English prose.
DEFAULT_CHUNK_WORDS: int = 600

# Overlap in words. 90 words ≈ 120 tokens ≈ 15% of DEFAULT_CHUNK_WORDS.
DEFAULT_OVERLAP_WORDS: int = 90

# A paragraph that exceeds this word count is "giant" — it cannot fit in a
# single chunk and must be force-split at sentence boundaries.
# Set to 1.5× the chunk size so one oversized paragraph gets two chunks.
_GIANT_PARA_WORDS: int = int(DEFAULT_CHUNK_WORDS * 1.5)

# Sentence boundary: end of sentence punctuation followed by whitespace and an
# uppercase letter (or end of string). Handles "Mr." by requiring >= 2 chars
# before the period via a minimal positive lookbehind.
# This regex intentionally does NOT split on "e.g.", "i.e.", "U.S.A.", etc.
# because those are followed by a lowercase letter.
_SENTENCE_END = re.compile(
    r"(?<=[a-z0-9\"\'\)])"   # char before punctuation (avoids "Mr.")
    r"[.!?]+"                # one or more sentence-final punctuation
    r"(?=\s+[A-Z\"\'])",     # followed by whitespace + sentence-start char
)

# Table detection: a line containing two or more tab-separated or pipe-
# separated cells, or a markdown-style table separator (|---|).
_TABLE_LINE = re.compile(
    r"(\|.*\|"               # markdown table row: | col | col |
    r"|\t[^\t]+\t"           # tab-separated row
    r"|^\s*[-|+]{3,})"       # table separator: --- or |---| or +++
    , re.MULTILINE,
)

# Malformed text: lines that are mostly non-printable or garbage characters.
# Threshold: if > 30% of chars in a line are replacement/control chars, drop.
_GARBAGE_LINE = re.compile(r"[\x00-\x08\x0b\x0e-\x1f�]")


# ── Data types ────────────────────────────────────────────────────────────────


@dataclass
class TextChunk:
    chunk_index: int
    content: str
    word_count: int           # for observability / unit tests
    source_paragraph: int     # 0-based index of the paragraph this chunk starts in


# ── Public API ────────────────────────────────────────────────────────────────


def split_text(
    text: str,
    chunk_words: int = DEFAULT_CHUNK_WORDS,
    overlap_words: int = DEFAULT_OVERLAP_WORDS,
) -> list[TextChunk]:
    """
    Split `text` into overlapping, paragraph-aware chunks.

    The algorithm:
      1. Split on paragraph boundaries (double newlines).
      2. Filter malformed/garbage lines.
      3. Detect table blocks and pass them through as atomic chunks.
      4. Accumulate paragraphs into a buffer until the buffer hits chunk_words.
      5. When the buffer is full, emit a chunk, then seed the next buffer
         with the last overlap_words words (overlap window).
      6. Giant paragraphs that exceed chunk_words alone are split at sentence
         boundaries; if no sentence boundary is found, force-split at words.

    Args:
        text:          Cleaned document text from PDFExtractor.
        chunk_words:   Target words per chunk (default 600 ≈ 800 tokens).
        overlap_words: Words shared between adjacent chunks (default 90 ≈ 15%).

    Returns:
        List of TextChunk ordered by chunk_index (0-based).
        Empty list if text has no usable content.
    """
    if not text or not text.strip():
        return []

    paragraphs = _split_into_paragraphs(text)
    if not paragraphs:
        return []

    chunks: list[TextChunk] = []
    buffer_words: list[str] = []       # accumulator for the current chunk
    buffer_para_start: int = 0         # paragraph index where this buffer started
    chunk_index: int = 0

    for para_idx, para in enumerate(paragraphs):
        para_words = para.split()

        if not para_words:
            continue

        # ── Tables are passed through as atomic chunks ─────────────────────
        if _is_table(para):
            # Flush current buffer first so the table is its own chunk.
            if buffer_words:
                chunk_index = _emit(
                    chunks, buffer_words, chunk_index, buffer_para_start
                )
                buffer_words = []
                buffer_para_start = para_idx

            # Emit the table block as a single chunk (even if it is large —
            # splitting a table mid-row destroys its structure entirely).
            chunk_index = _emit(chunks, para_words, chunk_index, para_idx)
            buffer_para_start = para_idx + 1
            continue

        # ── Giant paragraph: must be split at sentence boundaries ──────────
        # A paragraph that alone exceeds chunk_words cannot fit in a single
        # chunk — split it regardless of the fixed _GIANT_PARA_WORDS constant.
        if len(para_words) > chunk_words:
            # Flush buffer before handling the giant paragraph.
            if buffer_words:
                chunk_index = _emit(
                    chunks, buffer_words, chunk_index, buffer_para_start
                )
                buffer_words = []
                buffer_para_start = para_idx

            sentences = _split_sentences(para, chunk_words)
            for sentence in sentences:
                s_words = sentence.split()
                if not s_words:
                    continue

                # Flush the buffer whenever adding this sentence would overflow,
                # regardless of whether the buffer is currently empty. An empty
                # buffer + an oversized sentence still needs the while-drain below.
                if len(buffer_words) + len(s_words) > chunk_words and buffer_words:
                    chunk_index = _emit(
                        chunks, buffer_words, chunk_index, buffer_para_start
                    )
                    buffer_words = buffer_words[-overlap_words:] if overlap_words else []
                    buffer_para_start = para_idx

                buffer_words.extend(s_words)

                # A single sentence longer than chunk_words fills buffer beyond
                # the limit. Drain it immediately so the next sentence starts clean.
                while len(buffer_words) > chunk_words:
                    chunk_index = _emit(
                        chunks, buffer_words[:chunk_words], chunk_index, buffer_para_start
                    )
                    buffer_words = (
                        buffer_words[chunk_words - overlap_words:]
                        if overlap_words
                        else buffer_words[chunk_words:]
                    )

            continue

        # ── Normal paragraph ───────────────────────────────────────────────
        # If adding this paragraph would exceed the target, emit first.
        if buffer_words and len(buffer_words) + len(para_words) > chunk_words:
            chunk_index = _emit(
                chunks, buffer_words, chunk_index, buffer_para_start
            )
            # Overlap: seed the new buffer from the tail of the old one.
            # We use words (not paragraphs) so the overlap window is exact.
            buffer_words = buffer_words[-overlap_words:] if overlap_words else []
            buffer_para_start = para_idx

        buffer_words.extend(para_words)

    # ── Flush the final buffer ─────────────────────────────────────────────
    if buffer_words:
        _emit(chunks, buffer_words, chunk_index, buffer_para_start)

    return chunks


# ── Private helpers ───────────────────────────────────────────────────────────


def _split_into_paragraphs(text: str) -> list[str]:
    """
    Split on double newlines (paragraph boundaries preserved by PDFExtractor).
    Strip each paragraph; drop empty ones and garbage lines.
    """
    raw_paras = re.split(r"\n\n+", text)
    result = []
    for para in raw_paras:
        clean = _clean_paragraph(para.strip())
        if clean:
            result.append(clean)
    return result


def _clean_paragraph(para: str) -> str:
    """
    Remove garbage lines from within a paragraph.
    A line is garbage if > 30% of its characters are control/replacement chars.
    """
    lines = para.splitlines()
    clean_lines = []
    for line in lines:
        if not line.strip():
            continue
        garbage_chars = len(_GARBAGE_LINE.findall(line))
        if len(line) > 0 and garbage_chars / len(line) > 0.30:
            continue  # drop garbage line
        clean_lines.append(line)
    return "\n".join(clean_lines).strip()


def _is_table(para: str) -> bool:
    """
    Return True if the paragraph looks like a table.
    Heuristic: >= 2 lines match the table pattern, or > 40% of lines do.
    """
    lines = para.splitlines()
    if len(lines) < 2:
        return False
    table_lines = sum(1 for line in lines if _TABLE_LINE.search(line))
    return table_lines >= 2 or (len(lines) > 0 and table_lines / len(lines) > 0.4)


def _split_sentences(para: str, chunk_words: int = DEFAULT_CHUNK_WORDS) -> list[str]:
    """
    Split a giant paragraph at sentence boundaries.
    Falls back to word-based splitting if no sentence boundaries are found.
    Each returned sentence is stripped.
    """
    # Use regex to find split positions; re.split with a capturing group
    # preserves the punctuation in the preceding sentence.
    parts = _SENTENCE_END.split(para)

    # _SENTENCE_END splits ON the char after punctuation, which splits the
    # punctuation off. Reassemble: each odd element is the punctuation char.
    sentences = []
    i = 0
    while i < len(parts):
        if i + 1 < len(parts):
            # parts[i] ends just before the period; parts[i+1] is the space+cap.
            # We want to keep punctuation with the sentence that ends it.
            sentences.append(parts[i].strip())
            i += 1
        else:
            sentences.append(parts[i].strip())
            i += 1

    sentences = [s for s in sentences if s]

    # Fallback: no sentence boundaries found (e.g. all lowercase continuations).
    # Split by words at chunk_words so the caller's while-loop can flush them.
    if len(sentences) <= 1:
        words = para.split()
        sentences = [
            " ".join(words[i: i + chunk_words])
            for i in range(0, len(words), chunk_words)
        ]

    return sentences


def _emit(
    chunks: list[TextChunk],
    words: list[str],
    chunk_index: int,
    source_paragraph: int,
) -> int:
    """
    Append a TextChunk from `words` to `chunks`. Returns the next chunk_index.
    Words are joined with spaces; consecutive whitespace is collapsed.
    """
    content = " ".join(words)
    # Collapse any internal whitespace artefacts left by the join.
    content = re.sub(r"[ \t]{2,}", " ", content).strip()

    if content:
        chunks.append(
            TextChunk(
                chunk_index=chunk_index,
                content=content,
                word_count=len(words),
                source_paragraph=source_paragraph,
            )
        )
        return chunk_index + 1

    return chunk_index

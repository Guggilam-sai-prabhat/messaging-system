"""
Splits extracted document text into overlapping chunks for embedding.

Why overlap?
------------
If a sentence spans a chunk boundary, both the preceding and following
chunks contain it. This prevents splitting a key sentence in half and
losing its meaning in search results.

Chunk size decision (500 tokens ≈ 400 words)
---------------------------------------------
- Too small (50 tokens): each chunk lacks context. The LLM gets fragments.
- Too large (2000 tokens): fewer chunks, less precise retrieval.
  The LLM is handed an entire page when only one paragraph was relevant.
- 400-500 tokens with 10% overlap is the standard production default.
"""

from dataclasses import dataclass


@dataclass
class TextChunk:
    chunk_index: int
    content: str


def split_text(
    text: str,
    chunk_size: int = 400,
    overlap: int = 40,
) -> list[TextChunk]:
    """
    Split `text` into overlapping word-based chunks.

    Word-based (not character-based) splitting is used because embedding
    model token limits are roughly proportional to word count, not character
    count. A 400-word chunk is approximately 500 tokens for English text.

    Args:
        text:       The full extracted document text.
        chunk_size: Target number of words per chunk.
        overlap:    Number of words shared between consecutive chunks.

    Returns:
        List of TextChunk ordered by chunk_index (0-based).
    """
    words = text.split()
    if not words:
        return []

    chunks: list[TextChunk] = []
    step = chunk_size - overlap
    start = 0
    index = 0

    while start < len(words):
        end = min(start + chunk_size, len(words))
        content = " ".join(words[start:end])
        chunks.append(TextChunk(chunk_index=index, content=content))
        if end == len(words):
            break
        start += step
        index += 1

    return chunks

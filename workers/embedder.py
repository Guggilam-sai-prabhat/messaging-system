"""
Thin async wrapper around the OpenAI embeddings API.

Why a separate module
----------------------
Embedding generation is an I/O-bound network call to an external API.
Keeping it isolated from the repository means you can swap models (e.g.
a locally-hosted model via a compatible API) without touching SQL code.
"""

import logging
import os

from openai import AsyncOpenAI

logger = logging.getLogger("document.worker")

# text-embedding-3-small: 1536 dimensions, ~$0.02 / 1M tokens.
# text-embedding-3-large: 3072 dimensions, ~$0.13 / 1M tokens.
# For a messaging RAG system, small is the right default.
EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "text-embedding-3-small")
EMBEDDING_DIM = 1536


class Embedder:
    def __init__(self) -> None:
        self._client = AsyncOpenAI()  # reads OPENAI_API_KEY from env

    async def embed_texts(self, texts: list[str]) -> list[list[float]]:
        """
        Embed a batch of texts. Returns one vector per input, in order.

        OpenAI's batching endpoint accepts up to 2048 inputs per request.
        For typical document chunk counts (50–200 chunks per document) a
        single call is sufficient. If you process very large documents
        (thousands of chunks), split into batches of 512 and call in sequence.
        """
        if not texts:
            return []

        response = await self._client.embeddings.create(
            input=texts,
            model=EMBEDDING_MODEL,
        )

        # The API returns embeddings in the same order as the input.
        embeddings = [item.embedding for item in sorted(response.data, key=lambda x: x.index)]

        logger.debug(
            f"Embedded {len(texts)} texts → {len(embeddings[0])}d vectors "
            f"using {EMBEDDING_MODEL}"
        )
        return embeddings

    async def embed_query(self, query: str) -> list[float]:
        """Embed a single query string. Returns one vector."""
        results = await self.embed_texts([query])
        return results[0]

"""
Local embedding worker using sentence-transformers.

Why sentence-transformers instead of OpenAI
--------------------------------------------
- Free: no API key, no per-token cost, no rate limits.
- Offline: model is downloaded once (~400 MB) and runs fully locally.
- BAAI/bge-base-en-v1.5 (768d) benchmarks near text-embedding-3-small (1536d)
  on MTEB retrieval tasks while using half the vector storage.

Batching strategy
-----------------
sentence-transformers.encode() already batches internally, but we split at
BATCH_SIZE to cap peak RAM usage when a document has hundreds of chunks.
Each batch is encoded synchronously in a thread-pool executor so we never
block the asyncio event loop during the CPU/GPU-bound encode call.

Retry strategy
--------------
sentence-transformers runs locally so the only realistic transient failure is
an OOM spike or thread-pool timeout. We retry up to MAX_RETRIES times with
exponential back-off before propagating the exception. The document_worker
marks the document failed and commits the Kafka offset, so processing resumes
on the next restart without loss.
"""

import asyncio
import logging
import os
import time

logger = logging.getLogger("document.worker")

# ── Configuration ──────────────────────────────────────────────────────────────

MODEL_NAME = os.getenv("EMBEDDING_MODEL", "BAAI/bge-base-en-v1.5")
EMBEDDING_DIM = 768          # must match document_chunk.py and the DB schema
BATCH_SIZE = 64              # chunks per encode() call — tune for your RAM budget
MAX_RETRIES = 3
RETRY_BASE_DELAY_S = 1.0     # seconds; doubles on each retry (exponential back-off)

# BGE models use this instruction prefix for queries (not for document chunks).
BGE_QUERY_PREFIX = "Represent this sentence for searching relevant passages: "


class Embedder:
    """
    Async wrapper around a sentence-transformers model.

    The model is loaded lazily on first use so the worker process starts
    immediately; the ~1s model-load cost is paid on the first document.
    """

    def __init__(self) -> None:
        self._model = None
        self._lock = asyncio.Lock()

    # ── Public API ─────────────────────────────────────────────────────────────

    async def embed_texts(self, texts: list[str]) -> list[list[float]]:
        """
        Embed a list of document chunk texts.
        Returns one float vector per input, in the same order.

        Splits into batches of BATCH_SIZE and runs encode() in a thread-pool
        executor to avoid blocking the asyncio event loop.
        """
        if not texts:
            return []

        model = await self._get_model()
        loop = asyncio.get_running_loop()

        all_embeddings: list[list[float]] = []

        for batch_start in range(0, len(texts), BATCH_SIZE):
            batch = texts[batch_start : batch_start + BATCH_SIZE]
            embeddings = await self._encode_with_retry(model, batch, loop)
            all_embeddings.extend(embeddings)

        logger.debug(
            f"Embedded {len(texts)} texts → {EMBEDDING_DIM}d vectors "
            f"using {MODEL_NAME}"
        )
        return all_embeddings

    async def embed_query(self, query: str) -> list[float]:
        """
        Embed a single search query.

        BGE models use an instruction prefix for queries to improve retrieval
        quality. Document chunks are embedded without the prefix.
        """
        prefixed = BGE_QUERY_PREFIX + query
        results = await self.embed_texts([prefixed])
        return results[0]

    # ── Internal helpers ───────────────────────────────────────────────────────

    async def _get_model(self):
        """Lazy-load the sentence-transformer model (thread-safe via asyncio.Lock)."""
        if self._model is not None:
            return self._model

        async with self._lock:
            if self._model is not None:  # re-check after acquiring lock
                return self._model

            loop = asyncio.get_running_loop()
            logger.info(f"Loading embedding model: {MODEL_NAME}")
            t0 = time.monotonic()

            from sentence_transformers import SentenceTransformer

            model = await loop.run_in_executor(
                None,
                lambda: SentenceTransformer(MODEL_NAME),
            )

            elapsed_ms = (time.monotonic() - t0) * 1000
            logger.info(
                f"Embedding model loaded in {elapsed_ms:.0f}ms "
                f"(dim={model.get_sentence_embedding_dimension()})"
            )
            self._model = model

        return self._model

    async def _encode_with_retry(
        self,
        model,
        texts: list[str],
        loop: asyncio.AbstractEventLoop,
    ) -> list[list[float]]:
        """
        Encode one batch with exponential back-off retry.

        encode() is CPU-bound; we offload it to a thread-pool executor.
        normalize_embeddings=True produces unit vectors so cosine similarity
        equals dot product — pgvector's <=> operator handles both correctly.
        """
        delay = RETRY_BASE_DELAY_S

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                embeddings = await loop.run_in_executor(
                    None,
                    lambda: model.encode(
                        texts,
                        batch_size=len(texts),
                        normalize_embeddings=True,
                        show_progress_bar=False,
                    ).tolist(),
                )
                return embeddings

            except Exception as e:
                if attempt == MAX_RETRIES:
                    logger.error(
                        f"Embedding failed after {MAX_RETRIES} attempts: {e}"
                    )
                    raise

                logger.warning(
                    f"Embedding attempt {attempt}/{MAX_RETRIES} failed: {e} "
                    f"— retrying in {delay:.1f}s"
                )
                await asyncio.sleep(delay)
                delay *= 2

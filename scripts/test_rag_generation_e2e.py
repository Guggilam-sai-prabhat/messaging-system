"""
Manual end-to-end validation — RAG Generation
==============================================
Drives the full query -> embed -> pgvector retrieve -> prompt -> NVIDIA NIM
generate path against REAL Postgres and the REAL NVIDIA API (nothing mocked).
Companion to scripts/test_retrieval_pipeline.py, which validates retrieval
alone — this script additionally exercises ai_service/rag/generator.py's
NVIDIA chat completion call.

Requires:
  - Postgres reachable at DATABASE_URL with embedded chunks already present
    in the target channel (see CHANNEL_ID below; run the document upload
    flow first if the channel has no chunks).
  - NVIDIA_API_KEY set in the environment (or .env) — get one from
    https://build.nvidia.com. Without it, generation calls fail fast with
    a clear NvidiaClientError rather than hanging.

Usage:
    python -m scripts.test_rag_generation_e2e [channel_id]
"""

import asyncio
import os
import sys

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from ai_service.rag.generator import RagGenerator
from ai_service.rag.nvidia_client import NvidiaChatClient
from workers.chunk_repository import ChunkRepository
from workers.embedder import Embedder

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:new_password@localhost:5432/messaging",
)

# A channel with real embedded chunks, used when no channel_id is passed
# on the command line. Swap for a channel_id in your own environment.
DEFAULT_CHANNEL_ID = "b384da85-160b-4b19-bca3-55630432ce13"

QUERIES = [
    "What is the answer to question 4.22 about the horse-cart problem?",
    "What is the capital of France?",  # expected: no relevant chunks found
]


async def main() -> None:
    channel_id = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_CHANNEL_ID

    db_url = DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://", 1)
    engine = create_async_engine(db_url)
    session_factory = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)

    embedder = Embedder()
    chunk_repo = ChunkRepository(session_factory)
    nvidia_client = NvidiaChatClient()
    generator = RagGenerator(embedder, chunk_repo, nvidia_client)

    try:
        for query in QUERIES:
            print("=" * 80)
            print(f"CHANNEL: {channel_id}")
            print(f"QUERY:   {query}")

            result = await generator.answer(channel_id, query)

            print(f"had_error: {result.had_error}")
            print(f"sources_used: {len(result.sources_used)}")
            for source in result.sources_used:
                print(
                    f"  - document_id={source.document_id[:8]} "
                    f"chunk_index={source.chunk_index} score={source.score:.3f}"
                )
            print(f"ANSWER:\n{result.text}\n")
    finally:
        await generator.close()
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())

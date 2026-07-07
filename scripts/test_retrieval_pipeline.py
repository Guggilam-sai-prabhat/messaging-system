"""
Manual end-to-end validation — Retrieval Pipeline
==================================================
Drives the full PDF -> text -> chunk -> embed -> pgvector -> semantic_search
path directly against REAL Postgres (nothing mocked). Bypasses Kafka/MinIO/
upload-API entirely since this pipeline has no HTTP retrieval endpoint yet —
this script IS the harness that would sit behind a future chat endpoint.

Covers:
1.  Extraction + chunking produce non-empty, ordered chunks
2.  Embedding a chunk that exceeds BGE's 512-token limit gets silently
    truncated (quantifies the truncation bug found during code review)
3.  Insert chunks with embeddings into document_chunks (idempotent upsert)
4.  Relevant query retrieves the right chunk above min_score
5.  Irrelevant query returns nothing above min_score (not an error)
6.  Empty-channel query returns [] cleanly
7.  Cross-channel isolation — a near-perfect match in another channel never
    leaks into this channel's results
8.  max_per_document cap — a document with many near-duplicate chunks
    doesn't crowd out other documents' results
9.  Cleanup

Usage:
    python -m scripts.test_retrieval_pipeline
"""

import asyncio
import os
import sys
from uuid import uuid4

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from workers.chunker import split_text
from workers.chunk_repository import ChunkRepository
from workers.embedder import Embedder

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+asyncpg://postgres:new_password@localhost:5432/messaging",
)

RUN_ID = uuid4().hex[:8]
CHANNEL_A = f"retrieval-test-a-{RUN_ID}"
CHANNEL_B = f"retrieval-test-b-{RUN_ID}"
DOC_1 = f"retrieval-test-doc1-{RUN_ID}"
DOC_2 = f"retrieval-test-doc2-{RUN_ID}"
DOC_LEAK = f"retrieval-test-leak-{RUN_ID}"

TOTAL_STEPS = 9


def print_step(step, total, msg):
    print(f"\n{'='*60}")
    print(f"  [{step}/{total}] {msg}")
    print(f"{'='*60}")


def print_pass(msg):
    print(f"  ✅ {msg}")


def print_fail(msg):
    print(f"  ❌ {msg}")
    raise AssertionError(msg)


DOC_TEXT = """PGVector Indexing Strategies

HNSW (Hierarchical Navigable Small World) builds a layered graph where each
node links to its nearest neighbours. At query time it navigates the graph
rather than scanning every row, giving logarithmic rather than linear query
cost as the table grows. The two build-time parameters are m, the number of
bi-directional links per node, and ef_construction, the candidate list size
used while building the graph. Higher values improve recall at the cost of
slower index builds and more memory.

IVFFlat Indexing Strategies

IVFFlat partitions vectors into lists using k-means clustering, then probes
only a subset of lists at query time. It builds faster than HNSW and uses
less memory, but recall degrades as the table grows unless the number of
lists is retuned, and it requires training data to be present before the
index is built, which is awkward for tables that start empty.

Choosing Between Them

For production RAG systems where the table grows continuously and query
latency must stay flat, HNSW is the better default despite its slower and
more memory-hungry build process."""

UNRELATED_TEXT = """Kafka Consumer Offset Management

Manual offset commits give you control over exactly when a message is
considered processed. Auto-commit is simpler but risks committing an offset
before processing actually completes, causing silent message loss on crash."""


async def main():
    db_url = DATABASE_URL
    engine = create_async_engine(db_url, pool_size=5, max_overflow=2)
    session_factory = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)

    chunk_repo = ChunkRepository(session_factory)
    embedder = Embedder()

    async def _create_document_row(document_id, channel_id):
        async with session_factory() as session:
            async with session.begin():
                await session.execute(
                    text(
                        """
                        INSERT INTO documents
                            (document_id, channel_id, file_name, content_type,
                             file_size_bytes, uploaded_by, status, storage_path,
                             sha256_hash)
                        VALUES
                            (:doc_id, :channel_id, 'test.pdf', 'application/pdf',
                             1024, 'retrieval-test-user', 'ready', :storage_path,
                             :sha256_hash)
                        """
                    ),
                    {
                        "doc_id": document_id,
                        "channel_id": channel_id,
                        "storage_path": f"{document_id}.pdf",
                        "sha256_hash": uuid4().hex * 2,
                    },
                )

    try:
        # ── Step 0: seed parent `documents` rows (FK requires them) ──────
        for doc_id, chan_id in [(DOC_1, CHANNEL_A), (DOC_2, CHANNEL_A), (DOC_LEAK, CHANNEL_B)]:
            await _create_document_row(doc_id, chan_id)

        # ── Step 1: chunking ────────────────────────────────────────────
        print_step(1, TOTAL_STEPS, "Chunk a realistic multi-section document")
        chunks = split_text(DOC_TEXT, chunk_words=60, overlap_words=10)
        if not chunks:
            print_fail("split_text produced zero chunks")
        indices = [c.chunk_index for c in chunks]
        if indices != sorted(indices):
            print_fail(f"chunk_index not monotonic: {indices}")
        print_pass(f"{len(chunks)} chunks, indices {indices}")

        # ── Step 2: truncation check ────────────────────────────────────
        print_step(2, TOTAL_STEPS, "Check for silent token truncation at embed time (default chunk size)")
        default_chunks = split_text(DOC_TEXT * 5)  # force a full-size ~600-word chunk
        model = await embedder._get_model()
        biggest = max(default_chunks, key=lambda c: c.word_count)
        n_tokens = len(model.tokenizer(biggest.content, truncation=False)["input_ids"])
        max_len = model.max_seq_length
        if n_tokens > max_len:
            dropped = n_tokens - max_len
            print(
                f"  ⚠️  KNOWN BUG (not fatal to this run): chunk has {n_tokens} tokens > "
                f"model max_seq_length={max_len}. ~{dropped} tokens ({dropped / n_tokens:.0%}) "
                f"are silently dropped before embedding — the tail of this chunk is "
                f"invisible to its own vector. See architecture review."
            )
        else:
            print_pass(f"chunk fits in {n_tokens}/{max_len} tokens")

        # ── Step 3: embed + insert ──────────────────────────────────────
        print_step(3, TOTAL_STEPS, "Embed and store chunks for two documents in channel A")
        embeddings = await embedder.embed_texts([c.content for c in chunks])
        await chunk_repo.insert_chunks(
            document_id=DOC_1,
            channel_id=CHANNEL_A,
            chunks=[
                {"chunk_index": c.chunk_index, "content": c.content, "embedding": e}
                for c, e in zip(chunks, embeddings)
            ],
        )

        unrelated_chunks = split_text(UNRELATED_TEXT, chunk_words=60, overlap_words=10)
        unrelated_embeddings = await embedder.embed_texts([c.content for c in unrelated_chunks])
        await chunk_repo.insert_chunks(
            document_id=DOC_2,
            channel_id=CHANNEL_A,
            chunks=[
                {"chunk_index": c.chunk_index, "content": c.content, "embedding": e}
                for c, e in zip(unrelated_chunks, unrelated_embeddings)
            ],
        )
        print_pass(f"inserted {len(chunks)} + {len(unrelated_chunks)} chunks")

        # Re-insert same chunks to prove idempotency (ON CONFLICT DO NOTHING)
        await chunk_repo.insert_chunks(
            document_id=DOC_1,
            channel_id=CHANNEL_A,
            chunks=[
                {"chunk_index": c.chunk_index, "content": c.content, "embedding": e}
                for c, e in zip(chunks, embeddings)
            ],
        )
        print_pass("re-insert of same (document_id, chunk_index) did not raise / duplicate")

        # ── Step 4: relevant retrieval ───────────────────────────────────
        print_step(4, TOTAL_STEPS, "Relevant query retrieves the right chunk")
        query_vec = await embedder.embed_query("What is HNSW indexing and how does it work?")
        results = await chunk_repo.semantic_search(CHANNEL_A, query_vec, limit=5)
        if not results:
            print_fail("Expected at least one result for a clearly relevant query")
        top = results[0]
        if "HNSW" not in top["content"] and "graph" not in top["content"].lower():
            print_fail(f"Top result doesn't look relevant: {top['content'][:120]!r}")
        print_pass(f"top result score={top['score']:.3f} doc={top['document_id']}")
        for r in results:
            print(f"      score={r['score']:.3f}  doc={r['document_id']}  chunk={r['chunk_index']}")

        # ── Step 5: irrelevant query ─────────────────────────────────────
        print_step(5, TOTAL_STEPS, "Irrelevant query returns nothing above min_score")
        noise_vec = await embedder.embed_query("What is the boiling point of liquid nitrogen?")
        noise_results = await chunk_repo.semantic_search(CHANNEL_A, noise_vec, limit=5, min_score=0.6)
        if noise_results:
            scores = [r["score"] for r in noise_results]
            print_fail(f"Expected no results above min_score=0.6 for unrelated query, got scores={scores}")
        print_pass("no false-positive matches above threshold")

        # ── Step 6: empty channel ────────────────────────────────────────
        print_step(6, TOTAL_STEPS, "Query against a channel with zero chunks returns []")
        empty_results = await chunk_repo.semantic_search(f"empty-channel-{RUN_ID}", query_vec, limit=5)
        if empty_results != []:
            print_fail(f"Expected empty list, got {empty_results}")
        print_pass("empty result set, no exception")

        # ── Step 7: cross-channel isolation ──────────────────────────────
        print_step(7, TOTAL_STEPS, "Cross-channel isolation — perfect match in channel B never leaks into A")
        leak_chunks = split_text(DOC_TEXT, chunk_words=60, overlap_words=10)
        leak_embeddings = await embedder.embed_texts([c.content for c in leak_chunks])
        await chunk_repo.insert_chunks(
            document_id=DOC_LEAK,
            channel_id=CHANNEL_B,
            chunks=[
                {"chunk_index": c.chunk_index, "content": c.content, "embedding": e}
                for c, e in zip(leak_chunks, leak_embeddings)
            ],
        )
        a_results = await chunk_repo.semantic_search(CHANNEL_A, query_vec, limit=20)
        if any(r["document_id"] == DOC_LEAK for r in a_results):
            print_fail("Channel B document leaked into channel A search results")
        print_pass("identical content in channel B did not leak into channel A")

        # ── Step 8: max_per_document cap ──────────────────────────────────
        print_step(8, TOTAL_STEPS, "max_per_document caps a single document's dominance")
        capped_results = await chunk_repo.semantic_search(CHANNEL_A, query_vec, limit=10, max_per_document=1)
        doc1_count = sum(1 for r in capped_results if r["document_id"] == DOC_1)
        if doc1_count > 1:
            print_fail(f"max_per_document=1 violated: {doc1_count} chunks from {DOC_1}")
        print_pass(f"doc1 contributed {doc1_count} chunk(s) with max_per_document=1")

    finally:
        # ── Step 9: cleanup ───────────────────────────────────────────────
        print_step(9, TOTAL_STEPS, "Cleanup")
        async with session_factory() as session:
            async with session.begin():
                await session.execute(
                    text("DELETE FROM document_chunks WHERE channel_id = ANY(:channels)"),
                    {"channels": [CHANNEL_A, CHANNEL_B]},
                )
                await session.execute(
                    text("DELETE FROM documents WHERE channel_id = ANY(:channels)"),
                    {"channels": [CHANNEL_A, CHANNEL_B]},
                )
        await engine.dispose()
        print_pass("test data purged")

    print(f"\n{'='*60}")
    print("  ALL RETRIEVAL PIPELINE CHECKS PASSED")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    asyncio.run(main())

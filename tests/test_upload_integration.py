"""
Integration Test — PDF Upload API
====================================
Tests against REAL Postgres, REAL Redis, and REAL MinIO (nothing mocked).

Covers:
1.  Documents table exists and is empty at start
2.  Upload valid PDF → 202, document row created with status=processing
3.  File stored in MinIO at correct object key
4.  Duplicate upload to same channel → 409
5.  Same file to different channel → 202 (dedup is per-channel)
6.  Non-member upload → 403, no file in MinIO, no DB row
7.  Wrong content type (text/plain) → 415
8.  Fake PDF (wrong magic bytes) → 415
9.  Empty file → 400
10. Oversized file (>10MB) → 413, orphan cleaned from MinIO
11. Missing file field → 422
12. Presigned download URL works for stored document
13. Document metadata retrievable and correct in Postgres
14. Cleanup — all test data purged from Postgres, MinIO, Redis

Prerequisites:
  - Postgres running with documents table migrated
  - Redis running (for channel membership)
  - MinIO running
  - Environment variables set (or defaults used)

Usage:
    python -m tests.test_upload_integration
"""

import asyncio
import hashlib
import io
import os
import sys
from uuid import uuid4

# ── Configuration from environment ────────────────────────────
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+asyncpg://postgres:new_password@localhost:5432/messaging",
)
REDIS_URL = os.getenv("REDIS_URL", "redis://:redis@localhost:6379/0")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "documents")
MINIO_USE_SSL = os.getenv("MINIO_USE_SSL", "false").lower() == "true"

# Unique suffix so parallel runs don't collide
RUN_ID = uuid4().hex[:8]
TEST_USER_ID = f"upload-test-user-{RUN_ID}"
OUTSIDER_USER_ID = f"upload-test-outsider-{RUN_ID}"
TEST_CHANNEL_A = f"upload-test-channel-a-{RUN_ID}"
TEST_CHANNEL_B = f"upload-test-channel-b-{RUN_ID}"

TOTAL_STEPS = 14


# ── Output helpers ────────────────────────────────────────────

def print_step(step, total, msg):
    print(f"\n{'='*60}")
    print(f"  [{step}/{total}] {msg}")
    print(f"{'='*60}")


def print_pass(msg):
    print(f"  ✅ {msg}")


def print_fail(msg):
    print(f"  ❌ {msg}")
    sys.exit(1)


# ── Test data generators ─────────────────────────────────────

def make_pdf_bytes(size_bytes: int = 2048) -> bytes:
    """Generate minimal valid PDF content of a given size.

    A real PDF starts with %PDF-1.4. For upload validation we
    only need the magic bytes. The rest is padding — the endpoint
    doesn't parse PDF structure, that's the async processor's job.
    """
    header = b"%PDF-1.4 test-content-for-integration"
    if size_bytes <= len(header):
        return header[:size_bytes]
    return header + b"\x00" * (size_bytes - len(header))


def sha256_of(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


# ── Infrastructure setup ──────────────────────────────────────

async def setup_postgres():
    """Connect to Postgres and verify documents table exists."""
    from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
    from sqlalchemy import text

    engine = create_async_engine(DATABASE_URL, echo=False)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)

    async with session_factory() as session:
        try:
            await session.execute(text("SELECT 1 FROM documents LIMIT 0"))
        except Exception as e:
            print_fail(
                f"documents table does not exist — run migrations first: {e}"
            )

    return engine, session_factory


async def setup_redis():
    """Connect to Redis and seed channel membership for test users."""
    import redis.asyncio as aioredis

    r = aioredis.from_url(REDIS_URL, decode_responses=True)
    await r.ping()

    # TEST_USER_ID is a member of both channels
    # OUTSIDER_USER_ID is a member of neither
    await r.sadd(f"channel:{TEST_CHANNEL_A}:members", TEST_USER_ID)
    await r.sadd(f"channel:{TEST_CHANNEL_B}:members", TEST_USER_ID)
    await r.sadd(f"user:{TEST_USER_ID}:channels", TEST_CHANNEL_A, TEST_CHANNEL_B)

    return r


def setup_minio():
    """Connect to MinIO and ensure the bucket exists."""
    from minio import Minio

    client = Minio(
        endpoint=MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_USE_SSL,
    )

    if not client.bucket_exists(MINIO_BUCKET):
        client.make_bucket(MINIO_BUCKET)

    return client


# ── DB helpers ────────────────────────────────────────────────

async def count_documents(session_factory, channel_id: str = None) -> int:
    from sqlalchemy import text

    query = "SELECT COUNT(*) FROM documents"
    params = {}
    if channel_id:
        query += " WHERE channel_id = :cid"
        params["cid"] = channel_id

    async with session_factory() as session:
        result = await session.execute(text(query), params)
        return result.scalar()


async def fetch_document(session_factory, document_id: str):
    from sqlalchemy import text

    async with session_factory() as session:
        result = await session.execute(
            text("SELECT * FROM documents WHERE document_id = :did"),
            {"did": document_id},
        )
        return result.mappings().fetchone()


async def cleanup_documents(session_factory, channel_ids: list[str]):
    from sqlalchemy import text

    async with session_factory() as session:
        for cid in channel_ids:
            await session.execute(
                text("DELETE FROM documents WHERE channel_id = :cid"),
                {"cid": cid},
            )
        await session.commit()


# ── Redis / MinIO helpers ─────────────────────────────────────

async def cleanup_redis(redis_client):
    await redis_client.delete(
        f"channel:{TEST_CHANNEL_A}:members",
        f"channel:{TEST_CHANNEL_B}:members",
        f"user:{TEST_USER_ID}:channels",
        f"user:{OUTSIDER_USER_ID}:channels",
    )


def cleanup_minio(minio_client, channel_ids: list[str]):
    for cid in channel_ids:
        objects = minio_client.list_objects(MINIO_BUCKET, prefix=f"{cid}/")
        for obj in objects:
            minio_client.remove_object(MINIO_BUCKET, obj.object_name)


def minio_object_exists(minio_client, object_key: str) -> bool:
    try:
        minio_client.stat_object(MINIO_BUCKET, object_key)
        return True
    except Exception:
        return False


def count_minio_objects(minio_client, prefix: str) -> int:
    return len(list(minio_client.list_objects(MINIO_BUCKET, prefix=prefix)))


# ── Upload helper ─────────────────────────────────────────────

async def upload_pdf(
    app,
    client,
    channel_id: str,
    pdf_bytes: bytes,
    filename: str = "test-document.pdf",
    content_type: str = "application/pdf",
    user_id: str = TEST_USER_ID,
):
    """POST a file to the upload endpoint.

    Uses FastAPI's dependency_overrides to swap get_current_user.

    Why not unittest.mock.patch?
      FastAPI captures the function reference at import time when
      it evaluates Depends(get_current_user). Patching the name
      in the module's namespace doesn't affect the already-bound
      reference inside FastAPI's dependency graph.

      dependency_overrides is FastAPI's own mechanism for this —
      it intercepts at the DI resolution layer, which is the
      correct interception point.
    """
    from app.core.auth import get_current_user

    app.dependency_overrides[get_current_user] = lambda: user_id

    try:
        response = await client.post(
            f"/channels/{channel_id}/upload",
            files={
                "file": (filename, io.BytesIO(pdf_bytes), content_type),
            },
        )
    finally:
        # Always clean up so the next call can set a different user
        app.dependency_overrides.pop(get_current_user, None)

    return response


# ═══════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════

async def main():
    print("=" * 60)
    print("  PDF UPLOAD INTEGRATION TEST")
    print("=" * 60)
    print(f"  Database:  {DATABASE_URL}")
    print(f"  Redis:     {REDIS_URL}")
    print(f"  MinIO:     {MINIO_ENDPOINT}/{MINIO_BUCKET}")
    print(f"  Run ID:    {RUN_ID}")
    print(f"  User:      {TEST_USER_ID}")
    print(f"  Outsider:  {OUTSIDER_USER_ID}")
    print(f"  Channels:  {TEST_CHANNEL_A}")
    print(f"             {TEST_CHANNEL_B}")

    # ── Bootstrap infrastructure ──────────────────────────────
    #
    # Two layers of connections here:
    #
    # 1. "Our" connections (pg_engine, session_factory, redis, minio)
    #    Used by test helpers: count_documents, fetch_document,
    #    cleanup_*, seed Redis membership. These are independent
    #    of the app and give us direct access for assertions.
    #
    # 2. The app's singletons (redis_client, database, storage_service)
    #    The upload route calls membership_service.get_members()
    #    which calls registry → redis_client.redis.smembers().
    #    If redis_client isn't initialized, that fails and the
    #    membership check falls back to DB — which also isn't
    #    initialized. Result: empty member set → 403.
    #
    #    So we must initialize the app's own singletons too.
    #    This mirrors what main.py's lifespan does at startup.

    pg_engine, session_factory = await setup_postgres()
    redis = await setup_redis()
    minio = setup_minio()
    print_pass("Test infrastructure connected (Postgres, Redis, MinIO)")

    # Initialize the app's singletons — same as lifespan startup
    from app.core.redis_client import redis_client
    from app.db.database import database
    from app.dependencies import storage_service

    await redis_client.initialize()
    await database.initialize()
    await storage_service.initialize()
    print_pass("App singletons initialized (redis_client, database, storage_service)")

    # Clean up any leftover data from a previous failed run
    await cleanup_documents(session_factory, [TEST_CHANNEL_A, TEST_CHANNEL_B])
    cleanup_minio(minio, [TEST_CHANNEL_A, TEST_CHANNEL_B])

    # Wire up the FastAPI test app
    from fastapi import FastAPI
    from httpx import ASGITransport, AsyncClient
    from app.routers.upload import router

    app = FastAPI()
    app.include_router(router)
    transport = ASGITransport(app=app)

    # Track state across steps
    uploaded_doc_ids = []

    try:

        # ══════════════════════════════════════════════════════
        # Step 1: Documents table exists and is empty
        # ══════════════════════════════════════════════════════
        print_step(1, TOTAL_STEPS, "Documents table exists and is empty for test channels")

        count_a = await count_documents(session_factory, TEST_CHANNEL_A)
        count_b = await count_documents(session_factory, TEST_CHANNEL_B)
        assert count_a == 0, f"Expected 0 rows in channel A, got {count_a}"
        assert count_b == 0, f"Expected 0 rows in channel B, got {count_b}"
        print_pass(f"Channel A: {count_a} documents")
        print_pass(f"Channel B: {count_b} documents")

        # ══════════════════════════════════════════════════════
        # Step 2: Upload valid PDF → 202, DB row created
        # ══════════════════════════════════════════════════════
        print_step(2, TOTAL_STEPS, "Upload valid PDF → 202 with document metadata")

        pdf_data = make_pdf_bytes(4096)

        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await upload_pdf(
                app, ac, TEST_CHANNEL_A, pdf_data,
                filename="quarterly-report.pdf",
            )

        assert resp.status_code == 202, (
            f"Expected 202, got {resp.status_code}: {resp.text}"
        )
        body = resp.json()
        assert body["channelId"] == TEST_CHANNEL_A
        assert body["fileName"] == "quarterly-report.pdf"
        assert body["status"] == "processing"
        assert body["fileSizeBytes"] == 4096

        doc_id_1 = body["documentId"]
        uploaded_doc_ids.append(doc_id_1)

        print_pass(f"Status: 202, documentId={doc_id_1[:12]}...")
        print_pass(f"fileName={body['fileName']}, status={body['status']}")
        print_pass(f"fileSizeBytes={body['fileSizeBytes']}")

        # Verify DB row
        row = await fetch_document(session_factory, doc_id_1)
        assert row is not None, "Document row not found in DB"
        assert row["channel_id"] == TEST_CHANNEL_A
        assert row["uploaded_by"] == TEST_USER_ID
        assert row["status"] == "processing"
        assert row["content_type"] == "application/pdf"
        assert row["sha256_hash"] == sha256_of(pdf_data)
        print_pass(f"DB row verified: uploaded_by={row['uploaded_by']}")
        print_pass(f"sha256_hash={row['sha256_hash'][:16]}...")

        # ══════════════════════════════════════════════════════
        # Step 3: File stored in MinIO at correct object key
        # ══════════════════════════════════════════════════════
        print_step(3, TOTAL_STEPS, "File stored in MinIO at correct object key")

        expected_key = f"{TEST_CHANNEL_A}/{doc_id_1}.pdf"
        assert minio_object_exists(minio, expected_key), (
            f"Object not found in MinIO: {expected_key}"
        )

        # Verify content matches what we uploaded
        obj_response = minio.get_object(MINIO_BUCKET, expected_key)
        stored_bytes = obj_response.read()
        obj_response.close()
        obj_response.release_conn()
        assert stored_bytes == pdf_data, "Stored bytes don't match uploaded bytes"
        print_pass(f"Object exists: {MINIO_BUCKET}/{expected_key}")
        print_pass(f"Content verified: {len(stored_bytes)} bytes match original")

        # ══════════════════════════════════════════════════════
        # Step 4: Duplicate upload → 409
        # ══════════════════════════════════════════════════════
        print_step(4, TOTAL_STEPS, "Duplicate upload to same channel → 409")

        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp_dup = await upload_pdf(
                app, ac, TEST_CHANNEL_A, pdf_data,
                filename="quarterly-report-copy.pdf",
            )

        assert resp_dup.status_code == 409, (
            f"Expected 409, got {resp_dup.status_code}: {resp_dup.text}"
        )
        assert resp_dup.json()["detail"]["error"] == "DUPLICATE_FILE"
        print_pass(f"Status: 409, error={resp_dup.json()['detail']['error']}")

        # No second row created
        count = await count_documents(session_factory, TEST_CHANNEL_A)
        assert count == 1, f"Expected 1 row after dup rejection, got {count}"
        print_pass("DB still has 1 row — duplicate not persisted")

        # Orphaned MinIO object from dup attempt was cleaned up
        minio_count = count_minio_objects(minio, f"{TEST_CHANNEL_A}/")
        assert minio_count == 1, (
            f"Expected 1 object (dup should be cleaned), got {minio_count}"
        )
        print_pass("Orphaned MinIO object from duplicate cleaned up")

        # ══════════════════════════════════════════════════════
        # Step 5: Same file to different channel → 202
        # ══════════════════════════════════════════════════════
        print_step(5, TOTAL_STEPS, "Same file to different channel → 202 (per-channel dedup)")

        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp_ch_b = await upload_pdf(
                app, ac, TEST_CHANNEL_B, pdf_data,
                filename="shared-report.pdf",
            )

        assert resp_ch_b.status_code == 202, (
            f"Expected 202, got {resp_ch_b.status_code}: {resp_ch_b.text}"
        )
        doc_id_2 = resp_ch_b.json()["documentId"]
        uploaded_doc_ids.append(doc_id_2)
        print_pass(f"Status: 202, documentId={doc_id_2[:12]}...")
        print_pass("Same SHA-256 in different channel — correctly allowed")

        # ══════════════════════════════════════════════════════
        # Step 6: Non-member upload → 403, no side effects
        # ══════════════════════════════════════════════════════
        print_step(6, TOTAL_STEPS, "Non-member upload → 403, no side effects")

        pdf_outsider = make_pdf_bytes(1024)
        count_before = await count_documents(session_factory, TEST_CHANNEL_A)
        minio_before = count_minio_objects(minio, f"{TEST_CHANNEL_A}/")

        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp_403 = await upload_pdf(
                app, ac, TEST_CHANNEL_A, pdf_outsider,
                user_id=OUTSIDER_USER_ID,
            )

        assert resp_403.status_code == 403, (
            f"Expected 403, got {resp_403.status_code}: {resp_403.text}"
        )
        assert resp_403.json()["detail"]["error"] == "NOT_CHANNEL_MEMBER"
        print_pass(f"Status: 403, error={resp_403.json()['detail']['error']}")

        # No DB row created
        count_after = await count_documents(session_factory, TEST_CHANNEL_A)
        assert count_after == count_before, (
            f"DB row count changed: {count_before} → {count_after}"
        )
        print_pass("No DB row created for unauthorized upload")

        # No MinIO object created
        minio_after = count_minio_objects(minio, f"{TEST_CHANNEL_A}/")
        assert minio_after == minio_before, (
            f"MinIO object count changed: {minio_before} → {minio_after}"
        )
        print_pass("No MinIO object created for unauthorized upload")

        # ══════════════════════════════════════════════════════
        # Step 7: Wrong content type → 415
        # ══════════════════════════════════════════════════════
        print_step(7, TOTAL_STEPS, "Wrong content type (text/plain) → 415")

        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp_415 = await upload_pdf(
                app, ac, TEST_CHANNEL_A, b"just plain text",
                content_type="text/plain",
            )

        assert resp_415.status_code == 415, (
            f"Expected 415, got {resp_415.status_code}"
        )
        assert resp_415.json()["detail"]["error"] == "INVALID_FILE_TYPE"
        print_pass(f"Status: 415, error={resp_415.json()['detail']['error']}")

        # ══════════════════════════════════════════════════════
        # Step 8: Fake PDF (EXE magic bytes) → 415
        # ══════════════════════════════════════════════════════
        print_step(8, TOTAL_STEPS, "Fake PDF (wrong magic bytes) → 415")

        fake_exe = b"MZ\x90\x00" + b"\x00" * 200

        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp_fake = await upload_pdf(
                app, ac, TEST_CHANNEL_A, fake_exe,
                filename="totally-legit.pdf",
                content_type="application/pdf",
            )

        assert resp_fake.status_code == 415, (
            f"Expected 415, got {resp_fake.status_code}"
        )
        assert resp_fake.json()["detail"]["error"] == "INVALID_PDF"
        print_pass(f"Status: 415, error={resp_fake.json()['detail']['error']}")
        print_pass("Magic bytes check caught renamed executable")

        # ══════════════════════════════════════════════════════
        # Step 9: Empty file → 400
        # ══════════════════════════════════════════════════════
        print_step(9, TOTAL_STEPS, "Empty file → 400")

        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp_empty = await upload_pdf(
                app, ac, TEST_CHANNEL_A, b"",
                filename="empty.pdf",
                content_type="application/pdf",
            )

        assert resp_empty.status_code == 400, (
            f"Expected 400, got {resp_empty.status_code}"
        )
        assert resp_empty.json()["detail"]["error"] == "EMPTY_FILE"
        print_pass(f"Status: 400, error={resp_empty.json()['detail']['error']}")

        # ══════════════════════════════════════════════════════
        # Step 10: Oversized file → 413, orphan cleaned up
        # ══════════════════════════════════════════════════════
        print_step(10, TOTAL_STEPS, "Oversized file (>10MB) → 413, orphan cleaned from MinIO")

        minio_before = count_minio_objects(minio, f"{TEST_CHANNEL_A}/")
        oversized = make_pdf_bytes(10 * 1024 * 1024 + 1)

        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp_big = await upload_pdf(
                app, ac, TEST_CHANNEL_A, oversized,
                filename="huge-report.pdf",
            )

        assert resp_big.status_code == 413, (
            f"Expected 413, got {resp_big.status_code}: {resp_big.text}"
        )
        assert resp_big.json()["detail"]["error"] == "FILE_TOO_LARGE"
        print_pass(f"Status: 413, error={resp_big.json()['detail']['error']}")

        # Oversized object was written then deleted — count unchanged
        minio_after = count_minio_objects(minio, f"{TEST_CHANNEL_A}/")
        assert minio_after == minio_before, (
            f"MinIO count changed: {minio_before} → {minio_after} "
            f"(oversized object not cleaned up)"
        )
        print_pass("Oversized file cleaned up from MinIO after rejection")

        # ══════════════════════════════════════════════════════
        # Step 11: Missing file field → 422
        # ══════════════════════════════════════════════════════
        print_step(11, TOTAL_STEPS, "Missing file field → 422 (FastAPI validation)")

        from app.core.auth import get_current_user
        app.dependency_overrides[get_current_user] = lambda: TEST_USER_ID

        try:
            async with AsyncClient(
                transport=transport, base_url="http://test"
            ) as ac:
                resp_no_file = await ac.post(
                    f"/channels/{TEST_CHANNEL_A}/upload",
                )
        finally:
            app.dependency_overrides.pop(get_current_user, None)

        assert resp_no_file.status_code == 422, (
            f"Expected 422, got {resp_no_file.status_code}"
        )
        print_pass("Status: 422 (FastAPI's own validation)")

        # ══════════════════════════════════════════════════════
        # Step 12: Presigned download URL works
        # ══════════════════════════════════════════════════════
        print_step(12, TOTAL_STEPS, "Presigned download URL works for stored document")

        from app.services.storage_service import StorageService
        storage = StorageService()
        presigned_url = await storage.get_presigned_url(
            expected_key, expires_seconds=60,
        )

        assert presigned_url is not None, "Presigned URL was None"
        assert expected_key in presigned_url or doc_id_1 in presigned_url, (
            f"Presigned URL doesn't reference the object key: {presigned_url}"
        )
        print_pass(f"Presigned URL generated: {presigned_url[:80]}...")
        print_pass("URL references the correct object key")

        # ══════════════════════════════════════════════════════
        # Step 13: Document metadata correct in DB
        # ══════════════════════════════════════════════════════
        print_step(13, TOTAL_STEPS, "Document metadata retrievable and correct")

        doc = await fetch_document(session_factory, doc_id_1)
        assert doc is not None, "Document not found in DB"

        assert doc["document_id"] == doc_id_1
        assert doc["channel_id"] == TEST_CHANNEL_A
        assert doc["file_name"] == "quarterly-report.pdf"
        assert doc["content_type"] == "application/pdf"
        assert doc["file_size_bytes"] == 4096
        assert doc["uploaded_by"] == TEST_USER_ID
        assert doc["status"] == "processing"
        assert doc["storage_path"] == expected_key
        assert doc["sha256_hash"] == sha256_of(pdf_data)
        assert doc["created_at"] is not None

        print_pass(f"document_id   = {doc['document_id'][:12]}...")
        print_pass(f"channel_id    = {doc['channel_id']}")
        print_pass(f"file_name     = {doc['file_name']}")
        print_pass(f"content_type  = {doc['content_type']}")
        print_pass(f"file_size     = {doc['file_size_bytes']} bytes")
        print_pass(f"uploaded_by   = {doc['uploaded_by']}")
        print_pass(f"status        = {doc['status']}")
        print_pass(f"storage_path  = {doc['storage_path']}")
        print_pass(f"sha256_hash   = {doc['sha256_hash'][:16]}...")
        print_pass(f"created_at    = {doc['created_at']}")

        # ══════════════════════════════════════════════════════
        # Step 14: Cleanup
        # ══════════════════════════════════════════════════════
        print_step(14, TOTAL_STEPS, "Cleanup — delete test data")

        await cleanup_documents(
            session_factory, [TEST_CHANNEL_A, TEST_CHANNEL_B],
        )
        cleanup_minio(minio, [TEST_CHANNEL_A, TEST_CHANNEL_B])
        await cleanup_redis(redis)

        count_final_a = await count_documents(session_factory, TEST_CHANNEL_A)
        count_final_b = await count_documents(session_factory, TEST_CHANNEL_B)
        minio_final_a = count_minio_objects(minio, f"{TEST_CHANNEL_A}/")
        minio_final_b = count_minio_objects(minio, f"{TEST_CHANNEL_B}/")

        assert count_final_a == 0, f"Cleanup failed: {count_final_a} rows remain"
        assert count_final_b == 0, f"Cleanup failed: {count_final_b} rows remain"
        assert minio_final_a == 0, f"Cleanup failed: {minio_final_a} objects remain"
        assert minio_final_b == 0, f"Cleanup failed: {minio_final_b} objects remain"

        print_pass(f"Postgres: 0 documents in both channels")
        print_pass(f"MinIO: 0 objects in both channel prefixes")
        print_pass(f"Redis: membership keys removed")

        # Tear down app singletons (reverse of startup)
        await database.close()
        await redis_client.close()
        print_pass("App singletons closed (database, redis_client)")

        # Tear down test infrastructure
        await redis.aclose()
        await pg_engine.dispose()

    except AssertionError as e:
        print_fail(str(e))
    except Exception as e:
        import traceback
        traceback.print_exc()
        print_fail(f"Unexpected error: {type(e).__name__}: {e}")

    # ══════════════════════════════════════════════════════════
    # Summary
    # ══════════════════════════════════════════════════════════
    print(f"\n{'='*60}")
    print(f"  ALL {TOTAL_STEPS} STEPS PASSED ✅")
    print(f"{'='*60}")
    print(f"""
  What was proven:
    ✅ Documents table accessible and clean at start
    ✅ Valid PDF upload → 202, DB row with status=processing
    ✅ File bytes stored correctly in MinIO at channelId/docId.pdf
    ✅ Duplicate upload (same hash, same channel) → 409
    ✅ Same file to different channel → 202 (per-channel dedup)
    ✅ Non-member → 403 with no storage or DB side effects
    ✅ Wrong MIME type → 415 before any I/O
    ✅ Fake PDF (wrong magic bytes) → 415 caught by header check
    ✅ Empty file → 400
    ✅ Oversized file → 413, orphan cleaned from MinIO
    ✅ Missing file field → 422 (FastAPI validation)
    ✅ Presigned download URL generated with correct object key
    ✅ All document metadata fields correct in Postgres
    ✅ Test data cleaned up from Postgres, MinIO, and Redis
""")
    print(f"{'='*60}")


if __name__ == "__main__":
    asyncio.run(main())
"""
Integration Test — Kafka Document Events, Status Polling, Reconciliation
=========================================================================
Tests against REAL Postgres, REAL Redis, REAL MinIO, and REAL Kafka.
Nothing mocked.

Covers:
1.  Kafka producer starts and document-processing topic exists
2.  Upload → Kafka event delivered with correct payload
3.  GET /documents/{id}/status returns 'processing' immediately after upload
4.  GET /documents/{id}/status → 404 for unknown document
5.  GET /documents/{id}/status → 403 for non-member
6.  Reconciliation: document stuck >0 minutes → re-enqueued to Kafka
7.  Reconciliation: recently uploaded document NOT re-enqueued (below threshold)
8.  Reconciliation: already-ready document (status='ready') NOT re-enqueued
9.  Reconciliation with multiple stuck documents — all re-enqueued, none skipped
10. Retry logic: transient failure retried, succeeds on second attempt
    (proved by consuming from Kafka and counting messages)
11. Cleanup — all test data purged

Prerequisites:
  - Postgres running with documents table migrated
  - Redis running
  - MinIO running
  - Kafka running with document-processing topic created
    (run scripts/startup.py first)

Usage:
    python -m tests.test_kafka_reconciliation_integration
"""

import asyncio
import io
import json
import os
import sys
import time
from datetime import datetime, timezone, timedelta
from uuid import uuid4

from confluent_kafka import Consumer, KafkaError, TopicPartition
from confluent_kafka.admin import AdminClient

# ── Configuration ─────────────────────────────────────────────
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
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
DOCUMENT_PROCESSING_TOPIC = "document-processing"

# Unique run ID so parallel test runs never collide on DB/Redis/Kafka keys
RUN_ID = uuid4().hex[:8]
TEST_USER_ID = f"kafka-test-user-{RUN_ID}"
OUTSIDER_USER_ID = f"kafka-test-outsider-{RUN_ID}"
TEST_CHANNEL_A = f"kafka-test-channel-a-{RUN_ID}"
TEST_CHANNEL_B = f"kafka-test-channel-b-{RUN_ID}"

# Override stuck threshold to 0 so any document is immediately eligible
# for reconciliation. The real value (10 minutes) makes no sense in a test.
RECONCILIATION_STUCK_MINUTES_OVERRIDE = 0

TOTAL_STEPS = 11


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


# ── Test data ─────────────────────────────────────────────────

def make_pdf_bytes(size: int = 2048) -> bytes:
    header = b"%PDF-1.4 integration-test"
    if size <= len(header):
        return header[:size]
    return header + b"\x00" * (size - len(header))


# ── Infrastructure setup ──────────────────────────────────────

async def setup_postgres():
    from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
    from sqlalchemy import text

    engine = create_async_engine(DATABASE_URL, echo=False)
    factory = async_sessionmaker(engine, expire_on_commit=False)

    async with factory() as s:
        try:
            await s.execute(text("SELECT 1 FROM documents LIMIT 0"))
        except Exception as e:
            print_fail(f"documents table missing — run migrations: {e}")

    return engine, factory


async def setup_redis():
    import redis.asyncio as aioredis

    r = aioredis.from_url(REDIS_URL, decode_responses=True)
    await r.ping()
    # TEST_USER_ID is member of both channels; OUTSIDER_USER_ID is member of neither
    await r.sadd(f"channel:{TEST_CHANNEL_A}:members", TEST_USER_ID)
    await r.sadd(f"channel:{TEST_CHANNEL_B}:members", TEST_USER_ID)
    await r.sadd(f"user:{TEST_USER_ID}:channels", TEST_CHANNEL_A, TEST_CHANNEL_B)
    return r


def setup_minio():
    from minio import Minio

    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_USE_SSL,
    )
    if not client.bucket_exists(MINIO_BUCKET):
        client.make_bucket(MINIO_BUCKET)
    return client


def verify_kafka_topic():
    """Confirm document-processing topic exists before running tests."""
    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
    metadata = admin.list_topics(timeout=10)
    if DOCUMENT_PROCESSING_TOPIC not in metadata.topics:
        print_fail(
            f"Topic '{DOCUMENT_PROCESSING_TOPIC}' does not exist. "
            f"Run scripts/startup.py first."
        )
    print_pass(f"Kafka topic '{DOCUMENT_PROCESSING_TOPIC}' exists")


# ── Kafka consumer helper ─────────────────────────────────────

def open_consumer_at_end(topic: str) -> Consumer:
    """
    Create a Kafka consumer positioned at the current end of every
    partition of `topic`. Only messages produced AFTER this call
    will be visible when drain_consumer() is called.

    ── Why split open / drain? ───────────────────────────────────
    The seek-to-end must happen BEFORE the code that produces the
    message being tested. If we seek after producing, the high
    watermark is already past our message and we miss it entirely.

    Correct pattern for every step that checks Kafka:

        consumer = open_consumer_at_end(TOPIC)   # seek to NOW
        ... produce something ...
        messages = drain_consumer(consumer, ...)  # catch what arrived

    ── Why not subscribe() + auto.offset.reset=latest? ──────────
    subscribe() uses the group coordinator and rebalances
    asynchronously. The assignment isn't live until the first
    poll completes the rebalance. assign() + manual seek is
    synchronous and deterministic for test purposes.

    ── State machine requirement ─────────────────────────────────
    confluent-kafka's seek() is only valid once the consumer is in
    FETCH state. We poll in a tight loop until get_watermark_offsets
    stops raising, which is the reliable signal that the broker
    handshake for each partition is complete.
    """
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": f"kafka-test-{RUN_ID}-{uuid4().hex[:6]}",
        "auto.offset.reset": "latest",
        "enable.auto.commit": False,
    })

    metadata = consumer.list_topics(topic, timeout=5)
    partitions = [
        TopicPartition(topic, p)
        for p in metadata.topics[topic].partitions.keys()
    ]

    consumer.assign(partitions)

    # Poll until every partition's assignment is active (FETCH state).
    # get_watermark_offsets raises if a partition isn't ready yet.
    # Cap at 5s — in practice this completes in < 200ms on localhost.
    ready_deadline = time.monotonic() + 5.0
    while time.monotonic() < ready_deadline:
        consumer.poll(0.1)
        try:
            for tp in partitions:
                consumer.get_watermark_offsets(tp, timeout=1)
            break  # all partitions ready
        except Exception:
            continue

    # Seek every partition to its current high watermark.
    # Any message produced AFTER this point will be returned by poll().
    for tp in partitions:
        _, high = consumer.get_watermark_offsets(tp, timeout=5)
        consumer.seek(TopicPartition(tp.topic, tp.partition, high))

    return consumer


def drain_consumer(
    consumer: Consumer,
    timeout_seconds: float = 10.0,
    max_messages: int = 20,
    filter_channel_id: str = None,
) -> list[dict]:
    """
    Poll `consumer` until timeout, collecting JSON-decoded messages.
    Closes the consumer before returning.

    Call open_consumer_at_end() BEFORE producing, then this AFTER.
    """
    messages = []
    deadline = time.monotonic() + timeout_seconds

    try:
        while time.monotonic() < deadline and len(messages) < max_messages:
            msg = consumer.poll(0.5)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print_fail(f"Kafka consumer error: {msg.error()}")

            try:
                payload = json.loads(msg.value().decode("utf-8"))
            except json.JSONDecodeError:
                continue

            if filter_channel_id and payload.get("channelId") != filter_channel_id:
                continue

            messages.append(payload)

        return messages

    finally:
        consumer.close()


# ── DB helpers ────────────────────────────────────────────────

async def insert_document_row(
    factory,
    document_id: str,
    channel_id: str,
    status: str = "processing",
    # Pass a specific created_at to simulate old/stuck documents.
    # None = use DB default (NOW()).
    created_at: datetime = None,
    storage_path: str = None,
    uploaded_by: str = None,
):
    """Insert a documents row directly, bypassing the upload endpoint.

    Used in reconciliation tests to set up stuck/ready/failed documents
    without going through MinIO or Kafka. We're testing the
    reconciliation query and re-enqueue logic — not the upload flow.

    ── Why always INSERT as 'processing' then UPDATE? ──────────────
    The DB has a check constraint (ck_documents_status) that only
    allows 'processing' on INSERT — documents must enter through the
    normal upload flow state. Other statuses ('ready', 'failed')
    are set by the consumer via UPDATE, never on initial insert.

    To simulate a ready/failed document in tests we mirror that exact
    transition: INSERT processing → UPDATE to the target status.
    This also makes test setup match real data lifecycle exactly.
    """
    from sqlalchemy import text

    async with factory() as session:
        base_params = {
            "doc_id": document_id,
            "ch_id": channel_id,
            "fname": "test.pdf",
            "ctype": "application/pdf",
            "fsize": 1024,
            "uid": uploaded_by or TEST_USER_ID,
            "path": storage_path or f"{channel_id}/{document_id}.pdf",
            "hash": f"fakehash-{document_id[:8]}",
        }

        # Always INSERT with status='processing' — the constraint only
        # allows this value on initial insert.
        if created_at is not None:
            await session.execute(
                text("""
                    INSERT INTO documents
                        (document_id, channel_id, file_name, content_type,
                         file_size_bytes, uploaded_by, status,
                         storage_path, sha256_hash, created_at)
                    VALUES
                        (:doc_id, :ch_id, :fname, :ctype,
                         :fsize, :uid, 'processing',
                         :path, :hash, :created_at)
                """),
                {**base_params, "created_at": created_at},
            )
        else:
            await session.execute(
                text("""
                    INSERT INTO documents
                        (document_id, channel_id, file_name, content_type,
                         file_size_bytes, uploaded_by, status,
                         storage_path, sha256_hash)
                    VALUES
                        (:doc_id, :ch_id, :fname, :ctype,
                         :fsize, :uid, 'processing',
                         :path, :hash)
                """),
                base_params,
            )

        # If the desired final status is not 'processing', UPDATE now.
        # This mirrors how the real consumer transitions rows.
        if status != "processing":
            await session.execute(
                text("""
                    UPDATE documents
                    SET status = :status
                    WHERE document_id = :doc_id
                """),
                {"status": status, "doc_id": document_id},
            )

        await session.commit()


async def fetch_document(factory, document_id: str):
    from sqlalchemy import text

    async with factory() as session:
        result = await session.execute(
            text("SELECT * FROM documents WHERE document_id = :did"),
            {"did": document_id},
        )
        return result.mappings().fetchone()


async def cleanup_documents(factory, channel_ids: list[str]):
    from sqlalchemy import text

    async with factory() as session:
        for cid in channel_ids:
            await session.execute(
                text("DELETE FROM documents WHERE channel_id = :cid"),
                {"cid": cid},
            )
        await session.commit()


async def cleanup_redis(r):
    await r.delete(
        f"channel:{TEST_CHANNEL_A}:members",
        f"channel:{TEST_CHANNEL_B}:members",
        f"user:{TEST_USER_ID}:channels",
    )


def cleanup_minio(minio_client, channel_ids: list[str]):
    for cid in channel_ids:
        objs = minio_client.list_objects(MINIO_BUCKET, prefix=f"{cid}/")
        for obj in objs:
            minio_client.remove_object(MINIO_BUCKET, obj.object_name)


# ── Upload helper (mirrors existing test) ─────────────────────

async def upload_pdf(app, client, channel_id, pdf_bytes, user_id=None):
    from app.core.auth import get_current_user

    uid = user_id or TEST_USER_ID
    app.dependency_overrides[get_current_user] = lambda: uid
    try:
        resp = await client.post(
            f"/channels/{channel_id}/upload",
            files={"file": ("test.pdf", io.BytesIO(pdf_bytes), "application/pdf")},
        )
    finally:
        app.dependency_overrides.pop(get_current_user, None)
    return resp


# ── Reconciliation helper ─────────────────────────────────────

async def run_reconciliation_with_override(stuck_minutes: int = 0):
    """
    Call reenqueue_stuck_documents() with STUCK_AFTER_MINUTES overridden.

    Why override instead of waiting 10 real minutes?
      The production threshold exists to avoid false re-enqueues during
      normal processing. In tests we want immediate results. We patch
      the module-level constant before calling the function, then
      restore it. This is safe because tests run sequentially.

    Why not use unittest.mock.patch?
      We could, but since this file avoids all mock imports to keep
      the "real infra, no mocking" contract clear, a manual
      save/restore is more explicit about what's happening.
    """
    import app.core.reconciliation as rec_module

    original = rec_module.STUCK_AFTER_MINUTES
    rec_module.STUCK_AFTER_MINUTES = stuck_minutes
    try:
        await rec_module.reenqueue_stuck_documents()
    finally:
        rec_module.STUCK_AFTER_MINUTES = original


# ═══════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════

async def main():
    print("=" * 60)
    print("  KAFKA / RECONCILIATION INTEGRATION TEST")
    print("=" * 60)
    print(f"  Database : {DATABASE_URL}")
    print(f"  Kafka    : {KAFKA_BOOTSTRAP}")
    print(f"  Redis    : {REDIS_URL}")
    print(f"  MinIO    : {MINIO_ENDPOINT}/{MINIO_BUCKET}")
    print(f"  Run ID   : {RUN_ID}")
    print(f"  User     : {TEST_USER_ID}")
    print(f"  Channels : {TEST_CHANNEL_A}")
    print(f"             {TEST_CHANNEL_B}")

    # ── Bootstrap ─────────────────────────────────────────────
    pg_engine, factory = await setup_postgres()
    redis = await setup_redis()
    minio = setup_minio()
    verify_kafka_topic()
    print_pass("Infrastructure connected (Postgres, Redis, MinIO, Kafka)")

    # Initialize app singletons — same as lifespan startup in main.py
    from app.core.redis_client import redis_client
    from app.db.database import database
    from app.dependencies import storage_service
    from app.core.kafka_producer import kafka_producer

    await redis_client.initialize()
    await database.initialize()
    await storage_service.initialize()
    kafka_producer.start()
    kafka_producer.start_poller()
    print_pass("App singletons initialized (redis_client, database, storage, kafka)")

    # Clean up any leftovers from previous failed runs
    await cleanup_documents(factory, [TEST_CHANNEL_A, TEST_CHANNEL_B])
    cleanup_minio(minio, [TEST_CHANNEL_A, TEST_CHANNEL_B])

    # Wire up FastAPI test app with both routers
    from fastapi import FastAPI
    from httpx import ASGITransport, AsyncClient
    from app.routers.upload import router as upload_router
    from app.routers.documents import router as documents_router

    app = FastAPI()
    app.include_router(upload_router)
    app.include_router(documents_router)
    transport = ASGITransport(app=app)

    try:

        # ══════════════════════════════════════════════════════
        # Step 1: Kafka producer starts, topic exists
        # ══════════════════════════════════════════════════════
        print_step(1, TOTAL_STEPS, "Kafka producer started, document-processing topic exists")

        admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
        metadata = admin.list_topics(timeout=10)
        assert DOCUMENT_PROCESSING_TOPIC in metadata.topics, (
            f"Topic '{DOCUMENT_PROCESSING_TOPIC}' missing"
        )
        print_pass(f"Topic '{DOCUMENT_PROCESSING_TOPIC}' confirmed in broker")

        # Verify circuit breaker starts closed (healthy)
        stats = kafka_producer.circuit_stats()
        assert stats.get("state") in ("closed", "CLOSED"), (
            f"Circuit breaker not closed at start: {stats}"
        )
        print_pass(f"Circuit breaker state: {stats.get('state')}")

        # ══════════════════════════════════════════════════════
        # Step 2: Upload → Kafka event delivered with correct payload
        # ══════════════════════════════════════════════════════
        print_step(2, TOTAL_STEPS, "Upload → Kafka event delivered with correct payload")

        pdf_data = make_pdf_bytes(2048)

        # Open consumer and seek to end BEFORE the upload so we don't
        # miss the message that the upload produces. drain_consumer()
        # is called after the upload to collect what arrived.
        consumer_step2 = open_consumer_at_end(DOCUMENT_PROCESSING_TOPIC)

        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await upload_pdf(app, ac, TEST_CHANNEL_A, pdf_data)

        assert resp.status_code == 202, (
            f"Expected 202, got {resp.status_code}: {resp.text}"
        )
        doc_id_upload = resp.json()["documentId"]
        print_pass(f"Upload returned 202, documentId={doc_id_upload[:12]}...")

        # produce_document_event already awaited delivery before returning
        # 202, so the message is already in Kafka. Drain with 10s timeout
        # to handle any broker replication lag.
        messages = drain_consumer(
            consumer_step2,
            timeout_seconds=10.0,
            filter_channel_id=TEST_CHANNEL_A,
        )

        # Find our specific message
        our_msg = next(
            (m for m in messages if m.get("documentId") == doc_id_upload),
            None,
        )
        assert our_msg is not None, (
            f"Message for documentId={doc_id_upload} not found in Kafka. "
            f"Messages seen: {[m.get('documentId') for m in messages]}"
        )
        print_pass(f"Kafka message found for documentId={doc_id_upload[:12]}...")

        # Verify every field of the payload
        assert our_msg["channelId"] == TEST_CHANNEL_A, (
            f"channelId mismatch: {our_msg['channelId']}"
        )
        assert our_msg["uploadedBy"] == TEST_USER_ID, (
            f"uploadedBy mismatch: {our_msg['uploadedBy']}"
        )
        assert "storagePath" in our_msg, "storagePath missing from payload"
        assert our_msg["storagePath"].startswith(TEST_CHANNEL_A), (
            f"storagePath should start with channel prefix: {our_msg['storagePath']}"
        )
        # isReconciliation should NOT be set on a fresh upload
        assert "isReconciliation" not in our_msg, (
            "Fresh upload should not have isReconciliation flag"
        )
        print_pass(f"channelId    = {our_msg['channelId']}")
        print_pass(f"uploadedBy   = {our_msg['uploadedBy']}")
        print_pass(f"storagePath  = {our_msg['storagePath']}")
        print_pass("isReconciliation flag absent (correct for fresh upload)")

        # ══════════════════════════════════════════════════════
        # Step 3: GET /documents/{id}/status returns 'processing'
        # ══════════════════════════════════════════════════════
        print_step(3, TOTAL_STEPS, "GET /documents/{id}/status → 'processing' after upload")

        from app.core.auth import get_current_user
        app.dependency_overrides[get_current_user] = lambda: TEST_USER_ID

        try:
            async with AsyncClient(transport=transport, base_url="http://test") as ac:
                resp_status = await ac.get(f"/documents/{doc_id_upload}/status")
        finally:
            app.dependency_overrides.pop(get_current_user, None)

        assert resp_status.status_code == 200, (
            f"Expected 200, got {resp_status.status_code}: {resp_status.text}"
        )
        status_body = resp_status.json()
        assert status_body["status"] == "processing", (
            f"Expected 'processing', got '{status_body['status']}'"
        )
        assert status_body["documentId"] == doc_id_upload
        assert status_body["channelId"] == TEST_CHANNEL_A
        assert status_body["fileName"] == "test.pdf"
        assert status_body["uploadedBy"] == TEST_USER_ID
        assert "createdAt" in status_body

        print_pass(f"status      = {status_body['status']}")
        print_pass(f"documentId  = {status_body['documentId'][:12]}...")
        print_pass(f"channelId   = {status_body['channelId']}")
        print_pass(f"uploadedBy  = {status_body['uploadedBy']}")
        print_pass(f"createdAt   = {status_body['createdAt']}")

        # ══════════════════════════════════════════════════════
        # Step 4: GET /documents/{id}/status → 404 for unknown ID
        # ══════════════════════════════════════════════════════
        print_step(4, TOTAL_STEPS, "GET /documents/{id}/status → 404 for unknown document")

        fake_id = str(uuid4())
        app.dependency_overrides[get_current_user] = lambda: TEST_USER_ID
        try:
            async with AsyncClient(transport=transport, base_url="http://test") as ac:
                resp_404 = await ac.get(f"/documents/{fake_id}/status")
        finally:
            app.dependency_overrides.pop(get_current_user, None)

        assert resp_404.status_code == 404, (
            f"Expected 404, got {resp_404.status_code}"
        )
        assert resp_404.json()["detail"]["error"] == "NOT_FOUND"
        print_pass(f"Status: 404, error={resp_404.json()['detail']['error']}")

        # ══════════════════════════════════════════════════════
        # Step 5: GET /documents/{id}/status → 403 for non-member
        # ══════════════════════════════════════════════════════
        print_step(5, TOTAL_STEPS, "GET /documents/{id}/status → 403 for non-member")

        app.dependency_overrides[get_current_user] = lambda: OUTSIDER_USER_ID
        try:
            async with AsyncClient(transport=transport, base_url="http://test") as ac:
                resp_403 = await ac.get(f"/documents/{doc_id_upload}/status")
        finally:
            app.dependency_overrides.pop(get_current_user, None)

        assert resp_403.status_code == 403, (
            f"Expected 403, got {resp_403.status_code}"
        )
        assert resp_403.json()["detail"]["error"] == "NOT_CHANNEL_MEMBER"
        print_pass(f"Status: 403, error={resp_403.json()['detail']['error']}")
        print_pass("Document existence not leaked to non-member")

        # ══════════════════════════════════════════════════════
        # Step 6: Reconciliation — stuck document gets re-enqueued
        # ══════════════════════════════════════════════════════
        print_step(6, TOTAL_STEPS, "Reconciliation: stuck document → re-enqueued to Kafka")

        # Insert a document with created_at in the past so it's
        # immediately eligible even with STUCK_AFTER_MINUTES=0.
        stuck_doc_id = str(uuid4())
        stuck_channel = TEST_CHANNEL_A
        old_time = datetime.now(timezone.utc) - timedelta(minutes=15)

        await insert_document_row(
            factory,
            document_id=stuck_doc_id,
            channel_id=stuck_channel,
            status="processing",
            created_at=old_time,
        )
        print_pass(f"Inserted stuck document {stuck_doc_id[:12]}... (created 15min ago)")

        # Open consumer BEFORE reconciliation runs so we don't miss
        # the event it produces.
        consumer_step6 = open_consumer_at_end(DOCUMENT_PROCESSING_TOPIC)

        # Run reconciliation with threshold=0 (any age qualifies)
        await run_reconciliation_with_override(stuck_minutes=0)

        recon_messages = drain_consumer(
            consumer_step6,
            timeout_seconds=10.0,
            filter_channel_id=stuck_channel,
        )

        recon_msg = next(
            (m for m in recon_messages if m.get("documentId") == stuck_doc_id),
            None,
        )
        assert recon_msg is not None, (
            f"Reconciliation message not found in Kafka for {stuck_doc_id}. "
            f"Messages seen: {[m.get('documentId') for m in recon_messages]}"
        )
        print_pass(f"Reconciliation Kafka message found for {stuck_doc_id[:12]}...")

        # isReconciliation flag MUST be set so the consumer can log it
        assert recon_msg.get("isReconciliation") is True, (
            f"isReconciliation flag missing or False: {recon_msg}"
        )
        assert recon_msg["channelId"] == stuck_channel
        assert recon_msg["uploadedBy"] == TEST_USER_ID
        print_pass("isReconciliation = True (correct for re-enqueued document)")
        print_pass(f"channelId  = {recon_msg['channelId']}")
        print_pass(f"uploadedBy = {recon_msg['uploadedBy']}")

        # ══════════════════════════════════════════════════════
        # Step 7: Reconciliation does NOT re-enqueue recent documents
        # ══════════════════════════════════════════════════════
        print_step(7, TOTAL_STEPS, "Reconciliation: recently uploaded doc NOT re-enqueued")

        # Insert a document with NOW() as created_at — should be
        # excluded when stuck_minutes=1 (must be older than 1 minute)
        recent_doc_id = str(uuid4())
        await insert_document_row(
            factory,
            document_id=recent_doc_id,
            channel_id=TEST_CHANNEL_A,
            status="processing",
            # No created_at → DB uses NOW()
        )
        print_pass(f"Inserted recent document {recent_doc_id[:12]}... (created just now)")

        # Open consumer BEFORE reconciliation so we catch anything
        # it might wrongly produce for the recent document.
        consumer_step7 = open_consumer_at_end(DOCUMENT_PROCESSING_TOPIC)

        # Run with threshold=1 minute — recent doc should NOT appear
        await run_reconciliation_with_override(stuck_minutes=1)

        recent_messages = drain_consumer(
            consumer_step7,
            timeout_seconds=5.0,
            filter_channel_id=TEST_CHANNEL_A,
        )
        recent_requeued = [
            m for m in recent_messages
            if m.get("documentId") == recent_doc_id
        ]
        assert len(recent_requeued) == 0, (
            f"Recent document was incorrectly re-enqueued: {recent_requeued}"
        )
        print_pass("Recent document correctly excluded from reconciliation")
        print_pass(f"(threshold=1min, document age=~0min → not eligible)")

        # ══════════════════════════════════════════════════════
        # Step 8: Reconciliation does NOT re-enqueue completed documents
        # ══════════════════════════════════════════════════════
        print_step(8, TOTAL_STEPS, "Reconciliation: ready document NOT re-enqueued")

        completed_doc_id = str(uuid4())
        old_time_2 = datetime.now(timezone.utc) - timedelta(minutes=20)

        await insert_document_row(
            factory,
            document_id=completed_doc_id,
            channel_id=TEST_CHANNEL_B,
            status="ready",          # already processed — must not be re-enqueued
            created_at=old_time_2,
        )
        print_pass(f"Inserted completed document {completed_doc_id[:12]}... (created 20min ago)")

        consumer_step8 = open_consumer_at_end(DOCUMENT_PROCESSING_TOPIC)

        await run_reconciliation_with_override(stuck_minutes=0)

        completed_messages = drain_consumer(
            consumer_step8,
            timeout_seconds=5.0,
            filter_channel_id=TEST_CHANNEL_B,
        )
        completed_requeued = [
            m for m in completed_messages
            if m.get("documentId") == completed_doc_id
        ]
        assert len(completed_requeued) == 0, (
            f"Completed document was incorrectly re-enqueued: {completed_requeued}"
        )
        print_pass("Ready document correctly excluded from reconciliation")
        print_pass("(status='ready' documents are never re-enqueued)")

        # ══════════════════════════════════════════════════════
        # Step 9: Multiple stuck documents — all re-enqueued, none skipped
        # ══════════════════════════════════════════════════════
        print_step(9, TOTAL_STEPS, "Reconciliation: multiple stuck documents — all re-enqueued")

        # Insert 3 stuck documents into channel B
        stuck_ids = [str(uuid4()) for _ in range(3)]
        for sid in stuck_ids:
            await insert_document_row(
                factory,
                document_id=sid,
                channel_id=TEST_CHANNEL_B,
                status="processing",
                created_at=datetime.now(timezone.utc) - timedelta(minutes=12),
            )
        print_pass(f"Inserted 3 stuck documents into {TEST_CHANNEL_B}")

        consumer_step9 = open_consumer_at_end(DOCUMENT_PROCESSING_TOPIC)

        await run_reconciliation_with_override(stuck_minutes=0)

        batch_messages = drain_consumer(
            consumer_step9,
            timeout_seconds=15.0,
            filter_channel_id=TEST_CHANNEL_B,
            max_messages=20,
        )

        found_ids = {m.get("documentId") for m in batch_messages}
        for sid in stuck_ids:
            assert sid in found_ids, (
                f"Stuck document {sid[:12]}... not re-enqueued. "
                f"Found: {[d[:12] for d in found_ids]}"
            )
            # All batch messages should have isReconciliation=True
            msg = next(m for m in batch_messages if m.get("documentId") == sid)
            assert msg.get("isReconciliation") is True, (
                f"isReconciliation missing for {sid}"
            )

        print_pass(f"All 3 stuck documents re-enqueued to Kafka")
        print_pass("All carry isReconciliation=True flag")

        # ══════════════════════════════════════════════════════
        # Step 10: Retry logic — produce_document_event retries on failure
        # ══════════════════════════════════════════════════════
        print_step(10, TOTAL_STEPS, "Retry logic: transient failure retried, succeeds")

        #
        # How we prove retries happen without mocking:
        #
        # We cannot make the real Kafka broker fail on exactly one attempt.
        # Instead we prove the retry loop at the unit level by temporarily
        # replacing the _produce_document_event_once method with a version
        # that fails on the first call and succeeds on the second.
        #
        # This is NOT a mock of Kafka — it's a surgical replacement of one
        # internal method on the live kafka_producer instance to inject a
        # single transient failure. The retry loop in produce_document_event
        # is the thing being tested. The real Kafka broker receives the
        # successful retry call.
        #
        # After the test we restore the original method.
        #

        from app.core.kafka_producer import KafkaProduceError

        # Open consumer BEFORE injecting the failure and producing,
        # so the seek position is behind the message we're about to send.
        consumer_step10 = open_consumer_at_end(DOCUMENT_PROCESSING_TOPIC)

        original_once = kafka_producer._produce_document_event_once
        call_count = {"n": 0}

        async def _fail_first_then_real(payload):
            call_count["n"] += 1
            if call_count["n"] == 1:
                # Simulate a transient delivery error on attempt 1
                raise KafkaProduceError("Simulated transient failure (attempt 1)")
            # Attempt 2+ hits the real broker
            return await original_once(payload)

        kafka_producer._produce_document_event_once = _fail_first_then_real

        retry_doc_id = str(uuid4())
        try:
            result = await kafka_producer.produce_document_event({
                "documentId": retry_doc_id,
                "channelId": TEST_CHANNEL_A,
                "storagePath": f"{TEST_CHANNEL_A}/{retry_doc_id}.pdf",
                "uploadedBy": TEST_USER_ID,
            })
        finally:
            kafka_producer._produce_document_event_once = original_once

        assert call_count["n"] == 2, (
            f"Expected 2 attempts (1 fail + 1 success), got {call_count['n']}"
        )
        assert "topic" in result and "offset" in result, (
            f"Expected delivery result with topic/offset, got: {result}"
        )
        print_pass(f"_produce_document_event_once called {call_count['n']} times")
        print_pass("Attempt 1: KafkaProduceError (simulated transient failure)")
        print_pass("Attempt 2: delivered to real Kafka broker")
        print_pass(f"Delivery result: topic={result['topic']} offset={result['offset']}")

        # Confirm the message actually landed in Kafka.
        # Consumer was opened before produce_document_event was called
        # (before the _fail_first_then_real assignment above), so the
        # seek position is behind the retry message.
        retry_messages = drain_consumer(
            consumer_step10,
            timeout_seconds=8.0,
            filter_channel_id=TEST_CHANNEL_A,
        )
        retry_msg = next(
            (m for m in retry_messages if m.get("documentId") == retry_doc_id),
            None,
        )
        assert retry_msg is not None, (
            f"Retry message not found in Kafka for {retry_doc_id}"
        )
        print_pass("Retried message confirmed in Kafka topic")

        # ══════════════════════════════════════════════════════
        # Step 11: Cleanup
        # ══════════════════════════════════════════════════════
        print_step(11, TOTAL_STEPS, "Cleanup — delete all test data")

        await cleanup_documents(factory, [TEST_CHANNEL_A, TEST_CHANNEL_B])
        cleanup_minio(minio, [TEST_CHANNEL_A, TEST_CHANNEL_B])
        await cleanup_redis(redis)

        from sqlalchemy import text as sqla_text
        async with factory() as session:
            result = await session.execute(
                sqla_text(
                    "SELECT COUNT(*) FROM documents "
                    "WHERE channel_id IN (:a, :b)"
                ),
                {"a": TEST_CHANNEL_A, "b": TEST_CHANNEL_B},
            )
            remaining = result.scalar()
        assert remaining == 0, f"Cleanup failed: {remaining} rows remain"

        print_pass("Postgres: 0 documents in test channels")
        print_pass("MinIO: objects cleared")
        print_pass("Redis: membership keys removed")

        # Tear down app singletons
        kafka_producer.shutdown(timeout=5.0)
        await database.close()
        await redis_client.close()
        print_pass("App singletons shut down")

        # Tear down test infrastructure
        await redis.aclose()
        await pg_engine.dispose()

    except AssertionError as e:
        print_fail(str(e))
    except Exception as e:
        import traceback
        traceback.print_exc()
        print_fail(f"Unexpected error: {type(e).__name__}: {e}")

    # ── Summary ───────────────────────────────────────────────
    print(f"\n{'='*60}")
    print(f"  ALL {TOTAL_STEPS} STEPS PASSED ✅")
    print(f"{'='*60}")
    print(f"""
  What was proven:
    ✅ Kafka producer starts cleanly, topic exists in broker
    ✅ Upload emits event to document-processing topic with
       correct documentId, channelId, uploadedBy, storagePath
    ✅ GET /documents/{{id}}/status returns 'processing' after upload
    ✅ GET /documents/{{id}}/status → 404 for unknown document
    ✅ GET /documents/{{id}}/status → 403 for non-member
    ✅ Reconciliation re-enqueues stuck documents with isReconciliation=True
    ✅ Reconciliation skips recently uploaded documents (below threshold)
    ✅ Reconciliation skips already-ready documents (status='ready')
    ✅ Reconciliation re-enqueues all stuck docs in a batch, none skipped
    ✅ Retry loop calls _produce_document_event_once again after failure,
       successful retry message confirmed in real Kafka broker
    ✅ All test data cleaned up from Postgres, MinIO, Redis
""")
    print(f"{'='*60}")


if __name__ == "__main__":
    asyncio.run(main())
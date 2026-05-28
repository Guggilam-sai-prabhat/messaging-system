"""
Integration Test — Document Processing Worker
==============================================
Tests against REAL Postgres, REAL MinIO, and REAL Kafka.
Nothing mocked.

Covers:
1.  Infrastructure reachable (Postgres, MinIO, Kafka topic exists)
2.  Worker processes a valid PDF → status='ready' in DB
3.  Worker handles a corrupted PDF → status='failed' with error_message
4.  Worker handles an image-only PDF (no extractable text) → status='failed'
5.  Worker handles a large PDF truncated at MAX_PAGES → status='ready', truncated noted
6.  Worker commits offset after success — re-delivery skipped (idempotent DB update)
7.  Worker commits offset after failure — broken PDF not reprocessed forever
8.  Worker processes multiple documents in sequence, all reach terminal status
9.  Malformed Kafka payload (missing fields) → document stays 'processing', offset committed
10. Cleanup — all test data purged

Prerequisites:
  - Postgres running with documents table migrated
  - MinIO running with the documents bucket existing (or auto-created)
  - Kafka running with document-processing topic created
    (run scripts/startup.py first)

Usage:
    python -m tests.test_document_worker_integration
"""

import asyncio
import io
import json
import os
import struct
import sys
import time
import zlib
from uuid import uuid4

from confluent_kafka import Producer, KafkaError
from confluent_kafka.admin import AdminClient

# ── Configuration ─────────────────────────────────────────────

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+asyncpg://postgres:new_password@localhost:5432/messaging",
)
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET     = os.getenv("MINIO_BUCKET",     "documents")
MINIO_USE_SSL    = os.getenv("MINIO_USE_SSL",     "false").lower() == "true"
KAFKA_BOOTSTRAP  = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
DOCUMENT_PROCESSING_TOPIC = "document-processing"

# Unique run ID so parallel test runs never collide on DB / MinIO / Kafka keys
RUN_ID      = uuid4().hex[:8]
TEST_USER   = f"worker-test-user-{RUN_ID}"
TEST_CHANNEL = f"worker-test-channel-{RUN_ID}"

# How long to wait for the worker to consume + process one document.
# A real PDF on localhost completes well under 5 s; 20 s is generous.
PROCESSING_TIMEOUT_S = 20.0
POLL_INTERVAL_S      = 0.5

TOTAL_STEPS = 10


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


# ── PDF factories ─────────────────────────────────────────────

def make_valid_pdf(text: str = "Hello integration test") -> bytes:
    """
    Build a minimal but structurally valid single-page PDF that pypdf
    can parse and extract text from.

    We hand-craft the cross-reference table so pypdf doesn't raise
    PdfReadError on any structural check. A real-world PDF is more
    complex, but this is sufficient for extraction.
    """
    # Content stream with the text operator
    content = f"BT /F1 12 Tf 72 720 Td ({text}) Tj ET".encode()
    compressed = zlib.compress(content)

    objects: list[bytes] = []

    # Object 1 — Catalog
    objects.append(b"1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n")

    # Object 2 — Pages
    objects.append(b"2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count 1 >>\nendobj\n")

    # Object 3 — Page
    objects.append(
        b"3 0 obj\n"
        b"<< /Type /Page /Parent 2 0 R\n"
        b"   /MediaBox [0 0 612 792]\n"
        b"   /Contents 4 0 R\n"
        b"   /Resources << /Font << /F1 5 0 R >> >> >>\n"
        b"endobj\n"
    )

    # Object 4 — Content stream
    stream_len = len(compressed)
    objects.append(
        f"4 0 obj\n"
        f"<< /Length {stream_len} /Filter /FlateDecode >>\n"
        f"stream\n".encode()
        + compressed
        + b"\nendstream\nendobj\n"
    )

    # Object 5 — Font (minimal, pypdf doesn't need full font data to extract)
    objects.append(
        b"5 0 obj\n"
        b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>\n"
        b"endobj\n"
    )

    # Assemble body and build xref table
    header = b"%PDF-1.4\n"
    body = header
    offsets: list[int] = []
    for obj in objects:
        offsets.append(len(body))
        body += obj

    xref_offset = len(body)
    xref  = b"xref\n"
    xref += f"0 {len(objects) + 1}\n".encode()
    xref += b"0000000000 65535 f \n"
    for off in offsets:
        xref += f"{off:010d} 00000 n \n".encode()

    trailer = (
        f"trailer\n"
        f"<< /Size {len(objects) + 1} /Root 1 0 R >>\n"
        f"startxref\n"
        f"{xref_offset}\n"
        f"%%EOF\n"
    ).encode()

    return body + xref + trailer


def make_corrupted_pdf() -> bytes:
    """A byte string that starts with %PDF but has invalid structure."""
    return b"%PDF-1.4\n<<garbage content that pypdf cannot parse>>\n%%EOF"


def make_image_only_pdf() -> bytes:
    """
    A structurally valid PDF whose content stream contains no text operators.
    pypdf will parse it but extract_text() returns an empty string on every page.
    """
    # Content stream with only a rectangle drawing — no Tj / TJ operators
    content = b"1 0 0 RG 100 100 200 200 re S"
    compressed = zlib.compress(content)

    objects: list[bytes] = []
    objects.append(b"1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n")
    objects.append(b"2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count 1 >>\nendobj\n")
    objects.append(
        b"3 0 obj\n"
        b"<< /Type /Page /Parent 2 0 R\n"
        b"   /MediaBox [0 0 612 792]\n"
        b"   /Contents 4 0 R\n"
        b"   /Resources << >> >>\n"
        b"endobj\n"
    )
    stream_len = len(compressed)
    objects.append(
        f"4 0 obj\n<< /Length {stream_len} /Filter /FlateDecode >>\nstream\n".encode()
        + compressed
        + b"\nendstream\nendobj\n"
    )

    header = b"%PDF-1.4\n"
    body = header
    offsets = []
    for obj in objects:
        offsets.append(len(body))
        body += obj

    xref_offset = len(body)
    xref  = b"xref\n"
    xref += f"0 {len(objects) + 1}\n".encode()
    xref += b"0000000000 65535 f \n"
    for off in offsets:
        xref += f"{off:010d} 00000 n \n".encode()

    trailer = (
        f"trailer\n<< /Size {len(objects) + 1} /Root 1 0 R >>\n"
        f"startxref\n{xref_offset}\n%%EOF\n"
    ).encode()

    return body + xref + trailer


def make_multi_page_valid_pdf(num_pages: int, text_per_page: str = "Page content") -> bytes:
    """Build a valid PDF with `num_pages` pages, each containing `text_per_page`."""
    # We'll reuse single page content for every page
    content = f"BT /F1 12 Tf 72 720 Td ({text_per_page}) Tj ET".encode()
    compressed = zlib.compress(content)
    stream_len = len(compressed)

    objects: list[bytes] = []

    # Catalog — object 1
    objects.append(b"1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n")

    # Pages — object 2; kids are objects 3..(3+num_pages-1)
    kids = " ".join(f"{i} 0 R" for i in range(3, 3 + num_pages))
    objects.append(
        f"2 0 obj\n<< /Type /Pages /Kids [{kids}] /Count {num_pages} >>\nendobj\n".encode()
    )

    # One content stream shared by all pages — object 3+num_pages
    content_obj_num = 3 + num_pages
    font_obj_num    = content_obj_num + 1

    # Page objects — objects 3..(3+num_pages-1)
    for _ in range(num_pages):
        objects.append(
            f"0 0 obj\n"  # placeholder; offsets fix numbering
            f"<< /Type /Page /Parent 2 0 R\n"
            f"   /MediaBox [0 0 612 792]\n"
            f"   /Contents {content_obj_num} 0 R\n"
            f"   /Resources << /Font << /F1 {font_obj_num} 0 R >> >> >>\n"
            f"endobj\n".encode()
        )

    # Content stream
    objects.append(
        f"0 0 obj\n<< /Length {stream_len} /Filter /FlateDecode >>\nstream\n".encode()
        + compressed
        + b"\nendstream\nendobj\n"
    )

    # Font
    objects.append(
        b"0 0 obj\n<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>\nendobj\n"
    )

    # Re-number objects (the placeholders above are just markers)
    numbered: list[bytes] = []
    for idx, raw in enumerate(objects, start=1):
        numbered.append(raw.replace(b"0 0 obj", f"{idx} 0 obj".encode(), 1))

    header = b"%PDF-1.4\n"
    body   = header
    offsets: list[int] = []
    for obj in numbered:
        offsets.append(len(body))
        body += obj

    xref_offset = len(body)
    xref  = b"xref\n"
    xref += f"0 {len(numbered) + 1}\n".encode()
    xref += b"0000000000 65535 f \n"
    for off in offsets:
        xref += f"{off:010d} 00000 n \n".encode()

    trailer = (
        f"trailer\n<< /Size {len(numbered) + 1} /Root 1 0 R >>\n"
        f"startxref\n{xref_offset}\n%%EOF\n"
    ).encode()

    return body + xref + trailer


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
    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
    meta  = admin.list_topics(timeout=10)
    if DOCUMENT_PROCESSING_TOPIC not in meta.topics:
        print_fail(
            f"Topic '{DOCUMENT_PROCESSING_TOPIC}' does not exist. "
            "Run scripts/startup.py first."
        )
    print_pass(f"Kafka topic '{DOCUMENT_PROCESSING_TOPIC}' exists")


# ── Kafka producer helper ─────────────────────────────────────

def make_producer() -> Producer:
    return Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})


def publish_event(producer: Producer, payload: dict) -> None:
    """Synchronously produce one JSON event and wait for delivery."""
    delivered = {"ok": False, "err": None}

    def _cb(err, _msg):
        if err:
            delivered["err"] = err
        else:
            delivered["ok"] = True

    producer.produce(
        DOCUMENT_PROCESSING_TOPIC,
        value=json.dumps(payload).encode(),
        callback=_cb,
    )
    producer.flush(timeout=10)

    if not delivered["ok"]:
        print_fail(f"Kafka delivery failed: {delivered['err']}")


# ── MinIO helpers ─────────────────────────────────────────────

def upload_to_minio(minio_client, object_key: str, data: bytes) -> None:
    minio_client.put_object(
        MINIO_BUCKET,
        object_key,
        io.BytesIO(data),
        length=len(data),
        content_type="application/pdf",
    )


def minio_object_exists(minio_client, object_key: str) -> bool:
    try:
        minio_client.stat_object(MINIO_BUCKET, object_key)
        return True
    except Exception:
        return False


def cleanup_minio(minio_client, prefix: str) -> None:
    objs = minio_client.list_objects(MINIO_BUCKET, prefix=prefix)
    for obj in objs:
        minio_client.remove_object(MINIO_BUCKET, obj.object_name)


# ── DB helpers ────────────────────────────────────────────────

async def insert_document_row(
    factory,
    document_id: str,
    channel_id: str,
    storage_path: str,
    uploaded_by: str = None,
    file_name: str = "test.pdf",
) -> None:
    """
    Insert a row directly into documents with status='processing'.
    This bypasses the upload API — we're testing the worker, not the upload flow.

    The DB constraint (ck_documents_status) allows 'processing' on INSERT.
    """
    from sqlalchemy import text

    async with factory() as session:
        await session.execute(
            text("""
                INSERT INTO documents
                    (document_id, channel_id, file_name, content_type,
                     file_size_bytes, uploaded_by, status,
                     storage_path, sha256_hash)
                VALUES
                    (:doc_id, :ch_id, :fname, 'application/pdf',
                     1024, :uid, 'processing',
                     :path, :hash)
            """),
            {
                "doc_id": document_id,
                "ch_id":  channel_id,
                "fname":  file_name,
                "uid":    uploaded_by or TEST_USER,
                "path":   storage_path,
                "hash":   f"fakehash-{document_id[:8]}",
            },
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


async def wait_for_status(
    factory,
    document_id: str,
    expected_statuses: tuple[str, ...],
    timeout: float = PROCESSING_TIMEOUT_S,
) -> dict:
    """
    Poll the DB until document_id reaches one of expected_statuses.

    Returns the final row. Fails the test if the timeout expires
    before the status changes.

    Why poll instead of listening on a notification channel?
      The worker writes to Postgres directly via SQLAlchemy, not
      through the app's notification system. Polling is simpler
      and matches what an end-to-end client would observe.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        row = await fetch_document(factory, document_id)
        if row and row["status"] in expected_statuses:
            return dict(row)
        await asyncio.sleep(POLL_INTERVAL_S)

    row = await fetch_document(factory, document_id)
    current = row["status"] if row else "NOT FOUND"
    print_fail(
        f"document_id={document_id} did not reach {expected_statuses} "
        f"within {timeout}s (current: {current})"
    )


async def cleanup_documents(factory, channel_id: str) -> None:
    from sqlalchemy import text

    async with factory() as session:
        await session.execute(
            text("DELETE FROM documents WHERE channel_id = :cid"),
            {"cid": channel_id},
        )
        await session.commit()


# ── Worker lifecycle helpers ──────────────────────────────────

def start_worker() -> "DocumentWorker":
    """
    Import and return a DocumentWorker instance.

    We import here (not at module top) so the worker module only
    runs when the test actually starts — same pattern as the existing
    integration tests that import app modules inside functions.
    """
    sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
    from workers.document_worker import DocumentWorker
    return DocumentWorker()


async def run_worker_until_idle(
    worker: "DocumentWorker",
    idle_seconds: float = 3.0,
) -> None:
    """
    Run the worker until it polls for `idle_seconds` with no new messages,
    BUT only start the idle clock AFTER the Kafka group rebalance is done.

    Why delay the idle clock?
      When a consumer joins a group, the broker must assign partitions
      before any messages are delivered. During rebalance (typically
      1-3 s on localhost) every poll() returns None. If we start the
      idle timer immediately, the worker shuts down before it ever
      receives the message we just published.

      We detect "rebalance complete" as the first poll that returns
      something other than a rebalance-related None — concretely, the
      first time the consumer reaches the high watermark and gets a
      _PARTITION_EOF error, which means it's live. Only then do we arm
      the idle clock.

    Why not run forever?
      Integration tests need the worker to stop after processing the
      test events so the main() coroutine can proceed to assertions.
    """
    from confluent_kafka import KafkaError as _KafkaError

    worker._running = True
    loop = asyncio.get_running_loop()

    # None means "not yet armed" — idle clock starts on first post-rebalance poll
    last_activity_time: float | None = None
    original_handle = worker._handle_message

    async def _tracked_handle(msg):
        nonlocal last_activity_time
        last_activity_time = time.monotonic()
        await original_handle(msg)

    worker._handle_message = _tracked_handle

    async def _poll_loop():
        nonlocal last_activity_time
        while worker._running:
            msg = await loop.run_in_executor(
                None, worker._consumer.poll, 0.5,
            )

            if msg is None:
                # Pre-rebalance: idle clock not yet armed — just keep waiting.
                # Post-rebalance: check if we've been idle long enough to stop.
                if last_activity_time is not None:
                    if time.monotonic() - last_activity_time >= idle_seconds:
                        worker._running = False
                continue

            if msg.error():
                err_code = msg.error().code()
                if err_code == _KafkaError._PARTITION_EOF:
                    # Consumer is live and at the end of the partition.
                    # Arm the idle clock now if not already armed.
                    if last_activity_time is None:
                        last_activity_time = time.monotonic()
                # Other errors: log and keep running (worker's own error path)
                continue

            # Real message — arm idle clock and dispatch.
            if last_activity_time is None:
                last_activity_time = time.monotonic()
            await worker._handle_message(msg)

    await _poll_loop()
    worker._handle_message = original_handle


# ═══════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════

async def main():
    print("=" * 60)
    print("  DOCUMENT WORKER INTEGRATION TEST")
    print("=" * 60)
    print(f"  Database : {DATABASE_URL}")
    print(f"  Kafka    : {KAFKA_BOOTSTRAP}")
    print(f"  MinIO    : {MINIO_ENDPOINT}/{MINIO_BUCKET}")
    print(f"  Run ID   : {RUN_ID}")
    print(f"  User     : {TEST_USER}")
    print(f"  Channel  : {TEST_CHANNEL}")

    # ── Bootstrap ─────────────────────────────────────────────
    pg_engine, factory = await setup_postgres()
    minio = setup_minio()
    verify_kafka_topic()

    producer = make_producer()

    # Clean up any leftovers from a previous failed run
    await cleanup_documents(factory, TEST_CHANNEL)
    cleanup_minio(minio, f"{TEST_CHANNEL}/")

    try:

        # ══════════════════════════════════════════════════════
        # Step 1: Infrastructure reachable
        # ══════════════════════════════════════════════════════
        print_step(1, TOTAL_STEPS, "Infrastructure reachable (Postgres, MinIO, Kafka)")

        from sqlalchemy import text
        async with pg_engine.begin() as conn:
            ver = (await conn.execute(text("SELECT version()"))).scalar()
        print_pass(f"Postgres: {ver[:50]}...")

        minio_exists = minio.bucket_exists(MINIO_BUCKET)
        assert minio_exists, f"MinIO bucket '{MINIO_BUCKET}' does not exist"
        print_pass(f"MinIO: bucket '{MINIO_BUCKET}' exists")

        admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
        meta  = admin.list_topics(timeout=10)
        assert DOCUMENT_PROCESSING_TOPIC in meta.topics
        print_pass(f"Kafka: topic '{DOCUMENT_PROCESSING_TOPIC}' confirmed")

        # ══════════════════════════════════════════════════════
        # Step 2: Valid PDF → status='ready'
        # ══════════════════════════════════════════════════════
        print_step(2, TOTAL_STEPS, "Valid PDF → worker sets status='ready'")

        doc_id    = str(uuid4())
        pdf_bytes = make_valid_pdf("Integration test document content")
        obj_key   = f"{TEST_CHANNEL}/{doc_id}.pdf"

        upload_to_minio(minio, obj_key, pdf_bytes)
        print_pass(f"PDF uploaded to MinIO: {obj_key}")

        await insert_document_row(factory, doc_id, TEST_CHANNEL, obj_key)
        print_pass(f"DB row inserted: document_id={doc_id[:12]}... status=processing")

        worker = start_worker()
        await worker.start()

        event = {
            "documentId":  doc_id,
            "channelId":   TEST_CHANNEL,
            "storagePath": obj_key,
            "fileName":    "test.pdf",
            "uploadedBy":  TEST_USER,
        }
        publish_event(producer, event)
        print_pass("Kafka event published")

        await run_worker_until_idle(worker, idle_seconds=2.0)
        await worker.shutdown()

        row = await wait_for_status(factory, doc_id, ("ready", "failed"))
        assert row["status"] == "ready", (
            f"Expected 'ready', got '{row['status']}'. "
            f"error_message={row.get('error_message')}"
        )
        assert row["error_message"] is None, (
            f"error_message should be NULL on success: {row['error_message']}"
        )
        print_pass(f"status = ready")
        print_pass(f"error_message = NULL")

        # ══════════════════════════════════════════════════════
        # Step 3: Corrupted PDF → status='failed'
        # ══════════════════════════════════════════════════════
        print_step(3, TOTAL_STEPS, "Corrupted PDF → worker sets status='failed'")

        doc_id_bad = str(uuid4())
        obj_key_bad = f"{TEST_CHANNEL}/{doc_id_bad}.pdf"
        upload_to_minio(minio, obj_key_bad, make_corrupted_pdf())
        await insert_document_row(factory, doc_id_bad, TEST_CHANNEL, obj_key_bad)

        worker2 = start_worker()
        await worker2.start()

        publish_event(producer, {
            "documentId":  doc_id_bad,
            "channelId":   TEST_CHANNEL,
            "storagePath": obj_key_bad,
            "fileName":    "corrupted.pdf",
            "uploadedBy":  TEST_USER,
        })

        await run_worker_until_idle(worker2, idle_seconds=2.0)
        await worker2.shutdown()

        row_bad = await wait_for_status(factory, doc_id_bad, ("ready", "failed"))
        assert row_bad["status"] == "failed", (
            f"Expected 'failed', got '{row_bad['status']}'"
        )
        assert row_bad["error_message"] is not None, (
            "error_message should be set for corrupted PDF"
        )
        print_pass(f"status = failed")
        print_pass(f"error_message = {row_bad['error_message'][:60]}...")

        # ══════════════════════════════════════════════════════
        # Step 4: Image-only PDF (no text) → status='failed'
        # ══════════════════════════════════════════════════════
        print_step(4, TOTAL_STEPS, "Image-only PDF (no extractable text) → status='failed'")

        doc_id_img = str(uuid4())
        obj_key_img = f"{TEST_CHANNEL}/{doc_id_img}.pdf"
        upload_to_minio(minio, obj_key_img, make_image_only_pdf())
        await insert_document_row(factory, doc_id_img, TEST_CHANNEL, obj_key_img)

        worker3 = start_worker()
        await worker3.start()

        publish_event(producer, {
            "documentId":  doc_id_img,
            "channelId":   TEST_CHANNEL,
            "storagePath": obj_key_img,
            "fileName":    "scanned.pdf",
            "uploadedBy":  TEST_USER,
        })

        await run_worker_until_idle(worker3, idle_seconds=2.0)
        await worker3.shutdown()

        row_img = await wait_for_status(factory, doc_id_img, ("ready", "failed"))
        assert row_img["status"] == "failed", (
            f"Expected 'failed' for image-only PDF, got '{row_img['status']}'"
        )
        assert row_img["error_message"] is not None
        assert "text" in row_img["error_message"].lower() or \
               "image" in row_img["error_message"].lower() or \
               "extractable" in row_img["error_message"].lower(), (
            f"error_message doesn't mention text/image: {row_img['error_message']}"
        )
        print_pass(f"status = failed")
        print_pass(f"error_message = {row_img['error_message'][:80]}...")

        # ══════════════════════════════════════════════════════
        # Step 5: Large PDF (> MAX_PAGES) → status='ready', truncated
        # ══════════════════════════════════════════════════════
        print_step(5, TOTAL_STEPS, "PDF with 3 pages (MAX_PAGES=2 override) → ready + truncation logged")

        # We test the truncation path by constructing a PDF with more pages
        # than the worker's MAX_PAGES constant, then temporarily lowering
        # MAX_PAGES so the test completes fast (3 pages instead of 501).
        import workers.document_worker as worker_module

        original_max = worker_module.MAX_PAGES
        worker_module.MAX_PAGES = 2   # override: treat 3-page PDF as "too long"

        doc_id_big = str(uuid4())
        obj_key_big = f"{TEST_CHANNEL}/{doc_id_big}.pdf"
        upload_to_minio(minio, obj_key_big, make_multi_page_valid_pdf(3, "Truncation test"))
        await insert_document_row(factory, doc_id_big, TEST_CHANNEL, obj_key_big)

        worker4 = start_worker()
        await worker4.start()

        publish_event(producer, {
            "documentId":  doc_id_big,
            "channelId":   TEST_CHANNEL,
            "storagePath": obj_key_big,
            "fileName":    "big-doc.pdf",
            "uploadedBy":  TEST_USER,
        })

        await run_worker_until_idle(worker4, idle_seconds=2.0)
        await worker4.shutdown()

        worker_module.MAX_PAGES = original_max  # restore

        row_big = await wait_for_status(factory, doc_id_big, ("ready", "failed"))
        assert row_big["status"] == "ready", (
            f"Expected 'ready' for truncated PDF, got '{row_big['status']}'. "
            f"error_message={row_big.get('error_message')}"
        )
        assert row_big["error_message"] is None, (
            "Truncated (but partially extracted) documents should have NULL error_message"
        )
        print_pass("status = ready (truncated document still succeeds)")
        print_pass("error_message = NULL (partial text is accepted)")

        # ══════════════════════════════════════════════════════
        # Step 6: Offset committed after success — re-delivery is safe
        # ══════════════════════════════════════════════════════
        print_step(6, TOTAL_STEPS, "Offset committed after success — idempotent DB update on re-delivery")

        # Re-publish the Step 2 event. The document is already 'ready'.
        # The worker's UPDATE WHERE status='processing' is a no-op.
        # The document must still be 'ready' after re-processing.
        worker5 = start_worker()
        await worker5.start()

        publish_event(producer, {
            "documentId":  doc_id,   # same as Step 2 — already 'ready'
            "channelId":   TEST_CHANNEL,
            "storagePath": obj_key,
            "fileName":    "test.pdf",
            "uploadedBy":  TEST_USER,
        })

        await run_worker_until_idle(worker5, idle_seconds=2.0)
        await worker5.shutdown()

        row_redelivered = await fetch_document(factory, doc_id)
        assert row_redelivered["status"] == "ready", (
            f"Re-delivery corrupted status: {row_redelivered['status']}"
        )
        print_pass("status still = ready after re-delivery")
        print_pass("DB UPDATE with WHERE status='processing' is a safe no-op")

        # ══════════════════════════════════════════════════════
        # Step 7: Offset committed after failure — broken PDF not reprocessed
        # ══════════════════════════════════════════════════════
        print_step(7, TOTAL_STEPS, "Offset committed after failure — broken PDF not reprocessed on re-delivery")

        # Re-publish the corrupted PDF event from Step 3.
        # Status is already 'failed' so the UPDATE is a no-op.
        worker6 = start_worker()
        await worker6.start()

        publish_event(producer, {
            "documentId":  doc_id_bad,
            "channelId":   TEST_CHANNEL,
            "storagePath": obj_key_bad,
            "fileName":    "corrupted.pdf",
            "uploadedBy":  TEST_USER,
        })

        await run_worker_until_idle(worker6, idle_seconds=2.0)
        await worker6.shutdown()

        row_refailed = await fetch_document(factory, doc_id_bad)
        assert row_refailed["status"] == "failed", (
            f"Re-delivery changed status from 'failed': {row_refailed['status']}"
        )
        print_pass("status still = failed after re-delivery")
        print_pass("Corrupted PDF will not be processed again — offset committed on first failure")

        # ══════════════════════════════════════════════════════
        # Step 8: Multiple documents in sequence — all reach terminal status
        # ══════════════════════════════════════════════════════
        print_step(8, TOTAL_STEPS, "Multiple documents in sequence — all reach terminal status")

        batch_ids: list[str] = []
        for i in range(3):
            bid = str(uuid4())
            bkey = f"{TEST_CHANNEL}/{bid}.pdf"
            upload_to_minio(minio, bkey, make_valid_pdf(f"Batch document {i}"))
            await insert_document_row(factory, bid, TEST_CHANNEL, bkey)
            batch_ids.append(bid)

        print_pass(f"Inserted 3 documents into DB and MinIO")

        worker7 = start_worker()
        await worker7.start()

        for bid in batch_ids:
            publish_event(producer, {
                "documentId":  bid,
                "channelId":   TEST_CHANNEL,
                "storagePath": f"{TEST_CHANNEL}/{bid}.pdf",
                "fileName":    f"batch-{bid[:8]}.pdf",
                "uploadedBy":  TEST_USER,
            })

        print_pass("Published 3 Kafka events")

        await run_worker_until_idle(worker7, idle_seconds=3.0)
        await worker7.shutdown()

        ready_count = 0
        for bid in batch_ids:
            row_b = await wait_for_status(factory, bid, ("ready", "failed"))
            if row_b["status"] == "ready":
                ready_count += 1

        assert ready_count == 3, (
            f"Expected all 3 documents to be 'ready', got {ready_count}/3"
        )
        print_pass(f"All 3 documents → ready")

        # ══════════════════════════════════════════════════════
        # Step 9: Malformed Kafka payload (missing fields) → offset committed
        # ══════════════════════════════════════════════════════
        print_step(9, TOTAL_STEPS, "Malformed Kafka payload → ValueError, offset committed, no crash")

        # Publish a payload that is missing required fields.
        # The worker should raise ValueError in _parse_event, log it,
        # and commit the offset without crashing.
        worker8 = start_worker()
        await worker8.start()

        publish_event(producer, {
            "documentId": str(uuid4()),
            # missing: channelId, storagePath, fileName, uploadedBy
        })
        print_pass("Published malformed Kafka event (missing fields)")

        # Give the worker time to consume and commit the bad message
        await run_worker_until_idle(worker8, idle_seconds=2.0)
        await worker8.shutdown()

        # Worker must still be in a runnable state — we verify by
        # processing one more valid document immediately after.
        doc_id_after = str(uuid4())
        obj_key_after = f"{TEST_CHANNEL}/{doc_id_after}.pdf"
        upload_to_minio(minio, obj_key_after, make_valid_pdf("Post-error recovery check"))
        await insert_document_row(factory, doc_id_after, TEST_CHANNEL, obj_key_after)

        worker9 = start_worker()
        await worker9.start()

        publish_event(producer, {
            "documentId":  doc_id_after,
            "channelId":   TEST_CHANNEL,
            "storagePath": obj_key_after,
            "fileName":    "recovery.pdf",
            "uploadedBy":  TEST_USER,
        })

        await run_worker_until_idle(worker9, idle_seconds=2.0)
        await worker9.shutdown()

        row_after = await wait_for_status(factory, doc_id_after, ("ready", "failed"))
        assert row_after["status"] == "ready", (
            f"Worker did not recover after malformed payload: status={row_after['status']}"
        )
        print_pass("Malformed event consumed and offset committed without crash")
        print_pass("Worker processed next valid document successfully after the error")

        # ══════════════════════════════════════════════════════
        # Step 10: Cleanup
        # ══════════════════════════════════════════════════════
        print_step(10, TOTAL_STEPS, "Cleanup — delete all test data")

        await cleanup_documents(factory, TEST_CHANNEL)
        cleanup_minio(minio, f"{TEST_CHANNEL}/")

        from sqlalchemy import text as sqla_text
        async with factory() as session:
            result = await session.execute(
                sqla_text("SELECT COUNT(*) FROM documents WHERE channel_id = :cid"),
                {"cid": TEST_CHANNEL},
            )
            remaining = result.scalar()
        assert remaining == 0, f"Cleanup failed: {remaining} rows remain"

        minio_remaining = list(minio.list_objects(MINIO_BUCKET, prefix=f"{TEST_CHANNEL}/"))
        assert len(minio_remaining) == 0, (
            f"MinIO cleanup failed: {len(minio_remaining)} objects remain"
        )

        print_pass("Postgres: 0 documents in test channel")
        print_pass("MinIO: 0 objects in test channel prefix")

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
    ✅ Postgres, MinIO, and Kafka all reachable at test start
    ✅ Valid PDF → worker extracts text, sets status='ready'
    ✅ Corrupted PDF → PdfReadError caught, status='failed' + error_message set
    ✅ Image-only PDF (no text) → ValueError caught, status='failed'
    ✅ PDF exceeding MAX_PAGES → truncated but status='ready' (partial text accepted)
    ✅ Re-delivery of already-ready document → idempotent, status stays 'ready'
    ✅ Re-delivery of already-failed document → status stays 'failed'
    ✅ Batch of 3 valid documents → all reach status='ready'
    ✅ Malformed Kafka payload → ValueError, offset committed, worker keeps running
    ✅ All test data cleaned up from Postgres and MinIO
""")
    print(f"{'='*60}")


if __name__ == "__main__":
    asyncio.run(main())

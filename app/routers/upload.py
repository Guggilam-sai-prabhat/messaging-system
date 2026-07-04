# app/routers/upload.py

"""
PDF upload endpoint.

Flow:
  1. Auth check (get_current_user)
  2. Membership check (is user in channel?)
  3. File validation (type, size, not empty, actually a PDF)
  4. Stream to MinIO + compute hash
  5. Insert DB row (status='processing')
  6. Emit Kafka event → async consumer handles extraction/indexing
  7. Return 202 + document metadata

─── Why async document processing? ──────────────────────────────
PDF text extraction, OCR, and embedding generation are CPU/IO-bound
and can take 5–30 seconds per document. Doing that work inside this
request handler blocks an entire HTTP worker for every concurrent
upload. Emitting a Kafka event and returning 202 immediately keeps
the handler fast and lets the heavy lifting scale independently.

─── Why Kafka over a simple task queue (Celery/Redis)? ──────────
  Durability    — Messages are written to disk and replicated.
                  A broker restart loses nothing.
  Replayability — If the consumer crashes mid-processing, the Kafka
                  offset hasn't been committed, so the event is
                  redelivered automatically on restart. With
                  Celery/Redis the job vanishes the moment it's
                  dequeued and before it's ack'd.
  Decoupling    — New consumers (thumbnail generator, virus scanner)
                  subscribe without any change here.

─── Kafka failure handling ───────────────────────────────────────
produce_document_event retries up to 3 times internally (0.5s/1.0s
backoff) for transient errors. By the time an exception reaches this
handler, either:
  a) All 3 retries were exhausted (genuine broker problem), or
  b) The circuit breaker was open (sustained outage)

In both cases we log at WARNING and return 202 anyway. The document
row is already committed to the DB at status='processing'. The
APScheduler reconciliation job (runs every 5 minutes) will find it
and re-enqueue it. The user polls GET /documents/{id}/status and
eventually sees 'completed' — they never need to know a retry happened.

We NEVER roll back the DB insert on Kafka failure. The row in DB
is safe and idempotent. Kafka failure is the only partial-failure
mode and it is fully recoverable.

─── Validation order ─────────────────────────────────────────────
Membership is checked BEFORE reading the file body. If the user
isn't in the channel we reject immediately without wasting I/O on
a file we'll discard anyway.

─── Why 202 and not 201? ─────────────────────────────────────────
201 = "resource fully created and ready."
202 = "accepted for processing." Since the document isn't queryable
until the consumer finishes, 202 is semantically correct. Clients
should poll GET /documents/{id}/status to know when it's ready.
"""

import uuid
import logging
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File
from sqlalchemy import text

from app.core.auth import get_current_user
from app.core.kafka_producer import kafka_producer, KafkaProduceError
from app.dependencies import membership_service, storage_service
from app.db.database import database
from app.services.storage_service import StorageError

logger = logging.getLogger("upload")

router = APIRouter(prefix="/channels", tags=["uploads"])

MAX_FILE_SIZE = 10 * 1024 * 1024  # 10 MB
ALLOWED_CONTENT_TYPES = {"application/pdf"}
PDF_MAGIC_BYTES = b"%PDF"


# ── Helpers ───────────────────────────────────────────────────

async def _check_membership(channel_id: str, user_id: str) -> None:
    """Verify user belongs to channel. Raises 403 if not."""
    members = await membership_service.get_members(channel_id)
    if user_id not in members:
        raise HTTPException(
            status_code=403,
            detail={
                "error": "NOT_CHANNEL_MEMBER",
                "message": "You must be a member of this channel to upload.",
            },
        )


def _validate_content_type(upload: UploadFile) -> None:
    """Check declared MIME type. Belt — the magic byte check is suspenders.

    Some clients (browsers, Swagger UI) send application/octet-stream even
    for valid PDFs. We warn but don't reject here; _validate_pdf_magic is
    the authoritative gate.
    """
    if upload.content_type not in ALLOWED_CONTENT_TYPES:
        logger.warning(
            f"Upload content_type={upload.content_type!r} is not application/pdf "
            f"— proceeding to magic byte check."
        )


async def _validate_pdf_magic(upload: UploadFile) -> None:
    """Read first 4 bytes to confirm it's actually a PDF.

    Clients can lie about Content-Type. A renamed .exe with
    content_type=application/pdf passes the MIME check but fails
    here. We read 4 bytes, check, then seek back so the storage
    service receives the full file.
    """
    # Read directly from the underlying SpooledTemporaryFile.
    # upload.read() can return multipart framing bytes in some clients
    # (e.g. Swagger UI); upload.file is always the raw file payload.
    upload.file.seek(0)
    header = upload.file.read(4)
    upload.file.seek(0)

    logger.debug(
        f"_validate_pdf_magic: header={header!r} content_type={upload.content_type!r}"
    )

    if len(header) < 4:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "EMPTY_FILE",
                "message": "File is empty or too small to be a valid PDF.",
            },
        )
    if header != PDF_MAGIC_BYTES:
        raise HTTPException(
            status_code=415,
            detail={
                "error": "INVALID_PDF",
                "message": f"File does not appear to be a valid PDF "
                           f"(got header {header!r}, expected %PDF).",
            },
        )


async def _validate_file_size(upload: UploadFile) -> None:
    """Reject files over 10MB.

    UploadFile.size may be None if the client didn't send
    Content-Length (chunked transfer). In that case we can't
    pre-check — the storage layer enforces the limit during
    streaming. When available, fail fast here.
    """
    if upload.size is not None and upload.size > MAX_FILE_SIZE:
        raise HTTPException(
            status_code=413,
            detail={
                "error": "FILE_TOO_LARGE",
                "message": f"Maximum file size is "
                           f"{MAX_FILE_SIZE // (1024 * 1024)}MB. "
                           f"Got {upload.size / (1024 * 1024):.1f}MB.",
            },
        )
    if upload.size is not None and upload.size == 0:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "EMPTY_FILE",
                "message": "File is empty.",
            },
        )


# ── Endpoint ──────────────────────────────────────────────────

@router.post("/{channel_id}/upload", status_code=202)
async def upload_document(
    channel_id: str,
    user_id: str = Depends(get_current_user),
    file: UploadFile = File(...),
):
    """Upload a PDF to a channel.

    Returns 202 with document metadata. The document starts at
    status='processing'. Poll GET /documents/{documentId}/status
    until status becomes 'completed' or 'failed'.
    """
    # ── 1. Membership check (cheap — hits Redis) ──────────
    await _check_membership(channel_id, user_id)

    # ── 2. File validation ────────────────────────────────
    _validate_content_type(file)
    await _validate_file_size(file)
    await _validate_pdf_magic(file)

    # ── 3. Upload to MinIO ────────────────────────────────
    document_id = str(uuid.uuid4())

    try:
        object_key, size_bytes, sha256 = await storage_service.save_file(
            channel_id=channel_id,
            document_id=document_id,
            file_obj=file.file,
        )
    except StorageError as e:
        logger.error(f"MinIO upload failed: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                "error": "STORAGE_ERROR",
                "message": "Failed to store file. Please retry.",
            },
        )

    # ── 4. Size check (streaming case) ────────────────────
    # Content-Length not sent → we only know the real size after
    # writing. Clean up the object if it's over the limit.
    if size_bytes > MAX_FILE_SIZE:
        await storage_service.delete_file(object_key)
        raise HTTPException(
            status_code=413,
            detail={
                "error": "FILE_TOO_LARGE",
                "message": f"Maximum file size is "
                           f"{MAX_FILE_SIZE // (1024 * 1024)}MB.",
            },
        )

    if size_bytes == 0:
        await storage_service.delete_file(object_key)
        raise HTTPException(
            status_code=400,
            detail={
                "error": "EMPTY_FILE",
                "message": "File is empty.",
            },
        )

    # ── 5. Persist metadata ───────────────────────────────
    sanitized_name = (file.filename or "untitled.pdf")[:512]

    try:
        async with database.get_session() as session:
            await session.execute(
                text("""
                    INSERT INTO documents
                        (document_id, channel_id, file_name,
                         content_type, file_size_bytes, uploaded_by,
                         status, storage_path, sha256_hash)
                    VALUES
                        (:doc_id, :ch_id, :fname,
                         :ctype, :fsize, :uid,
                         'processing', :path, :hash)
                """),
                {
                    "doc_id": document_id,
                    "ch_id": channel_id,
                    "fname": sanitized_name,
                    "ctype": "application/pdf",
                    "fsize": size_bytes,
                    "uid": user_id,
                    "path": object_key,
                    "hash": sha256,
                },
            )
            await session.commit()
    except Exception as e:
        error_str = str(e).lower()
        if "unique" in error_str or "duplicate" in error_str:
            await storage_service.delete_file(object_key)
            raise HTTPException(
                status_code=409,
                detail={
                    "error": "DUPLICATE_FILE",
                    "message": "This file has already been uploaded "
                               "to this channel.",
                },
            )
        await storage_service.delete_file(object_key)
        logger.error(f"DB insert failed for document {document_id}: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                "error": "DATABASE_ERROR",
                "message": "Failed to record upload. Please retry.",
            },
        )

    # ── 6. Emit Kafka event for async processing ──────────
    #
    # DB commit must succeed before this call. The consumer will
    # immediately SELECT the document row by ID — if Kafka fires
    # before the commit, the consumer finds nothing.
    #
    # produce_document_event handles its own retries (3 attempts,
    # 0.5s/1.0s backoff). By the time an exception reaches here,
    # either all retries failed or the circuit breaker was open —
    # both mean a genuine broker problem, not a transient blip.
    #
    # Recovery: the APScheduler reconciliation job runs every 5
    # minutes and re-enqueues any document stuck at 'processing'
    # for more than 10 minutes. The user polling
    # GET /documents/{documentId}/status will eventually see
    # 'completed' without ever knowing a retry happened.
    try:
        await kafka_producer.produce_document_event(
            {
                "documentId": document_id,
                "channelId": channel_id,
                "storagePath": object_key,
                "fileName": sanitized_name,
                "uploadedBy": user_id,
            }
        )
    except KafkaProduceError as e:
        # Upload succeeded — don't punish the user for a broker issue.
        # WARNING not ERROR: the upload itself is fine, only the async
        # processing trigger failed. Do NOT re-raise. Do NOT delete the
        # DB row.
        logger.warning(
            f"Kafka publish failed for document {document_id} after all "
            f"retries: {e}. Reconciliation job will re-enqueue."
        )

    return {
        "documentId": document_id,
        "channelId": channel_id,
        "fileName": sanitized_name,
        "fileSizeBytes": size_bytes,
        "status": "processing",
    }
# routers/upload.py

"""
PDF upload endpoint.

Flow:
  1. Auth check (get_current_user)
  2. Membership check (is user in channel?)
  3. File validation (type, size, not empty, actually a PDF)
  4. Stream to MinIO + compute hash
  5. Insert DB row (status=processing)
  6. Return 202 + document metadata
  7. (Later) Kafka event triggers async processing

Validation order matters:
  We check membership BEFORE reading the file body. If the
  user isn't in the channel, we reject immediately without
  wasting I/O on a file we'll throw away. Same principle as
  your channel routes — fail fast, fail cheap.

Why 202 and not 201?
  201 means "the resource is fully created and ready."
  202 means "accepted for processing." Since we return while
  processing is still pending, 202 is semantically correct.
  The client should poll GET /documents/{id} or listen for
  a WebSocket event to know when it's ready.
"""

import uuid
import logging
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File
from sqlalchemy import text

from app.core.auth import get_current_user
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
    """Check declared MIME type. Belt — the magic byte check is suspenders."""
    if upload.content_type not in ALLOWED_CONTENT_TYPES:
        raise HTTPException(
            status_code=415,
            detail={
                "error": "INVALID_FILE_TYPE",
                "message": f"Expected application/pdf, "
                           f"got {upload.content_type}.",
            },
        )


async def _validate_pdf_magic(upload: UploadFile) -> None:
    """Read first 4 bytes to confirm it's actually a PDF.

    Clients can lie about Content-Type. A renamed .exe with
    content_type=application/pdf passes the MIME check but
    fails here. We read 4 bytes, check, then seek back.
    """
    header = await upload.read(4)
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
                "message": "File does not appear to be a valid PDF "
                           "(missing %PDF header).",
            },
        )
    # Seek back so the storage service gets the full file.
    await upload.seek(0)


async def _validate_file_size(upload: UploadFile) -> None:
    """Reject files over 10MB.

    UploadFile.size may be None if the client didn't send
    Content-Length (chunked transfer). In that case we can't
    pre-check — the storage layer will enforce the limit
    during streaming. But when available, fail fast.
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

    Returns 202 with document metadata. The document starts
    in 'processing' status — a background consumer handles
    text extraction, indexing, etc.
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
    # If Content-Length wasn't sent, we didn't know size
    # until after writing. Check now and clean up if too big.
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
        # Unknown DB error — clean up the orphaned object
        await storage_service.delete_file(object_key)
        logger.error(f"DB insert failed for document {document_id}: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                "error": "DATABASE_ERROR",
                "message": "Failed to record upload. Please retry.",
            },
        )

    # ── 6. TODO: Emit Kafka event for async processing ────
    # await kafka_producer.produce_message({
    #     "type": "document.uploaded",
    #     "documentId": document_id,
    #     "channelId": channel_id,
    #     "storagePath": object_key,
    #     "uploadedBy": user_id,
    # })

    return {
        "documentId": document_id,
        "channelId": channel_id,
        "fileName": sanitized_name,
        "fileSizeBytes": size_bytes,
        "status": "processing",
    }
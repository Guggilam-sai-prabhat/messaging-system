# app/routers/documents.py

"""
Document status endpoint.

GET /documents/{document_id}

Clients poll this endpoint after a successful POST /upload (202)
to track processing progress. The client should poll until status
is 'ready', 'embedding_failed', or 'failed', then stop.

─── Polling contract ─────────────────────────────────────────────
  Recommended client strategy:
    - Poll every 3 seconds for the first 30 seconds
    - Back off to every 10 seconds after that
    - Stop polling when status is 'ready', 'embedding_failed', or 'failed'
    - Surface a user-facing error if status stays 'processing'
      for more than 15 minutes (the reconciliation job runs every
      5 minutes, so 15 minutes means two full recovery cycles have
      passed and something is genuinely wrong)

  Status values:
    'processing'        — uploaded, Kafka event emitted (or pending retry),
                          consumer has not yet finished
    'ready'             — text extracted successfully; document is
                          readable. Chunks/embeddings may still be in
                          progress or may have failed (see error_message)
    'embedding_failed'  — text extraction succeeded but chunking/embedding
                          did not; document is readable but not searchable
    'failed'            — consumer gave up after its own retries; the
                          document could not be processed (corrupt PDF,
                          unsupported encoding, etc.)

─── Auth ─────────────────────────────────────────────────────────
  Requires a valid user token (get_current_user). The endpoint
  also verifies that the requesting user is a member of the
  document's channel — users should not be able to poll status
  for documents in channels they don't belong to.

─── Why not return the full document? ────────────────────────────
  This endpoint is deliberately minimal — just enough for the
  client to know whether to keep polling. A separate
  GET /channels/{channel_id}/documents endpoint can return the
  full list with metadata once you need it.
"""

import logging
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import text

from app.core.auth import get_current_user
from app.dependencies import membership_service
from app.db.database import database

logger = logging.getLogger("documents")

router = APIRouter(prefix="/documents", tags=["documents"])


@router.get("/{document_id}/status")
async def get_document_status(
    document_id: str,
    user_id: str = Depends(get_current_user),
):
    """Poll the processing status of an uploaded document.

    Returns the current status and basic metadata. The client
    should keep polling while status='processing'.

    Raises:
        404 — document_id does not exist
        403 — requesting user is not a member of the document's channel
    """
    # ── 1. Fetch document from DB ─────────────────────────
    # Single query — no joins needed, documents table has channel_id.
    try:
        async with database.get_session() as session:
            result = await session.execute(
                text("""
                    SELECT
                        document_id,
                        channel_id,
                        file_name,
                        file_size_bytes,
                        status,
                        uploaded_by,
                        created_at
                    FROM documents
                    WHERE document_id = :doc_id
                """),
                {"doc_id": document_id},
            )
            row = result.fetchone()
    except Exception as e:
        logger.error(f"DB error fetching document {document_id}: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                "error": "DATABASE_ERROR",
                "message": "Failed to fetch document status.",
            },
        )

    if row is None:
        raise HTTPException(
            status_code=404,
            detail={
                "error": "NOT_FOUND",
                "message": f"Document '{document_id}' not found.",
            },
        )

    # ── 2. Membership check ───────────────────────────────
    # Don't reveal that a document exists in a channel the user
    # can't see — return 403, not 404, so the caller knows their
    # auth is the issue rather than the document being missing.
    members = await membership_service.get_members(row.channel_id)
    if user_id not in members:
        raise HTTPException(
            status_code=403,
            detail={
                "error": "NOT_CHANNEL_MEMBER",
                "message": "You don't have access to this document.",
            },
        )

    # ── 3. Return status ──────────────────────────────────
    return {
        "documentId": row.document_id,
        "channelId": row.channel_id,
        "fileName": row.file_name,
        "fileSizeBytes": row.file_size_bytes,
        "status": row.status,           # 'processing' | 'ready' | 'embedding_failed' | 'failed'
        "uploadedBy": row.uploaded_by,
        "createdAt": row.created_at.isoformat(),
    }
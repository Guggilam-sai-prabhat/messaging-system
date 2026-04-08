"""
Channel HTTP endpoints.

Routes are THIN — parse request, call service, map errors to HTTP.

No business logic, no direct DB access, no Redis calls.
The same logic is needed from WebSocket handlers, background
tasks, and tests — it lives in the service layer.
"""

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from app.dependencies import channel_service
from app.core.auth import get_current_user
from app.services.channel_service import (
    ChannelServiceError,
    ChannelNotFoundError,
    AlreadyMemberError,
    NotMemberError,
    OwnerCannotLeaveError,
)

router = APIRouter(prefix="/channels", tags=["channels"])


# ── Request bodies ────────────────────────────────────────────

class CreateChannelRequest(BaseModel):
    name: str = Field(
        ...,
        min_length=1,
        max_length=256,
        description="Channel name.",
    )
    description: str | None = Field(
        None,
        max_length=4096,
        description="Optional channel description.",
    )


# ── Error mapping ─────────────────────────────────────────────

ERROR_STATUS_MAP = {
    "CHANNEL_NOT_FOUND": 404,
    "CHANNEL_DELETED": 410,
    "ALREADY_MEMBER": 409,
    "NOT_MEMBER": 404,
    "OWNER_CANNOT_LEAVE": 403,
    "NOT_OWNER": 403,
}


def _raise_http(err: ChannelServiceError) -> None:
    status = ERROR_STATUS_MAP.get(err.code, 400)
    raise HTTPException(
        status_code=status,
        detail={"error": err.code, "message": err.message},
    )


# ── Endpoints ─────────────────────────────────────────────────

@router.post("", status_code=201)
async def create_channel(
    body: CreateChannelRequest,
    user_id: str = Depends(get_current_user),
):
    """Create a new channel. The creator becomes the owner."""
    try:
        return await channel_service.create_channel(
            name=body.name,
            created_by=user_id,
            description=body.description,
        )
    except ChannelServiceError as e:
        _raise_http(e)


@router.post("/{channel_id}/join")
async def join_channel(
    channel_id: str,
    user_id: str = Depends(get_current_user),
):
    """Join an existing channel.

    Returns 409 if already a member — the client should know
    whether its action changed state. If you prefer silent
    idempotency (Slack-style), change the service to return
    the existing membership instead of raising.
    """
    try:
        return await channel_service.join_channel(channel_id, user_id)
    except ChannelServiceError as e:
        _raise_http(e)


@router.post("/{channel_id}/leave")
async def leave_channel(
    channel_id: str,
    user_id: str = Depends(get_current_user),
):
    """Leave a channel. Returns 403 if the user is the owner."""
    try:
        return await channel_service.leave_channel(channel_id, user_id)
    except ChannelServiceError as e:
        _raise_http(e)


@router.get("")
async def list_channels(
    user_id: str = Depends(get_current_user),
):
    """List all channels the authenticated user belongs to.

    Sorted by join date (newest first). No pagination yet —
    fine for <100 channels. When needed, add keyset pagination
    with ?cursor=<joined_at>&limit=50. Don't use OFFSET.
    """
    return await channel_service.list_user_channels(user_id)


@router.get("/browse")
async def browse_channels(
    user_id: str = Depends(get_current_user),
):
    """List all active channels so new users can discover what to join.

    Returns every non-deleted channel with member count and an
    `isMember` flag so the client knows whether to show Join or Joined.
    """
    return await channel_service.browse_all_channels(user_id)


@router.get("/{channel_id}")
async def get_channel(
    channel_id: str,
    user_id: str = Depends(get_current_user),
):
    """Get channel metadata + member count."""
    try:
        return await channel_service.get_channel(channel_id)
    except ChannelServiceError as e:
        _raise_http(e)


@router.get("/{channel_id}/members")
async def get_channel_members(
    channel_id: str,
    user_id: str = Depends(get_current_user),
):
    """List all members of a channel with their roles."""
    try:
        return await channel_service.get_channel_members(channel_id)
    except ChannelServiceError as e:
        _raise_http(e)


@router.delete("/{channel_id}", status_code=200)
async def delete_channel(
    channel_id: str,
    user_id: str = Depends(get_current_user),
):
    """Soft-delete a channel. Only the owner can do this."""
    try:
        return await channel_service.delete_channel(
            channel_id, deleted_by=user_id
        )
    except ChannelServiceError as e:
        _raise_http(e)
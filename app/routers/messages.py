"""
Message History API — REST endpoints for fetching messages.

These endpoints serve the use case that WebSocket delivery
can't: loading messages you MISSED. When a user opens a
channel, reconnects after being offline, or scrolls up to
load older messages, these endpoints provide the data.

Why REST and not WebSocket?
  History loading is request/response — "give me messages
  in #general after timestamp X." That's a perfect fit for
  HTTP GET. WebSocket is for real-time push of NEW messages.
  Using both together:
    1. Client connects WebSocket (gets new messages live)
    2. Client calls GET /channels/{id}/messages (loads history)
    3. Client merges both into one timeline

Pagination strategy: cursor-based using created_at.
  Why not offset/limit?
    If new messages arrive while the user is paginating,
    offset-based pagination skips or duplicates messages.
    Cursor-based ("give me 50 messages before this timestamp")
    is stable regardless of new inserts.

  Why created_at and not message_id?
    Timestamps are naturally ordered and meaningful to the
    client. The client already knows "the oldest message I
    have was sent at 1711990000" — it sends that as the
    cursor. No need to track opaque IDs for pagination.
"""

import logging
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Query, HTTPException
from pydantic import BaseModel

from sqlalchemy import select, func, and_
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.database import database
from app.db.models import Message

logger = logging.getLogger("api.messages")
router = APIRouter(prefix="/channels", tags=["messages"])


# ── Response Models ───────────────────────────────────────────

class MessageResponse(BaseModel):
    """Single message in API response.

    Uses camelCase to match the WebSocket payload format.
    Client sees the same field names whether the message
    came via WebSocket (real-time) or REST (history).
    """
    messageId: str
    channelId: str
    senderId: str
    content: str
    timestamp: float
    correlationId: Optional[str] = None
    clientRequestId: Optional[str] = None

    class Config:
        from_attributes = True


class MessageHistoryResponse(BaseModel):
    """Paginated message history response."""
    messages: list[MessageResponse]
    count: int
    hasMore: bool
    # Cursor for next page — client sends this as "before"
    # to load the next older page
    nextCursor: Optional[float] = None


class ChannelStatsResponse(BaseModel):
    channelId: str
    totalMessages: int
    lastMessageAt: Optional[float] = None
    firstMessageAt: Optional[float] = None


# ── Helper ────────────────────────────────────────────────────

def row_to_response(row: Message) -> MessageResponse:
    """Convert a SQLAlchemy Message row to API response."""
    return MessageResponse(
        messageId=row.message_id,
        channelId=row.channel_id,
        senderId=row.sender_id,
        content=row.content,
        timestamp=row.created_at.timestamp(),
        correlationId=row.correlation_id,
        clientRequestId=row.client_request_id,
    )


# ── Endpoints ─────────────────────────────────────────────────

@router.get(
    "/{channel_id}/messages",
    response_model=MessageHistoryResponse,
)
async def get_channel_messages(
    channel_id: str,
    limit: int = Query(
        default=50, ge=1, le=100,
        description="Number of messages to return (max 100)",
    ),
    before: Optional[float] = Query(
        default=None,
        description=(
            "Unix timestamp cursor — return messages BEFORE "
            "this time. Used for scrolling up (loading older). "
            "Omit to get the newest messages."
        ),
    ),
    after: Optional[float] = Query(
        default=None,
        description=(
            "Unix timestamp cursor — return messages AFTER "
            "this time. Used for catching up (loading newer "
            "messages since last seen)."
        ),
    ),
):
    """Get message history for a channel.

    Usage patterns:

    1. Open a channel (load newest messages):
       GET /channels/general/messages?limit=50

    2. Scroll up (load older messages):
       GET /channels/general/messages?before=1711990000&limit=50

    3. Reconnect catch-up (load messages since disconnect):
       GET /channels/general/messages?after=1711989500&limit=50

    The response includes a nextCursor. Send it as "before"
    to load the next page of older messages.
    """
    async with database.get_session() as session:
        # Build query
        query = (
            select(Message)
            .where(Message.channel_id == channel_id)
        )

        if before is not None:
            before_dt = datetime.fromtimestamp(
                before, tz=timezone.utc
            )
            query = query.where(Message.created_at < before_dt)

        if after is not None:
            after_dt = datetime.fromtimestamp(
                after, tz=timezone.utc
            )
            query = query.where(Message.created_at > after_dt)

        # Order: newest first for "before" (scrolling up),
        # oldest first for "after" (catching up)
        if after is not None:
            query = query.order_by(Message.created_at.asc())
        else:
            query = query.order_by(Message.created_at.desc())

        # Fetch one extra to determine hasMore
        query = query.limit(limit + 1)

        result = await session.execute(query)
        rows = list(result.scalars().all())

        has_more = len(rows) > limit
        if has_more:
            rows = rows[:limit]

        # For "after" queries, we fetched in ASC order
        # but still return newest-last for consistent rendering
        # (client appends to bottom of chat)
        messages = [row_to_response(r) for r in rows]

        # For "before"/default queries (DESC order), reverse
        # so messages are chronological (oldest first in array)
        if after is None:
            messages.reverse()

        # Next cursor for pagination
        next_cursor = None
        if has_more and messages:
            # Oldest message in this batch = cursor for next page
            next_cursor = messages[0].timestamp

        return MessageHistoryResponse(
            messages=messages,
            count=len(messages),
            hasMore=has_more,
            nextCursor=next_cursor,
        )


@router.get(
    "/{channel_id}/messages/{message_id}",
    response_model=MessageResponse,
)
async def get_message_by_id(channel_id: str, message_id: str):
    """Get a single message by ID.

    Useful for deep-linking to a specific message,
    or verifying a message was persisted.
    """
    async with database.get_session() as session:
        query = select(Message).where(
            and_(
                Message.message_id == message_id,
                Message.channel_id == channel_id,
            )
        )
        result = await session.execute(query)
        row = result.scalar_one_or_none()

        if not row:
            raise HTTPException(
                status_code=404,
                detail=f"Message {message_id} not found in channel {channel_id}",
            )

        return row_to_response(row)


@router.get(
    "/{channel_id}/stats",
    response_model=ChannelStatsResponse,
)
async def get_channel_stats(channel_id: str):
    """Get stats for a channel — message count, time range."""
    async with database.get_session() as session:
        query = select(
            func.count(Message.message_id),
            func.min(Message.created_at),
            func.max(Message.created_at),
        ).where(Message.channel_id == channel_id)

        result = await session.execute(query)
        total, first_at, last_at = result.one()

        return ChannelStatsResponse(
            channelId=channel_id,
            totalMessages=total,
            firstMessageAt=first_at.timestamp() if first_at else None,
            lastMessageAt=last_at.timestamp() if last_at else None,
        )
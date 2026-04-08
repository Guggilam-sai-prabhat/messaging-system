"""
Message History Service — queries PostgreSQL for chat history.

Extracted into its own module so the WebSocket manager stays
thin. The manager handles protocol, this handles data.

All queries use the indexes we created:
  idx_messages_channel_time → channel history + pagination
  idx_messages_sender_time  → user's message history
"""

import logging
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import select, func, and_

from app.db.database import database
from app.db.models import Message, User

logger = logging.getLogger("history")

# Max messages per request (prevent abuse)
MAX_LIMIT = 100
DEFAULT_LIMIT = 50


class MessageHistoryService:

    async def get_channel_messages(
        self,
        channel_id: str,
        limit: int = DEFAULT_LIMIT,
        before: Optional[float] = None,
        after: Optional[float] = None,
    ) -> dict:
        """Fetch messages for a channel with cursor pagination.

        Pagination modes:
          - No cursor: newest messages (user opens channel)
          - before: older messages (user scrolls up)
          - after: newer messages (user reconnects, catches up)

        Returns dict ready to send over WebSocket.
        """
        if limit > MAX_LIMIT:
            limit = MAX_LIMIT
        if limit < 1:
            limit = 1

        async with database.get_session() as session:
            query = (
                select(
                    Message.message_id,
                    Message.channel_id,
                    Message.sender_id,
                    Message.content,
                    Message.created_at,
                    Message.correlation_id,
                    Message.client_request_id,
                    User.display_name,
                )
                .join(User, User.user_id == Message.sender_id)
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

            # Order: DESC for "before"/default, ASC for "after"
            if after is not None:
                query = query.order_by(Message.created_at.asc())
            else:
                query = query.order_by(Message.created_at.desc())

            # Fetch one extra to determine hasMore
            query = query.limit(limit + 1)

            result = await session.execute(query)
            rows = list(result.all())

        has_more = len(rows) > limit
        if has_more:
            rows = rows[:limit]

        # Convert to dicts
        messages = [self._row_to_dict(r) for r in rows]

        # For DESC queries, reverse to chronological order
        if after is None:
            messages.reverse()

        # Next cursor
        next_cursor = None
        if has_more and messages:
            next_cursor = messages[0]["timestamp"]

        return {
            "messages": messages,
            "count": len(messages),
            "hasMore": has_more,
            "nextCursor": next_cursor,
        }

    async def get_message_by_id(
        self, channel_id: str, message_id: str
    ) -> Optional[dict]:
        """Fetch a single message."""
        async with database.get_session() as session:
            query = (
                select(
                    Message.message_id,
                    Message.channel_id,
                    Message.sender_id,
                    Message.content,
                    Message.created_at,
                    Message.correlation_id,
                    Message.client_request_id,
                    User.display_name,
                )
                .join(User, User.user_id == Message.sender_id)
                .where(
                    and_(
                        Message.message_id == message_id,
                        Message.channel_id == channel_id,
                    )
                )
            )
            result = await session.execute(query)
            row = result.one_or_none()

            if not row:
                return None

            return self._row_to_dict(row)

    async def get_channel_stats(self, channel_id: str) -> dict:
        """Message count and time range for a channel."""
        async with database.get_session() as session:
            query = select(
                func.count(Message.message_id),
                func.min(Message.created_at),
                func.max(Message.created_at),
            ).where(Message.channel_id == channel_id)

            result = await session.execute(query)
            total, first_at, last_at = result.one()

            return {
                "channelId": channel_id,
                "totalMessages": total,
                "firstMessageAt": first_at.timestamp() if first_at else None,
                "lastMessageAt": last_at.timestamp() if last_at else None,
            }

    def _row_to_dict(self, row) -> dict:
        """Convert to camelCase dict matching WebSocket format."""
        return {
            "messageId": row.message_id,
            "channelId": row.channel_id,
            "senderId": row.sender_id,
            "displayName": row.display_name,
            "content": row.content,
            "timestamp": row.created_at.timestamp(),
            "correlationId": row.correlation_id,
            "clientRequestId": row.client_request_id,
        }


# ── Module singleton ──────────────────────────────────────────
message_history = MessageHistoryService()
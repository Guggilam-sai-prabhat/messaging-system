"""
Channel Service — create, join, leave, list, delete.

Sits between HTTP routes and the data layer (DB + Redis).

Consistency model:
  DB is source of truth. Redis is a read cache.
  Writes go DB-first, then Redis. If Redis fails, data is
  still correct in DB. The membership service's get_members
  fallback detects the inconsistency on the next read and
  triggers a background rebuild.

  This is "write-through with fallback" — not as fast as
  Redis-first, but much easier to reason about in failures.

Why not put this in routes?
  Routes change when HTTP conventions change. Business logic
  shouldn't. The WebSocket handler, background tasks, and
  tests all need this logic too — it shouldn't live in an
  HTTP handler.
"""

import uuid
import logging
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import text, select, and_
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import Channel, ChannelMember, VALID_ROLES
from app.services.channel_membership_service import ChannelMembershipService

logger = logging.getLogger("channel.service")


# ─────────────────────────────────────────────────────────────
# Service-layer exceptions
# ─────────────────────────────────────────────────────────────
# Each carries a machine-readable `code` that routes map to
# HTTP status codes. This keeps HTTP concerns out of the service.

class ChannelServiceError(Exception):
    def __init__(self, message: str, code: str):
        self.message = message
        self.code = code
        super().__init__(message)


class ChannelNotFoundError(ChannelServiceError):
    def __init__(self, channel_id: str):
        super().__init__(
            f"Channel {channel_id} not found",
            "CHANNEL_NOT_FOUND",
        )


class AlreadyMemberError(ChannelServiceError):
    def __init__(self, channel_id: str, user_id: str):
        super().__init__(
            f"User {user_id} is already a member of {channel_id}",
            "ALREADY_MEMBER",
        )


class NotMemberError(ChannelServiceError):
    def __init__(self, channel_id: str, user_id: str):
        super().__init__(
            f"User {user_id} is not a member of {channel_id}",
            "NOT_MEMBER",
        )


class OwnerCannotLeaveError(ChannelServiceError):
    def __init__(self, channel_id: str):
        super().__init__(
            f"Owner cannot leave channel {channel_id}. "
            f"Transfer ownership or delete the channel.",
            "OWNER_CANNOT_LEAVE",
        )


class ChannelDeletedError(ChannelServiceError):
    def __init__(self, channel_id: str):
        super().__init__(
            f"Channel {channel_id} has been deleted",
            "CHANNEL_DELETED",
        )


# ─────────────────────────────────────────────────────────────
# Service
# ─────────────────────────────────────────────────────────────

class ChannelService:
    """
    Stateless service — all state lives in DB and Redis.

    Constructor args:
      membership:      ChannelMembershipService (wraps registry + Redis)
      session_factory:  async context manager that yields AsyncSession
    """

    def __init__(
        self,
        membership: ChannelMembershipService,
        session_factory,
    ):
        self._membership = membership
        self._session_factory = session_factory

    # ─────────────────────────────────────────────────────────
    # Create channel
    # ─────────────────────────────────────────────────────────

    async def create_channel(
        self,
        name: str,
        created_by: str,
        description: Optional[str] = None,
    ) -> dict:
        """Create a channel and add the creator as owner.

        Single transaction: if either INSERT fails, both roll back.
        The creator never sees a channel they don't own.

        After DB commit, we update Redis. If Redis fails, the
        membership service's fallback will fix it on first read.
        """
        channel_id = str(uuid.uuid4())
        now = datetime.now(timezone.utc)

        async with self._session_factory() as session:
            channel = Channel(
                channel_id=channel_id,
                name=name.strip(),
                description=description,
                created_by=created_by,
                created_at=now,
            )
            session.add(channel)

            member = ChannelMember(
                channel_id=channel_id,
                user_id=created_by,
                role="owner",
                joined_at=now,
            )
            session.add(member)

            await session.commit()

        # ── Update Redis ─────────────────────────────────────
        # Call registry directly — the DB write already happened,
        # so membership.join_channel would do a redundant INSERT
        # (harmless but wasteful).
        try:
            await self._membership._registry.join_channel(
                channel_id, created_by
            )
        except Exception as e:
            logger.error(
                f"Redis join failed after channel create "
                f"channel={channel_id} user={created_by}: {e}"
            )

        logger.info(
            f"Channel created: id={channel_id} name={name!r} "
            f"by={created_by}"
        )

        return {
            "channelId": channel_id,
            "name": name.strip(),
            "description": description,
            "createdBy": created_by,
            "createdAt": now.isoformat(),
            "role": "owner",
        }

    # ─────────────────────────────────────────────────────────
    # Join channel
    # ─────────────────────────────────────────────────────────

    async def join_channel(
        self,
        channel_id: str,
        user_id: str,
    ) -> dict:
        """Add a user to an existing channel.

        Race condition handling:
          Two concurrent joins for the same (channel, user) could
          both pass the "not already a member" check. The composite
          PK catches the second INSERT via IntegrityError, which we
          convert to AlreadyMemberError.
        """
        async with self._session_factory() as session:
            await self._get_active_channel(session, channel_id)

            # Check existing membership
            existing = await session.execute(
                select(ChannelMember).where(
                    and_(
                        ChannelMember.channel_id == channel_id,
                        ChannelMember.user_id == user_id,
                    )
                )
            )
            if existing.scalar_one_or_none() is not None:
                raise AlreadyMemberError(channel_id, user_id)

            member = ChannelMember(
                channel_id=channel_id,
                user_id=user_id,
                role="member",
                joined_at=datetime.now(timezone.utc),
            )
            session.add(member)

            try:
                await session.commit()
            except IntegrityError:
                # Race condition: another request inserted first.
                await session.rollback()
                raise AlreadyMemberError(channel_id, user_id)

        # ── Update Redis ─────────────────────────────────────
        try:
            await self._membership._registry.join_channel(
                channel_id, user_id
            )
        except Exception as e:
            logger.error(
                f"Redis join failed channel={channel_id} "
                f"user={user_id}: {e}"
            )

        logger.info(f"Joined: user={user_id} channel={channel_id}")
        return {
            "channelId": channel_id,
            "userId": user_id,
            "role": "member",
        }

    # ─────────────────────────────────────────────────────────
    # Leave channel
    # ─────────────────────────────────────────────────────────

    async def leave_channel(
        self,
        channel_id: str,
        user_id: str,
    ) -> dict:
        """Remove a user from a channel voluntarily.

        Owner restriction:
          The owner can't leave — prevents orphan channels with
          no one who can manage them. Must transfer ownership or
          delete the channel instead.

        WebSocket behavior:
          We do NOT disconnect the user's WebSocket. They might
          be in other channels. The delivery service stops routing
          messages to them for this channel because they're no
          longer in the Redis member set.
        """
        async with self._session_factory() as session:
            await self._get_active_channel(session, channel_id)

            result = await session.execute(
                select(ChannelMember).where(
                    and_(
                        ChannelMember.channel_id == channel_id,
                        ChannelMember.user_id == user_id,
                    )
                )
            )
            membership = result.scalar_one_or_none()

            if membership is None:
                raise NotMemberError(channel_id, user_id)

            if membership.role == "owner":
                raise OwnerCannotLeaveError(channel_id)

            await session.delete(membership)
            await session.commit()

        # ── Update Redis ─────────────────────────────────────
        try:
            await self._membership._registry.leave_channel(
                channel_id, user_id
            )
        except Exception as e:
            logger.error(
                f"Redis leave failed channel={channel_id} "
                f"user={user_id}: {e}"
            )

        logger.info(f"Left: user={user_id} channel={channel_id}")
        return {
            "channelId": channel_id,
            "userId": user_id,
            "removed": True,
        }

    # ─────────────────────────────────────────────────────────
    # List user's channels
    # ─────────────────────────────────────────────────────────

    async def list_user_channels(self, user_id: str) -> list[dict]:
        """Return all active channels a user belongs to.

        Always reads from DB, not Redis. Redis stores membership
        as sets (channel→users, user→channels) but not metadata
        (name, description, role). A single DB JOIN gets everything.

        For a listing endpoint — not on the message delivery hot
        path — one DB round-trip is fine.
        """
        async with self._session_factory() as session:
            rows = await session.execute(
                text("""
                    SELECT
                        c.channel_id,
                        c.name,
                        c.description,
                        c.created_by,
                        c.created_at,
                        cm.role,
                        cm.joined_at
                    FROM channel_members cm
                    JOIN channels c ON c.channel_id = cm.channel_id
                    WHERE cm.user_id = :uid
                      AND c.is_deleted = false
                    ORDER BY cm.joined_at DESC
                """),
                {"uid": user_id},
            )
            return [
                {
                    "channelId": r.channel_id,
                    "name": r.name,
                    "description": r.description,
                    "createdBy": r.created_by,
                    "createdAt": r.created_at.isoformat(),
                    "role": r.role,
                    "joinedAt": r.joined_at.isoformat(),
                }
                for r in rows.fetchall()
            ]

    # ─────────────────────────────────────────────────────────
    # Browse all channels (discovery for new users)
    # ─────────────────────────────────────────────────────────

    async def browse_all_channels(self, user_id: str) -> list[dict]:
        """Return all active channels so new users can discover and join.

        Includes member count and whether the calling user is already
        a member, so the client can show a Join/Joined button.
        """
        async with self._session_factory() as session:
            rows = await session.execute(
                text("""
                    SELECT
                        c.channel_id,
                        c.name,
                        c.description,
                        c.created_by,
                        c.created_at,
                        COUNT(cm.user_id) AS member_count,
                        BOOL_OR(cm.user_id = :uid) AS is_member
                    FROM channels c
                    LEFT JOIN channel_members cm
                        ON cm.channel_id = c.channel_id
                    WHERE c.is_deleted = false
                    GROUP BY c.channel_id, c.name, c.description,
                             c.created_by, c.created_at
                    ORDER BY c.created_at DESC
                """),
                {"uid": user_id},
            )
            return [
                {
                    "channelId": r.channel_id,
                    "name": r.name,
                    "description": r.description,
                    "createdBy": r.created_by,
                    "createdAt": r.created_at.isoformat(),
                    "memberCount": r.member_count,
                    "isMember": r.is_member,
                }
                for r in rows.fetchall()
            ]

    # ─────────────────────────────────────────────────────────
    # Get single channel info
    # ─────────────────────────────────────────────────────────

    async def get_channel(self, channel_id: str) -> dict:
        """Fetch channel metadata + member count from DB."""
        async with self._session_factory() as session:
            channel = await self._get_active_channel(
                session, channel_id
            )
            count_result = await session.execute(
                text(
                    "SELECT COUNT(*) FROM channel_members "
                    "WHERE channel_id = :cid"
                ),
                {"cid": channel_id},
            )
            member_count = count_result.scalar()

        return {
            "channelId": channel.channel_id,
            "name": channel.name,
            "description": channel.description,
            "createdBy": channel.created_by,
            "createdAt": channel.created_at.isoformat(),
            "memberCount": member_count,
        }

    # ─────────────────────────────────────────────────────────
    # Get channel members (for routes)
    # ─────────────────────────────────────────────────────────

    async def get_channel_members(self, channel_id: str) -> list[dict]:
        """Return all members of a channel with roles.

        Uses DB (not Redis) because we need role information
        that Redis doesn't store.
        """
        async with self._session_factory() as session:
            await self._get_active_channel(session, channel_id)

            rows = await session.execute(
                text("""
                    SELECT user_id, role, joined_at
                    FROM channel_members
                    WHERE channel_id = :cid
                    ORDER BY joined_at ASC
                """),
                {"cid": channel_id},
            )
            return [
                {
                    "userId": r.user_id,
                    "role": r.role,
                    "joinedAt": r.joined_at.isoformat(),
                }
                for r in rows.fetchall()
            ]

    # ─────────────────────────────────────────────────────────
    # Soft-delete channel
    # ─────────────────────────────────────────────────────────

    async def delete_channel(
        self,
        channel_id: str,
        deleted_by: str,
    ) -> dict:
        """Soft-delete a channel. Only the owner can do this.

        We don't remove channel_members rows — they become
        invisible because listings filter on is_deleted = false.
        Keeping them means:
          - Undo is trivial (flip is_deleted back)
          - Message history queries still work
          - Audit trail is intact

        Redis cleanup removes the member set so the delivery
        service stops routing messages there.
        """
        async with self._session_factory() as session:
            channel = await self._get_active_channel(
                session, channel_id
            )

            # Authorization: only the owner can delete
            owner_row = await session.execute(
                select(ChannelMember).where(
                    and_(
                        ChannelMember.channel_id == channel_id,
                        ChannelMember.user_id == deleted_by,
                        ChannelMember.role == "owner",
                    )
                )
            )
            if owner_row.scalar_one_or_none() is None:
                raise ChannelServiceError(
                    f"User {deleted_by} is not the owner of {channel_id}",
                    "NOT_OWNER",
                )

            channel.is_deleted = True
            channel.deleted_at = datetime.now(timezone.utc)
            await session.commit()

        # ── Clean up Redis ───────────────────────────────────
        await self._cleanup_redis_for_channel(channel_id)

        logger.info(
            f"Channel deleted: id={channel_id} by={deleted_by}"
        )
        return {"channelId": channel_id, "deleted": True}

    # ─────────────────────────────────────────────────────────
    # Validate message sender (called by ingest pipeline)
    # ─────────────────────────────────────────────────────────

    async def validate_sender(
        self,
        channel_id: str,
        sender_id: str,
    ) -> bool:
        """Check that a channel exists and the sender is a member.

        This is the integration point between the channel system
        and the message ingest pipeline. The ingest service calls
        this instead of going directly to the registry.

        Uses the membership service's get_members (Redis with DB
        fallback) for the membership check, and a DB query for
        the channel existence check.

        Returns True if valid. Raises ChannelServiceError if not.
        """
        # ── Channel existence ────────────────────────────────
        async with self._session_factory() as session:
            result = await session.execute(
                select(Channel.channel_id).where(
                    and_(
                        Channel.channel_id == channel_id,
                        Channel.is_deleted == False,  # noqa: E712
                    )
                )
            )
            if result.scalar_one_or_none() is None:
                raise ChannelNotFoundError(channel_id)

        # ── Membership check (Redis → DB fallback) ───────────
        members = await self._membership.get_members(channel_id)
        if sender_id not in members:
            raise NotMemberError(channel_id, sender_id)

        return True

    # ─────────────────────────────────────────────────────────
    # Internal helpers
    # ─────────────────────────────────────────────────────────

    async def _get_active_channel(
        self, session: AsyncSession, channel_id: str
    ) -> Channel:
        """Load a channel, raising if it doesn't exist or is deleted."""
        result = await session.execute(
            select(Channel).where(
                and_(
                    Channel.channel_id == channel_id,
                    Channel.is_deleted == False,  # noqa: E712
                )
            )
        )
        channel = result.scalar_one_or_none()
        if channel is None:
            raise ChannelNotFoundError(channel_id)
        return channel

    async def _cleanup_redis_for_channel(
        self, channel_id: str
    ) -> None:
        """Remove all Redis state for a deleted channel."""
        try:
            from app.core.redis_client import redis_client

            members_key = f"channel:{channel_id}:members"
            member_ids = await redis_client.redis.smembers(members_key)

            if member_ids:
                pipe = redis_client.redis.pipeline()
                for uid in member_ids:
                    pipe.srem(f"user:{uid}:channels", channel_id)
                pipe.delete(members_key)
                await pipe.execute()
            else:
                # No members in Redis (might already be cleaned),
                # but still delete the key to be safe.
                await redis_client.redis.delete(members_key)
        except Exception as e:
            logger.error(
                f"Redis cleanup failed for channel={channel_id}: {e}"
            )
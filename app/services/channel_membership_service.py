"""
Channel Membership Service — safety nets around Redis SET membership.

Wraps ConnectionRegistry's channel methods with three layers:

  1. Cache rebuild     — re-populate Redis from PostgreSQL on demand
                         (Redis restart, cold start, sync drift)
  2. Admin kick        — remove a user from a channel atomically,
                         clean up both Redis directions, and push a
                         "channel.kicked" event down their WebSocket
  3. Inconsistency     — if get_channel_members returns empty for a
     fallback            channel that exists in the DB, fall back to
                         the DB and schedule a background re-sync

The service does NOT replace registry.join_channel /
registry.leave_channel for the normal websocket join flow.
It sits above them and is called by:
  - Admin / moderation endpoints  (kick)
  - The delivery service           (get_members with fallback)
  - A startup / periodic task      (rebuild)

Redis key layout (unchanged from registry):
  channel:{id}:members   SET of user_ids
  user:{id}:channels     SET of channel_ids

DB source of truth:
  channel_members table  (channel_id, user_id, joined_at)
  — you need this table if it doesn't already exist; see
    the migration note at the bottom of this file.
"""

import json
import logging
import time
import asyncio
from typing import Optional

from sqlalchemy import text

from app.core.connection_registry import ConnectionRegistry
from app.core.redis_client import redis_client
from app.db.database import database

logger = logging.getLogger("membership")

# How long a "channel is empty in Redis" result can be
# considered trustworthy before we suspect stale data.
EMPTY_TRUST_TTL = 5.0          # seconds
# Lock TTL for rebuild — prevents thundering-herd rebuilds
# when Redis is cold and many requests arrive at once.
REBUILD_LOCK_TTL = 30          # seconds
# Kick event type sent to the WebSocket of the evicted user.
KICK_EVENT_TYPE = "channel.kicked"


class ChannelMembershipService:
    """
    Wraps registry channel methods with cache-safety and admin ops.

    Concurrency model:
      All async methods run on the event loop. The only shared
      in-process state is _rebuild_in_progress (a plain set).
      It gates the per-channel rebuild so two concurrent requests
      for the same empty channel don't both hit the DB. The Redis
      rebuild lock does the same across processes.
    """

    def __init__(self, registry: ConnectionRegistry):
        self._registry = registry
        # Tracks channels currently being rebuilt (this process).
        self._rebuild_in_progress: set[str] = set()

    # ─────────────────────────────────────────────────────────
    # 1. Cache rebuild
    # ─────────────────────────────────────────────────────────

    async def rebuild_channel(self, channel_id: str) -> int:
        """Re-populate Redis membership for one channel from the DB.

        Safe to call at any time — uses a Redis lock to prevent
        concurrent rebuilds of the same channel across processes.
        Existing members are preserved (SADD is idempotent);
        members removed from the DB since last sync are pruned
        via a replace strategy (DEL + SADD in a pipeline).

        Returns the number of members written.

        When to call:
          - Startup, after Redis flush / restart
          - Triggered by the inconsistency fallback below
          - Admin "resync" endpoint
        """
        lock_key = f"lock:rebuild:{channel_id}"
        lock_val = str(time.time())

        # Acquire a short-lived Redis lock so only one process
        # rebuilds at a time. NX = only set if key doesn't exist.
        acquired = await redis_client.redis.set(
            lock_key, lock_val, ex=REBUILD_LOCK_TTL, nx=True
        )
        if not acquired:
            logger.debug(
                f"Rebuild for {channel_id} already in progress "
                f"(another process holds the lock)"
            )
            return 0

        try:
            return await self._do_rebuild(channel_id)
        finally:
            # Release lock only if we still own it (not expired).
            stored = await redis_client.redis.get(lock_key)
            if stored == lock_val:
                await redis_client.redis.delete(lock_key)

    async def _do_rebuild(self, channel_id: str) -> int:
        """Inner rebuild — runs while holding the Redis lock."""
        async with database.get_session() as session:
            rows = await session.execute(
                text(
                    "SELECT user_id FROM channel_members "
                    "WHERE channel_id = :cid"
                ),
                {"cid": channel_id},
            )
            user_ids = [r[0] for r in rows.fetchall()]

        if not user_ids:
            # Channel truly has no members in the DB.
            # Delete the Redis key so we don't serve stale data.
            await redis_client.redis.delete(
                f"channel:{channel_id}:members"
            )
            logger.info(
                f"Rebuild {channel_id}: 0 members in DB, key cleared"
            )
            return 0

        # Atomic replace: delete old set, write fresh one.
        # MULTI/EXEC (pipeline) makes these two ops atomic from
        # the perspective of other readers.
        pipe = redis_client.redis.pipeline()
        pipe.delete(f"channel:{channel_id}:members")
        pipe.sadd(f"channel:{channel_id}:members", *user_ids)
        await pipe.execute()

        # Also rebuild the reverse index for each member.
        pipe = redis_client.redis.pipeline()
        for uid in user_ids:
            pipe.sadd(f"user:{uid}:channels", channel_id)
        await pipe.execute()

        logger.info(
            f"Rebuild {channel_id}: wrote {len(user_ids)} member(s)"
        )
        return len(user_ids)

    async def rebuild_all_channels(self) -> dict[str, int]:
        """Rebuild membership for every channel in the DB.

        Intended for startup after a Redis flush or migration.
        Runs rebuilds concurrently (one task per channel) with
        a semaphore so we don't hammer the DB.

        Returns {channel_id: member_count}.
        """
        async with database.get_session() as session:
            rows = await session.execute(
                text("SELECT DISTINCT channel_id FROM channel_members")
            )
            channel_ids = [r[0] for r in rows.fetchall()]

        sem = asyncio.Semaphore(10)   # max 10 concurrent DB queries

        async def _bounded(cid: str) -> tuple[str, int]:
            async with sem:
                n = await self._do_rebuild(cid)
                return cid, n

        results = await asyncio.gather(
            *[_bounded(cid) for cid in channel_ids],
            return_exceptions=True,
        )

        counts: dict[str, int] = {}
        for r in results:
            if isinstance(r, Exception):
                logger.error(f"rebuild_all_channels error: {r}")
            else:
                cid, n = r
                counts[cid] = n

        logger.info(
            f"rebuild_all_channels: rebuilt {len(counts)} channel(s)"
        )
        return counts

    # ─────────────────────────────────────────────────────────
    # 2. Admin kick
    # ─────────────────────────────────────────────────────────

    async def kick_user(
        self,
        channel_id: str,
        user_id: str,
        kicked_by: str,
        reason: Optional[str] = None,
    ) -> dict:
        """Remove a user from a channel and notify them.

        Steps:
          1. Remove from DB (channel_members table)
          2. Remove from Redis (both directions)
          3. Push "channel.kicked" down the user's WebSocket(s)
             so the client can update its UI immediately
          4. Return a summary

        Idempotent: safe to call even if the user is already
        not a member (both DB and Redis ops are no-ops in that
        case; we still attempt the WebSocket notify).

        Caller is responsible for authorization — this method
        does not check permissions.
        """
        # ── 1. Remove from DB ─────────────────────────────────
        db_removed = await self._db_remove_member(channel_id, user_id)

        # ── 2. Remove from Redis ──────────────────────────────
        redis_removed = await self._registry.leave_channel(
            channel_id, user_id
        )

        # ── 3. Notify user's active WebSocket connections ─────
        event = json.dumps({
            "type": KICK_EVENT_TYPE,
            "channelId": channel_id,
            "userId": user_id,
            "kickedBy": kicked_by,
            "reason": reason,
            "timestamp": time.time(),
        })
        notified = await self._notify_user(user_id, event)

        result = {
            "channelId": channel_id,
            "userId": user_id,
            "kickedBy": kicked_by,
            "dbRemoved": db_removed,
            "redisRemoved": redis_removed,
            "websocketNotified": notified,
        }
        logger.info(
            f"Kick: user={user_id} channel={channel_id} "
            f"by={kicked_by} db={db_removed} "
            f"redis={redis_removed} ws_notified={notified}"
        )
        return result

    async def _db_remove_member(
        self, channel_id: str, user_id: str
    ) -> bool:
        """Delete row from channel_members. Returns True if a row was deleted."""
        try:
            async with database.get_session() as session:
                result = await session.execute(
                    text(
                        "DELETE FROM channel_members "
                        "WHERE channel_id = :cid AND user_id = :uid"
                    ),
                    {"cid": channel_id, "uid": user_id},
                )
                await session.commit()
                return result.rowcount > 0
        except Exception as e:
            logger.error(
                f"DB remove_member failed "
                f"channel={channel_id} user={user_id}: {e}"
            )
            return False

    async def _notify_user(self, user_id: str, payload: str) -> int:
        """Send payload to all local WebSocket connections for a user.

        If the user is on another server, we publish via Redis
        Pub/Sub so that server's PubSubSubscriber delivers it.
        Returns the number of local connections notified.
        """
        connections = self._registry.get_user_connections(user_id)
        sent = 0
        for conn in connections:
            try:
                await conn.websocket.send_text(payload)
                sent += 1
            except Exception as e:
                logger.debug(
                    f"Kick notify WS send failed user={user_id}: {e}"
                )
                try:
                    await self._registry.remove_connection(conn.websocket)
                except Exception:
                    pass

        if sent == 0:
            # User not local — publish so another server picks it up.
            try:
                await redis_client.redis.publish(
                    f"deliver:{user_id}", payload
                )
            except Exception as e:
                logger.error(
                    f"Kick notify Redis publish failed user={user_id}: {e}"
                )

        return sent

    # ─────────────────────────────────────────────────────────
    # 3. get_members with inconsistency fallback
    # ─────────────────────────────────────────────────────────

    async def get_members(
        self,
        channel_id: str,
        *,
        trust_empty: bool = False,
    ) -> set[str]:
        """Get channel members, falling back to DB if Redis is empty.

        The normal path is a Redis SMEMBERS call — O(n) but fast.

        Inconsistency detection:
          If Redis returns empty AND trust_empty=False (the
          default), we cross-check the DB. An empty Redis set
          is suspicious because:
            - Redis was flushed / restarted
            - The key expired (no TTL, but possible after a
              FLUSHDB or migration)
            - Sync bug in the join path

          If the DB has members, we schedule a background
          rebuild and return the DB rows immediately so the
          caller isn't blocked.

          Pass trust_empty=True only when you've already
          confirmed the channel has zero members (e.g., after
          your own leave_channel call empties it).

        This does NOT fix write-path drift (a user who joined
        via the DB but not Redis). Call rebuild_channel() for
        that — typically at startup or via an admin endpoint.
        """
        # Fast path: Redis has members
        members = await self._registry.get_channel_members(channel_id)
        if members:
            return members

        if trust_empty:
            return set()

        # Slow path: Redis returned empty — consult the DB
        logger.warning(
            f"get_members: Redis empty for channel={channel_id}, "
            f"falling back to DB"
        )
        db_members = await self._db_get_members(channel_id)

        if not db_members:
            # DB also empty — channel genuinely has no members.
            return set()

        # Redis is out of sync. Schedule a non-blocking rebuild.
        logger.warning(
            f"get_members: Redis/DB mismatch for channel={channel_id} "
            f"({len(db_members)} DB members). Scheduling rebuild."
        )
        asyncio.create_task(
            self._background_rebuild(channel_id),
            name=f"rebuild-{channel_id}",
        )

        return db_members

    async def _db_get_members(self, channel_id: str) -> set[str]:
        """Read channel members directly from PostgreSQL."""
        try:
            async with database.get_session() as session:
                rows = await session.execute(
                    text(
                        "SELECT user_id FROM channel_members "
                        "WHERE channel_id = :cid"
                    ),
                    {"cid": channel_id},
                )
                return {r[0] for r in rows.fetchall()}
        except Exception as e:
            logger.error(
                f"DB get_members failed channel={channel_id}: {e}"
            )
            return set()

    async def _background_rebuild(self, channel_id: str) -> None:
        """Rebuild Redis for a channel without blocking the caller."""
        if channel_id in self._rebuild_in_progress:
            return
        self._rebuild_in_progress.add(channel_id)
        try:
            await self.rebuild_channel(channel_id)
        except Exception as e:
            logger.error(f"Background rebuild failed {channel_id}: {e}")
        finally:
            self._rebuild_in_progress.discard(channel_id)

    # ─────────────────────────────────────────────────────────
    # Convenience pass-throughs (so callers only import this)
    # ─────────────────────────────────────────────────────────

    async def join_channel(
        self, channel_id: str, user_id: str
    ) -> bool:
        """Join a channel in Redis AND persist to DB.

        Returns True if the user was newly added.
        """
        newly_added = await self._registry.join_channel(
            channel_id, user_id
        )
        if newly_added:
            await self._db_add_member(channel_id, user_id)
        return newly_added

    async def leave_channel(
        self, channel_id: str, user_id: str
    ) -> bool:
        """Leave a channel in Redis AND remove from DB."""
        was_member = await self._registry.leave_channel(
            channel_id, user_id
        )
        if was_member:
            await self._db_remove_member(channel_id, user_id)
        return was_member

    async def _db_add_member(
        self, channel_id: str, user_id: str
    ) -> None:
        """Insert into channel_members, ignoring duplicates."""
        try:
            async with database.get_session() as session:
                await session.execute(
                    text(
                        "INSERT INTO channel_members "
                        "(channel_id, user_id, joined_at) "
                        "VALUES (:cid, :uid, NOW()) "
                        "ON CONFLICT DO NOTHING"
                    ),
                    {"cid": channel_id, "uid": user_id},
                )
                await session.commit()
        except Exception as e:
            logger.error(
                f"DB add_member failed "
                f"channel={channel_id} user={user_id}: {e}"
            )


# ── Module singleton ──────────────────────────────────────────
# Instantiated in dependencies.py alongside registry / ws_manager.
# Don't import registry here at module level — import in
# dependencies.py to avoid circular imports.
#
#   from app.core.channel_membership_service import ChannelMembershipService
#   from app.dependencies import registry
#   membership_service = ChannelMembershipService(registry)


# ── Migration note ────────────────────────────────────────────
# If channel_members doesn't exist yet, add this Alembic migration:
#
#   op.create_table(
#       "channel_members",
#       sa.Column("channel_id", sa.String(128), nullable=False),
#       sa.Column("user_id",    sa.String(128), nullable=False),
#       sa.Column("joined_at",  sa.DateTime(timezone=True),
#                 server_default=sa.func.now(), nullable=False),
#       sa.PrimaryKeyConstraint("channel_id", "user_id"),
#   )
#   op.create_index(
#       "idx_channel_members_user",
#       "channel_members", ["user_id"]
#   )
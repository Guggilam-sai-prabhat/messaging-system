"""
Presence Service — Redis-backed user online/offline tracking.

What we already have (in ConnectionRegistry):
  ✅ user:{userId}:connections  → HASH of connection metadata
  ✅ user:{userId}:channels     → SET of channel IDs
  ✅ channel:{channelId}:members → SET of user IDs
  ✅ connections:active          → Sorted set for global count
  ✅ Multiple devices per user
  ✅ Cross-server connection tracking

What this service adds:
  1. Explicit online/offline status with TTL
  2. Heartbeat mechanism (stale connection cleanup)
  3. Presence events (notify channel members)
  4. Server crash recovery
  5. Presence query ("who is online in #general?")

Redis key design:

  user:{userId}:online
    TYPE: STRING with TTL
    VALUE: server_id (which server the user is on)
    TTL: 60 seconds (refreshed by heartbeat)
    WHY: Fast O(1) check "is this user online?" without
         counting connections. TTL ensures automatic cleanup
         if a server crashes — no heartbeat = key expires.

  server:{serverId}:users
    TYPE: SET of user_ids
    TTL: 90 seconds (refreshed by server heartbeat)
    WHY: Track which users are on which server. If a server
         dies, this set expires, and a cleanup sweep removes
         stale user:*:online keys.

  user:{userId}:last_seen
    TYPE: STRING (Unix timestamp)
    NO TTL: permanent record of when user was last online
    WHY: "Last seen 5 minutes ago" feature

Why Redis for presence (not in-memory)?
  In-memory works on ONE server. But with multiple servers:
  - Server 1 has alice, Server 2 has bob
  - Server 2 needs to know alice is online (to show green dot)
  - Only Redis (shared state) can answer that

Why TTL on the online key?
  If Server 1 crashes without running disconnect cleanup,
  alice's online key would stick forever. TTL = auto-cleanup.
  The heartbeat refreshes the TTL every 30 seconds. No
  heartbeat (server dead) = TTL expires = user goes offline.
"""

import time
import json
import asyncio
import logging
import uuid
from typing import Optional

from app.core.redis_client import redis_client

logger = logging.getLogger("presence")

# ── Config ────────────────────────────────────────────────────

ONLINE_TTL = 60            # seconds before online key expires
HEARTBEAT_INTERVAL = 30    # seconds between heartbeats
SERVER_TTL = 90            # seconds before server key expires
LAST_SEEN_TTL = 86400 * 30  # 30 days


class PresenceService:
    """Manages user presence across multiple servers."""

    def __init__(self):
        # Unique ID for this server instance.
        # Used to track which users are on which server,
        # and to clean up if this server crashes.
        self._server_id = f"server-{uuid.uuid4().hex[:8]}"
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._running = False

        # Registry reference for broadcasting presence events
        self._registry = None

    def set_registry(self, registry) -> None:
        self._registry = registry

    @property
    def server_id(self) -> str:
        return self._server_id

    # ── Lifecycle ─────────────────────────────────────────────

    async def start(self) -> None:
        """Start the heartbeat loop."""
        self._running = True
        self._heartbeat_task = asyncio.create_task(
            self._heartbeat_loop()
        )
        logger.info(
            f"Presence service started, server_id={self._server_id}"
        )

    async def shutdown(self) -> None:
        """Stop heartbeat and clean up this server's presence."""
        self._running = False

        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass

        # Clean up all users on this server
        await self._cleanup_server(self._server_id)
        logger.info("Presence service shut down")

    # ── User Online/Offline ───────────────────────────────────

    async def user_connected(self, user_id: str) -> dict:
        """Called when a user connects a WebSocket.

        Sets the online key with TTL, adds user to this
        server's user set, and broadcasts presence to
        channel members.

        Returns presence info dict.
        """
        try:
            pipe = redis_client.redis.pipeline()

            # Set online status with TTL
            pipe.set(
                f"user:{user_id}:online",
                self._server_id,
                ex=ONLINE_TTL,
            )

            # Track user on this server
            pipe.sadd(f"server:{self._server_id}:users", user_id)
            pipe.expire(
                f"server:{self._server_id}:users", SERVER_TTL
            )

            await pipe.execute()

            # Broadcast presence to channel members
            await self._broadcast_presence(
                user_id, status="online"
            )

            logger.info(f"User {user_id} → online (server={self._server_id})")

            return {
                "userId": user_id,
                "status": "online",
                "serverId": self._server_id,
            }

        except Exception as e:
            logger.error(f"user_connected failed for {user_id}: {e}")
            return {"userId": user_id, "status": "unknown"}

    async def user_disconnected(self, user_id: str) -> dict:
        """Called when a user's WebSocket disconnects.

        Only marks offline if the user has NO remaining
        connections (they might have other tabs/devices).

        The registry's remove_connection is called before this,
        so get_user_connections returns the REMAINING connections.
        """
        try:
            # Check if user still has connections on ANY server
            conn_count = await redis_client.redis.hlen(
                f"user:{user_id}:connections"
            )

            if conn_count > 0:
                # Still has other connections — stay online
                logger.debug(
                    f"User {user_id} disconnected one device, "
                    f"{conn_count} remaining — still online"
                )
                return {
                    "userId": user_id,
                    "status": "online",
                    "remainingConnections": conn_count,
                }

            # No connections left — go offline
            pipe = redis_client.redis.pipeline()
            pipe.delete(f"user:{user_id}:online")
            pipe.srem(f"server:{self._server_id}:users", user_id)

            # Record last seen
            pipe.set(
                f"user:{user_id}:last_seen",
                str(time.time()),
                ex=LAST_SEEN_TTL,
            )

            await pipe.execute()

            # Broadcast offline to channel members
            await self._broadcast_presence(
                user_id, status="offline"
            )

            logger.info(f"User {user_id} → offline")

            return {
                "userId": user_id,
                "status": "offline",
            }

        except Exception as e:
            logger.error(
                f"user_disconnected failed for {user_id}: {e}"
            )
            return {"userId": user_id, "status": "unknown"}

    # ── Presence Queries ──────────────────────────────────────

    async def is_online(self, user_id: str) -> bool:
        """Check if a single user is online. O(1) in Redis."""
        try:
            return await redis_client.redis.exists(
                f"user:{user_id}:online"
            ) > 0
        except Exception:
            return False

    async def get_user_presence(self, user_id: str) -> dict:
        """Get full presence info for a user."""
        try:
            pipe = redis_client.redis.pipeline()
            pipe.get(f"user:{user_id}:online")
            pipe.hlen(f"user:{user_id}:connections")
            pipe.get(f"user:{user_id}:last_seen")

            online_server, conn_count, last_seen = (
                await pipe.execute()
            )

            if online_server:
                return {
                    "userId": user_id,
                    "status": "online",
                    "serverId": online_server,
                    "connectionCount": conn_count,
                }
            else:
                return {
                    "userId": user_id,
                    "status": "offline",
                    "lastSeen": float(last_seen) if last_seen else None,
                }

        except Exception as e:
            logger.error(f"get_user_presence failed: {e}")
            return {"userId": user_id, "status": "unknown"}

    async def get_channel_presence(
        self, channel_id: str
    ) -> dict:
        """Get online status for all members of a channel.

        This is the "who's online in #general?" query.
        Uses a Redis pipeline to check all members in one
        round trip instead of N individual EXISTS calls.
        """
        try:
            # Get all channel members
            members = await redis_client.redis.smembers(
                f"channel:{channel_id}:members"
            )

            if not members:
                return {
                    "channelId": channel_id,
                    "online": [],
                    "offline": [],
                }

            # Check online status for all members in one pipeline
            pipe = redis_client.redis.pipeline()
            member_list = list(members)
            for user_id in member_list:
                pipe.exists(f"user:{user_id}:online")

            results = await pipe.execute()

            online = []
            offline = []
            for user_id, is_on in zip(member_list, results):
                if is_on:
                    online.append(user_id)
                else:
                    offline.append(user_id)

            return {
                "channelId": channel_id,
                "online": online,
                "offline": offline,
                "onlineCount": len(online),
                "totalMembers": len(member_list),
            }

        except Exception as e:
            logger.error(f"get_channel_presence failed: {e}")
            return {
                "channelId": channel_id,
                "online": [],
                "offline": [],
            }

    async def get_bulk_presence(
        self, user_ids: list[str]
    ) -> dict[str, bool]:
        """Check online status for multiple users at once.

        Useful for rendering a contact list or member sidebar
        with green/gray dots.
        """
        try:
            pipe = redis_client.redis.pipeline()
            for uid in user_ids:
                pipe.exists(f"user:{uid}:online")

            results = await pipe.execute()
            return {
                uid: bool(is_on)
                for uid, is_on in zip(user_ids, results)
            }
        except Exception:
            return {uid: False for uid in user_ids}

    # ── Presence Broadcasting ─────────────────────────────────

    async def _broadcast_presence(
        self, user_id: str, status: str
    ) -> None:
        """Notify channel members when a user goes online/offline.

        When alice comes online, everyone in her channels
        should see a green dot appear. We publish to Redis
        Pub/Sub for each channel she's in, and the delivery
        layer handles the rest.

        We publish to "presence:{channel_id}" — a separate
        pub/sub namespace from "deliver:{user_id}" so presence
        events don't get mixed with message delivery.
        """
        try:
            # Get all channels this user is in
            channels = await redis_client.redis.smembers(
                f"user:{user_id}:channels"
            )

            if not channels:
                return

            event = json.dumps({
                "type": "presence.update",
                "userId": user_id,
                "status": status,
                "timestamp": time.time(),
            })

            # Publish to each channel's presence topic
            pipe = redis_client.redis.pipeline()
            for ch in channels:
                pipe.publish(f"presence:{ch}", event)
            await pipe.execute()

        except Exception as e:
            logger.error(f"Presence broadcast failed: {e}")

    # ── Heartbeat ─────────────────────────────────────────────

    async def _heartbeat_loop(self) -> None:
        """Refresh TTLs for all users on this server.

        Runs every HEARTBEAT_INTERVAL seconds. For each user
        connected to this server, refreshes the TTL on their
        online key. Also refreshes the server's user set TTL.

        If this server crashes, heartbeats stop, TTLs expire,
        and users automatically appear offline.

        Why not let each WebSocket handler refresh its own TTL?
          1. One loop is simpler than N individual timers
          2. Pipeline batches all refreshes in one Redis call
          3. Centralized = easier to monitor and tune
        """
        while self._running:
            try:
                await asyncio.sleep(HEARTBEAT_INTERVAL)

                # Get users on this server
                users = await redis_client.redis.smembers(
                    f"server:{self._server_id}:users"
                )

                if not users:
                    continue

                # Refresh TTLs in one pipeline
                pipe = redis_client.redis.pipeline()

                # Refresh server set TTL
                pipe.expire(
                    f"server:{self._server_id}:users", SERVER_TTL
                )

                # Refresh each user's online TTL
                for user_id in users:
                    pipe.expire(
                        f"user:{user_id}:online", ONLINE_TTL
                    )

                await pipe.execute()

                logger.debug(
                    f"Heartbeat: refreshed {len(users)} user(s)"
                )

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Heartbeat error: {e}")
                await asyncio.sleep(5.0)

    # ── Crash Recovery ────────────────────────────────────────

    async def _cleanup_server(self, server_id: str) -> None:
        """Remove all presence data for a server.

        Called during graceful shutdown for THIS server.
        Could also be called by a monitoring service to
        clean up a crashed server.
        """
        try:
            users = await redis_client.redis.smembers(
                f"server:{server_id}:users"
            )

            if users:
                pipe = redis_client.redis.pipeline()
                for user_id in users:
                    pipe.delete(f"user:{user_id}:online")
                    pipe.set(
                        f"user:{user_id}:last_seen",
                        str(time.time()),
                        ex=LAST_SEEN_TTL,
                    )
                pipe.delete(f"server:{server_id}:users")
                await pipe.execute()

                logger.info(
                    f"Cleaned up {len(users)} user(s) "
                    f"from server {server_id}"
                )

        except Exception as e:
            logger.error(
                f"Server cleanup failed for {server_id}: {e}"
            )

    async def cleanup_stale_servers(self) -> int:
        """Find and clean up servers that crashed without shutdown.

        This can be called periodically by a background task
        or by an admin endpoint. It scans for server keys
        that still exist but whose TTL is about to expire
        (meaning the server stopped heartbeating).

        In practice, Redis TTL handles most of this automatically.
        This method catches edge cases where the online key TTL
        and server set TTL are out of sync.
        """
        cleaned = 0
        try:
            async for key in redis_client.redis.scan_iter(
                match="server:*:users"
            ):
                server_id = key.split(":")[1]
                ttl = await redis_client.redis.ttl(key)

                # If TTL is very low or expired, server is dead
                if ttl < 10:
                    await self._cleanup_server(server_id)
                    cleaned += 1

        except Exception as e:
            logger.error(f"Stale server cleanup failed: {e}")

        if cleaned:
            logger.info(f"Cleaned up {cleaned} stale server(s)")
        return cleaned

    # ── Stats ─────────────────────────────────────────────────

    async def stats(self) -> dict:
        try:
            server_users = await redis_client.redis.scard(
                f"server:{self._server_id}:users"
            )
        except Exception:
            server_users = -1

        return {
            "server_id": self._server_id,
            "running": self._running,
            "users_on_this_server": server_users,
        }


# ── Module singleton ──────────────────────────────────────────
presence_service = PresenceService()
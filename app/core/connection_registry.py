"""
Connection Registry — Redis-backed for multi-process support.

Two layers:
  1. LOCAL: WebSocket objects live in-process (can't serialize)
  2. REDIS: Channel membership + connection metadata (shared)

Why two layers?
  WebSocket objects are TCP connections bound to a specific
  process. You can't store them in Redis. So each process
  keeps a local map of ws → ConnectionInfo.

  But channel membership MUST be shared. If user A is on
  Server 1 and user B is on Server 2, both in channel
  "general", Server 2 needs to know A is a member when
  A sends a message. Redis makes this global.

Redis data structures:
  channel:{channel_id}:members  → SET of user_ids
  user:{user_id}:channels       → SET of channel_ids
  user:{user_id}:connections    → HASH of {conn_id: json metadata}
  connections:active            → sorted set for total count

Local data structures (per-process):
  _local_connections: dict[WebSocket, ConnectionInfo]
  _user_websockets: dict[user_id, dict[WebSocket, ConnectionInfo]]
"""

import json
import time
import logging
import uuid
from typing import Optional

from fastapi import WebSocket
from app.models import ConnectionInfo
from app.core.redis_client import redis_client

logger = logging.getLogger("registry")


class ConnectionRegistry:
    """Hybrid local + Redis connection registry."""

    def __init__(self):
        # ── Local state (WebSocket objects, per-process) ──────
        self._local_connections: dict[WebSocket, ConnectionInfo] = {}
        self._user_websockets: dict[str, dict[WebSocket, ConnectionInfo]] = {}
        self._ws_to_conn_id: dict[WebSocket, str] = {}

    # ── Connection Management (local + Redis) ─────────────────

    async def add_connection(self, info: ConnectionInfo) -> int:
        """Register a new connection locally and in Redis."""
        conn_id = str(uuid.uuid4())

        # Local
        self._local_connections[info.websocket] = info
        user_conns = self._user_websockets.setdefault(info.user_id, {})
        user_conns[info.websocket] = info
        self._ws_to_conn_id[info.websocket] = conn_id

        # Redis — track connection metadata
        try:
            pipe = redis_client.redis.pipeline()
            pipe.hset(
                f"user:{info.user_id}:connections",
                conn_id,
                json.dumps({
                    "device_id": info.device_id,
                    "connected_at": info.connected_at,
                    "conn_id": conn_id,
                }),
            )
            pipe.zadd(
                "connections:active",
                {conn_id: time.time()},
            )
            await pipe.execute()
        except Exception as e:
            logger.error(f"Redis add_connection failed: {e}")

        return len(user_conns)

    async def remove_connection(
        self, websocket: WebSocket
    ) -> Optional[ConnectionInfo]:
        """Remove connection locally and from Redis."""
        conn = self._local_connections.pop(websocket, None)
        if not conn:
            return None

        conn_id = self._ws_to_conn_id.pop(websocket, None)

        # Local
        user_conns = self._user_websockets.get(conn.user_id)
        if user_conns:
            user_conns.pop(websocket, None)
            if not user_conns:
                del self._user_websockets[conn.user_id]

        # Redis
        if conn_id:
            try:
                pipe = redis_client.redis.pipeline()
                pipe.hdel(
                    f"user:{conn.user_id}:connections",
                    conn_id,
                )
                pipe.zrem("connections:active", conn_id)
                await pipe.execute()
            except Exception as e:
                logger.error(f"Redis remove_connection failed: {e}")

        return conn

    def get_user_connections(
        self, user_id: str
    ) -> list[ConnectionInfo]:
        """Get LOCAL connections for a user (this process only)."""
        return list(
            self._user_websockets.get(user_id, {}).values()
        )

    async def is_user_online(self, user_id: str) -> bool:
        """Check if user has connections on ANY server."""
        # Check local first (fast path)
        if self._user_websockets.get(user_id):
            return True
        # Check Redis (cross-process)
        try:
            count = await redis_client.redis.hlen(
                f"user:{user_id}:connections"
            )
            return count > 0
        except Exception:
            return False

    # ── Channel Membership (Redis — shared across processes) ──

    async def join_channel(
        self, channel_id: str, user_id: str
    ) -> bool:
        """Join a channel. Returns True if newly added.

        Both directions are stored:
          channel:{id}:members → knows who's in the channel
          user:{id}:channels   → knows what channels a user is in
        """
        try:
            pipe = redis_client.redis.pipeline()
            pipe.sadd(f"channel:{channel_id}:members", user_id)
            pipe.sadd(f"user:{user_id}:channels", channel_id)
            results = await pipe.execute()
            was_new = results[0] == 1  # sadd returns 1 if new
            return was_new
        except Exception as e:
            logger.error(f"Redis join_channel failed: {e}")
            return False

    async def leave_channel(
        self, channel_id: str, user_id: str
    ) -> bool:
        """Leave a channel. Returns True if was a member."""
        try:
            pipe = redis_client.redis.pipeline()
            pipe.srem(f"channel:{channel_id}:members", user_id)
            pipe.srem(f"user:{user_id}:channels", channel_id)
            results = await pipe.execute()
            was_member = results[0] == 1
            return was_member
        except Exception as e:
            logger.error(f"Redis leave_channel failed: {e}")
            return False

    async def get_channel_members(
        self, channel_id: str
    ) -> set[str]:
        """Get all members of a channel (across all servers)."""
        try:
            members = await redis_client.redis.smembers(
                f"channel:{channel_id}:members"
            )
            return set(members)
        except Exception as e:
            logger.error(f"Redis get_channel_members failed: {e}")
            return set()

    async def get_user_channels(self, user_id: str) -> list[str]:
        """Get all channels a user has joined."""
        try:
            channels = await redis_client.redis.smembers(
                f"user:{user_id}:channels"
            )
            return list(channels)
        except Exception as e:
            logger.error(f"Redis get_user_channels failed: {e}")
            return []

    # ── Diagnostics ───────────────────────────────────────────

    async def stats(self) -> dict:
        """Registry stats combining local and Redis data."""
        local_connections = len(self._local_connections)
        local_users = len(self._user_websockets)

        try:
            global_connections = await redis_client.redis.zcard(
                "connections:active"
            )
        except Exception:
            global_connections = -1

        return {
            "local_connections": local_connections,
            "local_users": local_users,
            "global_connections": global_connections,
        }
"""
Connection Registry — the core bookkeeping layer.

Two data structures, each optimized for a different access pattern:
  1. user_connections: userId → list[ConnectionInfo]
  2. channel_members: channelId → set[userId]

ConnectionInfo is defined in app/models/connection.py — this module
owns the LOGIC (add, remove, lookup), not the data shape.
"""

from fastapi import WebSocket

from app.models import ConnectionInfo


class ConnectionRegistry:
    """In-memory connection registry.

    Safe under asyncio single-thread concurrency model.
    For multi-process, replace internals with Redis.
    """

    def __init__(self):
        self._user_connections: dict[str, list[ConnectionInfo]] = {}
        self._channel_members: dict[str, set[str]] = {}

    # ─── User Connections ─────────────────────────────────────

    def add_connection(self, info: ConnectionInfo) -> int:
        """Register a new connection. Returns total active count for user."""
        conns = self._user_connections.setdefault(info.user_id, [])
        conns.append(info)
        return len(conns)

    def remove_connection(self, websocket: WebSocket) -> ConnectionInfo | None:
        """Remove by WebSocket identity. Returns removed info or None."""
        for user_id, connections in self._user_connections.items():
            for i, conn in enumerate(connections):
                if conn.websocket is websocket:
                    removed = connections.pop(i)
                    if not connections:
                        del self._user_connections[user_id]
                    return removed
        return None

    def get_user_connections(self, user_id: str) -> list[ConnectionInfo]:
        return list(self._user_connections.get(user_id, []))

    def is_user_online(self, user_id: str) -> bool:
        return bool(self._user_connections.get(user_id))

    # ─── Channel Membership ───────────────────────────────────

    def join_channel(self, channel_id: str, user_id: str) -> bool:
        """Returns True if newly added."""
        members = self._channel_members.setdefault(channel_id, set())
        was_new = user_id not in members
        members.add(user_id)
        return was_new

    def leave_channel(self, channel_id: str, user_id: str) -> bool:
        members = self._channel_members.get(channel_id)
        if not members or user_id not in members:
            return False
        members.discard(user_id)
        if not members:
            del self._channel_members[channel_id]
        return True

    def get_channel_members(self, channel_id: str) -> set[str]:
        return set(self._channel_members.get(channel_id, set()))

    def get_user_channels(self, user_id: str) -> list[str]:
        return [
            ch for ch, members in self._channel_members.items()
            if user_id in members
        ]

    # ─── Diagnostics ──────────────────────────────────────────

    def stats(self) -> dict:
        total = sum(len(c) for c in self._user_connections.values())
        return {
            "online_users": len(self._user_connections),
            "total_connections": total,
            "active_channels": len(self._channel_members),
        }
"""
WebSocket Manager — handles all WebSocket events.

Changes from previous version:
  1. Auth: authenticate_token → decode_access_token (JWT)
     Returns UserClaims dataclass instead of raw dict.

  2. Channel join/leave: registry → channel_service
     Now writes to DB + Redis instead of Redis-only.
     This means channels joined via WebSocket are persistent
     and survive Redis restarts.

  3. Message ingest: passes membership_service for DB fallback
     on membership checks.

  4. ConnectionInfo: device_id comes from JWT claims, not a
     separate mock token dict.

Events:
  ping              → pong
  channel.join      → channel.joined
  channel.leave     → channel.left
  message.send      → message.ack
  messages.history  → messages.history
  messages.get      → messages.get
  channel.stats     → channel.stats
  reconnect         → reconnect.ack
"""

import time
import json
import logging
from fastapi import WebSocket, WebSocketDisconnect

from app.core.connection_registry import ConnectionRegistry
from app.core.security import decode_access_token
from app.services.channel_membership_service import ChannelMembershipService
from app.services.channel_service import (
    ChannelService,
    ChannelServiceError,
    AlreadyMemberError,
    NotMemberError,
    ChannelNotFoundError,
)
from app.services.message_ingest import (
    MessageIngestService,
    MessageValidationError,
)
from app.core.kafka_producer import KafkaProduceError, KafkaCircuitOpenError
from app.core.structred_log import ingest_log
from app.services.message_history import message_history
from app.core.pubsub_subscriber import pubsub_subscriber
from app.services.presence_service import presence_service
from app.models import ConnectionInfo

logger = logging.getLogger("ws.manager")


class WebSocketManager:
    """
    Constructor now takes all three dependencies:
      registry          — connection tracking (in-memory + Redis)
      membership        — Redis membership with DB fallback
      channel_service   — full channel CRUD (DB + Redis)

    Wired in dependencies.py:
      ws_manager = WebSocketManager(registry, membership_service, channel_service)
    """

    def __init__(
        self,
        registry: ConnectionRegistry,
        membership: ChannelMembershipService,
        channel_service: ChannelService,
    ):
        self._registry = registry
        self._channel_service = channel_service
        self._ingest = MessageIngestService(registry, membership)

    async def handle_connection(self, websocket: WebSocket, token: str):
        """Authenticate and manage a WebSocket connection.

        Auth flow:
          1. Decode the JWT access token
          2. If invalid/expired → close with 4001
          3. If valid → accept, register, start message loop

        The token comes as a query parameter because browsers
        can't set custom headers on WebSocket connections:
          ws://host/ws?token=eyJhbGciOiJIUzI1NiIs...

        Token refresh:
          Access tokens are short-lived (15 min). The client
          should refresh via POST /auth/refresh before the
          token expires and reconnect with the new token.

          We do NOT support mid-connection token refresh over
          the WebSocket itself. The client disconnects, refreshes
          via HTTP, and reconnects. This keeps the auth flow
          stateless and simple.
        """
        # ── JWT authentication ────────────────────────────────
        claims = decode_access_token(token)
        if claims is None:
            await websocket.accept()
            await websocket.close(code=4001, reason="Invalid or expired token")
            logger.warning("Rejected connection: invalid or expired token")
            return

        user_id = claims.user_id
        device_id = claims.device_id or "unknown"

        await websocket.accept()

        conn_info = ConnectionInfo(
            websocket=websocket,
            user_id=user_id,
            device_id=device_id,
            connected_at=time.time(),
        )
        active_count = await self._registry.add_connection(conn_info)
        await pubsub_subscriber.subscribe_user(user_id)
        await presence_service.user_connected(user_id)
        logger.info(
            f"Connected: user={user_id} device={device_id} "
            f"({active_count} active)"
        )

        await websocket.send_json({
            "type": "connection.established",
            "user_id": user_id,
            "device_id": device_id,
            "active_connections": active_count,
            "server_time": time.time(),
        })

        try:
            await self._message_loop(websocket, conn_info)
        except WebSocketDisconnect:
            logger.info(
                f"Disconnected: user={user_id} device={device_id}"
            )
        except Exception as e:
            logger.error(f"Error for user={user_id}: {e}")
        finally:
            await self._handle_disconnect(websocket, user_id)

    async def _message_loop(self, ws: WebSocket, conn: ConnectionInfo):
        while True:
            raw = await ws.receive_text()
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                await ws.send_json({
                    "type": "error",
                    "message": "Invalid JSON",
                })
                continue

            msg_type = msg.get("type")

            if msg_type == "ping":
                await ws.send_json({
                    "type": "pong",
                    "server_time": time.time(),
                })

            elif msg_type == "channel.join":
                await self._handle_channel_join(ws, msg, conn)

            elif msg_type == "channel.leave":
                await self._handle_channel_leave(ws, msg, conn)

            elif msg_type == "message.send":
                await self._handle_message_send(ws, msg, conn)

            # ── History Events ────────────────────────────────

            elif msg_type == "messages.history":
                await self._handle_messages_history(ws, msg)

            elif msg_type == "messages.get":
                await self._handle_message_get(ws, msg)

            elif msg_type == "channel.stats":
                await self._handle_channel_stats(ws, msg)

            # ── Presence Events ───────────────────────────────

            elif msg_type == "presence.query":
                await self._handle_presence_query(ws, msg)

            elif msg_type == "presence.channel":
                await self._handle_channel_presence(ws, msg)

            elif msg_type == "reconnect":
                last_seen = msg.get("last_event_id")
                await ws.send_json({
                    "type": "reconnect.ack",
                    "last_event_id": last_seen,
                    "message": (
                        "Reconnection noted "
                        "(replay not yet implemented)"
                    ),
                })

            else:
                await ws.send_json({
                    "type": "error",
                    "message": f"Unknown message type: {msg_type}",
                })

    # ── Channel Join/Leave ────────────────────────────────────
    #
    # These now go through channel_service (DB + Redis) instead
    # of the raw registry (Redis-only). This means:
    #   - Joins persist to the channel_members table
    #   - Channel existence is verified
    #   - Deleted channels are rejected
    #   - Duplicate joins return a clear error

    async def _handle_channel_join(
        self, ws: WebSocket, msg: dict, conn: ConnectionInfo
    ) -> None:
        ch_id = msg.get("channel_id")
        if not ch_id:
            await ws.send_json({
                "type": "error",
                "message": "channel_id required",
            })
            return

        try:
            result = await self._channel_service.join_channel(
                ch_id, conn.user_id
            )
            await ws.send_json({
                "type": "channel.joined",
                "channel_id": ch_id,
                "role": result.get("role", "member"),
            })
        except AlreadyMemberError:
            # Not an error from the client's perspective —
            # they might be reconnecting and re-joining.
            # Return success with a hint that they were
            # already a member.
            await ws.send_json({
                "type": "channel.joined",
                "channel_id": ch_id,
                "was_already_member": True,
            })
        except ChannelNotFoundError:
            await ws.send_json({
                "type": "error",
                "message": f"Channel '{ch_id}' not found",
            })
        except ChannelServiceError as e:
            await ws.send_json({
                "type": "error",
                "message": e.message,
            })

    async def _handle_channel_leave(
        self, ws: WebSocket, msg: dict, conn: ConnectionInfo
    ) -> None:
        ch_id = msg.get("channel_id")
        if not ch_id:
            await ws.send_json({
                "type": "error",
                "message": "channel_id required",
            })
            return

        try:
            await self._channel_service.leave_channel(
                ch_id, conn.user_id
            )
            await ws.send_json({
                "type": "channel.left",
                "channel_id": ch_id,
            })
        except NotMemberError:
            await ws.send_json({
                "type": "channel.left",
                "channel_id": ch_id,
                "was_member": False,
            })
        except ChannelServiceError as e:
            await ws.send_json({
                "type": "error",
                "message": e.message,
            })

    # ── Message Send Handler ──────────────────────────────────

    async def _handle_message_send(
        self, ws: WebSocket, msg: dict, conn: ConnectionInfo
    ) -> None:
        try:
            result = await self._ingest.validate_and_enrich(
                msg, conn.user_id
            )

            ack = {
                "type": "message.ack",
                "message_id": result.enriched.message_id,
                "correlation_id": result.enriched.correlation_id,
                "channel_id": result.enriched.channel_id,
                "timestamp": result.enriched.timestamp,
                "pipeline_ms": round(result.pipeline_ms, 2),
                "was_dedup": result.was_dedup,
            }
            if result.enriched.client_request_id:
                ack["client_request_id"] = (
                    result.enriched.client_request_id
                )

            await ws.send_json(ack)

            ingest_log.message_ack_sent(
                correlation_id=result.enriched.correlation_id,
                user_id=conn.user_id,
                message_id=result.enriched.message_id,
                total_ms=result.pipeline_ms,
            )

        except MessageValidationError as e:
            await ws.send_json({
                "type": "message.error",
                "correlation_id": e.correlation_id,
                "reason": e.reason,
                "retryable": False,
            })

        except KafkaCircuitOpenError as e:
            logger.warning(
                f"Circuit open for user={conn.user_id}: {e}"
            )
            await ws.send_json({
                "type": "message.kafka_error",
                "reason": (
                    "Service temporarily unavailable. "
                    "Please retry in a few seconds."
                ),
                "retryable": False,
            })

        except KafkaProduceError as e:
            logger.error(
                f"Kafka produce failed for user={conn.user_id}: {e}"
            )
            await ws.send_json({
                "type": "message.kafka_error",
                "reason": (
                    "Message could not be delivered. "
                    "Please retry."
                ),
                "retryable": True,
            })

    # ── History Handlers ──────────────────────────────────────

    async def _handle_messages_history(
        self, ws: WebSocket, msg: dict
    ) -> None:
        channel_id = msg.get("channel_id")
        if not channel_id:
            await ws.send_json({
                "type": "error",
                "message": "channel_id required",
            })
            return

        try:
            data = await message_history.get_channel_messages(
                channel_id=channel_id,
                limit=msg.get("limit", 50),
                before=msg.get("before"),
                after=msg.get("after"),
            )

            await ws.send_json({
                "type": "messages.history",
                "channel_id": channel_id,
                **data,
            })

        except Exception as e:
            logger.error(f"History query failed: {e}")
            await ws.send_json({
                "type": "error",
                "message": "Failed to load message history",
            })

    async def _handle_message_get(
        self, ws: WebSocket, msg: dict
    ) -> None:
        channel_id = msg.get("channel_id")
        message_id = msg.get("message_id")

        if not channel_id or not message_id:
            await ws.send_json({
                "type": "error",
                "message": "channel_id and message_id required",
            })
            return

        try:
            data = await message_history.get_message_by_id(
                channel_id=channel_id,
                message_id=message_id,
            )

            await ws.send_json({
                "type": "messages.get",
                "channel_id": channel_id,
                "message_id": message_id,
                "message": data,
            })

        except Exception as e:
            logger.error(f"Message get failed: {e}")
            await ws.send_json({
                "type": "error",
                "message": "Failed to load message",
            })

    async def _handle_channel_stats(
        self, ws: WebSocket, msg: dict
    ) -> None:
        channel_id = msg.get("channel_id")
        if not channel_id:
            await ws.send_json({
                "type": "error",
                "message": "channel_id required",
            })
            return

        try:
            data = await message_history.get_channel_stats(
                channel_id=channel_id
            )
            await ws.send_json({
                "type": "channel.stats",
                **data,
            })

        except Exception as e:
            logger.error(f"Channel stats failed: {e}")
            await ws.send_json({
                "type": "error",
                "message": "Failed to load channel stats",
            })

    # ── Presence Handlers ─────────────────────────────────────

    async def _handle_presence_query(
        self, ws: WebSocket, msg: dict
    ) -> None:
        user_ids = msg.get("user_ids", [])
        if not user_ids:
            await ws.send_json({
                "type": "error",
                "message": "user_ids required",
            })
            return

        if len(user_ids) > 100:
            user_ids = user_ids[:100]

        try:
            users = {}
            for uid in user_ids:
                users[uid] = await presence_service.get_user_presence(uid)

            await ws.send_json({
                "type": "presence.query",
                "users": users,
            })
        except Exception as e:
            logger.error(f"Presence query failed: {e}")
            await ws.send_json({
                "type": "error",
                "message": "Failed to query presence",
            })

    async def _handle_channel_presence(
        self, ws: WebSocket, msg: dict
    ) -> None:
        channel_id = msg.get("channel_id")
        if not channel_id:
            await ws.send_json({
                "type": "error",
                "message": "channel_id required",
            })
            return

        try:
            data = await presence_service.get_channel_presence(
                channel_id
            )
            await ws.send_json({
                "type": "presence.channel",
                **data,
            })
        except Exception as e:
            logger.error(f"Channel presence failed: {e}")
            await ws.send_json({
                "type": "error",
                "message": "Failed to query channel presence",
            })

    # ── Disconnect Handler ────────────────────────────────────

    async def _handle_disconnect(
        self, ws: WebSocket, user_id: str
    ) -> None:
        removed = await self._registry.remove_connection(ws)
        if removed:
            await pubsub_subscriber.unsubscribe_user(user_id)
            await presence_service.user_disconnected(user_id)
            remaining = len(
                self._registry.get_user_connections(user_id)
            )
            logger.info(
                f"Cleaned up: user={removed.user_id} "
                f"device={removed.device_id} "
                f"({remaining} remaining)"
            )
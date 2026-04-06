"""
WebSocket Manager — handles all WebSocket events.

Events:
  ping              → pong
  channel.join      → channel.joined
  channel.leave     → channel.left
  message.send      → message.ack
  messages.history  → messages.history (paginated channel messages)
  messages.get      → messages.get (single message by ID)
  channel.stats     → channel.stats (message count + time range)
  reconnect         → reconnect.ack
"""

import time
import json
import logging
from fastapi import WebSocket, WebSocketDisconnect

from app.core.connection_registry import ConnectionRegistry
from app.core.auth import authenticate_token
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
    def __init__(self, registry: ConnectionRegistry):
        self._registry = registry
        self._ingest = MessageIngestService(registry)

    async def handle_connection(self, websocket: WebSocket, token: str):
        claims = authenticate_token(token)
        if not claims:
            await websocket.accept()
            await websocket.close(code=4001, reason="Invalid token")
            logger.warning("Rejected connection: invalid token")
            return

        user_id = claims["user_id"]
        device_id = claims["device_id"]

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
                ch_id = msg.get("channel_id")
                if not ch_id:
                    await ws.send_json({
                        "type": "error",
                        "message": "channel_id required",
                    })
                    continue
                was_new = await self._registry.join_channel(
                    ch_id, conn.user_id
                )
                await ws.send_json({
                    "type": "channel.joined",
                    "channel_id": ch_id,
                    "was_new": was_new,
                })

            elif msg_type == "channel.leave":
                ch_id = msg.get("channel_id")
                if not ch_id:
                    await ws.send_json({
                        "type": "error",
                        "message": "channel_id required",
                    })
                    continue
                removed = await self._registry.leave_channel(
                    ch_id, conn.user_id
                )
                await ws.send_json({
                    "type": "channel.left",
                    "channel_id": ch_id,
                    "was_member": removed,
                })

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
        """Handle messages.history event.

        Client sends:
          {
            "type": "messages.history",
            "channel_id": "general",
            "limit": 50,            // optional, default 50
            "before": 1711990000.0, // optional, scroll up
            "after": 1711989500.0   // optional, catch up
          }

        Server responds:
          {
            "type": "messages.history",
            "channel_id": "general",
            "messages": [...],
            "count": 50,
            "hasMore": true,
            "nextCursor": 1711989000.0
          }
        """
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
        """Handle messages.get event.

        Client sends:
          {
            "type": "messages.get",
            "channel_id": "general",
            "message_id": "abc-123"
          }

        Server responds:
          {
            "type": "messages.get",
            "message": {...} or null
          }
        """
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
        """Handle channel.stats event.

        Client sends:
          {"type": "channel.stats", "channel_id": "general"}

        Server responds:
          {
            "type": "channel.stats",
            "channelId": "general",
            "totalMessages": 1234,
            "firstMessageAt": 1711000000.0,
            "lastMessageAt": 1711990000.0
          }
        """
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
        """Handle presence.query — check if specific users are online.

        Client sends:
          {
            "type": "presence.query",
            "user_ids": ["alice", "bob", "charlie"]
          }

        Server responds:
          {
            "type": "presence.query",
            "users": {
              "alice": {"status": "online", ...},
              "bob": {"status": "offline", "lastSeen": 1711990000},
              "charlie": {"status": "online", ...}
            }
          }
        """
        user_ids = msg.get("user_ids", [])
        if not user_ids:
            await ws.send_json({
                "type": "error",
                "message": "user_ids required",
            })
            return

        # Cap at 100 to prevent abuse
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
        """Handle presence.channel — who's online in a channel?

        Client sends:
          {"type": "presence.channel", "channel_id": "general"}

        Server responds:
          {
            "type": "presence.channel",
            "channelId": "general",
            "online": ["alice", "bob"],
            "offline": ["charlie"],
            "onlineCount": 2,
            "totalMembers": 3
          }
        """
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
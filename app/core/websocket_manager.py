"""
WebSocket Manager — lifecycle handler for every WS connection.

Orchestrates auth → register → message loop → cleanup.
"""

import time
import json
import logging
from fastapi import WebSocket, WebSocketDisconnect

from app.core.connection_registry import ConnectionRegistry
from app.core.auth import authenticate_token
from app.core.message_ingest import MessageIngestService, MessageValidationError
from app.models import ConnectionInfo

logger = logging.getLogger("ws.manager")


class WebSocketManager:
    def __init__(self, registry: ConnectionRegistry):
        self._registry = registry
        self._ingest = MessageIngestService(registry)

    async def handle_connection(self, websocket: WebSocket, token: str):
        """Full lifecycle: auth → accept → register → loop → cleanup."""

        # Auth BEFORE accept — reject cheaply.
        # We must accept() first, THEN close with our custom code.
        # Why? Starlette doesn't support sending custom WS close codes
        # during the HTTP handshake phase. If we call close() before
        # accept(), Starlette sends a raw HTTP 403 — which is fine for
        # the server, but the client never enters WebSocket protocol
        # and can't read our 4001 code or reason string.
        #
        # The pattern: accept → send error frame → close.
        # The accept is cheap (~200 bytes), and we close immediately,
        # so the "wasted" resources are negligible.
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
        active_count = self._registry.add_connection(conn_info)
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
            logger.info(f"Disconnected: user={user_id} device={device_id}")
        except Exception as e:
            logger.error(f"Error for user={user_id}: {e}")
        finally:
            await self._handle_disconnect(websocket, user_id)

    async def _message_loop(self, ws: WebSocket, conn: ConnectionInfo):
        """Process client messages: heartbeat + channel commands."""
        while True:
            raw = await ws.receive_text()
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                await ws.send_json({"type": "error", "message": "Invalid JSON"})
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
                    await ws.send_json({"type": "error", "message": "channel_id required"})
                    continue
                was_new = self._registry.join_channel(ch_id, conn.user_id)
                await ws.send_json({
                    "type": "channel.joined",
                    "channel_id": ch_id,
                    "was_new": was_new,
                })

            elif msg_type == "channel.leave":
                ch_id = msg.get("channel_id")
                if not ch_id:
                    await ws.send_json({"type": "error", "message": "channel_id required"})
                    continue
                removed = self._registry.leave_channel(ch_id, conn.user_id)
                await ws.send_json({
                    "type": "channel.left",
                    "channel_id": ch_id,
                    "was_member": removed,
                })

            elif msg_type == "message.send":
                # ── MESSAGE INGESTION ─────────────────────────
                # Validate → enrich → log. NO delivery.
                #
                # Why send_json back to the SENDER only?
                # This is an acknowledgment ("your message was accepted")
                # not delivery. The sender needs to know:
                #   1. The server-assigned messageId (for dedup)
                #   2. The server timestamp (for ordering)
                #
                # Delivery to OTHER users happens via Kafka consumer
                # (not yet implemented). Today we just log.
                try:
                    enriched = self._ingest.validate_and_enrich(msg, conn.user_id)
                    await ws.send_json({
                        "type": "message.ack",
                        "message_id": enriched.message_id,
                        "channel_id": enriched.channel_id,
                        "timestamp": enriched.timestamp,
                    })
                except MessageValidationError as e:
                    await ws.send_json({
                        "type": "message.error",
                        "reason": e.reason,
                    })

            elif msg_type == "reconnect":
                last_seen = msg.get("last_event_id")
                await ws.send_json({
                    "type": "reconnect.ack",
                    "last_event_id": last_seen,
                    "message": "Reconnection noted (replay not yet implemented)",
                })

            else:
                await ws.send_json({
                    "type": "error",
                    "message": f"Unknown message type: {msg_type}",
                })

    async def _handle_disconnect(self, ws: WebSocket, user_id: str):
        """Remove CONNECTION, not USER. Channel membership persists."""
        removed = self._registry.remove_connection(ws)
        if removed:
            remaining = len(self._registry.get_user_connections(user_id))
            logger.info(
                f"Cleaned up: user={removed.user_id} "
                f"device={removed.device_id} "
                f"({remaining} remaining)"
            )
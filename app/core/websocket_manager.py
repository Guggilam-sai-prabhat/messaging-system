"""
WebSocket Manager — lifecycle handler with full observability.

Changes from v0.1:
  - correlation_id flows from ingest → ack/error response
  - Three error types: validation, kafka, circuit-open
  - Structured ack with pipeline timing
"""

import time
import json
import logging
from fastapi import WebSocket, WebSocketDisconnect

from app.core.connection_registry import ConnectionRegistry
from app.core.auth import authenticate_token
from app.core.message_ingest import (
    MessageIngestService,
    MessageValidationError,
)
from app.core.kafka_producer import KafkaProduceError, KafkaCircuitOpenError
from app.core.structred_log import ingest_log
from app.models import ConnectionInfo

logger = logging.getLogger("ws.manager")


class WebSocketManager:
    def __init__(self, registry: ConnectionRegistry):
        self._registry = registry
        self._ingest = MessageIngestService(registry)

    async def handle_connection(self, websocket: WebSocket, token: str):
        """Full lifecycle: auth → accept → register → loop → cleanup."""

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
                was_new = self._registry.join_channel(
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
                removed = self._registry.leave_channel(
                    ch_id, conn.user_id
                )
                await ws.send_json({
                    "type": "channel.left",
                    "channel_id": ch_id,
                    "was_member": removed,
                })

            elif msg_type == "message.send":
                await self._handle_message_send(ws, msg, conn)

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

    async def _handle_message_send(
        self, ws: WebSocket, msg: dict, conn: ConnectionInfo
    ) -> None:
        """Handle message.send with three-tier error handling.

        Success → message.ack with correlation_id + timing
        ValidationError → message.error (client should fix payload)
        CircuitOpen → message.kafka_error retryable=False
                      (don't retry immediately, Kafka is down)
        KafkaProduceError → message.kafka_error retryable=True
                      (transient, retry with same client_request_id)
        """
        try:
            result = await self._ingest.validate_and_enrich(
                msg, conn.user_id
            )

            # ── Success ack ───────────────────────────────────
            ack = {
                "type": "message.ack",
                "message_id": result.enriched.message_id,
                "correlation_id": result.enriched.correlation_id,
                "channel_id": result.enriched.channel_id,
                "timestamp": result.enriched.timestamp,
                "pipeline_ms": round(result.pipeline_ms, 2),
            }
            # Echo client_request_id so the client can match
            # the ack to its local send queue.
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
            logger.warning(f"Circuit open for user={conn.user_id}: {e}")
            await ws.send_json({
                "type": "message.kafka_error",
                "reason": (
                    "Service temporarily unavailable. "
                    "Please retry in a few seconds."
                ),
                "retryable": False,  # Don't hammer — wait for cooldown
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

    async def _handle_disconnect(
        self, ws: WebSocket, user_id: str
    ) -> None:
        removed = self._registry.remove_connection(ws)
        if removed:
            remaining = len(
                self._registry.get_user_connections(user_id)
            )
            logger.info(
                f"Cleaned up: user={removed.user_id} "
                f"device={removed.device_id} "
                f"({remaining} remaining)"
            )
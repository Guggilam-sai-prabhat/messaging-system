"""
Redis Pub/Sub Subscriber — cross-server message delivery.

Problem:
  User A is on Server 1. User B is on Server 2. Both are in
  channel "general". When A sends a message, the delivery
  consumer (maybe on Server 1, maybe on Server 3) needs to
  get it to B. But B's WebSocket object lives in Server 2's
  memory — you can't reach it from anywhere else.

Solution:
  Redis Pub/Sub as a lightweight message bus between servers.

  Every WS server runs a PubSubSubscriber. When a user
  connects, the server subscribes to "deliver:{user_id}".
  When the delivery consumer can't find a user locally, it
  publishes to "deliver:{user_id}". The server holding that
  user's WebSocket receives the message and sends it down.

  ┌─────────────────────────────────────────────────────┐
  │              Redis Pub/Sub                          │
  │                                                     │
  │  Channel: "deliver:bob"                             │
  │     └── Server 2 is subscribed (bob is here)        │
  │                                                     │
  │  Channel: "deliver:alice"                           │
  │     └── Server 1 is subscribed (alice is here)      │
  │     └── Server 3 is subscribed (alice's 2nd device) │
  └─────────────────────────────────────────────────────┘

Why Redis Pub/Sub and not Redis Streams / Kafka / etc?
  1. Fire-and-forget is fine here. If the user disconnected
     between publish and receive, we don't need retry —
     they'll fetch from DB on reconnect.
  2. Ultra low latency. Pub/Sub is just TCP push, no disk
     writes, no consumer groups, no offsets.
  3. No persistence needed. This is ephemeral delivery for
     ONLINE users only. Kafka already handles durability.
  4. Simple subscription model. Subscribe per-user is natural
     and doesn't require partition management.

Tradeoffs:
  - If Redis goes down, cross-server delivery stops (but
    local delivery still works).
  - Pub/Sub messages are lost if no subscriber is listening
    (that's by design — offline users get messages from DB).
  - Each server subscribes to N channels (one per connected
    user). At 10K users per server, that's 10K subscriptions.
    Redis handles this fine up to ~100K subscriptions.

Architecture note:
  Redis Pub/Sub requires a DEDICATED connection — you can't
  use the same connection for pub/sub and regular commands.
  That's why we create a separate Redis connection in start().
"""

import json
import logging
import asyncio
from typing import Optional

import redis.asyncio as aioredis

from app.config import settings

logger = logging.getLogger("pubsub")


class PubSubSubscriber:
    """Subscribes to Redis Pub/Sub for cross-server delivery."""

    def __init__(self):
        # ── Dedicated Redis connection for Pub/Sub ────────────
        # Can't share with the main Redis client because
        # a connection in subscribe mode can ONLY receive
        # pub/sub messages — no GET, SET, SADD, etc.
        self._redis: Optional[aioredis.Redis] = None
        self._pubsub: Optional[aioredis.client.PubSub] = None
        self._listener_task: Optional[asyncio.Task] = None
        self._running = False

        # ── Registry reference (injected at startup) ──────────
        self._registry = None

        # ── Track subscriptions ───────────────────────────────
        # We need to know which user channels we're subscribed
        # to so we can unsubscribe when users disconnect.
        self._subscribed_users: set[str] = set()

    def set_registry(self, registry) -> None:
        """Inject the connection registry."""
        self._registry = registry

    # ── Lifecycle ─────────────────────────────────────────────

    async def start(self) -> None:
        """Create dedicated Redis connection and start listener.

        Must be called AFTER redis_client.initialize() in the
        app lifespan — we need Redis to be reachable.
        """
        self._redis = aioredis.from_url(
            settings.redis_url,
            encoding="utf-8",
            decode_responses=True,
        )
        self._pubsub = self._redis.pubsub()
        self._running = True
        self._listener_task = asyncio.create_task(
            self._listen_loop()
        )
        logger.info("Pub/Sub subscriber started")

    async def shutdown(self) -> None:
        """Unsubscribe from all channels and close connection."""
        self._running = False

        if self._pubsub:
            try:
                await self._pubsub.unsubscribe()
                await self._pubsub.close()
            except Exception as e:
                logger.error(f"Pub/Sub unsubscribe error: {e}")

        if self._listener_task:
            self._listener_task.cancel()
            try:
                await self._listener_task
            except asyncio.CancelledError:
                pass

        if self._redis:
            await self._redis.close()

        self._subscribed_users.clear()
        logger.info("Pub/Sub subscriber shut down")

    # ── Subscribe/Unsubscribe ─────────────────────────────────

    async def subscribe_user(self, user_id: str) -> None:
        """Subscribe to delivery channel for a user.

        Called when a user connects a WebSocket to THIS server.
        If this is their first connection on this server, we
        subscribe to their channel. If they already have other
        connections here, we're already subscribed — skip.

        Channel name: "deliver:{user_id}"
        """
        if user_id in self._subscribed_users:
            return  # Already subscribed on this server

        if not self._pubsub:
            logger.error("Pub/Sub not initialized")
            return

        try:
            await self._pubsub.subscribe(f"deliver:{user_id}")
            self._subscribed_users.add(user_id)
            logger.debug(f"Subscribed to deliver:{user_id}")
        except Exception as e:
            logger.error(
                f"Failed to subscribe deliver:{user_id}: {e}"
            )

    async def unsubscribe_user(self, user_id: str) -> None:
        """Unsubscribe when a user has NO more connections here.

        Only unsubscribe if the user has zero remaining local
        connections. If they still have another tab open on
        this server, keep the subscription.
        """
        if not self._registry:
            return

        # Check if user still has local connections
        remaining = self._registry.get_user_connections(user_id)
        if remaining:
            return  # Still has connections, keep subscription

        if user_id not in self._subscribed_users:
            return  # Not subscribed anyway

        if not self._pubsub:
            return

        try:
            await self._pubsub.unsubscribe(f"deliver:{user_id}")
            self._subscribed_users.discard(user_id)
            logger.debug(f"Unsubscribed from deliver:{user_id}")
        except Exception as e:
            logger.error(
                f"Failed to unsubscribe deliver:{user_id}: {e}"
            )

    # ── Listener Loop ─────────────────────────────────────────

    async def _listen_loop(self) -> None:
        """Async loop that receives Pub/Sub messages.

        get_message() is non-blocking with a short timeout.
        When a message arrives on "deliver:{user_id}", we
        extract the user_id from the channel name, look up
        their local WebSocket connections, and send_text().

        We wait until at least one user has subscribed before
        polling — Redis raises an error if you call get_message()
        with zero subscriptions.
        """
        while self._running:
            try:
                # Don't poll until someone has subscribed.
                # Without this, get_message() throws because
                # the pub/sub connection isn't active yet.
                if not self._subscribed_users:
                    await asyncio.sleep(0.5)
                    continue

                message = await self._pubsub.get_message(
                    ignore_subscribe_messages=True,
                    timeout=1.0,
                )

                if message is None:
                    continue

                if message["type"] != "message":
                    continue

                channel = message["channel"]  # "deliver:bob"
                data = message["data"]        # JSON payload

                # Extract user_id from channel name
                if not channel.startswith("deliver:"):
                    continue
                user_id = channel[len("deliver:"):]

                await self._deliver_to_local(user_id, data)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Pub/Sub listener error: {e}")
                await asyncio.sleep(1.0)

    async def _deliver_to_local(
        self, user_id: str, payload: str
    ) -> None:
        """Send a pub/sub message to the user's local WebSockets.

        Same logic as DeliveryService._deliver_local, but
        triggered by Redis Pub/Sub instead of Kafka consumption.
        """
        if not self._registry:
            return

        connections = self._registry.get_user_connections(user_id)
        if not connections:
            # User disconnected between publish and receive.
            # Normal — we'll unsubscribe on next cleanup cycle.
            return

        sent = 0
        for conn in connections:
            try:
                await conn.websocket.send_text(payload)
                sent += 1
            except Exception as e:
                logger.debug(
                    f"Pub/Sub WS send failed for {user_id}: {e}"
                )
                try:
                    await self._registry.remove_connection(
                        conn.websocket
                    )
                except Exception:
                    pass

        if sent > 0:
            # Parse just enough to log the correlation_id
            try:
                msg = json.loads(payload)
                logger.info(
                    f"[{msg.get('correlationId', '?')}] "
                    f"Pub/Sub delivered to {user_id}: "
                    f"{sent} connection(s)"
                )
            except Exception:
                logger.info(
                    f"Pub/Sub delivered to {user_id}: "
                    f"{sent} connection(s)"
                )

    # ── Stats ─────────────────────────────────────────────────

    def stats(self) -> dict:
        return {
            "running": self._running,
            "subscribed_users": len(self._subscribed_users),
            "subscriptions": list(self._subscribed_users),
        }


# ── Module singleton ──────────────────────────────────────────
pubsub_subscriber = PubSubSubscriber()
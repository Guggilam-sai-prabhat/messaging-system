"""
Delivery Service — Kafka consumer → WebSocket fan-out.

Architecture:
  The delivery service runs a confluent-kafka consumer in a
  dedicated thread (same reason as the producer poller: the
  consumer's poll() is a blocking C call that would freeze
  the asyncio event loop).

  ┌─────────────────────────────────────────────────────┐
  │            Kafka Topic: channel-messages             │
  │  ┌──────┐ ┌──────┐ ┌──────┐ ┌──────┐ ┌──────┐      │
  │  │  P0  │ │  P1  │ │  P2  │ │  P3  │ │  P4  │      │
  │  └──┬───┘ └──┬───┘ └──┬───┘ └──┬───┘ └──┬───┘      │
  └─────┼────────┼────────┼────────┼────────┼───────────┘
        │        │        │        │        │
  ┌─────▼────────▼────────▼────────▼────────▼───────────┐
  │         Consumer Thread (blocking poll loop)         │
  │                                                      │
  │   poll(1.0) → deserialize → enqueue to asyncio       │
  │              via call_soon_threadsafe                 │
  └──────────────────────┬──────────────────────────────┘
                         │
                         ▼
  ┌──────────────────────────────────────────────────────┐
  │        Async Delivery Pipeline (event loop)          │
  │                                                      │
  │  1. Get channel members from Redis                   │
  │  2. For each member:                                 │
  │     a. LOCAL connections? → send directly via WS     │
  │     b. No local conn? → publish to Redis Pub/Sub     │
  │        (other WS servers pick it up)                 │
  │  3. Commit offset                                    │
  └──────────────────────────────────────────────────────┘

Why a thread for the consumer?
  confluent-kafka-python's poll() is a blocking call into
  librdkafka (C library). If we await it on the event loop,
  everything else freezes — no WebSocket sends, no heartbeats,
  nothing. A dedicated thread lets poll() block freely while
  the event loop continues serving connections.

Why NOT just run the consumer in a separate process?
  You could. But then you'd ALWAYS need Redis Pub/Sub for
  every delivery, even to local users. Co-locating the
  consumer with the WS server means local users get direct
  delivery (fastest path), and only remote users go through
  pub/sub. This is a common optimization.

Consumer group: "delivery-group"
  All delivery service instances share this group. Kafka
  assigns partitions across them. If one dies, Kafka
  rebalances its partitions to survivors. Messages are
  never lost — just delayed until rebalance completes
  (typically seconds).

Offset management: manual commit after delivery
  We disable auto-commit. Offsets are committed only AFTER
  the message has been delivered (or skipped for offline
  users). This means if we crash mid-delivery, the message
  will be re-consumed after rebalance. This is "at-least-once"
  delivery — the client-side dedup (client_request_id) handles
  the rare duplicate.
"""

import json
import logging
import asyncio
import threading
import time
from typing import Optional

from confluent_kafka import Consumer, KafkaException, TopicPartition

from app.config import settings
from app.core.redis_client import redis_client

logger = logging.getLogger("delivery")

MESSAGES_TOPIC = "channel-messages"
CONSUMER_GROUP = "delivery-group"


class DeliveryService:
    """Consumes from Kafka, delivers to WebSocket connections."""

    def __init__(self):
        self._consumer: Optional[Consumer] = None
        self._consumer_thread: Optional[threading.Thread] = None
        self._running = False
        self._loop: Optional[asyncio.AbstractEventLoop] = None

        # ── Dedup: track recently delivered message IDs ────────
        # Why? "At-least-once" from Kafka means after a crash
        # and rebalance, we might re-consume a message we already
        # delivered. This set catches that. TTL cleanup keeps it
        # from growing forever.
        self._delivered_ids: dict[str, float] = {}  # msg_id → timestamp
        self._dedup_ttl = 300.0  # 5 minutes

        # ── Registry reference (set during startup) ───────────
        # The delivery service needs access to the connection
        # registry to find local WebSocket connections.
        self._registry = None

    def set_registry(self, registry) -> None:
        """Inject the connection registry. Called during app startup.

        Why injection instead of importing directly?
          Avoids circular imports. The registry module doesn't
          need to know about delivery, and delivery doesn't
          import registry at module level.
        """
        self._registry = registry

    # ── Lifecycle ─────────────────────────────────────────────

    def start(self) -> None:
        """Initialize the Kafka consumer. Call from lifespan."""
        conf = {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": CONSUMER_GROUP,
            "auto.offset.reset": "latest",
            # ── Manual offset commit ──────────────────────────
            # We commit AFTER successful delivery. If we crash
            # before committing, Kafka re-delivers on rebalance.
            # This is the "at-least-once" guarantee.
            "enable.auto.commit": False,
            # ── Session/heartbeat tuning ──────────────────────
            # session.timeout.ms: if the broker doesn't hear a
            #   heartbeat in this window, it considers us dead
            #   and triggers rebalance.
            # heartbeat.interval.ms: how often we ping the broker.
            #   Rule of thumb: 1/3 of session timeout.
            # max.poll.interval.ms: max time between poll() calls
            #   before broker assumes we're stuck. Set higher than
            #   your worst-case processing time.
            "session.timeout.ms": 30000,
            "heartbeat.interval.ms": 10000,
            "max.poll.interval.ms": 300000,
            # ── Performance ───────────────────────────────────
            "fetch.min.bytes": 1,
            "fetch.wait.max.ms": 500,
        }
        self._consumer = Consumer(conf)
        self._consumer.subscribe(
            [MESSAGES_TOPIC],
            on_assign=self._on_assign,
            on_revoke=self._on_revoke,
        )
        logger.info(
            f"Kafka consumer initialized, group={CONSUMER_GROUP}, "
            f"topic={MESSAGES_TOPIC}"
        )

    def _on_assign(self, consumer, partitions: list[TopicPartition]):
        """Called when partitions are assigned to this consumer.

        This fires during rebalance — when a new consumer joins
        the group or an old one dies. Useful for logging and
        initializing per-partition state.
        """
        parts = [f"P{p.partition}" for p in partitions]
        logger.info(f"Partitions assigned: {parts}")

    def _on_revoke(self, consumer, partitions: list[TopicPartition]):
        """Called when partitions are revoked during rebalance.

        Commit offsets for any partitions we're losing. Otherwise
        the next consumer to get these partitions will re-process
        already-delivered messages.
        """
        parts = [f"P{p.partition}" for p in partitions]
        logger.info(f"Partitions revoked: {parts}")
        try:
            consumer.commit(asynchronous=False)
        except Exception as e:
            logger.error(f"Commit on revoke failed: {e}")

    def start_consumer_thread(self) -> None:
        """Launch the blocking consumer loop in a dedicated thread."""
        self._loop = asyncio.get_running_loop()
        self._running = True
        self._consumer_thread = threading.Thread(
            target=self._consume_loop,
            name="kafka-delivery-consumer",
            daemon=True,
        )
        self._consumer_thread.start()
        logger.info("Delivery consumer thread started")

    def _consume_loop(self) -> None:
        """Blocking loop in a dedicated thread.

        poll(1.0) blocks for up to 1 second waiting for messages.
        When a message arrives, we hand it to the async pipeline
        via call_soon_threadsafe (crosses thread → event loop
        boundary safely).

        Periodic tasks:
          - Dedup cleanup every 60 seconds
          - Could add health checks, metrics, etc.
        """
        last_cleanup = time.monotonic()

        while self._running:
            try:
                msg = self._consumer.poll(1.0)

                if msg is None:
                    continue

                if msg.error():
                    logger.error(f"Consumer error: {msg.error()}")
                    continue

                # Deserialize
                try:
                    payload = json.loads(msg.value().decode("utf-8"))
                except (json.JSONDecodeError, UnicodeDecodeError) as e:
                    logger.error(
                        f"Bad message at {msg.topic()}/"
                        f"{msg.partition()}/{msg.offset()}: {e}"
                    )
                    self._commit_offset(msg)
                    continue

                logger.info(
                    f"Consumed: partition={msg.partition()} "
                    f"offset={msg.offset()} "
                    f"channel={payload.get('channelId')} "
                    f"sender={payload.get('senderId')}"
                )

                # Schedule async delivery on the event loop.
                #
                # _safe_deliver wraps _deliver_and_commit with
                # exception logging. Without this, any error in
                # the delivery pipeline gets silently swallowed
                # by the asyncio task — you'd never see it.
                coro = self._safe_deliver(payload, msg)
                self._loop.call_soon_threadsafe(
                    self._loop.create_task, coro
                )

                # Periodic dedup cleanup
                now = time.monotonic()
                if now - last_cleanup > 60.0:
                    self._cleanup_dedup()
                    last_cleanup = now

            except Exception as e:
                logger.error(f"Consumer loop error: {e}")
                time.sleep(1.0)

        # Clean shutdown
        if self._consumer:
            try:
                self._consumer.commit(asynchronous=False)
            except Exception:
                pass
            self._consumer.close()
            logger.info("Consumer closed")

    # ── Async Delivery Pipeline ───────────────────────────────

    async def _safe_deliver(self, payload: dict, kafka_msg) -> None:
        """Wrapper that catches and logs any delivery errors.

        Without this, exceptions in _deliver_and_commit are
        swallowed silently by asyncio — the task fails but
        nobody retrieves the exception. This is the #1 reason
        async pipelines appear to "do nothing."
        """
        try:
            await self._deliver_and_commit(payload, kafka_msg)
        except Exception as e:
            logger.error(
                f"Delivery pipeline error: {e}",
                exc_info=True,
            )

    async def _deliver_and_commit(self, payload: dict, kafka_msg) -> None:
        """The core delivery logic. Runs on the event loop.

        Steps:
          1. Dedup check (skip if already delivered)
          2. Look up channel members from Redis
          3. For each member:
             - If local WebSocket exists → send directly
             - If not local → publish via Redis Pub/Sub
          4. Commit offset
        """
        message_id = payload.get("messageId", "unknown")
        channel_id = payload.get("channelId")
        sender_id = payload.get("senderId")
        correlation_id = payload.get("correlationId", "?")

        if not channel_id:
            logger.error(
                f"[{correlation_id}] Message missing channelId, "
                f"skipping: {message_id}"
            )
            self._commit_offset(kafka_msg)
            return

        # ── Step 1: Dedup ─────────────────────────────────────
        if message_id in self._delivered_ids:
            logger.info(
                f"[{correlation_id}] Duplicate message {message_id}, "
                f"skipping"
            )
            self._commit_offset(kafka_msg)
            return

        # ── Step 2: Get channel members ───────────────────────
        try:
            members = await redis_client.redis.smembers(
                f"channel:{channel_id}:members"
            )
        except Exception as e:
            logger.error(
                f"[{correlation_id}] Redis lookup failed for "
                f"channel {channel_id}: {e}"
            )
            # DON'T commit — we'll retry on next poll.
            # The consumer will re-deliver this message.
            return

        if not members:
            logger.debug(
                f"[{correlation_id}] No members in channel "
                f"{channel_id}"
            )
            self._mark_delivered(message_id)
            self._commit_offset(kafka_msg)
            return

        # ── Step 3: Deliver to each member ────────────────────
        # Build the outbound payload once (don't re-serialize
        # per recipient).
        outbound = json.dumps({
            "type": "message.received",
            "messageId": message_id,
            "channelId": channel_id,
            "senderId": sender_id,
            "content": payload.get("content", ""),
            "timestamp": payload.get("timestamp"),
            "correlationId": correlation_id,
        })

        delivered_count = 0
        remote_count = 0

        for user_id in members:
            # Skip sending back to the sender — they already
            # have optimistic confirmation from the ingest ack.
            if user_id == sender_id:
                continue

            # Try local delivery first (fast path)
            local_sent = await self._deliver_local(
                user_id, outbound, correlation_id
            )

            if local_sent:
                delivered_count += local_sent
            else:
                # User not on this server → Redis Pub/Sub
                await self._deliver_remote(
                    user_id, outbound, correlation_id
                )
                remote_count += 1

        logger.info(
            f"[{correlation_id}] Delivered {message_id} to "
            f"channel {channel_id}: "
            f"{delivered_count} local, {remote_count} remote"
        )

        # ── Step 4: Mark delivered + commit ───────────────────
        self._mark_delivered(message_id)
        self._commit_offset(kafka_msg)

    async def _deliver_local(
        self,
        user_id: str,
        payload: str,
        correlation_id: str,
    ) -> int:
        """Send to user's local WebSocket connections.

        Returns number of successful sends, 0 if user has no
        local connections.

        Why send to ALL connections for a user?
          A user might have multiple tabs/devices connected to
          the same server. Each one should receive the message.

        Handling closed connections:
          WebSocket.send_text() can raise if the connection
          died between the last health check and now. We catch
          per-connection so one dead connection doesn't kill
          delivery to the user's other connections.
        """
        if not self._registry:
            return 0

        connections = self._registry.get_user_connections(user_id)
        if not connections:
            return 0

        sent = 0
        for conn in connections:
            try:
                await conn.websocket.send_text(payload)
                sent += 1
            except Exception as e:
                # Connection is dead. Remove it from registry.
                # This is expected — connections die between
                # heartbeats. Not an error, just cleanup.
                logger.debug(
                    f"[{correlation_id}] WS send failed for "
                    f"user {user_id}: {e}"
                )
                try:
                    await self._registry.remove_connection(
                        conn.websocket
                    )
                except Exception:
                    pass  # Best effort cleanup
        return sent

    async def _deliver_remote(
        self,
        user_id: str,
        payload: str,
        correlation_id: str,
    ) -> None:
        """Publish to Redis Pub/Sub for other WS servers.

        The channel name convention: "deliver:{user_id}"
        Each WS server subscribes to channels for its local
        users. When a message arrives on a user's channel,
        the server sends it down the user's WebSocket.

        If the user is offline everywhere, nobody is subscribed
        to their channel, and Redis just drops the message.
        That's fine — the message is already persisted in Kafka
        (and will be in the DB via separate persistence service).
        The client fetches missed messages on reconnect.
        """
        try:
            await redis_client.redis.publish(
                f"deliver:{user_id}",
                payload,
            )
        except Exception as e:
            logger.error(
                f"[{correlation_id}] Redis publish failed for "
                f"user {user_id}: {e}"
            )
            # Not fatal. User will get the message from DB
            # on next reconnect/fetch.

    # ── Offset Management ─────────────────────────────────────

    def _commit_offset(self, kafka_msg) -> None:
        """Commit the offset for a consumed message.

        Synchronous commit from the consumer thread context
        (called via the async pipeline, but commit itself is
        thread-safe on the consumer object).

        Why commit per-message instead of batching?
          Simplicity first. Per-message commit means on crash
          we re-process at most 1 message. Batched commits
          mean re-processing the whole batch. The throughput
          cost of per-message commits is modest for messaging
          workloads (not high-throughput event streaming).

          Optimization: batch commits every N messages or
          every T seconds. Add this when profiling shows
          commit overhead matters.
        """
        try:
            self._consumer.commit(
                offsets=[
                    TopicPartition(
                        kafka_msg.topic(),
                        kafka_msg.partition(),
                        kafka_msg.offset() + 1,
                    )
                ],
                asynchronous=False,
            )
        except Exception as e:
            logger.error(f"Offset commit failed: {e}")

    # ── Dedup Helpers ─────────────────────────────────────────

    def _mark_delivered(self, message_id: str) -> None:
        self._delivered_ids[message_id] = time.monotonic()

    def _cleanup_dedup(self) -> None:
        """Evict expired entries from the dedup set."""
        cutoff = time.monotonic() - self._dedup_ttl
        expired = [
            mid for mid, ts in self._delivered_ids.items()
            if ts < cutoff
        ]
        for mid in expired:
            del self._delivered_ids[mid]
        if expired:
            logger.debug(f"Dedup cleanup: evicted {len(expired)} entries")

    # ── Shutdown ──────────────────────────────────────────────

    def shutdown(self) -> None:
        """Stop consumer thread gracefully."""
        self._running = False
        if (
            self._consumer_thread
            and self._consumer_thread.is_alive()
        ):
            self._consumer_thread.join(timeout=5.0)
        logger.info("Delivery service shut down")

    # ── Stats ─────────────────────────────────────────────────

    def stats(self) -> dict:
        return {
            "running": self._running,
            "dedup_cache_size": len(self._delivered_ids),
        }


# ── Module singleton ──────────────────────────────────────────
delivery_service = DeliveryService()
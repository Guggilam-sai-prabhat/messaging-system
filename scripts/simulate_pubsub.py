"""
Redis Pub/Sub Cross-Server Simulation

The problem with testing Pub/Sub on a single server:
  The delivery service finds users locally and sends directly.
  Pub/Sub never gets triggered because _deliver_local succeeds.

To force the Pub/Sub path, this script bypasses the normal
ingest pipeline entirely. Instead of sending messages through
WebSockets, it:
  1. Connects real WebSocket clients (so Pub/Sub subscribes)
  2. Publishes directly to Redis "deliver:{user_id}" channels
  3. Checks if the WebSocket clients receive the messages

This simulates what happens when the delivery service on
Server 1 publishes to Redis because the user is on Server 2.
We ARE Server 2 in this test — our Pub/Sub subscriber should
pick up the message and deliver it to the local WebSocket.

  ┌──────────────────────────────────────────────────────┐
  │  What normally happens (multi-server):               │
  │                                                      │
  │  Server 1: Delivery consumer                         │
  │    → user not local                                  │
  │    → PUBLISH "deliver:alice" to Redis                │
  │                                                      │
  │  Server 2: Pub/Sub subscriber                        │
  │    → receives message                                │
  │    → finds alice's local WebSocket                   │
  │    → send_text()                                     │
  │                                                      │
  │  What THIS script does:                              │
  │                                                      │
  │  Script: acts as "Server 1"                          │
  │    → PUBLISH "deliver:alice" to Redis directly       │
  │                                                      │
  │  Your server: acts as "Server 2"                     │
  │    → Pub/Sub subscriber receives it                  │
  │    → finds alice's local WebSocket                   │
  │    → send_text()                                     │
  │                                                      │
  │  Script: checks alice's WebSocket got the message    │
  └──────────────────────────────────────────────────────┘

Prerequisites:
  - Your FastAPI server running: uvicorn app.main:app --port 8000
  - Redis running

Usage:
  python scripts/simulate_pubsub.py
"""

import json
import time
import asyncio
import uuid
import sys
from dataclasses import dataclass, field

import websockets
import redis.asyncio as aioredis

# ── Config ────────────────────────────────────────────────────

WS_URL = "ws://localhost:8000/ws"
REDIS_URL = "redis://:redis@localhost:6379/0"
RECEIVE_TIMEOUT = 10.0

MOCK_USERS = {
    "alice": [
        {"token": "token-alice-1", "device_id": "alice-phone"},
        {"token": "token-alice-2", "device_id": "alice-laptop"},
    ],
    "bob": [
        {"token": "token-bob-1", "device_id": "bob-phone"},
    ],
}


# ── Helpers ───────────────────────────────────────────────────

def log(msg: str, level: str = "INFO"):
    colors = {
        "INFO": "\033[36m",
        "OK": "\033[32m",
        "FAIL": "\033[31m",
        "WARN": "\033[33m",
        "SEND": "\033[35m",
        "RECV": "\033[34m",
        "PUB": "\033[33m",
    }
    reset = "\033[0m"
    color = colors.get(level, "")
    ts = time.strftime("%H:%M:%S")
    print(f"  {color}[{ts}] [{level:4s}]{reset} {msg}")


def header(title: str):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")


def result(name: str, passed: bool, detail: str = ""):
    status = "\033[32mPASS\033[0m" if passed else "\033[31mFAIL\033[0m"
    suffix = f" — {detail}" if detail else ""
    print(f"  [{status}] {name}{suffix}")


# ── Simulated Client (same as delivery simulation) ───────────

@dataclass
class SimulatedClient:
    user_id: str
    token: str
    ws_url: str
    device_id: str = "unknown"
    ws: object = None
    received: list = field(default_factory=list)
    _listener_task: object = None
    connected: bool = False

    async def connect(self) -> bool:
        url = f"{self.ws_url}?token={self.token}"
        try:
            self.ws = await websockets.connect(url)
            self.connected = True
            self._listener_task = asyncio.create_task(self._listen())
            log(f"{self.user_id} ({self.device_id}) connected")
            return True
        except Exception as e:
            log(f"{self.user_id} connection failed: {e}", "FAIL")
            return False

    async def _listen(self):
        try:
            async for raw in self.ws:
                msg = json.loads(raw)
                self.received.append(msg)
                log(
                    f"{self.user_id} ({self.device_id}) received: "
                    f"type={msg.get('type')} "
                    f"content=\"{msg.get('content', '')[:50]}\"",
                    "RECV",
                )
        except websockets.ConnectionClosed:
            log(f"{self.user_id} ({self.device_id}) connection closed", "WARN")
        except Exception as e:
            log(f"{self.user_id} ({self.device_id}) listener error: {e}", "WARN")
        finally:
            self.connected = False

    async def join_channel(self, channel_id: str):
        await self.ws.send(json.dumps({
            "type": "channel.join",
            "channel_id": channel_id,
        }))
        log(f"{self.user_id} ({self.device_id}) joined #{channel_id}", "SEND")
        await asyncio.sleep(0.3)

    async def wait_for_message(
        self,
        msg_type: str = "message.received",
        timeout: float = RECEIVE_TIMEOUT,
        count: int = 1,
    ) -> list[dict]:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            matching = [
                m for m in self.received
                if m.get("type") == msg_type
            ]
            if len(matching) >= count:
                return matching[:count]
            await asyncio.sleep(0.1)
        return [
            m for m in self.received
            if m.get("type") == msg_type
        ]

    async def disconnect(self):
        if self._listener_task:
            self._listener_task.cancel()
            try:
                await self._listener_task
            except asyncio.CancelledError:
                pass
        if self.ws:
            await self.ws.close()
        self.connected = False
        log(f"{self.user_id} ({self.device_id}) disconnected")

    def clear(self):
        self.received.clear()


def make_client(user_id: str, token_index: int = 0) -> SimulatedClient:
    tokens = MOCK_USERS.get(user_id, [])
    if not tokens or token_index >= len(tokens):
        raise ValueError(f"No mock token for {user_id} at index {token_index}")
    entry = tokens[token_index]
    return SimulatedClient(
        user_id=user_id,
        token=entry["token"],
        device_id=entry["device_id"],
        ws_url=WS_URL,
    )


# ── Redis Publisher (simulates "other server" delivery) ───────

class RemotePublisher:
    """Simulates what a delivery service on another server does.

    Publishes directly to Redis Pub/Sub channels, bypassing
    Kafka and the local delivery service entirely. This is
    exactly what _deliver_remote() does in delivery_service.py.
    """

    def __init__(self):
        self.redis = None

    async def connect(self):
        self.redis = aioredis.from_url(
            REDIS_URL, encoding="utf-8", decode_responses=True
        )
        await self.redis.ping()
        log("Redis publisher connected")

    async def publish_to_user(
        self,
        user_id: str,
        channel_id: str,
        sender_id: str,
        content: str,
        message_id: str = None,
        correlation_id: str = None,
    ) -> int:
        """Publish a message to deliver:{user_id}.

        Returns the number of subscribers who received it.
        If 0, nobody is listening (user not connected anywhere).
        """
        payload = json.dumps({
            "type": "message.received",
            "messageId": message_id or str(uuid.uuid4()),
            "channelId": channel_id,
            "senderId": sender_id,
            "content": content,
            "timestamp": time.time(),
            "correlationId": correlation_id or str(uuid.uuid4()),
        })

        receiver_count = await self.redis.publish(
            f"deliver:{user_id}", payload
        )
        log(
            f"Published to deliver:{user_id} "
            f"({receiver_count} subscriber(s))",
            "PUB",
        )
        return receiver_count

    async def close(self):
        if self.redis:
            await self.redis.close()


# ══════════════════════════════════════════════════════════════
# Scenarios
# ══════════════════════════════════════════════════════════════

async def scenario_basic_pubsub(publisher: RemotePublisher) -> bool:
    """Scenario 1: Basic Pub/Sub delivery.

    Alice connects via WebSocket (triggers Pub/Sub subscription).
    We publish directly to Redis deliver:alice.
    Alice should receive the message on her WebSocket.

    This simulates: delivery service on Server 1 publishes,
    Alice on Server 2 (our server) receives.
    """
    header("Scenario 1: Basic Pub/Sub Delivery")

    alice = make_client("alice")

    try:
        await alice.connect()
        # Need to join a channel so the server subscribes
        await alice.join_channel("pubsub-test")
        # Wait for subscription to be active
        await asyncio.sleep(0.5)

        # Simulate a remote server publishing to alice
        receivers = await publisher.publish_to_user(
            user_id="alice",
            channel_id="pubsub-test",
            sender_id="bob",
            content="Hello from another server!",
        )

        sub_ok = receivers >= 1
        result(
            "Redis has subscriber for alice",
            sub_ok,
            f"{receivers} subscriber(s)",
        )

        messages = await alice.wait_for_message(timeout=5.0)
        delivery_ok = (
            len(messages) >= 1
            and messages[0].get("content") == "Hello from another server!"
            and messages[0].get("senderId") == "bob"
        )

        result(
            "Alice received via Pub/Sub",
            delivery_ok,
            f"got {len(messages)} message(s)"
            + (f": \"{messages[0].get('content')}\"" if messages else ""),
        )

        return sub_ok and delivery_ok
    finally:
        await alice.disconnect()


async def scenario_multi_device_pubsub(publisher: RemotePublisher) -> bool:
    """Scenario 2: Pub/Sub to multiple devices.

    Alice connects on phone AND laptop.
    Publish to deliver:alice once.
    Both devices should receive the message.
    """
    header("Scenario 2: Multi-Device Pub/Sub")

    phone = make_client("alice", token_index=0)
    laptop = make_client("alice", token_index=1)

    try:
        await phone.connect()
        await phone.join_channel("pubsub-test-2")
        await laptop.connect()
        await laptop.join_channel("pubsub-test-2")
        await asyncio.sleep(0.5)

        receivers = await publisher.publish_to_user(
            user_id="alice",
            channel_id="pubsub-test-2",
            sender_id="charlie",
            content="Both devices should get this",
        )

        # Should be 1 subscriber (the server subscribes once
        # per user, not once per connection)
        result(
            "Redis subscriber active",
            receivers >= 1,
            f"{receivers} subscriber(s)",
        )

        phone_msgs = await phone.wait_for_message(timeout=5.0)
        laptop_msgs = await laptop.wait_for_message(timeout=5.0)

        phone_ok = (
            len(phone_msgs) >= 1
            and phone_msgs[0].get("content") == "Both devices should get this"
        )
        laptop_ok = (
            len(laptop_msgs) >= 1
            and laptop_msgs[0].get("content") == "Both devices should get this"
        )

        result("Alice-phone received", phone_ok)
        result("Alice-laptop received", laptop_ok)

        return phone_ok and laptop_ok
    finally:
        await phone.disconnect()
        await laptop.disconnect()


async def scenario_offline_pubsub(publisher: RemotePublisher) -> bool:
    """Scenario 3: Pub/Sub to offline user.

    Nobody is connected.
    Publish to deliver:ghost_user.
    Redis should return 0 subscribers.
    No crash, message silently dropped.
    """
    header("Scenario 3: Offline User — No Subscriber")

    receivers = await publisher.publish_to_user(
        user_id="ghost_user_nobody_connected",
        channel_id="whatever",
        sender_id="bob",
        content="This should go nowhere",
    )

    no_subscriber = receivers == 0
    result(
        "Zero subscribers for offline user",
        no_subscriber,
        f"{receivers} subscriber(s)",
    )

    result("No crash", True, "message silently dropped")
    return no_subscriber


async def scenario_disconnect_unsubscribe(publisher: RemotePublisher) -> bool:
    """Scenario 4: Unsubscribe after disconnect.

    Alice connects → subscribed to deliver:alice.
    Alice disconnects → should unsubscribe.
    Publish to deliver:alice → 0 subscribers.
    """
    header("Scenario 4: Unsubscribe After Disconnect")

    alice = make_client("alice")

    try:
        await alice.connect()
        await alice.join_channel("pubsub-test-4")
        await asyncio.sleep(0.5)

        # Verify subscription is active
        receivers_before = await publisher.publish_to_user(
            user_id="alice",
            channel_id="pubsub-test-4",
            sender_id="bob",
            content="Before disconnect",
        )
        before_ok = receivers_before >= 1
        result(
            "Subscribed while connected",
            before_ok,
            f"{receivers_before} subscriber(s)",
        )

        msgs = await alice.wait_for_message(timeout=5.0)
        got_before = len(msgs) >= 1
        result("Received while connected", got_before)

    finally:
        await alice.disconnect()

    # Wait for server to process disconnect and unsubscribe
    await asyncio.sleep(1.0)

    # Now publish again — should have 0 subscribers
    receivers_after = await publisher.publish_to_user(
        user_id="alice",
        channel_id="pubsub-test-4",
        sender_id="bob",
        content="After disconnect — should go nowhere",
    )

    after_ok = receivers_after == 0
    result(
        "Unsubscribed after disconnect",
        after_ok,
        f"{receivers_after} subscriber(s)"
        + (" (still subscribed!)" if not after_ok else ""),
    )

    return before_ok and got_before and after_ok


async def scenario_rapid_pubsub(publisher: RemotePublisher) -> bool:
    """Scenario 5: Rapid-fire Pub/Sub messages.

    Alice connects. We publish 20 messages in quick succession.
    Alice should receive all 20, in order.
    """
    header("Scenario 5: Rapid-Fire Pub/Sub (20 messages)")

    alice = make_client("alice")
    msg_count = 20

    try:
        await alice.connect()
        await alice.join_channel("pubsub-test-5")
        await asyncio.sleep(0.5)

        for i in range(msg_count):
            await publisher.publish_to_user(
                user_id="alice",
                channel_id="pubsub-test-5",
                sender_id="bot",
                content=f"rapid-{i:03d}",
            )
            # No sleep — fire as fast as possible

        messages = await alice.wait_for_message(
            timeout=10.0, count=msg_count
        )

        count_ok = len(messages) == msg_count
        result(
            f"Received all {msg_count} messages",
            count_ok,
            f"got {len(messages)}/{msg_count}",
        )

        if count_ok:
            contents = [m.get("content") for m in messages]
            expected = [f"rapid-{i:03d}" for i in range(msg_count)]
            order_ok = contents == expected
            result(
                "Messages in correct order",
                order_ok,
                f"{'correct' if order_ok else contents[:5]}...",
            )
            return order_ok

        return False
    finally:
        await alice.disconnect()


async def scenario_mixed_delivery(publisher: RemotePublisher) -> bool:
    """Scenario 6: Mixed local + Pub/Sub delivery.

    Alice and Bob connect, join a channel.
    Bob sends a real message (goes through Kafka → local delivery).
    THEN we publish via Redis Pub/Sub to alice (simulating remote).
    Alice should receive BOTH messages — one from each path.

    This tests that local delivery and Pub/Sub delivery
    don't interfere with each other.
    """
    header("Scenario 6: Mixed Local + Pub/Sub Delivery")

    alice = make_client("alice")
    bob = make_client("bob")
    channel = f"mixed-{uuid.uuid4().hex[:6]}"

    try:
        await alice.connect()
        await bob.connect()
        await alice.join_channel(channel)
        await bob.join_channel(channel)
        await asyncio.sleep(0.5)

        # Path 1: Normal message through Kafka (local delivery)
        await bob.ws.send(json.dumps({
            "type": "message.send",
            "channel_id": channel,
            "content": "local delivery path",
        }))
        log("Bob sent via normal WebSocket path", "SEND")

        local_msgs = await alice.wait_for_message(timeout=5.0)
        local_ok = (
            len(local_msgs) >= 1
            and local_msgs[0].get("content") == "local delivery path"
        )
        result("Alice received via local delivery", local_ok)

        # Path 2: Direct Redis Pub/Sub (simulating remote server)
        alice.clear()
        await publisher.publish_to_user(
            user_id="alice",
            channel_id=channel,
            sender_id="remote-charlie",
            content="pubsub delivery path",
        )

        pubsub_msgs = await alice.wait_for_message(timeout=5.0)
        pubsub_ok = (
            len(pubsub_msgs) >= 1
            and pubsub_msgs[0].get("content") == "pubsub delivery path"
        )
        result("Alice received via Pub/Sub", pubsub_ok)

        return local_ok and pubsub_ok
    finally:
        await alice.disconnect()
        await bob.disconnect()


# ══════════════════════════════════════════════════════════════
# Runner
# ══════════════════════════════════════════════════════════════

async def run_all():
    print("\n\033[1m  Redis Pub/Sub — Cross-Server Simulation\033[0m")
    print(f"  Server: {WS_URL}")
    print(f"  Redis:  {REDIS_URL}")
    print(f"  Time:   {time.strftime('%Y-%m-%d %H:%M:%S')}")

    publisher = RemotePublisher()
    await publisher.connect()

    scenarios = [
        ("Basic Pub/Sub", scenario_basic_pubsub),
        ("Multi-Device Pub/Sub", scenario_multi_device_pubsub),
        ("Offline User", scenario_offline_pubsub),
        ("Unsubscribe After Disconnect", scenario_disconnect_unsubscribe),
        ("Rapid-Fire Pub/Sub", scenario_rapid_pubsub),
        ("Mixed Local + Pub/Sub", scenario_mixed_delivery),
    ]

    results = []
    for name, fn in scenarios:
        try:
            passed = await fn(publisher)
            results.append((name, passed))
        except Exception as e:
            log(f"Scenario crashed: {e}", "FAIL")
            import traceback
            traceback.print_exc()
            results.append((name, False))
        await asyncio.sleep(1.0)

    await publisher.close()

    # ── Summary ───────────────────────────────────────────────
    header("Summary")
    passed = sum(1 for _, p in results if p)
    total = len(results)

    for name, p in results:
        status = "\033[32mPASS\033[0m" if p else "\033[31mFAIL\033[0m"
        print(f"  [{status}] {name}")

    print()
    color = "\033[32m" if passed == total else "\033[31m"
    print(f"  {color}{passed}/{total} scenarios passed\033[0m\n")

    return passed == total


def main():
    success = asyncio.run(run_all())
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
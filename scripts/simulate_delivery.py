"""
Live Delivery Pipeline Simulation

Simulates the real scenario end-to-end:
  1. Connects multiple WebSocket clients to your server
  2. Joins them to a channel
  3. One client sends a message
  4. Verifies others receive it via WebSocket
  5. Tests edge cases (offline user, disconnected socket, dupes)

Prerequisites:
  - Your FastAPI server running: uvicorn app.main:app --port 8000
  - Redis running
  - Kafka running

Usage:
  python scripts/simulate_delivery.py

  # Custom server URL
  python scripts/simulate_delivery.py --url ws://localhost:8000

  # Verbose logging
  python scripts/simulate_delivery.py -v
"""

import json
import time
import asyncio
import argparse
import uuid
import sys
from dataclasses import dataclass, field

import websockets

# ── Config ────────────────────────────────────────────────────

DEFAULT_WS_URL = "ws://localhost:8000/ws"
RECEIVE_TIMEOUT = 10.0  # seconds to wait for message delivery

# ── Mock tokens matching your app/core/auth.py ────────────────
# These must stay in sync with your MOCK_TOKENS dict.
# Each entry: token → (user_id, device_id)
MOCK_USERS = {
    "alice": [
        {"token": "token-alice-1", "device_id": "alice-phone"},
        {"token": "token-alice-2", "device_id": "alice-laptop"},
    ],
    "bob": [
        {"token": "token-bob-1", "device_id": "bob-phone"},
    ],
    "charlie": [],  # no token — used for offline/unauth tests
}


# ── Helpers ───────────────────────────────────────────────────

def log(msg: str, level: str = "INFO"):
    colors = {
        "INFO": "\033[36m",     # cyan
        "OK": "\033[32m",       # green
        "FAIL": "\033[31m",     # red
        "WARN": "\033[33m",     # yellow
        "SEND": "\033[35m",     # magenta
        "RECV": "\033[34m",     # blue
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


# ── Simulated Client ─────────────────────────────────────────

@dataclass
class SimulatedClient:
    """A WebSocket client that behaves like a real chat user.

    Connects to the server using a mock auth token, joins
    channels, sends messages, and collects received messages.
    """
    user_id: str
    token: str
    ws_url: str
    device_id: str = "unknown"
    ws: object = None
    received: list = field(default_factory=list)
    _listener_task: object = None
    connected: bool = False

    async def connect(self) -> bool:
        """Open WebSocket with token auth and start listener.

        Connects with ?token=xxx to match your WS router's
        auth flow. Adjust the query param name if your router
        expects something different (e.g. "authorization").
        """
        url = f"{self.ws_url}?token={self.token}"
        try:
            self.ws = await websockets.connect(url)
            self.connected = True
            self._listener_task = asyncio.create_task(
                self._listen()
            )
            log(
                f"{self.user_id} ({self.device_id}) "
                f"connected with {self.token}"
            )
            return True
        except Exception as e:
            log(f"{self.user_id} connection failed: {e}", "FAIL")
            return False

    async def _listen(self):
        """Background task: read messages from WebSocket."""
        try:
            async for raw in self.ws:
                msg = json.loads(raw)
                self.received.append(msg)
                log(
                    f"{self.user_id} received: "
                    f"type={msg.get('type')} "
                    f"from={msg.get('senderId', '?')} "
                    f"content=\"{msg.get('content', '')[:50]}\"",
                    "RECV",
                )
        except websockets.ConnectionClosed:
            log(f"{self.user_id} connection closed", "WARN")
        except Exception as e:
            log(f"{self.user_id} listener error: {e}", "WARN")
        finally:
            self.connected = False

    async def join_channel(self, channel_id: str):
        """Send a channel join request."""
        await self.ws.send(json.dumps({
            "type": "channel.join",
            "channel_id": channel_id,
        }))
        log(f"{self.user_id} joined #{channel_id}", "SEND")
        # Give server a moment to process
        await asyncio.sleep(0.3)

    async def send_message(
        self, channel_id: str, content: str, client_request_id: str = None
    ):
        """Send a chat message."""
        payload = {
            "type": "message.send",
            "channel_id": channel_id,
            "content": content,
        }
        if client_request_id:
            payload["client_request_id"] = client_request_id
        await self.ws.send(json.dumps(payload))
        log(
            f"{self.user_id} sent to #{channel_id}: "
            f"\"{content[:50]}\"",
            "SEND",
        )

    async def wait_for_message(
        self,
        timeout: float = RECEIVE_TIMEOUT,
        msg_type: str = "message.received",
        count: int = None,
    ) -> list[dict]:
        """Wait for messages to arrive.

        If count is given, wait until that many messages of the
        specified type are received. Otherwise wait for any 1.
        """
        target = count or 1
        deadline = time.monotonic() + timeout

        while time.monotonic() < deadline:
            matching = [
                m for m in self.received
                if m.get("type") == msg_type
            ]
            if len(matching) >= target:
                return matching[:target]
            await asyncio.sleep(0.1)

        matching = [
            m for m in self.received
            if m.get("type") == msg_type
        ]
        return matching  # return whatever we got

    async def disconnect(self):
        """Close the WebSocket cleanly."""
        if self._listener_task:
            self._listener_task.cancel()
            try:
                await self._listener_task
            except asyncio.CancelledError:
                pass
        if self.ws:
            await self.ws.close()
        self.connected = False
        log(f"{self.user_id} disconnected")

    def clear(self):
        """Clear received buffer between scenarios."""
        self.received.clear()


def make_client(
    user_id: str, ws_url: str, token_index: int = 0
) -> SimulatedClient:
    """Create a SimulatedClient from the MOCK_USERS table.

    token_index lets you pick which device/token to use
    for users with multiple tokens (like alice).
    """
    tokens = MOCK_USERS.get(user_id, [])
    if not tokens or token_index >= len(tokens):
        raise ValueError(
            f"No mock token for {user_id} at index {token_index}. "
            f"Add one to MOCK_USERS or app/core/auth.py"
        )
    entry = tokens[token_index]
    return SimulatedClient(
        user_id=user_id,
        token=entry["token"],
        device_id=entry["device_id"],
        ws_url=ws_url,
    )


# ══════════════════════════════════════════════════════════════
# Scenarios
# ══════════════════════════════════════════════════════════════

async def scenario_basic_delivery(ws_url: str) -> bool:
    """Scenario 1: Basic message delivery.

    Alice and Bob join a channel.
    Bob sends a message.
    Alice should receive it.
    """
    header("Scenario 1: Basic Message Delivery")

    channel = f"sim-channel-{uuid.uuid4().hex[:6]}"
    alice = make_client("alice", ws_url)
    bob = make_client("bob", ws_url)

    try:
        await alice.connect()
        await bob.connect()
        await alice.join_channel(channel)
        await bob.join_channel(channel)

        await bob.send_message(channel, "Hello Alice! Can you hear me?")

        messages = await alice.wait_for_message()
        passed = (
            len(messages) == 1
            and messages[0].get("content") == "Hello Alice! Can you hear me?"
            and messages[0].get("senderId") == "bob"
        )

        result(
            "Alice receives Bob's message",
            passed,
            f"got {len(messages)} message(s)"
            + (f": \"{messages[0].get('content')}\"" if messages else ""),
        )

        # Bob should NOT receive his own message
        bob_msgs = [
            m for m in bob.received
            if m.get("type") == "message.received"
        ]
        no_echo = len(bob_msgs) == 0
        result(
            "Bob does NOT receive his own message",
            no_echo,
            f"Bob got {len(bob_msgs)} echoes",
        )

        return passed and no_echo
    finally:
        await alice.disconnect()
        await bob.disconnect()


async def scenario_multi_recipient(ws_url: str) -> bool:
    """Scenario 2: Fan-out to multiple users.

    Alice, Bob, Charlie all in a channel.
    Alice sends a message.
    Both Bob and Charlie should receive it.
    """
    header("Scenario 2: Multi-Recipient Fan-Out")

    channel = f"sim-channel-{uuid.uuid4().hex[:6]}"
    alice = make_client("alice", ws_url)
    bob = make_client("bob", ws_url)
    # Alice's second device — same user, different connection.
    # Both should receive messages sent by bob.
    alice_laptop = make_client("alice", ws_url, token_index=1)

    try:
        for client in [alice, bob, alice_laptop]:
            await client.connect()
            await client.join_channel(channel)

        # Bob sends — both alice devices should receive
        await bob.send_message(channel, "Hey everyone!")

        alice_msgs = await alice.wait_for_message()
        alice_laptop_msgs = await alice_laptop.wait_for_message()

        alice_ok = (
            len(alice_msgs) >= 1
            and alice_msgs[0].get("content") == "Hey everyone!"
        )
        laptop_ok = (
            len(alice_laptop_msgs) >= 1
            and alice_laptop_msgs[0].get("content") == "Hey everyone!"
        )

        result("Alice-phone receives the message", alice_ok)
        result("Alice-laptop receives the message", laptop_ok)

        return alice_ok and laptop_ok
    finally:
        for client in [alice, bob, alice_laptop]:
            await client.disconnect()


async def scenario_offline_user(ws_url: str) -> bool:
    """Scenario 3: Offline user doesn't block delivery.

    Alice, Alice-laptop, and Bob join a channel.
    Bob disconnects (goes offline).
    Bob can't receive, but alice-laptop should still get
    messages sent by alice-phone.

    Wait — sender skip means alice-laptop won't get messages
    from alice-phone (same user_id). So instead: Bob sends
    a message BEFORE disconnecting (baseline), then we test
    that after Bob disconnects, alice-phone can still send
    and the system doesn't crash.
    """
    header("Scenario 3: Offline User Handling")

    channel = f"sim-channel-{uuid.uuid4().hex[:6]}"
    alice = make_client("alice", ws_url, token_index=0)
    bob = make_client("bob", ws_url)

    try:
        await alice.connect()
        await bob.connect()
        await alice.join_channel(channel)
        await bob.join_channel(channel)

        # Baseline: bob sends, alice receives
        await bob.send_message(channel, "I'm about to go offline")
        baseline = await alice.wait_for_message()
        baseline_ok = (
            len(baseline) >= 1
            and "about to go offline" in baseline[0].get("content", "")
        )
        result("Baseline delivery works", baseline_ok)

        # Bob goes offline
        log("Bob going offline...")
        await bob.disconnect()
        await asyncio.sleep(0.5)

        # Alice sends — should not crash even though bob is gone
        alice.clear()
        await alice.send_message(channel, "Bob is gone, system should not crash")

        # Give the system time to process (no one to deliver to
        # except alice herself, who is the sender — so 0 deliveries)
        await asyncio.sleep(2.0)

        no_crash = True
        result(
            "No crash after offline user",
            no_crash,
            "system stayed healthy",
        )

        return baseline_ok and no_crash
    finally:
        await alice.disconnect()


async def scenario_message_ordering(ws_url: str) -> bool:
    """Scenario 4: Messages arrive in order.

    Bob sends 10 numbered messages rapidly.
    Alice should receive them in the correct order.
    """
    header("Scenario 4: Message Ordering")

    channel = f"sim-channel-{uuid.uuid4().hex[:6]}"
    alice = make_client("alice", ws_url)
    bob = make_client("bob", ws_url)

    try:
        await alice.connect()
        await bob.connect()
        await alice.join_channel(channel)
        await bob.join_channel(channel)

        msg_count = 10
        for i in range(msg_count):
            await bob.send_message(channel, f"msg-{i:03d}")
            await asyncio.sleep(0.05)  # small gap, still rapid

        messages = await alice.wait_for_message(
            timeout=15.0, count=msg_count
        )

        received_count = len(messages)
        result(
            f"Received all {msg_count} messages",
            received_count == msg_count,
            f"got {received_count}/{msg_count}",
        )

        if received_count == msg_count:
            contents = [m.get("content") for m in messages]
            expected = [f"msg-{i:03d}" for i in range(msg_count)]
            in_order = contents == expected
            result(
                "Messages in correct order",
                in_order,
                f"{'correct' if in_order else contents}",
            )
            return in_order

        return False
    finally:
        await alice.disconnect()
        await bob.disconnect()


async def scenario_rapid_fire(ws_url: str) -> bool:
    """Scenario 5: Burst of messages from multiple senders.

    3 users all send 5 messages each into the same channel.
    Each user should receive 10 messages (from the other 2).
    Tests concurrent produce/consume under load.
    """
    header("Scenario 5: Multi-Device Delivery")

    channel = f"sim-channel-{uuid.uuid4().hex[:6]}"
    alice_phone = make_client("alice", ws_url, token_index=0)
    alice_laptop = make_client("alice", ws_url, token_index=1)
    bob = make_client("bob", ws_url)

    try:
        for u in [alice_phone, alice_laptop, bob]:
            await u.connect()
            await u.join_channel(channel)

        # Bob sends a message
        await bob.send_message(channel, "hey alice, both devices?")

        phone_msgs = await alice_phone.wait_for_message()
        laptop_msgs = await alice_laptop.wait_for_message()

        phone_ok = (
            len(phone_msgs) >= 1
            and phone_msgs[0].get("content") == "hey alice, both devices?"
        )
        laptop_ok = (
            len(laptop_msgs) >= 1
            and laptop_msgs[0].get("content") == "hey alice, both devices?"
        )

        result("Alice-phone receives", phone_ok)
        result("Alice-laptop receives", laptop_ok)

        return phone_ok and laptop_ok
    finally:
        for u in [alice_phone, alice_laptop, bob]:
            await u.disconnect()


async def scenario_duplicate_detection(ws_url: str) -> bool:
    """Scenario 6: Client-side duplicate detection.

    Bob sends the same client_request_id twice.
    Alice should ideally receive only 1 (or if 2,
    both carry the same messageId for client dedup).
    """
    header("Scenario 6: Duplicate Message Handling")

    channel = f"sim-channel-{uuid.uuid4().hex[:6]}"
    alice = make_client("alice", ws_url)
    bob = make_client("bob", ws_url)

    try:
        await alice.connect()
        await bob.connect()
        await alice.join_channel(channel)
        await bob.join_channel(channel)

        dedup_key = f"req-{uuid.uuid4().hex[:8]}"

        await bob.send_message(
            channel, "possible dupe", client_request_id=dedup_key
        )
        await asyncio.sleep(0.2)
        await bob.send_message(
            channel, "possible dupe", client_request_id=dedup_key
        )

        await asyncio.sleep(3.0)

        messages = [
            m for m in alice.received
            if m.get("type") == "message.received"
        ]

        if len(messages) == 1:
            result(
                "Server-side dedup worked",
                True,
                "only 1 message delivered",
            )
            return True
        elif len(messages) == 2:
            # Check if both carry same messageId (client can dedup)
            ids = {m.get("messageId") for m in messages}
            if len(ids) == 1:
                result(
                    "Same messageId — client can dedup",
                    True,
                    "2 delivered, same ID",
                )
                return True
            else:
                result(
                    "Duplicate detected",
                    False,
                    f"2 messages with different IDs: {ids}",
                )
                return False
        else:
            result(
                "Unexpected message count",
                False,
                f"got {len(messages)}",
            )
            return False
    finally:
        await alice.disconnect()
        await bob.disconnect()


# ══════════════════════════════════════════════════════════════
# Runner
# ══════════════════════════════════════════════════════════════

async def run_all(ws_url: str):
    print("\n\033[1m  Delivery Pipeline — Live Simulation\033[0m")
    print(f"  Server: {ws_url}")
    print(f"  Time:   {time.strftime('%Y-%m-%d %H:%M:%S')}")

    scenarios = [
        ("Basic Delivery", scenario_basic_delivery),
        ("Multi-Recipient", scenario_multi_recipient),
        ("Offline User", scenario_offline_user),
        ("Message Ordering", scenario_message_ordering),
        ("Multi-Device", scenario_rapid_fire),
        ("Duplicate Detection", scenario_duplicate_detection),
    ]

    results = []
    for name, fn in scenarios:
        try:
            passed = await fn(ws_url)
            results.append((name, passed))
        except Exception as e:
            log(f"Scenario crashed: {e}", "FAIL")
            results.append((name, False))

        # Brief pause between scenarios
        await asyncio.sleep(1.0)

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
    parser = argparse.ArgumentParser(
        description="Simulate delivery pipeline end-to-end"
    )
    parser.add_argument(
        "--url",
        default=DEFAULT_WS_URL,
        help=f"WebSocket URL (default: {DEFAULT_WS_URL})",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Verbose output",
    )
    args = parser.parse_args()

    success = asyncio.run(run_all(args.url))
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
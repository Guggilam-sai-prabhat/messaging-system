"""
WebSocket History Events — Live Simulation

Tests message history loading through the WebSocket
connection (no REST API needed).

Client events:
  messages.history → load channel messages (with pagination)
  messages.get     → fetch single message by ID
  channel.stats    → message count + time range

Scenarios:
  1. Load channel history via WebSocket
  2. Cursor pagination (scroll up)
  3. Reconnect catch-up (load missed messages)
  4. Single message lookup
  5. Channel stats
  6. Empty channel
  7. Full reconnect flow (live + history merge)

Prerequisites:
  - Server running: uvicorn app.main:app --port 8000
  - Redis, Kafka, PostgreSQL running
  - Migrations applied

Usage:
  python scripts/simulate_ws_history.py
"""

import json
import time
import asyncio
import uuid
import sys
from dataclasses import dataclass, field

import websockets
import asyncpg

# ── Config ────────────────────────────────────────────────────

WS_URL = "ws://localhost:8000/ws"
DATABASE_URL = "postgresql://postgres:new_password@localhost:5432/messaging"
PERSISTENCE_DELAY = 3.0

MOCK_USERS = {
    "alice": {"token": "token-alice-1", "device_id": "alice-phone"},
    "bob": {"token": "token-bob-1", "device_id": "bob-phone"},
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


# ── WebSocket Client ─────────────────────────────────────────

@dataclass
class SimulatedClient:
    user_id: str
    token: str
    device_id: str
    ws: object = None
    received: list = field(default_factory=list)
    _listener_task: object = None

    async def connect(self):
        url = f"{WS_URL}?token={self.token}"
        self.ws = await websockets.connect(url)
        self._listener_task = asyncio.create_task(self._listen())
        log(f"{self.user_id} connected")
        await asyncio.sleep(0.3)

    async def _listen(self):
        try:
            async for raw in self.ws:
                msg = json.loads(raw)
                self.received.append(msg)
        except (websockets.ConnectionClosed, Exception):
            pass

    async def join_channel(self, channel_id: str):
        await self.ws.send(json.dumps({
            "type": "channel.join",
            "channel_id": channel_id,
        }))
        await asyncio.sleep(0.3)

    async def send_message(self, channel_id: str, content: str) -> str | None:
        await self.ws.send(json.dumps({
            "type": "message.send",
            "channel_id": channel_id,
            "content": content,
        }))
        return await self._wait_for("message.ack")

    async def request_history(
        self, channel_id: str,
        limit: int = 50,
        before: float = None,
        after: float = None,
    ) -> dict | None:
        """Send messages.history event, wait for response."""
        payload = {
            "type": "messages.history",
            "channel_id": channel_id,
            "limit": limit,
        }
        if before is not None:
            payload["before"] = before
        if after is not None:
            payload["after"] = after

        await self.ws.send(json.dumps(payload))
        log(
            f"{self.user_id} requested history: "
            f"channel={channel_id} limit={limit}"
            + (f" before={before}" if before else "")
            + (f" after={after}" if after else ""),
            "SEND",
        )
        return await self._wait_for_response("messages.history")

    async def request_message(
        self, channel_id: str, message_id: str
    ) -> dict | None:
        """Send messages.get event, wait for response."""
        await self.ws.send(json.dumps({
            "type": "messages.get",
            "channel_id": channel_id,
            "message_id": message_id,
        }))
        log(f"{self.user_id} requested message: {message_id[:12]}...", "SEND")
        return await self._wait_for_response("messages.get")

    async def request_stats(self, channel_id: str) -> dict | None:
        """Send channel.stats event, wait for response."""
        await self.ws.send(json.dumps({
            "type": "channel.stats",
            "channel_id": channel_id,
        }))
        log(f"{self.user_id} requested stats: {channel_id}", "SEND")
        return await self._wait_for_response("channel.stats")

    async def _wait_for(self, msg_type: str, timeout: float = 5.0) -> str | None:
        """Wait for a specific message type, return message_id."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            for msg in self.received:
                if msg.get("type") == msg_type:
                    self.received.remove(msg)
                    return msg.get("message_id")
            await asyncio.sleep(0.1)
        return None

    async def _wait_for_response(
        self, msg_type: str, timeout: float = 5.0
    ) -> dict | None:
        """Wait for a specific response type, return full dict."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            for msg in self.received:
                if msg.get("type") == msg_type:
                    self.received.remove(msg)
                    log(
                        f"{self.user_id} got {msg_type} response",
                        "RECV",
                    )
                    return msg
            await asyncio.sleep(0.1)
        log(f"{self.user_id} timeout waiting for {msg_type}", "FAIL")
        return None

    def get_live_messages(self) -> list[dict]:
        """Get message.received events (live delivery)."""
        msgs = [
            m for m in self.received
            if m.get("type") == "message.received"
        ]
        return msgs

    async def disconnect(self):
        if self._listener_task:
            self._listener_task.cancel()
            try:
                await self._listener_task
            except asyncio.CancelledError:
                pass
        if self.ws:
            await self.ws.close()
        log(f"{self.user_id} disconnected")

    def clear(self):
        self.received.clear()


def make_client(user_id: str) -> SimulatedClient:
    info = MOCK_USERS[user_id]
    return SimulatedClient(
        user_id=user_id,
        token=info["token"],
        device_id=info["device_id"],
    )


# ── Seed + Cleanup ────────────────────────────────────────────

async def seed_messages(
    channel: str, messages: list[tuple[str, str]]
) -> list[str]:
    clients = {}
    message_ids = []
    try:
        for user_id, content in messages:
            if user_id not in clients:
                c = make_client(user_id)
                await c.connect()
                await c.join_channel(channel)
                clients[user_id] = c
            mid = await clients[user_id].send_message(channel, content)
            if mid:
                message_ids.append(mid)
            await asyncio.sleep(0.05)
        return message_ids
    finally:
        for c in clients.values():
            await c.disconnect()


async def cleanup(channel: str):
    conn = await asyncpg.connect(dsn=DATABASE_URL)
    await conn.execute(
        "DELETE FROM messages WHERE channel_id = $1", channel
    )
    await conn.close()


# ══════════════════════════════════════════════════════════════
# Scenarios
# ══════════════════════════════════════════════════════════════

async def scenario_load_history() -> bool:
    """Scenario 1: Load channel history via WebSocket.

    Client sends:
      {"type": "messages.history", "channel_id": "...", "limit": 50}

    Server responds with messages in chronological order.
    """
    header("Scenario 1: Load History via WebSocket")
    channel = f"ws-hist-{uuid.uuid4().hex[:6]}"

    try:
        ids = await seed_messages(channel, [
            ("alice", "First message"),
            ("bob", "Second message"),
            ("alice", "Third message"),
            ("bob", "Fourth message"),
            ("alice", "Fifth message"),
        ])
        log(f"Seeded {len(ids)} messages", "INFO")
        await asyncio.sleep(PERSISTENCE_DELAY)

        # Connect and request history
        alice = make_client("alice")
        await alice.connect()

        data = await alice.request_history(channel, limit=10)
        await alice.disconnect()

        if not data:
            result("Got history response", False, "timeout")
            return False

        result("Got history response", True)

        count_ok = data["count"] == 5
        result("All 5 messages", count_ok, f"got {data['count']}")

        contents = [m["content"] for m in data["messages"]]
        order_ok = contents == [
            "First message", "Second message", "Third message",
            "Fourth message", "Fifth message",
        ]
        result("Chronological order", order_ok, f"{contents}")

        senders = [m["senderId"] for m in data["messages"]]
        sender_ok = senders == ["alice", "bob", "alice", "bob", "alice"]
        result("Correct senders", sender_ok)

        no_more = data["hasMore"] is False
        result("hasMore is False", no_more)

        return count_ok and order_ok and sender_ok and no_more
    finally:
        await cleanup(channel)


async def scenario_pagination() -> bool:
    """Scenario 2: Scroll up with cursor pagination."""
    header("Scenario 2: Cursor Pagination via WebSocket")
    channel = f"ws-hist-{uuid.uuid4().hex[:6]}"

    try:
        messages = [
            ("bob" if i % 2 else "alice", f"msg-{i:02d}")
            for i in range(15)
        ]
        ids = await seed_messages(channel, messages)
        log(f"Seeded {len(ids)} messages", "INFO")
        await asyncio.sleep(PERSISTENCE_DELAY)

        alice = make_client("alice")
        await alice.connect()

        # Page 1
        page1 = await alice.request_history(channel, limit=10)
        if not page1:
            result("Page 1", False, "timeout")
            await alice.disconnect()
            return False

        p1_ok = page1["count"] == 10
        result("Page 1: 10 messages", p1_ok, f"got {page1['count']}")

        has_more = page1["hasMore"] is True
        result("Page 1: hasMore=True", has_more)

        cursor = page1.get("nextCursor")
        result("Page 1: has cursor", cursor is not None)

        # Page 2 using cursor
        page2 = await alice.request_history(
            channel, limit=10, before=cursor
        )
        await alice.disconnect()

        if not page2:
            result("Page 2", False, "timeout")
            return False

        p2_ok = page2["count"] == 5
        result("Page 2: 5 messages", p2_ok, f"got {page2['count']}")

        no_more = page2["hasMore"] is False
        result("Page 2: hasMore=False", no_more)

        # No overlap
        p1_ids = {m["messageId"] for m in page1["messages"]}
        p2_ids = {m["messageId"] for m in page2["messages"]}
        no_overlap = len(p1_ids & p2_ids) == 0
        result("No overlap", no_overlap)

        total = page1["count"] + page2["count"]
        result("Combined = 15", total == 15, f"got {total}")

        return all([p1_ok, has_more, p2_ok, no_more, no_overlap])
    finally:
        await cleanup(channel)


async def scenario_catchup() -> bool:
    """Scenario 3: Catch up on missed messages after reconnect."""
    header("Scenario 3: Reconnect Catch-Up")
    channel = f"ws-hist-{uuid.uuid4().hex[:6]}"

    try:
        # Send initial messages
        ids = await seed_messages(channel, [
            ("alice", "before-1"),
            ("bob", "before-2"),
        ])
        await asyncio.sleep(PERSISTENCE_DELAY)

        # Get last seen timestamp
        alice = make_client("alice")
        await alice.connect()
        data = await alice.request_history(channel, limit=10)
        last_seen = data["messages"][-1]["timestamp"]
        await alice.disconnect()

        log(f"Last seen: {last_seen}", "INFO")

        # More messages arrive while offline
        await asyncio.sleep(0.5)
        await seed_messages(channel, [
            ("bob", "missed-1"),
            ("alice", "missed-2"),
            ("bob", "missed-3"),
        ])
        await asyncio.sleep(PERSISTENCE_DELAY)

        # Reconnect and catch up
        alice = make_client("alice")
        await alice.connect()
        catchup = await alice.request_history(
            channel, after=last_seen, limit=50
        )
        await alice.disconnect()

        if not catchup:
            result("Got catch-up response", False, "timeout")
            return False

        count_ok = catchup["count"] == 3
        result("3 missed messages", count_ok, f"got {catchup['count']}")

        contents = [m["content"] for m in catchup["messages"]]
        content_ok = contents == ["missed-1", "missed-2", "missed-3"]
        result("Correct content", content_ok, f"{contents}")

        no_old = all("before" not in m["content"] for m in catchup["messages"])
        result("No old messages", no_old)

        return count_ok and content_ok and no_old
    finally:
        await cleanup(channel)


async def scenario_single_message() -> bool:
    """Scenario 4: Fetch single message by ID."""
    header("Scenario 4: Single Message Lookup")
    channel = f"ws-hist-{uuid.uuid4().hex[:6]}"

    try:
        ids = await seed_messages(channel, [
            ("bob", "Target message"),
        ])
        await asyncio.sleep(PERSISTENCE_DELAY)

        alice = make_client("alice")
        await alice.connect()

        # Fetch existing
        data = await alice.request_message(channel, ids[0])
        found = data is not None and data.get("message") is not None
        result("Message found", found)

        if found:
            msg = data["message"]
            result("Content matches", msg["content"] == "Target message")
            result("Sender matches", msg["senderId"] == "bob")

        # Fetch non-existent
        data2 = await alice.request_message(channel, "non-existent")
        not_found = data2 is not None and data2.get("message") is None
        result("Non-existent returns null", not_found)

        await alice.disconnect()
        return found and not_found
    finally:
        await cleanup(channel)


async def scenario_channel_stats() -> bool:
    """Scenario 5: Get channel stats."""
    header("Scenario 5: Channel Stats")
    channel = f"ws-hist-{uuid.uuid4().hex[:6]}"

    try:
        await seed_messages(channel, [
            ("alice", "stat-1"),
            ("bob", "stat-2"),
            ("alice", "stat-3"),
        ])
        await asyncio.sleep(PERSISTENCE_DELAY)

        alice = make_client("alice")
        await alice.connect()

        data = await alice.request_stats(channel)
        await alice.disconnect()

        if not data:
            result("Got stats", False, "timeout")
            return False

        count_ok = data["totalMessages"] == 3
        result("Total = 3", count_ok, f"got {data['totalMessages']}")

        has_times = (
            data.get("firstMessageAt") is not None
            and data.get("lastMessageAt") is not None
        )
        result("Has time range", has_times)

        if has_times:
            order_ok = data["firstMessageAt"] <= data["lastMessageAt"]
            result("first <= last", order_ok)
            return count_ok and order_ok

        return count_ok
    finally:
        await cleanup(channel)


async def scenario_empty_channel() -> bool:
    """Scenario 6: Empty channel returns zero messages."""
    header("Scenario 6: Empty Channel")
    channel = f"ws-empty-{uuid.uuid4().hex[:6]}"

    alice = make_client("alice")
    await alice.connect()

    data = await alice.request_history(channel, limit=50)
    await alice.disconnect()

    if not data:
        result("Got response", False, "timeout")
        return False

    empty = data["count"] == 0
    result("Zero messages", empty, f"got {data['count']}")

    no_more = data["hasMore"] is False
    result("hasMore is False", no_more)

    no_cursor = data.get("nextCursor") is None
    result("No cursor", no_cursor)

    return empty and no_more and no_cursor


async def scenario_full_reconnect() -> bool:
    """Scenario 7: Full reconnect — live messages + history catch-up.

    Phase 1: Alice online, gets live messages via WebSocket
    Phase 2: Alice offline, misses messages
    Phase 3: Alice reconnects, catches up via messages.history
    """
    header("Scenario 7: Full Reconnect Flow")
    channel = f"ws-hist-{uuid.uuid4().hex[:6]}"

    try:
        # Phase 1: Both online
        alice = make_client("alice")
        bob = make_client("bob")
        await alice.connect()
        await bob.connect()
        await alice.join_channel(channel)
        await bob.join_channel(channel)

        await bob.send_message(channel, "live-1")
        await bob.send_message(channel, "live-2")
        await asyncio.sleep(1.0)

        live_msgs = alice.get_live_messages()
        result(
            "Phase 1: Got 2 live messages",
            len(live_msgs) == 2,
            f"got {len(live_msgs)}",
        )

        last_seen = max(
            m.get("timestamp", 0) for m in live_msgs
        ) if live_msgs else time.time()

        # Phase 2: Alice offline
        await alice.disconnect()
        log("Alice went offline", "WARN")
        await asyncio.sleep(0.5)

        await bob.send_message(channel, "missed-3")
        await bob.send_message(channel, "missed-4")
        await bob.send_message(channel, "missed-5")
        await bob.disconnect()

        await asyncio.sleep(PERSISTENCE_DELAY)

        # Phase 3: Alice reconnects and catches up
        log("Alice reconnecting...", "INFO")
        alice = make_client("alice")
        await alice.connect()

        catchup = await alice.request_history(
            channel, after=last_seen, limit=50
        )

        missed_ok = catchup is not None and catchup["count"] == 3
        result(
            "Phase 3: Got 3 missed messages",
            missed_ok,
            f"got {catchup['count'] if catchup else 0}",
        )

        if catchup and catchup["messages"]:
            contents = [m["content"] for m in catchup["messages"]]
            content_ok = contents == ["missed-3", "missed-4", "missed-5"]
            result("Missed content correct", content_ok, f"{contents}")
        else:
            content_ok = False

        # Full history
        full = await alice.request_history(channel, limit=50)
        total_ok = full is not None and full["count"] == 5
        result(
            "Full history = 5 messages",
            total_ok,
            f"got {full['count'] if full else 0}",
        )

        await alice.disconnect()
        return missed_ok and content_ok and total_ok
    finally:
        await cleanup(channel)


# ══════════════════════════════════════════════════════════════
# Runner
# ══════════════════════════════════════════════════════════════

async def run_all():
    print("\n\033[1m  WebSocket History Events — Live Simulation\033[0m")
    print(f"  Server: {WS_URL}")
    print(f"  Time:   {time.strftime('%Y-%m-%d %H:%M:%S')}")

    scenarios = [
        ("Load History", scenario_load_history),
        ("Cursor Pagination", scenario_pagination),
        ("Reconnect Catch-Up", scenario_catchup),
        ("Single Message Lookup", scenario_single_message),
        ("Channel Stats", scenario_channel_stats),
        ("Empty Channel", scenario_empty_channel),
        ("Full Reconnect Flow", scenario_full_reconnect),
    ]

    results_list = []
    for name, fn in scenarios:
        try:
            passed = await fn()
            results_list.append((name, passed))
        except Exception as e:
            log(f"Scenario crashed: {e}", "FAIL")
            import traceback
            traceback.print_exc()
            results_list.append((name, False))
        await asyncio.sleep(1.0)

    header("Summary")
    passed = sum(1 for _, p in results_list if p)
    total = len(results_list)

    for name, p in results_list:
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
"""
Message History API — Live Simulation

Tests the REST API that clients use to load message history.
Sends messages via WebSocket (so they go through the full
Kafka → persistence → PostgreSQL pipeline), then fetches
them back via the HTTP API.

Scenarios:
  1. Load newest messages (open a channel)
  2. Cursor pagination (scroll up to load older messages)
  3. Catch-up after reconnect (load messages since timestamp)
  4. Single message lookup (deep link to a message)
  5. Channel stats
  6. Empty channel (no messages)
  7. Full reconnect flow (WebSocket + REST together)

Prerequisites:
  - Server running: uvicorn app.main:app --port 8000
  - Redis, Kafka, PostgreSQL running
  - Migrations applied: alembic upgrade head

Usage:
  python scripts/simulate_history_api.py
"""

import json
import time
import asyncio
import uuid
import sys
from dataclasses import dataclass, field

import websockets
import httpx

# ── Config ────────────────────────────────────────────────────

WS_URL = "ws://localhost:8000/ws"
API_URL = "http://localhost:8000"
DATABASE_URL = "postgresql://postgres:new_password@localhost:5432/messaging"

MOCK_USERS = {
    "alice": {"token": "token-alice-1", "device_id": "alice-phone"},
    "bob": {"token": "token-bob-1", "device_id": "bob-phone"},
}

# Time to wait for persistence batch to flush
PERSISTENCE_DELAY = 3.0


# ── Helpers ───────────────────────────────────────────────────

def log(msg: str, level: str = "INFO"):
    colors = {
        "INFO": "\033[36m",
        "OK": "\033[32m",
        "FAIL": "\033[31m",
        "WARN": "\033[33m",
        "SEND": "\033[35m",
        "RECV": "\033[34m",
        "HTTP": "\033[33m",
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


# ── WebSocket Client (simplified for seeding messages) ────────

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
        log(f"{self.user_id} connected via WebSocket")

    async def _listen(self):
        try:
            async for raw in self.ws:
                self.received.append(json.loads(raw))
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
        # Wait for ack
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline:
            for msg in self.received:
                if msg.get("type") == "message.ack":
                    self.received.remove(msg)
                    return msg.get("message_id")
            await asyncio.sleep(0.1)
        return None

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


def make_client(user_id: str) -> SimulatedClient:
    info = MOCK_USERS[user_id]
    return SimulatedClient(
        user_id=user_id,
        token=info["token"],
        device_id=info["device_id"],
    )


# ── Seed messages into a channel ──────────────────────────────

async def seed_messages(
    channel: str, messages: list[tuple[str, str]]
) -> list[str]:
    """Send messages via WebSocket, return message IDs.

    messages: list of (user_id, content) tuples
    """
    clients = {}
    message_ids = []

    try:
        for user_id, content in messages:
            if user_id not in clients:
                client = make_client(user_id)
                await client.connect()
                await client.join_channel(channel)
                clients[user_id] = client

            mid = await clients[user_id].send_message(channel, content)
            if mid:
                message_ids.append(mid)
            await asyncio.sleep(0.05)

        return message_ids
    finally:
        for client in clients.values():
            await client.disconnect()


# ── API Client ────────────────────────────────────────────────

async def api_get(
    client: httpx.AsyncClient, path: str, params: dict = None
) -> dict:
    """Make a GET request and return JSON."""
    resp = await client.get(f"{API_URL}{path}", params=params)
    log(
        f"GET {path} "
        f"{'?' + '&'.join(f'{k}={v}' for k, v in (params or {}).items()) if params else ''} "
        f"→ {resp.status_code}",
        "HTTP",
    )
    if resp.status_code != 200:
        log(f"  Response: {resp.text[:200]}", "FAIL")
    return resp.status_code, resp.json()


# ── Cleanup ───────────────────────────────────────────────────

async def cleanup_channel(channel: str):
    """Delete test messages via direct DB connection."""
    import asyncpg
    conn = await asyncpg.connect(dsn=DATABASE_URL)
    await conn.execute(
        "DELETE FROM messages WHERE channel_id = $1", channel
    )
    await conn.close()


# ══════════════════════════════════════════════════════════════
# Scenarios
# ══════════════════════════════════════════════════════════════

async def scenario_load_newest(http: httpx.AsyncClient) -> bool:
    """Scenario 1: Open a channel — load newest messages.

    This is what happens when a user clicks on #general.
    The client calls GET /channels/general/messages?limit=10
    and gets the most recent messages.
    """
    header("Scenario 1: Load Newest Messages")

    channel = f"hist-test-{uuid.uuid4().hex[:6]}"

    try:
        # Seed 5 messages
        ids = await seed_messages(channel, [
            ("alice", "First message"),
            ("bob", "Second message"),
            ("alice", "Third message"),
            ("bob", "Fourth message"),
            ("alice", "Fifth message"),
        ])
        log(f"Seeded {len(ids)} messages, waiting for persistence...", "INFO")
        await asyncio.sleep(PERSISTENCE_DELAY)

        # Fetch via API
        status, data = await api_get(
            http, f"/channels/{channel}/messages", {"limit": 10}
        )

        status_ok = status == 200
        result("API returns 200", status_ok)

        count_ok = data["count"] == 5
        result(
            "All 5 messages returned",
            count_ok,
            f"got {data['count']}",
        )

        if data["messages"]:
            # Should be chronological (oldest first in array)
            contents = [m["content"] for m in data["messages"]]
            order_ok = contents == [
                "First message", "Second message", "Third message",
                "Fourth message", "Fifth message",
            ]
            result("Chronological order", order_ok, f"{contents}")

            # Verify sender attribution
            senders = [m["senderId"] for m in data["messages"]]
            sender_ok = senders == ["alice", "bob", "alice", "bob", "alice"]
            result("Correct senders", sender_ok, f"{senders}")

            no_more = data["hasMore"] is False
            result("hasMore is False", no_more)

            return status_ok and count_ok and order_ok and sender_ok and no_more

        return False
    finally:
        await cleanup_channel(channel)


async def scenario_pagination(http: httpx.AsyncClient) -> bool:
    """Scenario 2: Cursor-based pagination — scroll up.

    Seed 15 messages. Fetch first page (10). Use nextCursor
    to fetch second page (5). Verify no overlap, no gaps.
    """
    header("Scenario 2: Cursor Pagination (Scroll Up)")

    channel = f"hist-test-{uuid.uuid4().hex[:6]}"

    try:
        messages = [
            ("bob" if i % 2 else "alice", f"msg-{i:02d}")
            for i in range(15)
        ]
        ids = await seed_messages(channel, messages)
        log(f"Seeded {len(ids)} messages", "INFO")
        await asyncio.sleep(PERSISTENCE_DELAY)

        # Page 1: newest 10
        status1, page1 = await api_get(
            http, f"/channels/{channel}/messages", {"limit": 10}
        )

        p1_count = page1["count"] == 10
        result("Page 1: 10 messages", p1_count, f"got {page1['count']}")

        p1_has_more = page1["hasMore"] is True
        result("Page 1: hasMore=True", p1_has_more)

        cursor = page1.get("nextCursor")
        has_cursor = cursor is not None
        result("Page 1: has nextCursor", has_cursor, f"cursor={cursor}")

        if not has_cursor:
            return False

        # Page 2: older messages using cursor
        status2, page2 = await api_get(
            http,
            f"/channels/{channel}/messages",
            {"limit": 10, "before": cursor},
        )

        p2_count = page2["count"] == 5
        result("Page 2: 5 messages", p2_count, f"got {page2['count']}")

        p2_no_more = page2["hasMore"] is False
        result("Page 2: hasMore=False", p2_no_more)

        # Verify no overlap between pages
        p1_ids = {m["messageId"] for m in page1["messages"]}
        p2_ids = {m["messageId"] for m in page2["messages"]}
        no_overlap = len(p1_ids & p2_ids) == 0
        result("No overlap between pages", no_overlap)

        # Verify combined = all 15 messages
        all_contents = (
            [m["content"] for m in page2["messages"]]
            + [m["content"] for m in page1["messages"]]
        )
        complete = len(all_contents) == 15
        result(
            "Combined = all 15 messages",
            complete,
            f"got {len(all_contents)}",
        )

        return all([p1_count, p1_has_more, p2_count, p2_no_more, no_overlap, complete])
    finally:
        await cleanup_channel(channel)


async def scenario_catchup(http: httpx.AsyncClient) -> bool:
    """Scenario 3: Reconnect catch-up — load missed messages.

    Seed 5 messages. Record the timestamp after message 2.
    Then use "after" to fetch only messages 3, 4, 5.
    This simulates: user was online, got messages 1-2,
    disconnected, reconnects, asks "what did I miss?"
    """
    header("Scenario 3: Reconnect Catch-Up (after cursor)")

    channel = f"hist-test-{uuid.uuid4().hex[:6]}"

    try:
        # Send first 2 messages
        ids_before = await seed_messages(channel, [
            ("alice", "before-disconnect-1"),
            ("bob", "before-disconnect-2"),
        ])
        await asyncio.sleep(PERSISTENCE_DELAY)

        # Record "last seen" timestamp
        _, data = await api_get(
            http, f"/channels/{channel}/messages", {"limit": 10}
        )
        last_seen = data["messages"][-1]["timestamp"]
        log(f"Last seen timestamp: {last_seen}", "INFO")

        # Send 3 more messages (user was offline for these)
        await asyncio.sleep(0.5)
        ids_after = await seed_messages(channel, [
            ("alice", "missed-1"),
            ("bob", "missed-2"),
            ("alice", "missed-3"),
        ])
        await asyncio.sleep(PERSISTENCE_DELAY)

        # Catch-up: "what happened after my last seen?"
        status, catchup = await api_get(
            http,
            f"/channels/{channel}/messages",
            {"after": last_seen, "limit": 50},
        )

        count_ok = catchup["count"] == 3
        result(
            "Got exactly 3 missed messages",
            count_ok,
            f"got {catchup['count']}",
        )

        if catchup["messages"]:
            contents = [m["content"] for m in catchup["messages"]]
            content_ok = contents == ["missed-1", "missed-2", "missed-3"]
            result("Correct missed messages", content_ok, f"{contents}")

            # Should NOT include the "before" messages
            no_old = all(
                "before" not in m["content"]
                for m in catchup["messages"]
            )
            result("No old messages included", no_old)

            return count_ok and content_ok and no_old

        return False
    finally:
        await cleanup_channel(channel)


async def scenario_single_message(http: httpx.AsyncClient) -> bool:
    """Scenario 4: Fetch a single message by ID.

    Deep-link use case — someone shares a link to a
    specific message.
    """
    header("Scenario 4: Single Message Lookup")

    channel = f"hist-test-{uuid.uuid4().hex[:6]}"

    try:
        ids = await seed_messages(channel, [
            ("bob", "This is the target message"),
        ])
        await asyncio.sleep(PERSISTENCE_DELAY)

        message_id = ids[0]

        # Fetch by ID
        status, data = await api_get(
            http,
            f"/channels/{channel}/messages/{message_id}",
        )

        found = status == 200
        result("Message found (200)", found)

        if found:
            content_ok = data["content"] == "This is the target message"
            result("Content matches", content_ok)

            sender_ok = data["senderId"] == "bob"
            result("Sender matches", sender_ok)

            # Try a non-existent message
            status_404, _ = await api_get(
                http,
                f"/channels/{channel}/messages/non-existent-id",
            )
            not_found = status_404 == 404
            result("Non-existent returns 404", not_found)

            return content_ok and sender_ok and not_found

        return False
    finally:
        await cleanup_channel(channel)


async def scenario_channel_stats(http: httpx.AsyncClient) -> bool:
    """Scenario 5: Channel stats endpoint."""
    header("Scenario 5: Channel Stats")

    channel = f"hist-test-{uuid.uuid4().hex[:6]}"

    try:
        ids = await seed_messages(channel, [
            ("alice", "stat-msg-1"),
            ("bob", "stat-msg-2"),
            ("alice", "stat-msg-3"),
        ])
        await asyncio.sleep(PERSISTENCE_DELAY)

        status, data = await api_get(
            http, f"/channels/{channel}/stats"
        )

        status_ok = status == 200
        result("Stats endpoint returns 200", status_ok)

        count_ok = data["totalMessages"] == 3
        result("Total count correct", count_ok, f"got {data['totalMessages']}")

        has_first = data["firstMessageAt"] is not None
        has_last = data["lastMessageAt"] is not None
        result("Has firstMessageAt", has_first)
        result("Has lastMessageAt", has_last)

        if has_first and has_last:
            order_ok = data["firstMessageAt"] <= data["lastMessageAt"]
            result("first <= last", order_ok)
            return status_ok and count_ok and order_ok

        return status_ok and count_ok
    finally:
        await cleanup_channel(channel)


async def scenario_empty_channel(http: httpx.AsyncClient) -> bool:
    """Scenario 6: Empty channel — no messages yet."""
    header("Scenario 6: Empty Channel")

    channel = f"hist-empty-{uuid.uuid4().hex[:6]}"

    status, data = await api_get(
        http, f"/channels/{channel}/messages", {"limit": 50}
    )

    status_ok = status == 200
    result("Returns 200 (not 404)", status_ok)

    empty = data["count"] == 0
    result("Zero messages", empty, f"got {data['count']}")

    no_more = data["hasMore"] is False
    result("hasMore is False", no_more)

    no_cursor = data["nextCursor"] is None
    result("No cursor", no_cursor)

    return status_ok and empty and no_more and no_cursor


async def scenario_reconnect_flow(http: httpx.AsyncClient) -> bool:
    """Scenario 7: Full reconnect flow — WebSocket + REST.

    Simulates what a real client does:
    1. Connect WebSocket, receive messages live
    2. Disconnect (go offline)
    3. Reconnect WebSocket
    4. Call REST API to catch up on missed messages
    5. Merge live + history into one timeline
    """
    header("Scenario 7: Full Reconnect Flow")

    channel = f"hist-test-{uuid.uuid4().hex[:6]}"

    try:
        # Phase 1: Alice online, gets messages live
        alice = make_client("alice")
        bob = make_client("bob")
        await alice.connect()
        await bob.connect()
        await alice.join_channel(channel)
        await bob.join_channel(channel)

        await bob.send_message(channel, "live-msg-1")
        await bob.send_message(channel, "live-msg-2")
        await asyncio.sleep(1.0)

        live_received = [
            m for m in alice.received
            if m.get("type") == "message.received"
        ]
        result(
            "Phase 1: Alice got 2 live messages",
            len(live_received) == 2,
            f"got {len(live_received)}",
        )

        # Record last seen timestamp from live messages
        last_seen = max(
            m.get("timestamp", 0) for m in live_received
        ) if live_received else time.time()

        # Phase 2: Alice disconnects
        await alice.disconnect()
        log("Alice went offline", "WARN")
        await asyncio.sleep(0.5)

        # Bob sends more messages while Alice is offline
        await bob.send_message(channel, "missed-msg-3")
        await bob.send_message(channel, "missed-msg-4")
        await bob.send_message(channel, "missed-msg-5")
        await asyncio.sleep(PERSISTENCE_DELAY)

        await bob.disconnect()

        # Phase 3: Alice reconnects and catches up via REST
        log("Alice reconnecting...", "INFO")
        status, catchup = await api_get(
            http,
            f"/channels/{channel}/messages",
            {"after": last_seen, "limit": 50},
        )

        missed_count = catchup["count"]
        missed_ok = missed_count == 3
        result(
            "Phase 3: REST returns 3 missed messages",
            missed_ok,
            f"got {missed_count}",
        )

        if catchup["messages"]:
            missed_contents = [m["content"] for m in catchup["messages"]]
            content_ok = missed_contents == [
                "missed-msg-3", "missed-msg-4", "missed-msg-5"
            ]
            result("Missed messages correct", content_ok, f"{missed_contents}")

            # Phase 4: Full history should have all 5
            _, full = await api_get(
                http,
                f"/channels/{channel}/messages",
                {"limit": 50},
            )
            total_ok = full["count"] == 5
            result(
                "Full history has all 5 messages",
                total_ok,
                f"got {full['count']}",
            )

            return missed_ok and content_ok and total_ok

        return False
    finally:
        await cleanup_channel(channel)


# ══════════════════════════════════════════════════════════════
# Runner
# ══════════════════════════════════════════════════════════════

async def run_all():
    print("\n\033[1m  Message History API — Live Simulation\033[0m")
    print(f"  Server: {API_URL}")
    print(f"  Time:   {time.strftime('%Y-%m-%d %H:%M:%S')}")

    async with httpx.AsyncClient(timeout=30.0) as http:
        scenarios = [
            ("Load Newest", scenario_load_newest),
            ("Cursor Pagination", scenario_pagination),
            ("Reconnect Catch-Up", scenario_catchup),
            ("Single Message Lookup", scenario_single_message),
            ("Channel Stats", scenario_channel_stats),
            ("Empty Channel", scenario_empty_channel),
            ("Full Reconnect Flow", scenario_reconnect_flow),
        ]

        results_list = []
        for name, fn in scenarios:
            try:
                passed = await fn(http)
                results_list.append((name, passed))
            except Exception as e:
                log(f"Scenario crashed: {e}", "FAIL")
                import traceback
                traceback.print_exc()
                results_list.append((name, False))
            await asyncio.sleep(1.0)

    # ── Summary ───────────────────────────────────────────────
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
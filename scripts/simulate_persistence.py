"""
Persistence Service — Live Simulation

Tests the full pipeline:
  WebSocket send → Kafka → persistence consumer → PostgreSQL

Scenarios:
  1. Basic persistence — send message, verify it lands in DB
  2. Multiple messages — send several, all should be in DB
  3. Duplicate handling — same client_request_id twice, only one row
  4. Batch persistence — rapid-fire messages, all persisted
  5. Channel history query — messages queryable by channel
  6. Message ordering — DB preserves chronological order

Prerequisites:
  - Server running: uvicorn app.main:app --port 8000
  - Redis, Kafka, PostgreSQL all running
  - Alembic migrations applied: alembic upgrade head

Usage:
  python scripts/simulate_persistence.py
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

MOCK_USERS = {
    "alice": [
        {"token": "token-alice-1", "device_id": "alice-phone"},
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
        "DB": "\033[33m",
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
    device_id: str = "unknown"
    ws: object = None
    received: list = field(default_factory=list)
    _listener_task: object = None
    connected: bool = False

    async def connect(self) -> bool:
        url = f"{WS_URL}?token={self.token}"
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
        except websockets.ConnectionClosed:
            pass
        except Exception:
            pass
        finally:
            self.connected = False

    async def join_channel(self, channel_id: str):
        await self.ws.send(json.dumps({
            "type": "channel.join",
            "channel_id": channel_id,
        }))
        log(f"{self.user_id} joined #{channel_id}", "SEND")
        await asyncio.sleep(0.3)

    async def send_message(
        self, channel_id: str, content: str,
        client_request_id: str = None
    ) -> str | None:
        """Send message and return message_id from ack."""
        payload = {
            "type": "message.send",
            "channel_id": channel_id,
            "content": content,
        }
        if client_request_id:
            payload["client_request_id"] = client_request_id

        await self.ws.send(json.dumps(payload))
        log(f"{self.user_id} sent: \"{content[:40]}\"", "SEND")

        # Wait for ack
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline:
            for msg in self.received:
                if (
                    msg.get("type") == "message.ack"
                    and msg.get("channel_id") == channel_id
                ):
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
        self.connected = False
        log(f"{self.user_id} disconnected")

    def clear(self):
        self.received.clear()


def make_client(user_id: str, token_index: int = 0) -> SimulatedClient:
    tokens = MOCK_USERS[user_id]
    entry = tokens[token_index]
    return SimulatedClient(
        user_id=user_id,
        token=entry["token"],
        device_id=entry["device_id"],
    )


# ── Database Checker ──────────────────────────────────────────

class DBChecker:
    """Directly queries PostgreSQL to verify persistence.

    This is the KEY part of the simulation — we send messages
    through WebSocket/Kafka, then check the DB directly to
    confirm they were written.
    """

    def __init__(self):
        self.pool = None

    async def connect(self):
        self.pool = await asyncpg.create_pool(
            dsn=DATABASE_URL, min_size=2, max_size=5
        )
        log("DB checker connected")

    async def close(self):
        if self.pool:
            await self.pool.close()

    async def find_message(
        self, message_id: str, timeout: float = 10.0
    ) -> dict | None:
        """Poll DB until message appears (or timeout).

        The persistence service batches writes, so there's
        a delay between Kafka produce and DB insert. We poll
        instead of sleeping a fixed amount.
        """
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow(
                    "SELECT * FROM messages WHERE message_id = $1",
                    message_id,
                )
                if row:
                    return dict(row)
            await asyncio.sleep(0.3)
        return None

    async def find_messages_in_channel(
        self, channel_id: str, timeout: float = 10.0
    ) -> list[dict]:
        """Get all messages in a channel, ordered by time."""
        # Wait a bit for batch flush
        await asyncio.sleep(timeout)
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT * FROM messages
                WHERE channel_id = $1
                ORDER BY created_at ASC
                """,
                channel_id,
            )
            return [dict(r) for r in rows]

    async def count_by_message_id(self, message_id: str) -> int:
        """Count rows with this message_id (should be 0 or 1)."""
        async with self.pool.acquire() as conn:
            return await conn.fetchval(
                "SELECT COUNT(*) FROM messages WHERE message_id = $1",
                message_id,
            )

    async def cleanup_channel(self, channel_id: str):
        """Delete test messages after a scenario."""
        async with self.pool.acquire() as conn:
            deleted = await conn.execute(
                "DELETE FROM messages WHERE channel_id = $1",
                channel_id,
            )
            log(f"Cleaned up {channel_id}: {deleted}", "DB")


# ══════════════════════════════════════════════════════════════
# Scenarios
# ══════════════════════════════════════════════════════════════

async def scenario_basic_persistence(db: DBChecker) -> bool:
    """Scenario 1: Send one message, verify it's in PostgreSQL.

    The simplest test — does the full pipeline work?

    Bob sends → Kafka → persistence consumer → PostgreSQL
    We check PostgreSQL directly and verify every field.
    """
    header("Scenario 1: Basic Persistence")

    channel = f"persist-test-{uuid.uuid4().hex[:6]}"
    bob = make_client("bob")

    try:
        await bob.connect()
        await bob.join_channel(channel)

        message_id = await bob.send_message(
            channel, "This should end up in PostgreSQL"
        )

        if not message_id:
            result("Got message ack", False, "no ack received")
            return False

        result("Got message ack", True, f"id={message_id[:12]}...")

        # Check DB
        log(f"Checking DB for message {message_id[:12]}...", "DB")
        row = await db.find_message(message_id)

        if not row:
            result("Message found in DB", False, "not found after 10s")
            return False

        result("Message found in DB", True)

        # Verify fields
        fields_ok = True

        checks = [
            ("channel_id", row["channel_id"] == channel),
            ("sender_id", row["sender_id"] == "bob"),
            ("content", row["content"] == "This should end up in PostgreSQL"),
            ("created_at exists", row["created_at"] is not None),
            ("persisted_at exists", row["persisted_at"] is not None),
            ("correlation_id exists", row.get("correlation_id") is not None),
        ]

        for field_name, ok in checks:
            result(f"  Field: {field_name}", ok)
            if not ok:
                fields_ok = False

        return fields_ok
    finally:
        await bob.disconnect()
        await db.cleanup_channel(channel)


async def scenario_multiple_messages(db: DBChecker) -> bool:
    """Scenario 2: Multiple messages from different senders.

    Alice and Bob both send messages. All should be persisted
    with correct sender attribution.
    """
    header("Scenario 2: Multiple Messages, Multiple Senders")

    channel = f"persist-test-{uuid.uuid4().hex[:6]}"
    alice = make_client("alice")
    bob = make_client("bob")
    message_ids = []

    try:
        await alice.connect()
        await bob.connect()
        await alice.join_channel(channel)
        await bob.join_channel(channel)

        # Alice sends 2, Bob sends 2
        for content, client in [
            ("Alice message 1", alice),
            ("Bob message 1", bob),
            ("Alice message 2", alice),
            ("Bob message 2", bob),
        ]:
            mid = await client.send_message(channel, content)
            if mid:
                message_ids.append(mid)
            await asyncio.sleep(0.1)

        result(
            "All messages acked",
            len(message_ids) == 4,
            f"{len(message_ids)}/4 acked",
        )

        # Wait for persistence batch to flush
        log("Waiting for batch flush...", "DB")
        all_found = True
        for mid in message_ids:
            row = await db.find_message(mid)
            if not row:
                result(f"  Message {mid[:12]}...", False, "not in DB")
                all_found = False
            else:
                result(
                    f"  Message {mid[:12]}...",
                    True,
                    f"sender={row['sender_id']}, "
                    f"content=\"{row['content'][:30]}\"",
                )

        return all_found
    finally:
        await alice.disconnect()
        await bob.disconnect()
        await db.cleanup_channel(channel)


async def scenario_duplicate_handling(db: DBChecker) -> bool:
    """Scenario 3: Duplicate message handling.

    Bob sends the same message twice with the same
    client_request_id. The ingest layer dedup catches it —
    only one Kafka message produced, only one DB row.
    """
    header("Scenario 3: Duplicate Handling (ON CONFLICT)")

    channel = f"persist-test-{uuid.uuid4().hex[:6]}"
    bob = make_client("bob")
    dedup_key = f"dedup-{uuid.uuid4().hex[:8]}"

    try:
        await bob.connect()
        await bob.join_channel(channel)

        # Send twice with same client_request_id
        mid1 = await bob.send_message(
            channel, "This is a dedup test",
            client_request_id=dedup_key,
        )
        mid2 = await bob.send_message(
            channel, "This is a dedup test",
            client_request_id=dedup_key,
        )

        result("First send acked", mid1 is not None, f"id={mid1[:12] if mid1 else '?'}...")
        result("Second send acked", mid2 is not None, f"id={mid2[:12] if mid2 else '?'}...")

        # Both should return the same message_id (ingest dedup)
        same_id = mid1 == mid2
        result(
            "Same message_id returned (ingest dedup)",
            same_id,
            f"{'match' if same_id else f'{mid1[:8]} vs {mid2[:8]}'}",
        )

        # Verify only one row in DB
        if mid1:
            await asyncio.sleep(3.0)  # wait for batch flush
            count = await db.count_by_message_id(mid1)
            result(
                "Exactly 1 row in DB",
                count == 1,
                f"found {count} row(s)",
            )
            return same_id and count == 1

        return False
    finally:
        await bob.disconnect()
        await db.cleanup_channel(channel)


async def scenario_batch_persistence(db: DBChecker) -> bool:
    """Scenario 4: Rapid-fire messages — tests batching.

    Bob sends 20 messages as fast as possible.
    The persistence service should batch them and write
    efficiently. All 20 should end up in the DB.
    """
    header("Scenario 4: Batch Persistence (20 rapid messages)")

    channel = f"persist-test-{uuid.uuid4().hex[:6]}"
    bob = make_client("bob")
    message_ids = []
    msg_count = 20

    try:
        await bob.connect()
        await bob.join_channel(channel)

        t_start = time.monotonic()
        for i in range(msg_count):
            mid = await bob.send_message(
                channel, f"batch-msg-{i:03d}"
            )
            if mid:
                message_ids.append(mid)
            # No sleep — fire as fast as possible

        send_time = (time.monotonic() - t_start) * 1000
        result(
            f"All {msg_count} acked",
            len(message_ids) == msg_count,
            f"{len(message_ids)}/{msg_count} in {send_time:.0f}ms",
        )

        # Wait for persistence
        log("Waiting for batch flush to DB...", "DB")
        rows = await db.find_messages_in_channel(channel, timeout=5.0)

        result(
            f"All {msg_count} in DB",
            len(rows) == msg_count,
            f"found {len(rows)}/{msg_count}",
        )

        return len(message_ids) == msg_count and len(rows) == msg_count
    finally:
        await bob.disconnect()
        await db.cleanup_channel(channel)


async def scenario_channel_history(db: DBChecker) -> bool:
    """Scenario 5: Channel history query.

    Send messages from multiple users into a channel.
    Then query by channel — simulates what happens when
    a user opens a channel and loads message history.
    """
    header("Scenario 5: Channel History Query")

    channel = f"persist-test-{uuid.uuid4().hex[:6]}"
    alice = make_client("alice")
    bob = make_client("bob")

    try:
        await alice.connect()
        await bob.connect()
        await alice.join_channel(channel)
        await bob.join_channel(channel)

        conversation = [
            (alice, "Hey Bob, how's the project going?"),
            (bob, "Pretty good! Just finished the persistence layer"),
            (alice, "Nice! Does batching work?"),
            (bob, "Yep, 100 messages in one INSERT"),
            (alice, "That's awesome"),
        ]

        for client, content in conversation:
            await client.send_message(channel, content)
            await asyncio.sleep(0.1)

        # Query channel history
        log("Querying channel history from DB...", "DB")
        rows = await db.find_messages_in_channel(channel, timeout=5.0)

        result(
            "All 5 messages in DB",
            len(rows) == 5,
            f"found {len(rows)}/5",
        )

        if len(rows) == 5:
            # Verify the conversation is intact
            senders = [r["sender_id"] for r in rows]
            expected_senders = ["alice", "bob", "alice", "bob", "alice"]
            sender_ok = senders == expected_senders
            result(
                "Sender attribution correct",
                sender_ok,
                f"{senders}",
            )

            contents = [r["content"] for r in rows]
            content_ok = all(
                exp in actual
                for exp, actual in zip(
                    [c for _, c in conversation],
                    contents,
                )
            )
            result("Content preserved", content_ok)

            return sender_ok and content_ok

        return False
    finally:
        await alice.disconnect()
        await bob.disconnect()
        await db.cleanup_channel(channel)


async def scenario_message_ordering(db: DBChecker) -> bool:
    """Scenario 6: Messages stored in chronological order.

    Bob sends numbered messages. The DB should preserve
    the exact order via the created_at timestamp from
    Kafka (not the persisted_at time).
    """
    header("Scenario 6: Message Ordering in DB")

    channel = f"persist-test-{uuid.uuid4().hex[:6]}"
    bob = make_client("bob")
    msg_count = 10

    try:
        await bob.connect()
        await bob.join_channel(channel)

        for i in range(msg_count):
            await bob.send_message(channel, f"order-{i:03d}")
            await asyncio.sleep(0.05)

        log("Checking order in DB...", "DB")
        rows = await db.find_messages_in_channel(channel, timeout=5.0)

        count_ok = len(rows) == msg_count
        result(
            f"All {msg_count} persisted",
            count_ok,
            f"found {len(rows)}/{msg_count}",
        )

        if count_ok:
            contents = [r["content"] for r in rows]
            expected = [f"order-{i:03d}" for i in range(msg_count)]
            order_ok = contents == expected
            result(
                "Chronological order preserved",
                order_ok,
                f"{'correct' if order_ok else contents}",
            )

            # Verify timestamps are monotonically increasing
            timestamps = [r["created_at"] for r in rows]
            monotonic = all(
                timestamps[i] <= timestamps[i + 1]
                for i in range(len(timestamps) - 1)
            )
            result("Timestamps monotonically increasing", monotonic)

            return order_ok and monotonic

        return False
    finally:
        await bob.disconnect()
        await db.cleanup_channel(channel)


# ══════════════════════════════════════════════════════════════
# Runner
# ══════════════════════════════════════════════════════════════

async def run_all():
    print("\n\033[1m  Persistence Service — Live Simulation\033[0m")
    print(f"  Server:   {WS_URL}")
    print(f"  Database: {DATABASE_URL.split('@')[1] if '@' in DATABASE_URL else DATABASE_URL}")
    print(f"  Time:     {time.strftime('%Y-%m-%d %H:%M:%S')}")

    db = DBChecker()
    await db.connect()

    scenarios = [
        ("Basic Persistence", scenario_basic_persistence),
        ("Multiple Messages", scenario_multiple_messages),
        ("Duplicate Handling", scenario_duplicate_handling),
        ("Batch Persistence", scenario_batch_persistence),
        ("Channel History", scenario_channel_history),
        ("Message Ordering", scenario_message_ordering),
    ]

    results_list = []
    for name, fn in scenarios:
        try:
            passed = await fn(db)
            results_list.append((name, passed))
        except Exception as e:
            log(f"Scenario crashed: {e}", "FAIL")
            import traceback
            traceback.print_exc()
            results_list.append((name, False))
        await asyncio.sleep(1.0)

    await db.close()

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
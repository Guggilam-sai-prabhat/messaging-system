"""
Full system test — run through every flow in order.

Start server first:
    uv run fastapi dev app/main.py

Then run:
    pip install websockets httpx
    python test_full_flow.py
"""

import asyncio
import json
import httpx
import websockets

BASE = "http://localhost:8000"
WS = "ws://localhost:8000"


def header(n, title):
    print(f"\n{'─' * 50}")
    print(f"  Step {n}: {title}")
    print(f"{'─' * 50}")


async def main():
    print("=" * 50)
    print("  FULL SYSTEM FLOW TEST")
    print("=" * 50)

    # ── Step 1: Health check ──────────────────────────
    header(1, "Health check (GET /health)")
    async with httpx.AsyncClient() as http:
        r = await http.get(f"{BASE}/health")
        data = r.json()
        print(f"  Status: {data['status']}")
        print(f"  Registry: {data['registry']}")
        assert data["registry"]["online_users"] == 0
        print("  PASS — server healthy, nobody online")

    # ── Step 2: Reject bad token ──────────────────────
    header(2, "Auth rejection (bad token)")
    try:
        async with websockets.connect(f"{WS}/ws?token=fake") as ws:
            await ws.recv()
    except websockets.exceptions.ConnectionClosed as e:
        print(f"  Closed with code: {e.code}")
        assert e.code == 4001
        print("  PASS — invalid token rejected")

    # ── Step 3: Connect alice ─────────────────────────
    header(3, "Connect alice (WS /ws?token=token-alice-1)")
    async with websockets.connect(f"{WS}/ws?token=token-alice-1") as alice:
        welcome = json.loads(await alice.recv())
        print(f"  type: {welcome['type']}")
        print(f"  user_id: {welcome['user_id']}")
        print(f"  device_id: {welcome['device_id']}")
        print(f"  active_connections: {welcome['active_connections']}")
        assert welcome["type"] == "connection.established"
        print("  PASS — alice connected")

        # ── Step 4: Check alice status via REST ───────
        header(4, "User status (GET /users/alice/status)")
        async with httpx.AsyncClient() as http:
            r = await http.get(f"{BASE}/users/alice/status")
            data = r.json()
            print(f"  online: {data['online']}")
            print(f"  devices: {[c['device_id'] for c in data['connections']]}")
            assert data["online"] is True
            print("  PASS — REST confirms alice online")

        # ── Step 5: Join channel ──────────────────────
        header(5, "Join channel (WS channel.join)")
        await alice.send(json.dumps({
            "type": "channel.join",
            "channel_id": "general",
        }))
        resp = json.loads(await alice.recv())
        print(f"  type: {resp['type']}")
        print(f"  channel_id: {resp['channel_id']}")
        print(f"  was_new: {resp['was_new']}")
        assert resp["type"] == "channel.joined"
        print("  PASS — joined #general")

        # ── Step 6: Verify channel via REST ───────────
        header(6, "Channel info (GET /channels/general)")
        async with httpx.AsyncClient() as http:
            r = await http.get(f"{BASE}/channels/general")
            data = r.json()
            print(f"  members: {[m['user_id'] for m in data['members']]}")
            print(f"  alice online: {data['members'][0]['online']}")
            assert len(data["members"]) == 1
            print("  PASS — REST confirms alice in #general")

        # ── Step 7: Send valid message ────────────────
        header(7, "Send message (WS message.send)")
        await alice.send(json.dumps({
            "type": "message.send",
            "channel_id": "general",
            "content": "Hello world!",
        }))
        ack = json.loads(await alice.recv())
        print(f"  type: {ack['type']}")
        print(f"  message_id: {ack['message_id']}")
        print(f"  channel_id: {ack['channel_id']}")
        print(f"  timestamp: {ack['timestamp']}")
        assert ack["type"] == "message.ack"
        assert "message_id" in ack
        print("  PASS — message ingested, ack received")
        print("  (check server terminal for structured log)")

        # ── Step 8: Error cases ───────────────────────
        header(8, "Validation errors")

        # 8a: not a member
        await alice.send(json.dumps({
            "type": "message.send",
            "channel_id": "secret-ops",
            "content": "should fail",
        }))
        err = json.loads(await alice.recv())
        print(f"  8a not-a-member: {err['reason']}")
        assert err["type"] == "message.error"

        # 8b: empty content
        await alice.send(json.dumps({
            "type": "message.send",
            "channel_id": "general",
            "content": "",
        }))
        err = json.loads(await alice.recv())
        print(f"  8b empty content: {err['reason']}")
        assert err["type"] == "message.error"

        # 8c: missing channel_id
        await alice.send(json.dumps({
            "type": "message.send",
            "content": "no channel",
        }))
        err = json.loads(await alice.recv())
        print(f"  8c missing field: {err['reason']}")
        assert err["type"] == "message.error"

        # 8d: content too long
        await alice.send(json.dumps({
            "type": "message.send",
            "channel_id": "general",
            "content": "x" * 5000,
        }))
        err = json.loads(await alice.recv())
        print(f"  8d too long:      {err['reason'][:60]}...")
        assert err["type"] == "message.error"
        print("  PASS — all 4 error cases rejected cleanly")

        # ── Step 9: Heartbeat ─────────────────────────
        header(9, "Heartbeat (WS ping/pong)")
        await alice.send(json.dumps({"type": "ping"}))
        pong = json.loads(await alice.recv())
        print(f"  type: {pong['type']}")
        print(f"  server_time: {pong['server_time']:.0f}")
        assert pong["type"] == "pong"
        print("  PASS — heartbeat working")

        # ── Step 10: Multi-device ─────────────────────
        header(10, "Multi-device (alice-laptop joins)")
        async with websockets.connect(f"{WS}/ws?token=token-alice-2") as laptop:
            w2 = json.loads(await laptop.recv())
            print(f"  laptop active_connections: {w2['active_connections']}")
            assert w2["active_connections"] == 2

            async with httpx.AsyncClient() as http:
                r = await http.get(f"{BASE}/users/alice/status")
                data = r.json()
                devices = [c["device_id"] for c in data["connections"]]
                print(f"  REST shows devices: {devices}")
                assert len(devices) == 2
            print("  PASS — two devices tracked")

        # laptop disconnected
        await asyncio.sleep(0.3)
        async with httpx.AsyncClient() as http:
            r = await http.get(f"{BASE}/users/alice/status")
            data = r.json()
            print(f"  After laptop disconnect: {len(data['connections'])} device(s)")
            assert len(data["connections"]) == 1
            print("  PASS — laptop cleaned up, phone remains")

        # ── Step 11: No delivery to Bob ───────────────
        header(11, "Verify NO delivery to Bob")
        async with websockets.connect(f"{WS}/ws?token=token-bob-1") as bob:
            await bob.recv()  # welcome
            await bob.send(json.dumps({
                "type": "channel.join",
                "channel_id": "general",
            }))
            await bob.recv()  # joined

            # Alice sends
            await alice.send(json.dumps({
                "type": "message.send",
                "channel_id": "general",
                "content": "Bob should NOT get this",
            }))
            ack = json.loads(await alice.recv())
            print(f"  Alice ack: {ack['type']}")

            # Bob should get NOTHING
            try:
                msg = await asyncio.wait_for(bob.recv(), timeout=1.0)
                print(f"  FAIL — Bob received: {msg}")
                assert False
            except asyncio.TimeoutError:
                print("  Bob received: nothing (timed out)")
                print("  PASS — no delivery without Kafka")

    # alice disconnected
    await asyncio.sleep(0.3)

    # ── Step 12: Final health check ───────────────────
    header(12, "Final health check")
    async with httpx.AsyncClient() as http:
        r = await http.get(f"{BASE}/health")
        data = r.json()
        reg = data["registry"]
        print(f"  online_users: {reg['online_users']}")
        print(f"  total_connections: {reg['total_connections']}")
        print(f"  active_channels: {reg['active_channels']}")
        # Everyone disconnected but channels persist
        assert reg["online_users"] == 0
        assert reg["active_channels"] == 1
        print("  PASS — clean state, channels persist")

    print(f"\n{'=' * 50}")
    print("  ALL 12 STEPS PASSED")
    print(f"{'=' * 50}")


if __name__ == "__main__":
    asyncio.run(main())
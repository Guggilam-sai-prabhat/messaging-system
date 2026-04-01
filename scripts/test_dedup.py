"""
Dedup test — sends the same client_request_id twice.

Expected behavior:
  - First send → message.ack with was_dedup=False
  - Second send → message.ack with was_dedup=True, SAME message_id
  - Kafka consumer sees only ONE message

Usage:
    python scripts/test_dedup.py
"""

import asyncio
import json
import websockets

WS_URL = "ws://localhost:8000/ws"
TOKEN = "token-bob-1"
CHANNEL = "general"
DEDUP_KEY = "dedup-test-001"


def print_response(label: str, data: dict):
    print(f"\n  ← {label}")
    for k, v in data.items():
        print(f"    {k}: {v}")


async def main():
    uri = f"{WS_URL}?token={TOKEN}"
    print(f"Connecting to {uri}...\n")

    async with websockets.connect(uri) as ws:
        # Connect
        resp = json.loads(await ws.recv())
        print_response("CONNECTED", resp)

        # Join channel
        await ws.send(json.dumps({
            "type": "channel.join",
            "channel_id": CHANNEL,
        }))
        resp = json.loads(await ws.recv())
        print_response("JOINED", resp)

        # ── First send ────────────────────────────────────────
        print("\n" + "=" * 50)
        print("FIRST SEND (should produce to Kafka)")
        print("=" * 50)

        await ws.send(json.dumps({
            "type": "message.send",
            "channel_id": CHANNEL,
            "content": "This message should only appear once in Kafka",
            "client_request_id": DEDUP_KEY,
        }))
        resp1 = json.loads(await ws.recv())
        print_response("ACK", resp1)

        first_message_id = resp1.get("message_id")
        first_was_dedup = resp1.get("was_dedup")

        # ── Second send (same client_request_id) ──────────────
        print("\n" + "=" * 50)
        print("SECOND SEND (should return cached, skip Kafka)")
        print("=" * 50)

        await ws.send(json.dumps({
            "type": "message.send",
            "channel_id": CHANNEL,
            "content": "This message should only appear once in Kafka",
            "client_request_id": DEDUP_KEY,
        }))
        resp2 = json.loads(await ws.recv())
        print_response("ACK", resp2)

        second_message_id = resp2.get("message_id")
        second_was_dedup = resp2.get("was_dedup")

        # ── Verify ────────────────────────────────────────────
        print("\n" + "=" * 50)
        print("VERIFICATION")
        print("=" * 50)

        print(f"\n  First message_id:  {first_message_id}")
        print(f"  Second message_id: {second_message_id}")
        print(f"  IDs match:         {first_message_id == second_message_id}")
        print(f"  First was_dedup:   {first_was_dedup}")
        print(f"  Second was_dedup:  {second_was_dedup}")

        if (
            first_message_id == second_message_id
            and first_was_dedup is False
            and second_was_dedup is True
        ):
            print("\n  ✅ DEDUP WORKING CORRECTLY")
        else:
            print("\n  ❌ DEDUP NOT WORKING")

        # ── Third send (different client_request_id) ──────────
        print("\n" + "=" * 50)
        print("THIRD SEND (different request_id, should produce)")
        print("=" * 50)

        await ws.send(json.dumps({
            "type": "message.send",
            "channel_id": CHANNEL,
            "content": "This is a genuinely new message",
            "client_request_id": "dedup-test-002",
        }))
        resp3 = json.loads(await ws.recv())
        print_response("ACK", resp3)

        third_message_id = resp3.get("message_id")
        if third_message_id != first_message_id:
            print("\n  ✅ New request_id produced new message")
        else:
            print("\n  ❌ Should have been a new message")


if __name__ == "__main__":
    asyncio.run(main())
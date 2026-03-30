"""
WebSocket test client — connects, joins a channel, sends messages.

Usage:
    python scripts/send_message.py

Interactive mode: type messages and see acks in real time.
Each message flows: WebSocket → Validate → Kafka → ack back here.

Requires: pip install websockets
(already in your dependencies)
"""

import asyncio
import json
import sys

import websockets

# ── Config ────────────────────────────────────────────────────
# Adjust these to match your auth setup.
# If your authenticate_token() accepts any token in dev mode,
# use a dummy token that returns valid claims.
WS_URL = "ws://localhost:8000/ws"
TOKEN = "token-alice-1"
CHANNEL = "general"


def print_response(label: str, data: dict):
    """Pretty-print a WebSocket response."""
    print(f"\n  ← {label}")
    print(f"    {json.dumps(data, indent=4)}")


async def main():
    uri = f"{WS_URL}?token={TOKEN}"
    print(f"Connecting to {uri}...")

    try:
        async with websockets.connect(uri) as ws:

            # ── 1. Connection established ─────────────────────
            resp = json.loads(await ws.recv())
            if resp.get("type") == "connection.established":
                print_response("CONNECTED", resp)
            else:
                print(f"Unexpected response: {resp}")
                return

            # ── 2. Join channel ───────────────────────────────
            await ws.send(json.dumps({
                "type": "channel.join",
                "channel_id": CHANNEL,
            }))
            resp = json.loads(await ws.recv())
            print_response("JOINED", resp)

            # ── 3. Send a test ping ───────────────────────────
            await ws.send(json.dumps({"type": "ping"}))
            resp = json.loads(await ws.recv())
            print_response("PONG", resp)

            # ── 4. Interactive message loop ───────────────────
            print(f"\nReady! Type messages to send to '{CHANNEL}'.")
            print("Commands:  /quit  /metrics  /channel <name>\n")

            current_channel = CHANNEL

            while True:
                try:
                    text = await asyncio.get_event_loop().run_in_executor(
                        None, lambda: input(f"[{current_channel}] → ")
                    )
                except EOFError:
                    break

                text = text.strip()
                if not text:
                    continue

                # ── Commands ──────────────────────────────────
                if text == "/quit":
                    print("Closing connection...")
                    break

                if text == "/metrics":
                    # Fetch metrics via a quick HTTP call
                    import urllib.request
                    try:
                        with urllib.request.urlopen(
                            "http://localhost:8000/metrics"
                        ) as r:
                            data = json.loads(r.read())
                            print(f"\n  Metrics:")
                            print(f"    {json.dumps(data, indent=4)}\n")
                    except Exception as e:
                        print(f"  Failed to fetch metrics: {e}")
                    continue

                if text.startswith("/channel "):
                    new_ch = text.split(" ", 1)[1].strip()
                    if not new_ch:
                        print("  Usage: /channel <name>")
                        continue
                    await ws.send(json.dumps({
                        "type": "channel.join",
                        "channel_id": new_ch,
                    }))
                    resp = json.loads(await ws.recv())
                    print_response("JOINED", resp)
                    current_channel = new_ch
                    continue

                # ── Send message ──────────────────────────────
                await ws.send(json.dumps({
                    "type": "message.send",
                    "channel_id": current_channel,
                    "content": text,
                    "client_request_id": f"cli-{id(text)}",
                }))

                resp = json.loads(await ws.recv())
                msg_type = resp.get("type", "")

                if msg_type == "message.ack":
                    print_response("ACK", resp)
                elif msg_type == "message.error":
                    print_response("VALIDATION ERROR", resp)
                elif msg_type == "message.kafka_error":
                    print_response("KAFKA ERROR", resp)
                else:
                    print_response("RESPONSE", resp)

    except websockets.exceptions.InvalidStatusCode as e:
        print(f"Connection rejected: {e}")
    except ConnectionRefusedError:
        print("Could not connect — is the server running?")
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    asyncio.run(main())
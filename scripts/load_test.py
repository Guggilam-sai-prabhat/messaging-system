"""
Load test — sends N messages across M channels and reports stats.

Usage:
    python scripts/load_test.py
    python scripts/load_test.py --messages 500 --channels 5

Connects a single WebSocket, joins all channels, fires messages
as fast as possible, then prints a summary.

Use this to:
  1. Verify messages/sec on the /metrics endpoint
  2. Stress-test the circuit breaker (kill Kafka mid-run)
  3. Check for message loss (compare sent count vs consumer count)
"""

import asyncio
import json
import time
import argparse

import websockets

WS_URL = "ws://localhost:8000/ws"
TOKEN = "dev-test-token"


async def main(num_messages: int, num_channels: int):
    uri = f"{WS_URL}?token={TOKEN}"
    channels = [f"load-test-{i}" for i in range(num_channels)]

    print(f"Connecting to {uri}...")
    print(f"Messages: {num_messages}  Channels: {num_channels}\n")

    async with websockets.connect(uri) as ws:
        # ── Connect ───────────────────────────────────────────
        resp = json.loads(await ws.recv())
        assert resp["type"] == "connection.established"
        print(f"Connected as {resp['user_id']}")

        # ── Join all channels ─────────────────────────────────
        for ch in channels:
            await ws.send(json.dumps({
                "type": "channel.join",
                "channel_id": ch,
            }))
            await ws.recv()
        print(f"Joined {num_channels} channels")

        # ── Fire messages ─────────────────────────────────────
        acks = 0
        errors = 0
        kafka_errors = 0
        latencies = []

        t_start = time.monotonic()
        print(f"Sending {num_messages} messages...\n")

        for i in range(num_messages):
            ch = channels[i % num_channels]
            t_send = time.monotonic()

            await ws.send(json.dumps({
                "type": "message.send",
                "channel_id": ch,
                "content": f"Load test message {i}",
                "client_request_id": f"load-{i}",
            }))

            resp = json.loads(await ws.recv())
            latency_ms = (time.monotonic() - t_send) * 1000

            msg_type = resp.get("type")
            if msg_type == "message.ack":
                acks += 1
                latencies.append(latency_ms)
            elif msg_type == "message.error":
                errors += 1
            elif msg_type == "message.kafka_error":
                kafka_errors += 1

            # Progress indicator
            if (i + 1) % 100 == 0:
                print(f"  Sent {i + 1}/{num_messages}...")

        elapsed = time.monotonic() - t_start

        # ── Report ────────────────────────────────────────────
        print("\n" + "=" * 50)
        print("LOAD TEST RESULTS")
        print("=" * 50)
        print(f"Total sent:     {num_messages}")
        print(f"Acks:           {acks}")
        print(f"Validation err: {errors}")
        print(f"Kafka errors:   {kafka_errors}")
        print(f"Elapsed:        {elapsed:.2f}s")
        print(f"Throughput:     {num_messages / elapsed:.1f} msg/s")

        if latencies:
            latencies.sort()
            n = len(latencies)
            print(f"\nLatency (round-trip WebSocket → Kafka → ack):")
            print(f"  avg:  {sum(latencies) / n:.1f}ms")
            print(f"  p50:  {latencies[n // 2]:.1f}ms")
            print(f"  p95:  {latencies[int(n * 0.95)]:.1f}ms")
            print(f"  p99:  {latencies[int(n * 0.99)]:.1f}ms")
            print(f"  max:  {latencies[-1]:.1f}ms")

        # ── Fetch server metrics ──────────────────────────────
        print("\nServer-side metrics:")
        import urllib.request
        try:
            with urllib.request.urlopen(
                "http://localhost:8000/metrics"
            ) as r:
                metrics = json.loads(r.read())
                print(f"  {json.dumps(metrics, indent=4)}")
        except Exception as e:
            print(f"  Could not fetch: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load test")
    parser.add_argument(
        "--messages", type=int, default=200,
        help="Number of messages to send (default: 200)",
    )
    parser.add_argument(
        "--channels", type=int, default=3,
        help="Number of channels to spread across (default: 3)",
    )
    args = parser.parse_args()
    asyncio.run(main(args.messages, args.channels))
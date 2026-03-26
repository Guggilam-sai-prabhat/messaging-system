import asyncio
import json
import websockets

async def test():
    uri = "ws://localhost:8000/ws?token=token-alice-1"
    async with websockets.connect(uri) as ws:
        # Wait for connection.established
        print(await ws.recv())

        # Join a channel first
        await ws.send(json.dumps({
            "type": "channel.join",
            "channel_id": "test-channel"
        }))
        print(await ws.recv())

        # Send a message
        await ws.send(json.dumps({
            "type": "message.send",
            "channel_id": "test-channel",
            "content": "Hello from Kafka integration test!"
        }))
        print(await ws.recv())  # Should be message.ack

asyncio.run(test())

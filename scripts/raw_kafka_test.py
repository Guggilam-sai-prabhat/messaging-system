import time
import json
from confluent_kafka import Producer

p = Producer({
    "bootstrap.servers": "localhost:9092",
    "acks": "all",
    "linger.ms": 5,
})

delivered = []

def cb(err, msg):
    delivered.append(time.monotonic())

for i in range(5):
    t = time.monotonic()
    p.produce("channel-messages", key=b"test", value=b'{"test": true}', callback=cb)
    p.flush()  # blocks until delivered
    latency = (time.monotonic() - t) * 1000
    print(f"Message {i}: {latency:.1f}ms")
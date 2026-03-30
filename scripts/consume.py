"""
Kafka consumer — watches channel-messages topic and prints every message.

Usage:
    python scripts/consume.py

Sits and waits. Every message that hits Kafka shows up here
with its partition key, partition number, and full payload.
Press Ctrl+C to stop.
"""

import json
import sys
from confluent_kafka import Consumer, KafkaError

TOPIC = "channel-messages"
BROKER = "localhost:9092"


def main():
    c = Consumer({
        "bootstrap.servers": BROKER,
        "group.id": "test-consumer-debug",
        "auto.offset.reset": "earliest",
    })
    c.subscribe([TOPIC])

    print(f"Consuming from '{TOPIC}' on {BROKER}")
    print(f"Waiting for messages... (Ctrl+C to stop)\n")
    print("-" * 60)

    try:
        while True:
            msg = c.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"ERROR: {msg.error()}", file=sys.stderr)
                continue

            key = msg.key().decode("utf-8") if msg.key() else "None"
            value = json.loads(msg.value().decode("utf-8"))

            print(f"Partition: {msg.partition()}  Offset: {msg.offset()}")
            print(f"Key:       {key}")
            print(f"Payload:   {json.dumps(value, indent=2)}")
            print("-" * 60)

    except KeyboardInterrupt:
        print("\nStopping consumer...")
    finally:
        c.close()


if __name__ == "__main__":
    main()
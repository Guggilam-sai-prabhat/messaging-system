from confluent_kafka import Consumer
import json

c = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "debug-check-2",
    "auto.offset.reset": "earliest",  # read ALL messages, not just new ones
})
c.subscribe(["channel-messages"])
print("Waiting for messages on channel-messages...")
print("(Run the simulation in another terminal)\n")

count = 0
while True:
    msg = c.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        print(f"Error: {msg.error()}")
        continue
    count += 1
    data = json.loads(msg.value())
    print(f"[MSG {count}] partition={msg.partition()} offset={msg.offset()}")
    print(f"  channelId: {data.get('channelId')}")
    print(f"  senderId:  {data.get('senderId')}")
    print(f"  content:   {data.get('content')}")
    print()
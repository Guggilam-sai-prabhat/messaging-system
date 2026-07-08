"""
Centralized configuration for the AI service, mirroring the pattern in
app/config.py and workers/config.py: env-overridable constants in one place.
"""

import os

# Sender identity the AI service uses when it *produces* a reply message.
# Reserved so incoming-message loop guards can recognize "this message came
# from the AI" without needing a separate message "type" field on the wire.
AI_SENDER_ID = os.getenv("AI_SENDER_ID", "ai-assistant")

# Sender identities that never trigger the AI, beyond AI_SENDER_ID itself.
# Populated with system/automation accounts as they're introduced.
SYSTEM_SENDER_IDS = frozenset(
    s for s in os.getenv("SYSTEM_SENDER_IDS", "").split(",") if s
)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_GROUP_ID = os.getenv("AI_KAFKA_GROUP_ID", "ai-service-group")

# Same topic app/core/kafka_producer.py publishes chat messages to.
MESSAGES_TOPIC = "channel-messages"

"""
Startup script — run before launching the app.

Creates required Kafka topics if they don't already exist.
Safe to run multiple times: existing topics are left untouched.
"""

import sys
import logging
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import KafkaException

# Import settings so bootstrap servers come from the same source of truth
sys.path.insert(0, ".")
from app.config import settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
logger = logging.getLogger("startup")

TOPICS: list[dict] = [
    {
        "name": "channel-messages",
        "num_partitions": 3,
        "replication_factor": 1,
    },
    {
        # Document processing events. Separate topic from channel-messages
        # so the document consumer scales independently and can have its
        # own retention policy (longer — documents may need reprocessing).
        # num_partitions=3 matches channel-messages so load is spread
        # evenly across brokers; increase if document upload volume grows.
        "name": "document-processing",
        "num_partitions": 3,
        "replication_factor": 1,
    },
]


def create_topics_if_missing() -> None:
    admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap_servers})

    # Fetch existing topics
    metadata = admin.list_topics(timeout=10)
    existing = set(metadata.topics.keys())
    logger.info(f"Existing Kafka topics: {existing or '(none)'}")

    to_create = [
        NewTopic(
            t["name"],
            num_partitions=t["num_partitions"],
            replication_factor=t["replication_factor"],
        )
        for t in TOPICS
        if t["name"] not in existing
    ]

    if not to_create:
        logger.info("All required topics already exist — nothing to create")
        return

    results = admin.create_topics(to_create)

    all_ok = True
    for topic, future in results.items():
        try:
            future.result()
            logger.info(f"Created topic: {topic}")
        except KafkaException as e:
            logger.error(f"Failed to create topic '{topic}': {e}")
            all_ok = False

    if not all_ok:
        sys.exit(1)


if __name__ == "__main__":
    logger.info(
        f"Running startup checks (brokers={settings.kafka_bootstrap_servers})"
    )
    create_topics_if_missing()
    logger.info("Startup checks complete")
"""
Application entrypoint — wires everything together.

Startup order:
  1. Redis (registry, dedup, pub/sub depend on it)
  2. PostgreSQL (persistence depends on it)
  3. Pub/Sub subscriber (needs Redis + registry)
  4. Kafka producer + poller
  5. Delivery consumer (needs registry + event loop)
  6. Persistence consumer (needs DB + event loop)

Shutdown is reverse order.
"""

import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI

from app.routers import health, channels, ws, messages
from app.core.kafka_producer import kafka_producer
from app.core.redis_client import redis_client
from app.db.database import database
from app.core.delivery_service import delivery_service
from app.core.persistence_service import persistence_service
from app.core.pubsub_subscriber import pubsub_subscriber
from app.dependencies import registry

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
logger = logging.getLogger("app")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # ── Startup ───────────────────────────────────────────────
    await redis_client.initialize()
    await database.initialize()

    pubsub_subscriber.set_registry(registry)
    await pubsub_subscriber.start()

    kafka_producer.start()
    kafka_producer.start_poller()

    delivery_service.set_registry(registry)
    delivery_service.start()
    delivery_service.start_consumer_thread()

    persistence_service.start()
    persistence_service.start_consumer_thread()

    logger.info("Server ready")
    yield

    # ── Shutdown ──────────────────────────────────────────────
    logger.info("Server shutting down")
    persistence_service.shutdown()
    delivery_service.shutdown()
    kafka_producer.shutdown(timeout=10.0)
    await pubsub_subscriber.shutdown()
    await database.close()
    await redis_client.close()


app = FastAPI(
    title="Messaging System",
    version="0.6.0",
    lifespan=lifespan,
)

app.include_router(health.router)
app.include_router(channels.router)
app.include_router(ws.router)
app.include_router(messages.router)
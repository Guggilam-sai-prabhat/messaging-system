"""
Application entrypoint — wires everything together.

Startup order:
  1. Redis (registry, dedup, pub/sub depend on it)
  2. PostgreSQL (persistence depends on it)
  3. Rebuild Redis channel membership from DB
  4. Pub/Sub subscriber (needs Redis + registry)
  5. Kafka producer + poller
  6. Delivery consumer (needs registry + event loop)
  7. Persistence consumer (needs DB + event loop)

Shutdown is reverse order.

Changes:
  - membership_service.rebuild_all_channels() at startup
    populates Redis from the DB so channels created via the
    new HTTP endpoints are available to the delivery pipeline
    even after a Redis restart.
"""

import logging
import subprocess
import sys
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.routers import health, channels, ws, messages, auth
from app.core.kafka_producer import kafka_producer
from app.core.redis_client import redis_client
from app.db.database import database
from app.services.delivery_service import delivery_service
from app.services.persistence_service import persistence_service
from app.core.pubsub_subscriber import pubsub_subscriber
from app.services.presence_service import presence_service
from app.dependencies import registry, membership_service

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
logger = logging.getLogger("app")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # ── Startup ───────────────────────────────────────────────
    result = subprocess.run(
        [sys.executable, "scripts/startup.py"],
        capture_output=True,
        text=True,
    )
    if result.stdout:
        logger.info(result.stdout.strip())
    if result.returncode != 0:
        logger.error(result.stderr.strip())
        raise RuntimeError("Startup script failed — aborting server start")

    await redis_client.initialize()
    await database.initialize()

    # Rebuild Redis channel membership from DB.
    # This ensures Redis has accurate membership data even after
    # a Redis flush/restart. Runs with a semaphore (max 10
    # concurrent DB queries) so it doesn't hammer Postgres.
    rebuild_counts = await membership_service.rebuild_all_channels()
    logger.info(
        f"Rebuilt {len(rebuild_counts)} channel(s) into Redis"
    )

    pubsub_subscriber.set_registry(registry)
    await pubsub_subscriber.start()

    presence_service.set_registry(registry)
    await presence_service.start()

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
    await presence_service.shutdown()
    await pubsub_subscriber.shutdown()
    await database.close()
    await redis_client.close()


app = FastAPI(
    title="Messaging System",
    version="0.7.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(auth.router)
app.include_router(health.router)
app.include_router(channels.router)
app.include_router(ws.router)
app.include_router(messages.router)
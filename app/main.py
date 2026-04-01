"""
Application entrypoint — wires routers together.

Startup order matters:
  1. Redis first (registry, dedup, delivery all depend on it)
  2. Kafka producer (needs settings)
  3. Kafka producer poller (needs running event loop)
  4. Delivery service consumer (needs registry + event loop)

Shutdown order is reverse. Delivery consumer stops first
(stops consuming new work), then producer flushes, then
Redis closes.
"""

import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI

from app.routers import health, channels, ws
from app.core.kafka_producer import kafka_producer
from app.core.redis_client import redis_client
from app.core.delivery_service import delivery_service
from app.dependencies import registry  # the ONE singleton everyone shares

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
logger = logging.getLogger("app")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # ── Startup ───────────────────────────────────────────────
    await redis_client.initialize()

    kafka_producer.start()
    kafka_producer.start_poller()

    # Delivery service needs the registry to find local
    # WebSocket connections. Inject it before starting.
    delivery_service.set_registry(registry)
    delivery_service.start()
    delivery_service.start_consumer_thread()

    logger.info("Server ready")
    yield

    # ── Shutdown ──────────────────────────────────────────────
    logger.info("Server shutting down")
    delivery_service.shutdown()     # stop consuming first
    kafka_producer.shutdown(timeout=10.0)
    await redis_client.close()


app = FastAPI(
    title="Messaging System — Connection + Delivery Layer",
    version="0.4.0",
    lifespan=lifespan,
)

app.include_router(health.router)
app.include_router(channels.router)
app.include_router(ws.router)
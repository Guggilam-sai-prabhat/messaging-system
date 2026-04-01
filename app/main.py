"""
Application entrypoint — wires routers together.
"""

import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI

from app.routers import health, channels, ws
from app.core.kafka_producer import kafka_producer
from app.core.redis_client import redis_client

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
logger = logging.getLogger("app")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # ── Startup ───────────────────────────────────────────────
    # Order matters:
    #   1. Redis first (registry + dedup depend on it)
    #   2. Kafka producer (needs config from settings)
    #   3. Kafka poller (needs running event loop)
    await redis_client.initialize()
    kafka_producer.start()
    kafka_producer.start_poller()
    logger.info("Server starting")

    yield

    # ── Shutdown ──────────────────────────────────────────────
    logger.info("Server shutting down")
    kafka_producer.shutdown(timeout=10.0)
    await redis_client.close()


app = FastAPI(
    title="Messaging System — Connection Layer",
    version="0.3.0",
    lifespan=lifespan,
)

app.include_router(health.router)
app.include_router(channels.router)
app.include_router(ws.router)
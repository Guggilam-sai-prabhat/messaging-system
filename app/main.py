"""
Application entrypoint — wires everything together.

Startup order:
  1. Redis (registry, dedup, pub/sub depend on it)
  2. PostgreSQL (persistence depends on it)
  3. Rebuild Redis channel membership from DB
  4. Pub/Sub subscriber (needs Redis + registry)
  5. Presence service (needs Redis + registry)
  6. Kafka producer + poller
  7. Delivery consumer (needs registry + event loop)
  8. Persistence consumer (needs DB + event loop)
  9. MinIO storage service
  10. APScheduler reconciliation job (needs Kafka + DB — must be last)

Shutdown is reverse order:
  10. APScheduler — stop scheduling new jobs before anything it
      depends on (Kafka, DB) starts tearing down
  9-7. consumers + producer — flush in-flight messages
  ...
  1. Redis

─── Why APScheduler is last in startup ──────────────────────────
The reconciliation job fires immediately on startup (next_run_time=now)
to catch documents stuck from before the last restart. It needs both
kafka_producer (to re-enqueue) and database (to query stuck rows).
Starting it last guarantees both dependencies are live when that
first run executes.

─── Why APScheduler is first in shutdown ────────────────────────
If we shut down Kafka first and the scheduler's final job happened
to fire at that exact moment, produce_document_event would hit a
dead producer. Stopping the scheduler first eliminates the race.
"""

import logging
import subprocess
import sys
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.routers import health, channels, ws, messages, auth, upload, documents
from app.core.kafka_producer import kafka_producer
from app.core.redis_client import redis_client
from app.core.reconciliation import create_scheduler
from app.db.database import database
from app.services.delivery_service import delivery_service
from app.services.persistence_service import persistence_service
from app.core.pubsub_subscriber import pubsub_subscriber
from app.services.presence_service import presence_service
from app.dependencies import registry, membership_service
from app.dependencies import storage_service

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
logger = logging.getLogger("app")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # ── Startup ───────────────────────────────────────────────

    # Run topic creation and other pre-flight checks.
    # startup.py is idempotent — safe to run every boot.
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

    # 1. Redis
    await redis_client.initialize()

    # 2. PostgreSQL
    await database.initialize()

    # 3. Rebuild Redis channel membership from DB.
    #    Ensures Redis has accurate data even after a flush/restart.
    rebuild_counts = await membership_service.rebuild_all_channels()
    logger.info(f"Rebuilt {len(rebuild_counts)} channel(s) into Redis")

    # 4. Pub/Sub subscriber
    pubsub_subscriber.set_registry(registry)
    await pubsub_subscriber.start()

    # 5. Presence service
    presence_service.set_registry(registry)
    await presence_service.start()

    # 6. Kafka producer + background poll thread.
    #    start_poller() must be called inside an async context because
    #    it captures the running event loop via get_running_loop().
    kafka_producer.start()
    kafka_producer.start_poller()

    # 7. Delivery consumer
    delivery_service.set_registry(registry)
    delivery_service.start()
    delivery_service.start_consumer_thread()

    # 8. Persistence consumer
    persistence_service.start()
    persistence_service.start_consumer_thread()

    # 9. MinIO storage
    await storage_service.initialize()

    # 10. APScheduler reconciliation job.
    #     Started last because the first run fires immediately
    #     (next_run_time=now in create_scheduler) and needs both
    #     Kafka (step 6) and DB (step 2) to be live.
    scheduler = create_scheduler()
    scheduler.start()
    logger.info("APScheduler reconciliation job started")

    logger.info("Server ready")
    yield

    # ── Shutdown (reverse order) ──────────────────────────────
    logger.info("Server shutting down")

    # 10. Stop scheduler first — prevents a reconciliation job from
    #     firing against a Kafka producer that's mid-shutdown.
    #     wait=False: don't block on any currently-running job;
    #     running coroutines complete naturally, we just stop
    #     scheduling new ones.
    scheduler.shutdown(wait=False)
    logger.info("APScheduler stopped")

    # 9-7. Consumers + Kafka
    persistence_service.shutdown()
    delivery_service.shutdown()
    kafka_producer.shutdown(timeout=10.0)

    # 5-4. Presence + pub/sub
    await presence_service.shutdown()
    await pubsub_subscriber.shutdown()

    # 2-1. DB + Redis
    await database.close()
    await redis_client.close()

    logger.info("Server shutdown complete")


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
app.include_router(upload.router)
app.include_router(documents.router)   # GET /documents/{document_id}/status
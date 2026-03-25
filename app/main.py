"""
Application entrypoint — wires routers together.

This file stays thin on purpose. All logic lives in
routers/ and core/. main.py is just the assembly point.
"""

import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI

from app.routers import health, channels, ws

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
logger = logging.getLogger("app")


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Server starting")
    yield
    logger.info("Server shutting down")


app = FastAPI(
    title="Messaging System — Connection Layer",
    version="0.1.0",
    lifespan=lifespan,
)

app.include_router(health.router)
app.include_router(channels.router)
app.include_router(ws.router)
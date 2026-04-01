"""
Redis client — shared async connection pool.

Why a module-level singleton?
  Every part of the app (registry, dedup, future presence,
  future pub/sub) needs Redis. Creating a connection per
  request is wasteful. A shared pool is the standard pattern.

Why redis.asyncio?
  Our server is asyncio-based. Blocking Redis calls would
  freeze the event loop. redis.asyncio uses non-blocking I/O
  so Redis calls behave like any other await.

Lifecycle:
  startup:  redis_client.initialize()  — creates the pool
  runtime:  redis_client.redis         — returns the connection
  shutdown: redis_client.close()       — drains the pool
"""

import logging
from typing import Optional
import redis.asyncio as aioredis

from app.config import settings

logger = logging.getLogger("redis.client")


class RedisClient:
    """Async Redis connection manager."""

    def __init__(self):
        self._redis: Optional[aioredis.Redis] = None

    async def initialize(self) -> None:
        """Create the connection pool. Call from lifespan startup."""
        self._redis = aioredis.from_url(
            settings.redis_url,
            encoding="utf-8",
            decode_responses=True,
            max_connections=20,
        )
        # Verify connectivity
        try:
            await self._redis.ping()
            logger.info(f"Redis connected: {settings.redis_url}")
        except Exception as e:
            logger.error(f"Redis connection failed: {e}")
            raise

    @property
    def redis(self) -> aioredis.Redis:
        """Get the Redis connection. Raises if not initialized."""
        if not self._redis:
            raise RuntimeError("Redis not initialized — call initialize() first")
        return self._redis

    async def close(self) -> None:
        """Drain connections. Call from lifespan shutdown."""
        if self._redis:
            await self._redis.close()
            logger.info("Redis connection closed")


# ── Module-level singleton ────────────────────────────────────
redis_client = RedisClient()
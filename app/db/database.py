"""
SQLAlchemy async engine + session factory.

Two layers here:
  1. Engine — manages the connection pool (like asyncpg.Pool)
  2. SessionFactory — creates sessions for transactional work

Why SQLAlchemy async instead of raw asyncpg?
  We need SQLAlchemy models for Alembic migrations anyway.
  Using SQLAlchemy's async session means our queries use the
  same models, and we get transaction management for free.
  Under the hood, it still uses asyncpg as the driver.

The engine URL uses "postgresql+asyncpg://" which tells
SQLAlchemy to use asyncpg as the underlying connection driver.
"""

import logging
from typing import Optional

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.config import settings

logger = logging.getLogger("database")


class Database:
    """Async SQLAlchemy engine + session manager."""

    def __init__(self):
        self._engine: Optional[AsyncEngine] = None
        self._session_factory: Optional[async_sessionmaker[AsyncSession]] = None

    async def initialize(self) -> None:
        """Create engine and session factory. Call from lifespan."""
        # Convert standard postgres URL to async format.
        # settings.database_url = "postgresql://user:pass@host/db"
        # SQLAlchemy async needs "postgresql+asyncpg://..."
        db_url = settings.database_url
        if db_url.startswith("postgresql://"):
            db_url = db_url.replace(
                "postgresql://", "postgresql+asyncpg://", 1
            )

        self._engine = create_async_engine(
            db_url,
            pool_size=10,
            max_overflow=10,
            pool_timeout=30,
            pool_recycle=3600,
            echo=False,  # set True to log all SQL
        )

        self._session_factory = async_sessionmaker(
            bind=self._engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )

        # Verify connectivity
        async with self._engine.begin() as conn:
            result = await conn.execute(
                __import__("sqlalchemy").text("SELECT version()")
            )
            version = result.scalar()
            logger.info(f"PostgreSQL connected: {version[:60]}...")

    @property
    def engine(self) -> AsyncEngine:
        if not self._engine:
            raise RuntimeError("Database not initialized")
        return self._engine

    @property
    def session_factory(self) -> async_sessionmaker[AsyncSession]:
        if not self._session_factory:
            raise RuntimeError("Database not initialized")
        return self._session_factory

    def get_session(self) -> AsyncSession:
        """Create a new async session."""
        return self.session_factory()

    async def close(self) -> None:
        if self._engine:
            await self._engine.dispose()
            logger.info("PostgreSQL engine disposed")


# ── Module singleton ──────────────────────────────────────────
database = Database()
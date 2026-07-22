"""
Rate limiting — bounds how much AI traffic a single user or channel can
trigger, checked immediately after trigger detection and before any
embedding/retrieval/NIM work happens (see [[project_ai_service_productionization]]
§1 — reject cheaply, before spending CPU or a billed NIM call).

Two independent limits:
    rate:user:{sender_id}       N requests / window   (default 5 / 60s)
    rate:channel:{channel_id}   M requests / window    (default 20 / 60s)

Fixed-window counter via Redis INCR + EXPIRE NX, not a token bucket —
token bucket's burst-smoothing isn't worth a Lua script here; this only
needs to stop abuse/cost blowups, not pace traffic precisely.

Redis-down fallback: an in-process fixed-window counter (LocalWindowCounter),
enforcing the SAME thresholds, takes over for whichever keys hit a Redis
error. This is a deliberate accuracy/availability trade-off, not a second
independent limit:
  - Single consumer process (today's deployment): equivalent protection to
    the Redis path, just process-local instead of shared.
  - Multiple consumer processes: each process enforces the limit
    independently against its own local counter, so the *effective* combined
    limit during a Redis outage is (N processes x configured limit) rather
    than the configured limit — a known, accepted degradation, not a bug.
    Restored to the exact shared limit as soon as Redis recovers.
This still beats failing fully open (no protection at all) and failing fully
closed (blocking the whole AI feature on a Redis blip) — it keeps the same
abuse/cost ceiling intact for the common single-process case, and degrades
gracefully rather than catastrophically otherwise.
"""

import logging
import time
from collections import defaultdict, deque

from ai_service.config import (
    AI_RATE_LIMIT_CHANNEL_MAX,
    AI_RATE_LIMIT_USER_MAX,
    AI_RATE_LIMIT_WINDOW_SECONDS,
)
from app.core.redis_client import redis_client

logger = logging.getLogger("ai_service.rag.rate_limiter")


class LocalWindowCounter:
    """In-process fixed-window counter, used only while Redis is unreachable.

    Not thread-safe by design — this consumer is single-threaded asyncio,
    so no lock is needed, unlike app/core/metrics.py's IngestMetrics which
    is shared across a multi-threaded server.
    """

    def __init__(self, window_seconds: int) -> None:
        self._window_seconds = window_seconds
        self._hits: dict[str, deque] = defaultdict(deque)

    def hit(self, key: str) -> int:
        """Record one hit for `key` and return the count within the window."""
        now = time.monotonic()
        cutoff = now - self._window_seconds
        dq = self._hits[key]
        dq.append(now)
        while dq and dq[0] < cutoff:
            dq.popleft()
        return len(dq)


class RateLimitService:
    """Redis-backed fixed-window rate limiting for AI trigger requests, with
    an in-process fallback counter for when Redis is unreachable."""

    def __init__(
        self,
        user_max: int = AI_RATE_LIMIT_USER_MAX,
        channel_max: int = AI_RATE_LIMIT_CHANNEL_MAX,
        window_seconds: int = AI_RATE_LIMIT_WINDOW_SECONDS,
    ) -> None:
        self._user_max = user_max
        self._channel_max = channel_max
        self._window_seconds = window_seconds
        self._local_counter = LocalWindowCounter(window_seconds)

    async def _under_limit(self, key: str, limit: int) -> bool:
        try:
            count = await redis_client.redis.incr(key)
            if count == 1:
                await redis_client.redis.expire(key, self._window_seconds)
            return count <= limit
        except Exception as e:
            local_count = self._local_counter.hit(key)
            logger.error(
                f"Rate limit check failed for key={key}, "
                f"falling back to local counter (count={local_count}): {e}"
            )
            return local_count <= limit

    async def check(self, *, sender_id: str, channel_id: str) -> "RateLimitResult":
        """
        Check both the per-user and per-channel limits for one trigger
        request. Both counters are incremented even if one is already over
        limit, so a user hammering one channel doesn't get a free pass on
        their per-user count just because the channel check ran first.
        """
        user_ok = await self._under_limit(f"rate:user:{sender_id}", self._user_max)
        channel_ok = await self._under_limit(f"rate:channel:{channel_id}", self._channel_max)

        if user_ok and channel_ok:
            return RateLimitResult(allowed=True)

        scope = "user" if not user_ok else "channel"
        return RateLimitResult(allowed=False, exceeded_scope=scope)


class RateLimitResult:
    def __init__(self, allowed: bool, exceeded_scope: str | None = None) -> None:
        self.allowed = allowed
        self.exceeded_scope = exceeded_scope


# ── Module-level singleton ────────────────────────────────────
rate_limit_service = RateLimitService()

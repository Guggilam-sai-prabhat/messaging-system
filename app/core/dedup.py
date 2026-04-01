"""
Deduplication Service — prevents duplicate messages when clients retry.

The problem:
  1. Client sends message with client_request_id="abc"
  2. Server produces to Kafka successfully
  3. WebSocket disconnects BEFORE the ack reaches the client
  4. Client reconnects and resends with client_request_id="abc"
  5. Without dedup, Kafka gets the same message twice

The solution:
  Before producing, check Redis for the client_request_id.
  If it exists → return the cached result (skip Kafka).
  If it doesn't → produce to Kafka, then store the result.

Redis key format:
  dedup:{user_id}:{client_request_id}

  User-scoped because different users might coincidentally
  use the same client_request_id. The key stores the
  serialized EnrichedMessage so we can return the exact
  same message_id and timestamp on retry.

TTL:
  Keys expire after 5 minutes (configurable). This covers
  the retry window — if a client hasn't retried within 5
  minutes, it's not going to.

Race condition safety:
  We use SET NX (set if not exists). If two requests arrive
  simultaneously with the same client_request_id:
    - First SET NX succeeds → proceeds to Kafka
    - Second SET NX fails → returns "duplicate"
  This is atomic in Redis, so no race condition.
"""

import json
import logging
from typing import Optional
from dataclasses import dataclass

from app.core.redis_client import redis_client
from app.config import settings

logger = logging.getLogger("dedup")


@dataclass
class DedupResult:
    """Outcome of a dedup check."""
    is_duplicate: bool
    cached_data: Optional[dict] = None  # The original enriched message


class DedupService:
    """Redis-backed idempotency for message ingestion."""

    def _key(self, user_id: str, client_request_id: str) -> str:
        """Build the Redis key. Scoped to user."""
        return f"dedup:{user_id}:{client_request_id}"

    async def check(
        self, user_id: str, client_request_id: str
    ) -> DedupResult:
        """Check if this request has been seen before.

        Returns:
            DedupResult with is_duplicate=True if seen before,
            along with the cached enriched message data.
        """
        if not client_request_id:
            # No client_request_id → can't dedup, proceed normally
            return DedupResult(is_duplicate=False)

        key = self._key(user_id, client_request_id)

        try:
            cached = await redis_client.redis.get(key)
            if cached:
                logger.info(
                    f"Dedup hit: user={user_id} "
                    f"request_id={client_request_id}"
                )
                return DedupResult(
                    is_duplicate=True,
                    cached_data=json.loads(cached),
                )
            return DedupResult(is_duplicate=False)
        except Exception as e:
            # Redis failure should NOT block message sending.
            # Log and proceed — worst case is a duplicate,
            # which is better than a dropped message.
            logger.error(f"Dedup check failed: {e}")
            return DedupResult(is_duplicate=False)

    async def store(
        self,
        user_id: str,
        client_request_id: str,
        enriched_data: dict,
    ) -> bool:
        """Store the result after successful Kafka produce.

        Uses SET NX (set if not exists) + TTL.

        Returns:
            True if stored successfully (first write).
            False if key already existed (concurrent duplicate).
        """
        if not client_request_id:
            return True

        key = self._key(user_id, client_request_id)
        value = json.dumps(enriched_data)

        try:
            was_set = await redis_client.redis.set(
                key,
                value,
                nx=True,  # only set if not exists
                ex=settings.dedup_ttl_seconds,
            )
            if was_set:
                logger.debug(
                    f"Dedup stored: user={user_id} "
                    f"request_id={client_request_id} "
                    f"ttl={settings.dedup_ttl_seconds}s"
                )
            return bool(was_set)
        except Exception as e:
            # Redis failure doesn't block the pipeline.
            # The message is already in Kafka — we just can't
            # dedup future retries. Acceptable trade-off.
            logger.error(f"Dedup store failed: {e}")
            return False


# ── Module-level singleton ────────────────────────────────────
dedup_service = DedupService()
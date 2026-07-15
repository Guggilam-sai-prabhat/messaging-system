"""
Reply dedup — prevents the AI from publishing two answers to the same
triggering message.

The gap this closes: ai_service/consumer.py's offset is committed after
publish_answer() runs, but a crash between a successful publish and the
commit (or any future redelivery of an already-answered message) has no
way to know "have I already replied to this one?" — see that module's
docstring, which flags this as a deliberate, unsolved trade-off.

Same Redis SET NX + TTL pattern as app/core/dedup.py, keyed on the
*triggering* message id (not a new random id, so redelivery of the same
trigger always maps to the same key):

    ai-reply:{triggering_message_id}

Fail-open: if Redis is unavailable, treat the message as not-yet-answered
and let it publish. A duplicate reply during a Redis outage is preferable
to the AI feature going silent — the same trade-off DedupService already
makes for ingest-side dedup.
"""

import logging

from ai_service.config import AI_REPLY_DEDUP_TTL_SECONDS
from app.core.redis_client import redis_client

logger = logging.getLogger("ai_service.rag.reply_dedup")


class ReplyDedupService:
    """Redis-backed idempotency for AI-generated replies."""

    def __init__(self, ttl_seconds: int = AI_REPLY_DEDUP_TTL_SECONDS) -> None:
        self._ttl_seconds = ttl_seconds

    def _key(self, triggering_message_id: str) -> str:
        return f"ai-reply:{triggering_message_id}"

    async def try_claim(self, triggering_message_id: str) -> bool:
        """
        Attempt to claim the right to answer `triggering_message_id`.

        Returns True if this call is the first to claim it (proceed with
        publish), False if it was already claimed (skip — already answered).

        On Redis failure, fails open (returns True) — see module docstring.
        """
        key = self._key(triggering_message_id)

        try:
            claimed = await redis_client.redis.set(
                key,
                "1",
                nx=True,
                ex=self._ttl_seconds,
            )
            if not claimed:
                logger.info(f"Reply dedup hit: messageId={triggering_message_id}")
            return bool(claimed)
        except Exception as e:
            logger.error(f"Reply dedup check failed, proceeding anyway: {e}")
            return True


# ── Module-level singleton ────────────────────────────────────
reply_dedup_service = ReplyDedupService()

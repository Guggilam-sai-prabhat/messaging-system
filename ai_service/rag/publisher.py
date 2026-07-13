"""
Publishes a generated RagAnswer back onto channel-messages.

Why the AI's answer goes back through Kafka instead of being written
directly to the DB or pushed straight over a WebSocket:

  1. Single source of truth for "what is a message in this channel".
     persistence_service and delivery_service both consume
     channel-messages and don't distinguish sender_id="ai-assistant"
     from any other sender — the AI's reply gets stored and delivered
     to connected clients through the exact same path a human message
     takes, with none of that logic duplicated here.

  2. Durability and ordering. Kafka gives the reply a persisted,
     replayable position on the channel's message stream (keyed by
     channelId, like every other producer in this codebase), so a
     crash between generation and delivery doesn't silently drop the
     answer — it's already durably queued before this call returns.

  3. Consistency with the ingest path. app/services/message_ingest.py
     is the only other writer to this topic; reusing the same
     KafkaProducerService singleton and topic means one delivery
     mechanism, one circuit breaker, one place that understands
     "channel-messages" — not a second bespoke write path for AI
     replies that could drift from the human one.

Bypassing Kafka (e.g. an AI-only DB insert or direct WebSocket push)
would create a second, inconsistent path for messages to enter a
channel — exactly the kind of split-brain this module intentionally
avoids.
"""

import asyncio
import logging
import time
import uuid

from app.core.kafka_producer import KafkaCircuitOpenError, KafkaProduceError, kafka_producer
from ai_service.config import AI_SENDER_ID
from ai_service.rag.generator import RagAnswer

logger = logging.getLogger("ai_service.rag.publisher")

# Transient Kafka errors (buffer briefly full, one delivery timeout) are
# worth a couple of quick retries; a fixed small count mirrors
# app/core/kafka_producer.py's own produce_document_event policy rather
# than introducing a different retry shape for this one call site.
MAX_PUBLISH_RETRIES = 3
RETRY_BACKOFF_BASE_S = 0.5  # 0.5s, 1.0s between attempts


class AnswerPublishError(Exception):
    """Raised when the AI's answer could not be published after all retries."""


def build_reply_payload(
    *,
    channel_id: str,
    reply_to_message_id: str,
    answer: RagAnswer,
) -> dict:
    """Build the channel-messages Kafka payload for an AI-generated reply.

    Same wire shape ordinary user messages use (see
    app/models/message.py EnrichedMessage.to_kafka_dict) so downstream
    consumers (persistence_service, delivery_service) need no special
    case for AI-authored messages — senderId is simply AI_SENDER_ID and
    replyToMessageId links it back to the triggering question.
    """
    return {
        "messageId": str(uuid.uuid4()),
        "channelId": channel_id,
        "senderId": AI_SENDER_ID,
        "content": answer.text,
        "timestamp": time.time(),
        "replyToMessageId": reply_to_message_id,
    }


async def publish_answer(
    *,
    channel_id: str,
    reply_to_message_id: str,
    answer: RagAnswer,
) -> dict:
    """
    Publish a generated answer onto channel-messages, retrying transient
    Kafka failures.

    Retry policy mirrors produce_document_event in app/core/kafka_producer.py:
      - Up to MAX_PUBLISH_RETRIES attempts, backoff 0.5s then 1.0s.
      - KafkaCircuitOpenError is NOT retried — the circuit only opens after
        5 consecutive delivery failures, meaning the broker has been down
        for a sustained period; retrying immediately just wastes time.
      - Any other KafkaProduceError (buffer full, single delivery timeout)
        IS retried since those are transient blips, not sustained outages.

    Raises AnswerPublishError if every attempt fails. The caller (the Kafka
    consumer that invoked RagGenerator.answer()) should log and drop rather
    than crash the consumer loop on a publish failure — the same posture
    RagGenerator.answer() already takes for LLM failures.
    """
    payload = build_reply_payload(
        channel_id=channel_id,
        reply_to_message_id=reply_to_message_id,
        answer=answer,
    )

    last_exc: Exception = KafkaProduceError("No attempts made")

    for attempt in range(MAX_PUBLISH_RETRIES):
        try:
            result = await kafka_producer.produce_message(payload)
            logger.info(
                f"channel_id={channel_id} AI reply published "
                f"messageId={payload['messageId']} "
                f"replyTo={reply_to_message_id} "
                f"offset={result.get('offset')}"
            )
            return result
        except KafkaCircuitOpenError as e:
            logger.warning(
                f"channel_id={channel_id} circuit open, not retrying "
                f"AI reply publish (attempt {attempt + 1}/{MAX_PUBLISH_RETRIES}): {e}"
            )
            raise AnswerPublishError(str(e)) from e
        except KafkaProduceError as e:
            last_exc = e
            if attempt < MAX_PUBLISH_RETRIES - 1:
                backoff = RETRY_BACKOFF_BASE_S * (attempt + 1)
                logger.warning(
                    f"channel_id={channel_id} AI reply publish failed "
                    f"(attempt {attempt + 1}/{MAX_PUBLISH_RETRIES}), "
                    f"retrying in {backoff}s: {e}"
                )
                await asyncio.sleep(backoff)

    logger.error(
        f"channel_id={channel_id} AI reply publish failed after "
        f"{MAX_PUBLISH_RETRIES} attempts, giving up: {last_exc}"
    )
    raise AnswerPublishError(str(last_exc)) from last_exc

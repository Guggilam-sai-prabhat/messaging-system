"""
Tests for ai_service/consumer.py's _handle_message — the glue between
pipeline.detect(), RagGenerator.answer(), and publish_answer(). Kafka,
the DB, and the LLM are all faked/bypassed; this only checks the
orchestration logic (when generation and publish get called, and how a
publish failure is handled).
"""

import json
from unittest.mock import AsyncMock

import pytest

from ai_service.consumer import AiServiceConsumer
from ai_service.rag.generator import RagAnswer
from ai_service.rag.publisher import AnswerPublishError
from ai_service.rag.rate_limiter import RateLimitResult


@pytest.fixture(autouse=True)
def allow_rate_limit(monkeypatch):
    """Default rate_limit_service.check() to "allowed" so existing tests
    don't need to know about rate limiting; tests that care override this."""
    monkeypatch.setattr(
        "ai_service.consumer.rate_limit_service.check",
        AsyncMock(return_value=RateLimitResult(allowed=True)),
    )


def make_kafka_msg(payload: dict):
    class FakeMsg:
        def error(self):
            return None

        def value(self):
            return json.dumps(payload).encode("utf-8")

    return FakeMsg()


def make_payload(sender_id="user-1", content="hello", message_id="msg-1", channel_id="chan-1"):
    return {
        "messageId": message_id,
        "correlationId": "corr-1",
        "channelId": channel_id,
        "senderId": sender_id,
        "content": content,
        "timestamp": 1234.0,
    }


def make_consumer_stub() -> AiServiceConsumer:
    """AiServiceConsumer with real __init__ skipped — only _generator is needed
    by _handle_message, and it's set directly to a mock."""
    consumer = AiServiceConsumer.__new__(AiServiceConsumer)
    consumer._generator = AsyncMock()
    return consumer


@pytest.mark.asyncio
async def test_handle_message_skips_non_trigger_content():
    consumer = make_consumer_stub()
    msg = make_kafka_msg(make_payload(content="just chatting"))

    await consumer._handle_message(msg)

    consumer._generator.answer.assert_not_called()


@pytest.mark.asyncio
async def test_handle_message_generates_and_publishes_on_trigger(monkeypatch):
    consumer = make_consumer_stub()
    consumer._generator.answer.return_value = RagAnswer(text="the answer", sources_used=[])

    publish_mock = AsyncMock(return_value={"offset": 1})
    monkeypatch.setattr("ai_service.consumer.publish_answer", publish_mock)
    monkeypatch.setattr(
        "ai_service.consumer.reply_dedup_service.try_claim", AsyncMock(return_value=True)
    )

    msg = make_kafka_msg(make_payload(content="/ask what is chapter 3 about?"))
    await consumer._handle_message(msg)

    consumer._generator.answer.assert_awaited_once_with("chan-1", "what is chapter 3 about?")
    publish_mock.assert_awaited_once()
    _, kwargs = publish_mock.await_args
    assert kwargs["channel_id"] == "chan-1"
    assert kwargs["reply_to_message_id"] == "msg-1"
    assert kwargs["answer"].text == "the answer"


@pytest.mark.asyncio
async def test_handle_message_swallows_publish_failure(monkeypatch):
    consumer = make_consumer_stub()
    consumer._generator.answer.return_value = RagAnswer(text="the answer", sources_used=[])

    publish_mock = AsyncMock(side_effect=AnswerPublishError("kafka down"))
    monkeypatch.setattr("ai_service.consumer.publish_answer", publish_mock)
    monkeypatch.setattr(
        "ai_service.consumer.reply_dedup_service.try_claim", AsyncMock(return_value=True)
    )

    msg = make_kafka_msg(make_payload(content="/ask anything?"))

    # Should not raise — a failed publish is logged, not propagated, so the
    # consumer's offset-commit in `finally` still runs undisturbed.
    await consumer._handle_message(msg)

    publish_mock.assert_awaited_once()


@pytest.mark.asyncio
async def test_handle_message_skips_publish_when_already_answered(monkeypatch):
    consumer = make_consumer_stub()
    consumer._generator.answer.return_value = RagAnswer(text="the answer", sources_used=[])

    publish_mock = AsyncMock(return_value={"offset": 1})
    monkeypatch.setattr("ai_service.consumer.publish_answer", publish_mock)
    monkeypatch.setattr(
        "ai_service.consumer.reply_dedup_service.try_claim", AsyncMock(return_value=False)
    )

    msg = make_kafka_msg(make_payload(content="/ask anything?"))
    await consumer._handle_message(msg)

    # Generation still runs (dedup is checked after generation), but a
    # duplicate publish must not happen.
    consumer._generator.answer.assert_awaited_once()
    publish_mock.assert_not_called()


@pytest.mark.asyncio
async def test_handle_message_skips_generation_when_rate_limited(monkeypatch):
    consumer = make_consumer_stub()
    monkeypatch.setattr(
        "ai_service.consumer.rate_limit_service.check",
        AsyncMock(return_value=RateLimitResult(allowed=False, exceeded_scope="user")),
    )
    publish_mock = AsyncMock()
    monkeypatch.setattr("ai_service.consumer.publish_answer", publish_mock)

    msg = make_kafka_msg(make_payload(content="/ask anything?"))
    await consumer._handle_message(msg)

    consumer._generator.answer.assert_not_called()
    publish_mock.assert_not_called()


@pytest.mark.asyncio
async def test_handle_message_checks_rate_limit_by_sender_and_channel(monkeypatch):
    consumer = make_consumer_stub()
    consumer._generator.answer.return_value = RagAnswer(text="the answer", sources_used=[])
    rate_check = AsyncMock(return_value=RateLimitResult(allowed=True))
    monkeypatch.setattr("ai_service.consumer.rate_limit_service.check", rate_check)
    monkeypatch.setattr("ai_service.consumer.publish_answer", AsyncMock(return_value={"offset": 1}))
    monkeypatch.setattr(
        "ai_service.consumer.reply_dedup_service.try_claim", AsyncMock(return_value=True)
    )

    msg = make_kafka_msg(make_payload(sender_id="user-9", channel_id="chan-9", content="/ask x?"))
    await consumer._handle_message(msg)

    rate_check.assert_awaited_once_with(sender_id="user-9", channel_id="chan-9")


@pytest.mark.asyncio
async def test_handle_message_skips_malformed_payload():
    consumer = make_consumer_stub()

    class FakeMsg:
        def error(self):
            return None

        def value(self):
            return b"not json"

    await consumer._handle_message(FakeMsg())

    consumer._generator.answer.assert_not_called()

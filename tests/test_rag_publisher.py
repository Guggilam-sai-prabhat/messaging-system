"""
Tests for ai_service/rag/publisher.py — publishing a generated RagAnswer
back onto channel-messages, with kafka_producer faked so no real broker
is touched.
"""

import pytest

from ai_service.config import AI_SENDER_ID
from ai_service.rag.generator import RagAnswer
from ai_service.rag.publisher import AnswerPublishError, publish_answer
from app.core.kafka_producer import KafkaCircuitOpenError, KafkaProduceError


class FakeKafkaProducer:
    def __init__(self, errors: list[Exception] | None = None):
        # One entry per produce_message() call before it succeeds;
        # exhausted entries mean the call succeeds.
        self._errors = list(errors or [])
        self.calls: list[dict] = []

    async def produce_message(self, payload: dict) -> dict:
        self.calls.append(payload)
        if self._errors:
            raise self._errors.pop(0)
        return {"topic": "channel-messages", "partition": 0, "offset": len(self.calls)}


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    import ai_service.rag.publisher as publisher_module

    async def instant_sleep(_seconds):
        return None

    monkeypatch.setattr(publisher_module.asyncio, "sleep", instant_sleep)


@pytest.mark.asyncio
async def test_publish_answer_sends_expected_payload(monkeypatch):
    fake_producer = FakeKafkaProducer()
    monkeypatch.setattr("ai_service.rag.publisher.kafka_producer", fake_producer)

    answer = RagAnswer(text="Chapter 3 explains HNSW indexing.", sources_used=[])
    await publish_answer(
        channel_id="chan-42",
        reply_to_message_id="msg-1",
        answer=answer,
    )

    assert len(fake_producer.calls) == 1
    payload = fake_producer.calls[0]
    assert payload["channelId"] == "chan-42"
    assert payload["senderId"] == AI_SENDER_ID
    assert payload["content"] == "Chapter 3 explains HNSW indexing."
    assert payload["replyToMessageId"] == "msg-1"
    assert "messageId" in payload
    assert "timestamp" in payload


@pytest.mark.asyncio
async def test_publish_answer_retries_transient_failures(monkeypatch):
    fake_producer = FakeKafkaProducer(errors=[KafkaProduceError("blip")])
    monkeypatch.setattr("ai_service.rag.publisher.kafka_producer", fake_producer)

    answer = RagAnswer(text="ok", sources_used=[])
    result = await publish_answer(channel_id="chan-1", reply_to_message_id="msg-1", answer=answer)

    assert len(fake_producer.calls) == 2
    assert result["offset"] == 2


@pytest.mark.asyncio
async def test_publish_answer_gives_up_after_max_retries(monkeypatch):
    fake_producer = FakeKafkaProducer(
        errors=[KafkaProduceError("down"), KafkaProduceError("down"), KafkaProduceError("down")]
    )
    monkeypatch.setattr("ai_service.rag.publisher.kafka_producer", fake_producer)

    answer = RagAnswer(text="ok", sources_used=[])
    with pytest.raises(AnswerPublishError):
        await publish_answer(channel_id="chan-1", reply_to_message_id="msg-1", answer=answer)

    assert len(fake_producer.calls) == 3


@pytest.mark.asyncio
async def test_publish_answer_does_not_retry_on_open_circuit(monkeypatch):
    fake_producer = FakeKafkaProducer(errors=[KafkaCircuitOpenError("circuit open")])
    monkeypatch.setattr("ai_service.rag.publisher.kafka_producer", fake_producer)

    answer = RagAnswer(text="ok", sources_used=[])
    with pytest.raises(AnswerPublishError):
        await publish_answer(channel_id="chan-1", reply_to_message_id="msg-1", answer=answer)

    assert len(fake_producer.calls) == 1

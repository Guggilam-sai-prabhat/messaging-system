"""
Tests for ai_service/rag/reply_dedup.py — Redis SET NX claim used to stop
the AI service from publishing two replies to the same triggering message.
"""

from unittest.mock import AsyncMock

import pytest

from ai_service.rag.reply_dedup import ReplyDedupService


def make_service_with_redis(monkeypatch, set_return=None, set_side_effect=None):
    service = ReplyDedupService(ttl_seconds=600)
    fake_redis = AsyncMock()
    fake_redis.set = AsyncMock(return_value=set_return, side_effect=set_side_effect)
    monkeypatch.setattr("ai_service.rag.reply_dedup.redis_client._redis", fake_redis)
    return service, fake_redis


@pytest.mark.asyncio
async def test_try_claim_succeeds_on_first_call(monkeypatch):
    service, fake_redis = make_service_with_redis(monkeypatch, set_return=True)

    claimed = await service.try_claim("msg-1")

    assert claimed is True
    fake_redis.set.assert_awaited_once_with("ai-reply:msg-1", "1", nx=True, ex=600)


@pytest.mark.asyncio
async def test_try_claim_fails_when_already_claimed(monkeypatch):
    service, fake_redis = make_service_with_redis(monkeypatch, set_return=False)

    claimed = await service.try_claim("msg-1")

    assert claimed is False


@pytest.mark.asyncio
async def test_try_claim_fails_open_on_redis_error(monkeypatch):
    service, fake_redis = make_service_with_redis(
        monkeypatch, set_side_effect=ConnectionError("redis down")
    )

    claimed = await service.try_claim("msg-1")

    assert claimed is True


@pytest.mark.asyncio
async def test_try_claim_keys_by_triggering_message_id(monkeypatch):
    service, fake_redis = make_service_with_redis(monkeypatch, set_return=True)

    await service.try_claim("abc-123")

    args, kwargs = fake_redis.set.await_args
    assert args[0] == "ai-reply:abc-123"

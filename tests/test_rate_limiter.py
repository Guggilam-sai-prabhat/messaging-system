"""
Tests for ai_service/rag/rate_limiter.py — Redis fixed-window rate limiting
on AI trigger requests, per sender and per channel.
"""

from unittest.mock import AsyncMock

import pytest

from ai_service.rag.rate_limiter import RateLimitService


def make_service(monkeypatch, user_max=5, channel_max=20, window_seconds=60, incr_side_effect=None):
    service = RateLimitService(user_max=user_max, channel_max=channel_max, window_seconds=window_seconds)
    fake_redis = AsyncMock()
    fake_redis.incr = AsyncMock(side_effect=incr_side_effect)
    fake_redis.expire = AsyncMock(return_value=True)
    monkeypatch.setattr("ai_service.rag.rate_limiter.redis_client._redis", fake_redis)
    return service, fake_redis


@pytest.mark.asyncio
async def test_check_allows_when_under_both_limits(monkeypatch):
    service, fake_redis = make_service(monkeypatch, incr_side_effect=[1, 1])

    result = await service.check(sender_id="user-1", channel_id="chan-1")

    assert result.allowed is True
    assert result.exceeded_scope is None


@pytest.mark.asyncio
async def test_check_denies_when_user_limit_exceeded(monkeypatch):
    service, fake_redis = make_service(monkeypatch, user_max=5, incr_side_effect=[6, 1])

    result = await service.check(sender_id="user-1", channel_id="chan-1")

    assert result.allowed is False
    assert result.exceeded_scope == "user"


@pytest.mark.asyncio
async def test_check_denies_when_channel_limit_exceeded(monkeypatch):
    service, fake_redis = make_service(monkeypatch, channel_max=20, incr_side_effect=[1, 21])

    result = await service.check(sender_id="user-1", channel_id="chan-1")

    assert result.allowed is False
    assert result.exceeded_scope == "channel"


@pytest.mark.asyncio
async def test_check_increments_both_counters_even_if_one_already_over(monkeypatch):
    service, fake_redis = make_service(monkeypatch, user_max=5, incr_side_effect=[6, 1])

    await service.check(sender_id="user-1", channel_id="chan-1")

    assert fake_redis.incr.await_count == 2
    calls = [c.args[0] for c in fake_redis.incr.await_args_list]
    assert calls == ["rate:user:user-1", "rate:channel:chan-1"]


@pytest.mark.asyncio
async def test_check_sets_expiry_only_on_first_increment(monkeypatch):
    service, fake_redis = make_service(monkeypatch, incr_side_effect=[1, 2])

    await service.check(sender_id="user-1", channel_id="chan-1")

    fake_redis.expire.assert_awaited_once_with("rate:user:user-1", 60)


@pytest.mark.asyncio
async def test_check_falls_back_to_local_counter_under_limit_on_redis_error(monkeypatch):
    service, fake_redis = make_service(
        monkeypatch, user_max=5, channel_max=20, incr_side_effect=ConnectionError("redis down")
    )

    result = await service.check(sender_id="user-1", channel_id="chan-1")

    # First hit for each key locally -> count=1, well under either limit.
    assert result.allowed is True


@pytest.mark.asyncio
async def test_check_local_fallback_denies_once_local_count_exceeds_limit(monkeypatch):
    service, fake_redis = make_service(
        monkeypatch, user_max=2, channel_max=20, incr_side_effect=ConnectionError("redis down")
    )

    # Redis stays down for 3 consecutive requests from the same user; the
    # local fallback counter should track and enforce user_max=2 on its own.
    await service.check(sender_id="user-1", channel_id="chan-1")
    await service.check(sender_id="user-1", channel_id="chan-1")
    result = await service.check(sender_id="user-1", channel_id="chan-1")

    assert result.allowed is False
    assert result.exceeded_scope == "user"


@pytest.mark.asyncio
async def test_check_local_fallback_is_scoped_per_key(monkeypatch):
    service, fake_redis = make_service(
        monkeypatch, user_max=2, channel_max=20, incr_side_effect=ConnectionError("redis down")
    )

    await service.check(sender_id="user-1", channel_id="chan-1")
    await service.check(sender_id="user-1", channel_id="chan-1")
    # A different user should have its own local counter, unaffected by
    # user-1 already being over its local limit.
    result = await service.check(sender_id="user-2", channel_id="chan-1")

    assert result.allowed is True


@pytest.mark.asyncio
async def test_check_recovers_to_redis_once_available_again(monkeypatch):
    service, fake_redis = make_service(monkeypatch, user_max=5, channel_max=20)
    fake_redis.incr = AsyncMock(side_effect=[ConnectionError("redis down"), 1, 1])

    first = await service.check(sender_id="user-1", channel_id="chan-1")
    second = await service.check(sender_id="user-1", channel_id="chan-1")

    assert first.allowed is True  # local fallback, count=1
    assert second.allowed is True  # redis back up, count=1 (independent counter)

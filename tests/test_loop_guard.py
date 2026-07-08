"""Tests for ai_service/detection/loop_guard.py"""

from ai_service.config import AI_SENDER_ID
from ai_service.detection.loop_guard import (
    is_from_system_sender,
    is_own_message,
    should_ignore,
)
from ai_service.models.events import ChatMessageEvent


def make_event(sender_id: str, content: str = "/ask hello") -> ChatMessageEvent:
    return ChatMessageEvent(
        message_id="m1",
        correlation_id="c1",
        channel_id="chan-1",
        sender_id=sender_id,
        content=content,
        timestamp=0.0,
    )


def test_is_own_message_true_for_ai_sender():
    assert is_own_message(make_event(AI_SENDER_ID))


def test_is_own_message_false_for_regular_user():
    assert not is_own_message(make_event("user-1"))


def test_should_ignore_ai_sender_even_with_trigger_content():
    # The AI's own message could itself contain "/ask"-shaped text
    # (e.g. echoing what it answered). It must still be ignored.
    event = make_event(AI_SENDER_ID, content="/ask what did you mean?")
    assert should_ignore(event)


def test_should_ignore_system_sender(monkeypatch):
    import ai_service.config as config
    import ai_service.detection.loop_guard as loop_guard

    monkeypatch.setattr(config, "SYSTEM_SENDER_IDS", frozenset({"system-bot"}))
    monkeypatch.setattr(loop_guard, "SYSTEM_SENDER_IDS", frozenset({"system-bot"}))

    assert is_from_system_sender(make_event("system-bot"))
    assert should_ignore(make_event("system-bot"))


def test_does_not_ignore_regular_user_request():
    assert not should_ignore(make_event("user-1"))

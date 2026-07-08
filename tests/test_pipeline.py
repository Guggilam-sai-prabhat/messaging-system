"""Tests for ai_service/pipeline.py — end-to-end trigger detection pipeline."""

import pytest

from ai_service.config import AI_SENDER_ID
from ai_service.pipeline import MessageParseError, TriggerResult, detect, parse_event, process


def make_payload(**overrides) -> dict:
    payload = {
        "messageId": "m1",
        "correlationId": "c1",
        "channelId": "chan-1",
        "senderId": "user-1",
        "content": "/ask what is HNSW?",
        "timestamp": 0.0,
    }
    payload.update(overrides)
    return payload


# ── parse_event: malformed messages ───────────────────────────────────────


def test_parse_event_accepts_well_formed_payload():
    event = parse_event(make_payload())
    assert event.sender_id == "user-1"
    assert event.content == "/ask what is HNSW?"


@pytest.mark.parametrize(
    "missing_field",
    ["messageId", "correlationId", "channelId", "senderId", "content", "timestamp"],
)
def test_parse_event_rejects_missing_field(missing_field):
    payload = make_payload()
    del payload[missing_field]
    with pytest.raises(MessageParseError):
        parse_event(payload)


def test_parse_event_rejects_empty_content():
    with pytest.raises(MessageParseError):
        parse_event(make_payload(content=""))


def test_parse_event_rejects_whitespace_only_content():
    with pytest.raises(MessageParseError):
        parse_event(make_payload(content="   "))


def test_parse_event_rejects_non_string_content():
    with pytest.raises(MessageParseError):
        parse_event(make_payload(content=12345))


def test_parse_event_rejects_non_dict_payload():
    with pytest.raises(MessageParseError):
        parse_event(["not", "a", "dict"])


# ── process: normal chat / AI / system messages ignored ──────────────────


def test_process_ignores_normal_chat_message():
    result = process(make_payload(content="just a normal chat message"))
    assert result == TriggerResult(should_respond=False, reason="no_trigger_match")


def test_process_ignores_ai_own_message_even_with_trigger_text():
    result = process(make_payload(senderId=AI_SENDER_ID, content="/ask does this loop?"))
    assert result.should_respond is False
    assert result.reason == "ignored_sender"


# ── process: the golden path ──────────────────────────────────────────────


def test_process_detects_valid_ask_request():
    result = process(make_payload(content="/ask What is the summary?"))
    assert result.should_respond is True
    assert result.query == "What is the summary?"


# ── edge cases: empty command, bare prefix, malformed ─────────────────────


def test_process_rejects_bare_ask_no_query():
    result = process(make_payload(content="/ask"))
    assert result.should_respond is False
    assert result.reason == "no_trigger_match"


def test_process_rejects_ask_with_only_whitespace_query():
    result = process(make_payload(content="/ask    "))
    assert result.should_respond is False
    assert result.reason == "no_trigger_match"


def test_process_rejects_prefix_not_at_message_start():
    result = process(make_payload(content="hey /ask summarize this"))
    assert result.should_respond is False


def test_detect_short_circuits_loop_guard_before_trigger_matching():
    # Loop guard must run before content is inspected at all, so it wins
    # even when the content wouldn't match the trigger either.
    event = parse_event(make_payload(senderId=AI_SENDER_ID, content="not a trigger"))
    result = detect(event)
    assert result.reason == "ignored_sender"

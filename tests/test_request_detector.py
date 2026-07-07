"""
Tests for ai_service/detection/request_detector.py

Command-prefix convention under test:
- prefix is "/ask", case-insensitive
- must start the message (after trimming leading/trailing whitespace)
- requires at least one non-whitespace character of query text after the
  prefix; a bare "/ask" (no query) is NOT an AI request
"""

import pytest

from ai_service.detection.request_detector import extract_query, is_ai_request
from ai_service.models.events import ChatMessageEvent


def make_event(content: str) -> ChatMessageEvent:
    return ChatMessageEvent(
        message_id="m1",
        correlation_id="c1",
        channel_id="chan-1",
        sender_id="user-1",
        content=content,
        timestamp=0.0,
    )


# ── is_ai_request ────────────────────────────────────────────────────────────


def test_matches_basic_prefix():
    assert is_ai_request(make_event("/ask summarize the Q3 doc"))


def test_matches_case_insensitive():
    assert is_ai_request(make_event("/ASK summarize the Q3 doc"))
    assert is_ai_request(make_event("/Ask summarize the Q3 doc"))


def test_matches_with_leading_trailing_whitespace_on_message():
    assert is_ai_request(make_event("   /ask what is HNSW?  "))


def test_rejects_prefix_not_at_start():
    assert not is_ai_request(make_event("hey /ask summarize this"))


def test_rejects_bare_prefix_no_query():
    assert not is_ai_request(make_event("/ask"))


def test_rejects_bare_prefix_with_trailing_whitespace_only():
    assert not is_ai_request(make_event("/ask   "))


def test_rejects_unrelated_message():
    assert not is_ai_request(make_event("just a normal chat message"))


def test_rejects_similar_but_wrong_prefix():
    assert not is_ai_request(make_event("/asking something"))
    assert not is_ai_request(make_event("/ai summarize this"))


def test_matches_multiline_query():
    assert is_ai_request(make_event("/ask summarize\nthis whole thread"))


# ── extract_query ─────────────────────────────────────────────────────────────


def test_extract_query_strips_prefix_and_whitespace():
    event = make_event("  /ask   what is HNSW?  ")
    assert extract_query(event) == "what is HNSW?"


def test_extract_query_preserves_internal_content():
    event = make_event("/ask summarize the Q3 doc in 3 bullets")
    assert extract_query(event) == "summarize the Q3 doc in 3 bullets"


def test_extract_query_raises_on_non_request():
    with pytest.raises(ValueError):
        extract_query(make_event("not an ai request"))

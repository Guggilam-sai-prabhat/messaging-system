"""Tests for ai_service/detection/triggers.py — pluggable trigger strategies."""

import pytest

from ai_service.detection.triggers import (
    CompositeTrigger,
    MentionTrigger,
    PrefixCommandTrigger,
)


# ── PrefixCommandTrigger ─────────────────────────────────────────────────


def test_prefix_trigger_matches_basic():
    trigger = PrefixCommandTrigger("/ask")
    assert trigger.matches("/ask what is HNSW?")
    assert trigger.extract_query("/ask what is HNSW?") == "what is HNSW?"


def test_prefix_trigger_rejects_bare_command():
    trigger = PrefixCommandTrigger("/ask")
    assert not trigger.matches("/ask")
    assert not trigger.matches("/ask   ")


def test_prefix_trigger_requires_leading_slash_prefix():
    with pytest.raises(ValueError):
        PrefixCommandTrigger("ask")


def test_prefix_trigger_extract_raises_on_non_match():
    trigger = PrefixCommandTrigger("/ask")
    with pytest.raises(ValueError):
        trigger.extract_query("not a request")


def test_prefix_trigger_is_configurable_for_different_commands():
    trigger = PrefixCommandTrigger("/summarize")
    assert trigger.matches("/summarize this thread")
    assert not trigger.matches("/ask this thread")


# ── MentionTrigger (example of a swappable second trigger) ───────────────


def test_mention_trigger_matches():
    trigger = MentionTrigger("@askai")
    assert trigger.matches("@askai what's the status?")
    assert trigger.extract_query("@askai what's the status?") == "what's the status?"


def test_mention_trigger_rejects_bare_mention():
    trigger = MentionTrigger("@askai")
    assert not trigger.matches("@askai")
    assert not trigger.matches("hey @askai")


def test_mention_trigger_matches_mid_message():
    trigger = MentionTrigger("@askai")
    assert trigger.matches("hey @askai summarize this")


# ── CompositeTrigger ──────────────────────────────────────────────────────


def test_composite_trigger_matches_any_strategy():
    trigger = CompositeTrigger([PrefixCommandTrigger("/ask"), MentionTrigger("@askai")])
    assert trigger.matches("/ask hello")
    assert trigger.matches("@askai hello")
    assert not trigger.matches("just chatting")


def test_composite_trigger_requires_at_least_one_strategy():
    with pytest.raises(ValueError):
        CompositeTrigger([])

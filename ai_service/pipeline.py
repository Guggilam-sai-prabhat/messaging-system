"""
AI trigger pipeline — the single entry point a Kafka consumer calls per
message to decide "should the AI respond, and to what?"

Stages, in order:
  1. message parsing    — raw Kafka payload (dict) -> ChatMessageEvent
  2. validation          — required fields present, non-empty content
  3. loop guard          — reject the AI's own messages / system senders
                            BEFORE content is ever inspected for a trigger
  4. command extraction  — delegate to the active TriggerStrategy

Steps 3 and 4 are intentionally ordered this way: loop prevention must not
depend on trigger content-matching, or a reply that happens to contain
trigger-shaped text could re-trigger itself (see loop_guard.py docstring).

This module is the only place that wires a concrete TriggerStrategy in.
Swap DEFAULT_TRIGGER (or pass a different `trigger` to detect()) to change
the trigger mechanism without touching parsing, validation, or the guard.
"""

from dataclasses import dataclass

from ai_service.detection.loop_guard import should_ignore
from ai_service.detection.request_detector import DEFAULT_TRIGGER
from ai_service.detection.triggers import TriggerStrategy
from ai_service.models.events import ChatMessageEvent


class MessageParseError(Exception):
    """Raised when a raw Kafka payload can't be parsed into a ChatMessageEvent."""


REQUIRED_FIELDS = ("messageId", "correlationId", "channelId", "senderId", "content", "timestamp")


def parse_event(payload: dict) -> ChatMessageEvent:
    """
    Stage 1+2: message parsing and field validation.

    Raises MessageParseError for malformed payloads — missing fields, wrong
    types, or blank content. Kept separate from ChatMessageEvent.from_kafka_dict
    (which assumes a well-formed payload) so the pipeline can distinguish
    "malformed Kafka message" from "well-formed message that isn't a request".
    """
    if not isinstance(payload, dict):
        raise MessageParseError(f"Payload must be a dict, got {type(payload).__name__}")

    missing = [f for f in REQUIRED_FIELDS if f not in payload]
    if missing:
        raise MessageParseError(f"Payload missing fields: {missing}")

    content = payload["content"]
    if not isinstance(content, str) or not content.strip():
        raise MessageParseError("Payload content must be a non-empty string")

    try:
        return ChatMessageEvent.from_kafka_dict(payload)
    except (KeyError, TypeError) as e:
        raise MessageParseError(f"Malformed payload: {e}") from e


@dataclass(frozen=True)
class TriggerResult:
    """Outcome of running the pipeline on one event."""
    should_respond: bool
    query: str | None = None
    reason: str | None = None  # why should_respond is False; None when True


def detect(event: ChatMessageEvent, trigger: TriggerStrategy = DEFAULT_TRIGGER) -> TriggerResult:
    """
    Stages 3+4: loop guard, then trigger matching and extraction.

    Pass `event` already parsed via parse_event(). Ignores normal chat,
    the AI's own messages, and system-sender messages by construction —
    each is a `should_respond=False` result with a `reason`, not an
    exception, since "not a request" is the expected outcome for most
    traffic.
    """
    if should_ignore(event):
        return TriggerResult(should_respond=False, reason="ignored_sender")

    if not trigger.matches(event.content):
        return TriggerResult(should_respond=False, reason="no_trigger_match")

    query = trigger.extract_query(event.content)
    return TriggerResult(should_respond=True, query=query)


def process(payload: dict, trigger: TriggerStrategy = DEFAULT_TRIGGER) -> TriggerResult:
    """
    Full pipeline: parse -> validate -> loop guard -> trigger extraction.

    Raises MessageParseError on malformed payloads — callers (e.g. the
    Kafka consumer) should catch this the same way document_worker.py
    treats a ValueError from _parse_event: log, skip, commit the offset.
    """
    event = parse_event(payload)
    return detect(event, trigger=trigger)

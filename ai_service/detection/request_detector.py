"""
Detects whether a chat message is an AI request.

This module is a thin facade over the default TriggerStrategy (see
triggers.py): today that's PrefixCommandTrigger("/ask"), case-insensitive,
requiring at least one non-whitespace character of query text after the
prefix. "/ask" or "/ask   " (no query) is NOT a request — bare-prefix
messages are left alone rather than triggering a usage-hint response, so
the detector stays a pure filter with no response-generation side effects.

Changing the trigger mechanism later (e.g. to "@askai") means swapping
DEFAULT_TRIGGER, not rewriting is_ai_request/extract_query or any caller.
"""

from ai_service.detection.triggers import PrefixCommandTrigger, TriggerStrategy
from ai_service.models.events import ChatMessageEvent

DEFAULT_TRIGGER: TriggerStrategy = PrefixCommandTrigger("/ask")


def is_ai_request(event: ChatMessageEvent) -> bool:
    return DEFAULT_TRIGGER.matches(event.content)


def extract_query(event: ChatMessageEvent) -> str:
    """
    Return the query text following the command prefix.

    Raises ValueError if `event` is not an AI request — call is_ai_request
    first. Kept as a separate function (rather than returning Optional from
    one call) so callers on the hot path only pay for the strip+match once
    via is_ai_request, and extract_query is a plain, non-Optional accessor.
    """
    return DEFAULT_TRIGGER.extract_query(event.content)

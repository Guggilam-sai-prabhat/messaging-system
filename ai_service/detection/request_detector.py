"""
Detects whether a chat message is an AI request.

Convention: a message is an AI request iff, after stripping leading/trailing
whitespace, it starts with the command prefix "/ask" (case-insensitive)
followed by at least one non-whitespace character of query text.

"/ask" or "/ask   " (no query text) is NOT a request — bare-prefix messages
are left alone rather than triggering a usage-hint response, so the detector
stays a pure filter with no response-generation side effects.
"""

import re

from ai_service.models.events import ChatMessageEvent

_PREFIX_RE = re.compile(r"^/ask\s+(\S.*)$", re.IGNORECASE | re.DOTALL)


def is_ai_request(event: ChatMessageEvent) -> bool:
    return _PREFIX_RE.match(event.content.strip()) is not None


def extract_query(event: ChatMessageEvent) -> str:
    """
    Return the query text following the command prefix.

    Raises ValueError if `event` is not an AI request — call is_ai_request
    first. Kept as a separate function (rather than returning Optional from
    one call) so callers on the hot path only pay for the strip+match once
    via is_ai_request, and extract_query is a plain, non-Optional accessor.
    """
    match = _PREFIX_RE.match(event.content.strip())
    if match is None:
        raise ValueError(f"Not an AI request: {event.content!r}")
    return match.group(1).strip()

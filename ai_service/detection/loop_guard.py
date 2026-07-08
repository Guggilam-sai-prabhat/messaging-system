"""
Loop prevention — stops the AI from ever reacting to its own output or to
other non-human traffic.

Why this has to live upstream of trigger matching: a trigger strategy only
answers "does this text look like a request?" It has no notion of *who*
sent the message. If the AI's own reply happened to contain a string that
matches a future trigger (e.g. it echoes the user's "/ask ..." while
explaining what it did), pure content-matching would fire again on its own
output. Loop prevention filters on sender identity instead, before content
is ever inspected, so it can't be fooled by response phrasing.
"""

from ai_service.config import AI_SENDER_ID, SYSTEM_SENDER_IDS
from ai_service.models.events import ChatMessageEvent


def is_own_message(event: ChatMessageEvent) -> bool:
    """True if this event was produced by the AI service itself."""
    return event.sender_id == AI_SENDER_ID


def is_from_system_sender(event: ChatMessageEvent) -> bool:
    """True if this event came from a reserved system/automation account."""
    return event.sender_id in SYSTEM_SENDER_IDS


def should_ignore(event: ChatMessageEvent) -> bool:
    """
    True if this event must never be considered for AI triggering,
    regardless of content — the AI's own messages and system messages.
    """
    return is_own_message(event) or is_from_system_sender(event)

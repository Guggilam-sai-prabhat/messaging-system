"""
Trigger strategies — the pluggable "how does a user ask for AI" layer.

Everything downstream (loop guard, pipeline, worker) depends only on the
TriggerStrategy protocol below, never on "/ask" specifically. Swapping the
trigger mechanism later (e.g. "@askai", or a UI button that sends a
structured event) means writing one new strategy class and changing the
single wiring point in pipeline.py — no changes to validation, loop
prevention, or the Kafka consumer.

To add a new trigger:
  1. Implement TriggerStrategy (matches + extract_query).
  2. Point pipeline.DEFAULT_TRIGGER at it (or compose several via
     CompositeTrigger to support more than one at once).
"""

from typing import Protocol
import re


class TriggerStrategy(Protocol):
    """Recognizes an AI request in raw message content and extracts the query."""

    def matches(self, content: str) -> bool:
        """Return True if `content` should invoke the AI."""
        ...

    def extract_query(self, content: str) -> str:
        """
        Return the query text implied by `content`.

        Raises ValueError if `content` does not match — callers should
        check matches() first.
        """
        ...


class PrefixCommandTrigger:
    """
    Matches a leading slash-command prefix (e.g. "/ask") followed by at
    least one non-whitespace character of query text. Case-insensitive;
    prefix must start the message after trimming surrounding whitespace.
    """

    def __init__(self, prefix: str):
        if not prefix.startswith("/"):
            raise ValueError(f"Prefix must start with '/': {prefix!r}")
        self._prefix = prefix
        self._pattern = re.compile(
            rf"^{re.escape(prefix)}\s+(\S.*)$", re.IGNORECASE | re.DOTALL
        )

    def matches(self, content: str) -> bool:
        return self._pattern.match(content.strip()) is not None

    def extract_query(self, content: str) -> str:
        match = self._pattern.match(content.strip())
        if match is None:
            raise ValueError(f"Not an AI request: {content!r}")
        return match.group(1).strip()


class MentionTrigger:
    """
    Matches an "@handle" mention anywhere in the message. Provided as a
    ready-made example of a second trigger mechanism (e.g. "@askai") that
    can be swapped in without touching validation or loop-prevention code.
    """

    def __init__(self, handle: str):
        self._handle = handle
        self._pattern = re.compile(
            rf"(?:^|\s){re.escape(handle)}\b[ \t]*(.*)", re.IGNORECASE | re.DOTALL
        )

    def matches(self, content: str) -> bool:
        match = self._pattern.search(content.strip())
        return match is not None and match.group(1).strip() != ""

    def extract_query(self, content: str) -> str:
        match = self._pattern.search(content.strip())
        if match is None or not match.group(1).strip():
            raise ValueError(f"Not an AI request: {content!r}")
        return match.group(1).strip()


class CompositeTrigger:
    """Matches if any of several strategies match; uses the first that matches."""

    def __init__(self, strategies: list[TriggerStrategy]):
        if not strategies:
            raise ValueError("CompositeTrigger requires at least one strategy")
        self._strategies = strategies

    def matches(self, content: str) -> bool:
        return any(s.matches(content) for s in self._strategies)

    def extract_query(self, content: str) -> str:
        for strategy in self._strategies:
            if strategy.matches(content):
                return strategy.extract_query(content)
        raise ValueError(f"Not an AI request: {content!r}")

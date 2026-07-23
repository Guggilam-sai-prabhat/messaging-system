"""
Per-partition offset low-watermark tracking for the concurrent consumer loop
in ai_service/consumer.py — see docs/ai_service_productionization.md §2.

Why this exists: once messages are handed to a worker pool, completion order
no longer matches poll order (worker A can finish offset 5 while worker B is
still processing offset 3). Committing whatever offset just finished would
risk committing offset 5 while offset 3 is still in flight — a crash at that
point skips offset 3's message entirely on redelivery.

This tracker instead only ever commits the highest *contiguous* offset that
has finished, per partition, so a still-in-flight lower offset always blocks
the commit from moving past it.
"""

import heapq
from dataclasses import dataclass, field


@dataclass
class _PartitionState:
    next_expected: int | None = None
    completed: list[int] = field(default_factory=list)  # min-heap of finished offsets


class OffsetWatermarkTracker:
    """Tracks in-flight and completed offsets per Kafka partition and
    reports the safe-to-commit watermark as messages finish out of order."""

    def __init__(self) -> None:
        self._partitions: dict[int, _PartitionState] = {}

    def track(self, partition: int, offset: int) -> None:
        """Register `offset` as dispatched (about to be processed) on `partition`."""
        state = self._partitions.setdefault(partition, _PartitionState())
        if state.next_expected is None:
            state.next_expected = offset

    def complete(self, partition: int, offset: int) -> int | None:
        """
        Mark `offset` as finished processing on `partition`.

        Returns the new safe-to-commit offset (the next offset Kafka should
        resume from) if the contiguous watermark advanced, or None if it
        didn't move (a lower offset on this partition is still in flight).
        """
        state = self._partitions[partition]
        heapq.heappush(state.completed, offset)

        advanced = False
        while state.completed and state.completed[0] == state.next_expected:
            heapq.heappop(state.completed)
            state.next_expected += 1
            advanced = True

        return state.next_expected if advanced else None

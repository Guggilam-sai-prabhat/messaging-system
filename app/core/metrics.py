"""
Lightweight in-process metrics for the ingestion layer.

Why not Prometheus/StatsD from day one?
  - We need the NUMBERS first to know what's normal.
  - This module collects the same four things Prometheus would,
    but stores them in memory with a rolling window.
  - When you add Prometheus later, you replace the internals
    of this module — all callsites stay the same.

What we track:
  1. messages_received  — entered the pipeline (before validation)
  2. messages_produced  — confirmed delivered to Kafka
  3. messages_failed    — failed at any stage (validation or Kafka)
  4. produce_latency_ms — time from produce() call to delivery callback

The /metrics endpoint exposes a snapshot for dashboards and alerts.
"""

import time
import threading
from collections import deque
from dataclasses import dataclass, field


@dataclass
class LatencyStats:
    """Summary stats for a collection of latency samples."""
    count: int = 0
    avg_ms: float = 0.0
    p50_ms: float = 0.0
    p95_ms: float = 0.0
    p99_ms: float = 0.0
    max_ms: float = 0.0


class IngestMetrics:
    """Thread-safe metrics collector with a rolling window.

    The rolling window (default 60s) means stats reflect
    RECENT behavior, not all-time averages. This is what
    you want for alerting — "failure rate in the last minute"
    is actionable, "failure rate since boot" is not.
    """

    def __init__(self, window_seconds: float = 60.0):
        self._window = window_seconds
        self._lock = threading.Lock()

        # Each entry is a timestamp — we count entries in the window.
        self._received: deque[float] = deque()
        self._produced: deque[float] = deque()
        self._failed: deque[float] = deque()

        # Each entry is (timestamp, latency_ms).
        self._latencies: deque[tuple[float, float]] = deque()

    # ── Recording ─────────────────────────────────────────────

    def record_received(self) -> None:
        now = time.monotonic()
        with self._lock:
            self._received.append(now)

    def record_produced(self, latency_ms: float) -> None:
        now = time.monotonic()
        with self._lock:
            self._produced.append(now)
            self._latencies.append((now, latency_ms))

    def record_failed(self) -> None:
        now = time.monotonic()
        with self._lock:
            self._failed.append(now)

    # ── Querying ──────────────────────────────────────────────

    def snapshot(self) -> dict:
        """Point-in-time metrics for the rolling window."""
        now = time.monotonic()
        cutoff = now - self._window

        with self._lock:
            self._evict(cutoff)

            received = len(self._received)
            produced = len(self._produced)
            failed = len(self._failed)
            latency_values = [
                lat for (ts, lat) in self._latencies
            ]

        elapsed = min(self._window, now)  # don't divide by > uptime
        rate = received / self._window if self._window > 0 else 0
        failure_rate = (
            failed / received if received > 0 else 0.0
        )

        return {
            "window_seconds": self._window,
            "messages_received": received,
            "messages_produced": produced,
            "messages_failed": failed,
            "messages_per_second": round(rate, 2),
            "failure_rate": round(failure_rate, 4),
            "produce_latency": self._calc_latency(latency_values),
        }

    # ── Internals ─────────────────────────────────────────────

    def _evict(self, cutoff: float) -> None:
        """Remove entries older than cutoff. Must hold lock."""
        for dq in (self._received, self._produced, self._failed):
            while dq and dq[0] < cutoff:
                dq.popleft()
        while self._latencies and self._latencies[0][0] < cutoff:
            self._latencies.popleft()

    @staticmethod
    def _calc_latency(values: list[float]) -> dict:
        if not values:
            return LatencyStats().__dict__

        values.sort()
        n = len(values)
        return LatencyStats(
            count=n,
            avg_ms=round(sum(values) / n, 2),
            p50_ms=round(values[n // 2], 2),
            p95_ms=round(values[int(n * 0.95)], 2),
            p99_ms=round(values[int(n * 0.99)], 2),
            max_ms=round(values[-1], 2),
        ).__dict__


# ── Module-level singleton ────────────────────────────────────
ingest_metrics = IngestMetrics(window_seconds=60.0)
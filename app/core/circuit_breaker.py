"""
Circuit Breaker — fail fast when Kafka is consistently down.

Without this, every message waits 10s for a Kafka timeout when
the broker is dead. With 100 concurrent users, that's 100
coroutines blocked for 10s each — your WebSocket server becomes
unresponsive.

The circuit breaker pattern:

  CLOSED (normal) ──→ OPEN (failing fast) ──→ HALF_OPEN (probing)
                  N failures                    1 success
                  in a row                      → back to CLOSED
                                                1 failure
                                                → back to OPEN

States:
  CLOSED:    everything flows through normally
  OPEN:      all calls rejected immediately (no waiting)
  HALF_OPEN: allow ONE call through to test if Kafka is back

This is simpler than time-windowed approaches and works well
for a single downstream dependency like Kafka.
"""

import time
import logging
from enum import Enum

logger = logging.getLogger("circuit_breaker")


class CircuitState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    """Consecutive-failure circuit breaker.

    Args:
        failure_threshold: failures in a row before opening
        recovery_timeout: seconds to wait before trying HALF_OPEN
        name: for logging
    """

    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 30.0,
        name: str = "kafka",
    ):
        self._threshold = failure_threshold
        self._recovery_timeout = recovery_timeout
        self._name = name

        self._state = CircuitState.CLOSED
        self._consecutive_failures = 0
        self._last_failure_time: float = 0
        self._total_rejected: int = 0

    @property
    def state(self) -> CircuitState:
        """Current state, accounting for recovery timeout."""
        if (
            self._state == CircuitState.OPEN
            and time.monotonic() - self._last_failure_time
            >= self._recovery_timeout
        ):
            self._state = CircuitState.HALF_OPEN
            logger.info(
                f"Circuit '{self._name}' → HALF_OPEN (probing)"
            )
        return self._state

    def allow_request(self) -> bool:
        """Should we attempt the call?"""
        current = self.state

        if current == CircuitState.CLOSED:
            return True

        if current == CircuitState.HALF_OPEN:
            # Allow one probe request
            return True

        # OPEN — reject immediately
        self._total_rejected += 1
        return False

    def record_success(self) -> None:
        """Call succeeded — reset to CLOSED."""
        if self._state != CircuitState.CLOSED:
            logger.info(
                f"Circuit '{self._name}' → CLOSED (recovered)"
            )
        self._consecutive_failures = 0
        self._state = CircuitState.CLOSED

    def record_failure(self) -> None:
        """Call failed — maybe trip the breaker."""
        self._consecutive_failures += 1
        self._last_failure_time = time.monotonic()

        if self._consecutive_failures >= self._threshold:
            if self._state != CircuitState.OPEN:
                logger.warning(
                    f"Circuit '{self._name}' → OPEN "
                    f"({self._consecutive_failures} consecutive failures, "
                    f"cooldown={self._recovery_timeout}s)"
                )
            self._state = CircuitState.OPEN

    def stats(self) -> dict:
        return {
            "name": self._name,
            "state": self.state.value,
            "consecutive_failures": self._consecutive_failures,
            "total_rejected": self._total_rejected,
        }
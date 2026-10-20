"""Failure-isolation primitives.

Retries alone make a dead dependency worse. When an upstream has been down for
ten minutes, every scheduler tick still pays the full retry budget -- four
attempts with backoff against a host that will not answer -- so the collector
spends its time in timeouts, Cloud Run holds instances open waiting on them,
and the agency receives a steady retry storm it did not ask for. Retries are
for a *transient* fault; a circuit breaker is for a *persistent* one, and a
pipeline needs both because it cannot tell which it has until it has tried.

The breaker here is deliberately counted in whole calls rather than individual
retry attempts: one scheduler tick that exhausted its retries is one failure.
Counting attempts would open the breaker partway through the first bad tick,
before the retry budget -- the thing that handles transient faults -- had been
spent at all.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from enum import StrEnum

import structlog

log = structlog.get_logger(__name__)


class CircuitState(StrEnum):
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"


class CircuitOpenError(RuntimeError):
    """Raised instead of calling a dependency the breaker has given up on."""

    def __init__(self, name: str, retry_after_seconds: float) -> None:
        super().__init__(f"circuit {name!r} is open; retry in {retry_after_seconds:.1f}s")
        self.name = name
        self.retry_after_seconds = retry_after_seconds


class CircuitBreaker:
    """Three-state breaker guarding one dependency.

    ``CLOSED`` passes calls through. ``consecutive_failures`` reaching the
    threshold moves it to ``OPEN``, where calls fail immediately without
    touching the dependency. Once the cooldown has elapsed the next call is
    admitted as a ``HALF_OPEN`` probe: if it succeeds the breaker closes, and
    if it fails the cooldown restarts. The half-open probe is what lets
    recovery be detected without anyone redeploying.
    """

    def __init__(
        self,
        *,
        name: str,
        failure_threshold: int,
        reset_timeout_seconds: float,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if failure_threshold < 1:
            raise ValueError("failure_threshold must be at least 1")
        self._name = name
        self._threshold = failure_threshold
        self._reset_timeout = reset_timeout_seconds
        # Monotonic by default: a wall clock can step backwards over an NTP
        # correction and leave the breaker stuck open for the length of the
        # jump. Injectable so the cooldown is testable without sleeping.
        self._clock = clock

        self._lock = threading.Lock()
        self._state = CircuitState.CLOSED
        self._consecutive_failures = 0
        self._opened_at: float | None = None

    @property
    def name(self) -> str:
        return self._name

    @property
    def state(self) -> CircuitState:
        with self._lock:
            return self._state

    @property
    def consecutive_failures(self) -> int:
        with self._lock:
            return self._consecutive_failures

    def retry_after_seconds(self) -> float:
        """How long until the breaker will admit a probe. Zero when closed."""
        with self._lock:
            return self._retry_after_locked()

    def _retry_after_locked(self) -> float:
        if self._state is not CircuitState.OPEN or self._opened_at is None:
            return 0.0
        remaining = self._reset_timeout - (self._clock() - self._opened_at)
        return max(0.0, remaining)

    def before_call(self) -> None:
        """Gate a call. Raises :class:`CircuitOpenError` while open.

        Call this *before* the retry loop, not inside it, so an open breaker
        costs one rejected call rather than a full retry budget.
        """
        with self._lock:
            if self._state is CircuitState.OPEN:
                if self._retry_after_locked() > 0.0:
                    raise CircuitOpenError(self._name, self._retry_after_locked())
                self._state = CircuitState.HALF_OPEN
                log.warning("circuit.half_open", circuit=self._name)

    def record_success(self) -> None:
        with self._lock:
            was = self._state
            self._consecutive_failures = 0
            self._opened_at = None
            self._state = CircuitState.CLOSED
        if was is not CircuitState.CLOSED:
            log.info("circuit.closed", circuit=self._name, previous_state=was)

    def record_failure(self) -> None:
        with self._lock:
            self._consecutive_failures += 1
            failures = self._consecutive_failures
            # A failed half-open probe re-opens immediately: the dependency
            # has just demonstrated it is still down, so waiting for the
            # threshold again would send another threshold's worth of traffic
            # at it.
            if self._state is CircuitState.HALF_OPEN or failures >= self._threshold:
                opened = self._state is not CircuitState.OPEN
                self._state = CircuitState.OPEN
                self._opened_at = self._clock()
            else:
                opened = False
        if opened:
            log.error(
                "circuit.opened",
                circuit=self._name,
                consecutive_failures=failures,
                cooldown_seconds=self._reset_timeout,
            )

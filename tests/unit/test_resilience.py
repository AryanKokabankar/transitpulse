"""Tests for the circuit breaker.

Driven by a fake clock rather than real time. The cooldown is sixty seconds in
production, and a test that waited for it would be unrunnable; a test that
shortened it to milliseconds would be measuring ``time.sleep`` rather than the
state machine.
"""

from __future__ import annotations

import pytest

from transitpulse_core.resilience import CircuitBreaker, CircuitOpenError, CircuitState


class FakeClock:
    """A monotonic clock the test advances by hand."""

    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def breaker(clock: FakeClock) -> CircuitBreaker:
    return CircuitBreaker(
        name="test-feed", failure_threshold=3, reset_timeout_seconds=60.0, clock=clock
    )


class TestClosedState:
    def test_starts_closed_and_allows_calls(self, breaker: CircuitBreaker) -> None:
        assert breaker.state is CircuitState.CLOSED
        breaker.before_call()

    def test_failures_below_the_threshold_keep_it_closed(self, breaker: CircuitBreaker) -> None:
        breaker.record_failure()
        breaker.record_failure()
        assert breaker.state is CircuitState.CLOSED
        assert breaker.consecutive_failures == 2
        breaker.before_call()

    def test_a_success_resets_the_failure_run(self, breaker: CircuitBreaker) -> None:
        """Consecutive, not cumulative: an upstream that fails once an hour is
        healthy, and counting cumulatively would trip on it eventually."""
        breaker.record_failure()
        breaker.record_failure()
        breaker.record_success()
        assert breaker.consecutive_failures == 0

        breaker.record_failure()
        breaker.record_failure()
        assert breaker.state is CircuitState.CLOSED

    def test_requires_a_positive_threshold(self) -> None:
        with pytest.raises(ValueError, match="failure_threshold"):
            CircuitBreaker(name="x", failure_threshold=0, reset_timeout_seconds=1.0)


class TestOpening:
    def test_opens_at_the_threshold(self, breaker: CircuitBreaker) -> None:
        for _ in range(3):
            breaker.record_failure()
        assert breaker.state is CircuitState.OPEN

    def test_rejects_calls_while_open(self, breaker: CircuitBreaker) -> None:
        """The whole point: a dead upstream stops being called at all."""
        for _ in range(3):
            breaker.record_failure()

        with pytest.raises(CircuitOpenError) as exc_info:
            breaker.before_call()
        assert exc_info.value.name == "test-feed"
        assert exc_info.value.retry_after_seconds == pytest.approx(60.0)

    def test_reports_a_shrinking_retry_after(
        self, breaker: CircuitBreaker, clock: FakeClock
    ) -> None:
        for _ in range(3):
            breaker.record_failure()
        clock.advance(45.0)
        assert breaker.retry_after_seconds() == pytest.approx(15.0)

    def test_retry_after_is_zero_when_closed(self, breaker: CircuitBreaker) -> None:
        assert breaker.retry_after_seconds() == 0.0


class TestHalfOpenRecovery:
    def test_admits_one_probe_after_the_cooldown(
        self, breaker: CircuitBreaker, clock: FakeClock
    ) -> None:
        for _ in range(3):
            breaker.record_failure()
        clock.advance(60.1)

        breaker.before_call()
        assert breaker.state is CircuitState.HALF_OPEN

    def test_a_successful_probe_closes_the_circuit(
        self, breaker: CircuitBreaker, clock: FakeClock
    ) -> None:
        """Recovery has to be detected without anyone redeploying."""
        for _ in range(3):
            breaker.record_failure()
        clock.advance(60.1)
        breaker.before_call()
        breaker.record_success()

        assert breaker.state is CircuitState.CLOSED
        assert breaker.consecutive_failures == 0
        breaker.before_call()

    def test_a_failed_probe_reopens_immediately(
        self, breaker: CircuitBreaker, clock: FakeClock
    ) -> None:
        # Not "wait for the threshold again": the probe has just proved the
        # dependency is still down, so sending two more calls at it would be
        # the retry storm this class exists to prevent.
        for _ in range(3):
            breaker.record_failure()
        clock.advance(60.1)
        breaker.before_call()
        breaker.record_failure()

        assert breaker.state is CircuitState.OPEN
        with pytest.raises(CircuitOpenError):
            breaker.before_call()

    def test_a_failed_probe_restarts_the_full_cooldown(
        self, breaker: CircuitBreaker, clock: FakeClock
    ) -> None:
        for _ in range(3):
            breaker.record_failure()
        clock.advance(60.1)
        breaker.before_call()
        breaker.record_failure()

        assert breaker.retry_after_seconds() == pytest.approx(60.0)

    def test_stays_closed_through_a_second_recovery_cycle(
        self, breaker: CircuitBreaker, clock: FakeClock
    ) -> None:
        for _cycle in range(2):
            for _ in range(3):
                breaker.record_failure()
            assert breaker.state is CircuitState.OPEN
            clock.advance(60.1)
            breaker.before_call()
            breaker.record_success()
            assert breaker.state is CircuitState.CLOSED

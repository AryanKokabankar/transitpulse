"""Tests for the load harness's rate limiter.

Every throughput figure this project publishes is produced by a loop paced by
this class, so these tests are about arithmetic rather than about wall-clock
timing: a limiter that runs 12% slow would quietly understate the pipeline in
every benchmark, and a test that measured real elapsed time would be too noisy
to catch that.

The fake clock advances only when the limiter sleeps, which also asserts the
property that matters most -- that the limiter derives tokens from the clock
instead of assuming its sleeps were exact.
"""

from __future__ import annotations

import pytest

from tools.replay.limiter import TokenBucket


class FakeTime:
    """A clock that only moves when something sleeps on it."""

    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []

    def clock(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds

    def oversleeping(self, factor: float) -> FakeTime:
        """Model a coarse OS timer that always sleeps longer than asked."""
        original = self.sleep

        def slow_sleep(seconds: float) -> None:
            original(seconds * factor - seconds)
            original(seconds)

        self.sleep = slow_sleep  # type: ignore[method-assign]
        return self

    @property
    def total_slept(self) -> float:
        return sum(self.sleeps)


@pytest.fixture
def fake() -> FakeTime:
    return FakeTime()


def bucket(fake: FakeTime, rate: float, capacity: float | None = None) -> TokenBucket:
    return TokenBucket(rate, capacity=capacity, clock=fake.clock, sleep=fake.sleep)


class TestConstruction:
    def test_rejects_a_non_positive_rate(self, fake: FakeTime) -> None:
        with pytest.raises(ValueError, match="positive"):
            bucket(fake, 0)

    def test_capacity_defaults_to_a_fraction_of_a_second(self, fake: FakeTime) -> None:
        """A full second of capacity would let a run open with a burst the size
        of the whole target rate, which measures a dogpile rather than a rate."""
        assert bucket(fake, 500).capacity == pytest.approx(50.0)

    def test_capacity_never_falls_below_one_token(self, fake: FakeTime) -> None:
        assert bucket(fake, 2).capacity == pytest.approx(1.0)

    def test_starts_empty(self, fake: FakeTime) -> None:
        # A full bucket would emit a capacity-sized burst in the first instant
        # and show up as a latency spike the pipeline never really met.
        assert bucket(fake, 100).available_tokens == pytest.approx(0.0)


class TestPacing:
    def test_the_first_acquisition_waits_for_its_tokens(self, fake: FakeTime) -> None:
        limiter = bucket(fake, 100)
        waited = limiter.acquire(10)
        assert waited == pytest.approx(0.1)
        assert fake.now == pytest.approx(0.1)

    def test_sustained_rate_matches_the_target(self, fake: FakeTime) -> None:
        """The headline property: N tokens at rate R take N/R seconds."""
        limiter = bucket(fake, 500)
        for _ in range(200):
            limiter.acquire(25)

        assert fake.now == pytest.approx(10.0, rel=1e-9)
        assert limiter.stats.tokens_granted == pytest.approx(5000.0)

    @pytest.mark.parametrize("rate", [1.0, 50.0, 500.0, 5000.0])
    def test_rate_is_accurate_across_magnitudes(self, fake: FakeTime, rate: float) -> None:
        limiter = bucket(fake, rate, capacity=rate)
        total = int(rate * 2)
        for _ in range(total):
            limiter.acquire(1)

        achieved = total / fake.now
        assert achieved == pytest.approx(rate, rel=1e-6)

    def test_accumulated_tokens_are_granted_without_waiting(self, fake: FakeTime) -> None:
        limiter = bucket(fake, 100, capacity=50)
        fake.now += 0.5  # idle: the bucket refills

        assert limiter.acquire(50) == 0.0
        assert fake.sleeps == []

    def test_capacity_caps_the_burst(self, fake: FakeTime) -> None:
        """An idle hour must not buy an hour's worth of credit."""
        limiter = bucket(fake, 100, capacity=20)
        fake.now += 3600.0

        assert limiter.available_tokens == pytest.approx(20.0)

    def test_overshooting_sleeps_do_not_accumulate_drift(self, fake: FakeTime) -> None:
        """A coarse OS timer must cost throughput once, not compound.

        Tokens are derived as ``elapsed * rate`` from the clock, so time lost
        to an oversleeping timer is still time that earned tokens; the next
        acquisition finds them waiting instead of sleeping again.
        """
        limiter = bucket(fake.oversleeping(3.0), 100, capacity=100)
        for _ in range(100):
            limiter.acquire(1)

        # 100 tokens at 100/s is one second of work. A limiter that trusted its
        # own sleeps would have taken three.
        assert fake.now == pytest.approx(1.0, abs=0.05)
        assert 100 / fake.now == pytest.approx(100.0, rel=0.05)


class TestFloatingPointSafety:
    """Regression tests for a hang found while writing these tests.

    Tokens accumulate by repeated float addition, so a bucket can sit a few
    attoseconds' worth below a request it has genuinely earned. The limiter
    then slept for a shortfall too small to move the clock, found itself short
    again, and spun forever -- and whether it happened at all depended on
    whether the rate and batch size divided cleanly in binary, which is the
    worst possible way for a load harness to fail.
    """

    @pytest.mark.parametrize(
        ("rate", "batch"),
        [
            (500.0, 25.0),
            (5000.0, 1.0),
            (300.0, 7.0),
            (1000.0, 3.0),
            (97.0, 13.0),
        ],
    )
    def test_awkward_rate_and_batch_combinations_terminate(
        self, fake: FakeTime, rate: float, batch: float
    ) -> None:
        limiter = bucket(fake, rate, capacity=max(batch, rate * 0.1))
        for _ in range(100):
            limiter.acquire(batch)

        expected = (100 * batch) / rate
        assert fake.now == pytest.approx(expected, rel=1e-6)

    def test_a_clock_too_coarse_to_see_the_shortfall_still_progresses(self, fake: FakeTime) -> None:
        """The failure mode directly: a sleep that cannot advance the clock.

        The limiter must grant the request it has all but earned rather than
        asking again for a slice the clock cannot represent.
        """
        limiter = bucket(fake, 500, capacity=50)
        for _ in range(20):
            limiter.acquire(25)

        assert fake.now == pytest.approx(1.0, rel=1e-9)
        # Each batch needed exactly one sleep: no spinning.
        assert limiter.stats.sleeps == 20


class TestTryAcquire:
    def test_fails_without_blocking_when_empty(self, fake: FakeTime) -> None:
        limiter = bucket(fake, 100)
        assert limiter.try_acquire(1) is False
        assert fake.sleeps == []

    def test_succeeds_once_tokens_exist(self, fake: FakeTime) -> None:
        limiter = bucket(fake, 100)
        fake.now += 0.1
        assert limiter.try_acquire(10) is True
        assert limiter.try_acquire(1) is False


class TestInvalidRequests:
    def test_rejects_a_request_larger_than_the_bucket(self, fake: FakeTime) -> None:
        """Would otherwise block forever: the bucket can never hold that much."""
        limiter = bucket(fake, 100, capacity=10)
        with pytest.raises(ValueError, match="capacity"):
            limiter.acquire(11)

    @pytest.mark.parametrize("tokens", [0, -1])
    def test_rejects_a_non_positive_request(self, fake: FakeTime, tokens: float) -> None:
        with pytest.raises(ValueError, match="positive"):
            bucket(fake, 100).acquire(tokens)


class TestStats:
    def test_counts_acquisitions_and_waiting(self, fake: FakeTime) -> None:
        limiter = bucket(fake, 100)
        for _ in range(4):
            limiter.acquire(5)

        assert limiter.stats.acquisitions == 4
        assert limiter.stats.tokens_granted == pytest.approx(20.0)
        assert limiter.stats.wait_seconds == pytest.approx(0.2)

    def test_reports_no_waiting_when_tokens_were_already_there(self, fake: FakeTime) -> None:
        """This is how the report tells a paced run from a saturated one: a
        limiter that never waited was never the constraint."""
        limiter = bucket(fake, 100, capacity=100)
        fake.now += 1.0
        limiter.acquire(100)

        assert limiter.stats.wait_seconds == 0.0
        assert limiter.stats.sleeps == 0

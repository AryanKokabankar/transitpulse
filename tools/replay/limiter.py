"""Token-bucket rate limiter for the load harness.

Every throughput figure this project will ever publish is produced by a loop
governed by this class, so its accuracy is not a detail -- a limiter that
quietly runs 12% slow turns into a benchmark that understates the pipeline, and
one that runs fast turns into a number that was never actually achieved.

Two design choices follow from that:

**Refill is computed from the clock, not accumulated per tick.** Tokens are
derived as ``elapsed * rate`` from a monotonic reading each time, so an
oversleeping ``time.sleep`` -- unavoidable, and coarse on Windows -- is
absorbed on the next acquisition instead of compounding into permanent drift.

**Capacity is a fraction of a second by default.** A bucket holding a full
second of tokens lets the harness open with a burst the size of the whole
rate, which measures how fast a queue accepts a dogpile rather than the rate
the pipeline sustains. Small capacity keeps the burst bounded while still
leaving enough headroom to recover from sleep overshoot.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass

# Ten bursts' worth of headroom per second: enough to absorb a scheduler
# hiccup, far too little to front-load a run.
DEFAULT_CAPACITY_SECONDS = 0.1

# Tokens accumulate by repeated float addition, so the bucket can end up a few
# attoseconds' worth short of a request it has genuinely earned -- 24.999...
# against a requested 25. Without a tolerance the limiter then computes a
# shortfall so small that sleeping for it does not advance the clock, finds
# itself short again, and spins forever: a hang whose appearance depends on
# whether a particular rate and batch size divide cleanly in binary. At any
# realistic rate this much credit is many orders of magnitude below one
# message, so it cannot flatter a measured throughput.
_TOKEN_EPSILON = 1e-9


@dataclass
class LimiterStats:
    """What the limiter did, so the report can say whether it was the
    bottleneck.

    ``wait_seconds`` near zero means the limiter never had to hold anything
    back: the loop could not keep up with the target on its own, so a shortfall
    in achieved rate belongs to the publisher, not to the pacing.
    """

    acquisitions: int = 0
    tokens_granted: float = 0.0
    wait_seconds: float = 0.0
    sleeps: int = 0


class TokenBucket:
    """Paces a loop to a target rate.

    ``clock`` and ``sleep`` are injectable so the pacing can be tested against
    a deterministic fake clock; a test that asserted real timing would be slow
    and flaky, and the arithmetic is the part that must be right.
    """

    def __init__(
        self,
        rate_per_second: float,
        *,
        capacity: float | None = None,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if rate_per_second <= 0:
            raise ValueError("rate_per_second must be positive")

        self._rate = float(rate_per_second)
        self._capacity = float(
            capacity
            if capacity is not None
            else max(1.0, rate_per_second * DEFAULT_CAPACITY_SECONDS)
        )
        self._clock = clock
        self._sleep = sleep

        # Starts empty rather than full: a full bucket would let the first
        # instant of the run emit a whole capacity's worth of messages, which
        # shows up in the results as a latency spike the pipeline never
        # actually met in steady state.
        self._tokens = 0.0
        self._updated = clock()
        self.stats = LimiterStats()

    @property
    def rate_per_second(self) -> float:
        return self._rate

    @property
    def capacity(self) -> float:
        return self._capacity

    @property
    def available_tokens(self) -> float:
        self._refill()
        return self._tokens

    def _refill(self) -> None:
        now = self._clock()
        elapsed = now - self._updated
        if elapsed <= 0:
            return
        self._tokens = min(self._capacity, self._tokens + elapsed * self._rate)
        self._updated = now

    def try_acquire(self, tokens: float = 1.0) -> bool:
        """Take tokens if they are already available. Never blocks."""
        self._refill()
        if self._tokens + _TOKEN_EPSILON < tokens:
            return False
        self._tokens -= tokens
        self.stats.acquisitions += 1
        self.stats.tokens_granted += tokens
        return True

    def acquire(self, tokens: float = 1.0) -> float:
        """Block until ``tokens`` are available. Returns seconds spent waiting.

        Asking for more than the bucket can ever hold would deadlock, so it is
        rejected rather than silently clamped: the caller has chosen a batch
        size the configured capacity cannot pace.
        """
        if tokens <= 0:
            raise ValueError("tokens must be positive")
        if tokens > self._capacity:
            raise ValueError(
                f"cannot acquire {tokens:g} tokens from a bucket of capacity "
                f"{self._capacity:g}; raise capacity or lower the batch size"
            )

        waited = 0.0
        while True:
            self._refill()
            if self._tokens + _TOKEN_EPSILON >= tokens:
                self._tokens -= tokens
                self.stats.acquisitions += 1
                self.stats.tokens_granted += tokens
                self.stats.wait_seconds += waited
                return waited

            # Sleep only for the shortfall, then re-read the clock rather than
            # assuming the sleep was exact -- that is what keeps overshoot from
            # accumulating, and it means the reported wait is the wait that
            # actually happened rather than the one that was asked for.
            shortfall = (tokens - self._tokens) / self._rate
            before = self._clock()
            self._sleep(shortfall)
            self.stats.sleeps += 1
            waited += max(0.0, self._clock() - before)

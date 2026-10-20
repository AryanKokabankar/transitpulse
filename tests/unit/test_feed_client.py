"""Tests for fetching the feed: retries, classification, and the breaker.

Driven through ``httpx.MockTransport`` rather than a patched client, so the
request actually travels httpx's own code path -- timeouts raise the exception
types httpx really raises, and the retry predicate is tested against those
rather than against a test double's idea of them.

Retry waits are configured to zero here. The backoff policy is tenacity's and
is not this project's code to test; what matters is how many attempts happen,
which exception types trigger them, and what the breaker does afterwards.
"""

from __future__ import annotations

from collections.abc import Callable

import httpx
import pytest

from services.collector.feed import (
    FeedClient,
    FeedDecodeError,
    FeedHTTPError,
    FeedTimeoutError,
)
from tests.gtfs_fixtures import VehicleEntitySpec, build_vehicle_feed
from transitpulse_core.config import Settings
from transitpulse_core.resilience import CircuitBreaker, CircuitOpenError, CircuitState

FEED_URL = "https://feed.example.test/gtfs-rt"

Handler = Callable[[httpx.Request], httpx.Response]


def settings_for(**overrides: object) -> Settings:
    base: dict[str, object] = {
        "feed_url": FEED_URL,
        "feed_agency": "test-agency",
        "feed_max_attempts": 4,
        # The wait policy belongs to tenacity; zero keeps the suite fast
        # without changing which attempts happen.
        "feed_retry_initial_seconds": 0.0,
        "feed_retry_max_seconds": 0.0,
        "feed_breaker_failure_threshold": 3,
        "feed_breaker_reset_seconds": 60.0,
    }
    base.update(overrides)
    return Settings(**base)  # type: ignore[arg-type]


def client_with(
    handler: Handler,
    *,
    settings: Settings | None = None,
    breaker: CircuitBreaker | None = None,
) -> FeedClient:
    return FeedClient(
        settings or settings_for(),
        client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        breaker=breaker,
    )


class CountingHandler:
    """Replays a scripted sequence of outcomes, counting the calls."""

    def __init__(self, *outcomes: httpx.Response | Exception) -> None:
        self._outcomes = list(outcomes)
        self.calls = 0

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.calls += 1
        outcome = self._outcomes[min(self.calls - 1, len(self._outcomes) - 1)]
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


def ok_response() -> httpx.Response:
    return httpx.Response(200, content=build_vehicle_feed(VehicleEntitySpec()))


class TestSuccessfulFetch:
    async def test_returns_the_payload_on_the_first_attempt(self) -> None:
        handler = CountingHandler(ok_response())
        client = client_with(handler)
        try:
            response = await client.fetch()
        finally:
            await client.aclose()

        assert handler.calls == 1
        assert response.attempts == 1
        assert response.payload
        assert response.duration_seconds >= 0.0
        assert response.fetched_at.tzinfo is not None

    async def test_leaves_the_breaker_closed(self) -> None:
        client = client_with(CountingHandler(ok_response()))
        try:
            await client.fetch()
        finally:
            await client.aclose()
        assert client.breaker.state is CircuitState.CLOSED


class TestRetries:
    async def test_retries_a_timeout_and_then_succeeds(self) -> None:
        handler = CountingHandler(
            httpx.TimeoutException("timed out"),
            httpx.TimeoutException("timed out"),
            ok_response(),
        )
        client = client_with(handler)
        try:
            response = await client.fetch()
        finally:
            await client.aclose()

        assert handler.calls == 3
        assert response.attempts == 3
        # A tick that recovered within its budget is not a failure.
        assert client.breaker.consecutive_failures == 0

    async def test_stops_at_the_configured_attempt_cap(self) -> None:
        """The retry budget is bounded: an unbounded one holds a Cloud Run
        instance open indefinitely on a dead upstream."""
        handler = CountingHandler(httpx.TimeoutException("timed out"))
        client = client_with(handler, settings=settings_for(feed_max_attempts=4))
        try:
            with pytest.raises(FeedTimeoutError):
                await client.fetch()
        finally:
            await client.aclose()

        assert handler.calls == 4

    @pytest.mark.parametrize("attempts", [1, 2, 5])
    async def test_honours_the_attempt_setting(self, attempts: int) -> None:
        handler = CountingHandler(httpx.ConnectError("refused"))
        client = client_with(handler, settings=settings_for(feed_max_attempts=attempts))
        try:
            with pytest.raises(FeedHTTPError):
                await client.fetch()
        finally:
            await client.aclose()

        assert handler.calls == attempts

    @pytest.mark.parametrize("status", [429, 500, 502, 503, 504, 408])
    async def test_retries_transient_status_codes(self, status: int) -> None:
        handler = CountingHandler(httpx.Response(status), ok_response())
        client = client_with(handler)
        try:
            await client.fetch()
        finally:
            await client.aclose()

        assert handler.calls == 2

    @pytest.mark.parametrize("status", [400, 401, 403, 404, 410])
    async def test_does_not_retry_a_permanent_status_code(self, status: int) -> None:
        """A 404 says the same thing on the fourth attempt, and retrying it
        only delays the alert that the feed URL is wrong."""
        handler = CountingHandler(httpx.Response(status))
        client = client_with(handler)
        try:
            with pytest.raises(FeedHTTPError) as exc_info:
                await client.fetch()
        finally:
            await client.aclose()

        assert handler.calls == 1
        assert exc_info.value.status_code == status
        assert exc_info.value.retryable is False

    async def test_retries_an_empty_body(self) -> None:
        # Observed during agency deploys: HTTP 200 with nothing in it.
        handler = CountingHandler(httpx.Response(200, content=b""), ok_response())
        client = client_with(handler)
        try:
            await client.fetch()
        finally:
            await client.aclose()

        assert handler.calls == 2

    async def test_an_always_empty_body_ends_as_a_decode_error(self) -> None:
        handler = CountingHandler(httpx.Response(200, content=b""))
        client = client_with(handler)
        try:
            with pytest.raises(FeedDecodeError):
                await client.fetch()
        finally:
            await client.aclose()

        assert handler.calls == 4


class TestCircuitBreaker:
    def breaker(self) -> CircuitBreaker:
        return CircuitBreaker(name="test-feed", failure_threshold=3, reset_timeout_seconds=60.0)

    async def test_counts_one_failure_per_tick_not_per_attempt(self) -> None:
        """A tick that exhausted four retries is one failure.

        Counting attempts would open the breaker partway through the very first
        bad tick, before the retry budget -- the thing that handles transient
        faults -- had been spent at all.
        """
        breaker = self.breaker()
        handler = CountingHandler(httpx.TimeoutException("timed out"))
        client = client_with(handler, breaker=breaker)
        try:
            with pytest.raises(FeedTimeoutError):
                await client.fetch()
        finally:
            await client.aclose()

        assert handler.calls == 4
        assert breaker.consecutive_failures == 1
        assert breaker.state is CircuitState.CLOSED

    async def test_opens_after_repeated_failing_ticks(self) -> None:
        breaker = self.breaker()
        handler = CountingHandler(httpx.TimeoutException("timed out"))
        client = client_with(handler, breaker=breaker)
        try:
            for _ in range(3):
                with pytest.raises(FeedTimeoutError):
                    await client.fetch()
            assert breaker.state is CircuitState.OPEN

            # The point of the breaker: the next tick costs one rejection
            # instead of another four timeouts against a dead host.
            calls_before = handler.calls
            with pytest.raises(CircuitOpenError):
                await client.fetch()
            assert handler.calls == calls_before
        finally:
            await client.aclose()

    async def test_a_recovered_feed_closes_the_breaker(self) -> None:
        """Recovery is detected by a probe, with no redeploy and no restart."""
        now = [1000.0]
        breaker = CircuitBreaker(
            name="test-feed",
            failure_threshold=2,
            reset_timeout_seconds=60.0,
            clock=lambda: now[0],
        )
        # Fails until the ninth call, which is the half-open probe.
        handler = CountingHandler(
            *([httpx.TimeoutException("down")] * 8),
            ok_response(),
        )
        client = client_with(handler, settings=settings_for(feed_max_attempts=4), breaker=breaker)
        try:
            for _ in range(2):
                with pytest.raises(FeedTimeoutError):
                    await client.fetch()
            assert breaker.state is CircuitState.OPEN

            with pytest.raises(CircuitOpenError):
                await client.fetch()

            now[0] += 60.1  # the cooldown elapses
            await client.fetch()
        finally:
            await client.aclose()

        assert breaker.state is CircuitState.CLOSED
        assert handler.calls == 9


class TestFailureClassification:
    async def test_a_timeout_is_labelled_as_one(self) -> None:
        client = client_with(CountingHandler(httpx.ReadTimeout("slow")))
        try:
            with pytest.raises(FeedTimeoutError) as exc_info:
                await client.fetch()
        finally:
            await client.aclose()
        assert str(exc_info.value.kind) == "timeout"

    async def test_a_transport_error_is_labelled_http(self) -> None:
        client = client_with(CountingHandler(httpx.ConnectError("refused")))
        try:
            with pytest.raises(FeedHTTPError) as exc_info:
                await client.fetch()
        finally:
            await client.aclose()
        assert str(exc_info.value.kind) == "http_error"

    async def test_an_empty_body_is_labelled_a_decode_error(self) -> None:
        client = client_with(CountingHandler(httpx.Response(200, content=b"")))
        try:
            with pytest.raises(FeedDecodeError) as exc_info:
                await client.fetch()
        finally:
            await client.aclose()
        assert str(exc_info.value.kind) == "decode_error"

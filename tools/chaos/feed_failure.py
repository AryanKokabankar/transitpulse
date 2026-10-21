"""Experiment 3: break the upstream feed and check the breaker stops calling it.

A circuit breaker is easy to write and easy to get wrong in a way that is
invisible: it reports itself open, the dashboard shows the state, and requests
keep going out anyway because the gate was placed inside the retry loop rather
than in front of it. The state is not the claim. **The claim is that the
upstream stops being called**, and only the upstream can confirm it -- so the
loopback server here counts the requests it is actually sent, and the check is
that the count stops rising.

Three parts, because they fail differently:

* **503s.** The retryable case. Every tick spends its full retry budget before
  the breaker counts one failure -- the breaker is deliberately counted in
  whole calls, not attempts, so a transient blip is handled by retries and
  only a persistent fault opens the gate.
* **Timeouts.** The expensive case, and the reason the breaker exists at all. A
  refused connection costs nothing; a hung one costs the whole timeout budget
  per attempt, and a dead agency would otherwise consume every tick that way
  indefinitely.
* **The service.** The collector's own handler, driven end to end, returning
  502 while the feed is merely failing and 503 with ``Retry-After`` once the
  breaker is open. Cloud Scheduler honours ``Retry-After``, so the cooldown is
  communicated rather than guessed at -- which is only true if the header is
  really there.

Two settings are shortened so a run takes seconds rather than minutes, and both
are reported in the result file: the breaker cooldown (60s by default) and, in
the timeout and service parts, the retry budget. Neither changes what is being
measured -- the breaker counts calls, so the backoff between attempts within a
call is not part of its arithmetic.

    python -m tools.chaos.feed_failure
"""

from __future__ import annotations

import argparse
import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime

import httpx
import structlog
from google.transit import gtfs_realtime_pb2 as gtfs

from services.collector.feed import FeedClient, FeedError, FeedFailureKind
from services.collector.main import app as collector_app
from tools.chaos.experiment import (
    ScenarioResult,
    add_common_arguments,
    at_least,
    equals,
    holds,
    scenario_main,
)
from tools.chaos.support import serve_feed, settings_override
from transitpulse_core.config import Settings
from transitpulse_core.resilience import CircuitBreaker, CircuitOpenError, CircuitState

NAME = "feed-failure"
SERVICE_NAME = "transitpulse-chaos-feed-failure"
QUESTION = "Does the breaker open, actually stop calling upstream, and recover on its own?"

log = structlog.get_logger(__name__)


@dataclass
class TickLog:
    """What a sequence of scheduler ticks did, from the caller's side."""

    ok: int = 0
    failed: int = 0
    rejected_by_breaker: int = 0
    kinds: dict[str, int] | None = None

    def record_kind(self, kind: str) -> None:
        if self.kinds is None:
            self.kinds = {}
        self.kinds[kind] = self.kinds.get(kind, 0) + 1

    def as_dict(self) -> dict[str, object]:
        return {
            "ok": self.ok,
            "failed": self.failed,
            "rejected_by_breaker": self.rejected_by_breaker,
            "kinds": self.kinds or {},
        }


async def tick(client: FeedClient, log_: TickLog) -> None:
    """One scheduler tick's worth of fetching, swallowing the outcome."""
    try:
        await client.fetch()
    except CircuitOpenError:
        # The cheap outcome the breaker exists to produce: one rejection
        # instead of a full retry budget against a host that will not answer.
        log_.rejected_by_breaker += 1
    except FeedError as exc:
        log_.failed += 1
        log_.record_kind(str(exc.kind))
    else:
        log_.ok += 1


def _healthy_feed_body() -> bytes:
    """The smallest body a recovered upstream could serve.

    Non-empty on purpose: the client treats a 200 with no body as a retryable
    decode failure -- observed in the wild during agency deploys -- which would
    make the half-open probe fail and the breaker re-open, so the experiment
    would conclude the opposite of the truth.
    """
    message = gtfs.FeedMessage()
    message.header.gtfs_realtime_version = "2.0"
    message.header.incrementality = gtfs.FeedHeader.FULL_DATASET
    message.header.timestamp = int(datetime.now(UTC).timestamp())
    return message.SerializeToString()


def _breaker(settings: Settings, cooldown: float) -> CircuitBreaker:
    return CircuitBreaker(
        name="gtfs-feed",
        failure_threshold=settings.feed_breaker_failure_threshold,
        reset_timeout_seconds=cooldown,
    )


async def _unavailable_upstream(
    settings: Settings, result: ScenarioResult, args: argparse.Namespace
) -> None:
    """Part 1: persistent 503s, with the real retry budget in play."""
    threshold = settings.feed_breaker_failure_threshold
    with serve_feed(status_code=503) as served:
        feed = settings.model_copy(update={"feed_url": served.url})
        breaker = _breaker(settings, args.cooldown_seconds)
        client = FeedClient(feed, breaker=breaker)
        try:
            failing = TickLog()
            for _ in range(threshold):
                await tick(client, failing)
            hits_at_open = served.hits
            state_at_open = breaker.state
            retry_after = breaker.retry_after_seconds()

            # The measurement that matters: more ticks, and the far end must
            # see none of them.
            while_open = TickLog()
            for _ in range(args.ticks_while_open):
                await tick(client, while_open)
            hits_while_open = served.hits - hits_at_open

            result.measure(
                breaker_threshold=threshold,
                feed_max_attempts=settings.feed_max_attempts,
                ticks_until_open=threshold,
                upstream_requests_before_open=hits_at_open,
                state_after_threshold=str(state_at_open),
                retry_after_seconds=round(retry_after, 2),
                ticks_while_open=args.ticks_while_open,
                upstream_requests_while_open=hits_while_open,
                failing_ticks=failing.as_dict(),
                open_ticks=while_open.as_dict(),
            )
            result.check(
                equals(
                    "the breaker opened on the threshold tick",
                    str(CircuitState.OPEN),
                    str(state_at_open),
                ),
                equals(
                    "each failing tick spent its whole retry budget before counting",
                    threshold * settings.feed_max_attempts,
                    hits_at_open,
                ),
                equals(
                    "the upstream was not called again while the breaker was open",
                    0,
                    hits_while_open,
                ),
                equals(
                    "every tick arriving at an open breaker was rejected outright",
                    args.ticks_while_open,
                    while_open.rejected_by_breaker,
                ),
                at_least("the rejection carried a cooldown to wait out", 0.1, retry_after),
            )

            # --- recovery, with no intervention ---------------------------
            await asyncio.sleep(args.cooldown_seconds + 0.5)
            served.status_code = 200
            served.body = _healthy_feed_body()
            hits_before_probe = served.hits
            recovery = TickLog()
            await tick(client, recovery)

            result.measure(
                recovery_tick=recovery.as_dict(),
                state_after_recovery=str(breaker.state),
                upstream_requests_for_probe=served.hits - hits_before_probe,
            )
            result.check(
                equals(
                    "the half-open probe was admitted once the cooldown elapsed",
                    1,
                    served.hits - hits_before_probe,
                ),
                equals(
                    "the breaker closed on a successful probe",
                    str(CircuitState.CLOSED),
                    str(breaker.state),
                ),
            )
        finally:
            await client.aclose()


async def _hung_upstream(
    settings: Settings, result: ScenarioResult, args: argparse.Namespace
) -> None:
    """Part 2: connections that are accepted and never answered."""
    threshold = settings.feed_breaker_failure_threshold
    timeout = 0.4
    with serve_feed(delay_seconds=timeout * 6) as served:
        feed = settings.model_copy(
            update={
                "feed_url": served.url,
                "feed_timeout_seconds": timeout,
                # One attempt per tick, so the part that costs real time -- a
                # whole timeout budget, four times over -- is not paid five
                # times for a property the breaker measures per call anyway.
                "feed_max_attempts": 1,
            }
        )
        breaker = _breaker(settings, args.cooldown_seconds)
        client = FeedClient(feed, breaker=breaker)
        try:
            hung = TickLog()
            for _ in range(threshold):
                await tick(client, hung)
            hits_at_open = served.hits

            after = TickLog()
            for _ in range(args.ticks_while_open):
                await tick(client, after)

            result.measure(
                timeout_seconds=timeout,
                timeout_ticks=hung.as_dict(),
                timeout_state_after_threshold=str(breaker.state),
                timeout_requests_while_open=served.hits - hits_at_open,
                timeout_open_ticks=after.as_dict(),
            )
            result.check(
                equals(
                    "a hung upstream is labelled a timeout, not a transport error",
                    threshold,
                    (hung.kinds or {}).get(str(FeedFailureKind.TIMEOUT), 0),
                ),
                equals(
                    "the breaker opened on hung connections too",
                    str(CircuitState.OPEN),
                    str(breaker.state),
                ),
                equals(
                    "no further connection was opened to the hung upstream",
                    0,
                    served.hits - hits_at_open,
                ),
            )
        finally:
            await client.aclose()


async def _service_level(
    settings: Settings, result: ScenarioResult, args: argparse.Namespace
) -> None:
    """Part 3: the collector's own handler, breaker and all."""
    threshold = settings.feed_breaker_failure_threshold
    with (
        serve_feed(status_code=503) as served,
        settings_override(
            TP_FEED_URL=served.url,
            TP_FEED_MAX_ATTEMPTS="1",
            TP_FEED_BREAKER_RESET_SECONDS=str(args.cooldown_seconds),
        ),
    ):
        async with collector_app.router.lifespan_context(collector_app):
            transport = httpx.ASGITransport(app=collector_app)
            async with httpx.AsyncClient(
                transport=transport, base_url="http://collector.local"
            ) as client:
                statuses: list[int] = []
                for _ in range(threshold):
                    statuses.append((await client.post("/collect", timeout=60.0)).status_code)
                blocked = await client.post("/collect", timeout=60.0)

    result.measure(
        collect_statuses_while_failing=statuses,
        collect_status_once_open=blocked.status_code,
        collect_retry_after=blocked.headers.get("Retry-After"),
    )
    result.check(
        equals(
            "a tick that cannot reach the agency is reported as a bad gateway",
            [502] * threshold,
            statuses,
        ),
        equals(
            "a tick arriving at an open breaker is reported as unavailable",
            503,
            blocked.status_code,
        ),
        holds(
            "the 503 tells the scheduler how long to wait",
            blocked.headers.get("Retry-After", "").isdigit(),
            f"Retry-After: {blocked.headers.get('Retry-After')!r}",
        ),
    )


def run(settings: Settings, args: argparse.Namespace) -> ScenarioResult:
    result = ScenarioResult(scenario=NAME, question=QUESTION)
    result.measure(started=datetime.now(UTC).isoformat())

    async def everything() -> None:
        await _unavailable_upstream(settings, result, args)
        await _hung_upstream(settings, result, args)
        await _service_level(settings, result, args)

    asyncio.run(everything())

    result.note(
        f"The breaker cooldown is shortened from {settings.feed_breaker_reset_seconds:.0f}s to"
        f" {args.cooldown_seconds:.0f}s so a run finishes in seconds. The breaker counts whole"
        " calls rather than retry attempts, so neither the cooldown nor the retry budget"
        " changes the behaviour being measured -- only how long it takes to observe.",
        "The upstream here is a loopback server that counts its own requests. That count,"
        " not the breaker's reported state, is what shows the calls really stopped.",
    )
    return result


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="chaos.feed_failure",
        description=QUESTION,
        epilog=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--cooldown-seconds",
        type=float,
        default=3.0,
        metavar="S",
        help="breaker cooldown to use instead of the configured one (default: 3)",
    )
    parser.add_argument(
        "--ticks-while-open",
        type=int,
        default=5,
        metavar="N",
        help="ticks to attempt while the breaker is open (default: 5)",
    )
    return add_common_arguments(parser)


def main(argv: list[str] | None = None) -> int:
    return scenario_main(run, build_parser(), name=NAME, service_name=SERVICE_NAME, argv=argv)


if __name__ == "__main__":  # pragma: no cover - module entry point
    raise SystemExit(main())

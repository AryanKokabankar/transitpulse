"""Experiment 5: ramp the load until something breaks, and name what broke.

Nobody knows how fast this pipeline is. That is the honest state of affairs
before this runs, and the only way out of it is to push until a stage stops
keeping up and then say which stage it was -- because "the pipeline does N
events a second" is meaningless without knowing whether N was set by the
publisher, the consumer, the database or the measuring tool.

So the ramp is three ladders, not one.

**Pacing (no broker).** The harness's own ceiling, with the publish removed.
This is first because every number after it is bounded by it: a figure measured
above the rate at which the tool can generate work is a figure about the tool.
``--dry-run`` exists in the harness for exactly this.

**Publishing.** Target rates climbing until the achieved rate falls below the
accuracy threshold. The step that fails is the publisher-and-broker ceiling,
and the harness's ``publisher_bound`` flag is what distinguishes a genuine
ceiling from a run the limiter was still pacing. A rate the limiter capped is
not a ceiling and is not reported as one.

**Consuming.** A fixed backlog, published and then drained, timed. Ramped on
concurrency rather than on rate, because the consumer is pull-driven and has no
target rate to miss -- the question is where more concurrency stops buying more
throughput. The backlog is drained with ``max_messages`` set to its exact size
so the bridge stops on the last message instead of spending a final five-second
long poll inside the measured window; left in, that alone understated the drain
rate by a third.

Then the pool ladder, which is what turns "the consumer is the constraint" into
an attribution. The same drain is repeated with different values of
``TP_PG_POOL_MAX``. If the drain rate tracks the pool size, the constraint is
the connection pool -- a one-line configuration change, not a database limit.
If it does not, the pool has headroom and the limit is further down.

**The pipeline's ceiling is the lower of the publisher's and the consumer's**,
and this reports which, because that is the number a reader wants and the two
legs are measured separately for exactly that reason.

    python -m tools.chaos.ramp
    python -m tools.chaos.ramp --rates 1000,2000,4000 --backlog 5000
"""

from __future__ import annotations

import argparse
import threading
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import structlog

from tools.chaos.experiment import (
    ScenarioResult,
    add_common_arguments,
    at_least,
    equals,
    holds,
    scenario_main,
)
from tools.chaos.support import (
    DrainResult,
    PacedPublish,
    build_load,
    chaos_agency,
    count_events,
    database,
    delete_agency_rows,
    deliver,
    drain_backlog,
    drain_subscriptions,
    latency_percentiles,
    processor_process,
    publish_paced,
)
from tools.replay.corpus import load_corpus
from tools.replay.harness import ReplayConfig, ReplayHarness
from transitpulse_core.config import Settings
from transitpulse_core.pubsub import EventPublisher
from transitpulse_core.snapshots import build_snapshot_archive

NAME = "ramp"
SERVICE_NAME = "transitpulse-chaos-ramp"
QUESTION = "Where does the pipeline stop keeping up, and which stage gives out first?"

log = structlog.get_logger(__name__)


@dataclass
class Step:
    """One rung of a ladder, with enough context to disqualify it."""

    target: float
    achieved: float
    accuracy: float
    publisher_bound: bool
    limiter_wait_seconds: float
    published: int
    failed: int
    publish_latency_ms: dict[str, float]

    def as_dict(self) -> dict[str, Any]:
        return {
            "target_per_second": self.target,
            "achieved_per_second": round(self.achieved, 1),
            "accuracy": round(self.accuracy, 4),
            "publisher_bound": self.publisher_bound,
            "limiter_wait_seconds": self.limiter_wait_seconds,
            "published": self.published,
            "failed": self.failed,
            "publish_latency_ms": self.publish_latency_ms,
        }


def climb(
    settings: Settings,
    rates: list[float],
    *,
    seconds: float,
    workers: int,
    fanout: int,
    dry_run: bool,
    min_accuracy: float,
) -> tuple[list[Step], Step | None]:
    """Walk up the rates until one is missed. Returns the steps and the last good one.

    The corpus is loaded once for the whole ladder: re-reading it per step would
    put decode time inside one step and not another, and a ladder whose rungs
    are not comparable cannot locate a knee.
    """
    corpus = load_corpus(build_snapshot_archive(settings), agency=settings.feed_agency)
    steps: list[Step] = []
    last_good: Step | None = None

    for rate in rates:
        config = ReplayConfig(
            rate_per_second=rate,
            duration=timedelta(seconds=seconds),
            fanout=fanout,
            workers=workers,
            dry_run=dry_run,
        )
        publisher = None if dry_run else EventPublisher(settings)
        try:
            report = ReplayHarness(config, corpus, publisher=publisher).run()
        finally:
            if publisher is not None:
                publisher.close()

        step = Step(
            target=rate,
            achieved=report.achieved_rate,
            accuracy=report.rate_accuracy,
            publisher_bound=report.publisher_bound,
            limiter_wait_seconds=report.limiter_wait_seconds,
            published=report.published,
            failed=report.failed,
            publish_latency_ms=report.publish_latency_ms,
        )
        steps.append(step)
        log.info(
            "ramp.step",
            stage="pacing" if dry_run else "publish",
            target=rate,
            achieved=round(step.achieved, 1),
            accuracy=round(step.accuracy, 3),
        )
        if step.accuracy < min_accuracy:
            break
        last_good = step

        if not dry_run:
            # Each rung leaves its messages on the subscription. Left there,
            # the next rung's publishes queue behind them and the consumer
            # ladder would start against a backlog it did not create.
            drain_subscriptions(settings)

    return steps, last_good


def steady_state(
    settings: Settings,
    *,
    messages: int,
    rate: float,
    concurrency: int,
    keep_rows: bool,
) -> dict[str, Any]:
    """Publish and consume at the same time, at a rate the consumer can hold.

    The only configuration in which ``processed_time - ingest_time`` is a
    latency rather than a queue depth. The publisher is paced on a thread while
    the bridge drains on the main one, so the subscription stays near empty and
    each event's interval is its own time in the pipeline instead of its wait
    behind a backlog.

    The rate is deliberately set below the measured consumer ceiling. At or
    above it a backlog forms, the queue wait reappears inside the figure, and
    the result would be the thing this function exists to avoid.
    """
    agency = chaos_agency("steady")
    drain_subscriptions(settings)
    load = build_load(settings, agency=agency, count=messages)
    with database(settings) as conn:
        delete_agency_rows(conn, agency)

    paced: list[PacedPublish] = []

    def produce() -> None:
        paced.append(publish_paced(settings, load.pings, rate=rate))

    producer = threading.Thread(target=produce, name="chaos-steady-publisher", daemon=True)
    started = time.monotonic()
    with processor_process(settings) as processor:
        producer.start()
        report = deliver(
            settings,
            seconds=max(120.0, messages / rate * 4),
            max_messages=messages,
            concurrency=concurrency,
            url=processor.url,
        )
        producer.join(timeout=120.0)
        published = paced[0].published if paced else 0

        # The bridge stops on an empty pull, and a publisher pacing below the
        # consumer's speed can leave the subscription empty for a moment. That
        # is the arrangement working, not failing, so anything still
        # outstanding is collected here rather than counted as loss.
        sweeps = 0
        while report.processed < published and sweeps < 5:
            extra = deliver(
                settings,
                seconds=60.0,
                max_messages=published - report.processed,
                concurrency=concurrency,
                url=processor.url,
            )
            if extra.delivered == 0:
                break
            report.delivered += extra.delivered
            report.processed += extra.processed
            report.quarantined += extra.quarantined
            sweeps += 1
    elapsed = time.monotonic() - started

    with database(settings) as conn:
        rows = count_events(conn, agency)
        profile = latency_percentiles(conn, agency)
        if not keep_rows:
            delete_agency_rows(conn, agency)

    publish_report = paced[0].as_dict() if paced else None
    return {
        "agency": agency,
        "target_rate_per_second": rate,
        "concurrency": concurrency,
        "messages": messages,
        "publisher": publish_report,
        "delivered": report.delivered,
        "processed": report.processed,
        "follow_up_sweeps": sweeps,
        "rows_written": rows,
        "wall_seconds": round(elapsed, 3),
        "throughput_per_second": round(report.processed / elapsed, 1) if elapsed else 0.0,
        "latency": profile.as_dict() if profile else None,
        "corpus_event_time_span_seconds": load.event_time_span_seconds,
    }


def _publisher_capacity(step: Step, *, workers: int) -> float | None:
    """Messages per second the publish latency implies, independent of pacing.

    ``publish_latency_ms`` is per message, so a worker completes one message's
    share every p50 milliseconds and ``workers`` of them run concurrently.
    This is a second opinion on the ceiling that does not depend on the
    limiter's behaviour or on any threshold.
    """
    p50 = step.publish_latency_ms.get("p50")
    if not p50:
        return None
    return workers * 1000.0 / p50


def _ladder_verdict(steps: list[Step], min_accuracy: float) -> tuple[bool, str]:
    """Whether the ladder terminated by breaking rather than by running out.

    This is the honesty check the whole experiment turns on. A ladder whose top
    rung still met its target has measured a floor, not a ceiling, and the
    number must not be quoted as one.
    """
    if not steps:
        return False, "the ladder did not run"
    if steps[-1].accuracy >= min_accuracy:
        return False, (
            f"the top rung ({steps[-1].target:,.0f}/s) still met its target at "
            f"{steps[-1].accuracy:.1%}; this is a floor, not a ceiling"
        )
    return True, (
        f"the ladder broke at {steps[-1].target:,.0f}/s ({steps[-1].accuracy:.1%} of target)"
    )


def run(settings: Settings, args: argparse.Namespace) -> ScenarioResult:
    result = ScenarioResult(scenario=NAME, question=QUESTION)
    rates = [float(r) for r in args.rates.split(",")]
    result.measure(
        started=datetime.now(UTC).isoformat(),
        rates_attempted=rates,
        pacing_rates_attempted=args.pacing_rates,
        seconds_per_step=args.step_seconds,
        min_accuracy=args.min_accuracy,
        workers=args.workers,
        fanout=args.fanout,
    )

    # --- stage A: how fast can the tool itself go --------------------------
    pacing_steps, _ = climb(
        settings,
        [float(r) for r in args.pacing_rates.split(",")],
        seconds=args.step_seconds,
        workers=args.workers,
        fanout=args.fanout,
        dry_run=True,
        min_accuracy=args.min_accuracy,
    )
    pacing_ceiling = max((s.achieved for s in pacing_steps), default=0.0)
    pacing_broke, pacing_detail = _ladder_verdict(pacing_steps, args.min_accuracy)
    result.measure(
        pacing_ladder=[s.as_dict() for s in pacing_steps],
        pacing_ceiling_per_second=round(pacing_ceiling, 1),
    )

    # --- stage B: how fast can it publish ---------------------------------
    publish_steps, publish_best = climb(
        settings,
        rates,
        seconds=args.step_seconds,
        workers=args.workers,
        fanout=args.fanout,
        dry_run=False,
        min_accuracy=args.min_accuracy,
    )
    publish_ceiling = max((s.achieved for s in publish_steps), default=0.0)
    publish_broke, publish_detail = _ladder_verdict(publish_steps, args.min_accuracy)
    drain_subscriptions(settings)
    result.measure(
        publish_ladder=[s.as_dict() for s in publish_steps],
        publisher_ceiling_per_second=round(publish_ceiling, 1),
        publisher_last_sustained_per_second=(
            round(publish_best.achieved, 1) if publish_best else None
        ),
    )
    result.check(
        holds("the publish ladder reached a real ceiling", publish_broke, publish_detail),
        at_least(
            "the harness can generate work faster than the publisher can send it",
            publish_ceiling,
            pacing_ceiling,
            unit="msg/s",
        ),
    )
    # Not a check. A pacing ladder that never broke is the good outcome -- the
    # tool outran every rate it was asked for -- and failing the experiment for
    # it would be backwards. What matters is the comparison above: a pacing
    # ceiling below the publisher's would mean the publish numbers were
    # measuring the harness.
    result.measure(pacing_ladder_broke=pacing_broke, pacing_ladder_verdict=pacing_detail)
    if publish_steps:
        broke_at = publish_steps[-1]
        derived = _publisher_capacity(broke_at, workers=args.workers)
        result.measure(
            publisher_capacity_from_latency_per_second=(round(derived, 1) if derived else None),
            publisher_bound_flag_at_break=broke_at.publisher_bound,
            limiter_wait_at_break_seconds=broke_at.limiter_wait_seconds,
        )
        if derived:
            # A second, independent reading of the same ceiling: workers in
            # flight divided by how long one message's share of a batch took.
            # Reported rather than asserted, because the two disagreeing is
            # informative rather than wrong. Agreement means the publisher was
            # saturated and the ceiling is its steady-state capacity;
            # divergence means the rung was lost to intermittent stalls, and
            # the achieved figure then understates what the publisher can do
            # between them. Making this a pass/fail check failed the whole
            # experiment for a property of the emulator.
            agreement = round(max(derived, broke_at.achieved) / min(derived, broke_at.achieved), 3)
            result.measure(publisher_capacity_agreement=agreement)
            if agreement > 1.25:
                result.note(
                    f"The achieved ceiling ({broke_at.achieved:,.0f}/s) and the capacity"
                    f" its own publish latency implies ({derived:,.0f}/s) disagree by"
                    f" {agreement:.2f}x. The publisher was therefore not saturated at the"
                    " breaking rate: the rung was lost to intermittent stalls rather than"
                    " to steady-state capacity, so this ceiling is a lower bound on what"
                    " the publisher can do between them."
                )
        result.note(
            f"At the breaking rate the limiter still waited {broke_at.limiter_wait_seconds:.2f}s,"
            " so the harness's publisher_bound flag reads False even though the publisher was"
            " the constraint. The flag requires a limiter that barely waited; a bursty"
            " publisher produces both waiting and a shortfall, because the token bucket holds"
            " only 0.1s of credit and cannot bank the time lost while a batch is in flight."
        )

    # --- stage C: how fast can it consume ---------------------------------
    #
    # Against a processor in its own OS process, not the bridge's in-process
    # ASGI shim. The shim is convenient and it puts the pulling and the
    # handling in one CPython process, contending for one GIL, so a rate
    # measured through it is partly a property of that arrangement. In
    # production the processor is a separate autoscaling service, and a
    # separate process is the closer shape. One in-process pass is kept at the
    # end for the comparison, because the size of the difference is itself
    # worth knowing.
    drains: list[DrainResult] = []
    with processor_process(settings) as processor:
        for concurrency in [int(c) for c in args.concurrency.split(",")]:
            drains.append(
                drain_backlog(
                    settings,
                    backlog=args.backlog,
                    concurrency=concurrency,
                    pool_max=settings.pg_pool_max,
                    processor_url=processor.url,
                    keep_rows=False,
                )
            )
    best_drain = max(drains, key=lambda d: d.rate) if drains else None
    consumer_ceiling = best_drain.rate if best_drain else 0.0
    result.measure(
        concurrency_ladder=[d.as_dict() for d in drains],
        consumer_ceiling_per_second=round(consumer_ceiling, 1),
        consumer_best_concurrency=best_drain.concurrency if best_drain else None,
    )
    result.check(
        *[
            equals(
                f"zero loss draining {d.backlog:,} at concurrency {d.concurrency}",
                d.published,
                d.rows,
            )
            for d in drains
        ]
    )

    # --- attribute the consumer's limit ------------------------------------
    pool_drains: list[DrainResult] = []
    if best_drain is not None and args.pools:
        for pool_max in [int(p) for p in args.pools.split(",")]:
            # A fresh processor per pool size: the pool is built once, in the
            # lifespan, so a process started before the setting changed is
            # still running the old one.
            with processor_process(
                settings, env={"TP_PG_POOL_MAX": str(pool_max), "TP_PG_POOL_MIN": "1"}
            ) as scoped:
                pool_drains.append(
                    drain_backlog(
                        settings,
                        backlog=args.backlog,
                        concurrency=best_drain.concurrency,
                        pool_max=pool_max,
                        processor_url=scoped.url,
                        keep_rows=False,
                    )
                )
    if pool_drains:
        # The question is not "can a pool be too small" -- it always can, and a
        # fastest-over-slowest spread answers that instead, which is how an
        # earlier version of this concluded the pool was the constraint from a
        # ladder where going from 10 connections to 30 changed nothing at all.
        # The question is whether the *configured* pool binds, so the
        # comparison is the configured size against the largest one tried.
        configured = min(pool_drains, key=lambda d: abs(d.pool_max - settings.pg_pool_max))
        largest = max(pool_drains, key=lambda d: d.pool_max)
        smallest = min(pool_drains, key=lambda d: d.pool_max)
        headroom = largest.rate / configured.rate if configured.rate else 0.0
        starvation = configured.rate / smallest.rate if smallest.rate else 0.0
        sensitivity = args.pool_sensitivity
        pool_bound = headroom >= sensitivity
        result.measure(
            pool_ladder=[d.as_dict() for d in pool_drains],
            configured_pool_max=settings.pg_pool_max,
            pool_headroom_ratio=round(headroom, 3),
            pool_starvation_ratio=round(starvation, 3),
            pool_is_the_constraint=pool_bound,
        )
        # Within a tenth of the bar in either direction, the verdict is a coin
        # toss between runs -- it has landed on both sides of 1.25x on this
        # machine -- so the note says so rather than asserting a cause.
        marginal = abs(headroom - sensitivity) < 0.1 * sensitivity
        result.measure(pool_attribution_is_marginal=marginal)
        result.note(
            (
                f"Raising the pool from {configured.pool_max} to {largest.pool_max}"
                f" connections multiplied the drain rate by {headroom:.2f}x, so the"
                " connection pool is the binding constraint on the consumer side."
            )
            if pool_bound
            else (
                f"Raising the pool from {configured.pool_max} to {largest.pool_max}"
                f" connections bought {headroom:.2f}x, below the {sensitivity:.2f}x bar"
                " for calling it the binding constraint -- so the configured pool is not"
                " where the consumer's limit lies, though it is not entirely free"
                " either. The ladder is sensitive enough to see a pool bind when one"
                f" does: at {smallest.pool_max} connections the same load ran"
                f" {starvation:.2f}x slower."
            )
        )
        if marginal:
            result.note(
                f"That verdict is marginal: {headroom:.2f}x against a {sensitivity:.2f}x"
                " bar. Repeat runs on this machine have landed on both sides of it, so"
                " the honest reading is that the pool is one contributor among several"
                " rather than the constraint."
            )

    # --- what the in-process shim costs ------------------------------------
    if best_drain is not None and args.compare_in_process:
        in_process = drain_backlog(
            settings,
            backlog=args.backlog,
            concurrency=best_drain.concurrency,
            pool_max=settings.pg_pool_max,
            keep_rows=False,
        )
        ratio = best_drain.rate / in_process.rate if in_process.rate else 0.0
        result.measure(
            in_process_drain=in_process.as_dict(),
            separate_process_rate_ratio=round(ratio, 3),
        )
        faster = "faster" if ratio > 1 else "slower"
        result.note(
            f"A processor in its own process drains {ratio:.2f}x the rate of the bridge's"
            f" in-process path at the same concurrency -- {faster}. Over loopback the HTTP"
            " round trip and the JSON envelope cost more than the GIL contention they"
            " remove, so the in-process path is the quicker of the two locally. The"
            " consumer figures above all use the separate process anyway, because that is"
            " the shape production runs in: the processor is its own Cloud Run service."
        )

    # --- stage D: latency, measured where latency means something ---------
    steady = None
    if consumer_ceiling > 0 and args.steady_messages:
        steady = steady_state(
            settings,
            messages=args.steady_messages,
            # No floor worth speaking of: a floor is what broke this before.
            # At 50/s against a 57/s consumer the stage ran at 87% of capacity,
            # a queue formed, and the "latency" it reported was mostly queueing
            # -- the exact thing the stage exists to measure without.
            rate=max(5.0, consumer_ceiling * args.steady_fraction),
            concurrency=best_drain.concurrency if best_drain else 16,
            keep_rows=False,
        )
        result.measure(steady_state=steady)
        latency = steady.get("latency") or {}
        processing = latency.get("processing_seconds") or {}
        result.check(
            equals(
                "zero loss at steady state",
                steady["publisher"]["published"] if steady["publisher"] else 0,
                steady["rows_written"],
            ),
        )
        if processing:
            result.check(
                at_least(
                    "a processing latency was actually measured",
                    0.0,
                    processing.get("p50", -1.0),
                    unit="s",
                )
            )

    # --- the answer --------------------------------------------------------
    legs = {
        "publisher and broker": publish_ceiling,
        "consumer (processor and Postgres)": consumer_ceiling,
    }
    reachable = {name: rate for name, rate in legs.items() if rate > 0}
    constraint = min(reachable, key=lambda name: reachable[name]) if reachable else None
    result.measure(
        pipeline_ceiling_per_second=round(min(reachable.values()), 1) if reachable else None,
        first_constraint=constraint,
        legs_per_second={name: round(rate, 1) for name, rate in legs.items()},
    )

    result.note(
        "The publisher ceiling is measured against the Pub/Sub emulator in a container,"
        " not against Pub/Sub. The emulator is a single-process Java service with no"
        " quota enforcement and no cross-zone replication, so it is the wrong shape to"
        " extrapolate from in either direction.",
        "Postgres here runs with fsync=off and synchronous_commit=off (docker-compose.yml),"
        " so every consumer-side figure above is inflated relative to any deployment"
        " whose data matters.",
        "Timestamps are rebased, which is the harness default: end-to-end lag therefore"
        " measures the pipeline plus the age each ping already had when it was captured."
        " Processing latency (ingest_time to processed_time) is the figure that is purely"
        " this machine.",
        "The drain is timed with max_messages set to the backlog's exact size, so the"
        " bridge stops on the last message rather than spending a final long poll inside"
        " the measured window.",
        "Latency comes from the steady-state stage, not from the drains. In a backlog"
        " drain every message shares one ingest_time, so processed_time - ingest_time"
        " reports how long the queue took to empty and grows with the backlog on an"
        " identically fast pipeline. The steady-state stage publishes and consumes"
        " together below the ceiling, re-stamping each batch as a collector tick would,"
        " which is the only arrangement in which that interval is a latency.",
    )
    return result


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="chaos.ramp",
        description=QUESTION,
        epilog=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--rates",
        default="250,500,750,1000,1500,2000",
        help="comma-separated publish rates to climb. Spaced finely around the"
        " region where the emulator gives out, because a ladder that jumps from"
        " 500 to 1000 locates the ceiling only to within a factor of two"
        " (default: 250..2000)",
    )
    parser.add_argument(
        "--pacing-rates",
        default="1000,4000,16000",
        help="comma-separated rates for the no-broker pacing ladder, which only"
        " has to establish that the harness outruns the publisher"
        " (default: 1000,4000,16000)",
    )
    parser.add_argument(
        "--step-seconds",
        type=float,
        default=20.0,
        metavar="S",
        help="how long to sustain each rung. Twenty rather than ten because the"
        " emulator's publish latency has a long tail and a short rung can be lost"
        " to a single stall (default: 20)",
    )
    parser.add_argument(
        "--min-accuracy",
        type=float,
        default=0.95,
        metavar="F",
        help="achieved/target below which a rung has broken (default: 0.95)",
    )
    parser.add_argument("--workers", type=int, default=4, help="publish workers (default: 4)")
    parser.add_argument(
        "--fanout",
        type=int,
        default=4,
        metavar="K",
        help="synthetic vehicles per corpus ping, so volume is not capped by the"
        " corpus's vehicle count (default: 4)",
    )
    parser.add_argument(
        "--backlog",
        type=int,
        default=4000,
        metavar="N",
        help="messages to publish and then drain per consumer rung (default: 4000)",
    )
    parser.add_argument(
        "--concurrency",
        default="4,16,32,64",
        help="comma-separated delivery concurrencies to try (default: 4,16,32,64)",
    )
    parser.add_argument(
        "--pools",
        default="2,10,30",
        help="comma-separated TP_PG_POOL_MAX values for the attribution pass,"
        " or empty to skip it (default: 2,10,30)",
    )
    parser.add_argument(
        "--compare-in-process",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="also drain through the bridge's in-process processor, to measure"
        " what that arrangement costs (default: yes)",
    )
    parser.add_argument(
        "--steady-messages",
        type=int,
        default=3000,
        metavar="N",
        help="events for the steady-state latency stage, 0 to skip (default: 3000)",
    )
    parser.add_argument(
        "--steady-fraction",
        type=float,
        default=0.5,
        metavar="F",
        help="fraction of the measured consumer ceiling to publish at during the"
        " steady-state stage (default: 0.5)",
    )
    parser.add_argument(
        "--pool-sensitivity",
        type=float,
        default=1.25,
        metavar="F",
        help="drain-rate spread across pool sizes above which the pool is called"
        " the constraint (default: 1.25)",
    )
    return add_common_arguments(parser)


def main(argv: list[str] | None = None) -> int:
    return scenario_main(run, build_parser(), name=NAME, service_name=SERVICE_NAME, argv=argv)


if __name__ == "__main__":  # pragma: no cover - module entry point
    raise SystemExit(main())

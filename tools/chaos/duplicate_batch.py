"""Experiment 4: republish an identical batch and watch the row count not move.

The pipeline has no transaction spanning its two sinks and does not want one:
every write is idempotent on a deterministic key instead, and retries are
allowed to happen freely (ADR-005). That design is only sound if the key really
collapses a repeat, and "really" means the row count after the second pass
equals the row count after the first. Not approximately, not modulo a race --
equal.

It is worth measuring rather than assuming because the key is easy to break
without noticing. It covers ``agency | vehicle_id | event_time`` and
deliberately excludes ``ingest_time``, which differs on every replay; adding
``ingest_time`` to it would be a one-word change that passed every other test
in the project while making replays write duplicate history for ever.

The two passes are sequential on purpose. Run concurrently, the pair could
interleave so that both inserts were in flight before either committed,
neither conflicted, and the experiment would have measured a race instead of a
key.

Three counts, because they fail differently:

* ``vehicle_events`` must not grow -- ``ON CONFLICT DO NOTHING`` on the key.
* ``vehicle_live`` must not grow and must not move backwards -- the upsert's
  ``WHERE excluded.event_time > vehicle_live.event_time`` guard, which is what
  stops a late duplicate dragging a live position back in time.
* The ``deduped`` counter must rise, because a pipeline that silently discarded
  the second pass would also leave the row counts flat. Flat counts alone do
  not distinguish deduplication from loss, which is the whole reason the two
  are counted separately.

    python -m tools.chaos.duplicate_batch --messages 1000
"""

from __future__ import annotations

import argparse
from datetime import UTC, datetime

import structlog

from tools.chaos.experiment import (
    ScenarioResult,
    add_common_arguments,
    at_least,
    equals,
    scenario_main,
)
from tools.chaos.support import (
    await_metric_export,
    build_load,
    chaos_agency,
    count_events,
    count_live,
    count_quarantine_since,
    database,
    delete_agency_rows,
    deliver,
    drain_subscriptions,
    latency_percentiles,
    prometheus_value,
    publish,
)
from transitpulse_core.config import Settings

NAME = "duplicate-batch"
SERVICE_NAME = "transitpulse-chaos-duplicate-batch"
QUESTION = "Does an exact duplicate batch write nothing, and is it counted as a duplicate?"

log = structlog.get_logger(__name__)

_DEDUPED = "sum(transitpulse_events_deduped_total)"


def run(settings: Settings, args: argparse.Namespace) -> ScenarioResult:
    result = ScenarioResult(scenario=NAME, question=QUESTION)
    agency = chaos_agency("dup")
    started_at = datetime.now(UTC)
    drain_subscriptions(settings)

    # Built once and published twice. Not regenerated for the second pass:
    # re-deriving the batch would risk a fresh timestamp somewhere and make the
    # keys differ, which is exactly the bug this experiment exists to catch.
    load = build_load(settings, agency=agency, count=args.messages)
    keys = {ping.idempotency_key for ping in load.pings}
    vehicles = {ping.vehicle_id for ping in load.pings}

    with database(settings) as conn:
        delete_agency_rows(conn, agency)

    deduped_before = prometheus_value(_DEDUPED) or 0.0

    first_published = publish(settings, load.pings)
    first_bridge = deliver(settings, seconds=args.drain_seconds, concurrency=args.concurrency)
    with database(settings) as conn:
        rows_after_first = count_events(conn, agency)
        live_after_first = count_live(conn, agency)

    second_published = publish(settings, load.pings)
    second_bridge = deliver(settings, seconds=args.drain_seconds, concurrency=args.concurrency)
    with database(settings) as conn:
        rows_after_second = count_events(conn, agency)
        live_after_second = count_live(conn, agency)
        quarantined = count_quarantine_since(conn, started_at)
        latency = latency_percentiles(conn, agency)
        if not args.keep_rows:
            delete_agency_rows(conn, agency)

    result.measure(
        agency=agency,
        batch_size=load.size,
        distinct_keys=len(keys),
        distinct_vehicles=len(vehicles),
        first_published=first_published,
        first_delivered=first_bridge.delivered,
        first_processed=first_bridge.processed,
        rows_after_first=rows_after_first,
        live_after_first=live_after_first,
        second_published=second_published,
        second_delivered=second_bridge.delivered,
        second_processed=second_bridge.processed,
        rows_after_second=rows_after_second,
        live_after_second=live_after_second,
        rows_added_by_the_duplicate=rows_after_second - rows_after_first,
        quarantined_during_run=quarantined,
    )
    if latency is not None:
        result.measure(latency=latency.as_dict())

    result.check(
        equals("the first pass wrote one row per distinct key", len(keys), rows_after_first),
        equals(
            "the second pass was delivered in full, not skipped",
            second_published,
            second_bridge.delivered,
        ),
        equals(
            "the duplicate batch added no history rows",
            0,
            rows_after_second - rows_after_first,
        ),
        equals("the duplicate batch added no live rows", live_after_first, live_after_second),
        equals("one live row per distinct vehicle", len(vehicles), live_after_second),
        equals("the duplicates were not quarantined", 0, quarantined),
    )

    # --- and it must be visible as deduplication, not as silence ----------
    await_metric_export()
    deduped_after = prometheus_value(_DEDUPED)
    delta = None if deduped_after is None else deduped_after - deduped_before
    result.measure(
        deduped_counter_before=deduped_before,
        deduped_counter_after=deduped_after,
        deduped_counter_delta=delta,
    )
    if delta is None:
        result.note(
            "Prometheus was not reachable, so the deduplication counter could not be"
            " read; the flat row counts above still hold, but on their own they cannot"
            " distinguish deduplication from loss."
        )
    else:
        result.check(
            at_least(
                "the deduplication counter rose, so the rows were collapsed and not lost",
                1.0,
                delta,
            )
        )

    result.note(
        "The batch is published twice and delivered twice, sequentially. Concurrently the"
        " pair could interleave so that neither insert saw the other, which would measure"
        " a race rather than the idempotency key.",
        "The key excludes ingest_time by design (ADR-005), which is what makes a replay of"
        " the archived corpus safe; this run is the end-to-end demonstration of that, and"
        " a unit test pins the key's composition.",
    )
    return result


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="chaos.duplicate_batch",
        description=QUESTION,
        epilog=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--messages",
        type=int,
        default=1000,
        metavar="N",
        help="events in the batch that is published twice (default: 1000)",
    )
    parser.add_argument(
        "--drain-seconds",
        type=float,
        default=240.0,
        metavar="S",
        help="cap on each delivery phase (default: 240)",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=16,
        metavar="N",
        help="deliveries in flight (default: 16)",
    )
    return add_common_arguments(parser)


def main(argv: list[str] | None = None) -> int:
    return scenario_main(run, build_parser(), name=NAME, service_name=SERVICE_NAME, argv=argv)


if __name__ == "__main__":  # pragma: no cover - module entry point
    raise SystemExit(main())

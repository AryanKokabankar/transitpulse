"""Experiment 1: kill the processor mid-stream and count what survives.

The claim under test is the one a message broker is bought for: that the queue
holds the backlog while the consumer is gone, and that every event still lands
once it comes back. It is also the claim most easily asserted and least often
measured, because on a healthy day it is indistinguishable from having no
broker at all.

    publish N -> consume some -> kill -> keep delivering into the hole
              -> restart -> drain -> replay the dead letters -> count

Three parts of the shape are deliberate.

**The processor runs in its own OS process and is terminated abruptly.** The
bridge's default in-process ASGI path cannot be killed without ending the
experiment, and a clean shutdown is not the failure being tested: a Cloud Run
instance disappears mid-request with its shutdown hooks unrun.

**A small backlog is left behind deliberately, and then hammered.** Delivery
continues while the processor is absent, which is what a push subscription
does and what consumes the delivery budget: every refused POST is a nack, and
the fifth one moves the message to the dead-letter topic. Running the pipeline
past the point where the broker gives up is the only place the replayer job
matters, so the experiment is arranged to get there.

That arrangement is not incidental. The first version refused deliveries
against the whole remaining backlog for eight seconds and dead-lettered
nothing: a refused connection costs about two seconds on loopback, and the
broker hands out fresh messages rather than the same ones, so every message
collected one refusal and none came near five. Holding back a bounded number
and refusing until the subscription has emptied is what makes the budget
genuinely run out -- and the check below insists it did, because a run that
dead-lettered nothing would have reported zero loss while never testing the
path that loses data.

**Zero loss is a count, and it is the last thing checked.** ``rows ==
published``, read from Postgres, after the replayer has had its turn. Anything
short of that is loss, whatever the logs say.

    python -m tools.chaos.kill_processor --messages 2000
"""

from __future__ import annotations

import argparse
import time
from datetime import UTC, datetime

import structlog

from services.jobs import dlq_replayer
from tools.chaos.experiment import (
    ScenarioResult,
    add_common_arguments,
    at_least,
    equals,
    scenario_main,
)
from tools.chaos.support import (
    build_load,
    chaos_agency,
    count_events,
    count_live,
    count_quarantine_since,
    database,
    dead_letter_depth,
    delete_agency_rows,
    deliver,
    drain_subscriptions,
    processor_process,
    publish,
)
from transitpulse_core.config import Settings

NAME = "kill-processor"
SERVICE_NAME = "transitpulse-chaos-kill-processor"
QUESTION = "Does the queue absorb the backlog, and does every published event still land?"

log = structlog.get_logger(__name__)


def run(settings: Settings, args: argparse.Namespace) -> ScenarioResult:
    result = ScenarioResult(scenario=NAME, question=QUESTION)
    agency = chaos_agency("kill")
    started_at = datetime.now(UTC)

    result.measure(agency=agency, drained_before=drain_subscriptions(settings))

    load = build_load(settings, agency=agency, count=args.messages)
    published = publish(settings, load.pings)
    result.measure(
        requested=load.size,
        published=published,
        corpus_pings=load.corpus_pings,
        corpus_distinct_keys=load.corpus_distinct_keys,
        corpus_event_time_span_seconds=load.event_time_span_seconds,
    )
    result.check(equals("every ping was accepted by the broker", load.size, published))

    with database(settings) as conn:
        delete_agency_rows(conn, agency)

    with processor_process(settings) as processor:
        # --- consume all but a bounded remainder, then pull the floor out --
        consume_first = max(0, published - args.leave_unconsumed)
        before_kill = deliver(
            settings,
            seconds=args.warmup_seconds,
            max_messages=consume_first,
            concurrency=args.concurrency,
            url=processor.url,
        )
        processor.kill()
        killed_at = datetime.now(UTC)
        backlog_at_kill = published - before_kill.processed
        log.info(
            "chaos.processor_killed",
            processed_before_kill=before_kill.processed,
            backlog_at_kill=backlog_at_kill,
        )

        # Deliveries keep arriving at a port with nothing behind it, exactly as
        # a push subscription's would. Repeated until the subscription itself
        # empties, which happens only when every message left on it has
        # exhausted its delivery attempts and the broker has moved it: that is
        # the condition this phase exists to reach, and a fixed duration does
        # not reliably reach it.
        refused = 0
        outage_started = time.monotonic()
        while time.monotonic() - outage_started < args.outage_seconds:
            attempt = deliver(
                settings,
                seconds=args.outage_step_seconds,
                concurrency=max(args.concurrency, args.leave_unconsumed),
                url=processor.url,
            )
            refused += attempt.failed + attempt.returned
            if attempt.delivered == 0 and attempt.failed == 0:
                break
        outage_seconds = time.monotonic() - outage_started

        result.measure(
            processed_before_kill=before_kill.processed,
            left_unconsumed_at_kill=args.leave_unconsumed,
            backlog_at_kill=backlog_at_kill,
            outage_seconds=round(outage_seconds, 2),
            deliveries_refused_during_outage=refused,
        )
        result.check(
            at_least("the processor consumed some of the stream first", 1, before_kill.processed),
            at_least("the queue still held the rest when it died", 1, backlog_at_kill),
            at_least("deliveries were refused while it was down", 1, refused),
            at_least(
                "each held-back message was refused its whole delivery budget",
                backlog_at_kill * settings.max_delivery_attempts,
                refused,
            ),
        )

        # --- bring it back and drain -------------------------------------
        processor.start()
        after_restart = deliver(
            settings,
            seconds=args.drain_seconds,
            concurrency=args.concurrency,
            url=processor.url,
        )
        result.measure(
            processed_after_restart=after_restart.processed,
            processor_starts=processor.starts,
            processor_kills=processor.kills,
        )

        # --- what the broker gave up on ----------------------------------
        dead_lettered = dead_letter_depth(settings)
        replay = dlq_replayer.run(settings)
        recovered = (
            deliver(
                settings,
                seconds=args.drain_seconds,
                concurrency=args.concurrency,
                url=processor.url,
            )
            if replay.republished
            else None
        )
        remaining = dead_letter_depth(settings)
        result.measure(
            dead_lettered=dead_lettered,
            replayer=replay.as_dict(),
            processed_after_replay=recovered.processed if recovered else 0,
            dead_letters_remaining=remaining,
        )
        result.check(
            at_least(
                "the broker gave up on the held-back messages, as intended",
                1,
                dead_lettered,
            ),
            equals(
                "everything the queue still held was dead-lettered, not lost",
                backlog_at_kill,
                dead_lettered,
            ),
            equals("the replayer recovered every dead letter", dead_lettered, replay.republished),
            equals("nothing was left on the dead-letter topic", 0, remaining),
        )

    # --- the only claim that matters -------------------------------------
    with database(settings) as conn:
        rows = count_events(conn, agency)
        live = count_live(conn, agency)
        quarantined = count_quarantine_since(conn, started_at)
        if not args.keep_rows:
            delete_agency_rows(conn, agency)

    result.measure(
        rows_in_vehicle_events=rows,
        rows_in_vehicle_live=live,
        distinct_vehicles=len({p.vehicle_id for p in load.pings}),
        quarantined_during_run=quarantined,
        killed_at=killed_at.isoformat(),
    )
    result.check(
        equals("zero loss: rows written equals events published", published, rows),
        equals("nothing was quarantined", 0, quarantined),
        equals(
            "one live row per distinct vehicle",
            len({p.vehicle_id for p in load.pings}),
            live,
        ),
    )
    result.note(
        "The corpus is filtered to one ping per idempotency key before publishing, so"
        " rows == published is a reachable assertion; a raw multi-snapshot capture"
        " re-sends about a third of its own observations and those deduplicate by"
        " design (experiment 4 measures that deliberately).",
        "The local bridge retries a refused delivery immediately with no backoff, so"
        " the delivery budget is consumed faster here than a real push subscription"
        " would consume it. That makes dead-lettering easier to provoke, which is the"
        " point, but the outage duration is not comparable to a production one.",
    )
    return result


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="chaos.kill_processor",
        description=QUESTION,
        epilog=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--messages", type=int, default=2000, metavar="N", help="events to publish (default: 2000)"
    )
    parser.add_argument(
        "--leave-unconsumed",
        type=int,
        default=40,
        metavar="N",
        help="messages to hold back on the subscription when the processor is"
        " killed, so their delivery budget can be exhausted (default: 40)",
    )
    parser.add_argument(
        "--warmup-seconds",
        type=float,
        default=60.0,
        metavar="S",
        help="cap on the pre-kill delivery phase (default: 60)",
    )
    parser.add_argument(
        "--outage-seconds",
        type=float,
        default=120.0,
        metavar="S",
        help="cap on the outage phase; it normally ends sooner, when the"
        " subscription has emptied into the dead-letter topic (default: 120)",
    )
    parser.add_argument(
        "--outage-step-seconds",
        type=float,
        default=10.0,
        metavar="S",
        help="length of each refused-delivery pass during the outage (default: 10)",
    )
    parser.add_argument(
        "--drain-seconds",
        type=float,
        default=180.0,
        metavar="S",
        help="cap on each post-restart drain (default: 180)",
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

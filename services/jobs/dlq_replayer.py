"""Put dead-lettered messages back on the topic once the fault is fixed.

A message that fails ``max_delivery_attempts`` deliveries is moved by the
broker to the dead-letter topic. That is the end of the automatic path: Pub/Sub
performs the move and then waits, which is the correct behaviour -- whatever
made five attempts fail is not going to be fixed by a sixth -- but it means the
events sit there until something deliberately brings them back. This job is
that something, and it is what lets "nothing is dropped silently" survive an
outage rather than only a bad payload.

**Messages are republished verbatim.** The ping is validated and sent on with
every field exactly as it arrived, including ``event_time`` and
``ingest_time``. Rebasing the timestamps would be the comfortable choice and it
would be a lie twice over: the idempotency key is derived from ``event_time``
(ADR-005), so a shifted key would write a second row for an observation the
pipeline already holds, and the ingest lag histogram would report the replay's
latency instead of the outage's. An event recovered an hour late genuinely
breached the 2s objective an hour ago. The SLI is supposed to say so.

**Validation decides, and a failure is not acked.** Each payload is re-checked
against the current :class:`VehiclePing` contract, which is the whole point of
running this after a fix: a schema bug that quarantined a whole morning of
traffic is corrected in the model, and the same bytes now pass. What still
fails is left on the dead-letter subscription -- not acked, not discarded. The
dead-letter topic is the last copy of those bytes, and a replayer that cleared
what it could not repair would destroy the evidence it exists to preserve.

**Republish is at-least-once; the effect is not.** Each message is acked only
after the broker has acknowledged its republish, so a crash in between
redelivers it and republishes it twice. That is deliberate: the alternative
ordering -- ack first -- loses the message outright on the same crash, and a
duplicate costs nothing because both sinks are idempotent on the key. For the
same reason the publishes are resolved one at a time rather than through
:meth:`EventPublisher.publish_batch`, which reports only a count: acking a
count would ack whichever messages in the batch had failed.

**What survives the broker's move, measured against the emulator:** the
payload, the ordering key and every original attribute, ``traceparent``
included. The message id does not -- the dead-lettered copy gets a new one, and
Pub/Sub documents no attribute carrying the original. That is why the
quarantine reconciliation below matches on the raw payload rather than on
``message_id``, which is the field that looks like it should work.

Reconciliation is best-effort by design, and the counts say so. Most
dead-lettered messages have no quarantine row at all, because the processor's
poison-message guard writes one and acks on the final attempt -- so a message
that reached the dead-letter topic is usually one whose *quarantine write*
also failed (see ``services/processor/main.py``). The rows that do match are
the ones a human quarantined by hand or that an earlier replay left behind, and
marking them is what stops the next operator reprocessing them a second time.

Run through the shared image entrypoint (``TP_SERVICE=job TP_JOB=dlq_replayer``)
on demand rather than on a schedule -- it is an incident-recovery tool, and a
cron that silently replayed a poison message every hour would be a loop -- or
locally with ``python -m services.jobs.dlq_replayer``.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections.abc import Sequence
from concurrent.futures import Future
from dataclasses import dataclass, field

import psycopg
import structlog
from google.api_core import exceptions as gcp_exceptions
from google.cloud import pubsub_v1
from pydantic import ValidationError

from transitpulse_core.config import Settings, get_settings
from transitpulse_core.models import VehiclePing
from transitpulse_core.pubsub import EventPublisher
from transitpulse_core.telemetry import (
    get_metrics,
    get_tracer,
    record_job_success,
    setup_telemetry,
)

SERVICE_NAME = "transitpulse-dlq-replayer"

# Attribute names Pub/Sub stamps onto a message when it dead-letters it.
# Verified against the emulator; they are the only provenance a dead-lettered
# message carries, and the source delivery count is what tells an operator
# whether this message exhausted its attempts or arrived some other way.
SOURCE_DELIVERY_COUNT = "CloudPubSubDeadLetterSourceDeliveryCount"
SOURCE_SUBSCRIPTION = "CloudPubSubDeadLetterSourceSubscription"

# Pulled in batches because the round trip dominates at these volumes, and
# capped so a dead-letter topic holding a morning's traffic does not pull the
# whole thing into memory at once.
_PULL_BATCH = 100
# Short, so a job that has drained the subscription exits instead of sitting on
# a long poll.
_PULL_TIMEOUT_SECONDS = 5.0
_PUBLISH_TIMEOUT_SECONDS = 60.0

log = structlog.get_logger(__name__)


@dataclass(frozen=True)
class DeadLetter:
    """One pulled dead-letter message, and the provenance that survived."""

    ack_id: str
    message_id: str
    payload: bytes
    attributes: dict[str, str]
    ordering_key: str

    @property
    def source_delivery_count(self) -> int | None:
        raw = self.attributes.get(SOURCE_DELIVERY_COUNT)
        if raw is None or not raw.isdigit():
            return None
        return int(raw)

    @property
    def source_subscription(self) -> str | None:
        return self.attributes.get(SOURCE_SUBSCRIPTION)

    @property
    def source_trace_id(self) -> str | None:
        """Trace id of the fetch that produced this event, if it carried one.

        Logged rather than used to parent a span: the original trace was
        exported and written hours ago, and a span appended to it now would
        not join the waterfall a reader is looking at. The id is enough to
        find that trace in Jaeger, which is what an operator actually wants.
        """
        traceparent = self.attributes.get("traceparent")
        if traceparent is None:
            return None
        parts = traceparent.split("-")
        return parts[1] if len(parts) >= 3 and len(parts[1]) == 32 else None


@dataclass
class Outcome:
    """What a run did. Counted from broker acknowledgements, never from intent."""

    pulled: int = 0
    republished: int = 0
    still_invalid: int = 0
    failed: int = 0
    quarantine_rows_marked: int = 0
    payloads_without_quarantine_row: int = 0
    invalid_reasons: dict[str, int] = field(default_factory=dict)
    dry_run: bool = False
    seconds: float = 0.0

    def as_dict(self) -> dict[str, object]:
        return {
            "pulled": self.pulled,
            "republished": self.republished,
            "still_invalid": self.still_invalid,
            "failed": self.failed,
            "quarantine_rows_marked": self.quarantine_rows_marked,
            "payloads_without_quarantine_row": self.payloads_without_quarantine_row,
            "invalid_reasons": self.invalid_reasons,
            "dry_run": self.dry_run,
            "seconds": round(self.seconds, 3),
        }

    def render(self) -> str:
        lines = [
            "",
            "  dlq replay results",
            "  " + "-" * 52,
            f"  pulled                 {self.pulled:,}",
            f"  {'would republish' if self.dry_run else 'republished':22s} {self.republished:,}",
            f"  still invalid          {self.still_invalid:,}  (left on the subscription)",
        ]
        if self.failed:
            lines.append(f"  republish failed       {self.failed:,}  (left on the subscription)")
        lines.extend(
            [
                f"  quarantine rows marked {self.quarantine_rows_marked:,}",
                f"  no quarantine row      {self.payloads_without_quarantine_row:,}"
                "  (expected; see the module docstring)",
                f"  elapsed                {self.seconds:,.2f}s",
            ]
        )
        for reason, count in sorted(self.invalid_reasons.items()):
            lines.append(f"    still invalid: {reason} x{count}")
        if self.dry_run:
            lines.append("  NOTE: dry run -- nothing was published or marked")
        lines.append("")
        return "\n".join(lines)


def pull_batch(
    subscriber: pubsub_v1.SubscriberClient, subscription: str, *, max_messages: int
) -> list[DeadLetter]:
    """One pull from the dead-letter subscription, newest-available first."""
    response = subscriber.pull(
        request={"subscription": subscription, "max_messages": max_messages},
        timeout=_PULL_TIMEOUT_SECONDS,
    )
    return [
        DeadLetter(
            ack_id=received.ack_id,
            message_id=received.message.message_id,
            payload=bytes(received.message.data),
            attributes=dict(received.message.attributes),
            ordering_key=received.message.ordering_key,
        )
        for received in response.received_messages
    ]


def revalidate(message: DeadLetter) -> VehiclePing | str:
    """Re-check one payload against the current contract.

    Returns the ping, or a one-line reason it is still unacceptable. The reason
    is condensed to the failing field names rather than the full pydantic
    report, because the useful output of a replay run is "47 messages still
    fail on bearing", not forty-seven multi-line tracebacks.
    """
    try:
        return VehiclePing.model_validate_json(message.payload)
    except ValidationError as exc:
        fields = sorted(
            {".".join(str(part) for part in err["loc"]) or "<root>" for err in exc.errors()}
        )
        return f"schema: {', '.join(fields)}"
    except ValueError as exc:
        # Not valid JSON at all, which is a different story from a payload
        # that parsed and then breached the contract.
        return f"payload: {str(exc).splitlines()[0][:120]}"


def mark_quarantine_replayed(
    conn: psycopg.Connection, payloads: Sequence[bytes]
) -> tuple[int, int]:
    """Stamp ``replayed_at`` on the quarantine rows holding these payloads.

    Returns ``(rows marked, payloads that matched no row)``. Both numbers are
    reported because neither alone is meaningful: one payload can own several
    rows -- quarantine writes are deliberately not idempotent (ADR-005), so a
    message that failed repeatedly left one row per attempt -- and most
    payloads own none at all.

    ``RETURNING`` the payload rather than counting rows is what makes the
    second number exact instead of inferred from the difference, which would
    go negative on the multiple-rows case.
    """
    if not payloads:
        return 0, 0
    unique = list(dict.fromkeys(payloads))
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE quarantine
               SET replayed_at = now()
             WHERE replayed_at IS NULL
               AND raw_payload = ANY(%s)
            RETURNING raw_payload
            """,
            (unique,),
        )
        marked = [bytes(row[0]) for row in cur.fetchall()]
    matched = set(marked)
    return len(marked), sum(1 for payload in unique if payload not in matched)


def run(
    settings: Settings,
    *,
    max_messages: int | None = None,
    dry_run: bool = False,
) -> Outcome:
    outcome = Outcome(dry_run=dry_run)
    started = time.perf_counter()
    metrics = get_metrics()

    subscriber = pubsub_v1.SubscriberClient()
    subscription = subscriber.subscription_path(settings.gcp_project, settings.subscription_dlq)
    publisher = None if dry_run else EventPublisher(settings)
    replayed_payloads: list[bytes] = []

    with get_tracer().start_as_current_span(
        "dlq.replay",
        attributes={
            "messaging.system": "gcp_pubsub",
            "messaging.source.name": settings.subscription_dlq,
            "dlq.dry_run": dry_run,
        },
    ) as span:
        # Every message this run has already reached a verdict on. Without it
        # the loop does not terminate: a message that is returned rather than
        # acked -- a dry run's whole output, and every payload that is still
        # invalid -- is back on the subscription before the next pull, so the
        # job is fed its own nacks for ever. An empty pull is not the only
        # stopping condition; a pull that brings nothing new is the other one.
        seen: set[str] = set()

        try:
            while max_messages is None or outcome.pulled < max_messages:
                wanted = _PULL_BATCH
                if max_messages is not None:
                    wanted = min(wanted, max_messages - outcome.pulled)
                batch = pull_batch(subscriber, subscription, max_messages=wanted)
                if not batch:
                    break

                fresh = [message for message in batch if message.message_id not in seen]
                redelivered = [message.ack_id for message in batch if message.message_id in seen]
                if redelivered:
                    _return_to_subscription(subscriber, subscription, redelivered)
                if not fresh:
                    break

                seen.update(message.message_id for message in fresh)
                outcome.pulled += len(fresh)
                _replay_batch(
                    fresh,
                    outcome,
                    subscriber=subscriber,
                    subscription=subscription,
                    publisher=publisher,
                    replayed_payloads=replayed_payloads,
                )
        finally:
            if publisher is not None:
                publisher.close()

        if not dry_run and replayed_payloads:
            with psycopg.connect(settings.pg_dsn_str, autocommit=True) as conn:
                marked, unmatched = mark_quarantine_replayed(conn, replayed_payloads)
            outcome.quarantine_rows_marked = marked
            outcome.payloads_without_quarantine_row = unmatched

        outcome.seconds = time.perf_counter() - started
        span.set_attribute("dlq.pulled", outcome.pulled)
        span.set_attribute("dlq.republished", outcome.republished)
        span.set_attribute("dlq.still_invalid", outcome.still_invalid)
        if not dry_run:
            # Including zero, so the freshness panel shows the job ran at all.
            # A replay that found nothing to recover is the healthy outcome and
            # must not look like a job that never fired.
            metrics.events_replayed.add(outcome.republished, {"outcome": "ok"})
            record_job_success("dlq_replayer")

    log.info("dlq.replayed", **outcome.as_dict())
    return outcome


def _replay_batch(
    batch: Sequence[DeadLetter],
    outcome: Outcome,
    *,
    subscriber: pubsub_v1.SubscriberClient,
    subscription: str,
    publisher: EventPublisher | None,
    replayed_payloads: list[bytes],
) -> None:
    """Validate, republish and ack one pulled batch, per message."""
    acks: list[str] = []
    returns: list[str] = []

    for message in batch:
        verdict = revalidate(message)
        if isinstance(verdict, str):
            outcome.still_invalid += 1
            outcome.invalid_reasons[verdict] = outcome.invalid_reasons.get(verdict, 0) + 1
            returns.append(message.ack_id)
            log.warning(
                "dlq.still_invalid",
                message_id=message.message_id,
                reason=verdict,
                source_delivery_count=message.source_delivery_count,
                source_trace_id=message.source_trace_id,
            )
            continue

        if publisher is None:
            outcome.republished += 1
            returns.append(message.ack_id)
            continue

        future: Future[str] = publisher.publish_one(verdict)
        try:
            future.result(timeout=_PUBLISH_TIMEOUT_SECONDS)
        except Exception as exc:  # noqa: BLE001 - one bad republish must not end the run
            outcome.failed += 1
            returns.append(message.ack_id)
            log.error("dlq.republish_failed", message_id=message.message_id, error=str(exc))
            continue

        outcome.republished += 1
        replayed_payloads.append(message.payload)
        # Only now: the broker holds the republished copy, so dropping this one
        # is safe. Acking before this point would lose the event on a failure.
        acks.append(message.ack_id)
        log.info(
            "dlq.republished",
            vehicle_id=verdict.vehicle_id,
            event_time=verdict.event_time.isoformat(),
            source_delivery_count=message.source_delivery_count,
            source_subscription=message.source_subscription,
            source_trace_id=message.source_trace_id,
        )

    if acks:
        subscriber.acknowledge(request={"subscription": subscription, "ack_ids": acks})
    if returns:
        _return_to_subscription(subscriber, subscription, returns)


def _return_to_subscription(
    subscriber: pubsub_v1.SubscriberClient, subscription: str, ack_ids: Sequence[str]
) -> None:
    """Hand messages back immediately instead of letting the deadline lapse.

    So a dry run, or a payload that is still invalid, leaves the dead-letter
    subscription exactly as it was found -- the next run sees the same
    messages rather than waiting out an ack deadline first.
    """
    subscriber.modify_ack_deadline(
        request={
            "subscription": subscription,
            "ack_ids": list(ack_ids),
            "ack_deadline_seconds": 0,
        }
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="dlq_replayer",
        description="Re-validate dead-lettered events and republish the ones that now pass.",
    )
    parser.add_argument(
        "--max-messages",
        type=int,
        default=None,
        metavar="N",
        help="stop after this many (default: until the subscription drains)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="report what would be republished without publishing or marking anything",
    )
    parser.add_argument("--json", action="store_true", help="emit the outcome as JSON")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    # Pinned instance id for the same reason partition_maintenance pins one:
    # this job stamps job_last_success, a gauge describing shared state, and a
    # per-process id would leave every past run's value alongside the current
    # one with no way to tell them apart. See setup_telemetry's docstring.
    setup_telemetry(SERVICE_NAME, instrument_libraries=False, instance_id=SERVICE_NAME)
    settings = get_settings()

    try:
        outcome = run(settings, max_messages=args.max_messages, dry_run=args.dry_run)
    except gcp_exceptions.NotFound:
        print(
            f"error: subscription {settings.subscription_dlq!r} does not exist; "
            "run `python tasks.py topology` first",
            file=sys.stderr,
        )
        return 2
    except psycopg.Error as exc:
        # The republish has already happened by the time the database is
        # touched, so this is a failure to reconcile, not a failure to recover.
        print(
            f"error: events were republished but quarantine marking failed: {exc}", file=sys.stderr
        )
        return 1

    if args.json:
        print(json.dumps(outcome.as_dict(), indent=2))
        return 0

    print(outcome.render())
    return 0


if __name__ == "__main__":  # pragma: no cover - module entry point
    raise SystemExit(main())

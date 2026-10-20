"""Keep ``vehicle_events`` partitioned ahead of the present, and prune behind it.

Declarative range partitioning has one sharp edge: an ``INSERT`` whose
``event_time`` falls outside every existing partition does not degrade, it
fails. The migration pre-creates a short window so a fresh clone can ingest
immediately, but that window expires, and when it does the processor starts
rejecting every event with ``no partition of relation "vehicle_events" found
for row``. Nothing else in the pipeline reports that as a capacity problem, so
this job exists to make the window self-maintaining.

Two halves, both idempotent:

* **ensure** -- create partitions from yesterday through
  ``partition_ahead_days`` out. Yesterday is included because late-arriving
  pings are normal, and the default fortnight of lead time means a broken
  schedule has to go unnoticed for two weeks before it costs any data.
* **prune** -- drop partitions older than ``partition_retention_days``.
  Turning retention into a ``DROP TABLE`` rather than a bulk ``DELETE`` is the
  main reason the table is partitioned at all: it returns the disk immediately
  and leaves nothing to vacuum.

Run through the shared image entrypoint
(``TP_SERVICE=job TP_JOB=partition_maintenance``) on a daily Cloud Scheduler
trigger, or locally with ``python -m services.jobs.partition_maintenance``.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta

import psycopg
import structlog
from psycopg import sql

from transitpulse_core.config import Settings, get_settings
from transitpulse_core.telemetry import (
    get_metrics,
    get_tracer,
    record_job_success,
    setup_telemetry,
)

SERVICE_NAME = "transitpulse-partition-maintenance"

PARENT_TABLE = "vehicle_events"
# Partition names are the parent plus a compact day stamp, exactly as created
# by the ensure_event_partition() function in migration 0001.
_NAME_PREFIX = f"{PARENT_TABLE}_"
_NAME_PATTERN = rf"^{_NAME_PREFIX}[0-9]{{8}}$"

log = structlog.get_logger(__name__)


@dataclass(frozen=True)
class Plan:
    """What a run intends to do, computed before touching the database."""

    today: date
    ensure_days: tuple[date, ...]
    drop_before: date

    @property
    def horizon(self) -> date:
        return self.ensure_days[-1]


@dataclass
class Outcome:
    created: list[str] = field(default_factory=list)
    existing: list[str] = field(default_factory=list)
    dropped: list[str] = field(default_factory=list)
    retained: list[str] = field(default_factory=list)
    dry_run: bool = False
    seconds: float = 0.0
    headroom_days: int = 0

    def as_dict(self) -> dict[str, object]:
        return {
            "created": self.created,
            "created_count": len(self.created),
            "existing_count": len(self.existing),
            "dropped": self.dropped,
            "dropped_count": len(self.dropped),
            "retained_count": len(self.retained),
            "headroom_days": self.headroom_days,
            "dry_run": self.dry_run,
            "seconds": round(self.seconds, 3),
        }


def build_plan(today: date, *, ahead_days: int, retention_days: int) -> Plan:
    """Decide the window without consulting the database.

    Separated from execution so the date arithmetic -- the part that is easy to
    get subtly wrong and expensive to get wrong in production -- is unit
    testable with no Postgres in sight.
    """
    if ahead_days < 1:
        raise ValueError(f"ahead_days must be at least 1, got {ahead_days}")
    if retention_days < 1:
        # Zero or negative retention would put drop_before on or after today
        # and delete partitions that are still being written to.
        raise ValueError(f"retention_days must be at least 1, got {retention_days}")

    # From yesterday, so a late ping for the previous service day still lands.
    start = today - timedelta(days=1)
    ensure_days = tuple(start + timedelta(days=i) for i in range(ahead_days + 2))
    return Plan(
        today=today,
        ensure_days=ensure_days,
        drop_before=today - timedelta(days=retention_days),
    )


def headroom_days(partition_days: Iterable[date], today: date) -> int:
    """Days of write capacity left: the *unbroken* run of days from today on.

    This is the number whose absence cost a development day. Partitions ran
    out mid-development and every insert started failing with ``no partition of
    relation "vehicle_events" found for row``; nothing anywhere reported the
    window shrinking, because "how much runway is left" was never measured --
    only "did today's run create anything", which stayed reassuringly true
    right up to the last partition.

    Contiguous rather than "furthest partition minus today", which is the
    obvious reading and is wrong: a single stray partition dated years ahead --
    left behind by a test, or created by hand during an incident -- makes the
    furthest-day arithmetic report years of runway over a gap that refuses
    every insert the moment the real window ends. Measured against this
    database the two answers were 14 days and 1,209.

    ``-1`` means today itself has no partition, so inserts are already being
    refused rather than about to be. ``0`` means today is covered and tomorrow
    is not.
    """
    days = set(partition_days)
    if today not in days:
        return -1
    ahead = 0
    while today + timedelta(days=ahead + 1) in days:
        ahead += 1
    return ahead


def partition_day(relname: str) -> date | None:
    """Parse a partition's day from its name, or None if it is not one of ours."""
    if not relname.startswith(_NAME_PREFIX):
        return None
    stamp = relname[len(_NAME_PREFIX) :]
    if len(stamp) != 8 or not stamp.isdigit():
        return None
    try:
        return datetime.strptime(stamp, "%Y%m%d").replace(tzinfo=UTC).date()
    except ValueError:
        return None


def existing_partitions(conn: psycopg.Connection) -> dict[str, date]:
    """Day-stamped partitions genuinely attached to the parent table.

    Joined through ``pg_inherits`` rather than matched on name alone: dropping
    a table because its name looked right would be an unbounded blast radius,
    and a stray ``vehicle_events_20260101_backup`` is exactly the sort of thing
    someone leaves lying around.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT child.relname
            FROM pg_inherits
            JOIN pg_class AS child  ON child.oid  = pg_inherits.inhrelid
            JOIN pg_class AS parent ON parent.oid = pg_inherits.inhparent
            WHERE parent.relname = %s
              AND child.relname ~ %s
            """,
            (PARENT_TABLE, _NAME_PATTERN),
        )
        found: dict[str, date] = {}
        for (relname,) in cur.fetchall():
            day = partition_day(relname)
            if day is not None:
                found[relname] = day
        return found


def apply_plan(conn: psycopg.Connection, plan: Plan, *, dry_run: bool = False) -> Outcome:
    """Create the missing partitions, then drop the expired ones."""
    outcome = Outcome(dry_run=dry_run)
    metrics = get_metrics()
    before = existing_partitions(conn)

    for day in plan.ensure_days:
        name = f"{_NAME_PREFIX}{day:%Y%m%d}"
        if name in before:
            outcome.existing.append(name)
            continue
        if dry_run:
            outcome.created.append(name)
            continue
        # Delegated to the function defined in migration 0001 so the partition
        # bound arithmetic lives in exactly one place.
        with conn.cursor() as cur:
            cur.execute("SELECT ensure_event_partition(%s)", (day,))
        outcome.created.append(name)

    for name, day in sorted(before.items(), key=lambda kv: kv[1]):
        if day >= plan.drop_before:
            outcome.retained.append(name)
            continue
        # Belt and braces: build_plan already rejects a retention window that
        # could reach today, but a DROP is irreversible, so the guard is
        # re-checked against the data and not only against the arguments.
        if day >= plan.today:
            log.warning("partitions.refused_drop_of_current_day", partition=name)
            outcome.retained.append(name)
            continue
        if dry_run:
            outcome.dropped.append(name)
            continue
        with conn.cursor() as cur:
            # DETACH first so the parent stops routing to it, then DROP. A bare
            # DROP holds an ACCESS EXCLUSIVE lock on the parent for the whole
            # operation, stalling every concurrent insert.
            cur.execute(
                sql.SQL("ALTER TABLE {parent} DETACH PARTITION {child}").format(
                    parent=sql.Identifier(PARENT_TABLE),
                    child=sql.Identifier(name),
                )
            )
            cur.execute(sql.SQL("DROP TABLE {child}").format(child=sql.Identifier(name)))
        outcome.dropped.append(name)

    if not dry_run:
        # Added unconditionally, including zero: a run that created nothing is
        # the normal, healthy case, and skipping the call would leave the
        # series absent so the panel reads "No data" on exactly the days
        # nothing was wrong.
        metrics.partitions_ensured.add(len(outcome.created))
        metrics.partitions_dropped.add(len(outcome.dropped))

    return outcome


def run(
    settings: Settings,
    *,
    today: date | None = None,
    ahead_days: int | None = None,
    retention_days: int | None = None,
    dry_run: bool = False,
) -> Outcome:
    plan = build_plan(
        today or datetime.now(UTC).date(),
        ahead_days=ahead_days if ahead_days is not None else settings.partition_ahead_days,
        retention_days=(
            retention_days if retention_days is not None else settings.partition_retention_days
        ),
    )

    started = time.perf_counter()
    with get_tracer().start_as_current_span(
        "partitions.maintain",
        attributes={
            "db.system": "postgresql",
            "partitions.horizon": plan.horizon.isoformat(),
            "partitions.drop_before": plan.drop_before.isoformat(),
            "partitions.dry_run": dry_run,
        },
    ) as span:
        # autocommit, so each partition is durable as it is created. Wrapping
        # the whole run in one transaction would mean a failure on day 12
        # discards the first eleven, and partial progress beats none.
        with psycopg.connect(settings.pg_dsn_str, autocommit=True) as conn:
            outcome = apply_plan(conn, plan, dry_run=dry_run)
            # Re-read rather than deriving the headroom from the plan: a run
            # that meant to guarantee a fortnight and failed on day three has
            # three days of runway, not fourteen, and the plan cannot tell the
            # difference. A dry run reports it too -- measuring the window is
            # an observation, not a change.
            outcome.headroom_days = headroom_days(existing_partitions(conn).values(), plan.today)

        outcome.seconds = time.perf_counter() - started
        span.set_attribute("partitions.created", len(outcome.created))
        span.set_attribute("partitions.dropped", len(outcome.dropped))
        span.set_attribute("partitions.headroom_days", outcome.headroom_days)
        get_metrics().partition_headroom.set(outcome.headroom_days, {"table": PARENT_TABLE})
        if not dry_run:
            record_job_success("partition_maintenance")

    log.info("partitions.maintained", **outcome.as_dict())
    return outcome


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="partition_maintenance",
        description="Create daily vehicle_events partitions ahead of now and prune old ones.",
    )
    parser.add_argument(
        "--ahead-days",
        type=int,
        default=None,
        metavar="N",
        help="days of future partitions to guarantee (default: TP_PARTITION_AHEAD_DAYS)",
    )
    parser.add_argument(
        "--retention-days",
        type=int,
        default=None,
        metavar="N",
        help="drop partitions older than this (default: TP_PARTITION_RETENTION_DAYS)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="report what would change without creating or dropping anything",
    )
    parser.add_argument("--json", action="store_true", help="emit the outcome as JSON")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    # A pinned instance id, because the headroom gauge below describes the
    # database rather than this process: successive runs must overwrite one
    # series instead of each leaving a stale reading behind. See
    # setup_telemetry's docstring for why aggregation cannot fix that after
    # the fact.
    setup_telemetry(SERVICE_NAME, instrument_libraries=False, instance_id=SERVICE_NAME)
    settings = get_settings()

    try:
        outcome = run(
            settings,
            ahead_days=args.ahead_days,
            retention_days=args.retention_days,
            dry_run=args.dry_run,
        )
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    except psycopg.Error as exc:
        print(f"error: partition maintenance failed: {exc}", file=sys.stderr)
        return 1

    if args.json:
        print(json.dumps(outcome.as_dict(), indent=2))
        return 0

    created_label = "would create" if outcome.dry_run else "created"
    print(f"{created_label}: {len(outcome.created)} partition(s)")
    for name in outcome.created:
        print(f"  + {name}")
    dropped_label = "would drop" if outcome.dry_run else "dropped"
    print(f"{dropped_label}: {len(outcome.dropped)} partition(s)")
    for name in outcome.dropped:
        print(f"  - {name}")
    print(f"retained: {len(outcome.retained)}, already present: {len(outcome.existing)}")
    print(f"headroom: {outcome.headroom_days} day(s) of write capacity")
    return 0


if __name__ == "__main__":  # pragma: no cover - module entry point
    raise SystemExit(main())

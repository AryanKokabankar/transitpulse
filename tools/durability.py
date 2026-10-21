"""Measure what ``fsync=off`` is worth, by running the same load against both.

The local Postgres runs with ``fsync=off`` and ``synchronous_commit=off``
(docker-compose.yml). That is a reasonable choice for a development stack and
an unreasonable one to publish a write-throughput figure from without saying
so, because it removes the single largest cost on the write path: waiting for
the disk. Every consumer-side number in ``BENCHMARKS.md`` is inflated by it.

"Inflated by some amount" is not a measurement, so this drives the identical
backlog through the identical pipeline twice -- once against the development
database and once against a second Postgres with durability left switched on --
and reports both, along with the ratio between them.

**The settings are read back from each database, not assumed.** ``SHOW fsync``
on both, recorded in the result file. A comparison that silently ran twice
against the same configuration would show a ratio near 1.0 and look like a
finding; this is the project's own lesson about numbers nobody checked, and the
run refuses outright if the two databases do not actually differ.

The durable database is deliberately a separate container rather than a
reconfiguration of the first. ``fsync`` is a postmaster-level setting whose
value here comes from a command-line flag in docker-compose.yml, and a
command-line flag outranks ``ALTER SYSTEM`` -- so the obvious approach of
setting it at runtime and reloading appears to work and changes nothing.

    docker compose --profile durable up -d postgres-durable
    python tasks.py durability

The first run has to create the schema in the new database; this does that
itself rather than leaving it as a step to forget, because a missing table
would otherwise surface as a drain that wrote nothing at an impressive rate.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from typing import Any

import psycopg
import structlog

from tools.chaos.experiment import ScenarioResult, equals, execute, holds, report
from tools.chaos.support import REPO_ROOT, drain_backlog, processor_process
from transitpulse_core.config import Settings, get_settings
from transitpulse_core.telemetry import setup_telemetry

NAME = "durability"
SERVICE_NAME = "transitpulse-durability"
QUESTION = "How much of the write throughput is borrowed from having fsync switched off?"

log = structlog.get_logger(__name__)

DURABLE_DSN = "postgresql://transitpulse:transitpulse@localhost:5433/transitpulse"

_SETTINGS_OF_INTEREST = ("fsync", "synchronous_commit", "shared_buffers", "max_connections")


def describe(dsn: str) -> dict[str, str]:
    """Read the durability settings a database is actually running with."""
    with psycopg.connect(dsn, connect_timeout=10) as conn, conn.cursor() as cur:
        found: dict[str, str] = {}
        for name in _SETTINGS_OF_INTEREST:
            cur.execute("SELECT current_setting(%s)", (name,))
            row = cur.fetchone()
            found[name] = str(row[0]) if row else "?"
        cur.execute("SHOW server_version")
        row = cur.fetchone()
        found["server_version"] = str(row[0]) if row else "?"
        return found


def reclaim(dsn: str) -> None:
    """VACUUM ANALYZE the tables the drain writes, before timing it.

    Not housekeeping. These tables absorb every experiment in the project and
    each one deletes its rows afterwards, so by the end of a session the
    development database is carrying far more dead tuples than a database that
    has run one pass. Comparing the two without this measures accumulated
    bloat and calls it durability -- which is exactly what the first run of
    this comparison did, reporting fsync=on as the faster configuration.
    """
    with psycopg.connect(dsn, autocommit=True, connect_timeout=10) as conn, conn.cursor() as cur:
        for table in ("vehicle_events", "vehicle_live", "quarantine"):
            cur.execute(f"VACUUM (ANALYZE) {table}")


def migrate(dsn: str) -> None:
    """Bring the comparison database up to the current schema."""
    for command in (
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        [sys.executable, "-m", "services.jobs.partition_maintenance"],
    ):
        completed = subprocess.run(  # noqa: S603 - fixed argv, no shell
            command,
            cwd=str(REPO_ROOT),
            env={**_env(), "TP_PG_DSN": dsn},
            capture_output=True,
            text=True,
        )
        if completed.returncode != 0:
            raise RuntimeError(
                f"{' '.join(command[2:])} failed against the durable database:\n"
                f"{completed.stderr[-2000:]}"
            )


def _env() -> dict[str, str]:
    """The current environment, so the subprocess reaches the same stack."""
    return os.environ.copy()


def run(settings: Settings, args: argparse.Namespace) -> ScenarioResult:
    result = ScenarioResult(scenario=NAME, question=QUESTION)

    development = describe(settings.pg_dsn_str)
    try:
        migrate(args.durable_dsn)
        durable = describe(args.durable_dsn)
    except (psycopg.Error, RuntimeError) as exc:
        result.check(holds("the durable database is reachable", False, str(exc)[:400]))
        result.note("Start it first:  docker compose --profile durable up -d postgres-durable")
        return result

    result.measure(development_postgres=development, durable_postgres=durable)
    result.check(
        equals("the development database has fsync off", "off", development["fsync"]),
        equals("the comparison database has fsync on", "on", durable["fsync"]),
        holds(
            "the two configurations genuinely differ",
            development["fsync"] != durable["fsync"],
            f"development fsync={development['fsync']}, durable fsync={durable['fsync']}",
        ),
    )
    if not result.passed:
        result.note("Refusing to report a ratio between two identically configured databases.")
        return result

    targets = {"development": settings.pg_dsn_str, "durable": args.durable_dsn}
    # Alternating, so that warm caches and any drift over the run land on both
    # configurations equally instead of on whichever went second.
    order = ["development", "durable", "durable", "development"][: args.repeats * 2]

    passes: list[dict[str, Any]] = []
    rates: dict[str, list[float]] = {"development": [], "durable": []}

    for index, label in enumerate(order, start=1):
        dsn = targets[label]
        reclaim(dsn)
        # Its own processor per pass, because the sink and its pool are built
        # once in the lifespan: a process started against one database does
        # not move to another when an environment variable changes.
        with processor_process(settings, env={"TP_PG_DSN": dsn}) as processor:
            drain = drain_backlog(
                settings,
                backlog=args.backlog,
                concurrency=args.concurrency,
                pool_max=settings.pg_pool_max,
                dsn=dsn,
                processor_url=processor.url,
                keep_rows=False,
            )
        passes.append({"pass": index, "configuration": label, **drain.as_dict()})
        rates[label].append(drain.rate)
        log.info("durability.pass", n=index, configuration=label, rate=round(drain.rate, 1))
        result.check(
            equals(
                f"zero loss against the {label} database, pass {index}",
                drain.published,
                drain.rows,
            )
        )

    dev_rates = _summarise(rates["development"])
    durable_rates = _summarise(rates["durable"])
    ratio = dev_rates["mean"] / durable_rates["mean"] if durable_rates["mean"] else 0.0
    # The widest single-configuration spread is the noise floor: a difference
    # between the two that does not clear it has not been resolved.
    noise = max(dev_rates["spread"], durable_rates["spread"])
    separation = abs(dev_rates["mean"] - durable_rates["mean"])
    resolved = separation > noise

    result.measure(
        passes=passes,
        order=order,
        development=dev_rates,
        durable=durable_rates,
        inflation_factor=round(ratio, 3),
        separation_per_second=round(separation, 2),
        noise_floor_per_second=round(noise, 2),
        difference_is_resolved=resolved,
    )
    if resolved:
        result.note(
            f"Writing with fsync off ran at {dev_rates['mean']:.1f}/s against"
            f" {durable_rates['mean']:.1f}/s with it on -- {ratio:.2f}x -- and the"
            f" {separation:.1f}/s gap clears the {noise:.1f}/s run-to-run spread, so the"
            " difference is real on this machine. Every consumer-side figure elsewhere"
            " in BENCHMARKS.md is the fsync-off number."
        )
    else:
        result.note(
            f"No difference was resolved: fsync off ran at {dev_rates['mean']:.1f}/s and"
            f" fsync on at {durable_rates['mean']:.1f}/s, a gap of {separation:.1f}/s against a"
            f" run-to-run spread of {noise:.1f}/s. At this pipeline's throughput that is"
            " the expected result rather than a surprising one -- the handler makes"
            " several database round trips per event and is bound by those, not by how"
            " each transaction is flushed, so the commit cost fsync adds is small"
            " against what the pipeline is already spending. It does NOT mean fsync is"
            " free: it means this workload is not where it shows up, and a bulk-loading"
            " or higher-throughput path on the same database would look different.",
            "The fsync=off caveat on every other figure stands regardless. What this"
            " measures is how much that caveat is worth for this workload on this"
            " machine, which turns out to be less than one would assume.",
        )
    result.note(
        "Both configurations are vacuumed before every pass and the order is alternated."
        " Without that the development database -- which absorbs every experiment in the"
        " project -- carries the session's dead tuples into the comparison, and the first"
        " run of this reported fsync=on as the faster configuration.",
        "Both passes use the same broker, the same processor and the same backlog; the"
        " database is the only thing that changes.",
        "This is one machine's NVMe SSD, not a cloud disk.",
    )
    return result


def _summarise(values: list[float]) -> dict[str, float]:
    if not values:
        return {"mean": 0.0, "min": 0.0, "max": 0.0, "spread": 0.0, "passes": 0}
    return {
        "mean": round(sum(values) / len(values), 2),
        "min": round(min(values), 2),
        "max": round(max(values), 2),
        "spread": round(max(values) - min(values), 2),
        "passes": len(values),
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="durability",
        description=QUESTION,
        epilog=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--durable-dsn",
        default=DURABLE_DSN,
        help=f"the fsync=on database to compare against (default: {DURABLE_DSN})",
    )
    parser.add_argument(
        "--backlog",
        type=int,
        default=4000,
        metavar="N",
        help="messages to publish and drain in each pass (default: 4000)",
    )
    parser.add_argument(
        "--repeats",
        type=int,
        default=2,
        choices=(1, 2),
        metavar="N",
        help="passes per configuration; 2 alternates the order so warm caches"
        " do not land on one of them (default: 2)",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=32,
        metavar="N",
        help="deliveries in flight (default: 32)",
    )
    parser.add_argument("--json", action="store_true", help="emit the result as JSON")
    parser.add_argument("--no-write", action="store_true", help="do not write the result file")
    parser.add_argument(
        "--keep-rows", action="store_true", help="leave the run's rows in both databases"
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    setup_telemetry(SERVICE_NAME, instrument_libraries=False)
    settings = get_settings()
    outcome = execute(run, settings, args, name=NAME)
    return report(outcome, args)


if __name__ == "__main__":  # pragma: no cover - module entry point
    raise SystemExit(main())

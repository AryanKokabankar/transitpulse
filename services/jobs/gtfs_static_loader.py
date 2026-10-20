"""Load an agency's static GTFS archive into the reference tables.

A Cloud Run Job, not a service: it runs when the agency publishes a new
timetable (weekly for most, daily for some), takes minutes, and has no
request to answer.

    TP_SERVICE=job TP_JOB=gtfs_static_loader docker run ...
    python -m services.jobs.gtfs_static_loader --help

The realtime feed says where a vehicle is; only this timetable says where it
was *supposed* to be, so schedule adherence does not exist until this job has
run. ``stop_times.txt`` is what makes that non-trivial: it is two orders of
magnitude larger than the other files -- a couple of million rows for a
mid-size agency -- so it goes in through ``COPY`` rather than ``INSERT``.

**The load is one transaction.** ``TRUNCATE`` then ``COPY``, committed
together, which makes a re-run a replacement rather than a merge and means a
failure half way through leaves the previous timetable intact. Enrichment
queries do not have to know a load is happening: Postgres holds them at the
old rows until the commit, at which point they see the new ones. The cost is
that ``TRUNCATE`` takes an ACCESS EXCLUSIVE lock, so readers block for the
length of the commit rather than reading a half-loaded timetable -- the right
way round, because a missing ``stop_times`` row does not look like an error
downstream, it looks like a trip with no schedule.
"""

from __future__ import annotations

import argparse
import csv
import io
import sys
import tempfile
import time
import zipfile
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx
import psycopg
import structlog
from psycopg import sql

from transitpulse_core.config import Settings, get_settings
from transitpulse_core.schedule import parse_gtfs_time
from transitpulse_core.telemetry import get_tracer, setup_telemetry

log = structlog.get_logger(__name__)

SERVICE_NAME = "transitpulse-gtfs-loader"

Row = tuple[Any, ...]
RowBuilder = Callable[[dict[str, str]], Row | None]


@dataclass(frozen=True)
class TableSpec:
    """One GTFS file and the table it lands in."""

    table: str
    member: str
    columns: tuple[str, ...]
    build: RowBuilder


def _text(row: dict[str, str], key: str) -> str | None:
    """A GTFS optional field: absent column and empty string both mean NULL."""
    value = (row.get(key) or "").strip()
    return value or None


def _integer(row: dict[str, str], key: str) -> int | None:
    value = _text(row, key)
    if value is None:
        return None
    try:
        return int(value)
    except ValueError:
        return None


def _real(row: dict[str, str], key: str) -> float | None:
    value = _text(row, key)
    if value is None:
        return None
    try:
        return float(value)
    except ValueError:
        return None


def _route(row: dict[str, str]) -> Row | None:
    route_id = _text(row, "route_id")
    if route_id is None:
        return None
    return (
        route_id,
        _text(row, "agency_id"),
        _text(row, "route_short_name"),
        _text(row, "route_long_name"),
        _integer(row, "route_type"),
    )


def _stop(row: dict[str, str]) -> Row | None:
    stop_id = _text(row, "stop_id")
    if stop_id is None:
        return None
    return (stop_id, _text(row, "stop_name"), _real(row, "stop_lat"), _real(row, "stop_lon"))


def _trip(row: dict[str, str]) -> Row | None:
    trip_id = _text(row, "trip_id")
    route_id = _text(row, "route_id")
    if trip_id is None or route_id is None:
        return None
    return (trip_id, route_id, _text(row, "service_id"), _integer(row, "direction_id"))


def _stop_time(row: dict[str, str]) -> Row | None:
    trip_id = _text(row, "trip_id")
    stop_id = _text(row, "stop_id")
    sequence = _integer(row, "stop_sequence")
    if trip_id is None or stop_id is None or sequence is None:
        return None
    try:
        arrival = parse_gtfs_time(row.get("arrival_time") or "")
        departure = parse_gtfs_time(row.get("departure_time") or "")
    except ValueError:
        # A stop time this code cannot parse would become a NULL schedule and
        # therefore an invisible gap in adherence coverage. Skipping it is
        # counted and reported, so the gap has a number attached.
        return None
    # Parsed into timedeltas rather than passed through as text: psycopg adapts
    # them to INTERVAL, which is what makes "25:30:00" survive the trip into
    # the database as 25h30m instead of being rejected as a clock time.
    return (trip_id, sequence, stop_id, arrival, departure)


# stop_times goes last because it is the only expensive one: a column or type
# mistake in the three small files then aborts the transaction in seconds
# rather than after a two-million-row COPY has already run.
TABLES: tuple[TableSpec, ...] = (
    TableSpec(
        "gtfs_routes",
        "routes.txt",
        ("route_id", "agency_id", "route_short_name", "route_long_name", "route_type"),
        _route,
    ),
    TableSpec("gtfs_stops", "stops.txt", ("stop_id", "stop_name", "stop_lat", "stop_lon"), _stop),
    TableSpec(
        "gtfs_trips", "trips.txt", ("trip_id", "route_id", "service_id", "direction_id"), _trip
    ),
    TableSpec(
        "gtfs_stop_times",
        "stop_times.txt",
        ("trip_id", "stop_sequence", "stop_id", "arrival_time", "departure_time"),
        _stop_time,
    ),
)


@dataclass
class TableLoad:
    rows: int = 0
    skipped: int = 0
    seconds: float = 0.0


def download(url: str, destination: Path, *, timeout: float) -> int:
    """Stream the archive to disk. Returns its size in bytes.

    Streamed rather than held in memory because these archives run to tens of
    megabytes and a Cloud Run Job's memory limit is the thing that would fail
    first, long after the code looked correct in testing against a small feed.
    """
    total = 0
    with (
        httpx.stream("GET", url, timeout=timeout, follow_redirects=True) as response,
        destination.open("wb") as sink,
    ):
        response.raise_for_status()
        for chunk in response.iter_bytes(chunk_size=1 << 16):
            sink.write(chunk)
            total += len(chunk)
    return total


def _open_member(archive: zipfile.ZipFile, member: str) -> Iterator[dict[str, str]]:
    """Yield CSV rows from one file inside the archive.

    ``utf-8-sig`` because a GTFS file written on Windows commonly carries a
    byte-order mark, which would otherwise end up inside the first column's
    name and make every lookup of it miss.
    """
    with archive.open(member) as raw:
        stream = io.TextIOWrapper(raw, encoding="utf-8-sig", newline="")
        yield from csv.DictReader(stream)


def _copy_table(
    connection: psycopg.Connection[Any], archive: zipfile.ZipFile, spec: TableSpec
) -> TableLoad:
    statement = sql.SQL("COPY {table} ({columns}) FROM STDIN").format(
        table=sql.Identifier(spec.table),
        columns=sql.SQL(", ").join(sql.Identifier(column) for column in spec.columns),
    )
    load = TableLoad()
    started = time.perf_counter()
    with connection.cursor() as cursor, cursor.copy(statement) as copy:
        for row in _open_member(archive, spec.member):
            values = spec.build(row)
            if values is None:
                load.skipped += 1
                continue
            copy.write_row(values)
            load.rows += 1
    load.seconds = time.perf_counter() - started
    log.info(
        "gtfs.table_loaded",
        table=spec.table,
        member=spec.member,
        rows=load.rows,
        skipped=load.skipped,
        seconds=round(load.seconds, 2),
    )
    return load


def check_agency_timezone(archive: zipfile.ZipFile, configured: str) -> str | None:
    """Compare the feed's own timezone with the configured one.

    Every schedule delay in the pipeline is computed in the agency's local
    timezone, so a wrong setting here does not fail -- it produces delays that
    are off by whole hours and look entirely plausible. Checking it against
    the feed is the only cheap way to catch that, and it belongs in the job
    that already has the feed open.
    """
    if "agency.txt" not in archive.namelist():
        return None
    for row in _open_member(archive, "agency.txt"):
        declared = _text(row, "agency_timezone")
        if declared and declared != configured:
            log.warning(
                "gtfs.timezone_mismatch",
                declared=declared,
                configured=configured,
                detail="schedule delays will be wrong by the offset between these zones",
            )
        return declared
    return None


def load_archive(zip_path: Path, settings: Settings) -> dict[str, TableLoad]:
    """Replace the reference tables from one archive, atomically."""
    results: dict[str, TableLoad] = {}
    with zipfile.ZipFile(zip_path) as archive:
        missing = [spec.member for spec in TABLES if spec.member not in archive.namelist()]
        if missing:
            raise ValueError(f"archive is missing required GTFS files: {', '.join(missing)}")
        check_agency_timezone(archive, settings.agency_timezone)

        with psycopg.connect(settings.pg_dsn_str) as connection, connection.transaction():
            # One statement so the four tables are never momentarily
            # inconsistent with each other, and so the lock is taken once.
            connection.execute(
                sql.SQL("TRUNCATE {}").format(
                    sql.SQL(", ").join(sql.Identifier(spec.table) for spec in TABLES)
                )
            )
            for spec in TABLES:
                results[spec.table] = _copy_table(connection, archive, spec)
    return results


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="gtfs_static_loader",
        description="Download an agency's static GTFS archive and load the reference tables.",
    )
    source = parser.add_mutually_exclusive_group()
    source.add_argument(
        "--url",
        help="GTFS archive URL (default: the configured TP_GTFS_STATIC_URL)",
    )
    source.add_argument(
        "--from-file",
        type=Path,
        help="load an archive already on disk instead of downloading one",
    )
    return parser


def run(url: str | None, from_file: Path | None, settings: Settings) -> dict[str, TableLoad]:
    with get_tracer().start_as_current_span("gtfs.load_static") as span:
        if from_file is not None:
            span.set_attribute("gtfs.source", str(from_file))
            return load_archive(from_file, settings)

        source = url or settings.gtfs_static_url
        span.set_attribute("gtfs.source", source)
        # Its own empty directory: the archive is an untrusted download, and
        # giving it a directory of its own means nothing else is in reach of a
        # member name that tries to escape.
        with tempfile.TemporaryDirectory(prefix="transitpulse-gtfs-") as scratch:
            destination = Path(scratch) / "gtfs.zip"
            size = download(source, destination, timeout=settings.gtfs_download_timeout_seconds)
            span.set_attribute("gtfs.archive_bytes", size)
            log.info("gtfs.downloaded", url=source, bytes=size)
            return load_archive(destination, settings)


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    setup_telemetry(SERVICE_NAME, instrument_libraries=False)
    settings = get_settings()

    started = time.perf_counter()
    try:
        results = run(args.url, args.from_file, settings)
    except (httpx.HTTPError, OSError, ValueError, zipfile.BadZipFile) as exc:
        print(f"error: could not load the GTFS archive: {exc}", file=sys.stderr)
        return 1
    except psycopg.Error as exc:
        # The transaction rolled back, so the previous timetable is still
        # loaded; saying so is the difference between "retry" and "panic".
        print(f"error: load rolled back, previous data intact: {exc}", file=sys.stderr)
        return 1

    elapsed = time.perf_counter() - started
    log.info(
        "gtfs.load_completed",
        seconds=round(elapsed, 2),
        **{table: load.rows for table, load in results.items()},
    )
    for table, load in results.items():
        print(f"{table}: {load.rows} rows ({load.skipped} skipped) in {load.seconds:.1f}s")
    print(f"loaded in {elapsed:.1f}s")
    return 0


if __name__ == "__main__":  # pragma: no cover - module entry point
    raise SystemExit(main())

"""Integration tests for the static GTFS loader against real PostgreSQL.

Everything asserted here is the database's behaviour rather than ours:
``COPY`` adapting a Python ``timedelta`` into an ``INTERVAL`` past 24 hours,
``TRUNCATE`` and ``COPY`` committing as one unit, and the rollback that keeps
the previous timetable when a feed turns out to be broken half way through.
A mock would reproduce our assumptions about all three.

The archive is built in the test rather than checked in, so the awkward rows
it exists to cover -- a byte-order mark, an empty optional column, a stop time
at 25:30:00 -- are visible next to the assertions about them.

Run with: python tasks.py up && python tasks.py test-integration
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import timedelta
from pathlib import Path
from typing import Any

import psycopg
import pytest

from services.jobs.gtfs_static_loader import load_archive, main
from tests.gtfs_fixtures import build_static_gtfs_zip
from transitpulse_core.config import get_settings
from transitpulse_core.sinks.postgres import PostgresSink

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def sink() -> Iterator[PostgresSink]:
    instance = PostgresSink(get_settings())
    if not instance.healthcheck():
        pytest.skip("PostgreSQL is not reachable; run `python tasks.py up` first")
    yield instance
    instance.close()


@pytest.fixture(autouse=True)
def _clean_reference_tables(sink: PostgresSink) -> Iterator[None]:
    """Leave the reference tables empty either side of every test.

    They are shared with the processor's own integration module, which asserts
    that an unloaded timetable produces no delay -- so a timetable left behind
    here would fail a test somewhere else entirely.
    """
    yield
    with sink.pool.connection() as conn, conn.cursor() as cur:
        cur.execute("TRUNCATE gtfs_routes, gtfs_trips, gtfs_stops, gtfs_stop_times")


def counts(sink: PostgresSink) -> dict[str, int]:
    with sink.pool.connection() as conn, conn.cursor() as cur:
        result = {}
        for table in ("gtfs_routes", "gtfs_stops", "gtfs_trips", "gtfs_stop_times"):
            cur.execute(f"SELECT count(*) FROM {table}")  # noqa: S608 - fixed table list
            result[table] = (cur.fetchone() or [0])[0]
        return result


def query(sink: PostgresSink, sql: str, params: tuple[Any, ...] = ()) -> list[tuple[Any, ...]]:
    with sink.pool.connection() as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        return list(cur.fetchall())


class TestLoading:
    def test_every_table_is_populated(self, sink: PostgresSink, tmp_path: Path) -> None:
        load_archive(build_static_gtfs_zip(tmp_path / "gtfs.zip"), get_settings())

        assert counts(sink) == {
            "gtfs_routes": 2,
            "gtfs_stops": 2,
            "gtfs_trips": 2,
            "gtfs_stop_times": 4,
        }

    def test_the_byte_order_mark_does_not_reach_the_data(
        self, sink: PostgresSink, tmp_path: Path
    ) -> None:
        """The fixture writes routes.txt with a BOM, as real feeds do. Read as
        part of the first column's name, every lookup of ``route_id`` misses
        and the table loads empty -- or with a NULL primary key."""
        load_archive(build_static_gtfs_zip(tmp_path / "gtfs.zip"), get_settings())

        assert query(
            sink, "SELECT route_id, route_short_name FROM gtfs_routes ORDER BY route_id"
        ) == [
            ("B", "B"),
            ("Q", "Q"),
        ]

    def test_a_stop_time_past_midnight_round_trips_as_an_interval(
        self, sink: PostgresSink, tmp_path: Path
    ) -> None:
        """The reason these columns are INTERVAL and not TIME.

        ``25:30:00`` is 01:30 on the next calendar day, still belonging to the
        service day that began the morning before. A TIME column rejects it
        outright, and anything that silently wraps it to 01:30 makes every
        overnight trip look a day early.
        """
        load_archive(build_static_gtfs_zip(tmp_path / "gtfs.zip"), get_settings())

        rows = query(
            sink,
            "SELECT departure_time FROM gtfs_stop_times WHERE trip_id = %s AND stop_sequence = %s",
            ("NIGHT1", 2),
        )

        assert rows == [(timedelta(days=1, hours=1, minutes=30),)]

    def test_a_stop_without_times_loads_with_nulls(
        self, sink: PostgresSink, tmp_path: Path
    ) -> None:
        load_archive(build_static_gtfs_zip(tmp_path / "gtfs.zip"), get_settings())

        assert query(
            sink,
            "SELECT arrival_time, departure_time FROM gtfs_stop_times "
            "WHERE trip_id = %s AND stop_sequence = %s",
            ("NIGHT1", 3),
        ) == [(None, None)]

    def test_a_stop_without_coordinates_loads_with_nulls(
        self, sink: PostgresSink, tmp_path: Path
    ) -> None:
        load_archive(build_static_gtfs_zip(tmp_path / "gtfs.zip"), get_settings())

        assert query(
            sink, "SELECT stop_lat, stop_lon FROM gtfs_stops WHERE stop_id = %s", ("Q04N",)
        ) == [(None, None)]

    def test_loaded_at_is_stamped_by_the_database(self, sink: PostgresSink, tmp_path: Path) -> None:
        """COPY names only the GTFS columns, so the column default applies --
        which is what makes "how old is this timetable" answerable."""
        load_archive(build_static_gtfs_zip(tmp_path / "gtfs.zip"), get_settings())

        assert all(row[0] is not None for row in query(sink, "SELECT loaded_at FROM gtfs_routes"))


class TestIdempotence:
    def test_a_second_run_replaces_rather_than_duplicates(
        self, sink: PostgresSink, tmp_path: Path
    ) -> None:
        """The agency republishes the same archive routinely, and the job is
        scheduled rather than triggered by a change -- so a re-run has to be a
        replacement, not an append onto a primary key violation."""
        archive = build_static_gtfs_zip(tmp_path / "gtfs.zip")

        load_archive(archive, get_settings())
        first = counts(sink)
        load_archive(archive, get_settings())

        assert counts(sink) == first

    def test_a_removed_route_is_gone_after_a_reload(
        self, sink: PostgresSink, tmp_path: Path
    ) -> None:
        """An upsert would leave withdrawn routes behind forever, and a
        withdrawn route still joined to by enrichment is worse than none."""
        load_archive(build_static_gtfs_zip(tmp_path / "first.zip"), get_settings())
        load_archive(
            build_static_gtfs_zip(
                tmp_path / "second.zip",
                routes=[{"route_id": "Q", "route_short_name": "Q-express"}],
            ),
            get_settings(),
        )

        assert query(sink, "SELECT route_id, route_short_name FROM gtfs_routes") == [
            ("Q", "Q-express")
        ]


class TestAtomicity:
    def test_a_broken_feed_leaves_the_previous_timetable_intact(
        self, sink: PostgresSink, tmp_path: Path
    ) -> None:
        """The load is one transaction, so a feed that fails half way through
        rolls back to the previous timetable rather than leaving the pipeline
        with part of one. A partial timetable does not look like an error
        downstream -- it looks like trips that have no schedule.
        """
        load_archive(build_static_gtfs_zip(tmp_path / "good.zip"), get_settings())
        before = counts(sink)

        duplicate_key = [
            {
                "trip_id": "ABC123",
                "stop_sequence": "4",
                "stop_id": "Q05N",
                "arrival_time": "08:15:00",
                "departure_time": "08:15:00",
            }
        ] * 2
        broken = build_static_gtfs_zip(tmp_path / "broken.zip", stop_times=duplicate_key)

        with pytest.raises(psycopg.errors.UniqueViolation):
            load_archive(broken, get_settings())

        assert counts(sink) == before
        assert query(sink, "SELECT count(*) FROM gtfs_routes")[0][0] == 2


class TestEnrichmentLookup:
    def test_the_enrichment_join_finds_what_the_loader_wrote(
        self, sink: PostgresSink, tmp_path: Path
    ) -> None:
        """The two halves of the schedule feature meeting: the loader writes
        the interval and the sink's ``(trip_id, stop_sequence)`` lookup -- the
        one enrichment calls per event -- reads it back unchanged."""
        load_archive(build_static_gtfs_zip(tmp_path / "gtfs.zip"), get_settings())

        found = sink.fetch_scheduled_stop_times([("ABC123", 4), ("NIGHT1", 2), ("NOPE", 1)])

        assert found == {
            ("ABC123", 4): timedelta(hours=8, minutes=15, seconds=30),
            ("NIGHT1", 2): timedelta(days=1, hours=1, minutes=30),
        }

    def test_a_stop_with_only_an_arrival_time_still_resolves(
        self, sink: PostgresSink, tmp_path: Path
    ) -> None:
        """``departure_time`` falls back to ``arrival_time``: either one
        answers "when was this vehicle meant to be here", and a feed that
        carries only the arrival would otherwise have no schedule at all."""
        load_archive(
            build_static_gtfs_zip(
                tmp_path / "gtfs.zip",
                stop_times=[
                    {
                        "trip_id": "ARR",
                        "stop_sequence": "1",
                        "stop_id": "Q05N",
                        "arrival_time": "09:05:00",
                        "departure_time": "",
                    }
                ],
            ),
            get_settings(),
        )

        assert sink.fetch_scheduled_stop_times([("ARR", 1)]) == {
            ("ARR", 1): timedelta(hours=9, minutes=5)
        }

    def test_an_empty_lookup_makes_no_query(self, sink: PostgresSink) -> None:
        assert sink.fetch_scheduled_stop_times([]) == {}


class TestCommandLine:
    def test_loading_from_a_local_archive(
        self, sink: PostgresSink, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """``--from-file`` is how the job is rehearsed against a downloaded
        archive without re-downloading it, and how this suite avoids depending
        on an agency's CDN."""
        archive = build_static_gtfs_zip(tmp_path / "gtfs.zip")

        assert main(["--from-file", str(archive)]) == 0

        assert counts(sink)["gtfs_stop_times"] == 4
        assert "gtfs_stop_times: 4 rows" in capsys.readouterr().out

    def test_a_missing_archive_exits_non_zero(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """A scheduled job that failed has to fail visibly, or a stale
        timetable is indistinguishable from a fresh one."""
        assert main(["--from-file", str(tmp_path / "absent.zip")]) == 1
        assert "error:" in capsys.readouterr().err

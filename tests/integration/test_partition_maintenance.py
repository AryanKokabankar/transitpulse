"""Integration tests for partition maintenance against real PostgreSQL.

The unit tests cover the window arithmetic; these cover the things only
Postgres can answer: that a created partition actually accepts an insert, that
``DETACH``/``DROP`` removes one without disturbing the parent, and that the job
is safe to run twice.

Run with: python tasks.py up && python tasks.py test-integration
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, date, datetime, timedelta

import psycopg
import pytest

from services.jobs.partition_maintenance import (
    build_plan,
    existing_partitions,
    partition_day,
    run,
)
from tests.conftest import MakeEvent
from transitpulse_core.config import get_settings
from transitpulse_core.sinks.postgres import PostgresSink

pytestmark = pytest.mark.integration

# Deliberately in the PAST, not the future. Pruning drops every partition older
# than `today - retention_days`, so a fixture day in the future would make the
# real present look expired and delete the partitions the pipeline is actually
# using -- which is exactly what an earlier version of this file did to the
# local database. Anchored behind the project's own history, the drop cutoff can
# never reach a partition these tests did not create.
FIXTURE_DAY = date(2021, 6, 15)


@pytest.fixture
def conn() -> Iterator[psycopg.Connection]:
    settings = get_settings()
    try:
        connection = psycopg.connect(settings.pg_dsn_str, autocommit=True, connect_timeout=5)
    except psycopg.Error:
        pytest.skip("PostgreSQL is not reachable; run `python tasks.py up` first")
    yield connection
    connection.close()


@pytest.fixture(autouse=True)
def _drop_fixture_partitions(conn: psycopg.Connection) -> Iterator[None]:
    """Remove only the far-past partitions these tests create."""

    def cleanup() -> None:
        for name, day in existing_partitions(conn).items():
            if day.year <= FIXTURE_DAY.year:
                with conn.cursor() as cur:
                    cur.execute(f'ALTER TABLE vehicle_events DETACH PARTITION "{name}"')
                    cur.execute(f'DROP TABLE "{name}"')

    cleanup()
    yield
    cleanup()


class TestEnsure:
    def test_creates_the_whole_window(self, conn: psycopg.Connection) -> None:
        outcome = run(get_settings(), today=FIXTURE_DAY, ahead_days=5, retention_days=3650)
        present = existing_partitions(conn)
        plan = build_plan(FIXTURE_DAY, ahead_days=5, retention_days=3650)
        for day in plan.ensure_days:
            assert f"vehicle_events_{day:%Y%m%d}" in present
        assert outcome.created

    def test_is_idempotent(self, conn: psycopg.Connection) -> None:
        """Runs daily on a schedule, so a second run must be a no-op."""
        first = run(get_settings(), today=FIXTURE_DAY, ahead_days=5, retention_days=3650)
        second = run(get_settings(), today=FIXTURE_DAY, ahead_days=5, retention_days=3650)
        assert first.created
        assert second.created == []
        assert len(second.existing) >= len(first.created)

    def test_a_created_partition_accepts_an_insert(
        self, conn: psycopg.Connection, make_event: MakeEvent
    ) -> None:
        """The whole point. A partition that exists but will not take a row
        would leave the job reporting success while ingestion fails.
        """
        run(get_settings(), today=FIXTURE_DAY, ahead_days=2, retention_days=3650)

        event_time = datetime(
            FIXTURE_DAY.year, FIXTURE_DAY.month, FIXTURE_DAY.day, 12, 0, tzinfo=UTC
        )
        sink = PostgresSink(get_settings())
        try:
            ping = make_event().ping.model_copy(
                update={"event_time": event_time, "ingest_time": event_time}
            )
            event = make_event(ping=ping, idempotency_key=ping.idempotency_key)
            result = sink.write_batch([event])
            assert result.ok, result.errors
            assert result.written == 1
        finally:
            sink.close()

        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM vehicle_events WHERE event_time = %s", (event_time,))
            assert (cur.fetchone() or [0])[0] == 1

    def test_dry_run_changes_nothing(self, conn: psycopg.Connection) -> None:
        before = set(existing_partitions(conn))
        outcome = run(
            get_settings(), today=FIXTURE_DAY, ahead_days=5, retention_days=3650, dry_run=True
        )
        assert outcome.created, "dry run should still report what it would create"
        assert set(existing_partitions(conn)) == before


class TestPrune:
    def test_drops_a_partition_past_retention(self, conn: psycopg.Connection) -> None:
        old_day = FIXTURE_DAY - timedelta(days=400)
        with conn.cursor() as cur:
            cur.execute("SELECT ensure_event_partition(%s)", (old_day,))
        old_name = f"vehicle_events_{old_day:%Y%m%d}"
        assert old_name in existing_partitions(conn)

        outcome = run(get_settings(), today=FIXTURE_DAY, ahead_days=2, retention_days=30)
        assert old_name in outcome.dropped
        assert old_name not in existing_partitions(conn)

    def test_keeps_a_partition_inside_retention(self, conn: psycopg.Connection) -> None:
        recent_day = FIXTURE_DAY - timedelta(days=3)
        with conn.cursor() as cur:
            cur.execute("SELECT ensure_event_partition(%s)", (recent_day,))
        recent_name = f"vehicle_events_{recent_day:%Y%m%d}"

        outcome = run(get_settings(), today=FIXTURE_DAY, ahead_days=2, retention_days=30)
        assert recent_name not in outcome.dropped
        assert recent_name in existing_partitions(conn)

    def test_never_drops_the_current_day(self, conn: psycopg.Connection) -> None:
        """Even with an aggressive retention, today must survive -- dropping it
        would delete the partition the processor is writing to right now.
        """
        run(get_settings(), today=FIXTURE_DAY, ahead_days=2, retention_days=1)
        outcome = run(get_settings(), today=FIXTURE_DAY, ahead_days=2, retention_days=1)
        today_name = f"vehicle_events_{FIXTURE_DAY:%Y%m%d}"
        assert today_name not in outcome.dropped
        assert today_name in existing_partitions(conn)

    def test_dry_run_drops_nothing(self, conn: psycopg.Connection) -> None:
        old_day = FIXTURE_DAY - timedelta(days=400)
        with conn.cursor() as cur:
            cur.execute("SELECT ensure_event_partition(%s)", (old_day,))
        old_name = f"vehicle_events_{old_day:%Y%m%d}"

        outcome = run(
            get_settings(), today=FIXTURE_DAY, ahead_days=2, retention_days=30, dry_run=True
        )
        assert old_name in outcome.dropped, "dry run should report the intended drop"
        assert old_name in existing_partitions(conn), "dry run must not actually drop"


class TestDiscovery:
    def test_only_reports_attached_partitions(self, conn: psycopg.Connection) -> None:
        """A look-alike table that is not attached must be invisible to the
        job, because the job's next step after discovery is DROP.
        """
        decoy = "vehicle_events_20210101_backup"
        with conn.cursor() as cur:
            cur.execute(f'CREATE TABLE IF NOT EXISTS "{decoy}" (id int)')
        try:
            assert decoy not in existing_partitions(conn)
            assert partition_day(decoy) is None
        finally:
            with conn.cursor() as cur:
                cur.execute(f'DROP TABLE IF EXISTS "{decoy}"')

    def test_every_reported_name_parses_to_its_day(self, conn: psycopg.Connection) -> None:
        for name, day in existing_partitions(conn).items():
            assert partition_day(name) == day

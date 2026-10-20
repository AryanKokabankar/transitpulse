"""Integration tests for the schema and the Postgres sink.

Against a real PostgreSQL, because every behaviour asserted here is the
database's behaviour, not ours: declarative partitioning, ``ON CONFLICT``
semantics, and the out-of-order guard on the serving table. A mock or an
in-memory substitute would reproduce our assumptions rather than test them.

Run with: python tasks.py up && python tasks.py test-integration
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta

import pytest

from tests.conftest import MakeEvent, MakePing
from transitpulse_core.config import get_settings
from transitpulse_core.models import QuarantineReason, QuarantineRecord
from transitpulse_core.sinks.postgres import PostgresSink

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def sink() -> Iterator[PostgresSink]:
    s = PostgresSink(get_settings())
    if not s.healthcheck():
        pytest.skip("PostgreSQL is not reachable; run `python tasks.py up` first")
    yield s
    s.close()


@pytest.fixture(autouse=True)
def _clean_tables(sink: PostgresSink) -> Iterator[None]:
    """Each test starts from empty tables but keeps the schema."""
    with sink.pool.connection() as conn, conn.cursor() as cur:
        cur.execute("TRUNCATE vehicle_events, vehicle_live, quarantine")
    yield


class TestSchemaShape:
    def test_vehicle_events_is_partitioned(self, sink: PostgresSink) -> None:
        with sink.pool.connection() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT partstrat FROM pg_partitioned_table
                WHERE partrelid = 'vehicle_events'::regclass
                """
            )
            row = cur.fetchone()
        assert row is not None, "vehicle_events is not a partitioned table"
        assert row[0] == "r", "expected RANGE partitioning"

    def test_partitions_exist_for_today(self, sink: PostgresSink) -> None:
        """An INSERT with no matching partition fails outright, so the
        migration must pre-create a usable window."""
        today = datetime.now(UTC).date()
        expected = f"vehicle_events_{today:%Y%m%d}"
        with sink.pool.connection() as conn, conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_class WHERE relname = %s", (expected,))
            assert cur.fetchone() is not None, f"missing partition {expected}"

    def test_partition_helper_is_idempotent(self, sink: PostgresSink) -> None:
        future = datetime.now(UTC).date() + timedelta(days=90)
        with sink.pool.connection() as conn, conn.cursor() as cur:
            cur.execute("SELECT ensure_event_partition(%s)", (future,))
            first = (cur.fetchone() or [None])[0]
            cur.execute("SELECT ensure_event_partition(%s)", (future,))
            second = (cur.fetchone() or [None])[0]
        assert first == second == f"vehicle_events_{future:%Y%m%d}"

    def test_expected_indexes_exist(self, sink: PostgresSink) -> None:
        with sink.pool.connection() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT indexname FROM pg_indexes WHERE tablename IN "
                "('vehicle_events', 'vehicle_live', 'quarantine')"
            )
            names = {row[0] for row in cur.fetchall()}
        assert {"ix_live_route", "ix_quarantine_pending"} <= names


class TestIdempotentWrites:
    def test_writes_a_batch(self, sink: PostgresSink, make_event: MakeEvent) -> None:
        result = sink.write_batch([make_event()])
        assert result.ok
        assert result.written == 1

    def test_rewriting_the_same_batch_dedupes(
        self, sink: PostgresSink, make_event: MakeEvent
    ) -> None:
        """The property the replay harness depends on: re-processing the same
        observation must not create a second history row."""
        events = [make_event()]
        first = sink.write_batch(events)
        second = sink.write_batch(events)

        assert first.written == 1
        assert second.written == 0
        assert second.deduped == 1
        assert second.ok, "a duplicate is a success, not a failure"

        with sink.pool.connection() as conn, conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM vehicle_events")
            assert (cur.fetchone() or [0])[0] == 1

    def test_live_row_is_upserted_not_duplicated(
        self, sink: PostgresSink, make_event: MakeEvent, make_ping: MakePing
    ) -> None:
        base = make_ping()
        later = make_ping(
            event_time=base.event_time + timedelta(seconds=30),
            ingest_time=base.ingest_time + timedelta(seconds=30),
            latitude=40.7200,
        )
        sink.write_batch([make_event(ping=base, idempotency_key=base.idempotency_key)])
        sink.write_batch([make_event(ping=later, idempotency_key=later.idempotency_key)])

        with sink.pool.connection() as conn, conn.cursor() as cur:
            cur.execute("SELECT count(*), max(latitude) FROM vehicle_live")
            count, latitude = cur.fetchone() or (0, None)
        assert count == 1, "vehicle_live must hold one row per vehicle"
        assert latitude == pytest.approx(40.7200)

    def test_stale_ping_cannot_move_a_vehicle_backwards(
        self, sink: PostgresSink, make_event: MakeEvent, make_ping: MakePing
    ) -> None:
        """The WHERE guard on the live upsert. Without it, a late-arriving
        stale ping overwrites a fresher position and the map jumps backwards.
        """
        fresh = make_ping(latitude=40.7500)
        stale = make_ping(
            event_time=fresh.event_time - timedelta(minutes=5),
            latitude=40.1000,
        )
        sink.write_batch([make_event(ping=fresh, idempotency_key=fresh.idempotency_key)])
        sink.write_batch([make_event(ping=stale, idempotency_key=stale.idempotency_key)])

        with sink.pool.connection() as conn, conn.cursor() as cur:
            cur.execute("SELECT latitude FROM vehicle_live")
            latitude = (cur.fetchone() or [None])[0]
        assert latitude == pytest.approx(40.7500), "stale ping overwrote a fresher position"

        with sink.pool.connection() as conn, conn.cursor() as cur:
            # The history still records both observations; only the serving
            # row is guarded.
            cur.execute("SELECT count(*) FROM vehicle_events")
            assert (cur.fetchone() or [0])[0] == 2


class TestQuarantine:
    def test_persists_the_raw_payload_and_trace_id(self, sink: PostgresSink) -> None:
        written = sink.write_quarantine(
            [
                QuarantineRecord(
                    reason=QuarantineReason.DECODE_FAILURE,
                    detail="truncated protobuf at byte 41",
                    agency="mta-nyct",
                    raw_payload=b"\x08\x96\x01broken",
                    message_id="msg-1",
                    delivery_attempt=3,
                    trace_id="4bf92f3577b34da6a3ce929d0e0e4736",
                )
            ]
        )
        assert written == 1

        with sink.pool.connection() as conn, conn.cursor() as cur:
            cur.execute("SELECT reason, raw_payload, trace_id, replayed_at FROM quarantine")
            reason, payload, trace_id, replayed_at = cur.fetchone() or (None,) * 4

        assert reason == "DECODE_FAILURE"
        assert bytes(payload) == b"\x08\x96\x01broken"
        assert trace_id == "4bf92f3577b34da6a3ce929d0e0e4736"
        assert replayed_at is None, "a new quarantine row must start unreplayed"


class TestBatchReads:
    def test_fetch_last_positions_returns_one_row_per_vehicle(
        self, sink: PostgresSink, make_event: MakeEvent
    ) -> None:
        """Batched so enrichment is one query per batch, not one per event."""
        event = make_event()
        sink.write_batch([event])
        positions = sink.fetch_last_positions("mta-nyct", [event.ping.vehicle_id, "absent"])
        assert set(positions) == {event.ping.vehicle_id}

    def test_fetch_last_positions_handles_an_empty_list(self, sink: PostgresSink) -> None:
        # A batch of all-new vehicles must not produce `WHERE id = ANY('{}')`
        # round trips.
        assert sink.fetch_last_positions("mta-nyct", []) == {}

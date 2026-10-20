"""PostgreSQL sink -- the operational serving layer.

Deliberately raw psycopg3 rather than an ORM. The schema lives in hand-written
Alembic migrations under ``migrations/versions/``, and this hot write path
builds one multi-row ``INSERT`` per batch over a pooled connection. On a
500-row batch that is roughly an order of magnitude fewer round trips than
per-row ORM flushes, and the emitted SQL is readable verbatim in
``pg_stat_statements`` when something is slow. ADR-003 records the trade-off.

Two conflict strategies live here, and the difference matters:

* ``vehicle_events`` -- ``ON CONFLICT DO NOTHING`` on the idempotency key.
  Replays and agency re-sends become no-ops, and the row count difference
  gives us the dedup metric for free.
* ``vehicle_live`` -- ``ON CONFLICT DO UPDATE ... WHERE excluded.event_time >
  vehicle_live.event_time``. Without that ``WHERE``, a late-arriving stale ping
  would overwrite a fresher position and the live map would jump backwards.
"""

from __future__ import annotations

import time
from collections.abc import Sequence
from datetime import timedelta
from typing import Any

import structlog
from opentelemetry.trace import SpanKind
from psycopg import sql
from psycopg_pool import ConnectionPool

from transitpulse_core.config import Settings, get_settings
from transitpulse_core.models import EnrichedVehicleEvent, QuarantineRecord
from transitpulse_core.sinks import WriteResult
from transitpulse_core.telemetry import get_metrics, get_tracer

log = structlog.get_logger(__name__)

_EVENT_COLUMNS = (
    "idempotency_key",
    "agency",
    "vehicle_id",
    "trip_id",
    "route_id",
    "route_short_name",
    "direction_id",
    "start_date",
    "schedule_relationship",
    "latitude",
    "longitude",
    "bearing",
    "reported_speed_mps",
    "computed_speed_mps",
    "distance_from_last_m",
    "seconds_since_last_ping",
    "schedule_delay_seconds",
    "is_on_time",
    "current_stop_sequence",
    "stop_id",
    "occupancy_status",
    "event_time",
    "ingest_time",
    "processed_time",
)


class PostgresSink:
    """Pooled, batching writer for the serving tables."""

    name = "postgres"

    def __init__(self, settings: Settings | None = None, *, open_pool: bool = True) -> None:
        self._settings = settings or get_settings()
        self._metrics = get_metrics()
        self._tracer = get_tracer()
        self._pool = ConnectionPool(
            conninfo=self._settings.pg_dsn_str,
            min_size=self._settings.pg_pool_min,
            max_size=self._settings.pg_pool_max,
            open=open_pool,
            timeout=10.0,
            # Recycle connections so a long-lived Cloud Run instance does not
            # hold a stale socket after Cloud SQL restarts underneath it.
            max_lifetime=30 * 60.0,
        )

    @property
    def pool(self) -> ConnectionPool:
        """The shared pool, for readers that need their own SQL.

        Exposed deliberately: the serving API's queries are presentation
        concerns and do not belong in the sink, but they must share one pool
        with it rather than opening a second set of connections.
        """
        return self._pool

    # ------------------------------------------------------------------
    # writes
    # ------------------------------------------------------------------
    def write_batch(self, events: list[EnrichedVehicleEvent]) -> WriteResult:
        if not events:
            return WriteResult(sink=self.name)

        with self._tracer.start_as_current_span(
            "postgres.write_batch",
            kind=SpanKind.CLIENT,
            attributes={"db.system": "postgresql", "batch.size": len(events)},
        ) as span:
            started = time.perf_counter()
            try:
                # Both statements share one transaction: the history row and
                # the live row for a batch either both land or neither does,
                # so the serving table can never claim a position the history
                # has no record of.
                with self._pool.connection() as conn, conn.transaction():
                    inserted = self._insert_events(conn, events)
                    live_updated = self._upsert_live(conn, events)

                elapsed = time.perf_counter() - started
                deduped = len(events) - inserted
                span.set_attribute("db.rows_inserted", inserted)
                span.set_attribute("db.rows_deduped", deduped)
                span.set_attribute("db.live_rows_updated", live_updated)

                self._metrics.sink_write_duration.record(
                    elapsed, {"sink": self.name, "outcome": "ok"}
                )
                if deduped:
                    self._metrics.events_deduped.add(deduped, {"sink": self.name})

                return WriteResult(sink=self.name, written=inserted, deduped=deduped)
            except Exception as exc:
                self._metrics.sink_write_duration.record(
                    time.perf_counter() - started, {"sink": self.name, "outcome": "error"}
                )
                span.record_exception(exc)
                log.error("postgres.write_failed", batch_size=len(events), exc_info=True)
                return WriteResult(sink=self.name, failed=len(events), errors=[str(exc)])

    def _insert_events(self, conn: Any, events: Sequence[EnrichedVehicleEvent]) -> int:
        statement = sql.SQL(
            """
            INSERT INTO vehicle_events ({cols})
            VALUES ({placeholders})
            ON CONFLICT (idempotency_key, event_time) DO NOTHING
            """
        ).format(
            cols=sql.SQL(", ").join(sql.Identifier(c) for c in _EVENT_COLUMNS),
            placeholders=sql.SQL(", ").join(sql.Placeholder() * len(_EVENT_COLUMNS)),
        )
        rows = [self._event_row(e) for e in events]
        with conn.cursor() as cur:
            cur.executemany(statement, rows)
            # psycopg3 reports the total affected across an executemany run,
            # which is exactly the inserted-row count given DO NOTHING.
            return max(0, int(cur.rowcount))

    def _upsert_live(self, conn: Any, events: Sequence[EnrichedVehicleEvent]) -> int:
        """Keep one current row per vehicle, never moving it backwards in time."""
        statement = """
            INSERT INTO vehicle_live (
                agency, vehicle_id, trip_id, route_id, route_short_name,
                latitude, longitude, bearing, computed_speed_mps,
                schedule_delay_seconds, is_on_time, stop_id,
                current_stop_sequence, occupancy_status,
                event_time, processed_time
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (agency, vehicle_id) DO UPDATE SET
                trip_id                = EXCLUDED.trip_id,
                route_id               = EXCLUDED.route_id,
                route_short_name       = EXCLUDED.route_short_name,
                latitude               = EXCLUDED.latitude,
                longitude              = EXCLUDED.longitude,
                bearing                = EXCLUDED.bearing,
                computed_speed_mps     = EXCLUDED.computed_speed_mps,
                schedule_delay_seconds = EXCLUDED.schedule_delay_seconds,
                is_on_time             = EXCLUDED.is_on_time,
                stop_id                = EXCLUDED.stop_id,
                current_stop_sequence  = EXCLUDED.current_stop_sequence,
                occupancy_status       = EXCLUDED.occupancy_status,
                event_time             = EXCLUDED.event_time,
                processed_time         = EXCLUDED.processed_time
            WHERE EXCLUDED.event_time > vehicle_live.event_time
        """
        rows = []
        for e in events:
            p = e.ping
            rows.append(
                (
                    p.agency,
                    p.vehicle_id,
                    p.trip_id,
                    p.route_id,
                    e.route_short_name,
                    p.latitude,
                    p.longitude,
                    p.bearing,
                    e.computed_speed_mps,
                    e.schedule_delay_seconds,
                    e.is_on_time,
                    p.stop_id,
                    p.current_stop_sequence,
                    str(p.occupancy_status),
                    p.event_time,
                    e.processed_time,
                )
            )
        with conn.cursor() as cur:
            cur.executemany(statement, rows)
            return max(0, int(cur.rowcount))

    def write_quarantine(self, records: Sequence[QuarantineRecord]) -> int:
        """Persist rejected events with their raw payload and trace id."""
        if not records:
            return 0
        statement = """
            INSERT INTO quarantine (
                reason, detail, agency, vehicle_id, raw_payload,
                message_id, delivery_attempt, trace_id, quarantined_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        rows = [
            (
                str(r.reason),
                r.detail,
                r.agency,
                r.vehicle_id,
                r.raw_payload,
                r.message_id,
                r.delivery_attempt,
                r.trace_id,
                r.quarantined_at,
            )
            for r in records
        ]
        with self._pool.connection() as conn, conn.cursor() as cur:
            cur.executemany(statement, rows)
        for r in records:
            self._metrics.events_quarantined.add(1, {"reason": str(r.reason)})
        return len(rows)

    # ------------------------------------------------------------------
    # reads used by enrichment and the serving API
    # ------------------------------------------------------------------
    def fetch_last_positions(
        self, agency: str, vehicle_ids: Sequence[str]
    ) -> dict[str, tuple[float, float, Any]]:
        """Previous position per vehicle, for the motion delta.

        One query for the whole batch rather than one per event -- the N+1 here
        was the first bottleneck the traces exposed (see BENCHMARKS.md).
        """
        if not vehicle_ids:
            return {}
        with self._pool.connection() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT vehicle_id, latitude, longitude, event_time
                FROM vehicle_live
                WHERE agency = %s AND vehicle_id = ANY(%s)
                """,
                (agency, list(vehicle_ids)),
            )
            return {row[0]: (row[1], row[2], row[3]) for row in cur.fetchall()}

    def fetch_scheduled_stop_times(
        self, keys: Sequence[tuple[str, int]]
    ) -> dict[tuple[str, int], timedelta]:
        """Scheduled departure per ``(trip_id, stop_sequence)``.

        ``unnest`` of two parallel arrays rather than an ``IN`` list of tuples:
        it keeps the statement text constant whatever the batch size, so
        Postgres can reuse the plan and ``pg_stat_statements`` shows one entry
        instead of one per distinct batch length. The join hits
        ``ix_stop_times_lookup`` directly.

        ``departure_time`` falls back to ``arrival_time`` because a frequency-
        based or non-timepoint stop may carry only one of the two, and either
        answers "when was this vehicle meant to be here".
        """
        if not keys:
            return {}
        trip_ids = [trip_id for trip_id, _ in keys]
        sequences = [sequence for _, sequence in keys]
        with self._pool.connection() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT st.trip_id,
                       st.stop_sequence,
                       COALESCE(st.departure_time, st.arrival_time)
                FROM gtfs_stop_times AS st
                JOIN unnest(%s::text[], %s::int[]) AS wanted(trip_id, stop_sequence)
                  ON st.trip_id = wanted.trip_id
                 AND st.stop_sequence = wanted.stop_sequence
                """,
                (trip_ids, sequences),
            )
            return {(row[0], row[1]): row[2] for row in cur.fetchall() if row[2] is not None}

    def fetch_route_names(self, route_ids: Sequence[str]) -> dict[str, str]:
        """Static-GTFS route lookup for enrichment."""
        if not route_ids:
            return {}
        with self._pool.connection() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT route_id, route_short_name FROM gtfs_routes WHERE route_id = ANY(%s)",
                (list(route_ids),),
            )
            return {row[0]: row[1] for row in cur.fetchall()}

    def healthcheck(self) -> bool:
        try:
            with self._pool.connection(timeout=2.0) as conn, conn.cursor() as cur:
                cur.execute("SELECT 1")
                return cur.fetchone() is not None
        except Exception:  # noqa: BLE001 - health probes must not raise
            return False

    # ------------------------------------------------------------------
    def _event_row(self, e: EnrichedVehicleEvent) -> tuple[Any, ...]:
        p = e.ping
        return (
            e.idempotency_key,
            p.agency,
            p.vehicle_id,
            p.trip_id,
            p.route_id,
            e.route_short_name,
            p.direction_id,
            p.start_date,
            str(p.schedule_relationship),
            p.latitude,
            p.longitude,
            p.bearing,
            p.reported_speed_mps,
            e.computed_speed_mps,
            e.distance_from_last_m,
            e.seconds_since_last_ping,
            e.schedule_delay_seconds,
            e.is_on_time,
            p.current_stop_sequence,
            p.stop_id,
            str(p.occupancy_status),
            p.event_time,
            p.ingest_time,
            e.processed_time,
        )

    def flush(self) -> None:
        """No client-side buffer to flush; writes are synchronous per batch."""

    def close(self) -> None:
        self._pool.close()

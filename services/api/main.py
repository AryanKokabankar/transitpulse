"""Serving API -- reads the live positions the processor maintains.

This is the one service that reads rather than writes, and it reads only from
``vehicle_live``: one indexed row per vehicle instead of a scan over the
partitioned history. That split is the whole reason the pipeline writes to two
stores (see ``libs/transitpulse_core/sinks/__init__.py``).

Readiness is gated on the database because an instance that cannot reach
Postgres cannot serve any endpoint here -- unlike the collector, which is still
useful while the database is down.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, ConfigDict

from services.health import build_health_router
from transitpulse_core.config import get_settings
from transitpulse_core.sinks.postgres import PostgresSink
from transitpulse_core.telemetry import setup_telemetry

SERVICE_NAME = "transitpulse-api"

_sink: PostgresSink | None = None


def get_sink() -> PostgresSink:
    if _sink is None:  # pragma: no cover - set during lifespan
        raise RuntimeError("database pool is not initialised")
    return _sink


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    global _sink
    setup_telemetry(SERVICE_NAME)

    # Instrumenting the app inside lifespan rather than at import time keeps
    # the module importable in tests without a tracer provider configured.
    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

    FastAPIInstrumentor.instrument_app(app, excluded_urls="healthz,readyz")

    _sink = PostgresSink()
    try:
        yield
    finally:
        _sink.close()
        _sink = None


app = FastAPI(
    title="TransitPulse API",
    version="0.1.0",
    summary="Live transit vehicle positions and route reliability",
    lifespan=lifespan,
)
app.include_router(build_health_router(SERVICE_NAME, readiness=lambda: get_sink().healthcheck()))


class LiveVehicle(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    agency: str
    vehicle_id: str
    route_id: str | None
    route_short_name: str | None
    latitude: float
    longitude: float
    bearing: float | None
    computed_speed_mps: float | None
    schedule_delay_seconds: int | None
    is_on_time: bool | None
    event_time: datetime
    staleness_seconds: float


@app.get("/vehicles/live", response_model=list[LiveVehicle])
def live_vehicles(
    agency: str = Query(default="mbta", max_length=64),
    route_id: str | None = Query(default=None, max_length=64),
    max_staleness_seconds: int = Query(default=300, ge=1, le=86_400),
    limit: int = Query(default=500, ge=1, le=5000),
) -> list[LiveVehicle]:
    """Current position of every vehicle reporting recently enough to trust.

    ``max_staleness_seconds`` exists because a vehicle that stopped reporting
    an hour ago is still a row in ``vehicle_live``; returning it unqualified
    would put ghost buses on a map. The caller decides how stale is too stale.
    """
    sink = get_sink()
    conditions = ["agency = %s", "event_time > now() - make_interval(secs => %s)"]
    params: list[object] = [agency, max_staleness_seconds]
    if route_id is not None:
        conditions.append("route_id = %s")
        params.append(route_id)
    params.append(limit)

    sql = f"""
        SELECT agency, vehicle_id, route_id, route_short_name,
               latitude, longitude, bearing, computed_speed_mps,
               schedule_delay_seconds, is_on_time, event_time,
               EXTRACT(EPOCH FROM (now() - event_time)) AS staleness_seconds
        FROM vehicle_live
        WHERE {" AND ".join(conditions)}
        ORDER BY event_time DESC
        LIMIT %s
    """  # noqa: S608 - conditions are fixed literals; values stay parameterised

    with sink.pool.connection() as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        columns = [c.name for c in cur.description or []]
        return [LiveVehicle(**dict(zip(columns, row, strict=True))) for row in cur.fetchall()]


@app.get("/routes/{route_id}/reliability")
def route_reliability(
    route_id: str,
    hours: int = Query(default=1, ge=1, le=24),
) -> dict[str, object]:
    """On-time performance for a route over the recent window.

    Served from the partitioned history: the ``event_time`` predicate lets
    Postgres prune to the day partitions actually involved rather than scanning
    the whole table.
    """
    sink = get_sink()
    with sink.pool.connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT count(*)                                   AS samples,
                   count(*) FILTER (WHERE is_on_time)         AS on_time,
                   avg(schedule_delay_seconds)                AS avg_delay,
                   percentile_cont(0.95) WITHIN GROUP (
                       ORDER BY schedule_delay_seconds
                   )                                          AS p95_delay,
                   avg(computed_speed_mps)                    AS avg_speed
            FROM vehicle_events
            WHERE route_id = %s
              AND event_time > now() - make_interval(hours => %s)
            """,
            (route_id, hours),
        )
        row = cur.fetchone()

    if row is None or row[0] == 0:
        raise HTTPException(
            status_code=404,
            detail=f"no events for route {route_id!r} in the last {hours}h",
        )

    samples, on_time, avg_delay, p95_delay, avg_speed = row
    return {
        "route_id": route_id,
        "window_hours": hours,
        "samples": samples,
        "on_time_pct": round(100.0 * (on_time or 0) / samples, 2),
        "avg_delay_seconds": round(float(avg_delay), 1) if avg_delay is not None else None,
        "p95_delay_seconds": round(float(p95_delay), 1) if p95_delay is not None else None,
        "avg_speed_mps": round(float(avg_speed), 2) if avg_speed is not None else None,
    }


@app.get("/stats/pipeline")
def pipeline_stats() -> dict[str, object]:
    """Counts the dashboards cannot derive from metrics alone.

    Metrics tell you the *rate* of quarantine; this tells you how much is
    sitting there right now waiting to be replayed.
    """
    sink = get_sink()
    with sink.pool.connection() as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM vehicle_live")
        live_vehicles_count = (cur.fetchone() or [0])[0]
        cur.execute("SELECT count(*) FROM quarantine WHERE replayed_at IS NULL")
        pending_quarantine = (cur.fetchone() or [0])[0]
        cur.execute(
            """
            SELECT reason, count(*)
            FROM quarantine
            WHERE replayed_at IS NULL
            GROUP BY reason
            ORDER BY 2 DESC
            """
        )
        by_reason = {row[0]: row[1] for row in cur.fetchall()}

    return {
        "env": get_settings().env,
        "live_vehicles": live_vehicles_count,
        "pending_quarantine": pending_quarantine,
        "quarantine_by_reason": by_reason,
    }

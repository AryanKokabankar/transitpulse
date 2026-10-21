"""Shared test fixtures and environment bootstrap.

Imports come first and the environment is configured immediately after, which
is safe because nothing in ``transitpulse_core`` reads the environment at
import time -- :func:`get_settings` is lazily cached, and the autouse fixture
below clears that cache around every test. So the ordering that matters is
"before the first ``get_settings()`` call", not "before the first import".
"""

from __future__ import annotations

import os
from collections.abc import Callable, Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from dotenv import load_dotenv

from transitpulse_core.config import Settings, get_settings
from transitpulse_core.models import (
    EnrichedVehicleEvent,
    OccupancyStatus,
    ScheduleRelationship,
    VehiclePing,
)

# Variables without the TP_ prefix -- PUBSUB_EMULATOR_HOST, OTEL_* -- are read
# straight from the process environment by their own SDKs rather than by
# Settings, so loading .env here is what lets a bare `pytest` reach the local
# stack. Without it the integration tests would need the emulator host passed
# on the command line every time, and a forgotten export would silently point
# them at real Pub/Sub.
load_dotenv(Path(__file__).resolve().parents[1] / ".env", override=False)

# Forced, not setdefault: .env sets TP_OTEL_ENABLED=true for normal local runs
# and has just been loaded above, so a setdefault would leave export switched
# on and every test would pay OTLP connection backoff.
os.environ["TP_OTEL_ENABLED"] = "false"
os.environ.setdefault("TP_ENV", "local")

# Fixed for the duration of a run, and always a few minutes in the past.
#
# Three constraints have to hold at once, and the first two attempts each
# satisfied two of them:
#
# 1. Stable within a run, so idempotency keys derived from it do not change
#    between two calls in the same test.
# 2. Inside the maintained partition window. vehicle_events is partitioned by
#    day and an insert outside every partition fails outright, so the original
#    hardcoded 2026-10-15 worked only while the database had been migrated that
#    week and then failed with "no partition of relation found for row".
# 3. Earlier than the real clock. Code under test stamps processed_time from
#    datetime.now(), and assertions compare it against the fixture's
#    ingest_time, so a fixture in the future makes those comparisons fail.
#    Pinning the time of day to 14:30 UTC broke this: the suite passed only
#    when run after 14:30 UTC and failed before it -- an intermittent failure
#    that depends on what time of day CI happens to run.
#
# Offsetting backwards from now satisfies all three. Crossing UTC midnight is
# fine: the offset lands on the previous day, and the partition window always
# includes yesterday for exactly this kind of late arrival.
FIXED_NOW = (datetime.now(UTC) - timedelta(minutes=5)).replace(microsecond=0)


@pytest.fixture(autouse=True)
def _reset_settings_cache() -> Iterator[None]:
    """Stop a settings singleton leaking between tests that patch the env."""
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


@pytest.fixture
def settings() -> Settings:
    return get_settings()


# Exported so test modules can annotate the fixture without redeclaring it.
MakePing = Callable[..., VehiclePing]
MakeEvent = Callable[..., EnrichedVehicleEvent]


@pytest.fixture
def make_ping() -> MakePing:
    """Factory for valid pings; override any field per test."""

    def _make(**overrides: object) -> VehiclePing:
        defaults: dict[str, object] = {
            "agency": "mta-nyct",
            "vehicle_id": "MTA NYCT_7801",
            "trip_id": "ABC123",
            "route_id": "Q",
            "direction_id": 0,
            "start_date": FIXED_NOW.strftime("%Y%m%d"),
            "schedule_relationship": ScheduleRelationship.SCHEDULED,
            "latitude": 40.7128,
            "longitude": -74.0060,
            "bearing": 93.5,
            "reported_speed_mps": 11.2,
            "current_stop_sequence": 4,
            "stop_id": "Q05N",
            "occupancy_status": OccupancyStatus.FEW_SEATS_AVAILABLE,
            "event_time": FIXED_NOW,
            "ingest_time": FIXED_NOW + timedelta(milliseconds=450),
        }
        defaults.update(overrides)
        return VehiclePing(**defaults)  # type: ignore[arg-type]

    return _make


@pytest.fixture
def make_event(make_ping: MakePing) -> MakeEvent:
    """Factory for enriched events wrapping a valid ping."""

    def _make(**overrides: object) -> EnrichedVehicleEvent:
        ping = overrides.pop("ping", None) or make_ping()
        defaults: dict[str, object] = {
            "ping": ping,
            "idempotency_key": ping.idempotency_key,
            "computed_speed_mps": 10.8,
            "distance_from_last_m": 324.0,
            "seconds_since_last_ping": 30.0,
            "schedule_delay_seconds": 42,
            "is_on_time": True,
            "route_short_name": "Q",
            "processed_time": ping.ingest_time + timedelta(milliseconds=120),
        }
        defaults.update(overrides)
        return EnrichedVehicleEvent(**defaults)  # type: ignore[arg-type]

    return _make

"""Tests for the data contract.

These assert the properties the rest of the pipeline depends on rather than
merely that the models construct: the idempotency key must be stable across
replays, bad coordinates must be rejected at the boundary, and naive datetimes
must never get in.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from tests.conftest import FIXED_NOW
from transitpulse_core.models import (
    SCHEMA_VERSION,
    EnrichedVehicleEvent,
    QuarantineReason,
    QuarantineRecord,
    VehiclePing,
)

MakePing = Callable[..., VehiclePing]


class TestIdempotencyKey:
    def test_is_stable_for_the_same_observation(self, make_ping: MakePing) -> None:
        assert make_ping().idempotency_key == make_ping().idempotency_key

    def test_ignores_ingest_time(self, make_ping: MakePing) -> None:
        """The key must survive a replay, where ingest_time necessarily differs.

        This is the property that makes the whole replay harness safe: feeding
        the corpus back through the pipeline must not create duplicate rows.
        """
        original = make_ping()
        replayed = make_ping(ingest_time=FIXED_NOW + timedelta(days=30))
        assert original.idempotency_key == replayed.idempotency_key

    def test_ignores_derived_and_mutable_fields(self, make_ping: MakePing) -> None:
        """Agencies revise occupancy and stop data for a ping already sent."""
        assert (
            make_ping().idempotency_key
            == make_ping(occupancy_status="FULL", bearing=12.0).idempotency_key
        )

    @pytest.mark.parametrize(
        ("field", "value"),
        [
            ("vehicle_id", "MTA NYCT_9999"),
            ("agency", "other-agency"),
            ("event_time", FIXED_NOW + timedelta(seconds=1)),
        ],
    )
    def test_changes_with_identity_fields(
        self, make_ping: MakePing, field: str, value: object
    ) -> None:
        assert make_ping().idempotency_key != make_ping(**{field: value}).idempotency_key

    def test_is_timezone_independent(self, make_ping: MakePing) -> None:
        """The same instant expressed in another offset is the same observation."""
        in_utc = make_ping(event_time=datetime(2026, 10, 15, 14, 30, tzinfo=UTC))
        in_est = make_ping(
            event_time=datetime(2026, 10, 15, 9, 30, tzinfo=timezone(-timedelta(hours=5)))
        )
        assert in_utc.idempotency_key == in_est.idempotency_key


class TestCoordinateValidation:
    def test_rejects_null_island(self, make_ping: MakePing) -> None:
        """(0, 0) is the most common GPS failure value in transit feeds."""
        with pytest.raises(ValidationError, match="null-island"):
            make_ping(latitude=0.0, longitude=0.0)

    def test_accepts_a_genuine_position_near_the_equator(self, make_ping: MakePing) -> None:
        # Only exactly-zero is rejected; a real vehicle at 0.5N must get through.
        assert make_ping(latitude=0.5, longitude=0.5).latitude == 0.5

    @pytest.mark.parametrize(
        ("lat", "lon"),
        [(91.0, 0.5), (-91.0, 0.5), (40.0, 181.0), (40.0, -181.0)],
    )
    def test_rejects_out_of_range(self, make_ping: MakePing, lat: float, lon: float) -> None:
        with pytest.raises(ValidationError):
            make_ping(latitude=lat, longitude=lon)


class TestTimestampHandling:
    def test_rejects_naive_event_time(self, make_ping: MakePing) -> None:
        """Assuming UTC for a naive timestamp hides DST-sized lag bugs."""
        with pytest.raises(ValidationError, match="timezone-aware"):
            make_ping(event_time=datetime(2026, 10, 15, 14, 30))  # noqa: DTZ001

    def test_normalises_to_utc(self, make_ping: MakePing) -> None:
        ping = make_ping(
            event_time=datetime(2026, 10, 15, 9, 30, tzinfo=timezone(-timedelta(hours=5)))
        )
        assert ping.event_time.tzinfo is UTC
        assert ping.event_time.hour == 14

    def test_ingest_lag(self, make_ping: MakePing) -> None:
        assert make_ping().ingest_lag_seconds == pytest.approx(0.45)


class TestEnrichedEvent:
    def test_end_to_end_lag_spans_event_to_processed(
        self, make_event: Callable[..., EnrichedVehicleEvent]
    ) -> None:
        # 450ms ingest + 120ms processing
        assert make_event().end_to_end_lag_seconds == pytest.approx(0.57)

    def test_processing_duration_excludes_upstream_delay(
        self, make_event: Callable[..., EnrichedVehicleEvent]
    ) -> None:
        """Separating the two is how a trace tells "ours" from "theirs"."""
        assert make_event().processing_duration_seconds == pytest.approx(0.12)

    def test_is_immutable(self, make_event: Callable[..., EnrichedVehicleEvent]) -> None:
        with pytest.raises(ValidationError):
            make_event().computed_speed_mps = 99.0  # type: ignore[misc]


class TestSchemaEvolution:
    def test_ping_carries_its_schema_version(self, make_ping: MakePing) -> None:
        """Consumers read this to decide whether they can handle a message."""
        assert make_ping().schema_version == SCHEMA_VERSION

    def test_unknown_fields_are_rejected(self, make_ping: MakePing) -> None:
        # extra="forbid": a producer sending a field we do not model should
        # fail loudly in CI, not get silently dropped in production.
        with pytest.raises(ValidationError):
            make_ping(unexpected_field="surprise")


class TestQuarantineRecord:
    def test_keeps_the_raw_payload(self) -> None:
        """Without the original bytes a quarantined event cannot be diagnosed."""
        record = QuarantineRecord(
            reason=QuarantineReason.DECODE_FAILURE,
            detail="truncated protobuf",
            raw_payload=b"\x08\x96\x01garbage",
            trace_id="4bf92f3577b34da6a3ce929d0e0e4736",
        )
        assert record.raw_payload == b"\x08\x96\x01garbage"
        assert record.quarantined_at.tzinfo is not None

"""Tests for the runtime-built BigQuery row descriptor.

The descriptor is generated from a Python column list instead of a checked-in
``_pb2.py``, which removes a build step but means a typo surfaces at write time
in production rather than at compile time. These tests are what buy that back.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime

from transitpulse_core.models import EnrichedVehicleEvent
from transitpulse_core.sinks.bigquery import build_row_descriptor, event_to_row

MakeEvent = Callable[..., EnrichedVehicleEvent]


class TestRowDescriptor:
    def test_field_numbers_are_unique_and_contiguous(self) -> None:
        """Reused or shifted field numbers silently corrupt in-flight rows."""
        numbers = [f.number for f in build_row_descriptor().field]
        assert numbers == list(range(1, len(numbers) + 1))

    def test_field_names_are_unique(self) -> None:
        names = [f.name for f in build_row_descriptor().field]
        assert len(names) == len(set(names))

    def test_every_field_declares_presence(self) -> None:
        """proto3_optional is what makes an unset field a real BigQuery NULL
        instead of a zero -- important for nullable metrics like delay."""
        assert all(f.proto3_optional for f in build_row_descriptor().field)

    def test_covers_the_identity_and_timing_columns(self) -> None:
        names = {f.name for f in build_row_descriptor().field}
        assert {
            "idempotency_key",
            "agency",
            "vehicle_id",
            "event_time",
            "ingest_time",
            "processed_time",
        } <= names


class TestRowConversion:
    def test_includes_the_required_columns(self, make_event: MakeEvent) -> None:
        row = event_to_row(make_event())
        assert row["agency"] == "mta-nyct"
        assert row["vehicle_id"] == "MTA NYCT_7801"
        assert row["idempotency_key"] == make_event().idempotency_key

    def test_timestamps_become_epoch_microseconds(self, make_event: MakeEvent) -> None:
        """BigQuery TIMESTAMP over the Storage Write API is int64 micros."""
        event = make_event()
        assert row_is_micros(event_to_row(event)["event_time"], event.ping.event_time)

    def test_omits_none_valued_columns(self, make_event: MakeEvent) -> None:
        """An omitted field is NULL; sending 0.0 would fake a real measurement.

        The first ping for a vehicle genuinely has no computed speed, and a
        zero there would drag every speed average downwards.
        """
        row = event_to_row(make_event(computed_speed_mps=None, schedule_delay_seconds=None))
        assert "computed_speed_mps" not in row
        assert "schedule_delay_seconds" not in row

    def test_keeps_a_genuine_zero(self, make_event: MakeEvent) -> None:
        """Zero is a real speed for a stopped bus and must survive the filter."""
        assert event_to_row(make_event(computed_speed_mps=0.0))["computed_speed_mps"] == 0.0

    def test_keeps_a_false_boolean(self, make_event: MakeEvent) -> None:
        # False is meaningful here (the vehicle is late), so it must not be
        # dropped the way None is.
        assert event_to_row(make_event(is_on_time=False))["is_on_time"] is False

    def test_enums_serialise_as_their_string_value(self, make_event: MakeEvent) -> None:
        row = event_to_row(make_event())
        assert row["schedule_relationship"] == "SCHEDULED"
        assert row["occupancy_status"] == "FEW_SEATS_AVAILABLE"

    def test_every_row_key_exists_in_the_descriptor(self, make_event: MakeEvent) -> None:
        """Guards the failure mode the generated descriptor cannot: a row key
        that no proto field matches is rejected by the API at runtime."""
        declared = {f.name for f in build_row_descriptor().field}
        assert set(event_to_row(make_event())) <= declared


def row_is_micros(value: object, expected: datetime) -> bool:
    assert isinstance(value, int)
    return value == int(expected.astimezone(UTC).timestamp() * 1_000_000)

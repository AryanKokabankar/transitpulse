"""Tests for decoding and normalising a GTFS-Realtime feed.

The decoder's contract is that it *partitions* a feed: everything valid comes
out as a ping, everything invalid comes out as a quarantine record with a
reason, and nothing is dropped. These tests are written against that invariant
rather than against the happy path, because the happy path is the case a real
agency feed almost never presents in isolation.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from services.collector.feed import FeedDecodeError, decode_feed
from tests.gtfs_fixtures import (
    FIXED_EPOCH,
    FIXED_NOW,
    FeedSpec,
    TripUpdateEntitySpec,
    VehicleEntitySpec,
    build_feed,
    build_vehicle_feed,
)
from transitpulse_core.models import (
    OccupancyStatus,
    QuarantineReason,
    ScheduleRelationship,
)

AGENCY = "mta-nyct"
INGEST_TIME = FIXED_NOW + timedelta(milliseconds=450)


def decode(payload: bytes) -> object:
    return decode_feed(payload, agency=AGENCY, ingest_time=INGEST_TIME)


class TestValidEntity:
    def test_normalises_every_field(self) -> None:
        decoded = decode_feed(
            build_vehicle_feed(VehicleEntitySpec()),
            agency=AGENCY,
            ingest_time=INGEST_TIME,
        )

        assert len(decoded.pings) == 1
        assert decoded.quarantined == []
        ping = decoded.pings[0]
        assert ping.agency == AGENCY
        assert ping.vehicle_id == "MTA NYCT_7801"
        assert ping.trip_id == "ABC123"
        assert ping.route_id == "Q"
        assert ping.direction_id == 0
        assert ping.start_date == "20261015"
        assert ping.schedule_relationship is ScheduleRelationship.SCHEDULED
        assert ping.latitude == pytest.approx(40.7128)
        assert ping.longitude == pytest.approx(-74.0060)
        assert ping.bearing == pytest.approx(93.5)
        assert ping.reported_speed_mps == pytest.approx(11.2)
        assert ping.stop_id == "Q05N"
        assert ping.current_stop_sequence == 4
        assert ping.occupancy_status is OccupancyStatus.FEW_SEATS_AVAILABLE
        assert ping.event_time == FIXED_NOW
        assert ping.ingest_time == INGEST_TIME

    def test_reports_feed_timestamp_and_counts(self) -> None:
        decoded = decode_feed(
            build_vehicle_feed(
                VehicleEntitySpec(entity_id="a", vehicle_id="bus-a"),
                VehicleEntitySpec(entity_id="b", vehicle_id="bus-b"),
            ),
            agency=AGENCY,
            ingest_time=INGEST_TIME,
        )
        assert decoded.feed_timestamp == FIXED_NOW
        assert decoded.entity_count == 2
        assert decoded.vehicle_entity_count == 2
        assert len(decoded.pings) == 2

    def test_unset_optional_fields_become_none_not_zero(self) -> None:
        """A speed of 0.0 and an unreported speed are different facts.

        Collapsing them would drag every downstream speed average toward zero
        for vehicles whose feed simply omits the field.
        """
        decoded = decode_feed(
            build_vehicle_feed(
                VehicleEntitySpec(speed=None, bearing=None, current_stop_sequence=None)
            ),
            agency=AGENCY,
            ingest_time=INGEST_TIME,
        )
        ping = decoded.pings[0]
        assert ping.reported_speed_mps is None
        assert ping.bearing is None
        assert ping.current_stop_sequence is None

    def test_zero_speed_is_preserved_as_zero(self) -> None:
        decoded = decode_feed(
            build_vehicle_feed(VehicleEntitySpec(speed=0.0)),
            agency=AGENCY,
            ingest_time=INGEST_TIME,
        )
        assert decoded.pings[0].reported_speed_mps == 0.0

    def test_empty_strings_become_none(self) -> None:
        decoded = decode_feed(
            build_vehicle_feed(VehicleEntitySpec(trip_id="", route_id="", stop_id="")),
            agency=AGENCY,
            ingest_time=INGEST_TIME,
        )
        ping = decoded.pings[0]
        assert ping.trip_id is None
        assert ping.route_id is None
        assert ping.stop_id is None


class TestVehicleIdentity:
    def test_falls_back_to_entity_id(self) -> None:
        """Several real feeds omit vehicle.vehicle.id entirely."""
        decoded = decode_feed(
            build_vehicle_feed(VehicleEntitySpec(entity_id="NYCT_42", vehicle_id=None)),
            agency=AGENCY,
            ingest_time=INGEST_TIME,
        )
        assert decoded.pings[0].vehicle_id == "NYCT_42"

    def test_prefers_the_vehicle_descriptor_over_the_entity_id(self) -> None:
        decoded = decode_feed(
            build_vehicle_feed(VehicleEntitySpec(entity_id="e-1", vehicle_id="bus-9")),
            agency=AGENCY,
            ingest_time=INGEST_TIME,
        )
        assert decoded.pings[0].vehicle_id == "bus-9"

    def test_quarantines_when_both_identifiers_are_missing(self) -> None:
        # Without an identifier there is no ordering key and no idempotency
        # key, so the ping cannot be published correctly at all.
        decoded = decode_feed(
            build_vehicle_feed(VehicleEntitySpec(entity_id="", vehicle_id=None)),
            agency=AGENCY,
            ingest_time=INGEST_TIME,
        )
        assert decoded.pings == []
        assert decoded.quarantined[0].reason is QuarantineReason.SCHEMA_VALIDATION
        assert "entity.id" in decoded.quarantined[0].detail

    def test_strips_whitespace_padding(self) -> None:
        decoded = decode_feed(
            build_vehicle_feed(VehicleEntitySpec(vehicle_id="  bus-7  ")),
            agency=AGENCY,
            ingest_time=INGEST_TIME,
        )
        assert decoded.pings[0].vehicle_id == "bus-7"


class TestNullIsland:
    def test_is_quarantined_with_its_own_reason(self) -> None:
        """(0, 0) is an upstream GPS fault, not a contract breach, and the
        quarantine metric is only useful if it can tell them apart."""
        decoded = decode_feed(
            build_vehicle_feed(VehicleEntitySpec(latitude=0.0, longitude=0.0)),
            agency=AGENCY,
            ingest_time=INGEST_TIME,
        )
        assert decoded.pings == []
        assert len(decoded.quarantined) == 1
        assert decoded.quarantined[0].reason is QuarantineReason.NULL_ISLAND

    def test_an_explicit_zero_pair_is_rejected_whatever_else_is_valid(self) -> None:
        decoded = decode_feed(
            build_vehicle_feed(
                VehicleEntitySpec(latitude=0.0, longitude=0.0, speed=12.0, route_id="Q")
            ),
            agency=AGENCY,
            ingest_time=INGEST_TIME,
        )
        assert decoded.pings == []
        assert decoded.quarantined[0].reason is QuarantineReason.NULL_ISLAND

    def test_a_genuine_equatorial_position_is_accepted(self) -> None:
        decoded = decode_feed(
            build_vehicle_feed(VehicleEntitySpec(latitude=0.5, longitude=0.5)),
            agency=AGENCY,
            ingest_time=INGEST_TIME,
        )
        assert len(decoded.pings) == 1


class TestMalformedEntities:
    def test_missing_position_is_quarantined(self) -> None:
        decoded = decode_feed(
            build_vehicle_feed(VehicleEntitySpec(include_position=False)),
            agency=AGENCY,
            ingest_time=INGEST_TIME,
        )
        assert decoded.quarantined[0].reason is QuarantineReason.SCHEMA_VALIDATION
        assert "no position" in decoded.quarantined[0].detail

    @pytest.mark.parametrize("bearing", [400.0, -10.0])
    def test_out_of_range_bearing_is_quarantined(self, bearing: float) -> None:
        decoded = decode_feed(
            build_vehicle_feed(VehicleEntitySpec(bearing=bearing)),
            agency=AGENCY,
            ingest_time=INGEST_TIME,
        )
        assert decoded.pings == []
        assert decoded.quarantined[0].reason is QuarantineReason.SCHEMA_VALIDATION
        assert "bearing" in decoded.quarantined[0].detail

    def test_negative_speed_is_quarantined(self) -> None:
        decoded = decode_feed(
            build_vehicle_feed(VehicleEntitySpec(speed=-5.0)),
            agency=AGENCY,
            ingest_time=INGEST_TIME,
        )
        assert "reported_speed_mps" in decoded.quarantined[0].detail

    @pytest.mark.parametrize(("lat", "lon"), [(91.0, -74.0), (40.7, 181.0)])
    def test_impossible_coordinates_are_quarantined(self, lat: float, lon: float) -> None:
        decoded = decode_feed(
            build_vehicle_feed(VehicleEntitySpec(latitude=lat, longitude=lon)),
            agency=AGENCY,
            ingest_time=INGEST_TIME,
        )
        assert decoded.pings == []
        assert decoded.quarantined[0].reason is QuarantineReason.SCHEMA_VALIDATION

    def test_quarantine_record_keeps_the_raw_entity(self) -> None:
        """The payload is what makes the rejection diagnosable later."""
        decoded = decode_feed(
            build_vehicle_feed(VehicleEntitySpec(vehicle_id="bus-bad", bearing=400.0)),
            agency=AGENCY,
            ingest_time=INGEST_TIME,
        )
        record = decoded.quarantined[0]
        assert record.agency == AGENCY
        assert record.vehicle_id == "bus-bad"
        assert record.raw_payload
        assert b"bus-bad" in record.raw_payload


class TestTimestamps:
    def test_falls_back_to_the_header_timestamp(self) -> None:
        """An unset vehicle timestamp means "as of the feed header" per spec."""
        header = FIXED_EPOCH - 30
        decoded = decode_feed(
            build_vehicle_feed(VehicleEntitySpec(timestamp=None), header_timestamp=header),
            agency=AGENCY,
            ingest_time=INGEST_TIME,
        )
        assert decoded.pings[0].event_time == datetime.fromtimestamp(header, tz=UTC)

    def test_quarantines_when_no_timestamp_exists_anywhere(self) -> None:
        # Inventing event_time as "now" would report zero ingest lag for the
        # one feed whose lag cannot be measured, which is worse than a gap.
        decoded = decode_feed(
            build_vehicle_feed(VehicleEntitySpec(timestamp=None), header_timestamp=None),
            agency=AGENCY,
            ingest_time=INGEST_TIME,
        )
        assert decoded.pings == []
        assert decoded.quarantined[0].reason is QuarantineReason.SCHEMA_VALIDATION
        assert "timestamp" in decoded.quarantined[0].detail

    def test_event_time_is_timezone_aware_utc(self) -> None:
        decoded = decode_feed(
            build_vehicle_feed(VehicleEntitySpec()),
            agency=AGENCY,
            ingest_time=INGEST_TIME,
        )
        assert decoded.pings[0].event_time.tzinfo is not None
        assert decoded.pings[0].ingest_lag_seconds == pytest.approx(0.45)


class TestEnumMapping:
    @pytest.mark.parametrize(
        ("gtfs_value", "expected"),
        [
            ("SCHEDULED", ScheduleRelationship.SCHEDULED),
            ("ADDED", ScheduleRelationship.ADDED),
            ("CANCELED", ScheduleRelationship.CANCELED),
            ("UNSCHEDULED", ScheduleRelationship.UNSCHEDULED),
        ],
    )
    def test_known_schedule_relationships_map_across(
        self, gtfs_value: str, expected: ScheduleRelationship
    ) -> None:
        decoded = decode_feed(
            build_vehicle_feed(VehicleEntitySpec(schedule_relationship=gtfs_value)),
            agency=AGENCY,
            ingest_time=INGEST_TIME,
        )
        assert decoded.pings[0].schedule_relationship is expected

    @pytest.mark.parametrize("gtfs_value", ["DUPLICATED", "REPLACEMENT", "NEW", "DELETED"])
    def test_unknown_schedule_relationship_degrades_to_unknown(self, gtfs_value: str) -> None:
        """The spec keeps adding values. A new one is not a reason to reject a
        position that is otherwise perfectly good."""
        decoded = decode_feed(
            build_vehicle_feed(VehicleEntitySpec(schedule_relationship=gtfs_value)),
            agency=AGENCY,
            ingest_time=INGEST_TIME,
        )
        assert len(decoded.pings) == 1
        assert decoded.pings[0].schedule_relationship is ScheduleRelationship.UNKNOWN

    @pytest.mark.parametrize("gtfs_value", ["NO_DATA_AVAILABLE", "NOT_BOARDABLE"])
    def test_unmapped_occupancy_degrades_to_unknown(self, gtfs_value: str) -> None:
        decoded = decode_feed(
            build_vehicle_feed(VehicleEntitySpec(occupancy_status=gtfs_value)),
            agency=AGENCY,
            ingest_time=INGEST_TIME,
        )
        assert decoded.pings[0].occupancy_status is OccupancyStatus.UNKNOWN

    def test_unset_relationship_is_unknown(self) -> None:
        decoded = decode_feed(
            build_vehicle_feed(
                VehicleEntitySpec(schedule_relationship=None, occupancy_status=None)
            ),
            agency=AGENCY,
            ingest_time=INGEST_TIME,
        )
        ping = decoded.pings[0]
        assert ping.schedule_relationship is ScheduleRelationship.UNKNOWN
        assert ping.occupancy_status is OccupancyStatus.UNKNOWN


class TestNonVehicleEntities:
    def test_trip_updates_are_skipped_not_quarantined(self) -> None:
        """A feed legitimately mixes entity types; the others are not this
        service's business and are not failures."""
        decoded = decode_feed(
            build_feed(
                FeedSpec(
                    entities=[
                        TripUpdateEntitySpec(),
                        VehicleEntitySpec(vehicle_id="bus-1"),
                        TripUpdateEntitySpec(entity_id="tu-2"),
                    ]
                )
            ),
            agency=AGENCY,
            ingest_time=INGEST_TIME,
        )
        assert len(decoded.pings) == 1
        assert decoded.quarantined == []
        assert decoded.entity_count == 3
        assert decoded.vehicle_entity_count == 1

    def test_an_empty_feed_is_not_an_error(self) -> None:
        decoded = decode_feed(
            build_feed(FeedSpec(entities=[])), agency=AGENCY, ingest_time=INGEST_TIME
        )
        assert decoded.pings == []
        assert decoded.quarantined == []
        assert decoded.entity_count == 0


class TestUndecodablePayloads:
    @pytest.mark.parametrize(
        ("label", "payload"),
        [
            ("html error page", b"<html><body>502 Bad Gateway</body></html>"),
            ("json error", b'{"error": "unauthorized"}'),
            ("truncated", b"\x08"),
            ("plain text", b"this is not protobuf at all"),
        ],
    )
    def test_raise_a_decode_error(self, label: str, payload: bytes) -> None:
        """An agency serving an HTML error page with HTTP 200 is the realistic
        version of this, and it must not look like an empty feed."""
        with pytest.raises(FeedDecodeError):
            decode_feed(payload, agency=AGENCY, ingest_time=INGEST_TIME)

    def test_a_position_missing_required_fields_is_quarantined_not_raised(self) -> None:
        """A non-compliant producer omits the proto2 ``required`` coordinates.

        The Python parser accepts such a message (the fields read back as 0.0)
        while the strict serialiser refuses to write it out again, so building
        the quarantine record is the step that would blow up. This asserts the
        entity is kept rather than taking the whole tick down with an
        EncodeError from inside the error path.
        """
        payload = build_feed(
            FeedSpec(
                entities=[
                    VehicleEntitySpec(
                        vehicle_id="bus-partial", latitude=None, longitude=None, speed=None
                    )
                ],
                serialize_partial=True,
            )
        )
        decoded = decode_feed(payload, agency=AGENCY, ingest_time=INGEST_TIME)

        assert decoded.pings == []
        record = decoded.quarantined[0]
        assert record.reason is QuarantineReason.NULL_ISLAND
        assert record.vehicle_id == "bus-partial"
        assert record.raw_payload


class TestMixedFeed:
    def test_partitions_a_realistic_feed(self) -> None:
        """The case that matters: good and bad entities in one response, with
        every bad one accounted for."""
        decoded = decode_feed(
            build_feed(
                FeedSpec(
                    entities=[
                        VehicleEntitySpec(entity_id="1", vehicle_id="good-1"),
                        VehicleEntitySpec(entity_id="2", vehicle_id="good-2", route_id="B"),
                        VehicleEntitySpec(
                            entity_id="3", vehicle_id="null-island", latitude=0.0, longitude=0.0
                        ),
                        VehicleEntitySpec(entity_id="4", vehicle_id="bad-bearing", bearing=999.0),
                        VehicleEntitySpec(
                            entity_id="5", vehicle_id="no-pos", include_position=False
                        ),
                        TripUpdateEntitySpec(entity_id="6"),
                    ]
                )
            ),
            agency=AGENCY,
            ingest_time=INGEST_TIME,
        )

        assert [ping.vehicle_id for ping in decoded.pings] == ["good-1", "good-2"]
        assert decoded.entity_count == 6
        assert decoded.vehicle_entity_count == 5
        assert [record.reason for record in decoded.quarantined] == [
            QuarantineReason.NULL_ISLAND,
            QuarantineReason.SCHEMA_VALIDATION,
            QuarantineReason.SCHEMA_VALIDATION,
        ]
        # Nothing vanished: every vehicle entity is either published or kept.
        assert len(decoded.pings) + len(decoded.quarantined) == decoded.vehicle_entity_count

    def test_distinct_vehicles_produce_distinct_idempotency_keys(self) -> None:
        decoded = decode_feed(
            build_vehicle_feed(
                VehicleEntitySpec(entity_id="1", vehicle_id="bus-1"),
                VehicleEntitySpec(entity_id="2", vehicle_id="bus-2"),
            ),
            agency=AGENCY,
            ingest_time=INGEST_TIME,
        )
        keys = {ping.idempotency_key for ping in decoded.pings}
        assert len(keys) == 2

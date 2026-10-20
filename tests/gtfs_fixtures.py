"""Synthetic GTFS-Realtime payloads for the decoder tests.

Real protobuf rather than a stub, because the decoder's job is to survive what
a real feed does and a stub would only confirm that the test author and the
implementation agree. Building a genuine ``FeedMessage`` means the tests
exercise proto2 field presence -- the difference between "speed is 0 m/s" and
"speed was not reported" -- which is where the subtle normalisation bugs live.

Every field is overridable and ``None`` means "leave unset", so a malformed
entity is expressed as the absence it actually is rather than as a sentinel.
"""

from __future__ import annotations

import csv
import io
import zipfile
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

from google.transit import gtfs_realtime_pb2 as gtfs

FIXED_NOW = datetime(2026, 10, 15, 14, 30, 0, tzinfo=UTC)
FIXED_EPOCH = int(FIXED_NOW.timestamp())


@dataclass
class VehicleEntitySpec:
    """One vehicle-position entity. ``None`` leaves the field unset."""

    entity_id: str = "entity-1"
    vehicle_id: str | None = "MTA NYCT_7801"
    latitude: float | None = 40.7128
    longitude: float | None = -74.0060
    bearing: float | None = 93.5
    speed: float | None = 11.2
    timestamp: int | None = FIXED_EPOCH
    trip_id: str | None = "ABC123"
    route_id: str | None = "Q"
    direction_id: int | None = 0
    start_date: str | None = "20261015"
    schedule_relationship: str | None = "SCHEDULED"
    occupancy_status: str | None = "FEW_SEATS_AVAILABLE"
    stop_id: str | None = "Q05N"
    current_stop_sequence: int | None = 4
    include_position: bool = True


@dataclass
class TripUpdateEntitySpec:
    """A non-vehicle entity, which a real feed mixes in freely."""

    entity_id: str = "trip-update-1"
    trip_id: str = "ABC123"


EntitySpec = VehicleEntitySpec | TripUpdateEntitySpec


@dataclass
class FeedSpec:
    entities: list[EntitySpec] = field(default_factory=list)
    header_timestamp: int | None = FIXED_EPOCH
    # GTFS-RT declares Position.latitude and .longitude as proto2 *required*,
    # so a compliant encoder cannot emit a position without them. A
    # non-compliant upstream can, and this flag is the only way to build that
    # payload for the decoder to be tested against.
    serialize_partial: bool = False


def build_feed(spec: FeedSpec) -> bytes:
    """Serialise a feed specification to wire bytes."""
    message = gtfs.FeedMessage()
    message.header.gtfs_realtime_version = "2.0"
    message.header.incrementality = gtfs.FeedHeader.FULL_DATASET
    if spec.header_timestamp is not None:
        message.header.timestamp = spec.header_timestamp

    for entity_spec in spec.entities:
        entity = message.entity.add()
        if isinstance(entity_spec, TripUpdateEntitySpec):
            entity.id = entity_spec.entity_id
            entity.trip_update.trip.trip_id = entity_spec.trip_id
            continue
        _populate_vehicle(entity, entity_spec)

    if spec.serialize_partial:
        partial: bytes = message.SerializePartialToString()
        return partial
    return message.SerializeToString()


def build_vehicle_feed(
    *specs: VehicleEntitySpec, header_timestamp: int | None = FIXED_EPOCH
) -> bytes:
    """Shorthand for the common case: a feed of vehicle positions only."""
    return build_feed(FeedSpec(entities=list(specs), header_timestamp=header_timestamp))


def _populate_vehicle(entity: gtfs.FeedEntity, spec: VehicleEntitySpec) -> None:
    entity.id = spec.entity_id
    vehicle = entity.vehicle

    if spec.vehicle_id is not None:
        vehicle.vehicle.id = spec.vehicle_id
    if spec.timestamp is not None:
        vehicle.timestamp = spec.timestamp
    if spec.stop_id is not None:
        vehicle.stop_id = spec.stop_id
    if spec.current_stop_sequence is not None:
        vehicle.current_stop_sequence = spec.current_stop_sequence
    if spec.occupancy_status is not None:
        vehicle.occupancy_status = gtfs.VehiclePosition.OccupancyStatus.Value(spec.occupancy_status)

    trip = vehicle.trip
    if spec.trip_id is not None:
        trip.trip_id = spec.trip_id
    if spec.route_id is not None:
        trip.route_id = spec.route_id
    if spec.direction_id is not None:
        trip.direction_id = spec.direction_id
    if spec.start_date is not None:
        trip.start_date = spec.start_date
    if spec.schedule_relationship is not None:
        trip.schedule_relationship = gtfs.TripDescriptor.ScheduleRelationship.Value(
            spec.schedule_relationship
        )

    if not spec.include_position:
        return

    position = vehicle.position
    if spec.latitude is not None:
        position.latitude = spec.latitude
    if spec.longitude is not None:
        position.longitude = spec.longitude
    if spec.bearing is not None:
        position.bearing = spec.bearing
    if spec.speed is not None:
        position.speed = spec.speed


# ----------------------------------------------------------------------
# Static GTFS
# ----------------------------------------------------------------------
# The realtime fixtures above are protobuf; a static feed is a zip of CSV, and
# the loader's job is the awkward parts of that -- a byte-order mark, an empty
# optional column, a stop time past 24:00:00. Built here rather than checked in
# as a binary so those cases are visible in the test that depends on them.

DEFAULT_AGENCY_TIMEZONE = "America/New_York"

_DEFAULT_ROUTES = [
    {
        "route_id": "Q",
        "agency_id": "1",
        "route_short_name": "Q",
        "route_long_name": "Broadway Express",
        "route_type": "1",
    },
    {
        "route_id": "B",
        "agency_id": "1",
        "route_short_name": "B",
        "route_long_name": "Brighton Local",
        "route_type": "1",
    },
]

_DEFAULT_STOPS = [
    {"stop_id": "Q05N", "stop_name": "96 St", "stop_lat": "40.7840", "stop_lon": "-73.9470"},
    # Deliberately missing coordinates: GTFS allows it for a station entrance,
    # and a loader that cannot tolerate an empty optional column fails on a
    # real feed rather than on this test.
    {"stop_id": "Q04N", "stop_name": "72 St", "stop_lat": "", "stop_lon": ""},
]

_DEFAULT_TRIPS = [
    {"trip_id": "ABC123", "route_id": "Q", "service_id": "WEEKDAY", "direction_id": "0"},
    {"trip_id": "NIGHT1", "route_id": "B", "service_id": "WEEKDAY", "direction_id": "1"},
]

_DEFAULT_STOP_TIMES = [
    {
        "trip_id": "ABC123",
        "stop_sequence": "4",
        "stop_id": "Q05N",
        "arrival_time": "08:15:00",
        "departure_time": "08:15:30",
    },
    {
        "trip_id": "ABC123",
        "stop_sequence": "5",
        "stop_id": "Q04N",
        "arrival_time": "08:20:00",
        "departure_time": "08:20:30",
    },
    # Past midnight: 01:30 the following calendar day, still on this service
    # day. This row is the whole reason the loader parses times itself.
    {
        "trip_id": "NIGHT1",
        "stop_sequence": "2",
        "stop_id": "Q05N",
        "arrival_time": "25:30:00",
        "departure_time": "25:30:00",
    },
    # No timepoint, so no times at all.
    {
        "trip_id": "NIGHT1",
        "stop_sequence": "3",
        "stop_id": "Q04N",
        "arrival_time": "",
        "departure_time": "",
    },
]


def _csv_bytes(rows: list[dict[str, str]], *, bom: bool = False) -> bytes:
    """Render rows as GTFS-style CSV, optionally with the BOM real feeds carry."""
    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(buffer, fieldnames=list(rows[0]), lineterminator="\r\n")
    writer.writeheader()
    writer.writerows(rows)
    text = buffer.getvalue()
    return ("\ufeff" + text).encode("utf-8") if bom else text.encode("utf-8")


def build_static_gtfs_zip(
    destination: Path,
    *,
    routes: list[dict[str, str]] | None = None,
    stops: list[dict[str, str]] | None = None,
    trips: list[dict[str, str]] | None = None,
    stop_times: list[dict[str, str]] | None = None,
    agency_timezone: str | None = DEFAULT_AGENCY_TIMEZONE,
    omit: Sequence[str] = (),
) -> Path:
    """Write a small but structurally real static GTFS archive."""
    members: dict[str, bytes] = {
        # With a BOM, because a loader that reads it as part of the first
        # column name misses every lookup of that column.
        "routes.txt": _csv_bytes(routes if routes is not None else _DEFAULT_ROUTES, bom=True),
        "stops.txt": _csv_bytes(stops if stops is not None else _DEFAULT_STOPS),
        "trips.txt": _csv_bytes(trips if trips is not None else _DEFAULT_TRIPS),
        "stop_times.txt": _csv_bytes(stop_times if stop_times is not None else _DEFAULT_STOP_TIMES),
    }
    if agency_timezone is not None:
        members["agency.txt"] = _csv_bytes(
            [
                {
                    "agency_id": "1",
                    "agency_name": "Test Transit",
                    "agency_url": "https://example.invalid",
                    "agency_timezone": agency_timezone,
                }
            ]
        )

    with zipfile.ZipFile(destination, "w", zipfile.ZIP_DEFLATED) as archive:
        for name, payload in members.items():
            if name in omit:
                continue
            archive.writestr(name, payload)
    return destination

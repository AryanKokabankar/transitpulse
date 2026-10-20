"""The data contract for the pipeline.

Three timestamps travel with every event and they are not interchangeable:

* ``event_time``     -- when the vehicle reported the position (set by the agency)
* ``ingest_time``    -- when the collector decoded it
* ``processed_time`` -- when the processor finished enriching it

``processed_time - event_time`` is the pipeline's headline SLI (ingest lag), so
all three are mandatory and all three are timezone-aware. Naive datetimes are
rejected at the boundary rather than silently assumed to be UTC.
"""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from enum import StrEnum
from typing import Annotated, Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

SCHEMA_VERSION = 1

Latitude = Annotated[float, Field(ge=-90.0, le=90.0)]
Longitude = Annotated[float, Field(ge=-180.0, le=180.0)]


class ScheduleRelationship(StrEnum):
    SCHEDULED = "SCHEDULED"
    ADDED = "ADDED"
    UNSCHEDULED = "UNSCHEDULED"
    CANCELED = "CANCELED"
    UNKNOWN = "UNKNOWN"


class OccupancyStatus(StrEnum):
    EMPTY = "EMPTY"
    MANY_SEATS_AVAILABLE = "MANY_SEATS_AVAILABLE"
    FEW_SEATS_AVAILABLE = "FEW_SEATS_AVAILABLE"
    STANDING_ROOM_ONLY = "STANDING_ROOM_ONLY"
    CRUSHED_STANDING_ROOM_ONLY = "CRUSHED_STANDING_ROOM_ONLY"
    FULL = "FULL"
    NOT_ACCEPTING_PASSENGERS = "NOT_ACCEPTING_PASSENGERS"
    UNKNOWN = "UNKNOWN"


class QuarantineReason(StrEnum):
    SCHEMA_VALIDATION = "SCHEMA_VALIDATION"
    DECODE_FAILURE = "DECODE_FAILURE"
    NULL_ISLAND = "NULL_ISLAND"
    IMPOSSIBLE_SPEED = "IMPOSSIBLE_SPEED"
    STALE_EVENT = "STALE_EVENT"
    UNKNOWN_ROUTE = "UNKNOWN_ROUTE"
    SINK_WRITE_FAILURE = "SINK_WRITE_FAILURE"


def is_null_island(latitude: float, longitude: float) -> bool:
    """Whether a coordinate is the (0, 0) GPS failure value.

    Exposed as a predicate because two callers need the same threshold for
    different purposes: :class:`VehiclePing` rejects the coordinate, and the
    collector needs to label the rejection ``NULL_ISLAND`` rather than
    ``SCHEMA_VALIDATION`` -- an upstream GPS fault and a feed that breached the
    contract are different operational stories, and the quarantine metric is
    only useful if it can tell them apart.
    """
    return abs(latitude) < 1e-6 and abs(longitude) < 1e-6


def _require_aware(value: datetime) -> datetime:
    """Reject naive datetimes outright, then normalise to UTC.

    Silently attaching UTC to a naive timestamp is how pipelines end up with
    hour-scale lag bugs that only show up during a DST transition.
    """
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("datetime must be timezone-aware")
    return value.astimezone(UTC)


class VehiclePing(BaseModel):
    """A single normalised vehicle position, as published by the collector.

    This is the wire format on the ``vehicle-events`` topic. It is deliberately
    flat and free of derived fields -- enrichment happens downstream so that a
    bug in the enrichment logic can be fixed by replaying the corpus rather
    than by re-scraping a feed that no longer serves yesterday's data.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    schema_version: int = SCHEMA_VERSION
    agency: str = Field(min_length=1, max_length=64)
    vehicle_id: str = Field(min_length=1, max_length=128)

    trip_id: str | None = Field(default=None, max_length=128)
    route_id: str | None = Field(default=None, max_length=64)
    direction_id: int | None = Field(default=None, ge=0, le=1)
    start_date: str | None = Field(default=None, pattern=r"^\d{8}$")
    schedule_relationship: ScheduleRelationship = ScheduleRelationship.UNKNOWN

    latitude: Latitude
    longitude: Longitude
    bearing: float | None = Field(default=None, ge=0.0, le=360.0)
    reported_speed_mps: float | None = Field(default=None, ge=0.0, le=200.0)

    current_stop_sequence: int | None = Field(default=None, ge=0)
    stop_id: str | None = Field(default=None, max_length=64)
    occupancy_status: OccupancyStatus = OccupancyStatus.UNKNOWN

    event_time: datetime
    ingest_time: datetime

    _aware = field_validator("event_time", "ingest_time")(_require_aware)

    @model_validator(mode="after")
    def _reject_null_island(self) -> Self:
        """(0, 0) is the single most common GPS failure value in transit feeds.

        It is a valid coordinate in the Gulf of Guinea and an invalid one for
        every bus on earth, so it is rejected here rather than in enrichment.
        """
        if is_null_island(self.latitude, self.longitude):
            raise ValueError("null-island coordinate (0, 0) is not a real position")
        return self

    @property
    def idempotency_key(self) -> str:
        """Stable identity for an observation, used by every sink.

        An agency re-publishing the same ping in the next feed cycle -- which
        they do constantly -- must not produce a second row. The key covers
        agency, vehicle and the agency-assigned event time; ingest_time is
        deliberately excluded because it differs on every replay.
        """
        canonical = f"{self.agency}|{self.vehicle_id}|{int(self.event_time.timestamp())}"
        return hashlib.blake2b(canonical.encode("utf-8"), digest_size=16).hexdigest()

    @property
    def ingest_lag_seconds(self) -> float:
        return (self.ingest_time - self.event_time).total_seconds()


class EnrichedVehicleEvent(BaseModel):
    """A ping plus everything the processor derived from it.

    Written to Postgres (upsert, hot serving path) and BigQuery (append-only
    history) by the same handler invocation.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    ping: VehiclePing
    idempotency_key: str

    # derived from the previous ping for this vehicle
    computed_speed_mps: float | None = None
    distance_from_last_m: float | None = None
    seconds_since_last_ping: float | None = None

    # derived from the static GTFS schedule
    schedule_delay_seconds: int | None = None
    is_on_time: bool | None = None
    route_short_name: str | None = None

    processed_time: datetime

    _aware_processed = field_validator("processed_time")(_require_aware)

    @property
    def end_to_end_lag_seconds(self) -> float:
        """The number the SLO is written against."""
        return (self.processed_time - self.ping.event_time).total_seconds()

    @property
    def processing_duration_seconds(self) -> float:
        return (self.processed_time - self.ping.ingest_time).total_seconds()


class QuarantineRecord(BaseModel):
    """An event the pipeline refused to process, kept for inspection.

    Dropping bad data silently makes a pipeline look healthy while it loses
    rows. Everything rejected lands here with the original payload attached so
    it can be diagnosed and, once the bug is fixed, replayed.

    ``raw_payload`` holds a fragment of protobuf, which is arbitrary binary and
    therefore not valid UTF-8. Base64 is set explicitly in both directions
    because pydantic's default would raise while serialising the one field that
    makes a quarantine row worth keeping.
    """

    model_config = ConfigDict(
        extra="forbid",
        ser_json_bytes="base64",
        val_json_bytes="base64",
    )

    reason: QuarantineReason
    detail: str = Field(max_length=4096)
    agency: str | None = None
    vehicle_id: str | None = None
    raw_payload: bytes | None = None
    message_id: str | None = None
    delivery_attempt: int | None = None
    trace_id: str | None = None
    quarantined_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class FeedSnapshot(BaseModel):
    """Metadata for one archived raw feed response.

    The archive is what makes the load harness possible: real payloads, real
    skew, real garbage, replayable at any multiple long after the agency has
    forgotten them.
    """

    model_config = ConfigDict(extra="forbid")

    agency: str
    fetched_at: datetime
    feed_timestamp: datetime | None
    entity_count: int
    payload_bytes: int
    object_path: str
    content_sha256: str

    _aware_fetched = field_validator("fetched_at")(_require_aware)

    @staticmethod
    def sha256_of(payload: bytes) -> str:
        return hashlib.sha256(payload).hexdigest()

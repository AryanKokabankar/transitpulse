"""Fetching and normalising one GTFS-Realtime feed response.

Kept separate from the FastAPI module so the two things most worth testing --
what happens when the agency is down, and what happens when its protobuf is
garbage -- are testable without an HTTP server or a broker.

The decode half exists because a public transit feed is an untrusted input that
nobody owns. Entities arrive with coordinates at (0, 0), bearings of 400
degrees, timestamps from 1970, enum values added to the spec after this code
was written, and occasionally a body that is not protobuf at all. The decoder's
job is therefore not to parse a feed but to *partition* one: a list of pings
that satisfy the contract, and a list of quarantine records explaining, per
entity, why the rest did not. Dropping the remainder would be the one
unacceptable outcome -- it makes a lossy pipeline look perfect.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Final, cast

import httpx
import structlog
from google.protobuf.message import DecodeError
from google.transit import gtfs_realtime_pb2 as gtfs
from opentelemetry.trace import SpanKind, Status, StatusCode
from pydantic import ValidationError
from tenacity import (
    AsyncRetrying,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential_jitter,
)

from transitpulse_core.config import Settings, get_settings
from transitpulse_core.models import (
    OccupancyStatus,
    QuarantineReason,
    QuarantineRecord,
    ScheduleRelationship,
    VehiclePing,
    is_null_island,
)
from transitpulse_core.resilience import CircuitBreaker
from transitpulse_core.telemetry import current_trace_id, get_metrics, get_tracer

log = structlog.get_logger(__name__)

# A quarantined entity keeps its raw bytes so the rejection can be diagnosed,
# but a whole unparseable feed can be megabytes and this column lives in the
# serving database. Enough to identify what arrived, not enough to bloat it.
MAX_QUARANTINE_PAYLOAD_BYTES: Final = 64 * 1024

# 429 means "later", not "never", so it retries alongside the 5xx family even
# though it is a 4xx. A 401 or 404 does not: it is a deploy-time fault, and
# retrying it only delays the alert.
_RETRYABLE_STATUSES: Final = frozenset({408, 425, 429, 500, 502, 503, 504})


class FeedFailureKind(StrEnum):
    """Label values for ``transitpulse.feed.fetch.failures``.

    The point of the label is to let one dashboard answer "is the agency slow,
    broken, or publishing rubbish" without opening a trace.
    """

    TIMEOUT = "timeout"
    HTTP_ERROR = "http_error"
    DECODE_ERROR = "decode_error"
    CIRCUIT_OPEN = "circuit_open"


class FeedError(Exception):
    """Base for upstream feed failures, carrying its own metric label."""

    kind: FeedFailureKind = FeedFailureKind.HTTP_ERROR

    @property
    def retryable(self) -> bool:
        return True


class FeedTimeoutError(FeedError):
    kind = FeedFailureKind.TIMEOUT


class FeedDecodeError(FeedError):
    kind = FeedFailureKind.DECODE_ERROR


class FeedHTTPError(FeedError):
    kind = FeedFailureKind.HTTP_ERROR

    def __init__(self, message: str, *, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code

    @property
    def retryable(self) -> bool:
        """Transport errors retry; a status code decides for itself."""
        return self.status_code is None or self.status_code in _RETRYABLE_STATUSES


def _should_retry(exc: BaseException) -> bool:
    return isinstance(exc, FeedError) and exc.retryable


@dataclass(frozen=True)
class FeedResponse:
    payload: bytes
    fetched_at: datetime
    duration_seconds: float
    attempts: int


@dataclass(frozen=True)
class DecodedFeed:
    """One feed response, split into what the contract accepts and what it
    does not."""

    pings: list[VehiclePing]
    quarantined: list[QuarantineRecord]
    feed_timestamp: datetime | None
    entity_count: int
    vehicle_entity_count: int


class FeedClient:
    """Fetches the agency feed, with retries inside a circuit breaker.

    The two layers handle different faults and neither substitutes for the
    other: retries ride out a dropped connection or a momentary 503, while the
    breaker stops the collector spending every scheduler tick on an upstream
    that has been dead for ten minutes. See
    :mod:`transitpulse_core.resilience`.
    """

    def __init__(
        self,
        settings: Settings | None = None,
        *,
        client: httpx.AsyncClient | None = None,
        breaker: CircuitBreaker | None = None,
    ) -> None:
        self._settings = settings or get_settings()
        self._metrics = get_metrics()
        self._tracer = get_tracer()
        self._owns_client = client is None
        self._client = client or httpx.AsyncClient(
            timeout=httpx.Timeout(self._settings.feed_timeout_seconds),
            follow_redirects=True,
        )
        self._breaker = breaker or CircuitBreaker(
            name="gtfs-feed",
            failure_threshold=self._settings.feed_breaker_failure_threshold,
            reset_timeout_seconds=self._settings.feed_breaker_reset_seconds,
        )

    @property
    def breaker(self) -> CircuitBreaker:
        return self._breaker

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def fetch(self) -> FeedResponse:
        """Fetch the feed, retrying transient failures.

        The breaker is consulted once per call rather than once per attempt, so
        a tick that gets through still spends its full retry budget on a
        transient fault, while a tick arriving at an open breaker costs one
        rejection instead of four timeouts.
        """
        self._breaker.before_call()

        url = httpx.URL(self._settings.feed_url)
        retrying: AsyncRetrying = AsyncRetrying(
            stop=stop_after_attempt(self._settings.feed_max_attempts),
            # Jitter is tied to the base interval rather than left at
            # tenacity's fixed one second: several Cloud Run instances whose
            # ticks collide must spread out proportionally to the backoff they
            # are already applying, not by a constant that would dominate a
            # sub-second base and vanish against a ten-second one.
            wait=wait_exponential_jitter(
                multiplier=self._settings.feed_retry_initial_seconds,
                max=self._settings.feed_retry_max_seconds,
                jitter=self._settings.feed_retry_initial_seconds,
            ),
            retry=retry_if_exception(_should_retry),
            reraise=True,
        )

        started = time.monotonic()
        with self._tracer.start_as_current_span(
            "collector.fetch_feed",
            kind=SpanKind.CLIENT,
            attributes={
                "feed.agency": self._settings.feed_agency,
                # Host and path only. Some agencies carry their API key in the
                # query string, and a span attribute is not the place for it.
                "server.address": url.host,
                "url.path": url.path,
                "feed.max_attempts": self._settings.feed_max_attempts,
            },
        ) as span:
            try:
                payload = cast("bytes", await retrying(self._attempt_fetch))
            except FeedError as exc:
                self._breaker.record_failure()
                span.set_attribute("feed.attempts", _attempts_made(retrying))
                span.set_status(Status(StatusCode.ERROR, str(exc)))
                span.record_exception(exc)
                self._metrics.feed_fetch_duration.record(
                    time.monotonic() - started,
                    {"agency": self._settings.feed_agency, "outcome": "error"},
                )
                raise

            duration = time.monotonic() - started
            self._breaker.record_success()
            span.set_attribute("feed.attempts", _attempts_made(retrying))
            span.set_attribute("feed.payload_bytes", len(payload))
            self._metrics.feed_fetch_duration.record(
                duration, {"agency": self._settings.feed_agency, "outcome": "ok"}
            )
            return FeedResponse(
                payload=payload,
                fetched_at=datetime.now(UTC),
                duration_seconds=duration,
                attempts=_attempts_made(retrying),
            )

    async def _attempt_fetch(self) -> bytes:
        """One HTTP call, with every failure mode mapped to a labelled error."""
        try:
            response = await self._client.get(self._settings.feed_url)
        except httpx.TimeoutException as exc:
            self._count_failure(FeedFailureKind.TIMEOUT)
            raise FeedTimeoutError(
                f"feed timed out after {self._settings.feed_timeout_seconds}s"
            ) from exc
        except httpx.HTTPError as exc:
            self._count_failure(FeedFailureKind.HTTP_ERROR)
            raise FeedHTTPError(f"feed transport error: {exc}") from exc

        if response.status_code >= 400:
            self._count_failure(FeedFailureKind.HTTP_ERROR, status=response.status_code)
            raise FeedHTTPError(
                f"feed returned HTTP {response.status_code}",
                status_code=response.status_code,
            )

        if not response.content:
            # Observed in the wild during agency deploys: 200 with no body.
            # Transient, so it retries rather than quarantining a whole tick.
            self._count_failure(FeedFailureKind.DECODE_ERROR)
            raise FeedDecodeError("feed returned HTTP 200 with an empty body")

        return response.content

    def _count_failure(self, kind: FeedFailureKind, *, status: int | None = None) -> None:
        attributes = {"agency": self._settings.feed_agency, "kind": str(kind)}
        if status is not None:
            attributes["status_code"] = str(status)
        self._metrics.feed_fetch_failures.add(1, attributes)


def _attempts_made(retrying: AsyncRetrying) -> int:
    attempts: Any = retrying.statistics.get("attempt_number", 0)
    return int(attempts)


def decode_feed(
    payload: bytes,
    *,
    agency: str,
    ingest_time: datetime,
) -> DecodedFeed:
    """Partition a raw feed body into valid pings and quarantine records.

    Never raises for a bad *entity* -- only for a body that is not a parseable
    ``FeedMessage`` at all, which is a property of the response rather than of
    any one vehicle.
    """
    metrics = get_metrics()
    tracer = get_tracer()
    trace_id = current_trace_id()

    with tracer.start_as_current_span(
        "collector.decode_feed",
        attributes={"feed.agency": agency, "feed.payload_bytes": len(payload)},
    ) as span:
        started = time.monotonic()
        message = gtfs.FeedMessage()
        try:
            message.ParseFromString(payload)
        except (DecodeError, UnicodeDecodeError) as exc:
            metrics.feed_fetch_failures.add(
                1, {"agency": agency, "kind": str(FeedFailureKind.DECODE_ERROR)}
            )
            span.set_status(Status(StatusCode.ERROR, "protobuf decode failed"))
            raise FeedDecodeError(f"feed body is not a GTFS-Realtime FeedMessage: {exc}") from exc

        feed_timestamp = (
            _epoch_to_utc(message.header.timestamp)
            if message.header.HasField("timestamp") and message.header.timestamp
            else None
        )

        pings: list[VehiclePing] = []
        quarantined: list[QuarantineRecord] = []
        vehicle_entities = 0

        for entity in message.entity:
            # A feed legitimately mixes trip updates, alerts and vehicle
            # positions in one message. Entities of the other kinds are not
            # this service's business, and skipping them is not a rejection.
            if not entity.HasField("vehicle"):
                continue
            vehicle_entities += 1
            outcome = _normalise_entity(
                entity,
                agency=agency,
                ingest_time=ingest_time,
                feed_timestamp=feed_timestamp,
                trace_id=trace_id,
            )
            if isinstance(outcome, VehiclePing):
                pings.append(outcome)
            else:
                quarantined.append(outcome)

        metrics.decode_duration.record(time.monotonic() - started, {"agency": agency})
        for record in quarantined:
            metrics.events_quarantined.add(1, {"agency": agency, "reason": str(record.reason)})

        span.set_attribute("feed.entity_count", len(message.entity))
        span.set_attribute("feed.vehicle_entity_count", vehicle_entities)
        span.set_attribute("feed.pings_valid", len(pings))
        span.set_attribute("feed.pings_quarantined", len(quarantined))

        return DecodedFeed(
            pings=pings,
            quarantined=quarantined,
            feed_timestamp=feed_timestamp,
            entity_count=len(message.entity),
            vehicle_entity_count=vehicle_entities,
        )


def _normalise_entity(
    entity: Any,
    *,
    agency: str,
    ingest_time: datetime,
    feed_timestamp: datetime | None,
    trace_id: str | None,
) -> VehiclePing | QuarantineRecord:
    vehicle = entity.vehicle

    def reject(
        reason: QuarantineReason, detail: str, vehicle_id: str | None = None
    ) -> QuarantineRecord:
        return QuarantineRecord(
            reason=reason,
            detail=detail[:4096],
            agency=agency,
            vehicle_id=vehicle_id,
            # Partial, not strict: GTFS-RT marks Position.latitude and
            # .longitude proto2 ``required``, and the Python parser accepts a
            # message that omits them while the strict serialiser refuses to
            # write one back. Re-serialising strictly here would raise
            # EncodeError on exactly the entities this function exists to
            # preserve, turning a quarantine record into a 500.
            raw_payload=entity.SerializePartialToString()[:MAX_QUARANTINE_PAYLOAD_BYTES],
            trace_id=trace_id,
        )

    # vehicle.vehicle.id is optional in the spec and absent in several real
    # feeds, so entity.id -- required, and stable per vehicle for position
    # entities -- is the fallback. This identifier becomes the Pub/Sub ordering
    # key and part of the idempotency key, so it has to be the same value on
    # every tick: both candidates are, and neither is invented here.
    vehicle_id = vehicle.vehicle.id.strip() or entity.id.strip()
    if not vehicle_id:
        return reject(
            QuarantineReason.SCHEMA_VALIDATION,
            "entity has neither vehicle.vehicle.id nor entity.id",
        )

    if not vehicle.HasField("position"):
        return reject(
            QuarantineReason.SCHEMA_VALIDATION,
            "vehicle entity carries no position",
            vehicle_id,
        )

    position = vehicle.position
    if is_null_island(position.latitude, position.longitude):
        return reject(
            QuarantineReason.NULL_ISLAND,
            "position is the (0, 0) GPS failure value",
            vehicle_id,
        )

    # An unset vehicle timestamp means "as of the feed header" per the spec.
    # With neither, the event time would have to be invented, and inventing it
    # as "now" would report zero ingest lag for the one feed whose lag cannot
    # be measured -- so the entity is quarantined instead.
    event_time = (
        _epoch_to_utc(vehicle.timestamp)
        if vehicle.HasField("timestamp") and vehicle.timestamp
        else feed_timestamp
    )
    if event_time is None:
        return reject(
            QuarantineReason.SCHEMA_VALIDATION,
            "neither the entity nor the feed header carries a timestamp",
            vehicle_id,
        )

    trip = vehicle.trip
    try:
        return VehiclePing(
            agency=agency,
            vehicle_id=vehicle_id,
            trip_id=trip.trip_id or None,
            route_id=trip.route_id or None,
            direction_id=trip.direction_id if trip.HasField("direction_id") else None,
            start_date=trip.start_date or None,
            schedule_relationship=_map_schedule_relationship(trip),
            latitude=position.latitude,
            longitude=position.longitude,
            bearing=position.bearing if position.HasField("bearing") else None,
            reported_speed_mps=position.speed if position.HasField("speed") else None,
            current_stop_sequence=(
                vehicle.current_stop_sequence if vehicle.HasField("current_stop_sequence") else None
            ),
            stop_id=vehicle.stop_id or None,
            occupancy_status=_map_occupancy(vehicle),
            event_time=event_time,
            ingest_time=ingest_time,
        )
    except ValidationError as exc:
        # Everything the contract rejects for a reason the decoder did not
        # anticipate: a bearing of 400 degrees, a negative speed, a stop id
        # longer than its column. Kept rather than dropped, with the pydantic
        # message as the detail so the fix is obvious from the quarantine row.
        return reject(
            QuarantineReason.SCHEMA_VALIDATION,
            "; ".join(
                f"{'.'.join(str(part) for part in err['loc'])}: {err['msg']}"
                for err in exc.errors()
            ),
            vehicle_id,
        )


def _map_schedule_relationship(trip: Any) -> ScheduleRelationship:
    """Map GTFS's enum onto the contract's, degrading to ``UNKNOWN``.

    The spec has gained values (``DUPLICATED``, ``NEW``) since this contract
    was written and will gain more. An unrecognised relationship is not a
    reason to reject a position that is otherwise fine.
    """
    if not trip.HasField("schedule_relationship"):
        return ScheduleRelationship.UNKNOWN
    name = gtfs.TripDescriptor.ScheduleRelationship.Name(trip.schedule_relationship)
    return ScheduleRelationship.__members__.get(str(name), ScheduleRelationship.UNKNOWN)


def _map_occupancy(vehicle: Any) -> OccupancyStatus:
    if not vehicle.HasField("occupancy_status"):
        return OccupancyStatus.UNKNOWN
    name = gtfs.VehiclePosition.OccupancyStatus.Name(vehicle.occupancy_status)
    return OccupancyStatus.__members__.get(str(name), OccupancyStatus.UNKNOWN)


def _epoch_to_utc(seconds: int) -> datetime:
    return datetime.fromtimestamp(seconds, tz=UTC)

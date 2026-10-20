"""Turning a raw ping into an enriched event.

Kept out of the FastAPI module so the interesting decisions -- what counts as
a teleport, which service day a 00:20 ping belongs to -- are testable without
an HTTP server or a broker, the same split the collector uses between
``main.py`` and ``feed.py``.

Enrichment adds three things the agency does not send:

* **Motion.** Distance and speed against the vehicle's previous position, from
  ``vehicle_live``. This is also the pipeline's data-quality gate: urban GPS
  reports coordinate jumps of several kilometres between consecutive pings,
  and accepting one silently corrupts every speed aggregate computed from the
  history afterwards.
* **Route naming**, from the static GTFS tables, so the serving API does not
  have to join on every request.
* **Schedule adherence**, which is the whole reason the static loader exists.
  See :mod:`transitpulse_core.schedule` for why that arithmetic is not
  "subtract two timestamps".

All three lookups degrade rather than fail: a vehicle seen for the first time
has no previous position, a trip added in realtime has no timetable, and an
event is still worth writing without either. Only an impossible speed is
treated as a rejection, because that one says the position itself is wrong.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta, tzinfo

import structlog

from transitpulse_core import schedule
from transitpulse_core.config import Settings, get_settings
from transitpulse_core.geo import haversine_meters, is_plausible_speed, speed_mps
from transitpulse_core.models import EnrichedVehicleEvent, VehiclePing
from transitpulse_core.sinks.postgres import PostgresSink

log = structlog.get_logger(__name__)


class ImplausibleSpeedError(Exception):
    """The position implies a speed no transit vehicle reaches.

    Carries the numbers that produced it so the quarantine row says *why* the
    event was rejected, not merely that it was -- a row reading "impossible
    speed" is a dead end during an incident, one reading "4.2 km in 11 s" is a
    diagnosis.
    """

    def __init__(self, *, speed: float, distance_m: float, seconds: float) -> None:
        self.speed = speed
        self.distance_m = distance_m
        self.seconds = seconds
        super().__init__(
            f"implausible speed {speed:.1f} m/s "
            f"({distance_m:.0f} m in {seconds:.1f} s) exceeds the plausible ceiling"
        )


@dataclass(frozen=True)
class _Motion:
    distance_m: float | None = None
    seconds: float | None = None
    speed: float | None = None


class Enricher:
    """Derives the enriched event for one ping.

    Takes the sink rather than a pool so that enrichment shares the serving
    layer's single connection pool; a second pool against the same Cloud SQL
    instance would double the connection count for no benefit.
    """

    def __init__(self, sink: PostgresSink, settings: Settings | None = None) -> None:
        self._sink = sink
        self._settings = settings or get_settings()
        self._timezone: tzinfo = self._settings.agency_tzinfo

    def enrich(self, ping: VehiclePing) -> EnrichedVehicleEvent:
        """Blocking: three short reads against Postgres. Call off the loop."""
        motion = self._motion(ping)
        adherence = self._adherence(ping)
        return EnrichedVehicleEvent(
            ping=ping,
            idempotency_key=ping.idempotency_key,
            computed_speed_mps=motion.speed,
            distance_from_last_m=motion.distance_m,
            seconds_since_last_ping=motion.seconds,
            schedule_delay_seconds=adherence.delay_seconds if adherence else None,
            is_on_time=adherence.on_time if adherence else None,
            route_short_name=self._route_short_name(ping),
            processed_time=datetime.now(UTC),
        )

    # ------------------------------------------------------------------
    def _motion(self, ping: VehiclePing) -> _Motion:
        """Distance and speed against the last known position.

        The speed gate is applied here rather than by the caller because this
        is the only place all three numbers exist together, and a rejection
        that cannot quote the distance and the interval is not diagnosable.
        """
        previous = self._sink.fetch_last_positions(ping.agency, [ping.vehicle_id]).get(
            ping.vehicle_id
        )
        if previous is None:
            return _Motion()

        last_lat, last_lon, last_event_time = previous
        seconds = (ping.event_time - last_event_time).total_seconds()
        distance = haversine_meters(last_lat, last_lon, ping.latitude, ping.longitude)
        # speed_mps returns None below its minimum interval, which also covers
        # the redelivery case: the same ping arriving twice has a zero gap,
        # and dividing a GPS jitter distance by it would invent a teleport.
        speed = speed_mps(distance, seconds)
        if speed is not None and not is_plausible_speed(speed):
            raise ImplausibleSpeedError(speed=speed, distance_m=distance, seconds=seconds)
        return _Motion(distance_m=distance, seconds=seconds, speed=speed)

    def _route_short_name(self, ping: VehiclePing) -> str | None:
        if ping.route_id is None:
            return None
        return self._sink.fetch_route_names([ping.route_id]).get(ping.route_id)

    def _adherence(self, ping: VehiclePing) -> schedule.ScheduleAdherence | None:
        """Delay against the timetable, or ``None`` when there is no timetable.

        A trip added in realtime, a feed that omits ``current_stop_sequence``,
        and a static load that has not run yet all land here. None of them is
        an error: the event is still a position worth recording, it just has no
        schedule to be late against.
        """
        if ping.trip_id is None or ping.current_stop_sequence is None:
            return None

        key = (ping.trip_id, ping.current_stop_sequence)
        scheduled: timedelta | None = self._sink.fetch_scheduled_stop_times([key]).get(key)
        if scheduled is None:
            return None

        return schedule.adherence(
            event_time=ping.event_time,
            scheduled_offset=scheduled,
            timezone=self._timezone,
            start_date=ping.start_date,
        )

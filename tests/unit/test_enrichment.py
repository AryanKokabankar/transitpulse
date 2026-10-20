"""Tests for the processor's enrichment maths.

The sink is stubbed rather than mocked at the driver level: what matters here
is what the enricher *derives* from a previous position and a timetable, and
that is arithmetic, not SQL. The SQL has its own integration tests.

The distinction being guarded is between a lookup that is merely absent -- a
vehicle seen for the first time, a trip added in realtime -- and a value that
is wrong. The first must degrade to ``None`` and still produce an event,
because a position is worth recording without a speed; the second must reject
the event, because a bad coordinate poisons every speed aggregate computed
from the history afterwards.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from services.processor.enrich import Enricher, ImplausibleSpeedError
from tests.conftest import FIXED_NOW, MakePing
from transitpulse_core.config import Settings
from transitpulse_core.geo import haversine_meters

# ~333 m north of the base coordinate, which at a 30 s gap is a plausible
# 11 m/s; the same jump over 10 s would not be.
BASE_LAT, BASE_LON = 40.7128, -74.0060
NEXT_LAT = 40.7158


class StubSink:
    """Answers the three enrichment lookups from dictionaries."""

    def __init__(
        self,
        *,
        last_positions: dict[str, tuple[float, float, datetime]] | None = None,
        route_names: dict[str, str] | None = None,
        stop_times: dict[tuple[str, int], timedelta] | None = None,
    ) -> None:
        self._last_positions = last_positions or {}
        self._route_names = route_names or {}
        self._stop_times = stop_times or {}
        self.stop_time_queries: list[list[tuple[str, int]]] = []

    def fetch_last_positions(
        self, agency: str, vehicle_ids: Any
    ) -> dict[str, tuple[float, float, datetime]]:
        return {v: self._last_positions[v] for v in vehicle_ids if v in self._last_positions}

    def fetch_route_names(self, route_ids: Any) -> dict[str, str]:
        return {r: self._route_names[r] for r in route_ids if r in self._route_names}

    def fetch_scheduled_stop_times(self, keys: Any) -> dict[tuple[str, int], timedelta]:
        self.stop_time_queries.append(list(keys))
        return {k: self._stop_times[k] for k in keys if k in self._stop_times}


def enricher(sink: StubSink, **overrides: object) -> Enricher:
    settings = Settings(agency_timezone="America/New_York", **overrides)  # type: ignore[arg-type]
    return Enricher(sink, settings)  # type: ignore[arg-type]


class TestMotion:
    def test_a_vehicle_seen_for_the_first_time_has_no_motion(self, make_ping: MakePing) -> None:
        """No previous position is normal, not an error: every vehicle has a
        first ping, and so does every instance after a cold start."""
        event = enricher(StubSink()).enrich(make_ping())

        assert event.distance_from_last_m is None
        assert event.seconds_since_last_ping is None
        assert event.computed_speed_mps is None

    def test_distance_and_speed_come_from_the_previous_position(self, make_ping: MakePing) -> None:
        sink = StubSink(
            last_positions={
                "bus-1": (BASE_LAT, BASE_LON, FIXED_NOW - timedelta(seconds=30)),
            }
        )
        ping = make_ping(
            vehicle_id="bus-1", latitude=NEXT_LAT, longitude=BASE_LON, event_time=FIXED_NOW
        )

        event = enricher(sink).enrich(ping)

        expected = haversine_meters(BASE_LAT, BASE_LON, NEXT_LAT, BASE_LON)
        assert event.distance_from_last_m == pytest.approx(expected)
        assert event.seconds_since_last_ping == 30.0
        assert event.computed_speed_mps == pytest.approx(expected / 30.0)

    def test_an_impossible_speed_is_rejected(self, make_ping: MakePing) -> None:
        """A 0.1 degree jump in ten seconds is a GPS glitch, not a vehicle."""
        sink = StubSink(
            last_positions={"bus-1": (BASE_LAT, BASE_LON, FIXED_NOW - timedelta(seconds=10))}
        )
        ping = make_ping(
            vehicle_id="bus-1", latitude=BASE_LAT + 0.1, longitude=BASE_LON, event_time=FIXED_NOW
        )

        with pytest.raises(ImplausibleSpeedError) as raised:
            enricher(sink).enrich(ping)

        assert raised.value.speed > 90.0
        assert raised.value.seconds == 10.0

    def test_the_rejection_quotes_the_numbers_that_caused_it(self, make_ping: MakePing) -> None:
        """A quarantine row reading "impossible speed" is a dead end during an
        incident; one reading "11 km in 10 s" is a diagnosis."""
        sink = StubSink(
            last_positions={"bus-1": (BASE_LAT, BASE_LON, FIXED_NOW - timedelta(seconds=10))}
        )
        ping = make_ping(
            vehicle_id="bus-1", latitude=BASE_LAT + 0.1, longitude=BASE_LON, event_time=FIXED_NOW
        )

        with pytest.raises(ImplausibleSpeedError) as raised:
            enricher(sink).enrich(ping)

        message = str(raised.value)
        assert "m in" in message and "s)" in message

    def test_a_redelivered_ping_computes_no_speed(self, make_ping: MakePing) -> None:
        """Redelivery is the normal operating condition, not an edge case.

        The live row already holds this event_time, so the gap is zero and any
        GPS jitter in the coordinates would divide into an invented teleport.
        """
        sink = StubSink(last_positions={"bus-1": (BASE_LAT, BASE_LON, FIXED_NOW)})
        ping = make_ping(
            vehicle_id="bus-1", latitude=NEXT_LAT, longitude=BASE_LON, event_time=FIXED_NOW
        )

        event = enricher(sink).enrich(ping)

        assert event.seconds_since_last_ping == 0.0
        assert event.computed_speed_mps is None

    def test_a_stale_out_of_order_ping_computes_no_speed(self, make_ping: MakePing) -> None:
        """An out-of-order delivery has a negative gap; a speed derived from
        it would be negative, which no aggregate downstream expects."""
        sink = StubSink(
            last_positions={"bus-1": (BASE_LAT, BASE_LON, FIXED_NOW + timedelta(seconds=30))}
        )
        ping = make_ping(
            vehicle_id="bus-1", latitude=NEXT_LAT, longitude=BASE_LON, event_time=FIXED_NOW
        )

        event = enricher(sink).enrich(ping)

        assert event.seconds_since_last_ping == -30.0
        assert event.computed_speed_mps is None


class TestRouteNaming:
    def test_the_short_name_is_attached(self, make_ping: MakePing) -> None:
        sink = StubSink(route_names={"Q": "Q"})
        event = enricher(sink).enrich(make_ping(route_id="Q"))
        assert event.route_short_name == "Q"

    def test_an_unloaded_route_leaves_it_unset(self, make_ping: MakePing) -> None:
        """Before the static loader has run, nothing resolves -- and the
        positions are still worth writing."""
        event = enricher(StubSink()).enrich(make_ping(route_id="Q"))
        assert event.route_short_name is None

    def test_a_ping_without_a_route_is_not_looked_up(self, make_ping: MakePing) -> None:
        event = enricher(StubSink()).enrich(make_ping(route_id=None))
        assert event.route_short_name is None


class TestScheduleDelay:
    def test_the_delay_joins_trip_and_stop_sequence(self, make_ping: MakePing) -> None:
        # Scheduled 08:15 local on the 15th; FIXED_NOW is 14:30 UTC = 10:30
        # EDT, so this vehicle is badly late.
        sink = StubSink(stop_times={("ABC123", 4): timedelta(hours=8, minutes=15)})
        ping = make_ping(trip_id="ABC123", current_stop_sequence=4, start_date="20261015")

        event = enricher(sink).enrich(ping)

        assert sink.stop_time_queries == [[("ABC123", 4)]]
        assert event.schedule_delay_seconds == 2 * 3600 + 15 * 60
        assert event.is_on_time is False

    def test_a_punctual_vehicle_is_on_time(self, make_ping: MakePing) -> None:
        sink = StubSink(stop_times={("ABC123", 4): timedelta(hours=10, minutes=30)})
        ping = make_ping(trip_id="ABC123", current_stop_sequence=4, start_date="20261015")

        event = enricher(sink).enrich(ping)

        assert event.schedule_delay_seconds == 0
        assert event.is_on_time is True

    def test_a_stop_time_past_midnight_is_not_a_day_of_delay(self, make_ping: MakePing) -> None:
        """The same case as the schedule unit tests, through the enricher, so
        a regression in the wiring is caught as well as one in the maths."""
        event_time = datetime(2026, 10, 16, 5, 32, tzinfo=UTC)  # 01:32 EDT
        sink = StubSink(stop_times={("NIGHT", 2): timedelta(hours=25, minutes=30)})
        ping = make_ping(
            trip_id="NIGHT",
            current_stop_sequence=2,
            start_date="20261015",
            event_time=event_time,
            ingest_time=event_time,
        )

        event = enricher(sink).enrich(ping)

        assert event.schedule_delay_seconds == 120

    def test_a_trip_with_no_timetable_has_no_delay(self, make_ping: MakePing) -> None:
        """Realtime-added trips genuinely have no scheduled time, so the
        absence has to be representable rather than defaulted to zero -- a
        zero would mean "exactly on time" and inflate the on-time rate."""
        ping = make_ping(trip_id="ADDED-1", current_stop_sequence=4)
        event = enricher(StubSink()).enrich(ping)
        assert event.schedule_delay_seconds is None
        assert event.is_on_time is None

    @pytest.mark.parametrize(
        ("trip_id", "stop_sequence"), [(None, 4), ("ABC123", None), (None, None)]
    )
    def test_an_incomplete_key_skips_the_lookup(
        self, make_ping: MakePing, trip_id: str | None, stop_sequence: int | None
    ) -> None:
        sink = StubSink(stop_times={("ABC123", 4): timedelta(hours=8)})
        event = enricher(sink).enrich(
            make_ping(trip_id=trip_id, current_stop_sequence=stop_sequence)
        )
        assert sink.stop_time_queries == []
        assert event.schedule_delay_seconds is None


class TestEventShape:
    def test_the_idempotency_key_is_the_pings_own(self, make_ping: MakePing) -> None:
        """Recomputing it here instead of copying it would let enrichment
        change the identity of an event and break deduplication (ADR-005)."""
        ping = make_ping()
        event = enricher(StubSink()).enrich(ping)
        assert event.idempotency_key == ping.idempotency_key

    def test_processed_time_is_timezone_aware(self, make_ping: MakePing) -> None:
        event = enricher(StubSink()).enrich(make_ping())
        assert event.processed_time.tzinfo is not None
        assert event.processed_time >= make_ping().ingest_time

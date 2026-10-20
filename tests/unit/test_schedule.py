"""Tests for GTFS schedule arithmetic.

Every bug this file guards against produces a *plausible* wrong answer rather
than an exception, which is why it is tested directly rather than through the
handler: a delay of 86,520 seconds is obviously broken, but one that is wrong
by exactly an hour on two Sundays a year looks like the feed misbehaving.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from transitpulse_core.schedule import (
    ON_TIME_EARLY_SECONDS,
    ON_TIME_LATE_SECONDS,
    adherence,
    is_on_time,
    parse_gtfs_time,
    parse_service_date,
    scheduled_instant,
    service_day_origin,
)

NY = ZoneInfo("America/New_York")


class TestParseGtfsTime:
    def test_an_ordinary_morning_time(self) -> None:
        assert parse_gtfs_time("08:15:30") == timedelta(hours=8, minutes=15, seconds=30)

    def test_hours_past_midnight_are_not_wrapped(self) -> None:
        """``25:30:00`` is 01:30 the next calendar day, not 01:30 today.

        Wrapping it would backdate the departure by 24 hours and report a
        punctual vehicle as a day early -- which is a number nobody reads as a
        bug in the parser.
        """
        assert parse_gtfs_time("25:30:00") == timedelta(days=1, hours=1, minutes=30)

    def test_exactly_midnight_at_the_end_of_a_service_day(self) -> None:
        assert parse_gtfs_time("24:00:00") == timedelta(days=1)

    def test_a_very_late_service_day(self) -> None:
        # Overnight services legitimately schedule past 48:00:00.
        assert parse_gtfs_time("49:05:00") == timedelta(days=2, hours=1, minutes=5)

    def test_a_single_digit_hour_is_accepted(self) -> None:
        # Not every producer zero-pads, and the spec does not require it.
        assert parse_gtfs_time("8:15:00") == timedelta(hours=8, minutes=15)

    @pytest.mark.parametrize("value", ["", "   "])
    def test_an_empty_time_is_absent_not_zero(self, value: str) -> None:
        """GTFS omits times at stops that are not timepoints.

        Reading that as 00:00:00 would make every such stop look scheduled for
        the start of the service day.
        """
        assert parse_gtfs_time(value) is None

    @pytest.mark.parametrize(
        "value", ["08:15", "08:60:00", "08:15:60", "2026-10-15 08:15:00", "eight fifteen", "0815"]
    )
    def test_malformed_times_raise(self, value: str) -> None:
        with pytest.raises(ValueError, match="not a GTFS time"):
            parse_gtfs_time(value)


class TestParseServiceDate:
    def test_parses_the_realtime_start_date(self) -> None:
        assert parse_service_date("20261015") == date(2026, 10, 15)

    @pytest.mark.parametrize("value", ["2026-10-15", "261015", ""])
    def test_malformed_dates_raise(self, value: str) -> None:
        with pytest.raises(ValueError, match="not a GTFS service date"):
            parse_service_date(value)


class TestServiceDayOrigin:
    def test_is_local_midnight_on_an_ordinary_summer_day(self) -> None:
        origin = service_day_origin(date(2026, 6, 15), NY)
        assert origin == datetime(2026, 6, 15, 4, 0, tzinfo=UTC)
        assert origin.astimezone(NY).hour == 0

    def test_is_local_midnight_on_an_ordinary_winter_day(self) -> None:
        """The same local time, a different UTC offset.

        Computing the origin in UTC directly -- rather than from the local
        date -- would be five hours out for half the year.
        """
        origin = service_day_origin(date(2026, 1, 15), NY)
        assert origin == datetime(2026, 1, 15, 5, 0, tzinfo=UTC)
        assert origin.astimezone(NY).hour == 0

    def test_dst_start_shortens_the_service_day(self) -> None:
        """On 2026-03-08 the clocks jump forward, so the day is 23 hours.

        The spec defines the origin as noon minus twelve *elapsed* hours, so
        subtracting a wall-clock twelve hours from local noon -- which is what
        ``aware_datetime - timedelta`` does -- gives the wrong instant by an
        hour here. The assertion is the elapsed-time answer.
        """
        local_noon = datetime.combine(date(2026, 3, 8), datetime.min.time(), tzinfo=NY).replace(
            hour=12
        )
        wall_clock_version = local_noon - timedelta(hours=12)

        origin = service_day_origin(date(2026, 3, 8), NY)

        assert origin == local_noon.astimezone(UTC) - timedelta(hours=12)
        assert origin != wall_clock_version.astimezone(UTC)
        assert (wall_clock_version.astimezone(UTC) - origin) == timedelta(hours=1)

    def test_dst_end_lengthens_the_service_day(self) -> None:
        local_noon = datetime.combine(date(2026, 11, 1), datetime.min.time(), tzinfo=NY).replace(
            hour=12
        )
        origin = service_day_origin(date(2026, 11, 1), NY)
        assert origin == local_noon.astimezone(UTC) - timedelta(hours=12)

    def test_noon_exists_exactly_once_on_every_date(self) -> None:
        """Why noon is the anchor: midnight is skipped or repeated in some
        timezones, so anchoring there would be ambiguous on those dates."""
        for day in (date(2026, 3, 8), date(2026, 11, 1), date(2026, 6, 15)):
            noon = datetime.combine(day, datetime.min.time(), tzinfo=NY).replace(hour=12)
            assert noon.astimezone(UTC).astimezone(NY) == noon


class TestScheduledInstant:
    def test_a_time_past_midnight_lands_on_the_next_calendar_day(self) -> None:
        instant = scheduled_instant(
            date(2026, 10, 15), parse_gtfs_time("25:30:00") or timedelta(), NY
        )
        local = instant.astimezone(NY)
        assert (local.date(), local.hour, local.minute) == (date(2026, 10, 16), 1, 30)

    def test_a_morning_time_lands_on_the_service_date(self) -> None:
        instant = scheduled_instant(date(2026, 10, 15), timedelta(hours=8, minutes=15), NY)
        local = instant.astimezone(NY)
        assert (local.date(), local.hour, local.minute) == (date(2026, 10, 15), 8, 15)


class TestOnTimeWindow:
    @pytest.mark.parametrize("delay", [0, 60, ON_TIME_LATE_SECONDS, ON_TIME_EARLY_SECONDS])
    def test_inside_the_window(self, delay: int) -> None:
        assert is_on_time(delay)

    @pytest.mark.parametrize("delay", [ON_TIME_LATE_SECONDS + 1, ON_TIME_EARLY_SECONDS - 1, 3600])
    def test_outside_the_window(self, delay: int) -> None:
        assert not is_on_time(delay)

    def test_late_matches_the_partial_index_threshold(self) -> None:
        """The schema indexes ``schedule_delay_seconds > 300``. If the window
        and the index disagreed, the index built for investigating late
        vehicles would not cover the rows the metric calls late."""
        assert ON_TIME_LATE_SECONDS == 300


class TestAdherence:
    def test_a_punctual_vehicle(self) -> None:
        result = adherence(
            event_time=datetime(2026, 10, 15, 12, 15, tzinfo=UTC),
            scheduled_offset=timedelta(hours=8, minutes=15),
            timezone=NY,
            start_date="20261015",
        )
        assert result.delay_seconds == 0
        assert result.on_time

    def test_a_late_vehicle(self) -> None:
        result = adherence(
            event_time=datetime(2026, 10, 15, 12, 22, tzinfo=UTC),
            scheduled_offset=timedelta(hours=8, minutes=15),
            timezone=NY,
            start_date="20261015",
        )
        assert result.delay_seconds == 420
        assert not result.on_time

    def test_an_early_vehicle_has_a_negative_delay(self) -> None:
        result = adherence(
            event_time=datetime(2026, 10, 15, 12, 10, tzinfo=UTC),
            scheduled_offset=timedelta(hours=8, minutes=15),
            timezone=NY,
            start_date="20261015",
        )
        assert result.delay_seconds == -300
        assert not result.on_time

    def test_a_stop_time_past_midnight_gives_a_small_delay(self) -> None:
        """The headline case: scheduled 25:30 on the 15th, seen at 01:32 local
        on the 16th, two minutes late.

        Mishandling the 25-hour time instead reports roughly a day of delay --
        the same arithmetic, off by one service day.
        """
        result = adherence(
            event_time=datetime(2026, 10, 16, 5, 32, tzinfo=UTC),  # 01:32 EDT
            scheduled_offset=timedelta(hours=25, minutes=30),
            timezone=NY,
            start_date="20261015",
        )
        assert result.delay_seconds == 120
        assert result.on_time
        assert result.scheduled_time == datetime(2026, 10, 16, 5, 30, tzinfo=UTC)

    def test_the_agency_service_date_wins_over_the_calendar_date(self) -> None:
        """A 01:32 local ping belongs to the previous service day, and
        ``start_date`` is the agency saying so. Using the UTC -- or even the
        local -- calendar date instead moves the whole timetable a day."""
        result = adherence(
            event_time=datetime(2026, 10, 16, 5, 32, tzinfo=UTC),
            scheduled_offset=timedelta(hours=25, minutes=30),
            timezone=NY,
            start_date="20261015",
        )
        assert result.service_date == date(2026, 10, 15)

    def test_without_a_start_date_the_nearer_service_day_is_chosen(self) -> None:
        """A feed that omits ``start_date`` still has to be placed somewhere.

        Scheduled 00:15 of the service day after the 15th, seen at 00:20 local
        on the 16th: five minutes late on the 15th's service day, or almost 24
        hours early on the 16th's. The former is the only sane reading.
        """
        result = adherence(
            event_time=datetime(2026, 10, 16, 4, 20, tzinfo=UTC),  # 00:20 EDT
            scheduled_offset=timedelta(hours=24, minutes=15),
            timezone=NY,
        )
        assert result.service_date == date(2026, 10, 15)
        assert result.delay_seconds == 300

    def test_without_a_start_date_a_daytime_ping_uses_today(self) -> None:
        result = adherence(
            event_time=datetime(2026, 10, 15, 12, 20, tzinfo=UTC),  # 08:20 EDT
            scheduled_offset=timedelta(hours=8, minutes=15),
            timezone=NY,
        )
        assert result.service_date == date(2026, 10, 15)
        assert result.delay_seconds == 300

    def test_a_utc_timezone_would_shift_the_delay_by_the_offset(self) -> None:
        """The reason ``agency_timezone`` is configuration and not a constant.

        The same event and the same timetable, read in UTC instead of Eastern,
        differ by exactly the agency's offset -- four hours of invented delay
        that no part of the pipeline would flag.
        """
        arguments = {
            "event_time": datetime(2026, 10, 15, 12, 15, tzinfo=UTC),
            "scheduled_offset": timedelta(hours=8, minutes=15),
            "start_date": "20261015",
        }
        local = adherence(timezone=NY, **arguments)  # type: ignore[arg-type]
        utc = adherence(timezone=UTC, **arguments)  # type: ignore[arg-type]
        assert local.delay_seconds == 0
        assert utc.delay_seconds - local.delay_seconds == 4 * 3600

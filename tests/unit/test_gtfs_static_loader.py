"""Tests for the static GTFS loader's parsing, without a database.

What is tested here is the translation from a real feed's CSV to database rows:
byte-order marks, empty optional columns, and the stop times past 24:00:00
that are the reason this job parses times itself instead of letting Postgres
cast the strings. The transactional load has its own integration test, because
atomicity is the database's behaviour rather than ours.
"""

from __future__ import annotations

import zipfile
from datetime import timedelta
from pathlib import Path

import pytest

from services.jobs.gtfs_static_loader import (
    TABLES,
    _route,
    _stop,
    _stop_time,
    _trip,
    check_agency_timezone,
    load_archive,
)
from tests.gtfs_fixtures import build_static_gtfs_zip
from transitpulse_core.config import Settings


class TestStopTimeRows:
    def test_an_ordinary_time_becomes_an_interval(self) -> None:
        row = _stop_time(
            {
                "trip_id": "ABC123",
                "stop_sequence": "4",
                "stop_id": "Q05N",
                "arrival_time": "08:15:00",
                "departure_time": "08:15:30",
            }
        )
        assert row == (
            "ABC123",
            4,
            "Q05N",
            timedelta(hours=8, minutes=15),
            timedelta(hours=8, minutes=15, seconds=30),
        )

    def test_a_time_past_midnight_keeps_its_extra_day(self) -> None:
        """``25:30:00`` has to reach the database as 25h30m.

        Casting it as a clock time is rejected outright; truncating it to
        01:30 loads cleanly and makes every overnight trip look a day early.
        """
        row = _stop_time(
            {
                "trip_id": "NIGHT1",
                "stop_sequence": "2",
                "stop_id": "Q05N",
                "arrival_time": "25:30:00",
                "departure_time": "25:30:00",
            }
        )
        assert row is not None
        assert row[4] == timedelta(days=1, hours=1, minutes=30)

    def test_a_stop_without_times_loads_with_nulls(self) -> None:
        """Non-timepoint stops have no times, and they are still real stops on
        a real trip -- dropping the row would lose the sequence."""
        row = _stop_time(
            {
                "trip_id": "NIGHT1",
                "stop_sequence": "3",
                "stop_id": "Q04N",
                "arrival_time": "",
                "departure_time": "",
            }
        )
        assert row == ("NIGHT1", 3, "Q04N", None, None)

    @pytest.mark.parametrize(
        "row",
        [
            {"stop_sequence": "4", "stop_id": "Q05N"},
            {"trip_id": "ABC123", "stop_id": "Q05N"},
            {"trip_id": "ABC123", "stop_sequence": "4"},
            {"trip_id": "ABC123", "stop_sequence": "not a number", "stop_id": "Q05N"},
        ],
    )
    def test_a_row_missing_part_of_the_key_is_skipped(self, row: dict[str, str]) -> None:
        """The primary key is ``(trip_id, stop_sequence)``; a row without one
        would abort the whole COPY and take the timetable with it."""
        assert _stop_time(row) is None

    def test_an_unparseable_time_is_skipped_rather_than_nulled(self) -> None:
        """A NULL here is indistinguishable from a legitimate non-timepoint,
        so the row is skipped and counted instead -- the gap in adherence
        coverage then has a number attached to it."""
        assert (
            _stop_time(
                {
                    "trip_id": "ABC123",
                    "stop_sequence": "4",
                    "stop_id": "Q05N",
                    "arrival_time": "8:15 AM",
                    "departure_time": "",
                }
            )
            is None
        )


class TestReferenceRows:
    def test_a_route_with_every_column(self) -> None:
        assert _route(
            {
                "route_id": "Q",
                "agency_id": "1",
                "route_short_name": "Q",
                "route_long_name": "Broadway Express",
                "route_type": "1",
            }
        ) == ("Q", "1", "Q", "Broadway Express", 1)

    def test_a_route_with_only_a_long_name(self) -> None:
        """Rail operators commonly omit ``route_short_name`` entirely."""
        assert _route({"route_id": "Q", "route_long_name": "Broadway Express"}) == (
            "Q",
            None,
            None,
            "Broadway Express",
            None,
        )

    def test_a_stop_without_coordinates(self) -> None:
        assert _stop({"stop_id": "Q04N", "stop_name": "72 St", "stop_lat": "", "stop_lon": ""}) == (
            "Q04N",
            "72 St",
            None,
            None,
        )

    def test_a_trip_needs_a_route(self) -> None:
        # route_id is NOT NULL in the schema, so a trip without one would
        # abort the load rather than simply be unjoinable.
        assert _trip({"trip_id": "ABC123"}) is None

    @pytest.mark.parametrize("builder", [_route, _stop, _trip])
    def test_a_row_without_its_primary_key_is_skipped(self, builder: object) -> None:
        assert builder({}) is None  # type: ignore[operator]


class TestArchiveValidation:
    def test_a_missing_gtfs_file_is_refused_before_truncating(self, tmp_path: Path) -> None:
        """The error has to come before the TRUNCATE, or a feed that happens
        to omit stop_times.txt would replace a working timetable with nothing.
        """
        archive = build_static_gtfs_zip(tmp_path / "gtfs.zip", omit=["stop_times.txt"])

        with pytest.raises(ValueError, match="missing required GTFS files"):
            load_archive(archive, Settings())

    def test_the_error_names_every_missing_file(self, tmp_path: Path) -> None:
        archive = build_static_gtfs_zip(tmp_path / "gtfs.zip", omit=["stop_times.txt", "trips.txt"])

        with pytest.raises(ValueError, match=r"stop_times\.txt") as raised:
            load_archive(archive, Settings())

        assert "trips.txt" in str(raised.value)

    def test_every_table_maps_to_a_gtfs_file(self) -> None:
        assert {spec.member for spec in TABLES} == {
            "routes.txt",
            "stops.txt",
            "trips.txt",
            "stop_times.txt",
        }


class TestAgencyTimezoneCheck:
    def test_a_matching_timezone_is_returned(self, tmp_path: Path) -> None:
        path = build_static_gtfs_zip(tmp_path / "gtfs.zip", agency_timezone="America/New_York")
        with zipfile.ZipFile(path) as archive:
            assert check_agency_timezone(archive, "America/New_York") == "America/New_York"

    def test_a_mismatch_is_reported(self, tmp_path: Path) -> None:
        """A wrong timezone does not fail -- it produces schedule delays that
        are off by whole hours and look entirely plausible. Comparing against
        the feed's own declaration is the only cheap way to catch it."""
        path = build_static_gtfs_zip(tmp_path / "gtfs.zip", agency_timezone="America/Chicago")
        with zipfile.ZipFile(path) as archive:
            assert check_agency_timezone(archive, "America/New_York") == "America/Chicago"

    def test_a_feed_without_agency_txt_is_tolerated(self, tmp_path: Path) -> None:
        # agency.txt is required by the spec, but the loader's job is to load
        # the timetable, not to validate the whole feed.
        path = build_static_gtfs_zip(tmp_path / "gtfs.zip", agency_timezone=None)
        with zipfile.ZipFile(path) as archive:
            assert check_agency_timezone(archive, "America/New_York") is None

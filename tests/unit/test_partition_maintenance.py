"""Tests for the partition window arithmetic and the drop guards.

No database here on purpose. ``build_plan`` was split out of the execution path
precisely so the off-by-one risks -- which cost either refused inserts or
deleted data -- can be pinned down cheaply.
"""

from __future__ import annotations

from datetime import date, timedelta
from itertools import pairwise

import pytest

from services.jobs.partition_maintenance import (
    Outcome,
    build_plan,
    partition_day,
)

TODAY = date(2026, 10, 20)


class TestPlanWindow:
    def test_starts_from_yesterday(self) -> None:
        """Late pings for the previous service day are normal traffic."""
        plan = build_plan(TODAY, ahead_days=14, retention_days=7)
        assert plan.ensure_days[0] == date(2026, 10, 19)

    def test_covers_today(self) -> None:
        assert TODAY in build_plan(TODAY, ahead_days=1, retention_days=7).ensure_days

    def test_reaches_at_least_the_requested_days_ahead(self) -> None:
        """ahead_days is a floor, not a target: a job that runs a few hours
        late must still leave the requested margin in front of it."""
        plan = build_plan(TODAY, ahead_days=14, retention_days=7)
        assert plan.horizon >= TODAY + timedelta(days=14)

    def test_days_are_contiguous_with_no_gaps(self) -> None:
        """A single missing day in the middle would refuse inserts for that
        day only -- the most confusing possible failure."""
        days = build_plan(TODAY, ahead_days=30, retention_days=7).ensure_days
        gaps = [(a, b) for a, b in pairwise(days) if (b - a).days != 1]
        assert gaps == []

    def test_drop_cutoff_is_retention_days_behind_today(self) -> None:
        plan = build_plan(TODAY, ahead_days=14, retention_days=7)
        assert plan.drop_before == date(2026, 10, 13)

    def test_cutoff_never_reaches_today(self) -> None:
        """The invariant that stops the job deleting live data."""
        for retention in range(1, 400):
            plan = build_plan(TODAY, ahead_days=1, retention_days=retention)
            assert plan.drop_before < TODAY

    def test_ensure_and_drop_windows_do_not_overlap(self) -> None:
        plan = build_plan(TODAY, ahead_days=14, retention_days=7)
        assert all(day >= plan.drop_before for day in plan.ensure_days)

    @pytest.mark.parametrize("ahead", [0, -1, -365])
    def test_rejects_a_non_positive_horizon(self, ahead: int) -> None:
        with pytest.raises(ValueError, match="ahead_days"):
            build_plan(TODAY, ahead_days=ahead, retention_days=7)

    @pytest.mark.parametrize("retention", [0, -1, -30])
    def test_rejects_a_non_positive_retention(self, retention: int) -> None:
        """retention_days=0 would compute a cutoff of today and drop the
        partition currently being written to."""
        with pytest.raises(ValueError, match="retention_days"):
            build_plan(TODAY, ahead_days=14, retention_days=retention)

    def test_crosses_a_month_boundary(self) -> None:
        plan = build_plan(date(2026, 10, 30), ahead_days=5, retention_days=2)
        assert date(2026, 11, 4) in plan.ensure_days

    def test_crosses_a_year_boundary(self) -> None:
        plan = build_plan(date(2026, 12, 30), ahead_days=5, retention_days=2)
        assert date(2027, 1, 3) in plan.ensure_days

    def test_handles_a_leap_day(self) -> None:
        plan = build_plan(date(2028, 2, 28), ahead_days=2, retention_days=2)
        assert date(2028, 2, 29) in plan.ensure_days


class TestPartitionDayParsing:
    def test_parses_a_generated_name(self) -> None:
        assert partition_day("vehicle_events_20261020") == date(2026, 10, 20)

    @pytest.mark.parametrize(
        "relname",
        [
            "vehicle_events",  # the parent itself
            "vehicle_events_20261020_backup",  # someone's leftover copy
            "vehicle_events_2026102",  # too short
            "vehicle_events_202610200",  # too long
            "vehicle_events_20261320",  # month 13
            "vehicle_events_20260230",  # 30 February
            "vehicle_events_abcdefgh",  # not digits
            "vehicle_live",  # a different table
            "other_table_20261020",  # wrong prefix
        ],
    )
    def test_refuses_anything_it_did_not_generate(self, relname: str) -> None:
        """This function gates a DROP, so a false positive deletes data."""
        assert partition_day(relname) is None


class TestOutcome:
    def test_reports_counts_for_structured_logging(self) -> None:
        outcome = Outcome(created=["a", "b"], dropped=["c"], retained=["d", "e", "f"])
        summary = outcome.as_dict()
        assert summary["created_count"] == 2
        assert summary["dropped_count"] == 1
        assert summary["retained_count"] == 3

    def test_defaults_to_a_real_run(self) -> None:
        assert Outcome().dry_run is False

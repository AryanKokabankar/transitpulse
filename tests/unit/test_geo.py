"""Tests for the motion maths and the data-quality speed gate."""

from __future__ import annotations

import pytest

from transitpulse_core.geo import (
    MAX_PLAUSIBLE_SPEED_MPS,
    bearing_degrees,
    haversine_meters,
    is_plausible_speed,
    speed_mps,
)

# Known reference pairs, distances from an independent geodesic calculator.
NYC = (40.7128, -74.0060)
PHILLY = (39.9526, -75.1652)
TIMES_SQ = (40.7580, -73.9855)
GRAND_CENTRAL = (40.7527, -73.9772)


class TestHaversine:
    def test_zero_for_identical_points(self) -> None:
        assert haversine_meters(*NYC, *NYC) == pytest.approx(0.0, abs=1e-9)

    def test_city_scale_distance(self) -> None:
        # NYC -> Philadelphia is ~129.6 km; 0.5% tolerance covers the
        # difference between a spherical and an ellipsoidal model.
        assert haversine_meters(*NYC, *PHILLY) == pytest.approx(129_600, rel=0.005)

    def test_block_scale_distance(self) -> None:
        # Times Square -> Grand Central: ~915 m great-circle. Deliberately the
        # straight-line figure, not the ~1.05 km walking route -- vehicle
        # positions are compared as points, so street distance is the wrong
        # reference and would make this test wrong in a way that looks right.
        assert haversine_meters(*TIMES_SQ, *GRAND_CENTRAL) == pytest.approx(915, rel=0.02)

    def test_is_symmetric(self) -> None:
        assert haversine_meters(*NYC, *PHILLY) == pytest.approx(haversine_meters(*PHILLY, *NYC))

    def test_handles_antimeridian(self) -> None:
        """Crossing +/-180 must not produce a near-circumference distance."""
        assert haversine_meters(0.0, 179.9, 0.0, -179.9) == pytest.approx(22_240, rel=0.01)


class TestBearing:
    def test_due_north(self) -> None:
        assert bearing_degrees(0.0, 0.0, 10.0, 0.0) == pytest.approx(0.0, abs=0.01)

    def test_due_east(self) -> None:
        assert bearing_degrees(0.0, 0.0, 0.0, 10.0) == pytest.approx(90.0, abs=0.01)

    def test_always_in_range(self) -> None:
        assert 0.0 <= bearing_degrees(*GRAND_CENTRAL, *TIMES_SQ) < 360.0


class TestSpeed:
    def test_computes_average_speed(self) -> None:
        assert speed_mps(300.0, 30.0) == pytest.approx(10.0)

    def test_returns_none_below_the_minimum_interval(self) -> None:
        """Sub-second gaps are GPS noise, so a speed from them is meaningless."""
        assert speed_mps(50.0, 0.4) is None

    @pytest.mark.parametrize("value", [0.0, 11.2, MAX_PLAUSIBLE_SPEED_MPS])
    def test_accepts_plausible_speeds(self, value: float) -> None:
        assert is_plausible_speed(value)

    @pytest.mark.parametrize("value", [MAX_PLAUSIBLE_SPEED_MPS + 0.1, 5_000.0, -1.0])
    def test_rejects_implausible_speeds(self, value: float) -> None:
        assert not is_plausible_speed(value)

    def test_treats_missing_speed_as_acceptable(self) -> None:
        # A vehicle with no previous ping has no computed speed; that is a
        # normal first observation, not a quality failure.
        assert is_plausible_speed(None)

    def test_catches_a_gps_teleport(self) -> None:
        """The case this gate exists for: a 40km jump between 30s pings."""
        distance = haversine_meters(*NYC, 40.9, -74.2)
        assert not is_plausible_speed(speed_mps(distance, 30.0))

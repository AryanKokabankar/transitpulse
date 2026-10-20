"""Geospatial helpers for motion enrichment.

Pure functions with no I/O, so the enrichment maths is unit-testable without a
database, a broker, or a feed. The speed gate below is the main data-quality
control in the pipeline: GPS in dense urban transit environments routinely
reports coordinate jumps of several kilometres between consecutive pings, and
accepting those silently corrupts every downstream speed aggregate.
"""

from __future__ import annotations

import math

EARTH_RADIUS_M = 6_371_008.8

# A tunnel-running subway car tops out near 45 m/s. Anything materially above
# that is a coordinate glitch, not a vehicle, so 90 m/s (~200 mph) is a
# deliberately generous ceiling that still catches teleports.
MAX_PLAUSIBLE_SPEED_MPS = 90.0

# Below this gap the position delta is mostly GPS noise, so computing a speed
# from it produces garbage rather than signal.
MIN_INTERVAL_SECONDS = 1.0


def haversine_meters(
    lat1: float,
    lon1: float,
    lat2: float,
    lon2: float,
) -> float:
    """Great-circle distance in metres.

    Haversine rather than a projected-plane approximation: transit agencies
    span enough latitude that a flat-earth shortcut drifts noticeably, and the
    cost difference is irrelevant next to the network I/O around it.
    """
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    d_phi = phi2 - phi1
    d_lambda = math.radians(lon2 - lon1)

    a = math.sin(d_phi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(d_lambda / 2) ** 2
    return 2 * EARTH_RADIUS_M * math.asin(math.sqrt(min(1.0, a)))


def bearing_degrees(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Initial compass bearing from point 1 to point 2, in [0, 360)."""
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    d_lambda = math.radians(lon2 - lon1)

    y = math.sin(d_lambda) * math.cos(phi2)
    x = math.cos(phi1) * math.sin(phi2) - math.sin(phi1) * math.cos(phi2) * math.cos(d_lambda)
    return (math.degrees(math.atan2(y, x)) + 360.0) % 360.0


def speed_mps(distance_m: float, seconds: float) -> float | None:
    """Average speed, or ``None`` when the interval is too short to mean anything."""
    if seconds < MIN_INTERVAL_SECONDS:
        return None
    return distance_m / seconds


def is_plausible_speed(value: float | None) -> bool:
    """Whether a computed speed is physically possible for a transit vehicle."""
    if value is None:
        return True
    return 0.0 <= value <= MAX_PLAUSIBLE_SPEED_MPS

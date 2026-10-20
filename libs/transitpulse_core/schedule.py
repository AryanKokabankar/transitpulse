"""Schedule adherence: turning a static GTFS timetable into a delay in seconds.

Two properties of GTFS time make the obvious implementation wrong by hours,
and both of them fail quietly -- the delays that come out are the right order
of magnitude and the wrong number, so nothing looks broken.

**GTFS times are offsets from the service day, not clock times.** A trip
departing at 01:30 the morning after its service day began is written
``25:30:00``. ``strptime`` raises on that; taking the hour modulo 24 instead
backdates the departure a full day and reports a punctual vehicle as 24 hours
early. So times are parsed into a :class:`~datetime.timedelta` and added to
the service day's origin, which is the only representation that survives the
values past midnight.

**The service day is local and is not always 24 hours long.** The spec puts
its origin at noon minus twelve hours in the *agency's* timezone -- local
midnight on every day except the two when DST shifts. Subtracting twelve
wall-clock hours from local noon yields local midnight on those days too,
which is the wrong instant by an hour; the spec means twelve *elapsed* hours,
so the subtraction has to happen in UTC. An hour of phantom delay on two days
a year is exactly the kind of defect that gets diagnosed as "the feed was
weird that morning".

Pure functions with no I/O, so all of this is unit-testable without a database.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta, tzinfo

# Hours are unbounded in principle but a GTFS day is at most ~48h in practice;
# three digits accepts anything real while still rejecting a stray date.
_GTFS_TIME = re.compile(r"^(\d{1,3}):([0-5]\d):([0-5]\d)$")
_SERVICE_DATE = re.compile(r"^\d{8}$")

# The on-time window transit agencies actually publish against: a little early
# is fine, five minutes late is the threshold. It matches the partial index on
# ``schedule_delay_seconds > 300`` in the schema, so "late" means the same
# thing to the query planner and to the metric.
ON_TIME_EARLY_SECONDS = -60
ON_TIME_LATE_SECONDS = 300


@dataclass(frozen=True)
class ScheduleAdherence:
    """How far off the timetable one observation was."""

    delay_seconds: int
    on_time: bool
    scheduled_time: datetime
    service_date: date


def parse_gtfs_time(value: str) -> timedelta | None:
    """Parse ``HH:MM:SS`` as an offset from the start of the service day.

    Hours of 24 and above are normal and meaningful: ``25:30:00`` is 01:30 on
    the following calendar day, still belonging to the service day that began
    the morning before. ``None`` is returned for an empty value, which GTFS
    allows at stops that are not timepoints.
    """
    stripped = value.strip()
    if not stripped:
        return None
    match = _GTFS_TIME.match(stripped)
    if match is None:
        raise ValueError(f"not a GTFS time: {value!r}")
    hours, minutes, seconds = (int(part) for part in match.groups())
    return timedelta(hours=hours, minutes=minutes, seconds=seconds)


def parse_service_date(value: str) -> date:
    """Parse the ``YYYYMMDD`` service date GTFS-RT puts on a trip descriptor."""
    if not _SERVICE_DATE.match(value):
        raise ValueError(f"not a GTFS service date: {value!r}")
    return date(int(value[0:4]), int(value[4:6]), int(value[6:8]))


def service_day_origin(service_date: date, timezone: tzinfo) -> datetime:
    """The instant a service day begins, as defined by the GTFS spec.

    Noon minus twelve hours, with the subtraction done in UTC so that it is
    twelve elapsed hours rather than twelve wall-clock hours. Noon is the
    anchor precisely because it is the one local time that is never skipped or
    repeated by a DST transition, so it exists exactly once on every date.
    """
    local_noon = datetime.combine(service_date, time(12, 0), tzinfo=timezone)
    return local_noon.astimezone(UTC) - timedelta(hours=12)


def scheduled_instant(service_date: date, offset: timedelta, timezone: tzinfo) -> datetime:
    """The absolute UTC instant a stop time refers to."""
    return service_day_origin(service_date, timezone) + offset


def is_on_time(delay_seconds: int) -> bool:
    return ON_TIME_EARLY_SECONDS <= delay_seconds <= ON_TIME_LATE_SECONDS


def adherence(
    *,
    event_time: datetime,
    scheduled_offset: timedelta,
    timezone: tzinfo,
    start_date: str | None = None,
) -> ScheduleAdherence:
    """Compare an observation against its scheduled stop time.

    ``start_date`` is the service date the agency itself put on the trip and is
    used verbatim when present. Without it the service date has to be guessed,
    and the naive guess -- the local calendar date of the observation -- is
    wrong for exactly the vehicles this module exists to handle: a bus pinging
    at 00:20 local is usually still running the previous service day's trip.
    Both candidate dates are therefore scored against the timetable and the
    nearer one wins, which is the only evidence available without the calendar.
    """
    if start_date is not None:
        candidates = [parse_service_date(start_date)]
    else:
        local_date = event_time.astimezone(timezone).date()
        candidates = [local_date, local_date - timedelta(days=1)]

    service_date, scheduled = min(
        ((day, scheduled_instant(day, scheduled_offset, timezone)) for day in candidates),
        key=lambda pair: abs(pair[1] - event_time),
    )
    delay = round((event_time - scheduled).total_seconds())
    return ScheduleAdherence(
        delay_seconds=delay,
        on_time=is_on_time(delay),
        scheduled_time=scheduled,
        service_date=service_date,
    )

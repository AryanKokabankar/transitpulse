"""Turning the archived snapshot corpus back into publishable pings.

The corpus is real feed output, which is the point: synthetic positions are too
clean to exercise the quarantine path, and too uniform to produce a realistic
spread of route ids and update intervals. But a corpus captured from one agency
holds only as many vehicles as that agency runs, which caps load long before
the pipeline's limit. Fan-out closes that gap by cloning each ping into *K*
distinct synthetic vehicles with jittered positions, so the shape of the data
stays real while the volume becomes a parameter.

Two correctness properties the harness depends on:

**Synthetic vehicles are genuinely distinct.** Each clone gets its own
``vehicle_id``, so each gets its own Pub/Sub ordering key and its own
idempotency key. Cloning without that would publish *K* copies of one event and
measure the deduplication path while appearing to measure the write path.

**Timestamps are re-based by default.** Replaying yesterday's corpus with its
original ``event_time`` makes every message hours stale, which both floods the
lag SLI with values that describe the corpus rather than the pipeline and
dedupes against rows a previous run already wrote. ``--preserve-timestamps``
keeps the original times for the case that genuinely wants them: reprocessing a
window after fixing a bug.
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import structlog

from services.collector.feed import decode_feed
from transitpulse_core.models import VehiclePing, is_null_island
from transitpulse_core.snapshots import SnapshotArchive

log = structlog.get_logger(__name__)

# Roughly a kilometre at mid-latitudes: enough that clones are distinct
# positions rather than a stack of identical coordinates, small enough that a
# clone stays inside the agency's service area.
JITTER_DEGREES = 0.01

VEHICLE_ID_MAX_LENGTH = 128


class EmptyCorpusError(RuntimeError):
    """Raised when there is nothing to replay, with how to fix it."""


@dataclass(frozen=True)
class LoadedCorpus:
    """Every ping the corpus yielded, with the provenance to explain it."""

    pings: list[VehiclePing]
    snapshot_count: int
    quarantined_count: int
    event_time_span: timedelta
    duplicate_key_count: int = 0

    @property
    def size(self) -> int:
        return len(self.pings)

    @property
    def duplicate_key_fraction(self) -> float:
        """Share of the corpus that will deduplicate instead of being written.

        Agencies re-send a ping they have already sent, so a corpus captured
        over several ticks genuinely contains the same observation more than
        once. Replaying it is faithful, but that fraction of a load run
        exercises the deduplication path rather than the write path -- and a
        throughput figure that silently included it would overstate how much
        work the pipeline actually did.
        """
        return self.duplicate_key_count / len(self.pings) if self.pings else 0.0


def load_corpus(
    archive: SnapshotArchive,
    *,
    agency: str | None = None,
    limit: int | None = None,
) -> LoadedCorpus:
    """Decode archived snapshots into pings, newest-last.

    Entities the decoder rejects are counted rather than raised: a corpus that
    contains garbage is a corpus doing its job, and refusing to load because of
    it would make the harness unusable against real captures.
    """
    paths = archive.list_payloads(agency=agency, limit=limit)
    if not paths:
        raise EmptyCorpusError(
            "the replay corpus is empty. Capture at least one snapshot first: "
            "start the collector and POST /collect, or run `python tasks.py capture`."
        )

    pings: list[VehiclePing] = []
    quarantined = 0
    # Decoding needs an ingest_time and the harness re-stamps it per emission
    # anyway, so this value never reaches the broker.
    load_time = datetime.now(UTC)

    for path in paths:
        payload = archive.load_payload(path)
        decoded = decode_feed(
            payload,
            agency=agency or _agency_from_path(path),
            ingest_time=load_time,
        )
        pings.extend(decoded.pings)
        quarantined += len(decoded.quarantined)

    if not pings:
        raise EmptyCorpusError(
            f"{len(paths)} snapshot(s) decoded but yielded no valid pings "
            f"({quarantined} entities quarantined); the corpus is unusable as load."
        )

    event_times = [ping.event_time for ping in pings]
    duplicate_keys = len(pings) - len({ping.idempotency_key for ping in pings})
    log.info(
        "corpus.loaded",
        snapshots=len(paths),
        pings=len(pings),
        quarantined=quarantined,
        vehicles=len({ping.vehicle_id for ping in pings}),
        duplicate_keys=duplicate_keys,
    )
    return LoadedCorpus(
        pings=pings,
        snapshot_count=len(paths),
        quarantined_count=quarantined,
        event_time_span=max(event_times) - min(event_times),
        duplicate_key_count=duplicate_keys,
    )


def _agency_from_path(path: str) -> str:
    """The archive layout puts the agency first, so the corpus is
    self-describing."""
    return path.split("/", 1)[0]


def fan_out(pings: list[VehiclePing], factor: int, *, seed: int = 0) -> list[VehiclePing]:
    """Expand each ping into ``factor`` distinct synthetic vehicles.

    ``factor`` of 1 returns the originals untouched, so a run without fan-out
    replays exactly what was captured.
    """
    if factor < 1:
        raise ValueError("fanout factor must be at least 1")
    if factor == 1:
        return list(pings)

    # Seeded so a run is reproducible: a benchmark whose input changes between
    # runs cannot be used to detect a regression. Not a security context -- this
    # jitters synthetic bus positions.
    rng = random.Random(seed)  # noqa: S311
    expanded: list[VehiclePing] = []

    for ping in pings:
        expanded.append(ping)
        for clone_index in range(1, factor):
            expanded.append(_clone_vehicle(ping, clone_index, rng))
    return expanded


def suffixed_vehicle_id(base: str, suffix: str) -> str:
    """Append a synthetic-vehicle suffix without breaking the contract.

    ``vehicle_id`` is capped at 128 characters, and an agency that already
    uses long identifiers would otherwise fail validation partway through a
    run.
    """
    return f"{base[: VEHICLE_ID_MAX_LENGTH - len(suffix)]}{suffix}"


def _clone_vehicle(ping: VehiclePing, clone_index: int, rng: random.Random) -> VehiclePing:
    suffix = f"#r{clone_index}"

    latitude = _clamp(ping.latitude + rng.uniform(-JITTER_DEGREES, JITTER_DEGREES), 90.0)
    longitude = _clamp(ping.longitude + rng.uniform(-JITTER_DEGREES, JITTER_DEGREES), 180.0)
    if is_null_island(latitude, longitude):
        # Vanishingly unlikely, but a jittered clone that landed on (0, 0)
        # would fail validation and abort the run mid-flight.
        latitude += JITTER_DEGREES

    # Reconstructed through the model rather than copied, so a clone that the
    # contract would reject is caught here instead of at publish time.
    return VehiclePing(
        **{
            **ping.model_dump(),
            "vehicle_id": suffixed_vehicle_id(ping.vehicle_id, suffix),
            "latitude": latitude,
            "longitude": longitude,
        }
    )


def _clamp(value: float, bound: float) -> float:
    return max(-bound, min(bound, value))

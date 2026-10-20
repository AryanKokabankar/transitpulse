"""Capture one feed snapshot into the replay corpus, without publishing.

The load harness needs a corpus before it can replay anything, and the corpus
is only ever produced by a real fetch -- so this is the one-shot version of a
collector tick with the broker left out:

    python tasks.py capture            # one snapshot
    python tasks.py capture -- --count 20 --interval 15

Running it with ``--count`` and the agency's own publish interval is how a
corpus worth load-testing against gets built: twenty ticks fifteen seconds
apart hold real vehicle movement, which is what makes the replayed positions
produce plausible speeds downstream rather than a stack of identical
coordinates.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from datetime import UTC, datetime

import structlog

from services.collector.feed import FeedClient, FeedError, decode_feed
from transitpulse_core.config import get_settings
from transitpulse_core.models import FeedSnapshot
from transitpulse_core.snapshots import build_snapshot_archive
from transitpulse_core.telemetry import setup_telemetry

log = structlog.get_logger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="capture",
        description="Fetch the configured GTFS-Realtime feed and archive the raw response.",
    )
    parser.add_argument(
        "--count", type=int, default=1, help="how many snapshots to capture (default: 1)"
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=15.0,
        help="seconds between captures; match the agency's publish interval (default: 15)",
    )
    return parser


async def capture_once() -> FeedSnapshot:
    """One fetch, archived exactly as the collector would archive it."""
    settings = get_settings()
    archive = build_snapshot_archive(settings)
    client = FeedClient(settings)
    try:
        fetched = await client.fetch()
    finally:
        await client.aclose()

    path = archive.put_payload(
        fetched.payload, agency=settings.feed_agency, fetched_at=fetched.fetched_at
    )
    decoded = decode_feed(
        fetched.payload, agency=settings.feed_agency, ingest_time=datetime.now(UTC)
    )
    snapshot = FeedSnapshot(
        agency=settings.feed_agency,
        fetched_at=fetched.fetched_at,
        feed_timestamp=decoded.feed_timestamp,
        entity_count=decoded.entity_count,
        payload_bytes=len(fetched.payload),
        object_path=path,
        content_sha256=FeedSnapshot.sha256_of(fetched.payload),
    )
    archive.put_metadata(snapshot)
    if decoded.quarantined:
        archive.put_quarantine(path, decoded.quarantined)

    log.info(
        "capture.stored",
        path=path,
        bytes=len(fetched.payload),
        entities=decoded.entity_count,
        pings=len(decoded.pings),
        quarantined=len(decoded.quarantined),
    )
    return snapshot


async def _run(count: int, interval: float) -> int:
    captured = 0
    for index in range(count):
        try:
            await capture_once()
            captured += 1
        except FeedError as exc:
            print(f"error: feed unavailable: {exc}", file=sys.stderr)
            # Keep going: a corpus with nineteen of twenty ticks is still a
            # corpus, and a single bad fetch during a long capture is normal.
            if count == 1:
                return 1
        if index + 1 < count:
            await asyncio.sleep(interval)

    print(f"captured {captured} of {count} snapshot(s)")
    return 0 if captured else 1


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    setup_telemetry("transitpulse-capture", instrument_libraries=False)
    return asyncio.run(_run(args.count, args.interval))


if __name__ == "__main__":  # pragma: no cover - module entry point
    raise SystemExit(main())

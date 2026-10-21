"""Experiment 2: corrupt a known number of entities and account for all of them.

"Nothing is dropped silently" is the project's central claim and the hardest
one to check, because a pipeline that drops data quietly looks exactly like a
pipeline with nothing to drop. The only way to tell is to put in a known number
of bad records and insist that every one of them is found somewhere afterwards.

So the arithmetic is the experiment:

    vehicle entities in the feed  ==  published  +  quarantined

If that identity holds and the quarantined count equals the baseline plus
exactly what was injected, nothing went missing. If it fails, the shortfall is
the number of events the pipeline lost, which is a far more useful output than
a log line.

Both producers that exist are exercised, because they fail in different places:

* **The feed path.** A real archived snapshot, with every *n*-th otherwise-valid
  entity corrupted in place and the whole thing re-serialised and served over
  loopback, driven through the real ``POST /collect``. The collector has no
  database (ADR-006), so its rejections land in the archive sidecar beside the
  payload that produced them -- which is where this checks for them.
* **The broker path.** Payloads published straight to the topic, as a producer
  that is not this codebase would. Those reach the processor, so they land in
  the ``quarantine`` table with their raw bytes.

Only entities the decoder accepted in the baseline are corrupted. Corrupting
one that was already being rejected would inject a record that changes no
count, and the experiment would pass while measuring nothing.

Timestamps are shifted onto the present by one constant offset, which keeps the
feed's internal skew intact while putting the events in today's partition. The
alternative -- replaying a five-day-old capture as-is -- fails on whichever day
the partition for that date is pruned, which is a false negative waiting to
happen.

    python -m tools.chaos.malformed_feed --corrupt-every 5
"""

from __future__ import annotations

import argparse
import asyncio
import json
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime

import httpx
import structlog
from google.transit import gtfs_realtime_pb2 as gtfs

from services.collector.feed import decode_feed
from services.collector.main import CollectResult
from services.collector.main import app as collector_app
from tools.chaos.experiment import (
    ScenarioResult,
    add_common_arguments,
    at_least,
    equals,
    scenario_main,
)
from tools.chaos.support import (
    await_metric_export,
    chaos_agency,
    count_events,
    count_quarantine_since,
    database,
    delete_agency_rows,
    delete_agency_snapshots,
    deliver,
    drain_subscriptions,
    prometheus_value,
    publish_raw,
    serve_feed,
    settings_override,
)
from transitpulse_core.config import Settings
from transitpulse_core.models import QuarantineReason
from transitpulse_core.snapshots import build_snapshot_archive

NAME = "malformed-feed"
SERVICE_NAME = "transitpulse-chaos-malformed-feed"
QUESTION = "Does every injected bad record turn up as a rejection, with the good ones unharmed?"

log = structlog.get_logger(__name__)

# One corruption per quarantine reason the decoder can give for a vehicle
# entity whose feed is otherwise well-formed, rotated so a run exercises all
# three rather than the first one that happens to work.
_CORRUPTIONS = ("null_island", "bearing_out_of_range", "position_missing")

_EXPECTED_REASONS = {
    "null_island": QuarantineReason.NULL_ISLAND,
    "bearing_out_of_range": QuarantineReason.SCHEMA_VALIDATION,
    "position_missing": QuarantineReason.SCHEMA_VALIDATION,
}

# Bodies no validator could accept, published as a third-party producer would.
# The first is not JSON at all; the rest parse and then breach the contract.
_BAD_PRODUCER_PAYLOADS = (
    b"\x08\x96\x01not-a-ping-at-all",
    b'{"schema_version": 1, "agency": "chaos"}',
    b'{"schema_version": 1, "agency": "chaos", "vehicle_id": "v", "latitude": 91.0,'
    b' "longitude": 0.5, "event_time": "2026-10-20T00:00:00Z",'
    b' "ingest_time": "2026-10-20T00:00:00Z"}',
)


@dataclass(frozen=True)
class Injection:
    """A corrupted feed body and the exact damage done to it."""

    payload: bytes
    vehicle_entities: int
    corrupted: int
    by_corruption: dict[str, int]


def _entity_vehicle_id(entity: gtfs.FeedEntity) -> str:
    """The id the decoder will settle on for this entity.

    Mirrors ``_normalise_entity``'s fallback deliberately rather than importing
    it: this needs the identifier for an entity it is about to break, and the
    decoder's function returns a ping or a rejection, not an id.
    """
    return entity.vehicle.vehicle.id.strip() or entity.id.strip()


def _corrupt(entity: gtfs.FeedEntity, kind: str) -> None:
    if kind == "null_island":
        entity.vehicle.position.latitude = 0.0
        entity.vehicle.position.longitude = 0.0
    elif kind == "bearing_out_of_range":
        entity.vehicle.position.bearing = 400.0
    elif kind == "position_missing":
        entity.vehicle.ClearField("position")
    else:  # pragma: no cover - the tuple above is the only caller
        raise ValueError(f"unknown corruption {kind!r}")


def inject(payload: bytes, *, valid_ids: set[str], every: int, now: datetime) -> Injection:
    """Shift the feed onto the present, then break every n-th valid entity.

    ``SerializePartialToString`` on the way out, because GTFS-RT marks
    ``Position.latitude`` and ``.longitude`` as proto2 ``required`` and the
    ``position_missing`` corruption is precisely a message that omits them. A
    strict serialiser raises on exactly the payload this function exists to
    produce.
    """
    message = gtfs.FeedMessage()
    message.ParseFromString(payload)

    # One offset for the whole feed, so the skew between entities -- which is
    # real, and is what makes the corpus worth replaying -- is preserved.
    offset = int(now.timestamp()) - (message.header.timestamp or int(now.timestamp()))
    if message.header.timestamp:
        message.header.timestamp += offset

    vehicle_entities = 0
    corrupted = 0
    by_corruption: dict[str, int] = {}

    for entity in message.entity:
        if not entity.HasField("vehicle"):
            continue
        vehicle_entities += 1
        if entity.vehicle.HasField("timestamp") and entity.vehicle.timestamp:
            entity.vehicle.timestamp += offset

        if _entity_vehicle_id(entity) not in valid_ids:
            continue
        if vehicle_entities % every:
            continue

        kind = _CORRUPTIONS[corrupted % len(_CORRUPTIONS)]
        _corrupt(entity, kind)
        corrupted += 1
        by_corruption[kind] = by_corruption.get(kind, 0) + 1

    return Injection(
        payload=message.SerializePartialToString(),
        vehicle_entities=vehicle_entities,
        corrupted=corrupted,
        by_corruption=by_corruption,
    )


def _collect_once(url: str, agency: str) -> CollectResult:
    """One scheduler tick against the served feed, through the real handler.

    The response is parsed back into the collector's own ``CollectResult``
    rather than read as loose JSON, so a field this scenario depends on cannot
    be renamed out from under it without the parse failing.

    The override wraps the lifespan, not the import: the handler reads settings
    at request time, so redirecting the feed before the lifespan builds the
    client is what matters.
    """

    async def tick() -> CollectResult:
        with settings_override(TP_FEED_URL=url, TP_FEED_AGENCY=agency):
            async with collector_app.router.lifespan_context(collector_app):
                transport = httpx.ASGITransport(app=collector_app)
                async with httpx.AsyncClient(
                    transport=transport, base_url="http://collector.local"
                ) as client:
                    response = await client.post("/collect", timeout=120.0)
                    response.raise_for_status()
                    return CollectResult.model_validate(response.json())

    return asyncio.run(tick())


def _sidecar_records(settings: Settings, snapshot_path: str) -> list[dict[str, object]]:
    """The rejections the collector wrote beside the snapshot it decoded."""
    archive = build_snapshot_archive(settings)
    root = getattr(archive, "root", None)
    if root is None:  # pragma: no cover - GCS archive, not reachable locally
        return []
    sidecar = root / (snapshot_path.removesuffix(".pb") + ".quarantine.json")
    if not sidecar.exists():
        return []
    records: list[dict[str, object]] = json.loads(sidecar.read_text(encoding="utf-8"))
    return records


def _snapshot_payloads(settings: Settings, *, limit: int) -> Iterator[bytes]:
    archive = build_snapshot_archive(settings)
    for path in archive.list_payloads(agency=settings.feed_agency, limit=limit):
        yield archive.load_payload(path)


def run(settings: Settings, args: argparse.Namespace) -> ScenarioResult:
    result = ScenarioResult(scenario=NAME, question=QUESTION)
    agency = chaos_agency("malformed")
    started_at = datetime.now(UTC)
    drain_subscriptions(settings)

    quarantined_before = prometheus_value("sum(transitpulse_events_quarantined_total)") or 0.0

    # --- baseline: what this snapshot yields untouched ---------------------
    payload = next(_snapshot_payloads(settings, limit=1), None)
    if payload is None:
        result.check(
            equals("the replay corpus holds a snapshot to corrupt", True, False),
        )
        result.note("Run `python tasks.py capture` first; there is nothing to inject into.")
        return result

    now = datetime.now(UTC)
    baseline = decode_feed(payload, agency=agency, ingest_time=now)
    valid_ids = {ping.vehicle_id for ping in baseline.pings}

    injected = inject(payload, valid_ids=valid_ids, every=args.corrupt_every, now=now)
    result.measure(
        agency=agency,
        snapshot_bytes=len(payload),
        vehicle_entities=injected.vehicle_entities,
        baseline_valid=len(baseline.pings),
        baseline_quarantined=len(baseline.quarantined),
        corrupt_every=args.corrupt_every,
        injected=injected.corrupted,
        injected_by_corruption=injected.by_corruption,
    )
    if not injected.corrupted:
        result.check(equals("the injection corrupted something", True, False))
        result.note("--corrupt-every is larger than the number of valid entities in a snapshot.")
        return result

    # --- the feed path: real collector, corrupted upstream ----------------
    with database(settings) as conn:
        delete_agency_rows(conn, agency)

    with serve_feed(body=injected.payload) as served:
        tick = _collect_once(served.url, agency)
    published = tick.published
    quarantined = tick.quarantined

    sidecar = _sidecar_records(settings, tick.snapshot_path)
    sidecar_reasons: dict[str, int] = {}
    for record in sidecar:
        reason = str(record.get("reason"))
        sidecar_reasons[reason] = sidecar_reasons.get(reason, 0) + 1

    result.measure(
        collector_published=published,
        collector_quarantined=quarantined,
        collector_entity_count=tick.entity_count,
        collector_vehicle_entity_count=tick.vehicle_entity_count,
        collector_fetch_seconds=round(tick.fetch_seconds, 4),
        sidecar_records=len(sidecar),
        sidecar_reasons=sidecar_reasons,
        snapshot_path=tick.snapshot_path,
    )
    result.check(
        equals(
            "the quarantine count rose by exactly the injected count",
            len(baseline.quarantined) + injected.corrupted,
            quarantined,
        ),
        equals(
            "nothing is unaccounted for: published + quarantined == vehicle entities",
            injected.vehicle_entities,
            published + quarantined,
        ),
        equals(
            "the valid entities in the same feed were still published",
            len(baseline.pings) - injected.corrupted,
            published,
        ),
        equals(
            "every rejection was persisted beside the payload that caused it",
            quarantined,
            len(sidecar),
        ),
        at_least(
            "the null-island rejections are labelled as such, not as schema failures",
            injected.by_corruption.get("null_island", 0),
            sidecar_reasons.get(str(QuarantineReason.NULL_ISLAND), 0),
        ),
    )

    # --- the broker path: a producer that is not this codebase ------------
    bad = publish_raw(settings, _BAD_PRODUCER_PAYLOADS)
    bridged = deliver(settings, seconds=args.drain_seconds, concurrency=4)
    with database(settings) as conn:
        rows = count_events(conn, agency)
        quarantine_rows = count_quarantine_since(conn, started_at)
        if not args.keep_rows:
            delete_agency_rows(conn, agency)
    if not args.keep_rows:
        result.measure(snapshots_removed=delete_agency_snapshots(settings, agency))

    # Distinct keys, from the corrupted feed the collector actually decoded:
    # two entities in one snapshot could in principle share an identity, and
    # the sink would collapse them rather than lose them.
    expected_rows = len(
        {
            ping.idempotency_key
            for ping in decode_feed(injected.payload, agency=agency, ingest_time=now).pings
        }
    )

    result.measure(
        bad_producer_payloads=bad,
        delivered=bridged.delivered,
        processed=bridged.processed,
        bridge_quarantined=bridged.quarantined,
        rows_in_vehicle_events=rows,
        quarantine_rows_written=quarantine_rows,
        expected_rows=expected_rows,
    )
    result.check(
        equals(
            "every bad payload from the broker reached the quarantine table",
            bad,
            quarantine_rows,
        ),
        equals("the good events from the same run landed as rows", expected_rows, rows),
    )

    # --- and the counter the dashboard reads ------------------------------
    await_metric_export()
    quarantined_after = prometheus_value("sum(transitpulse_events_quarantined_total)")
    delta = None if quarantined_after is None else quarantined_after - quarantined_before
    result.measure(
        quarantined_counter_before=quarantined_before,
        quarantined_counter_after=quarantined_after,
        quarantined_counter_delta=delta,
    )
    if delta is None:
        result.note(
            "Prometheus was not reachable, so the exported counter could not be read; the"
            " row and sidecar counts above are the authoritative ones either way."
        )
    else:
        result.check(
            at_least(
                "the exported quarantine counter rose by at least the injected count",
                injected.corrupted + bad,
                delta,
            )
        )
        result.note(
            "The exported counter is a lower bound, not an equality: it is cumulative"
            " across processes and the drivers here are short-lived, so a count that has"
            " not been exported yet is missing rather than wrong."
        )

    result.note(
        "Only entities the decoder accepted in the baseline were corrupted; breaking one"
        " that was already being rejected would change no count and the experiment would"
        " pass having measured nothing.",
        "The collector writes its rejections to the snapshot archive, not to Postgres --"
        " it deliberately has no database (ADR-006) -- which is why the feed path is"
        " checked against the sidecar and the broker path against the table.",
    )
    return result


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="chaos.malformed_feed",
        description=QUESTION,
        epilog=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--corrupt-every",
        type=int,
        default=5,
        metavar="N",
        help="corrupt every N-th otherwise-valid entity (default: 5)",
    )
    parser.add_argument(
        "--drain-seconds",
        type=float,
        default=180.0,
        metavar="S",
        help="cap on the delivery phase (default: 180)",
    )
    return add_common_arguments(parser)


def main(argv: list[str] | None = None) -> int:
    return scenario_main(run, build_parser(), name=NAME, service_name=SERVICE_NAME, argv=argv)


if __name__ == "__main__":  # pragma: no cover - module entry point
    raise SystemExit(main())

"""Collector service -- fetches the upstream feed and publishes normalised pings.

Invoked by Cloud Scheduler rather than running its own loop, so the schedule is
infrastructure state (visible, auditable, changeable without a deploy) instead
of a sleep buried in application code. Scaling to zero between ticks is also
what keeps the GCP bill near nothing.

The order of work inside a tick is deliberate: **archive, then decode**. The
raw bytes reach the corpus before anything tries to parse them, so the one
payload most worth having -- the one that breaks the decoder -- is not lost
along with the tick that fetched it.

Failures are reported as status codes Cloud Scheduler already understands. A
tick that cannot reach the agency returns 502 and a tick that arrives while the
circuit breaker is open returns 503 with ``Retry-After``; both are retried by
the scheduler, which is the right place for that policy to live.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime

import structlog
from fastapi import FastAPI, HTTPException, Response, status
from opentelemetry.trace import Span, Status, StatusCode
from pydantic import BaseModel
from starlette.concurrency import run_in_threadpool

from services.collector.feed import (
    DecodedFeed,
    FeedClient,
    FeedError,
    FeedFailureKind,
    FeedResponse,
    decode_feed,
)
from services.health import build_health_router
from transitpulse_core.config import get_settings
from transitpulse_core.models import FeedSnapshot, QuarantineReason, QuarantineRecord
from transitpulse_core.pubsub import EventPublisher
from transitpulse_core.resilience import CircuitOpenError
from transitpulse_core.snapshots import SnapshotArchive, build_snapshot_archive
from transitpulse_core.telemetry import (
    current_trace_id,
    get_metrics,
    get_tracer,
    setup_telemetry,
)

SERVICE_NAME = "transitpulse-collector"

log = structlog.get_logger(__name__)

_feed_client: FeedClient | None = None
_publisher: EventPublisher | None = None
_archive: SnapshotArchive | None = None


def get_feed_client() -> FeedClient:
    if _feed_client is None:  # pragma: no cover - set during lifespan
        raise RuntimeError("feed client is not initialised")
    return _feed_client


def get_publisher() -> EventPublisher:
    if _publisher is None:  # pragma: no cover - set during lifespan
        raise RuntimeError("publisher is not initialised")
    return _publisher


def get_archive() -> SnapshotArchive:
    if _archive is None:  # pragma: no cover - set during lifespan
        raise RuntimeError("snapshot archive is not initialised")
    return _archive


class CollectResult(BaseModel):
    """What one scheduler tick did, in the response body.

    Returned rather than merely logged so that a tick can be triggered by hand
    during an incident and the outcome read directly, without correlating a log
    query against the clock.
    """

    agency: str
    fetched_at: datetime
    feed_timestamp: datetime | None
    attempts: int
    payload_bytes: int
    snapshot_path: str
    entity_count: int
    vehicle_entity_count: int
    published: int
    quarantined: int
    fetch_seconds: float
    trace_id: str | None


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    global _feed_client, _publisher, _archive
    setup_telemetry(SERVICE_NAME)

    # Instrumented inside lifespan rather than at import time so the module
    # stays importable in tests with no tracer provider configured.
    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

    FastAPIInstrumentor.instrument_app(app, excluded_urls="healthz,readyz")

    settings = get_settings()
    _feed_client = FeedClient(settings)
    _publisher = EventPublisher(settings)
    _archive = build_snapshot_archive(settings)
    log.info(
        "collector.ready",
        agency=settings.feed_agency,
        snapshot_backend=_archive.name,
        topic=_publisher.topic_path,
    )
    try:
        yield
    finally:
        await _feed_client.aclose()
        _publisher.close()
        _feed_client = None
        _publisher = None
        _archive = None


app = FastAPI(title=SERVICE_NAME, lifespan=lifespan)
app.include_router(build_health_router(SERVICE_NAME))


@app.post("/collect", response_model=CollectResult, tags=["collector"])
async def collect(response: Response) -> CollectResult:
    """Fetch, archive, decode and publish one feed snapshot."""
    tracer = get_tracer()
    # An explicit span for the whole tick, rather than relying on the server
    # span the FastAPI instrumentation contributes. Without one, fetch, decode
    # and publish each become a trace root and the question this project is
    # built to answer -- where did the time go between the agency and the
    # broker -- needs three traces stitched together by eye. It also makes the
    # traceparent that leaves on each message belong to the tick, which is what
    # lets the processor's span join this one.
    with tracer.start_as_current_span("collector.collect") as tick_span:
        return await _collect_tick(response, tick_span)


async def _collect_tick(response: Response, tick_span: Span) -> CollectResult:
    settings = get_settings()
    metrics = get_metrics()
    feed_client = get_feed_client()
    publisher = get_publisher()
    archive = get_archive()
    agency = settings.feed_agency
    tick_span.set_attribute("feed.agency", agency)

    try:
        fetched = await feed_client.fetch()
    except CircuitOpenError as exc:
        metrics.feed_fetch_failures.add(
            1, {"agency": agency, "kind": str(FeedFailureKind.CIRCUIT_OPEN)}
        )
        tick_span.set_status(Status(StatusCode.ERROR, "circuit open"))
        log.warning("collect.circuit_open", agency=agency, retry_after=exc.retry_after_seconds)
        # Retry-After is honoured by Cloud Scheduler, so the cooldown is
        # communicated rather than guessed at by the caller.
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=str(exc),
            headers={"Retry-After": str(max(1, int(exc.retry_after_seconds)))},
        ) from exc
    except FeedError as exc:
        tick_span.set_status(Status(StatusCode.ERROR, str(exc)))
        log.error("collect.fetch_failed", agency=agency, kind=str(exc.kind), error=str(exc))
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"upstream feed unavailable: {exc}",
        ) from exc

    # Before the decoder runs: a body that cannot be parsed is exactly the
    # body worth keeping, and this is the only moment it exists in memory.
    snapshot_path = await run_in_threadpool(
        archive.put_payload, fetched.payload, agency=agency, fetched_at=fetched.fetched_at
    )

    ingest_time = datetime.now(UTC)
    try:
        decoded = decode_feed(fetched.payload, agency=agency, ingest_time=ingest_time)
    except FeedError as exc:
        await _archive_quarantine(
            archive,
            snapshot_path,
            [
                QuarantineRecord(
                    reason=QuarantineReason.DECODE_FAILURE,
                    detail=str(exc),
                    agency=agency,
                    trace_id=current_trace_id(),
                )
            ],
        )
        metrics.events_quarantined.add(
            1, {"agency": agency, "reason": str(QuarantineReason.DECODE_FAILURE)}
        )
        tick_span.set_status(Status(StatusCode.ERROR, str(exc)))
        log.error("collect.decode_failed", agency=agency, snapshot=snapshot_path, error=str(exc))
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"upstream feed is not decodable: {exc}",
        ) from exc

    # publish_batch resolves every future before returning, so it blocks; the
    # threadpool keeps it off the event loop. It also owns the
    # events_published counter, which is why this handler does not touch it.
    published = await run_in_threadpool(publisher.publish_batch, decoded.pings)

    if decoded.quarantined:
        await _archive_quarantine(archive, snapshot_path, decoded.quarantined)

    snapshot = FeedSnapshot(
        agency=agency,
        fetched_at=fetched.fetched_at,
        feed_timestamp=decoded.feed_timestamp,
        entity_count=decoded.entity_count,
        payload_bytes=len(fetched.payload),
        object_path=snapshot_path,
        content_sha256=FeedSnapshot.sha256_of(fetched.payload),
    )
    await run_in_threadpool(archive.put_metadata, snapshot)

    tick_span.set_attribute("feed.published", published)
    tick_span.set_attribute("feed.quarantined", len(decoded.quarantined))
    tick_span.set_attribute("snapshot.path", snapshot_path)

    _log_tick(agency, fetched, decoded, published, snapshot_path)

    # A tick where the feed parsed but nothing survived validation is a
    # successful fetch of a broken feed, not a successful collection. 207
    # keeps it visibly distinct in the scheduler's history without triggering
    # a retry that would fetch the same broken payload again.
    if published == 0 and decoded.quarantined:
        response.status_code = status.HTTP_207_MULTI_STATUS

    return CollectResult(
        agency=agency,
        fetched_at=fetched.fetched_at,
        feed_timestamp=decoded.feed_timestamp,
        attempts=fetched.attempts,
        payload_bytes=len(fetched.payload),
        snapshot_path=snapshot_path,
        entity_count=decoded.entity_count,
        vehicle_entity_count=decoded.vehicle_entity_count,
        published=published,
        quarantined=len(decoded.quarantined),
        fetch_seconds=round(fetched.duration_seconds, 4),
        trace_id=current_trace_id(),
    )


async def _archive_quarantine(
    archive: SnapshotArchive, snapshot_path: str, records: list[QuarantineRecord]
) -> None:
    await run_in_threadpool(archive.put_quarantine, snapshot_path, records)


def _log_tick(
    agency: str,
    fetched: FeedResponse,
    decoded: DecodedFeed,
    published: int,
    snapshot_path: str,
) -> None:
    """One line per tick, with the numbers an operator would ask for first."""
    reasons: dict[str, int] = {}
    for record in decoded.quarantined:
        reasons[str(record.reason)] = reasons.get(str(record.reason), 0) + 1

    log.info(
        "collect.completed",
        agency=agency,
        attempts=fetched.attempts,
        fetch_seconds=round(fetched.duration_seconds, 4),
        payload_bytes=len(fetched.payload),
        entity_count=decoded.entity_count,
        vehicle_entity_count=decoded.vehicle_entity_count,
        published=published,
        quarantined=len(decoded.quarantined),
        quarantine_reasons=reasons or None,
        snapshot=snapshot_path,
    )

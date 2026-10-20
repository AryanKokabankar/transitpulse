"""Processor service -- validates, enriches and dual-writes each event.

Runs as a Pub/Sub **push** target: Cloud Run autoscales on inbound HTTP
concurrency, so push lets the broker drive scaling directly instead of needing
a pull loop whose worker count has to be tuned by hand.

**The status code is the acknowledgement.** Pub/Sub acks a pushed message on a
2xx and redelivers on anything else, so every ``return`` in this module is a
durability decision rather than a formality. Getting it backwards is silent in
both directions: a 200 on a transient database error drops the event for good,
and a 5xx on a permanently malformed one burns five deliveries and a
dead-letter slot discovering that it is still malformed. The rule applied
throughout is that **a failure the next attempt could fix returns 503, and one
it could not returns 200 with the event in the quarantine table**:

===========================================  ==========================  ======
 situation                                    action                      HTTP
===========================================  ==========================  ======
 valid event, written                         ack                          200
 body is not a Pub/Sub push envelope          quarantine DECODE_FAILURE    200
 payload fails the data contract              quarantine SCHEMA_VALIDATION 200
 position implies an impossible speed         quarantine IMPOSSIBLE_SPEED  200
 sink or lookup failed, attempts remain       nack, let Pub/Sub retry      503
 sink failed on the final delivery attempt    quarantine SINK_WRITE_FAILURE 200
 the quarantine write itself failed           nack; the DLQ is the backstop 503
===========================================  ==========================  ======

The last two rows are the poison-message handling. The attempt count is
checked on the *failure* path rather than at the top of the handler on
purpose: a message on its fifth delivery is usually a good event whose first
four attempts hit a database that was down, and discarding it unread would
turn a recovered outage into permanent data loss. Only once this attempt has
also failed does giving up become the better option -- at which point the
quarantine row, with its raw payload and trace id, is a more useful artefact
than a dead-lettered message. If even that write fails the handler nacks, and
the dead-letter policy catches what the quarantine table could not.

The first thing the handler does with a message is start its span from the
``traceparent`` the collector left in the message attributes. That single line
is what makes one trace span ``fetch -> decode -> publish -> consume -> enrich
-> Postgres``; without it the consumer's work becomes an unrelated trace root
and the question this project exists to answer stops being answerable.
"""

from __future__ import annotations

import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Final

import structlog
from fastapi import FastAPI, HTTPException, Request, status
from opentelemetry.trace import Span, SpanKind, Status, StatusCode
from pydantic import BaseModel, ValidationError
from starlette.concurrency import run_in_threadpool

from services.health import build_health_router
from services.processor.enrich import Enricher, ImplausibleSpeedError
from transitpulse_core.config import get_settings
from transitpulse_core.models import (
    EnrichedVehicleEvent,
    QuarantineReason,
    QuarantineRecord,
    VehiclePing,
)
from transitpulse_core.pubsub import PushEnvelope, decode_push_body
from transitpulse_core.sinks.bigquery import BigQuerySink
from transitpulse_core.sinks.postgres import PostgresSink
from transitpulse_core.telemetry import (
    current_trace_id,
    extract_trace_context,
    get_metrics,
    get_tracer,
    setup_telemetry,
)

SERVICE_NAME = "transitpulse-processor"

# One push delivery is one ping, so a body materially larger than this is not
# a ping at all. The cap exists because the quarantine column lives in the
# serving database and an attacker-controlled -- or merely misrouted -- body
# can be megabytes; keeping enough to identify what arrived is the point, not
# keeping all of it.
MAX_QUARANTINE_PAYLOAD_BYTES: Final = 16 * 1024

log = structlog.get_logger(__name__)

_sink: PostgresSink | None = None
_bigquery: BigQuerySink | None = None
_enricher: Enricher | None = None


def get_sink() -> PostgresSink:
    if _sink is None:  # pragma: no cover - set during lifespan
        raise RuntimeError("database pool is not initialised")
    return _sink


def get_enricher() -> Enricher:
    if _enricher is None:  # pragma: no cover - set during lifespan
        raise RuntimeError("enricher is not initialised")
    return _enricher


def get_bigquery() -> BigQuerySink | None:
    """The history sink, or ``None`` when BigQuery is disabled.

    Local development has no BigQuery emulator, so ``TP_BQ_ENABLED`` defaults
    to false and the pipeline writes to Postgres only (ADR-002).
    """
    return _bigquery


class ProcessResult(BaseModel):
    """What the handler did with one message.

    Returned in the body rather than only logged so that the handler can be
    driven by hand with ``curl`` during an incident and the decision read
    directly -- a 200 alone cannot distinguish "written" from "quarantined",
    and those are opposite operational stories.
    """

    outcome: str
    reason: str | None = None
    message_id: str | None = None
    delivery_attempt: int = 1
    trace_id: str | None = None
    lag_seconds: float | None = None


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    global _sink, _bigquery, _enricher
    setup_telemetry(SERVICE_NAME)

    # Instrumented inside lifespan rather than at import time so the module
    # stays importable in tests with no tracer provider configured.
    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

    FastAPIInstrumentor.instrument_app(app, excluded_urls="healthz,readyz")

    settings = get_settings()
    _sink = PostgresSink(settings)
    _bigquery = BigQuerySink(settings) if settings.bq_enabled else None
    _enricher = Enricher(_sink, settings)
    log.info(
        "processor.ready",
        bigquery_enabled=settings.bq_enabled,
        agency_timezone=settings.agency_timezone,
        max_delivery_attempts=settings.max_delivery_attempts,
    )
    try:
        yield
    finally:
        # The BigQuery sink buffers; closing it without flushing would lose
        # every row accepted since the last append.
        if _bigquery is not None:
            _bigquery.close()
        _sink.close()
        _sink = None
        _bigquery = None
        _enricher = None


app = FastAPI(title=SERVICE_NAME, lifespan=lifespan)
app.include_router(build_health_router(SERVICE_NAME, readiness=lambda: get_sink().healthcheck()))


@app.post("/", response_model=ProcessResult, tags=["processor"])
async def handle_push(request: Request) -> ProcessResult:
    """Consume one pushed message. The path is ``/`` because that is where a
    Pub/Sub push subscription delivers."""
    started = time.perf_counter()
    body = await request.body()

    try:
        envelope = decode_push_body(body)
    except Exception as exc:  # noqa: BLE001 - any parse failure is the same verdict
        # No envelope means no message id, no delivery attempt and no trace
        # context to join. Retrying cannot change the shape of a request, so
        # this is quarantined and acked on the first attempt.
        log.error("processor.undecodable_body", error=str(exc), bytes=len(body))
        await _quarantine(
            reason=QuarantineReason.DECODE_FAILURE,
            detail=f"push body is not a Pub/Sub envelope: {exc}",
            payload=body,
        )
        return ProcessResult(
            outcome="quarantined",
            reason=str(QuarantineReason.DECODE_FAILURE),
            trace_id=current_trace_id(),
        )

    # The line this phase exists for. Passing the extracted context explicitly
    # overrides the ambient server span the FastAPI instrumentation created,
    # which belongs to a trace of its own; this span instead becomes a child of
    # the collector's publish span, in the collector's trace.
    context = extract_trace_context(envelope.message.attributes)
    with get_tracer().start_as_current_span(
        "processor.handle",
        context=context,
        kind=SpanKind.CONSUMER,
        attributes={
            "messaging.system": "gcp_pubsub",
            "messaging.operation": "process",
            "messaging.message.id": envelope.message.message_id,
            "messaging.gcp_pubsub.message.delivery_attempt": envelope.delivery_attempt,
        },
    ) as span:
        return await _process(envelope, span, started)


async def _process(envelope: PushEnvelope, span: Span, started: float) -> ProcessResult:
    metrics = get_metrics()
    payload = envelope.message.decoded()

    try:
        ping = VehiclePing.model_validate_json(payload)
    except ValidationError as exc:
        return await _reject(
            envelope,
            span,
            payload,
            reason=QuarantineReason.SCHEMA_VALIDATION,
            detail=f"payload does not satisfy the contract: {exc}",
        )

    span.set_attribute("transitpulse.agency", ping.agency)
    span.set_attribute("transitpulse.vehicle_id", ping.vehicle_id)

    try:
        event = await run_in_threadpool(get_enricher().enrich, ping)
    except ImplausibleSpeedError as exc:
        return await _reject(
            envelope,
            span,
            payload,
            reason=QuarantineReason.IMPOSSIBLE_SPEED,
            detail=str(exc),
            ping=ping,
        )
    except Exception as exc:  # noqa: BLE001 - enrichment reads Postgres; treat as transient
        return await _retry_or_give_up(
            envelope, span, payload, detail=f"enrichment failed: {exc}", ping=ping
        )

    written = await _write(event)
    if written is not None:
        return await _retry_or_give_up(envelope, span, payload, detail=written, ping=ping)

    lag = event.end_to_end_lag_seconds
    metrics.ingest_lag.record(lag, {"agency": ping.agency})
    metrics.processing_duration.record(
        time.perf_counter() - started, {"service": "processor", "outcome": "ok"}
    )
    metrics.events_processed.add(1, {"agency": ping.agency, "outcome": "ok"})
    span.set_attribute("transitpulse.lag_seconds", lag)

    log.info(
        "processor.handled",
        agency=ping.agency,
        vehicle_id=ping.vehicle_id,
        message_id=envelope.message.message_id,
        delivery_attempt=envelope.delivery_attempt,
        lag_seconds=round(lag, 4),
        schedule_delay_seconds=event.schedule_delay_seconds,
        computed_speed_mps=event.computed_speed_mps,
    )
    return ProcessResult(
        outcome="processed",
        message_id=envelope.message.message_id,
        delivery_attempt=envelope.delivery_attempt,
        trace_id=current_trace_id(),
        lag_seconds=round(lag, 4),
    )


async def _write(event: EnrichedVehicleEvent) -> str | None:
    """Dual-write one event. Returns an error description, or ``None`` on success.

    One event per call, because a push subscription delivers exactly one
    message per HTTP request: there is no batch here to amortise. Accumulating
    events across requests to build one would mean holding acknowledged work
    in memory that no later request is guaranteed to arrive and flush, and
    Cloud Run scales instances down without warning -- the buffer would go with
    the instance. The BigQuery sink already buffers internally and flushes on
    its own timeout and on shutdown, which is where that batching belongs.
    """
    postgres = await run_in_threadpool(get_sink().write_batch, [event])
    if not postgres.ok:
        return f"postgres write failed: {'; '.join(postgres.errors)}"

    bigquery = get_bigquery()
    if bigquery is None:
        return None

    # A BigQuery failure after a successful Postgres write is retried whole.
    # Safe because both writes are idempotent on the same key (ADR-005): the
    # redelivery's Postgres write collapses to a no-op and only the missing
    # BigQuery row is actually added.
    history = await run_in_threadpool(bigquery.write_batch, [event])
    if not history.ok:
        return f"bigquery write failed: {'; '.join(history.errors)}"
    return None


async def _reject(
    envelope: PushEnvelope,
    span: Span,
    payload: bytes,
    *,
    reason: QuarantineReason,
    detail: str,
    ping: VehiclePing | None = None,
) -> ProcessResult:
    """Quarantine a message no retry could fix, and ack it."""
    span.set_status(Status(StatusCode.ERROR, str(reason)))
    span.set_attribute("transitpulse.quarantine_reason", str(reason))
    await _quarantine(reason=reason, detail=detail, payload=payload, envelope=envelope, ping=ping)
    log.warning(
        "processor.quarantined",
        reason=str(reason),
        detail=detail,
        message_id=envelope.message.message_id,
        delivery_attempt=envelope.delivery_attempt,
    )
    return ProcessResult(
        outcome="quarantined",
        reason=str(reason),
        message_id=envelope.message.message_id,
        delivery_attempt=envelope.delivery_attempt,
        trace_id=current_trace_id(),
    )


async def _retry_or_give_up(
    envelope: PushEnvelope,
    span: Span,
    payload: bytes,
    *,
    detail: str,
    ping: VehiclePing | None = None,
) -> ProcessResult:
    """Nack a transient failure, unless this was the last delivery attempt."""
    span.set_status(Status(StatusCode.ERROR, detail))
    attempts_left = envelope.delivery_attempt < get_settings().max_delivery_attempts
    if attempts_left:
        log.warning(
            "processor.retrying",
            detail=detail,
            message_id=envelope.message.message_id,
            delivery_attempt=envelope.delivery_attempt,
        )
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=detail)

    return await _reject(
        envelope,
        span,
        payload,
        reason=QuarantineReason.SINK_WRITE_FAILURE,
        detail=f"giving up after {envelope.delivery_attempt} delivery attempts: {detail}",
        ping=ping,
    )


async def _quarantine(
    *,
    reason: QuarantineReason,
    detail: str,
    payload: bytes,
    envelope: PushEnvelope | None = None,
    ping: VehiclePing | None = None,
) -> None:
    """Persist a rejection, or refuse to acknowledge the message.

    Raising on a failed quarantine write is deliberate: acking a message whose
    rejection was never recorded loses it entirely and leaves the pipeline
    looking healthy, which is the one outcome this table exists to prevent.
    The 503 sends it back to Pub/Sub, and the dead-letter policy holds it until
    the database is back.
    """
    record = QuarantineRecord(
        reason=reason,
        detail=detail[:4096],
        agency=ping.agency if ping else None,
        vehicle_id=ping.vehicle_id if ping else None,
        raw_payload=payload[:MAX_QUARANTINE_PAYLOAD_BYTES] or None,
        message_id=envelope.message.message_id if envelope else None,
        delivery_attempt=envelope.delivery_attempt if envelope else None,
        trace_id=current_trace_id(),
    )
    try:
        await run_in_threadpool(get_sink().write_quarantine, [record])
    except Exception as exc:
        log.error("processor.quarantine_write_failed", reason=str(reason), error=str(exc))
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"could not record quarantine for a {reason} rejection: {exc}",
        ) from exc

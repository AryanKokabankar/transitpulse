"""OpenTelemetry bootstrap, metric registry, and cross-broker trace propagation.

One ``setup_telemetry()`` call per service process wires up traces, metrics and
trace-correlated structured logs.

The part worth reading is :func:`inject_trace_context` /
:func:`extract_trace_context`. Pub/Sub is a process boundary that ordinary
auto-instrumentation cannot see across: the publisher's span ends when the
message is accepted and the subscriber's span starts fresh, leaving two
unrelated traces. Carrying the W3C ``traceparent`` in the message *attributes*
stitches them into one trace, so a single waterfall covers
fetch -> decode -> publish -> consume -> enrich -> Postgres -> BigQuery.
"""

from __future__ import annotations

import logging
import sys
from collections.abc import MutableMapping
from dataclasses import dataclass
from functools import lru_cache
from typing import Any

import structlog
from opentelemetry import metrics, trace
from opentelemetry.context import Context
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.metrics import Counter, Histogram, UpDownCounter
from opentelemetry.propagate import extract, inject
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import (
    ConsoleMetricExporter,
    PeriodicExportingMetricReader,
)
from opentelemetry.sdk.metrics.view import ExplicitBucketHistogramAggregation, View
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter

from transitpulse_core.config import get_settings

# Lag is the SLI, so it gets buckets chosen around the 2s SLO target rather
# than the SDK defaults, which bunch everything above 10s into one bucket.
_LAG_BUCKETS = [0.1, 0.25, 0.5, 1.0, 1.5, 2.0, 3.0, 5.0, 10.0, 30.0, 60.0, 300.0]
_DURATION_BUCKETS = [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0]

_INSTRUMENTATION_NAME = "transitpulse"


@dataclass(frozen=True)
class Metrics:
    """Every metric the pipeline emits, in one place.

    Named so that each one answers an operational question:
    "is it keeping up", "is it losing data", "where is the time going".
    """

    ingest_lag: Histogram
    processing_duration: Histogram
    feed_fetch_duration: Histogram
    decode_duration: Histogram
    sink_write_duration: Histogram
    events_processed: Counter
    events_quarantined: Counter
    events_deduped: Counter
    events_published: Counter
    feed_fetch_failures: Counter
    dlq_depth: UpDownCounter
    inflight_batches: UpDownCounter


@lru_cache(maxsize=1)
def get_metrics() -> Metrics:
    """Build the instrument registry once per process."""
    meter = metrics.get_meter(_INSTRUMENTATION_NAME)
    return Metrics(
        ingest_lag=meter.create_histogram(
            "transitpulse.ingest.lag",
            unit="s",
            description="processed_time - event_time; the pipeline SLI",
        ),
        processing_duration=meter.create_histogram(
            "transitpulse.processing.duration",
            unit="s",
            description="Time spent inside the processor handler",
        ),
        feed_fetch_duration=meter.create_histogram(
            "transitpulse.feed.fetch.duration",
            unit="s",
            description="Upstream GTFS-RT HTTP fetch latency",
        ),
        decode_duration=meter.create_histogram(
            "transitpulse.feed.decode.duration",
            unit="s",
            description="Protobuf decode and normalisation time per snapshot",
        ),
        sink_write_duration=meter.create_histogram(
            "transitpulse.sink.write.duration",
            unit="s",
            description="Write latency per sink, labelled by sink name",
        ),
        events_processed=meter.create_counter(
            "transitpulse.events.processed",
            description="Events successfully written to all enabled sinks",
        ),
        events_quarantined=meter.create_counter(
            "transitpulse.events.quarantined",
            description="Events rejected, labelled by reason",
        ),
        events_deduped=meter.create_counter(
            "transitpulse.events.deduped",
            description="Events suppressed by the idempotency key",
        ),
        events_published=meter.create_counter(
            "transitpulse.events.published",
            description="Events published to Pub/Sub by the collector",
        ),
        feed_fetch_failures=meter.create_counter(
            "transitpulse.feed.fetch.failures",
            description="Upstream feed errors, labelled by kind",
        ),
        dlq_depth=meter.create_up_down_counter(
            "transitpulse.dlq.depth",
            description="Messages currently parked in the dead-letter queue",
        ),
        inflight_batches=meter.create_up_down_counter(
            "transitpulse.sink.inflight_batches",
            description="Sink batches submitted but not yet acknowledged",
        ),
    )


def get_tracer() -> trace.Tracer:
    return trace.get_tracer(_INSTRUMENTATION_NAME)


# ----------------------------------------------------------------------
# Cross-broker propagation
# ----------------------------------------------------------------------
def inject_trace_context(attributes: dict[str, str] | None = None) -> dict[str, str]:
    """Add the current span's W3C ``traceparent`` to Pub/Sub message attributes.

    Pub/Sub attributes are a ``str -> str`` map, which is exactly the TextMap
    carrier shape OpenTelemetry's propagators expect, so no adapter is needed.
    Call this while the publishing span is still current.
    """
    carrier: dict[str, str] = dict(attributes or {})
    inject(carrier)
    return carrier


def extract_trace_context(attributes: dict[str, str] | None) -> Context:
    """Rebuild the publisher's context on the consuming side.

    Pass the result as ``context=`` when starting the consumer span; the new
    span then becomes a child of the publisher's span instead of a trace root.
    """
    return extract(dict(attributes or {}))


def current_trace_id() -> str | None:
    """Hex trace id of the active span, for stamping onto database rows.

    Writing this into the quarantine table is what turns "row 4,812 is bad"
    into a clickable trace showing exactly which feed fetch produced it.
    """
    ctx = trace.get_current_span().get_span_context()
    if not ctx.is_valid:
        return None
    return format(ctx.trace_id, "032x")


# ----------------------------------------------------------------------
# Logging
# ----------------------------------------------------------------------
def _inject_trace_into_log(
    _logger: Any, _name: str, event_dict: MutableMapping[str, Any]
) -> MutableMapping[str, Any]:
    """structlog processor correlating every log line with its trace."""
    ctx = trace.get_current_span().get_span_context()
    if ctx.is_valid:
        event_dict["trace_id"] = format(ctx.trace_id, "032x")
        event_dict["span_id"] = format(ctx.span_id, "016x")
    return event_dict


def _configure_logging(service_name: str, log_level: str, json_logs: bool) -> None:
    renderer: Any = (
        structlog.processors.JSONRenderer()
        if json_logs
        else structlog.dev.ConsoleRenderer(colors=False)
    )
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            _inject_trace_into_log,
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            renderer,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging.getLevelName(log_level)),
        logger_factory=structlog.PrintLoggerFactory(file=sys.stdout),
        cache_logger_on_first_use=True,
    )
    structlog.contextvars.bind_contextvars(service=service_name)


# ----------------------------------------------------------------------
# Bootstrap
# ----------------------------------------------------------------------
def setup_telemetry(service_name: str, *, instrument_libraries: bool = True) -> None:
    """Configure traces, metrics and logs. Call once, at process start.

    Exporters are OTLP/gRPC, which both the local Collector and Cloud Trace
    accept -- the same binary runs in both places with only an endpoint
    environment variable changing.
    """
    settings = get_settings()
    _configure_logging(
        service_name,
        settings.log_level,
        json_logs=not settings.is_local,
    )

    if not settings.otel_enabled:
        structlog.get_logger().info("telemetry.disabled", service=service_name)
        return

    resource = Resource.create(
        {
            "service.name": service_name,
            "service.version": "0.1.0",
            "service.namespace": "transitpulse",
            "deployment.environment": settings.env,
        }
    )

    tracer_provider = TracerProvider(resource=resource)
    tracer_provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
    if settings.otel_console_export:
        tracer_provider.add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))
    trace.set_tracer_provider(tracer_provider)

    readers: list[PeriodicExportingMetricReader] = [
        PeriodicExportingMetricReader(OTLPMetricExporter())
    ]
    if settings.otel_console_export:
        readers.append(PeriodicExportingMetricReader(ConsoleMetricExporter()))
    metrics.set_meter_provider(
        MeterProvider(
            resource=resource,
            metric_readers=readers,
            views=[
                View(
                    instrument_name="transitpulse.ingest.lag",
                    aggregation=ExplicitBucketHistogramAggregation(_LAG_BUCKETS),
                ),
                View(
                    instrument_name="transitpulse.*.duration",
                    aggregation=ExplicitBucketHistogramAggregation(_DURATION_BUCKETS),
                ),
            ],
        )
    )

    if instrument_libraries:
        _instrument_libraries()

    structlog.get_logger().info("telemetry.ready", service=service_name, env=settings.env)


def _instrument_libraries() -> None:
    """Auto-instrument the clients we use, tolerating absent extras.

    Each instrumentor is optional at runtime: the collector has no database and
    the jobs have no HTTP server, so a missing package is not an error.
    """
    try:
        from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor

        HTTPXClientInstrumentor().instrument()
    except ImportError:  # pragma: no cover - optional extra
        pass

    try:
        from opentelemetry.instrumentation.psycopg import PsycopgInstrumentor

        PsycopgInstrumentor().instrument(enable_commenter=True)
    except ImportError:  # pragma: no cover - optional extra
        pass

    try:
        from opentelemetry.instrumentation.logging import LoggingInstrumentor

        LoggingInstrumentor().instrument(set_logging_format=False)
    except ImportError:  # pragma: no cover - optional extra
        pass

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

Instrument names here are *not* the names queries are written against. The
Collector's Prometheus exporter rewrites every one of them: dots become
underscores, a monotonic counter gains ``_total``, and the ``unit`` gains a
suffix of its own -- ``s`` becomes ``_seconds`` and ``d`` becomes ``_days``. So
``transitpulse.ingest.lag`` with ``unit="s"`` is queried as
``transitpulse_ingest_lag_seconds_bucket``, and changing a unit here silently
renames a series and empties a dashboard panel. The rule is written down in
``tests/unit/test_dashboard_contract.py``, which derives every name this
registry can produce and fails if a panel queries anything else, and checked
against a live Collector by ``tests/integration/test_dashboard_panels.py``.
"""

from __future__ import annotations

import logging
import time
from collections.abc import MutableMapping
from dataclasses import dataclass
from functools import lru_cache
from typing import Any

import structlog
from opentelemetry import metrics, trace
from opentelemetry.context import Context
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.metrics import (
    CallbackOptions,
    Counter,
    Histogram,
    Meter,
    ObservableGauge,
    Observation,
    UpDownCounter,
)
from opentelemetry.metrics import (
    # The synchronous gauge is still marked experimental in the OTel Python
    # API, so its only exported name is underscore-prefixed. Aliased here
    # rather than spelled out at each annotation: the instrument is stable in
    # the metrics data model, and this is the one line to revisit when the API
    # promotes it.
    _Gauge as Gauge,
)
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

# Read once at import, so every observation of it reports the same instant
# whatever the clock does afterwards.
_PROCESS_START = time.time()


def _observe_start_time(_options: CallbackOptions) -> list[Observation]:
    """Re-report this process's start time on every metric collection.

    An *observable* gauge rather than a value set once at bootstrap, because a
    synchronous gauge is only exported in the collection cycle that follows the
    ``set()``. Nothing sets this one again, so after that single export the
    series would stop being sent, the Collector would expire it, and the
    "something restarted" annotation would quietly have no data to draw from --
    the exact failure mode this phase exists to remove.
    """
    return [Observation(_PROCESS_START)]


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
    events_dead_lettered: Counter
    events_replayed: Counter
    feed_fetch_failures: Counter
    inflight_batches: UpDownCounter
    partitions_ensured: Counter
    partitions_dropped: Counter
    partition_headroom: Gauge
    job_last_success: Gauge
    # Callback-driven, so nothing calls it -- held here only to keep it alive.
    # An ObservableGauge dropped by the garbage collector stops being observed.
    service_start: ObservableGauge


@lru_cache(maxsize=1)
def get_metrics() -> Metrics:
    """The process-wide instrument registry, built once."""
    return build_metrics(metrics.get_meter(_INSTRUMENTATION_NAME))


def build_metrics(meter: Meter) -> Metrics:
    """Create every instrument against a given meter.

    Takes the meter rather than reaching for the global provider so that the
    registry can be built against an in-memory reader and inspected. The names
    and units here are the sole input to the series names every dashboard panel
    and alert rule is written against, and the global provider can only be set
    once per process -- which would leave the one thing most worth checking
    uncheckable without a subprocess.
    """
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
        events_dead_lettered=meter.create_counter(
            "transitpulse.events.dead_lettered",
            description="Events whose last delivery attempt was refused, labelled by reason",
        ),
        events_replayed=meter.create_counter(
            "transitpulse.events.replayed",
            description="Dead-lettered events the replayer put back on the topic",
        ),
        feed_fetch_failures=meter.create_counter(
            "transitpulse.feed.fetch.failures",
            description="Upstream feed errors, labelled by kind",
        ),
        inflight_batches=meter.create_up_down_counter(
            "transitpulse.sink.inflight_batches",
            description="Sink batches submitted but not yet acknowledged",
        ),
        partitions_ensured=meter.create_counter(
            "transitpulse.partitions.ensured",
            description="Daily partitions created by the maintenance job",
        ),
        partitions_dropped=meter.create_counter(
            "transitpulse.partitions.dropped",
            description="Daily partitions dropped past the retention window",
        ),
        partition_headroom=meter.create_gauge(
            "transitpulse.partitions.headroom",
            unit="d",
            description="Unbroken days of partitions from today onward; write capacity",
        ),
        job_last_success=meter.create_gauge(
            "transitpulse.job.last_success_timestamp",
            unit="s",
            description="Unix time a scheduled job last finished successfully",
        ),
        service_start=meter.create_observable_gauge(
            "transitpulse.service.start_timestamp",
            unit="s",
            description="Unix time this process started",
            callbacks=[_observe_start_time],
        ),
    )


def record_job_success(job_name: str) -> None:
    """Stamp a scheduled job's completion time, for freshness alerting.

    The label is ``job_name`` and not ``job``: the Collector's Prometheus
    exporter already writes a ``job`` label (the scrape job), and a colliding
    attribute would be silently renamed to ``exported_job``, so every query
    written against ``job`` would match nothing.

    What this guards against is a job that stops running rather than one that
    fails -- a crashing job is loud, a job whose schedule was never wired up is
    not, and a partition job that silently stopped is how this pipeline came to
    refuse every insert mid-development.
    """
    get_metrics().job_last_success.set(time.time(), {"job_name": job_name})


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


def _configure_logging(
    service_name: str, log_level: str, *, json_logs: bool, is_local: bool
) -> None:
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
        # No explicit file= here, deliberately. structlog's PrintLogger resolves
        # sys.stdout at write time only while it holds the exact stdout object
        # structlog imported; handing it a replaced stream pins that stream
        # forever instead. Under pytest, sys.stdout is a per-test capture buffer
        # that gets closed at teardown, so a pinned one makes every later log
        # call raise "I/O operation on closed file" -- in a different test file
        # from the one that caused it.
        logger_factory=structlog.PrintLoggerFactory(),
        # Caching binds one logger (and its stream) for the process lifetime,
        # which is a worthwhile saving only where that stream is stable. It is
        # in the deployed services; it is not locally, under pytest or in a
        # notebook, so locally a logger is built per call.
        cache_logger_on_first_use=not is_local,
    )
    structlog.contextvars.bind_contextvars(service=service_name)


# ----------------------------------------------------------------------
# Bootstrap
# ----------------------------------------------------------------------
def setup_telemetry(
    service_name: str,
    *,
    instrument_libraries: bool = True,
    instance_id: str | None = None,
) -> None:
    """Configure traces, metrics and logs. Call once, at process start.

    Exporters are OTLP/gRPC, which both the local Collector and Cloud Trace
    accept -- the same binary runs in both places with only an endpoint
    environment variable changing.

    ``instance_id`` pins ``service.instance.id`` instead of letting the SDK
    generate a fresh one per process. Pass it from **singleton batch jobs that
    report a gauge describing shared state**, and nowhere else.

    The reason is specific. The Collector's Prometheus exporter promotes
    resource attributes to labels, so an auto-generated instance id makes every
    invocation its own time series. For a counter that is harmless. For a gauge
    of a global fact -- "how many days of partitions exist" -- it is not: the
    exporter keeps re-publishing each dead run's last value until it expires,
    so Prometheus sees one truth alongside several stale readings, all carrying
    the current scrape timestamp. No timestamp arithmetic can tell them apart,
    and any aggregation across them is wrong in one direction or the other
    (``min`` pages on an old low reading, ``max`` hides a real one). A stable id
    means repeat runs overwrite one series, which is the only reading that can
    be correct.

    Long-lived services must NOT pass this: there a per-instance id is what
    makes "which replica is slow" answerable.
    """
    settings = get_settings()
    _configure_logging(
        service_name,
        settings.log_level,
        json_logs=not settings.is_local,
        is_local=settings.is_local,
    )

    if not settings.otel_enabled:
        structlog.get_logger().info("telemetry.disabled", service=service_name)
        return

    attributes: dict[str, str] = {
        "service.name": service_name,
        "service.version": "0.1.0",
        "service.namespace": "transitpulse",
        "deployment.environment": settings.env,
    }
    if instance_id is not None:
        attributes["service.instance.id"] = instance_id
    resource = Resource.create(attributes)

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

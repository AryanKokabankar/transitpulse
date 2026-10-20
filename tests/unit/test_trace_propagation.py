"""Tests for trace context crossing the Pub/Sub boundary.

This is the mechanism that makes one trace span the collector and the
processor. If it regresses, nothing breaks loudly -- traces just quietly
fragment into unrelated halves -- so it is worth asserting directly.
"""

from __future__ import annotations

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from transitpulse_core.telemetry import (
    current_trace_id,
    extract_trace_context,
    inject_trace_context,
)


def _tracer() -> tuple[trace.Tracer, InMemorySpanExporter]:
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    return provider.get_tracer("test"), exporter


class TestInjection:
    def test_adds_traceparent_while_a_span_is_active(self) -> None:
        tracer, _ = _tracer()
        with tracer.start_as_current_span("publish"):
            attributes = inject_trace_context()
        assert "traceparent" in attributes

    def test_preserves_existing_attributes(self) -> None:
        """Injection must not clobber the business attributes already set."""
        tracer, _ = _tracer()
        with tracer.start_as_current_span("publish"):
            attributes = inject_trace_context({"agency": "mta-nyct"})
        assert attributes["agency"] == "mta-nyct"

    def test_emits_nothing_without_an_active_span(self) -> None:
        assert inject_trace_context({"agency": "x"}) == {"agency": "x"}


class TestRoundTrip:
    def test_consumer_span_joins_the_publisher_trace(self) -> None:
        """The whole point: one trace id across two processes."""
        tracer, exporter = _tracer()

        with tracer.start_as_current_span("collector.publish") as producer:
            carrier = inject_trace_context({"agency": "mta-nyct"})
            expected_trace_id = producer.get_span_context().trace_id

        # Simulating the consumer process: only the attribute map crosses over.
        context = extract_trace_context(carrier)
        with tracer.start_as_current_span("processor.consume", context=context) as consumer:
            assert consumer.get_span_context().trace_id == expected_trace_id

        exported = {span.name: span for span in exporter.get_finished_spans()}
        assert exported["processor.consume"].parent is not None
        assert (
            exported["processor.consume"].parent.span_id
            == exported["collector.publish"].get_span_context().span_id
        )

    def test_missing_attributes_start_a_fresh_trace(self) -> None:
        """A message published before instrumentation must not crash the consumer."""
        tracer, _ = _tracer()
        context = extract_trace_context(None)
        with tracer.start_as_current_span("processor.consume", context=context) as span:
            assert span.get_span_context().is_valid

    def test_malformed_traceparent_is_ignored(self) -> None:
        tracer, _ = _tracer()
        context = extract_trace_context({"traceparent": "not-a-valid-header"})
        with tracer.start_as_current_span("processor.consume", context=context) as span:
            assert span.get_span_context().is_valid


class TestCurrentTraceId:
    def test_returns_hex_id_inside_a_span(self) -> None:
        tracer, _ = _tracer()
        with tracer.start_as_current_span("work"):
            trace_id = current_trace_id()
        assert trace_id is not None
        assert len(trace_id) == 32
        int(trace_id, 16)  # must be valid hex for Cloud Trace links to resolve

    def test_returns_none_outside_a_span(self) -> None:
        # Stamping "trace_id: 000...0" onto a quarantine row would be worse
        # than stamping NULL, because it looks like a real link.
        assert current_trace_id() is None

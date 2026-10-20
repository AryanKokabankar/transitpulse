"""Integration tests for the processor, from the real broker to real Postgres.

The unit tests cover what the handler decides. This covers the two things that
can only be established end to end:

**Rows actually land.** The enriched event goes through real SQL into a real
partitioned table and a real upsert, so the schema, the conflict clauses and
the enrichment lookups are exercised rather than assumed.

**One trace spans the broker.** The handler's span must be a child of the
collector's publish span, in the same trace, after the ``traceparent`` has
made a round trip through Pub/Sub as a message attribute. Nothing fails
loudly when that regresses -- the traces simply fragment into unrelated halves
and the pipeline becomes undebuggable while looking perfectly healthy -- so it
is asserted directly, against the broker rather than against a carrier dict.

Run with: python tasks.py up && python tasks.py test-integration
"""

from __future__ import annotations

import base64
import os
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient
from google.api_core import exceptions as gcp_exceptions
from google.cloud import pubsub_v1
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from services.processor import main as processor_main
from services.processor.enrich import Enricher
from tests.gtfs_fixtures import build_static_gtfs_zip
from transitpulse_core.config import get_settings
from transitpulse_core.models import VehiclePing
from transitpulse_core.pubsub import EventPublisher, ensure_local_topology
from transitpulse_core.sinks.postgres import PostgresSink

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module", autouse=True)
def _require_emulator() -> None:
    """Refuse to run against real Pub/Sub.

    Without this guard a missing PUBSUB_EMULATOR_HOST would publish into
    whatever project the ambient credentials point at.
    """
    if not os.environ.get("PUBSUB_EMULATOR_HOST"):
        pytest.skip("PUBSUB_EMULATOR_HOST is unset; run `python tasks.py up` first")


@pytest.fixture(scope="module")
def exported_spans() -> InMemorySpanExporter:
    """Real spans, collected in memory so parentage can be asserted.

    The suite disables OTel export by default, which leaves a no-op provider
    that records nothing and injects no trace context -- and the headline
    assertion in this module is about trace context. The global provider can
    only be set once per process, so one installed by another module is reused
    and only the exporter is added.
    """
    provider = trace.get_tracer_provider()
    if not isinstance(provider, TracerProvider):
        provider = TracerProvider()
        trace.set_tracer_provider(provider)
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    return exporter


@pytest.fixture(scope="module", autouse=True)
def topology() -> None:
    try:
        ensure_local_topology(get_settings())
    except Exception as exc:  # noqa: BLE001 - surfaced as a skip, not a failure
        pytest.skip(f"Pub/Sub emulator is not reachable: {exc}")


@pytest.fixture(scope="module")
def sink() -> Iterator[PostgresSink]:
    instance = PostgresSink(get_settings())
    if not instance.healthcheck():
        pytest.skip("PostgreSQL is not reachable; run `python tasks.py up` first")
    yield instance
    instance.close()


@pytest.fixture(scope="module")
def publisher() -> Iterator[EventPublisher]:
    instance = EventPublisher(get_settings())
    yield instance
    instance.close()


@pytest.fixture(scope="module")
def subscriber() -> Iterator[pubsub_v1.SubscriberClient]:
    client = pubsub_v1.SubscriberClient()
    yield client
    client.close()


@pytest.fixture
def processor(sink: PostgresSink, monkeypatch: pytest.MonkeyPatch) -> Iterator[TestClient]:
    """The real handler over the real sink, with BigQuery off.

    Not ``with TestClient(...)``: entering it would run the lifespan and open
    a second connection pool against the same database.
    """
    monkeypatch.setattr(processor_main, "_sink", sink)
    monkeypatch.setattr(processor_main, "_enricher", Enricher(sink, get_settings()))
    monkeypatch.setattr(processor_main, "_bigquery", None)
    yield TestClient(processor_main.app)


@pytest.fixture(autouse=True)
def _clean_tables(sink: PostgresSink) -> None:
    with sink.pool.connection() as conn, conn.cursor() as cur:
        cur.execute("TRUNCATE vehicle_events, vehicle_live, quarantine")


def drain(
    subscriber: pubsub_v1.SubscriberClient, max_messages: int = 50, timeout: float = 10.0
) -> list[Any]:
    cfg = get_settings()
    path = subscriber.subscription_path(cfg.gcp_project, cfg.subscription_processor)
    try:
        response = subscriber.pull(
            request={"subscription": path, "max_messages": max_messages}, timeout=timeout
        )
    except gcp_exceptions.DeadlineExceeded:
        return []
    if response.received_messages:
        subscriber.acknowledge(
            request={
                "subscription": path,
                "ack_ids": [m.ack_id for m in response.received_messages],
            }
        )
    return list(response.received_messages)


@pytest.fixture(autouse=True)
def _clear_backlog(subscriber: pubsub_v1.SubscriberClient) -> None:
    """Start each test with an empty subscription.

    The load harness publishes thousands of messages against this same
    emulator, so a suite run after `python tasks.py replay` would otherwise
    find a backlog it could not clear. The emulator does not implement `seek`,
    so draining is the only way to purge.
    """
    for _ in range(60):
        if not drain(subscriber, max_messages=1000, timeout=1.0):
            break


def push_body(received: Any, *, delivery_attempt: int = 1) -> dict[str, Any]:
    """Rebuild the push envelope Cloud Run would receive for a pulled message.

    The emulator has no way to push to a local test server, so the delivery is
    reconstructed from what the broker actually handed back -- the real
    attributes, including the real ``traceparent``, and the real message id.
    """
    message = received.message
    return {
        "message": {
            "data": base64.b64encode(bytes(message.data)).decode(),
            "attributes": dict(message.attributes),
            "messageId": message.message_id,
            "publishTime": message.publish_time.isoformat(),
        },
        "subscription": "projects/transitpulse-local/subscriptions/vehicle-events-processor",
        "deliveryAttempt": delivery_attempt,
    }


def make_live_ping(**overrides: Any) -> VehiclePing:
    """A ping timestamped now, so it lands in today's partition."""
    now = datetime.now(UTC)
    defaults: dict[str, Any] = {
        "agency": get_settings().feed_agency,
        "vehicle_id": "bus-1",
        "trip_id": "ABC123",
        "route_id": "Q",
        "current_stop_sequence": 4,
        "stop_id": "Q05N",
        "latitude": 40.7128,
        "longitude": -74.0060,
        "event_time": now,
        "ingest_time": now,
    }
    defaults.update(overrides)
    return VehiclePing(**defaults)


def publish_and_pull(
    publisher: EventPublisher,
    subscriber: pubsub_v1.SubscriberClient,
    ping: VehiclePing,
    *,
    span_name: str = "collector.collect",
) -> tuple[str, Any]:
    """Publish one ping inside a tick span and pull it back out.

    Returns the tick's trace id and the received message, which together are
    what the trace assertions compare.
    """
    tracer = trace.get_tracer("test")
    with tracer.start_as_current_span(span_name) as tick:
        trace_id = format(tick.get_span_context().trace_id, "032x")
        assert publisher.publish_batch([ping]) == 1
    received = drain(subscriber)
    assert len(received) == 1, f"expected exactly one message, got {len(received)}"
    return trace_id, received[0]


class TestRowsLand:
    def test_a_published_ping_becomes_rows_in_both_tables(
        self,
        processor: TestClient,
        publisher: EventPublisher,
        subscriber: pubsub_v1.SubscriberClient,
        sink: PostgresSink,
    ) -> None:
        ping = make_live_ping(vehicle_id="bus-1")
        _, received = publish_and_pull(publisher, subscriber, ping)

        response = processor.post("/", json=push_body(received))

        assert response.status_code == 200
        assert response.json()["outcome"] == "processed"
        with sink.pool.connection() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT idempotency_key, vehicle_id FROM vehicle_events WHERE vehicle_id = %s",
                ("bus-1",),
            )
            events = cur.fetchall()
            cur.execute(
                "SELECT vehicle_id, latitude FROM vehicle_live WHERE vehicle_id = %s", ("bus-1",)
            )
            live = cur.fetchall()

        assert [row[0] for row in events] == [ping.idempotency_key]
        assert [row[0] for row in live] == ["bus-1"]

    def test_the_second_delivery_of_a_message_adds_no_row(
        self,
        processor: TestClient,
        publisher: EventPublisher,
        subscriber: pubsub_v1.SubscriberClient,
        sink: PostgresSink,
    ) -> None:
        """At-least-once delivery is the normal operating condition, not an
        edge case, so a redelivery has to be a no-op rather than a duplicate
        (ADR-005)."""
        ping = make_live_ping(vehicle_id="bus-dup")
        _, received = publish_and_pull(publisher, subscriber, ping)
        body = push_body(received)

        first = processor.post("/", json=body)
        second = processor.post("/", json=push_body(received, delivery_attempt=2))

        assert (first.status_code, second.status_code) == (200, 200)
        with sink.pool.connection() as conn, conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM vehicle_events WHERE vehicle_id = %s", ("bus-dup",))
            assert (cur.fetchone() or [None])[0] == 1

    def test_motion_is_computed_against_the_previous_delivery(
        self,
        processor: TestClient,
        publisher: EventPublisher,
        subscriber: pubsub_v1.SubscriberClient,
        sink: PostgresSink,
    ) -> None:
        """The previous position comes out of ``vehicle_live``, which the
        earlier message wrote -- so this is the enrichment read and the
        serving write meeting in the database."""
        now = datetime.now(UTC)
        first = make_live_ping(vehicle_id="bus-move", event_time=now - timedelta(seconds=30))
        second = make_live_ping(vehicle_id="bus-move", latitude=40.7158, event_time=now)

        _, received_first = publish_and_pull(publisher, subscriber, first)
        processor.post("/", json=push_body(received_first))
        _, received_second = publish_and_pull(publisher, subscriber, second)
        processor.post("/", json=push_body(received_second))

        with sink.pool.connection() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT distance_from_last_m, seconds_since_last_ping, computed_speed_mps
                FROM vehicle_events WHERE vehicle_id = %s ORDER BY event_time
                """,
                ("bus-move",),
            )
            rows = cur.fetchall()

        assert rows[0] == (None, None, None), "the first sighting has nothing to compare against"
        distance, seconds, speed = rows[1]
        assert distance == pytest.approx(333.0, abs=5.0)
        assert seconds == pytest.approx(30.0, abs=0.1)
        assert speed == pytest.approx(distance / seconds)

    def test_a_malformed_payload_lands_in_quarantine(
        self,
        processor: TestClient,
        sink: PostgresSink,
    ) -> None:
        """Rejected, but not lost: the raw payload and the trace id are what
        make it diagnosable and, once the bug is fixed, replayable."""
        body = {
            "message": {
                "data": base64.b64encode(b'{"agency": "mbta", "latitude": 999.0}').decode(),
                "attributes": {},
                "messageId": "msg-bad",
            },
            "deliveryAttempt": 1,
        }

        response = processor.post("/", json=body)

        assert response.status_code == 200
        with sink.pool.connection() as conn, conn.cursor() as cur:
            cur.execute("SELECT reason, message_id, raw_payload FROM quarantine")
            rows = cur.fetchall()

        assert len(rows) == 1
        reason, message_id, payload = rows[0]
        assert reason == "SCHEMA_VALIDATION"
        assert message_id == "msg-bad"
        assert b"999.0" in bytes(payload)


class TestTracePropagation:
    def test_the_handler_span_joins_the_publishers_trace(
        self,
        processor: TestClient,
        publisher: EventPublisher,
        subscriber: pubsub_v1.SubscriberClient,
        exported_spans: InMemorySpanExporter,
    ) -> None:
        """The single most important line in the phase.

        The handler starts its span from the context carried in the message
        attributes, which makes it a child of the publish span rather than a
        trace root. One waterfall then covers fetch -> decode -> publish ->
        consume -> enrich -> Postgres, which is what makes "where did the
        latency go" a question with an answer.
        """
        exported_spans.clear()
        trace_id, received = publish_and_pull(
            publisher, subscriber, make_live_ping(vehicle_id="bus-trace")
        )
        assert "traceparent" in dict(received.message.attributes), (
            "trace context did not cross the broker"
        )

        response = processor.post("/", json=push_body(received))

        assert response.status_code == 200
        assert response.json()["trace_id"] == trace_id

        spans = {span.name: span for span in exported_spans.get_finished_spans()}
        handler = spans["processor.handle"]
        assert format(handler.context.trace_id, "032x") == trace_id
        assert handler.parent is not None, "the handler span is a trace root"
        assert handler.parent.span_id == spans["pubsub.publish_batch"].context.span_id

    def test_the_sink_write_is_in_the_same_trace(
        self,
        processor: TestClient,
        publisher: EventPublisher,
        subscriber: pubsub_v1.SubscriberClient,
        exported_spans: InMemorySpanExporter,
    ) -> None:
        """The database write has to be inside the joined trace too, or the
        waterfall stops exactly where the interesting latency usually is."""
        exported_spans.clear()
        trace_id, received = publish_and_pull(
            publisher, subscriber, make_live_ping(vehicle_id="bus-trace-2")
        )

        processor.post("/", json=push_body(received))

        write_spans = [
            span
            for span in exported_spans.get_finished_spans()
            if span.name == "postgres.write_batch"
        ]
        assert write_spans, "the sink produced no span"
        assert format(write_spans[-1].context.trace_id, "032x") == trace_id

    def test_a_message_without_trace_context_still_processes(
        self,
        processor: TestClient,
        publisher: EventPublisher,
        subscriber: pubsub_v1.SubscriberClient,
    ) -> None:
        """A message published before instrumentation, or replayed from the
        corpus by a tool that does not trace, must not crash the consumer --
        it just starts a fresh trace."""
        _, received = publish_and_pull(
            publisher, subscriber, make_live_ping(vehicle_id="bus-untraced")
        )
        body = push_body(received)
        body["message"]["attributes"] = {}

        response = processor.post("/", json=body)

        assert response.status_code == 200
        assert response.json()["outcome"] == "processed"


class TestScheduleEnrichment:
    @pytest.fixture
    def timetable(self, sink: PostgresSink, tmp_path: Path) -> Iterator[None]:
        """Load the small static fixture, then put the tables back empty.

        The loader is what populates these in production, so the test uses it
        rather than hand-written INSERTs -- otherwise the join could pass here
        and fail against anything the loader actually writes.
        """
        from services.jobs.gtfs_static_loader import load_archive

        load_archive(build_static_gtfs_zip(tmp_path / "gtfs.zip"), get_settings())
        yield
        with sink.pool.connection() as conn, conn.cursor() as cur:
            cur.execute("TRUNCATE gtfs_routes, gtfs_trips, gtfs_stops, gtfs_stop_times")

    def test_the_route_name_and_delay_are_attached(
        self,
        processor: TestClient,
        publisher: EventPublisher,
        subscriber: pubsub_v1.SubscriberClient,
        sink: PostgresSink,
        timetable: None,
    ) -> None:
        ping = make_live_ping(vehicle_id="bus-sched", trip_id="ABC123", current_stop_sequence=4)
        _, received = publish_and_pull(publisher, subscriber, ping)

        processor.post("/", json=push_body(received))

        with sink.pool.connection() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT route_short_name, schedule_delay_seconds, is_on_time
                FROM vehicle_events WHERE vehicle_id = %s
                """,
                ("bus-sched",),
            )
            row = cur.fetchone()

        assert row is not None
        route_short_name, delay, on_time = row
        assert route_short_name == "Q"
        # The fixture schedules this stop at 08:15 local, so a ping from now
        # is late by whatever "now" happens to be -- the assertion worth
        # making is that a delay was computed at all and agrees with the flag.
        assert delay is not None
        assert on_time is (-60 <= delay <= 300)

    def test_a_trip_with_no_timetable_gets_no_delay(
        self,
        processor: TestClient,
        publisher: EventPublisher,
        subscriber: pubsub_v1.SubscriberClient,
        sink: PostgresSink,
        timetable: None,
    ) -> None:
        """Realtime-added trips have no scheduled time. A zero here would read
        as "exactly on time" and inflate the on-time rate."""
        ping = make_live_ping(vehicle_id="bus-added", trip_id="ADDED-999")
        _, received = publish_and_pull(publisher, subscriber, ping)

        processor.post("/", json=push_body(received))

        with sink.pool.connection() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT schedule_delay_seconds, is_on_time FROM vehicle_events WHERE vehicle_id = %s",
                ("bus-added",),
            )
            assert cur.fetchone() == (None, None)

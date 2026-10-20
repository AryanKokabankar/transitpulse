"""Integration tests for the collector against the real Pub/Sub emulator.

The unit tests cover what the handler decides; these cover what actually
crosses the broker. The property worth the Docker dependency is the trace
context: a published message must carry the tick's own ``traceparent``, so the
processor's span can join the collector's trace and one waterfall covers
fetch -> decode -> publish -> consume. Nothing fails loudly when that breaks --
traces simply fragment, and the pipeline becomes undebuggable while looking
perfectly healthy.

The upstream feed is still stubbed. A test that depended on a live agency
endpoint would fail for reasons that have nothing to do with this code, and
would make CI depend on the internet.

Run with: python tasks.py up && python tasks.py test-integration
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from google.api_core import exceptions as gcp_exceptions
from google.cloud import pubsub_v1
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from services.collector import main as collector_main
from services.collector.feed import FeedResponse
from tests.gtfs_fixtures import VehicleEntitySpec, build_vehicle_feed
from transitpulse_core.config import get_settings
from transitpulse_core.models import VehiclePing
from transitpulse_core.pubsub import EventPublisher, ensure_local_topology
from transitpulse_core.snapshots import LocalSnapshotArchive
from transitpulse_core.telemetry import extract_trace_context

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module", autouse=True)
def _require_emulator() -> None:
    """Refuse to run against real Pub/Sub.

    Without this guard a missing PUBSUB_EMULATOR_HOST would publish into
    whatever project the ambient credentials point at.
    """
    if not os.environ.get("PUBSUB_EMULATOR_HOST"):
        pytest.skip("PUBSUB_EMULATOR_HOST is unset; run `python tasks.py up` first")


@pytest.fixture(scope="module", autouse=True)
def _real_tracer_provider() -> None:
    """Install an SDK tracer provider so spans and traceparents are real.

    The suite disables OTel export by default, which leaves a no-op provider
    that injects no trace context -- and this module exists to assert that the
    context is injected. The global provider can only be set once per process,
    so an SDK provider installed by another module is reused rather than
    fought over.
    """
    if not isinstance(trace.get_tracer_provider(), TracerProvider):
        provider = TracerProvider()
        provider.add_span_processor(SimpleSpanProcessor(InMemorySpanExporter()))
        trace.set_tracer_provider(provider)


@pytest.fixture(scope="module", autouse=True)
def topology() -> Iterator[None]:
    try:
        ensure_local_topology(get_settings())
    except Exception as exc:  # noqa: BLE001 - surfaced as a skip, not a failure
        pytest.skip(f"Pub/Sub emulator is not reachable: {exc}")
    yield


@pytest.fixture
def subscriber() -> Iterator[pubsub_v1.SubscriberClient]:
    client = pubsub_v1.SubscriberClient()
    yield client
    client.close()


def _drain(
    subscriber: pubsub_v1.SubscriberClient,
    max_messages: int = 50,
    timeout: float = 10.0,
) -> list[pubsub_v1.types.ReceivedMessage]:
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

    Generously bounded rather than "a few pulls": the load harness publishes
    thousands of messages against this same emulator, so a suite run after
    `python tasks.py replay` would otherwise find a backlog it could not clear
    and fail on message counts that have nothing to do with the code. The
    emulator does not implement `seek`, so draining is the only way to purge.
    A short deadline keeps the common case -- an already-empty subscription --
    to a single pull.
    """
    for _ in range(60):
        if not _drain(subscriber, max_messages=1000, timeout=1.0):
            break


class StubFeedClient:
    """Stands in for the agency, so the test depends on the broker only."""

    def __init__(self, payload: bytes) -> None:
        self._payload = payload

    async def fetch(self) -> FeedResponse:
        return FeedResponse(
            payload=self._payload,
            fetched_at=datetime.now(UTC),
            duration_seconds=0.05,
            attempts=1,
        )


@pytest.fixture
def collector(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Iterator[tuple[TestClient, LocalSnapshotArchive]]:
    """The real handler with the real publisher, a stub feed and a temp corpus."""
    archive = LocalSnapshotArchive(tmp_path)
    publisher = EventPublisher(get_settings())
    monkeypatch.setattr(collector_main, "_publisher", publisher)
    monkeypatch.setattr(collector_main, "_archive", archive)
    try:
        yield TestClient(collector_main.app), archive
    finally:
        publisher.close()


def _set_feed(monkeypatch: pytest.MonkeyPatch, payload: bytes) -> None:
    monkeypatch.setattr(collector_main, "_feed_client", StubFeedClient(payload))


class TestEndToEndTick:
    def test_publishes_decoded_pings_to_the_broker(
        self,
        collector: tuple[TestClient, LocalSnapshotArchive],
        subscriber: pubsub_v1.SubscriberClient,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        client, _ = collector
        _set_feed(
            monkeypatch,
            build_vehicle_feed(
                VehicleEntitySpec(entity_id="1", vehicle_id="bus-1"),
                VehicleEntitySpec(entity_id="2", vehicle_id="bus-2", route_id="B"),
            ),
        )

        response = client.post("/collect")
        assert response.status_code == 200
        assert response.json()["published"] == 2

        received = _drain(subscriber)
        assert len(received) == 2
        restored = [VehiclePing.model_validate_json(m.message.data) for m in received]
        assert {ping.vehicle_id for ping in restored} == {"bus-1", "bus-2"}

    def test_the_ordering_key_is_the_vehicle_id(
        self,
        collector: tuple[TestClient, LocalSnapshotArchive],
        subscriber: pubsub_v1.SubscriberClient,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Per-vehicle ordering: correct without serialising the whole stream."""
        client, _ = collector
        _set_feed(monkeypatch, build_vehicle_feed(VehicleEntitySpec(vehicle_id="bus-7")))

        client.post("/collect")

        received = _drain(subscriber)
        assert received[0].message.ordering_key == "bus-7"

    def test_the_snapshot_and_the_published_payload_agree(
        self,
        collector: tuple[TestClient, LocalSnapshotArchive],
        subscriber: pubsub_v1.SubscriberClient,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The corpus has to be a faithful record of what was published from
        it, or a replay reproduces something that never happened."""
        client, archive = collector
        payload = build_vehicle_feed(VehicleEntitySpec(vehicle_id="bus-1"))
        _set_feed(monkeypatch, payload)

        body = client.post("/collect").json()

        assert archive.load_payload(body["snapshot_path"]) == payload
        received = _drain(subscriber)
        published = VehiclePing.model_validate_json(received[0].message.data)
        assert published.vehicle_id == "bus-1"

    def test_quarantined_entities_are_not_published(
        self,
        collector: tuple[TestClient, LocalSnapshotArchive],
        subscriber: pubsub_v1.SubscriberClient,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        client, archive = collector
        _set_feed(
            monkeypatch,
            build_vehicle_feed(
                VehicleEntitySpec(entity_id="1", vehicle_id="good"),
                VehicleEntitySpec(entity_id="2", vehicle_id="null", latitude=0.0, longitude=0.0),
            ),
        )

        body = client.post("/collect").json()

        assert body["published"] == 1
        assert body["quarantined"] == 1
        received = _drain(subscriber)
        assert len(received) == 1
        # Rejected, but not lost: it is on disk beside the payload.
        assert list(archive.root.rglob("*.quarantine.json"))


class TestTracePropagation:
    def test_trace_context_survives_the_round_trip(
        self,
        collector: tuple[TestClient, LocalSnapshotArchive],
        subscriber: pubsub_v1.SubscriberClient,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The headline mechanism, end to end through the real broker.

        The tick reports its own trace id; the message that comes back out of
        Pub/Sub must carry a ``traceparent`` naming that same trace, and a span
        started from it must land in that trace rather than beginning a new one.
        """
        client, _ = collector
        _set_feed(monkeypatch, build_vehicle_feed(VehicleEntitySpec(vehicle_id="bus-1")))

        body = client.post("/collect").json()
        tick_trace_id = body["trace_id"]
        assert tick_trace_id, "the collect handler produced no trace id"

        received = _drain(subscriber)
        assert len(received) == 1
        attributes = dict(received[0].message.attributes)
        assert "traceparent" in attributes, "trace context did not cross the broker"
        assert tick_trace_id in attributes["traceparent"]

        # The consuming side: a span built from the extracted context belongs
        # to the collector's trace, which is what makes one waterfall possible.
        context = extract_trace_context(attributes)
        tracer = trace.get_tracer("test")
        with tracer.start_as_current_span("processor.consume", context=context) as consumer:
            assert format(consumer.get_span_context().trace_id, "032x") == tick_trace_id

    def test_every_message_in_a_tick_shares_one_trace(
        self,
        collector: tuple[TestClient, LocalSnapshotArchive],
        subscriber: pubsub_v1.SubscriberClient,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """One fetch produced them all, so one trace should explain them all."""
        client, _ = collector
        _set_feed(
            monkeypatch,
            build_vehicle_feed(
                *[VehicleEntitySpec(entity_id=str(i), vehicle_id=f"bus-{i}") for i in range(5)]
            ),
        )

        body = client.post("/collect").json()
        received = _drain(subscriber)

        assert len(received) == 5
        trace_ids = {dict(m.message.attributes)["traceparent"].split("-")[1] for m in received}
        assert trace_ids == {body["trace_id"]}

    def test_separate_ticks_get_separate_traces(
        self,
        collector: tuple[TestClient, LocalSnapshotArchive],
        subscriber: pubsub_v1.SubscriberClient,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # Otherwise a day of ticks collapses into one unreadable trace.
        client, _ = collector
        _set_feed(monkeypatch, build_vehicle_feed(VehicleEntitySpec(vehicle_id="bus-1")))

        first = client.post("/collect").json()["trace_id"]
        _drain(subscriber)
        second = client.post("/collect").json()["trace_id"]
        _drain(subscriber)

        assert first != second


class TestBusinessAttributes:
    def test_messages_carry_the_routing_attributes(
        self,
        collector: tuple[TestClient, LocalSnapshotArchive],
        subscriber: pubsub_v1.SubscriberClient,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The processor filters and deduplicates on these without having to
        deserialise the body first."""
        client, _ = collector
        _set_feed(monkeypatch, build_vehicle_feed(VehicleEntitySpec(vehicle_id="bus-1")))

        client.post("/collect")

        attributes = dict(_drain(subscriber)[0].message.attributes)
        assert attributes["agency"] == get_settings().feed_agency
        assert attributes["schema_version"] == "1"
        assert attributes["idempotency_key"]
        assert attributes["event_time"]

    def test_the_idempotency_key_is_stable_across_ticks(
        self,
        collector: tuple[TestClient, LocalSnapshotArchive],
        subscriber: pubsub_v1.SubscriberClient,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """An agency re-serving the same ping must not produce a second row.

        The collector re-publishes it -- deduplication is the processor's
        job -- but the key has to match, and it only does if ingest_time is
        excluded from it as the contract intends.
        """
        client, _ = collector
        _set_feed(monkeypatch, build_vehicle_feed(VehicleEntitySpec(vehicle_id="bus-1")))

        client.post("/collect")
        first = dict(_drain(subscriber)[0].message.attributes)["idempotency_key"]
        client.post("/collect")
        second = dict(_drain(subscriber)[0].message.attributes)["idempotency_key"]

        assert first == second

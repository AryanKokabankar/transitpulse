"""Integration tests for publishing and trace propagation through Pub/Sub.

Against the real emulator rather than a mocked client, because the two
properties asserted here are the broker's: that ordering keys are accepted and
honoured, and that message attributes survive a round trip intact.

The trace-propagation test is the important one. If that mechanism regresses,
nothing fails loudly -- traces simply fragment into unrelated halves, and the
pipeline looks fine while becoming undebuggable.

Run with: python tasks.py up && python tasks.py test-integration
"""

from __future__ import annotations

import os
from collections.abc import Iterator

import pytest
from google.api_core import exceptions as gcp_exceptions
from google.cloud import pubsub_v1
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from tests.conftest import MakePing
from transitpulse_core.config import get_settings
from transitpulse_core.models import VehiclePing
from transitpulse_core.pubsub import (
    EventPublisher,
    PushEnvelope,
    decode_push_body,
    ensure_local_topology,
)
from transitpulse_core.telemetry import extract_trace_context

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module", autouse=True)
def _require_emulator() -> None:
    """Refuse to run against real Pub/Sub.

    Without this guard a missing PUBSUB_EMULATOR_HOST would make these tests
    create topics in whatever project the ambient credentials point at.
    """
    if not os.environ.get("PUBSUB_EMULATOR_HOST"):
        pytest.skip("PUBSUB_EMULATOR_HOST is unset; run `python tasks.py up` first")


@pytest.fixture(scope="module")
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
    subscriber: pubsub_v1.SubscriberClient, max_messages: int = 10
) -> list[pubsub_v1.types.ReceivedMessage]:
    """Pull whatever is pending, acking it so tests do not interfere."""
    cfg = get_settings()
    path = subscriber.subscription_path(cfg.gcp_project, cfg.subscription_processor)
    try:
        response = subscriber.pull(
            request={"subscription": path, "max_messages": max_messages},
            timeout=10.0,
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
def _clear_backlog(topology: None, subscriber: pubsub_v1.SubscriberClient) -> None:
    """Start each test with an empty subscription."""
    for _ in range(5):
        if not _drain(subscriber, max_messages=100):
            break


class TestTopology:
    def test_is_idempotent(self) -> None:
        """Called on every local start, so a second call must be a no-op."""
        ensure_local_topology(get_settings())
        ensure_local_topology(get_settings())

    def test_subscription_has_a_dead_letter_policy(
        self, subscriber: pubsub_v1.SubscriberClient
    ) -> None:
        cfg = get_settings()
        sub = subscriber.get_subscription(
            request={
                "subscription": subscriber.subscription_path(
                    cfg.gcp_project, cfg.subscription_processor
                )
            }
        )
        assert sub.dead_letter_policy.max_delivery_attempts == 5
        assert sub.dead_letter_policy.dead_letter_topic.endswith(cfg.topic_dlq)
        assert sub.enable_message_ordering


class TestPublish:
    def test_publishes_a_batch(self, make_ping: MakePing) -> None:
        publisher = EventPublisher(get_settings())
        try:
            assert publisher.publish_batch([make_ping()]) == 1
        finally:
            publisher.close()

    def test_empty_batch_is_a_no_op(self) -> None:
        publisher = EventPublisher(get_settings())
        try:
            assert publisher.publish_batch([]) == 0
        finally:
            publisher.close()

    def test_message_carries_the_business_attributes(
        self, make_ping: MakePing, subscriber: pubsub_v1.SubscriberClient
    ) -> None:
        ping = make_ping()
        publisher = EventPublisher(get_settings())
        try:
            publisher.publish_batch([ping])
        finally:
            publisher.close()

        received = _drain(subscriber)
        assert len(received) == 1
        attributes = dict(received[0].message.attributes)
        assert attributes["agency"] == ping.agency
        assert attributes["idempotency_key"] == ping.idempotency_key
        assert attributes["schema_version"] == str(ping.schema_version)

    def test_payload_round_trips_through_the_model(
        self, make_ping: MakePing, subscriber: pubsub_v1.SubscriberClient
    ) -> None:
        """The wire format must deserialise back into an identical ping."""
        ping = make_ping()
        publisher = EventPublisher(get_settings())
        try:
            publisher.publish_batch([ping])
        finally:
            publisher.close()

        received = _drain(subscriber)
        assert len(received) == 1
        restored = VehiclePing.model_validate_json(received[0].message.data)
        assert restored == ping
        assert restored.idempotency_key == ping.idempotency_key

    def test_ordering_key_is_the_vehicle_id(
        self, make_ping: MakePing, subscriber: pubsub_v1.SubscriberClient
    ) -> None:
        """Per-vehicle ordering, not global: correctness without serialising
        the whole stream."""
        ping = make_ping()
        publisher = EventPublisher(get_settings())
        try:
            publisher.publish_batch([ping])
        finally:
            publisher.close()

        received = _drain(subscriber)
        assert received[0].message.ordering_key == ping.vehicle_id


class TestTracePropagation:
    def test_traceparent_survives_the_broker(
        self, make_ping: MakePing, subscriber: pubsub_v1.SubscriberClient
    ) -> None:
        """The headline mechanism: one trace spanning two processes.

        Publishes inside a span, pulls the message back, and asserts that a
        span started from the extracted context lands in the same trace.
        """
        exporter = InMemorySpanExporter()
        provider = TracerProvider()
        provider.add_span_processor(SimpleSpanProcessor(exporter))
        tracer = provider.get_tracer("test")

        publisher = EventPublisher(get_settings())
        try:
            with tracer.start_as_current_span("collector.tick") as producer:
                expected_trace_id = producer.get_span_context().trace_id
                publisher.publish_batch([make_ping()])
        finally:
            publisher.close()

        received = _drain(subscriber)
        assert len(received) == 1
        attributes = dict(received[0].message.attributes)
        assert "traceparent" in attributes, "trace context did not cross the broker"

        context = extract_trace_context(attributes)
        with tracer.start_as_current_span("processor.consume", context=context) as consumer:
            assert consumer.get_span_context().trace_id == expected_trace_id


class TestPushEnvelope:
    def test_decodes_a_real_push_body(self) -> None:
        """Cloud Run receives this shape; deliveryAttempt is what makes
        poison-message handling possible."""
        body = (
            b'{"message":{"data":"eyJhIjoxfQ==","attributes":{"agency":"mta-nyct"},'
            b'"messageId":"123","orderingKey":"bus-1"},'
            b'"subscription":"projects/p/subscriptions/s","deliveryAttempt":4}'
        )
        envelope = decode_push_body(body)
        assert isinstance(envelope, PushEnvelope)
        assert envelope.delivery_attempt == 4
        assert envelope.message.message_id == "123"
        assert envelope.message.ordering_key == "bus-1"
        assert envelope.message.decoded() == b'{"a":1}'

    def test_defaults_delivery_attempt_when_absent(self) -> None:
        # Subscriptions without a DLQ policy omit the field entirely; treating
        # that as attempt 1 keeps the handler's retry logic uniform.
        envelope = decode_push_body(b'{"message":{"data":""},"subscription":"s"}')
        assert envelope.delivery_attempt == 1
        assert envelope.message.decoded() == b""

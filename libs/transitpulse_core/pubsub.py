"""Pub/Sub publish and push-delivery handling.

Two things here are load-bearing for the whole pipeline:

**Ordering keys.** Every message is published with ``ordering_key=vehicle_id``.
Pub/Sub then guarantees in-order delivery *per key* without serialising the
whole stream, so vehicle A's pings cannot overtake each other while remaining
fully parallel with vehicle B's. Global ordering would cap throughput at one
consumer; per-vehicle ordering is both correct and horizontally scalable.

**Trace attributes.** The publisher injects ``traceparent`` into message
attributes so the consumer can continue the same trace. See
:mod:`transitpulse_core.telemetry`.
"""

from __future__ import annotations

import base64
from collections.abc import Iterable, Sequence
from concurrent.futures import Future
from datetime import datetime
from typing import Any, cast

import google.cloud.pubsub_v1 as pubsub_v1
import orjson
import structlog
from google.api_core import retry
from opentelemetry.trace import SpanKind
from pydantic import BaseModel, ConfigDict, Field

from transitpulse_core.config import Settings, get_settings
from transitpulse_core.models import VehiclePing
from transitpulse_core.telemetry import get_metrics, get_tracer, inject_trace_context

log = structlog.get_logger(__name__)


class PushMessage(BaseModel):
    """The inner ``message`` object of a Pub/Sub push delivery."""

    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    data: str = ""
    attributes: dict[str, str] = Field(default_factory=dict)
    message_id: str = Field(default="", alias="messageId")
    publish_time: datetime | None = Field(default=None, alias="publishTime")
    ordering_key: str = Field(default="", alias="orderingKey")

    def decoded(self) -> bytes:
        return base64.b64decode(self.data) if self.data else b""


class PushEnvelope(BaseModel):
    """The full body Cloud Run receives for a push subscription.

    ``delivery_attempt`` is the field that makes poison-message handling
    possible: it tells the handler how many times this message has already
    failed, so it can stop retrying and quarantine instead.
    """

    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    message: PushMessage
    subscription: str = ""
    delivery_attempt: int = Field(default=1, alias="deliveryAttempt")


class EventPublisher:
    """Batching, ordered publisher for normalised pings.

    Batching is configured rather than hand-rolled: the client library already
    coalesces by size and latency, and fighting it only adds a second buffer to
    reason about during an incident.
    """

    def __init__(self, settings: Settings | None = None) -> None:
        self._settings = settings or get_settings()
        self._metrics = get_metrics()
        self._tracer = get_tracer()
        self._client = pubsub_v1.PublisherClient(
            batch_settings=pubsub_v1.types.BatchSettings(
                max_messages=self._settings.publish_max_batch_size,
                max_latency=self._settings.publish_max_latency_seconds,
            ),
            publisher_options=pubsub_v1.types.PublisherOptions(
                enable_message_ordering=True,
                retry=retry.Retry(initial=0.1, maximum=10.0, multiplier=2.0, deadline=60.0),
            ),
        )
        self._topic: str = self._client.topic_path(
            self._settings.gcp_project, self._settings.topic_vehicle_events
        )

    @property
    def topic_path(self) -> str:
        return self._topic

    def publish_one(self, ping: VehiclePing) -> Future[str]:
        """Publish a single ping. Returns the future without blocking."""
        payload = ping.model_dump_json().encode("utf-8")
        attributes = inject_trace_context(
            {
                "schema_version": str(ping.schema_version),
                "agency": ping.agency,
                "event_time": ping.event_time.isoformat(),
                "idempotency_key": ping.idempotency_key,
            }
        )
        return cast(
            "Future[str]",
            self._client.publish(
                self._topic,
                data=payload,
                ordering_key=ping.vehicle_id,
                **attributes,
            ),
        )

    def publish_batch(self, pings: Iterable[VehiclePing]) -> int:
        """Publish many pings under one span, then wait for all acks.

        Resolving every future before returning means the collector cannot
        report success for messages the broker never accepted -- the kind of
        silent gap that makes a pipeline look lossless while it is not.
        """
        batch: Sequence[VehiclePing] = list(pings)
        if not batch:
            return 0

        with self._tracer.start_as_current_span(
            "pubsub.publish_batch",
            kind=SpanKind.PRODUCER,
            attributes={
                "messaging.system": "gcp_pubsub",
                "messaging.destination.name": self._settings.topic_vehicle_events,
                "messaging.batch.message_count": len(batch),
            },
        ) as span:
            futures = [self.publish_one(ping) for ping in batch]

            published = 0
            failures = 0
            for future, ping in zip(futures, batch, strict=True):
                try:
                    future.result(timeout=60.0)
                    published += 1
                except Exception as exc:
                    failures += 1
                    # A publish failure for an ordering key pauses that key
                    # until resumed, otherwise every later ping for the same
                    # vehicle fails too.
                    self._client.resume_publish(self._topic, ping.vehicle_id)
                    log.error(
                        "publish.failed",
                        vehicle_id=ping.vehicle_id,
                        error=str(exc),
                        exc_info=True,
                    )

            span.set_attribute("messaging.batch.published", published)
            span.set_attribute("messaging.batch.failed", failures)
            self._metrics.events_published.add(
                published, {"agency": batch[0].agency, "outcome": "ok"}
            )
            if failures:
                self._metrics.events_published.add(
                    failures, {"agency": batch[0].agency, "outcome": "error"}
                )
            return published

    def close(self) -> None:
        self._client.stop()


def ensure_local_topology(settings: Settings | None = None) -> None:
    """Create topics, subscription and DLQ wiring against the emulator.

    Idempotent, so it is safe to run on every local start. The real
    environments get the same topology from Terraform instead -- this exists so
    that ``tasks.py up`` gives a working stack with no cloud account at all.
    """
    cfg = settings or get_settings()
    publisher = pubsub_v1.PublisherClient()
    subscriber = pubsub_v1.SubscriberClient()

    main_topic = publisher.topic_path(cfg.gcp_project, cfg.topic_vehicle_events)
    dlq_topic = publisher.topic_path(cfg.gcp_project, cfg.topic_dlq)
    subscription = subscriber.subscription_path(cfg.gcp_project, cfg.subscription_processor)
    dlq_subscription = subscriber.subscription_path(cfg.gcp_project, cfg.subscription_dlq)

    for topic in (main_topic, dlq_topic):
        try:
            publisher.create_topic(request={"name": topic})
            log.info("topology.topic_created", topic=topic)
        except Exception:  # noqa: BLE001 - AlreadyExists is the happy path here
            log.debug("topology.topic_exists", topic=topic)

    try:
        subscriber.create_subscription(
            request={
                "name": subscription,
                "topic": main_topic,
                "enable_message_ordering": True,
                "ack_deadline_seconds": 60,
                "dead_letter_policy": {
                    "dead_letter_topic": dlq_topic,
                    "max_delivery_attempts": cfg.max_delivery_attempts,
                },
            }
        )
        log.info("topology.subscription_created", subscription=subscription)
    except Exception:  # noqa: BLE001 - AlreadyExists is the happy path here
        log.debug("topology.subscription_exists", subscription=subscription)

    try:
        subscriber.create_subscription(
            request={
                "name": dlq_subscription,
                "topic": dlq_topic,
                # No ordering and no dead-letter policy of its own, both
                # deliberately. Ordered delivery would serialise the replayer
                # per vehicle for no benefit -- order is re-established by the
                # ordering key when it republishes to the main topic -- and a
                # dead-letter policy on a dead-letter subscription is a loop
                # with nowhere left to send a message that keeps failing.
                "ack_deadline_seconds": 60,
            }
        )
        log.info("topology.dlq_subscription_created", subscription=dlq_subscription)
    except Exception:  # noqa: BLE001 - AlreadyExists is the happy path here
        log.debug("topology.dlq_subscription_exists", subscription=dlq_subscription)


def decode_push_body(body: bytes) -> PushEnvelope:
    """Parse a push delivery body with orjson, which is ~3x stdlib json here."""
    raw: dict[str, Any] = orjson.loads(body)
    return PushEnvelope.model_validate(raw)

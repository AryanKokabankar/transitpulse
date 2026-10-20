"""Integration tests for the load harness against the real Pub/Sub emulator.

The unit tests prove the limiter's arithmetic against a fake clock. These
prove the thing the arithmetic is for: that a real run, through the real
ordered publisher and a real broker, lands the messages it claims to have
landed and reports a rate it actually achieved.

Rates here are deliberately modest. This is a correctness test of the
measurement, not a benchmark -- the published figures come from a deliberate
run on known hardware in a later phase, and asserting a throughput number
inside a test suite that shares a laptop with Docker would be exactly the kind
of number this project promises not to quote.

Run with: python tasks.py up && python tasks.py test-integration
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from google.api_core import exceptions as gcp_exceptions
from google.cloud import pubsub_v1

from tests.gtfs_fixtures import VehicleEntitySpec, build_vehicle_feed
from tools.replay.corpus import load_corpus
from tools.replay.harness import ReplayConfig, ReplayHarness
from transitpulse_core.config import get_settings
from transitpulse_core.models import VehiclePing
from transitpulse_core.pubsub import EventPublisher, ensure_local_topology
from transitpulse_core.snapshots import LocalSnapshotArchive

pytestmark = pytest.mark.integration

AGENCY = "mta-nyct"


@pytest.fixture(scope="module", autouse=True)
def _require_emulator() -> None:
    if not os.environ.get("PUBSUB_EMULATOR_HOST"):
        pytest.skip("PUBSUB_EMULATOR_HOST is unset; run `python tasks.py up` first")


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
    max_messages: int = 200,
    timeout: float = 5.0,
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


@pytest.fixture
def corpus_archive(tmp_path: Path) -> LocalSnapshotArchive:
    """A two-snapshot corpus with real protobuf and a little vehicle movement."""
    archive = LocalSnapshotArchive(tmp_path)
    base = datetime(2026, 10, 15, 14, 30, 0, tzinfo=UTC)
    for tick in range(2):
        tick_time = base + timedelta(seconds=15 * tick)
        archive.put_payload(
            build_vehicle_feed(
                *[
                    VehicleEntitySpec(
                        entity_id=f"v{index}",
                        vehicle_id=f"bus-{index}",
                        # Each tick carries its own vehicle timestamps, as a
                        # real feed does. Reusing one instant across both ticks
                        # would make the corpus hold the same observation twice,
                        # and half the replay would deduplicate instead of
                        # being written.
                        timestamp=int(tick_time.timestamp()),
                        latitude=40.7 + index * 0.01 + tick * 0.001,
                        longitude=-74.0 - index * 0.01,
                    )
                    for index in range(5)
                ],
                header_timestamp=int(tick_time.timestamp()),
            ),
            agency=AGENCY,
            fetched_at=tick_time,
        )
    return archive


@pytest.fixture
def publisher() -> Iterator[EventPublisher]:
    client = EventPublisher(get_settings())
    yield client
    client.close()


class TestReplayThroughTheBroker:
    def test_publishes_the_corpus_at_the_target_rate(
        self,
        corpus_archive: LocalSnapshotArchive,
        publisher: EventPublisher,
        subscriber: pubsub_v1.SubscriberClient,
    ) -> None:
        """A paced run must both land its messages and report honestly."""
        corpus = load_corpus(corpus_archive, agency=AGENCY)
        config = ReplayConfig(
            rate_per_second=50, duration=timedelta(seconds=2), batch_size=5, workers=2
        )

        report = ReplayHarness(config, corpus, publisher=publisher).run()

        assert report.published > 0
        assert report.failed == 0
        assert report.achieved_rate == pytest.approx(50, rel=0.35)
        assert report.duration_seconds == pytest.approx(2.0, abs=0.5)

        # The messages are really on the topic, not merely handed to a buffer.
        delivered = 0
        for _ in range(20):
            batch = _drain(subscriber, timeout=2.0)
            delivered += len(batch)
            if delivered >= report.published:
                break
        assert delivered >= report.published

    def test_published_messages_deserialise_back_into_the_contract(
        self,
        corpus_archive: LocalSnapshotArchive,
        publisher: EventPublisher,
        subscriber: pubsub_v1.SubscriberClient,
    ) -> None:
        corpus = load_corpus(corpus_archive, agency=AGENCY)
        config = ReplayConfig(
            rate_per_second=20, duration=timedelta(seconds=1), batch_size=5, workers=1
        )

        ReplayHarness(config, corpus, publisher=publisher).run()

        received = _drain(subscriber, timeout=5.0)
        assert received
        pings = [VehiclePing.model_validate_json(m.message.data) for m in received]
        assert all(ping.agency == AGENCY for ping in pings)
        # Re-based by default, so a load run exercises the write path rather
        # than the deduplication path.
        # Re-based into the past, never the future: a negative ingest lag
        # would corrupt the histogram the SLO is written against.
        assert all(ping.ingest_lag_seconds >= 0.0 for ping in pings)

    def test_replayed_messages_carry_trace_context(
        self,
        corpus_archive: LocalSnapshotArchive,
        publisher: EventPublisher,
        subscriber: pubsub_v1.SubscriberClient,
    ) -> None:
        """Load runs have to be traceable too: a saturation test is useless if
        the resulting latency cannot be attributed to a stage."""
        corpus = load_corpus(corpus_archive, agency=AGENCY)
        config = ReplayConfig(
            rate_per_second=20, duration=timedelta(seconds=1), batch_size=5, workers=1
        )

        ReplayHarness(config, corpus, publisher=publisher).run()

        received = _drain(subscriber, timeout=5.0)
        assert received
        attributes = dict(received[0].message.attributes)
        assert attributes["idempotency_key"]
        assert attributes["agency"] == AGENCY

    def test_fanout_produces_distinct_vehicles_on_the_topic(
        self,
        corpus_archive: LocalSnapshotArchive,
        publisher: EventPublisher,
        subscriber: pubsub_v1.SubscriberClient,
    ) -> None:
        """Fan-out is only load if the broker sees genuinely different
        vehicles: identical ordering keys would serialise the stream and
        identical idempotency keys would measure deduplication."""
        corpus = load_corpus(corpus_archive, agency=AGENCY)
        config = ReplayConfig(
            rate_per_second=40,
            duration=timedelta(seconds=1),
            fanout=4,
            batch_size=10,
            workers=2,
        )

        report = ReplayHarness(config, corpus, publisher=publisher).run()
        # The corpus holds the same five vehicles in both snapshots, so fan-out
        # multiplies the distinct vehicle count, not the ping count.
        base_vehicles = len({ping.vehicle_id for ping in corpus.pings})
        assert report.distinct_vehicles == base_vehicles * 4

        received: list[pubsub_v1.types.ReceivedMessage] = []
        for _ in range(10):
            batch = _drain(subscriber, timeout=2.0)
            received.extend(batch)
            if len(received) >= report.published:
                break

        ordering_keys = {m.message.ordering_key for m in received}
        assert len(ordering_keys) == report.distinct_vehicles

        # Distinct keys throughout, so the processor would write every one of
        # these rather than discarding clones as duplicates of each other.
        assert corpus.duplicate_key_count == 0
        assert len({m.message.attributes["idempotency_key"] for m in received}) == len(received)


class TestReportHonesty:
    def test_the_achieved_rate_counts_acknowledged_messages(
        self,
        corpus_archive: LocalSnapshotArchive,
        publisher: EventPublisher,
        subscriber: pubsub_v1.SubscriberClient,
    ) -> None:
        """Not submissions: the drain is inside the measured window, so the
        last batch cannot be published for free."""
        corpus = load_corpus(corpus_archive, agency=AGENCY)
        config = ReplayConfig(
            rate_per_second=30, duration=timedelta(seconds=1), batch_size=5, workers=2
        )

        report = ReplayHarness(config, corpus, publisher=publisher).run()

        assert report.published == report.batches * report.batch_size - report.failed
        assert report.achieved_rate == pytest.approx(
            report.published / report.duration_seconds, rel=1e-6
        )

    def test_publish_latency_is_measured_against_the_real_broker(
        self,
        corpus_archive: LocalSnapshotArchive,
        publisher: EventPublisher,
        subscriber: pubsub_v1.SubscriberClient,
    ) -> None:
        corpus = load_corpus(corpus_archive, agency=AGENCY)
        config = ReplayConfig(
            rate_per_second=30, duration=timedelta(seconds=1), batch_size=5, workers=1
        )

        report = ReplayHarness(config, corpus, publisher=publisher).run()

        assert set(report.publish_latency_ms) == {"p50", "p95", "p99"}
        assert report.publish_latency_ms["p50"] > 0.0
        assert report.publish_latency_ms["p99"] >= report.publish_latency_ms["p50"]

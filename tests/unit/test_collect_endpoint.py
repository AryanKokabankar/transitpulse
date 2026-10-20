"""Tests for the collector's ``POST /collect`` handler.

The feed client, publisher and archive are substituted through the module's
dependency accessors, so the handler's own decisions are what gets tested:
what it archives and when, what status code a given upstream failure produces,
and whether a tick that decoded nothing usable is still reported as a success.

``TestClient`` is used without its context manager on purpose. Entering it
would run the lifespan, which builds a real ``EventPublisher`` and so needs a
broker -- and these tests have to pass in CI with nothing running.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from services.collector import main as collector_main
from services.collector.feed import (
    FeedHTTPError,
    FeedResponse,
    FeedTimeoutError,
)
from tests.gtfs_fixtures import (
    FeedSpec,
    TripUpdateEntitySpec,
    VehicleEntitySpec,
    build_feed,
    build_vehicle_feed,
)
from transitpulse_core.models import QuarantineReason, VehiclePing
from transitpulse_core.resilience import CircuitOpenError
from transitpulse_core.snapshots import LocalSnapshotArchive

AGENCY = "mta-nyct"


class StubFeedClient:
    """Returns a scripted payload, or raises a scripted failure."""

    def __init__(self, payload: bytes | None = None, error: Exception | None = None) -> None:
        self._payload = payload
        self._error = error
        self.calls = 0

    async def fetch(self) -> FeedResponse:
        self.calls += 1
        if self._error is not None:
            raise self._error
        assert self._payload is not None
        return FeedResponse(
            payload=self._payload,
            fetched_at=datetime(2026, 10, 15, 14, 30, 5, tzinfo=UTC),
            duration_seconds=0.123,
            attempts=1,
        )


class StubPublisher:
    """Records what it was asked to publish, and can refuse some of it."""

    def __init__(self, *, accept: int | None = None) -> None:
        self.batches: list[list[VehiclePing]] = []
        self._accept = accept

    def publish_batch(self, pings: list[VehiclePing]) -> int:
        batch = list(pings)
        self.batches.append(batch)
        return len(batch) if self._accept is None else min(self._accept, len(batch))

    @property
    def published(self) -> list[VehiclePing]:
        return [ping for batch in self.batches for ping in batch]


@pytest.fixture
def archive(tmp_path: Path) -> LocalSnapshotArchive:
    return LocalSnapshotArchive(tmp_path)


@pytest.fixture
def client() -> Iterator[TestClient]:
    # Not `with TestClient(...)`: entering it runs the lifespan, which needs a
    # real broker.
    yield TestClient(collector_main.app)
    collector_main.app.dependency_overrides.clear()


def wire(
    feed_client: StubFeedClient,
    publisher: StubPublisher,
    archive: LocalSnapshotArchive,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Point the handler's module-level accessors at the stubs."""
    monkeypatch.setattr(collector_main, "_feed_client", feed_client)
    monkeypatch.setattr(collector_main, "_publisher", publisher)
    monkeypatch.setattr(collector_main, "_archive", archive)


class TestSuccessfulTick:
    def test_publishes_every_valid_ping(
        self,
        client: TestClient,
        archive: LocalSnapshotArchive,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        feed = StubFeedClient(
            build_vehicle_feed(
                VehicleEntitySpec(entity_id="1", vehicle_id="bus-1"),
                VehicleEntitySpec(entity_id="2", vehicle_id="bus-2"),
            )
        )
        publisher = StubPublisher()
        wire(feed, publisher, archive, monkeypatch)

        response = client.post("/collect")

        assert response.status_code == 200
        body = response.json()
        assert body["published"] == 2
        assert body["quarantined"] == 0
        assert body["entity_count"] == 2
        assert body["attempts"] == 1
        assert {ping.vehicle_id for ping in publisher.published} == {"bus-1", "bus-2"}

    def test_uses_the_existing_ordered_publisher_once_per_tick(
        self,
        client: TestClient,
        archive: LocalSnapshotArchive,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """One batch per tick, so the publisher's own batching and ordering
        guarantees apply rather than being re-implemented here."""
        feed = StubFeedClient(
            build_vehicle_feed(
                *[VehicleEntitySpec(entity_id=str(i), vehicle_id=f"bus-{i}") for i in range(5)]
            )
        )
        publisher = StubPublisher()
        wire(feed, publisher, archive, monkeypatch)

        client.post("/collect")

        assert len(publisher.batches) == 1
        assert len(publisher.batches[0]) == 5

    def test_reports_the_non_vehicle_entity_split(
        self,
        client: TestClient,
        archive: LocalSnapshotArchive,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        feed = StubFeedClient(
            build_feed(
                FeedSpec(
                    entities=[
                        VehicleEntitySpec(entity_id="1", vehicle_id="bus-1"),
                        TripUpdateEntitySpec(entity_id="2"),
                    ]
                )
            )
        )
        wire(feed, StubPublisher(), archive, monkeypatch)

        body = client.post("/collect").json()
        assert body["entity_count"] == 2
        assert body["vehicle_entity_count"] == 1
        assert body["published"] == 1


class TestSnapshotArchiving:
    def test_archives_the_unmodified_bytes(
        self,
        client: TestClient,
        archive: LocalSnapshotArchive,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        payload = build_vehicle_feed(VehicleEntitySpec())
        wire(StubFeedClient(payload), StubPublisher(), archive, monkeypatch)

        body = client.post("/collect").json()

        assert archive.load_payload(body["snapshot_path"]) == payload
        assert body["payload_bytes"] == len(payload)

    def test_writes_the_metadata_record(
        self,
        client: TestClient,
        archive: LocalSnapshotArchive,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        payload = build_vehicle_feed(
            VehicleEntitySpec(entity_id="1", vehicle_id="bus-1"),
            VehicleEntitySpec(entity_id="2", vehicle_id="bus-2"),
        )
        wire(StubFeedClient(payload), StubPublisher(), archive, monkeypatch)

        body = client.post("/collect").json()
        snapshot = archive.load_metadata(body["snapshot_path"])

        assert snapshot is not None
        assert snapshot.entity_count == 2
        assert snapshot.payload_bytes == len(payload)
        assert snapshot.content_sha256 == snapshot.sha256_of(payload)

    def test_archives_a_payload_the_decoder_cannot_read(
        self,
        client: TestClient,
        archive: LocalSnapshotArchive,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The whole reason archiving precedes decoding: the payload that
        breaks the decoder is the one worth having, and it exists in memory
        only during this tick."""
        garbage = b"<html><body>502 Bad Gateway</body></html>"
        wire(StubFeedClient(garbage), StubPublisher(), archive, monkeypatch)

        response = client.post("/collect")

        assert response.status_code == 502
        stored = archive.list_payloads()
        assert len(stored) == 1
        assert archive.load_payload(stored[0]) == garbage

    def test_records_the_decode_failure_beside_the_payload(
        self,
        client: TestClient,
        archive: LocalSnapshotArchive,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        wire(StubFeedClient(b"not protobuf"), StubPublisher(), archive, monkeypatch)

        client.post("/collect")

        sidecar = next(archive.root.rglob("*.quarantine.json"))
        assert QuarantineReason.DECODE_FAILURE in sidecar.read_text(encoding="utf-8")

    def test_nothing_is_published_when_the_feed_cannot_be_decoded(
        self,
        client: TestClient,
        archive: LocalSnapshotArchive,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        publisher = StubPublisher()
        wire(StubFeedClient(b"not protobuf"), publisher, archive, monkeypatch)

        client.post("/collect")

        assert publisher.batches == []


class TestQuarantineRouting:
    def test_bad_entities_are_kept_and_good_ones_still_publish(
        self,
        client: TestClient,
        archive: LocalSnapshotArchive,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """One bad vehicle must not cost the other nine hundred."""
        feed = StubFeedClient(
            build_vehicle_feed(
                VehicleEntitySpec(entity_id="1", vehicle_id="good-1"),
                VehicleEntitySpec(entity_id="2", vehicle_id="null", latitude=0.0, longitude=0.0),
                VehicleEntitySpec(entity_id="3", vehicle_id="bad-bearing", bearing=400.0),
            )
        )
        publisher = StubPublisher()
        wire(feed, publisher, archive, monkeypatch)

        body = client.post("/collect").json()

        assert body["published"] == 1
        assert body["quarantined"] == 2
        assert [ping.vehicle_id for ping in publisher.published] == ["good-1"]

    def test_the_sidecar_names_each_reason(
        self,
        client: TestClient,
        archive: LocalSnapshotArchive,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        feed = StubFeedClient(
            build_vehicle_feed(
                VehicleEntitySpec(entity_id="1", vehicle_id="good-1"),
                VehicleEntitySpec(entity_id="2", vehicle_id="null", latitude=0.0, longitude=0.0),
                VehicleEntitySpec(entity_id="3", vehicle_id="no-pos", include_position=False),
            )
        )
        wire(feed, StubPublisher(), archive, monkeypatch)

        body = client.post("/collect").json()
        sidecar = archive.root / body["snapshot_path"].replace(".pb", ".quarantine.json")
        contents = sidecar.read_text(encoding="utf-8")

        assert QuarantineReason.NULL_ISLAND in contents
        assert QuarantineReason.SCHEMA_VALIDATION in contents
        assert "no-pos" in contents

    def test_no_sidecar_is_written_for_a_clean_tick(
        self,
        client: TestClient,
        archive: LocalSnapshotArchive,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        wire(
            StubFeedClient(build_vehicle_feed(VehicleEntitySpec())),
            StubPublisher(),
            archive,
            monkeypatch,
        )

        client.post("/collect")

        assert list(archive.root.rglob("*.quarantine.json")) == []

    def test_a_tick_with_nothing_publishable_is_reported_distinctly(
        self,
        client: TestClient,
        archive: LocalSnapshotArchive,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A feed that parsed but contained nothing valid is a successful fetch
        of a broken feed. Retrying would just fetch the same broken payload, so
        it is reported rather than failed."""
        feed = StubFeedClient(
            build_vehicle_feed(
                VehicleEntitySpec(entity_id="1", vehicle_id="a", latitude=0.0, longitude=0.0),
                VehicleEntitySpec(entity_id="2", vehicle_id="b", bearing=999.0),
            )
        )
        wire(feed, StubPublisher(), archive, monkeypatch)

        response = client.post("/collect")

        assert response.status_code == 207
        assert response.json()["published"] == 0
        assert response.json()["quarantined"] == 2

    def test_an_empty_feed_is_an_ordinary_success(
        self,
        client: TestClient,
        archive: LocalSnapshotArchive,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # An agency publishing no vehicles at 03:00 is not a fault.
        wire(
            StubFeedClient(build_feed(FeedSpec(entities=[]))),
            StubPublisher(),
            archive,
            monkeypatch,
        )

        response = client.post("/collect")
        assert response.status_code == 200
        assert response.json()["published"] == 0


class TestUpstreamFailures:
    def test_a_timeout_becomes_502(
        self,
        client: TestClient,
        archive: LocalSnapshotArchive,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Cloud Scheduler retries 5xx, which is where that policy belongs."""
        wire(
            StubFeedClient(error=FeedTimeoutError("timed out after 10.0s")),
            StubPublisher(),
            archive,
            monkeypatch,
        )

        response = client.post("/collect")

        assert response.status_code == 502
        assert "upstream feed unavailable" in response.json()["detail"]

    def test_a_permanent_http_error_becomes_502(
        self,
        client: TestClient,
        archive: LocalSnapshotArchive,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        wire(
            StubFeedClient(error=FeedHTTPError("feed returned HTTP 404", status_code=404)),
            StubPublisher(),
            archive,
            monkeypatch,
        )
        assert client.post("/collect").status_code == 502

    def test_nothing_is_archived_when_the_fetch_failed(
        self,
        client: TestClient,
        archive: LocalSnapshotArchive,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        wire(
            StubFeedClient(error=FeedTimeoutError("timed out")),
            StubPublisher(),
            archive,
            monkeypatch,
        )

        client.post("/collect")

        assert archive.list_payloads() == []

    def test_an_open_circuit_becomes_503_with_retry_after(
        self,
        client: TestClient,
        archive: LocalSnapshotArchive,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The cooldown is communicated rather than left for the caller to
        guess at."""
        wire(
            StubFeedClient(error=CircuitOpenError("gtfs-feed", 42.5)),
            StubPublisher(),
            archive,
            monkeypatch,
        )

        response = client.post("/collect")

        assert response.status_code == 503
        assert response.headers["retry-after"] == "42"

    def test_retry_after_is_never_zero(
        self,
        client: TestClient,
        archive: LocalSnapshotArchive,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # A Retry-After of 0 invites an immediate retry, which is the opposite
        # of what an open breaker is asking for.
        wire(
            StubFeedClient(error=CircuitOpenError("gtfs-feed", 0.4)),
            StubPublisher(),
            archive,
            monkeypatch,
        )

        assert client.post("/collect").headers["retry-after"] == "1"


class TestPartialPublish:
    def test_reports_only_what_the_broker_accepted(
        self,
        client: TestClient,
        archive: LocalSnapshotArchive,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Reporting submissions rather than acks is how a pipeline looks
        lossless while losing messages."""
        feed = StubFeedClient(
            build_vehicle_feed(
                *[VehicleEntitySpec(entity_id=str(i), vehicle_id=f"bus-{i}") for i in range(4)]
            )
        )
        wire(feed, StubPublisher(accept=2), archive, monkeypatch)

        assert client.post("/collect").json()["published"] == 2


class TestHealthEndpoints:
    def test_liveness_needs_no_dependencies(self, client: TestClient) -> None:
        # Touching the feed here would make a brief agency outage restart every
        # healthy instance at once.
        assert client.get("/healthz").status_code == 200

    def test_readiness_does_not_depend_on_the_upstream_feed(self, client: TestClient) -> None:
        """The collector is still worth keeping in rotation while the agency is
        down: pulling it would not fix the feed and would hide the breaker."""
        assert client.get("/readyz").json()["ready"] is True

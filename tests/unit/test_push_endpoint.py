"""Tests for the processor's push handler, and above all its status codes.

Pub/Sub acknowledges a pushed message on a 2xx and redelivers on anything
else, so the status code *is* the durability decision and both ways of getting
it wrong are silent. A 200 on a transient database error drops the event
permanently while the dashboards stay green; a 5xx on a permanently malformed
one spends five deliveries and a dead-letter slot rediscovering that it is
still malformed. Neither shows up as an error anywhere, which is why the whole
decision table is asserted here rather than left to integration coverage.

``TestClient`` is used without its context manager on purpose: entering it
would run the lifespan, which opens a real connection pool, and these tests
have to pass in CI with nothing running.
"""

from __future__ import annotations

import base64
from collections.abc import Iterator
from datetime import timedelta
from typing import Any

import orjson
import pytest
from fastapi.testclient import TestClient

from services.processor import enrich as enrich_module
from services.processor import main as processor_main
from tests.conftest import MakePing
from transitpulse_core.models import (
    EnrichedVehicleEvent,
    QuarantineReason,
    QuarantineRecord,
    VehiclePing,
)
from transitpulse_core.sinks import WriteResult


class StubSink:
    """A Postgres sink that records writes and can be told to fail."""

    name = "postgres"

    def __init__(self, *, write_error: str | None = None, quarantine_error: bool = False) -> None:
        self.batches: list[list[EnrichedVehicleEvent]] = []
        self.quarantined: list[QuarantineRecord] = []
        self._write_error = write_error
        self._quarantine_error = quarantine_error

    def write_batch(self, events: list[EnrichedVehicleEvent]) -> WriteResult:
        self.batches.append(list(events))
        if self._write_error is not None:
            # Mirrors the real sink, which catches and reports rather than
            # raising -- a WriteResult with failures, not an exception.
            return WriteResult(sink=self.name, failed=len(events), errors=[self._write_error])
        return WriteResult(sink=self.name, written=len(events))

    def write_quarantine(self, records: Any) -> int:
        if self._quarantine_error:
            raise RuntimeError("quarantine table is unreachable")
        self.quarantined.extend(records)
        return len(records)

    def healthcheck(self) -> bool:
        return True

    @property
    def written(self) -> list[EnrichedVehicleEvent]:
        return [event for batch in self.batches for event in batch]


class StubBigQuery:
    name = "bigquery"

    def __init__(self, *, write_error: str | None = None) -> None:
        self.batches: list[list[EnrichedVehicleEvent]] = []
        self._write_error = write_error

    def write_batch(self, events: list[EnrichedVehicleEvent]) -> WriteResult:
        self.batches.append(list(events))
        if self._write_error is not None:
            return WriteResult(sink=self.name, failed=len(events), errors=[self._write_error])
        return WriteResult(sink=self.name, written=len(events))


class StubEnricher:
    """Enriches without touching a database, or raises a scripted failure."""

    def __init__(self, error: Exception | None = None) -> None:
        self._error = error
        self.calls: list[VehiclePing] = []

    def enrich(self, ping: VehiclePing) -> EnrichedVehicleEvent:
        self.calls.append(ping)
        if self._error is not None:
            raise self._error
        return EnrichedVehicleEvent(
            ping=ping,
            idempotency_key=ping.idempotency_key,
            computed_speed_mps=11.1,
            processed_time=ping.ingest_time + timedelta(milliseconds=90),
        )


@pytest.fixture
def client() -> Iterator[TestClient]:
    # Not `with TestClient(...)`: entering it runs the lifespan, which opens a
    # real connection pool.
    yield TestClient(processor_main.app, raise_server_exceptions=False)
    processor_main.app.dependency_overrides.clear()


def wire(
    monkeypatch: pytest.MonkeyPatch,
    *,
    sink: StubSink | None = None,
    enricher: StubEnricher | None = None,
    bigquery: StubBigQuery | None = None,
) -> tuple[StubSink, StubEnricher]:
    """Point the handler's module-level accessors at the stubs."""
    resolved_sink = sink or StubSink()
    resolved_enricher = enricher or StubEnricher()
    monkeypatch.setattr(processor_main, "_sink", resolved_sink)
    monkeypatch.setattr(processor_main, "_enricher", resolved_enricher)
    monkeypatch.setattr(processor_main, "_bigquery", bigquery)
    return resolved_sink, resolved_enricher


def push_body(
    ping: VehiclePing | None = None,
    *,
    data: bytes | None = None,
    attributes: dict[str, str] | None = None,
    message_id: str = "msg-1",
    delivery_attempt: int = 1,
) -> dict[str, Any]:
    """Build a Pub/Sub push envelope, using the wire field names."""
    payload = data if data is not None else (ping.model_dump_json().encode() if ping else b"{}")
    return {
        "message": {
            "data": base64.b64encode(payload).decode(),
            "attributes": attributes or {"agency": "mta-nyct", "schema_version": "1"},
            "messageId": message_id,
            "publishTime": "2026-10-15T14:30:01.000Z",
        },
        "subscription": "projects/transitpulse-local/subscriptions/vehicle-events-processor",
        "deliveryAttempt": delivery_attempt,
    }


class TestAcknowledgedAndWritten:
    def test_a_valid_message_is_written_and_acked(
        self, client: TestClient, make_ping: MakePing, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        sink, enricher = wire(monkeypatch)
        ping = make_ping(vehicle_id="bus-1")

        response = client.post("/", json=push_body(ping))

        assert response.status_code == 200
        body = response.json()
        assert body["outcome"] == "processed"
        assert body["message_id"] == "msg-1"
        assert [p.vehicle_id for p in enricher.calls] == ["bus-1"]
        assert [e.ping.vehicle_id for e in sink.written] == ["bus-1"]
        assert not sink.quarantined

    def test_the_response_reports_the_lag_it_measured(
        self, client: TestClient, make_ping: MakePing, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The SLI, returned in the body so a hand-driven message during an
        incident shows the lag without a metrics query."""
        wire(monkeypatch)

        body = client.post("/", json=push_body(make_ping())).json()

        assert body["lag_seconds"] == pytest.approx(0.54, abs=0.01)

    def test_one_event_is_written_per_request(
        self, client: TestClient, make_ping: MakePing, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A push subscription delivers one message per request, so there is
        no batch to build. Accumulating across requests would leave
        acknowledged events in a buffer that dies with the instance."""
        sink, _ = wire(monkeypatch)

        for index in range(3):
            client.post("/", json=push_body(make_ping(vehicle_id=f"bus-{index}")))

        assert [len(batch) for batch in sink.batches] == [1, 1, 1]


class TestQuarantinedAndAcked:
    def test_a_body_that_is_not_a_push_envelope_is_acked(
        self, client: TestClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Retrying cannot change the shape of a request, so a 5xx here would
        buy five deliveries and a dead-letter slot for nothing."""
        sink, _ = wire(monkeypatch)

        response = client.post("/", content=b"this is not a push envelope")

        assert response.status_code == 200
        assert response.json()["reason"] == str(QuarantineReason.DECODE_FAILURE)
        assert [r.reason for r in sink.quarantined] == [QuarantineReason.DECODE_FAILURE]

    def test_the_undecodable_body_itself_is_kept(
        self, client: TestClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        sink, _ = wire(monkeypatch)

        client.post("/", content=b"this is not a push envelope")

        assert sink.quarantined[0].raw_payload == b"this is not a push envelope"

    def test_a_payload_failing_the_contract_is_quarantined_and_acked(
        self, client: TestClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        sink, enricher = wire(monkeypatch)
        body = push_body(data=orjson.dumps({"agency": "mta-nyct", "latitude": 999.0}))

        response = client.post("/", json=body)

        assert response.status_code == 200
        assert response.json()["reason"] == str(QuarantineReason.SCHEMA_VALIDATION)
        assert not enricher.calls, "an invalid payload must not reach enrichment"
        assert [r.reason for r in sink.quarantined] == [QuarantineReason.SCHEMA_VALIDATION]

    def test_a_quarantine_row_carries_what_it_takes_to_diagnose_it(
        self, client: TestClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Payload, message id and delivery attempt together are what turn a
        rejection into something replayable rather than a counter."""
        sink, _ = wire(monkeypatch)
        payload = orjson.dumps({"agency": "mta-nyct", "latitude": 999.0})

        client.post("/", json=push_body(data=payload, message_id="msg-42", delivery_attempt=3))

        record = sink.quarantined[0]
        assert record.raw_payload == payload
        assert record.message_id == "msg-42"
        assert record.delivery_attempt == 3
        assert "latitude" in record.detail

    def test_an_impossible_speed_is_quarantined_and_acked(
        self, client: TestClient, make_ping: MakePing, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A bad coordinate will be just as bad on the next delivery, and
        every retry of it is a retry that a real backlog is not getting."""
        enricher = StubEnricher(
            enrich_module.ImplausibleSpeedError(speed=1112.0, distance_m=11120.0, seconds=10.0)
        )
        sink, _ = wire(monkeypatch, enricher=enricher)

        response = client.post("/", json=push_body(make_ping()))

        assert response.status_code == 200
        assert response.json()["reason"] == str(QuarantineReason.IMPOSSIBLE_SPEED)
        assert [r.reason for r in sink.quarantined] == [QuarantineReason.IMPOSSIBLE_SPEED]
        assert "1112.0 m/s" in sink.quarantined[0].detail

    def test_a_quarantined_event_is_not_written_to_the_sink(
        self, client: TestClient, make_ping: MakePing, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        enricher = StubEnricher(
            enrich_module.ImplausibleSpeedError(speed=1112.0, distance_m=11120.0, seconds=10.0)
        )
        sink, _ = wire(monkeypatch, enricher=enricher)

        client.post("/", json=push_body(make_ping()))

        assert sink.batches == []


class TestTransientFailureIsRetried:
    def test_a_sink_failure_nacks_while_attempts_remain(
        self, client: TestClient, make_ping: MakePing, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The database being down is the case a retry exists for; acking here
        would drop the event and leave the pipeline looking healthy."""
        sink, _ = wire(monkeypatch, sink=StubSink(write_error="connection refused"))

        response = client.post("/", json=push_body(make_ping(), delivery_attempt=1))

        assert response.status_code == 503
        assert "connection refused" in response.json()["detail"]
        assert not sink.quarantined, "a retryable failure must not be quarantined yet"

    def test_an_enrichment_failure_nacks(
        self, client: TestClient, make_ping: MakePing, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Enrichment reads Postgres, so its failures are the same outage."""
        enricher = StubEnricher(RuntimeError("pool timeout"))
        sink, _ = wire(monkeypatch, enricher=enricher)

        response = client.post("/", json=push_body(make_ping(), delivery_attempt=2))

        assert response.status_code == 503
        assert not sink.quarantined

    def test_a_bigquery_failure_nacks_even_after_postgres_succeeded(
        self, client: TestClient, make_ping: MakePing, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The dual write is not atomic (ADR-002). Retrying the whole thing is
        safe because both sinks are idempotent on the same key (ADR-005): the
        redelivery's Postgres write collapses to a no-op."""
        bigquery = StubBigQuery(write_error="AppendRows deadline exceeded")
        sink, _ = wire(monkeypatch, bigquery=bigquery)

        response = client.post("/", json=push_body(make_ping(), delivery_attempt=1))

        assert response.status_code == 503
        assert len(sink.written) == 1
        assert len(bigquery.batches) == 1

    def test_bigquery_is_skipped_when_disabled(
        self, client: TestClient, make_ping: MakePing, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Local development has no BigQuery emulator, so the local pipeline
        writes to Postgres only (ADR-002)."""
        sink, _ = wire(monkeypatch, bigquery=None)

        response = client.post("/", json=push_body(make_ping()))

        assert response.status_code == 200
        assert len(sink.written) == 1


class TestPoisonMessage:
    def test_the_final_attempt_quarantines_instead_of_nacking(
        self, client: TestClient, make_ping: MakePing, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Otherwise the same message is redelivered forever, and a permanent
        failure consumes the delivery capacity of a real backlog."""
        sink, _ = wire(monkeypatch, sink=StubSink(write_error="constraint violation"))

        response = client.post("/", json=push_body(make_ping(), delivery_attempt=5))

        assert response.status_code == 200
        assert response.json()["reason"] == str(QuarantineReason.SINK_WRITE_FAILURE)
        assert [r.reason for r in sink.quarantined] == [QuarantineReason.SINK_WRITE_FAILURE]
        assert "5 delivery attempts" in sink.quarantined[0].detail

    @pytest.mark.parametrize("attempt", [1, 2, 3, 4])
    def test_earlier_attempts_still_nack(
        self,
        client: TestClient,
        make_ping: MakePing,
        monkeypatch: pytest.MonkeyPatch,
        attempt: int,
    ) -> None:
        wire(monkeypatch, sink=StubSink(write_error="connection refused"))
        response = client.post("/", json=push_body(make_ping(), delivery_attempt=attempt))
        assert response.status_code == 503

    def test_a_late_attempt_that_succeeds_is_not_quarantined(
        self, client: TestClient, make_ping: MakePing, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Why the attempt count is checked on the failure path and not at the
        top: a message on its fifth delivery is usually a good event whose
        first four attempts hit a database that was down. Discarding it unread
        would turn a recovered outage into permanent data loss.
        """
        sink, _ = wire(monkeypatch)

        response = client.post("/", json=push_body(make_ping(), delivery_attempt=5))

        assert response.status_code == 200
        assert response.json()["outcome"] == "processed"
        assert len(sink.written) == 1
        assert not sink.quarantined


class TestQuarantineWriteFailure:
    def test_a_failed_quarantine_write_nacks_rather_than_acking(
        self, client: TestClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Acking a rejection that was never recorded loses the event entirely
        -- the one outcome the quarantine table exists to prevent. The 503
        hands it back, and the dead-letter policy holds it until the database
        is back.
        """
        wire(monkeypatch, sink=StubSink(quarantine_error=True))

        response = client.post("/", json=push_body(data=orjson.dumps({"agency": "x"})))

        assert response.status_code == 503
        assert "quarantine" in response.json()["detail"]

    def test_it_also_nacks_an_undecodable_body(
        self, client: TestClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        wire(monkeypatch, sink=StubSink(quarantine_error=True))
        assert client.post("/", content=b"not an envelope").status_code == 503

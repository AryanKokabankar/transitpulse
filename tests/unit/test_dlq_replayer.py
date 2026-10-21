"""Tests for the dead-letter replayer's verdicts, with no broker in sight.

What matters here is not that the job can pull a message -- the integration
test covers that against the real emulator -- but that each verdict reaches the
right *acknowledgement*, because an acknowledgement is irreversible. The
dead-letter topic holds the last copy of these bytes, so a message acked
without being republished is data destroyed by the tool whose entire purpose is
to recover it. Every test below is ultimately about that one decision.

The stub subscriber records acks and returns rather than performing them, which
is what lets those decisions be asserted directly instead of inferred from
whatever the broker happens to hold afterwards.
"""

from __future__ import annotations

from concurrent.futures import Future
from datetime import UTC, datetime
from typing import Any

import pytest

from services.jobs.dlq_replayer import (
    SOURCE_DELIVERY_COUNT,
    SOURCE_SUBSCRIPTION,
    DeadLetter,
    Outcome,
    _replay_batch,
    revalidate,
)
from transitpulse_core.models import VehiclePing

SUBSCRIPTION = "projects/transitpulse-local/subscriptions/vehicle-events-dlq-replayer"
TRACEPARENT = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"


def make_ping(vehicle_id: str = "bus-1", **overrides: object) -> VehiclePing:
    now = datetime(2026, 10, 20, 14, 30, tzinfo=UTC)
    defaults: dict[str, object] = {
        "agency": "mbta",
        "vehicle_id": vehicle_id,
        "latitude": 42.3601,
        "longitude": -71.0589,
        "event_time": now,
        "ingest_time": now,
    }
    defaults.update(overrides)
    return VehiclePing(**defaults)  # type: ignore[arg-type]


def dead_letter(payload: bytes, *, message_id: str = "1", **attributes: str) -> DeadLetter:
    return DeadLetter(
        ack_id=f"ack-{message_id}",
        message_id=message_id,
        payload=payload,
        attributes={
            SOURCE_DELIVERY_COUNT: "5",
            SOURCE_SUBSCRIPTION: "vehicle-events-processor",
            **attributes,
        },
        ordering_key="bus-1",
    )


class StubSubscriber:
    """Records what the job would ack and what it would hand back."""

    def __init__(self) -> None:
        self.acked: list[str] = []
        self.returned: list[str] = []

    def acknowledge(self, *, request: dict[str, Any]) -> None:
        self.acked.extend(request["ack_ids"])

    def modify_ack_deadline(self, *, request: dict[str, Any]) -> None:
        assert request["ack_deadline_seconds"] == 0, "a return must be immediate"
        self.returned.extend(request["ack_ids"])


class StubPublisher:
    """Accepts or refuses each publish, so both ack paths are reachable."""

    def __init__(self, *, fail: bool = False) -> None:
        self.published: list[VehiclePing] = []
        self._fail = fail

    def publish_one(self, ping: VehiclePing) -> Future[str]:
        future: Future[str] = Future()
        if self._fail:
            future.set_exception(RuntimeError("broker refused the publish"))
        else:
            self.published.append(ping)
            future.set_result("message-id")
        return future


def replay(
    *messages: DeadLetter, publisher: StubPublisher | None = None
) -> tuple[Outcome, StubSubscriber, StubPublisher | None]:
    outcome = Outcome(dry_run=publisher is None)
    subscriber = StubSubscriber()
    _replay_batch(
        messages,
        outcome,
        subscriber=subscriber,  # type: ignore[arg-type]
        subscription=SUBSCRIPTION,
        publisher=publisher,  # type: ignore[arg-type]
        replayed_payloads=[],
    )
    return outcome, subscriber, publisher


class TestRevalidation:
    def test_a_payload_that_now_passes_returns_the_ping(self) -> None:
        ping = make_ping()
        verdict = revalidate(dead_letter(ping.model_dump_json().encode()))
        assert isinstance(verdict, VehiclePing)
        assert verdict.vehicle_id == "bus-1"

    def test_a_payload_that_still_breaches_the_contract_returns_a_reason(self) -> None:
        """The reason names the failing field, because the useful output of a
        replay run is "47 still fail on bearing", not 47 tracebacks."""
        payload = make_ping().model_dump_json().replace('"bearing":null', '"bearing":400.0')
        verdict = revalidate(dead_letter(payload.encode()))
        assert isinstance(verdict, str)
        assert "bearing" in verdict

    def test_a_payload_that_is_not_json_is_distinguished_from_one_that_is(self) -> None:
        """Different operational stories: a bad producer versus a contract
        change, and conflating them hides which one is happening."""
        assert revalidate(dead_letter(b"\x00\x01not json")) != revalidate(
            dead_letter(b'{"agency": "mbta"}')
        )

    def test_the_republished_ping_is_byte_for_byte_the_same_observation(self) -> None:
        """Nothing is rebased on the way through.

        The idempotency key is derived from event_time (ADR-005), so a shifted
        timestamp would write a second row for an observation already held --
        and would report the replay's latency instead of the outage's.
        """
        ping = make_ping()
        verdict = revalidate(dead_letter(ping.model_dump_json().encode()))
        assert isinstance(verdict, VehiclePing)
        assert verdict.event_time == ping.event_time
        assert verdict.ingest_time == ping.ingest_time
        assert verdict.idempotency_key == ping.idempotency_key


class TestProvenance:
    def test_reads_the_source_delivery_count_the_broker_stamped(self) -> None:
        assert dead_letter(b"{}").source_delivery_count == 5

    def test_a_missing_or_malformed_delivery_count_is_none_not_zero(self) -> None:
        """Zero would read as "never delivered", which is the opposite of what
        an absent attribute means."""
        for value in ("", "lots", "5.0"):
            message = dead_letter(b"{}", **{SOURCE_DELIVERY_COUNT: value})
            assert message.source_delivery_count is None

    def test_recovers_the_original_trace_id_from_the_surviving_traceparent(self) -> None:
        message = dead_letter(b"{}", traceparent=TRACEPARENT)
        assert message.source_trace_id == "0af7651916cd43dd8448eb211c80319c"

    def test_a_traceparent_that_is_not_one_yields_no_trace_id(self) -> None:
        for value in ("", "garbage", "00-tooshort-b7ad6b7169203331-01", "00-abc"):
            assert dead_letter(b"{}", traceparent=value).source_trace_id is None

    def test_no_traceparent_at_all_is_not_an_error(self) -> None:
        assert dead_letter(b"{}").source_trace_id is None


class TestAcknowledgement:
    def test_a_republished_message_is_acked(self) -> None:
        ping = make_ping()
        publisher = StubPublisher()
        outcome, subscriber, _ = replay(
            dead_letter(ping.model_dump_json().encode()), publisher=publisher
        )
        assert outcome.republished == 1
        assert publisher.published == [ping]
        assert subscriber.acked == ["ack-1"]
        assert subscriber.returned == []

    def test_a_payload_that_is_still_invalid_is_never_acked(self) -> None:
        """The dead-letter topic is the last copy. Clearing what cannot be
        repaired would destroy the evidence this job exists to preserve."""
        outcome, subscriber, _ = replay(
            dead_letter(b'{"agency": "mbta"}'), publisher=StubPublisher()
        )
        assert outcome.still_invalid == 1
        assert outcome.republished == 0
        assert subscriber.acked == []
        assert subscriber.returned == ["ack-1"]

    def test_a_failed_republish_is_never_acked(self) -> None:
        """Acking here would lose the message to a broker hiccup."""
        ping = make_ping()
        outcome, subscriber, _ = replay(
            dead_letter(ping.model_dump_json().encode()),
            publisher=StubPublisher(fail=True),
        )
        assert outcome.failed == 1
        assert outcome.republished == 0
        assert subscriber.acked == []
        assert subscriber.returned == ["ack-1"]

    def test_one_bad_message_does_not_stop_the_rest_of_the_batch(self) -> None:
        ping = make_ping("bus-2")
        publisher = StubPublisher()
        outcome, subscriber, _ = replay(
            dead_letter(b"not json", message_id="1"),
            dead_letter(ping.model_dump_json().encode(), message_id="2"),
            publisher=publisher,
        )
        assert (outcome.republished, outcome.still_invalid) == (1, 1)
        assert subscriber.acked == ["ack-2"]
        assert subscriber.returned == ["ack-1"]

    def test_a_dry_run_acks_nothing_and_publishes_nothing(self) -> None:
        ping = make_ping()
        outcome, subscriber, publisher = replay(dead_letter(ping.model_dump_json().encode()))
        assert publisher is None
        assert outcome.republished == 1
        assert subscriber.acked == []
        assert subscriber.returned == ["ack-1"]

    def test_reasons_are_tallied_rather_than_listed_one_per_message(self) -> None:
        outcome, _, _ = replay(
            dead_letter(b'{"agency": "mbta"}', message_id="1"),
            dead_letter(b'{"agency": "mbta"}', message_id="2"),
            publisher=StubPublisher(),
        )
        assert sum(outcome.invalid_reasons.values()) == 2
        assert len(outcome.invalid_reasons) == 1


class TestReport:
    def test_the_dry_run_report_never_claims_to_have_published(self) -> None:
        rendered = Outcome(pulled=3, republished=3, dry_run=True).render()
        assert "would republish" in rendered
        assert "dry run" in rendered

    def test_a_real_run_reports_what_it_published(self) -> None:
        rendered = Outcome(pulled=3, republished=3).render()
        assert "republished" in rendered
        assert "would republish" not in rendered

    @pytest.mark.parametrize(
        "field_name",
        ["pulled", "republished", "still_invalid", "quarantine_rows_marked"],
    )
    def test_every_count_survives_json_serialisation(self, field_name: str) -> None:
        """--json is what a runbook or a CI gate reads, so a count that only
        exists in the rendered text is a count nothing can act on."""
        assert field_name in Outcome().as_dict()

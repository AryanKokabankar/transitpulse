"""Integration tests for the dead-letter replayer against the real emulator.

The unit tests pin the acknowledgement decisions. These cover the half only a
broker can answer, and it is the half that was wrong on the first attempt:

* that a message nacked past ``max_delivery_attempts`` genuinely arrives on the
  dead-letter topic -- the emulator honours the policy, which was worth
  establishing rather than assuming, because a stubbed broker would have
  dead-lettered obligingly whatever the real one did;
* that the payload and the ordering key survive the broker's move while the
  message id does not, which is why the quarantine reconciliation matches on
  the payload;
* that a dry run leaves the subscription exactly as it found it, and that a
  second real run finds nothing -- the first version of the pull loop never
  terminated here, because a message it handed back was on the subscription
  again before the next pull.

Run with: python tasks.py up && python tasks.py test-integration
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import Iterator
from datetime import timedelta
from typing import Any

import psycopg
import pytest
from google.cloud import pubsub_v1

from services.jobs.dlq_replayer import DeadLetter, run
from tests.conftest import MakePing
from tools.pushbridge import bridge
from transitpulse_core.config import Settings, get_settings
from transitpulse_core.models import VehiclePing
from transitpulse_core.pubsub import EventPublisher, ensure_local_topology

pytestmark = pytest.mark.integration

# Its own agency, so every count in this module is of rows these tests created
# and nothing else running against the local stack can perturb them.
AGENCY = "dlq-replayer-test"

# A pull with nothing to return waits out its whole deadline, and this module
# makes a lot of deliberately empty pulls. Three seconds is long enough for the
# emulator to answer and short enough that the drains do not dominate the
# suite -- at the 15s this started with, the file took nine minutes.
PULL_TIMEOUT = 3.0


@pytest.fixture(scope="module", autouse=True)
def _require_emulator() -> None:
    if not os.environ.get("PUBSUB_EMULATOR_HOST"):
        pytest.skip("PUBSUB_EMULATOR_HOST is unset; run `python tasks.py up` first")


@pytest.fixture(scope="module")
def settings() -> Settings:
    try:
        ensure_local_topology(get_settings())
    except Exception as exc:  # noqa: BLE001 - surfaced as a skip, not a failure
        pytest.skip(f"Pub/Sub emulator is not reachable: {exc}")
    return get_settings()


@pytest.fixture
def subscriber() -> Iterator[pubsub_v1.SubscriberClient]:
    client = pubsub_v1.SubscriberClient()
    yield client
    client.close()


@pytest.fixture(autouse=True)
def _drain(settings: Settings, subscriber: pubsub_v1.SubscriberClient) -> Iterator[None]:
    """Empty both subscriptions around every test.

    Acked rather than handed back: a returned message is immediately available
    again, so a drain that nacked would leave the next test looking at it.
    """

    def drain() -> None:
        for name in (settings.subscription_processor, settings.subscription_dlq):
            path = subscriber.subscription_path(settings.gcp_project, name)
            # Two consecutive empty pulls, not one. A message another test
            # handed back can become available again a moment after the first
            # empty pull, and every ping in this module serialises to the same
            # bytes -- conftest pins one event time -- so a straggler is
            # indistinguishable from the next test's own message.
            empty = 0
            while empty < 2:
                pulled = subscriber.pull(
                    request={"subscription": path, "max_messages": 1000},
                    timeout=PULL_TIMEOUT,
                )
                if not pulled.received_messages:
                    empty += 1
                    continue
                empty = 0
                subscriber.acknowledge(
                    request={
                        "subscription": path,
                        "ack_ids": [m.ack_id for m in pulled.received_messages],
                    }
                )

    drain()
    yield
    # And again afterwards. Draining only on the way in looked like a saving
    # and was a flake: a test that leaves messages on the dead-letter
    # subscription hands them to whichever test runs next, which then counts
    # them as its own.
    drain()


@pytest.fixture
def conn(settings: Settings) -> Iterator[psycopg.Connection]:
    try:
        connection = psycopg.connect(settings.pg_dsn_str, autocommit=True, connect_timeout=5)
    except psycopg.Error:
        pytest.skip("PostgreSQL is not reachable; run `python tasks.py up` first")
    yield connection
    connection.close()


@pytest.fixture(autouse=True)
def _clean_rows(conn: psycopg.Connection) -> Iterator[None]:
    def clean() -> None:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM quarantine WHERE agency = %s", (AGENCY,))
            cur.execute("DELETE FROM vehicle_events WHERE agency = %s", (AGENCY,))
            cur.execute("DELETE FROM vehicle_live WHERE agency = %s", (AGENCY,))

    clean()
    yield
    clean()


def dead_letter(
    settings: Settings,
    subscriber: pubsub_v1.SubscriberClient,
    *,
    ping: VehiclePing | None = None,
    raw: bytes | None = None,
) -> bytes:
    """Publish, then exhaust the delivery budget so the broker moves it.

    Returns the exact bytes that were published, which is what the quarantine
    reconciliation has to match on.
    """
    if ping is not None:
        publisher = EventPublisher(settings)
        try:
            assert publisher.publish_batch([ping]) == 1
        finally:
            publisher.close()
        payload = ping.model_dump_json().encode("utf-8")
    else:
        assert raw is not None
        client = pubsub_v1.PublisherClient()
        topic = client.topic_path(settings.gcp_project, settings.topic_vehicle_events)
        client.publish(topic, data=raw).result(timeout=30)
        payload = raw

    path = subscriber.subscription_path(settings.gcp_project, settings.subscription_processor)
    for _ in range(settings.max_delivery_attempts + 1):
        pulled = subscriber.pull(
            request={"subscription": path, "max_messages": 10}, timeout=PULL_TIMEOUT
        )
        if not pulled.received_messages:
            return payload
        subscriber.modify_ack_deadline(
            request={
                "subscription": path,
                "ack_ids": [m.ack_id for m in pulled.received_messages],
                "ack_deadline_seconds": 0,
            }
        )
    pytest.fail("the message was still being delivered after its attempts were exhausted")


def _subscription_message_ids(
    settings: Settings, subscriber: pubsub_v1.SubscriberClient
) -> set[str]:
    """Ids currently on the processor subscription, handed straight back."""
    path = subscriber.subscription_path(settings.gcp_project, settings.subscription_processor)
    pulled = subscriber.pull(
        request={"subscription": path, "max_messages": 100}, timeout=PULL_TIMEOUT
    )
    if not pulled.received_messages:
        return set()
    subscriber.modify_ack_deadline(
        request={
            "subscription": path,
            "ack_ids": [m.ack_id for m in pulled.received_messages],
            "ack_deadline_seconds": 0,
        }
    )
    return {m.message.message_id for m in pulled.received_messages}


def _exhaust_delivery_budget(settings: Settings, subscriber: pubsub_v1.SubscriberClient) -> None:
    path = subscriber.subscription_path(settings.gcp_project, settings.subscription_processor)
    for _ in range(settings.max_delivery_attempts + 1):
        pulled = subscriber.pull(
            request={"subscription": path, "max_messages": 10}, timeout=PULL_TIMEOUT
        )
        if not pulled.received_messages:
            return
        subscriber.modify_ack_deadline(
            request={
                "subscription": path,
                "ack_ids": [m.ack_id for m in pulled.received_messages],
                "ack_deadline_seconds": 0,
            }
        )


def _only_dlq_message(settings: Settings, subscriber: pubsub_v1.SubscriberClient) -> Any:
    """Read the one dead letter, and acknowledge it on the way out.

    The ack is not tidiness. A pulled-and-unacked message stays leased for the
    subscription's whole ack deadline, during which the drain between tests
    cannot see it -- so it silently reappears a minute later, inside whichever
    test is running then, and every ping in this module serialises to the same
    bytes. That produced a failure three tests away from its cause.
    """
    path = subscriber.subscription_path(settings.gcp_project, settings.subscription_dlq)
    pulled = subscriber.pull(
        request={"subscription": path, "max_messages": 10}, timeout=PULL_TIMEOUT
    )
    assert len(pulled.received_messages) == 1, "expected exactly one dead letter"
    received = pulled.received_messages[0]
    subscriber.acknowledge(request={"subscription": path, "ack_ids": [received.ack_id]})
    return received.message


class TestDeadLetterPolicy:
    def test_the_broker_moves_an_exhausted_message_to_the_dlq(
        self, settings: Settings, subscriber: pubsub_v1.SubscriberClient, make_ping: MakePing
    ) -> None:
        """The premise everything else in this module rests on."""
        dead_letter(settings, subscriber, ping=make_ping(agency=AGENCY))
        outcome = run(settings, dry_run=True)
        assert outcome.pulled == 1

    def test_the_payload_and_the_publishers_attributes_survive_the_move(
        self, settings: Settings, subscriber: pubsub_v1.SubscriberClient, make_ping: MakePing
    ) -> None:
        """What the replayer is allowed to rely on after the broker has moved a
        message, established rather than assumed."""
        ping = make_ping(agency=AGENCY)
        published = dead_letter(settings, subscriber, ping=ping)

        moved = _only_dlq_message(settings, subscriber)
        assert bytes(moved.data) == published
        assert moved.ordering_key == ping.vehicle_id
        assert moved.attributes["idempotency_key"] == ping.idempotency_key
        assert moved.attributes["agency"] == AGENCY
        assert moved.attributes["schema_version"] == "1"

    def test_the_message_id_does_not_survive_the_move(
        self, settings: Settings, subscriber: pubsub_v1.SubscriberClient, make_ping: MakePing
    ) -> None:
        """Why the quarantine reconciliation matches on bytes.

        ``message_id`` is the field that looks like it should work: the
        quarantine row records the id of the delivery that failed. But the
        broker republishes to the dead-letter topic, which mints a new id, and
        Pub/Sub documents no attribute carrying the original -- confirmed here
        by the original id appearing nowhere on the moved message.
        """
        ping = make_ping(agency=AGENCY)
        original_ids = _subscription_message_ids(settings, subscriber)
        dead_letter(settings, subscriber, ping=ping)

        moved = _only_dlq_message(settings, subscriber)
        assert moved.message_id not in original_ids
        assert not [key for key, value in moved.attributes.items() if value in original_ids], (
            "an attribute carrying the source message id would be the better key"
        )

    def test_a_traceparent_survives_so_the_original_trace_stays_findable(
        self, settings: Settings, subscriber: pubsub_v1.SubscriberClient
    ) -> None:
        """The mechanism ``DeadLetter.source_trace_id`` depends on.

        Published with the raw client and an explicit traceparent, because this
        suite runs with telemetry disabled: ``EventPublisher`` injects whatever
        the active span context holds, which with no tracer provider is
        nothing. The attribute's survival is the broker's behaviour and is what
        this asserts; whether one was set in the first place is the
        publisher's.
        """
        traceparent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
        client = pubsub_v1.PublisherClient()
        topic = client.topic_path(settings.gcp_project, settings.topic_vehicle_events)
        client.publish(topic, data=b'{"not": "a ping"}', traceparent=traceparent).result(timeout=30)
        _exhaust_delivery_budget(settings, subscriber)

        moved = _only_dlq_message(settings, subscriber)
        assert moved.attributes["traceparent"] == traceparent
        recovered = DeadLetter(
            ack_id="",
            message_id=moved.message_id,
            payload=bytes(moved.data),
            attributes=dict(moved.attributes),
            ordering_key=moved.ordering_key,
        )
        assert recovered.source_trace_id == "0af7651916cd43dd8448eb211c80319c"


class TestReplay:
    def test_a_valid_event_is_republished_and_lands_in_postgres(
        self,
        settings: Settings,
        subscriber: pubsub_v1.SubscriberClient,
        conn: psycopg.Connection,
        make_ping: MakePing,
    ) -> None:
        """The whole point: an event lost to an outage is recovered by running
        this, and ends up as a row."""
        ping = make_ping(agency=AGENCY)
        dead_letter(settings, subscriber, ping=ping)

        outcome = run(settings)
        assert (outcome.republished, outcome.still_invalid, outcome.failed) == (1, 0, 0)

        delivered = asyncio.run(
            bridge(
                settings,
                duration=timedelta(seconds=60),
                max_messages=1,
                concurrency=1,
                url=None,
            )
        )
        assert delivered.processed == 1

        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM vehicle_events WHERE agency = %s AND idempotency_key = %s",
                (AGENCY, ping.idempotency_key),
            )
            row = cur.fetchone()
        assert row is not None and row[0] == 1

    def test_a_dry_run_leaves_the_message_where_it_was(
        self, settings: Settings, subscriber: pubsub_v1.SubscriberClient, make_ping: MakePing
    ) -> None:
        dead_letter(settings, subscriber, ping=make_ping(agency=AGENCY))

        first = run(settings, dry_run=True)
        assert (first.pulled, first.republished) == (1, 1)

        second = run(settings, dry_run=True)
        assert second.pulled == 1, "a dry run must not consume what it reports"

    def test_a_real_run_drains_the_subscription(
        self, settings: Settings, subscriber: pubsub_v1.SubscriberClient, make_ping: MakePing
    ) -> None:
        """Idempotent in the sense that matters: running it twice does not
        republish twice."""
        dead_letter(settings, subscriber, ping=make_ping(agency=AGENCY))

        # At least one rather than exactly one: the property being tested is
        # that a second run finds nothing left, and pinning the first count
        # makes the test fail on a straggler from a neighbour instead of on
        # the behaviour it exists to check.
        assert run(settings).republished >= 1
        assert run(settings).republished == 0

    def test_a_payload_that_still_fails_is_left_on_the_subscription(
        self, settings: Settings, subscriber: pubsub_v1.SubscriberClient
    ) -> None:
        """A replayer that cleared what it could not repair would be the last
        thing to touch those bytes."""
        dead_letter(settings, subscriber, raw=b'{"agency": "' + AGENCY.encode() + b'"}')

        outcome = run(settings)
        assert (outcome.republished, outcome.still_invalid) == (0, 1)
        assert run(settings, dry_run=True).pulled == 1

    def test_the_loop_terminates_when_nothing_can_be_replayed(
        self, settings: Settings, subscriber: pubsub_v1.SubscriberClient
    ) -> None:
        """The regression guard for the pull loop.

        Every message here is handed straight back, so without the seen-set the
        job pulls its own nacks until something kills it. pytest's own timeout
        would not catch that -- there is no timeout configured -- so the
        assertion is on the count, which can only be right if the loop ended.
        """
        for index in range(3):
            dead_letter(settings, subscriber, raw=f'{{"n": {index}}}'.encode())

        outcome = run(settings)
        assert outcome.pulled == 3
        assert outcome.still_invalid == 3


class TestQuarantineReconciliation:
    def test_marks_the_row_holding_the_same_payload(
        self,
        settings: Settings,
        subscriber: pubsub_v1.SubscriberClient,
        conn: psycopg.Connection,
        make_ping: MakePing,
    ) -> None:
        ping = make_ping(agency=AGENCY)
        payload = dead_letter(settings, subscriber, ping=ping)
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO quarantine (reason, detail, agency, vehicle_id, raw_payload)
                VALUES ('SINK_WRITE_FAILURE', 'quarantined by a test', %s, %s, %s)
                """,
                (AGENCY, ping.vehicle_id, payload),
            )

        outcome = run(settings)
        assert outcome.republished == 1
        assert outcome.quarantine_rows_marked == 1
        assert outcome.payloads_without_quarantine_row == 0

        with conn.cursor() as cur:
            cur.execute("SELECT replayed_at FROM quarantine WHERE agency = %s", (AGENCY,))
            row = cur.fetchone()
        assert row is not None and row[0] is not None

    def test_marks_every_row_one_repeatedly_failing_message_left(
        self,
        settings: Settings,
        subscriber: pubsub_v1.SubscriberClient,
        conn: psycopg.Connection,
        make_ping: MakePing,
    ) -> None:
        """Quarantine writes are deliberately not idempotent (ADR-005), so one
        payload can own several rows. Counting rows and counting unmatched
        payloads separately is what keeps the second number from going
        negative here."""
        ping = make_ping(agency=AGENCY)
        payload = dead_letter(settings, subscriber, ping=ping)
        with conn.cursor() as cur:
            for attempt in (4, 5):
                cur.execute(
                    """
                    INSERT INTO quarantine
                        (reason, detail, agency, vehicle_id, raw_payload, delivery_attempt)
                    VALUES ('SINK_WRITE_FAILURE', 'attempt', %s, %s, %s, %s)
                    """,
                    (AGENCY, ping.vehicle_id, payload, attempt),
                )

        outcome = run(settings)
        assert outcome.quarantine_rows_marked == 2
        assert outcome.payloads_without_quarantine_row == 0

    def test_reports_a_republished_payload_that_owns_no_row(
        self, settings: Settings, subscriber: pubsub_v1.SubscriberClient, make_ping: MakePing
    ) -> None:
        """The common case, not an error: the processor's poison-message guard
        acks after writing a quarantine row, so a message that reached the
        dead-letter topic is usually one whose quarantine write also failed."""
        dead_letter(settings, subscriber, ping=make_ping(agency=AGENCY))

        outcome = run(settings)
        assert outcome.republished == 1
        assert outcome.quarantine_rows_marked == 0
        assert outcome.payloads_without_quarantine_row == 1

    def test_an_already_replayed_row_is_not_marked_twice(
        self,
        settings: Settings,
        subscriber: pubsub_v1.SubscriberClient,
        conn: psycopg.Connection,
        make_ping: MakePing,
    ) -> None:
        ping = make_ping(agency=AGENCY)
        payload = dead_letter(settings, subscriber, ping=ping)
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO quarantine
                    (reason, detail, agency, vehicle_id, raw_payload, replayed_at)
                VALUES ('SINK_WRITE_FAILURE', 'already done', %s, %s, %s, now())
                """,
                (AGENCY, ping.vehicle_id, payload),
            )

        outcome = run(settings)
        assert outcome.quarantine_rows_marked == 0
        assert outcome.payloads_without_quarantine_row == 1

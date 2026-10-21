"""Tests for the parts of the chaos tooling that decide what a run concluded.

No Docker and no broker. What is pinned down here is the machinery that turns a
run into a claim -- the injection that has to damage a known number of
entities, the load builder whose keys have to be unique for "zero loss" to be
a reachable assertion, the ladder verdict that is the difference between a
ceiling and a floor, and the screenshot check that stops a picture of an error
message being committed as evidence.

Every one of these is a place where a bug would produce a passing experiment
that measured nothing, which is the failure mode worth paying for tests
against.
"""

from __future__ import annotations

import struct
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from google.transit import gtfs_realtime_pb2 as gtfs

from services.collector.feed import decode_feed
from tests.gtfs_fixtures import FIXED_EPOCH, VehicleEntitySpec, build_vehicle_feed
from tools.chaos.experiment import (
    Check,
    ScenarioResult,
    at_least,
    at_most,
    equals,
    holds,
)
from tools.chaos.malformed_feed import inject
from tools.chaos.ramp import Step, _ladder_verdict
from tools.chaos.support import _dsn_label, build_load, distinct_pings
from tools.screenshots import PLACEHOLDER_SIZE, png_dimensions
from transitpulse_core.config import Settings
from transitpulse_core.models import QuarantineReason, VehiclePing
from transitpulse_core.snapshots import LocalSnapshotArchive

AGENCY = "chaos-test"
NOW = datetime(2026, 10, 20, 14, 30, tzinfo=UTC)


def ping(vehicle_id: str, event_time: datetime, agency: str = AGENCY) -> VehiclePing:
    return VehiclePing(
        agency=agency,
        vehicle_id=vehicle_id,
        latitude=42.3601,
        longitude=-71.0589,
        event_time=event_time,
        ingest_time=event_time,
    )


class TestDistinctPings:
    def test_drops_a_repeat_of_the_same_observation(self) -> None:
        """An agency re-sending a ping it already sent is normal traffic, and
        it deduplicates at the sink -- so a zero-loss count that included it
        could never balance."""
        first = ping("bus-1", NOW)
        again = ping("bus-1", NOW)
        assert first.idempotency_key == again.idempotency_key
        assert distinct_pings([first, again]) == [first]

    def test_keeps_the_same_vehicle_at_a_different_time(self) -> None:
        kept = distinct_pings([ping("bus-1", NOW), ping("bus-1", NOW + timedelta(seconds=30))])
        assert len(kept) == 2

    def test_keeps_different_vehicles_at_the_same_time(self) -> None:
        assert len(distinct_pings([ping("bus-1", NOW), ping("bus-2", NOW)])) == 2

    def test_preserves_order(self) -> None:
        pings = [ping(f"bus-{i}", NOW) for i in range(5)]
        assert distinct_pings(pings) == pings


class TestBuildLoad:
    """The property every zero-loss assertion rests on: one key per message."""

    @pytest.fixture
    def settings(self, tmp_path: Path) -> Settings:
        archive = LocalSnapshotArchive(tmp_path)
        archive.put_payload(
            build_vehicle_feed(
                *[
                    VehicleEntitySpec(
                        entity_id=str(i),
                        vehicle_id=f"bus-{i}",
                        timestamp=FIXED_EPOCH - i * 10,
                    )
                    for i in range(6)
                ]
            ),
            agency="mbta",
            fetched_at=NOW,
        )
        # Twice, so the corpus genuinely contains repeats of its own
        # observations the way a multi-snapshot capture does.
        archive.put_payload(
            build_vehicle_feed(
                *[
                    VehicleEntitySpec(
                        entity_id=str(i),
                        vehicle_id=f"bus-{i}",
                        timestamp=FIXED_EPOCH - i * 10,
                    )
                    for i in range(6)
                ]
            ),
            agency="mbta",
            fetched_at=NOW + timedelta(seconds=15),
        )
        return Settings(
            snapshot_backend="local",
            snapshot_local_dir=str(tmp_path),
            feed_agency="mbta",
        )

    def test_every_message_has_its_own_idempotency_key(self, settings: Settings) -> None:
        load = build_load(settings, agency=AGENCY, count=24)
        assert load.size == 24
        assert len({p.idempotency_key for p in load.pings}) == 24

    def test_keys_stay_unique_past_the_corpus_size(self, settings: Settings) -> None:
        """Cycling the corpus must re-identify, not re-time: a second pass that
        reused identities would deduplicate and a zero-loss count would fail on
        a pipeline that lost nothing."""
        load = build_load(settings, agency=AGENCY, count=30)
        assert len({p.idempotency_key for p in load.pings}) == 30

    def test_nothing_is_dated_into_the_future(self, settings: Settings) -> None:
        """A future event_time feeds negative values into the lag histogram the
        SLO is written against."""
        anchor = datetime.now(UTC)
        load = build_load(settings, agency=AGENCY, count=24, now=anchor)
        assert max(p.event_time for p in load.pings) <= anchor

    def test_the_run_owns_its_agency(self, settings: Settings) -> None:
        load = build_load(settings, agency=AGENCY, count=12)
        assert {p.agency for p in load.pings} == {AGENCY}

    def test_reports_the_corpus_spread_that_lands_inside_end_to_end_lag(
        self, settings: Settings
    ) -> None:
        load = build_load(settings, agency=AGENCY, count=12)
        assert load.event_time_span_seconds > 0
        assert load.corpus_distinct_keys <= load.corpus_pings


class TestInjection:
    def feed(self, count: int = 20) -> bytes:
        return build_vehicle_feed(
            *[VehicleEntitySpec(entity_id=str(i), vehicle_id=f"bus-{i}") for i in range(count)]
        )

    def valid_ids(self, payload: bytes) -> set[str]:
        return {p.vehicle_id for p in decode_feed(payload, agency=AGENCY, ingest_time=NOW).pings}

    def test_corrupts_exactly_every_nth_entity(self) -> None:
        payload = self.feed(20)
        result = inject(payload, valid_ids=self.valid_ids(payload), every=5, now=NOW)
        assert result.vehicle_entities == 20
        assert result.corrupted == 4

    def test_the_decoder_rejects_exactly_what_was_injected(self) -> None:
        """The arithmetic the whole experiment turns on, checked without a
        pipeline in the way."""
        payload = self.feed(20)
        result = inject(payload, valid_ids=self.valid_ids(payload), every=5, now=NOW)
        decoded = decode_feed(result.payload, agency=AGENCY, ingest_time=NOW)
        assert len(decoded.quarantined) == result.corrupted
        assert len(decoded.pings) == result.vehicle_entities - result.corrupted

    def test_nothing_is_unaccounted_for(self) -> None:
        payload = self.feed(17)
        result = inject(payload, valid_ids=self.valid_ids(payload), every=3, now=NOW)
        decoded = decode_feed(result.payload, agency=AGENCY, ingest_time=NOW)
        assert len(decoded.pings) + len(decoded.quarantined) == result.vehicle_entities

    def test_null_island_keeps_its_own_reason(self) -> None:
        """A GPS fault and a feed that breached the contract are different
        operational stories, and the quarantine metric is only useful if it can
        tell them apart."""
        payload = self.feed(20)
        result = inject(payload, valid_ids=self.valid_ids(payload), every=5, now=NOW)
        decoded = decode_feed(result.payload, agency=AGENCY, ingest_time=NOW)
        reasons = [r.reason for r in decoded.quarantined]
        assert reasons.count(QuarantineReason.NULL_ISLAND) == result.by_corruption["null_island"]

    def test_it_never_breaks_an_entity_that_was_already_being_rejected(self) -> None:
        """Corrupting one of those would change no count, and the experiment
        would pass having measured nothing."""
        payload = build_vehicle_feed(
            *[
                VehicleEntitySpec(
                    entity_id=str(i), vehicle_id=f"bus-{i}", latitude=0.0, longitude=0.0
                )
                if i % 2
                else VehicleEntitySpec(entity_id=str(i), vehicle_id=f"bus-{i}")
                for i in range(20)
            ]
        )
        valid = self.valid_ids(payload)
        baseline = len(decode_feed(payload, agency=AGENCY, ingest_time=NOW).quarantined)
        result = inject(payload, valid_ids=valid, every=2, now=NOW)
        decoded = decode_feed(result.payload, agency=AGENCY, ingest_time=NOW)
        assert len(decoded.quarantined) == baseline + result.corrupted

    def test_the_feed_is_shifted_onto_the_present(self) -> None:
        """Replaying a capture with its original dates fails on whichever day
        that partition is pruned."""
        payload = self.feed(10)
        anchor = datetime.now(UTC)
        result = inject(payload, valid_ids=self.valid_ids(payload), every=100, now=anchor)
        decoded = decode_feed(result.payload, agency=AGENCY, ingest_time=anchor)
        assert decoded.pings
        for emitted in decoded.pings:
            assert abs((anchor - emitted.event_time).total_seconds()) < 120

    def test_a_feed_with_no_valid_entities_corrupts_nothing(self) -> None:
        payload = self.feed(10)
        result = inject(payload, valid_ids=set(), every=2, now=NOW)
        assert result.corrupted == 0

    def test_the_corrupted_feed_still_parses_as_a_feed_message(self) -> None:
        """A body the decoder cannot parse at all would be a different
        experiment: it rejects the whole tick rather than per entity."""
        payload = self.feed(12)
        result = inject(payload, valid_ids=self.valid_ids(payload), every=3, now=NOW)
        message = gtfs.FeedMessage()
        message.ParseFromString(result.payload)
        assert len(message.entity) == 12


class TestLadderVerdict:
    def step(self, target: float, accuracy: float) -> Step:
        return Step(
            target=target,
            achieved=target * accuracy,
            accuracy=accuracy,
            publisher_bound=accuracy < 0.98,
            limiter_wait_seconds=0.0,
            published=int(target * accuracy),
            failed=0,
            publish_latency_ms={},
        )

    def test_a_ladder_that_broke_has_found_a_ceiling(self) -> None:
        broke, detail = _ladder_verdict([self.step(500, 1.0), self.step(1000, 0.4)], 0.95)
        assert broke
        assert "1,000" in detail

    def test_a_ladder_that_ran_out_of_rungs_has_found_a_floor(self) -> None:
        """The honesty check the ramp turns on: a top rung that still met its
        target is a floor, and quoting it as a ceiling would be the exact kind
        of unearned number this project exists not to publish."""
        broke, detail = _ladder_verdict([self.step(500, 1.0), self.step(1000, 1.0)], 0.95)
        assert not broke
        assert "floor" in detail

    def test_an_empty_ladder_concludes_nothing(self) -> None:
        broke, _ = _ladder_verdict([], 0.95)
        assert not broke

    def test_the_threshold_decides_the_verdict(self) -> None:
        steps = [self.step(1000, 0.96)]
        assert not _ladder_verdict(steps, 0.95)[0]
        assert _ladder_verdict(steps, 0.99)[0]


class TestChecks:
    def test_equals_reports_both_numbers(self) -> None:
        check = equals("zero loss", 2000, 1997)
        assert not check.passed
        assert "2000" in check.detail and "1997" in check.detail

    def test_at_least_and_at_most_are_inclusive(self) -> None:
        assert at_least("floor", 5, 5).passed
        assert at_most("ceiling", 5, 5).passed
        assert not at_least("floor", 5, 4).passed

    def test_a_result_fails_if_any_check_failed(self) -> None:
        result = ScenarioResult(scenario="x", question="?")
        result.check(equals("a", 1, 1), equals("b", 1, 2))
        assert not result.passed

    def test_a_result_with_no_checks_has_concluded_nothing_but_claims_nothing(self) -> None:
        assert ScenarioResult(scenario="x", question="?").passed

    def test_the_rendering_shows_every_check(self) -> None:
        result = ScenarioResult(scenario="x", question="does it?")
        result.check(equals("a", 1, 1), holds("b", False, "nope"))
        rendered = result.render()
        assert "PASS" in rendered and "FAIL" in rendered
        assert "CHECKS FAILED" in rendered

    def test_notes_survive_into_the_result_file(self) -> None:
        result = ScenarioResult(scenario="x", question="?")
        result.note("fsync is off")
        result.measure(rate=10.5)
        payload = result.as_dict()
        assert payload["notes"] == ["fsync is off"]
        assert payload["measurements"] == {"rate": 10.5}

    @pytest.mark.parametrize("passed", [True, False])
    def test_the_verdict_is_serialised_not_inferred(self, passed: bool) -> None:
        result = ScenarioResult(scenario="x", question="?")
        result.check(Check(name="c", passed=passed, detail=""))
        assert result.as_dict()["passed"] is passed


class TestScreenshotVerification:
    def png(self, width: int, height: int) -> bytes:
        return b"\x89PNG\r\n\x1a\n" + b"\x00" * 8 + struct.pack(">II", width, height)

    def test_reads_the_dimensions_out_of_the_header(self) -> None:
        assert png_dimensions(self.png(1400, 900)) == (1400, 900)

    def test_rejects_something_that_is_not_a_png(self) -> None:
        assert png_dimensions(b"<html>No image renderer</html>") is None

    def test_rejects_a_truncated_png(self) -> None:
        assert png_dimensions(b"\x89PNG\r\n\x1a\n") is None

    def test_the_placeholder_is_recognisable_by_size(self) -> None:
        """Grafana answers 200 with this when it has no renderer, whatever
        dimensions were asked for -- which is how a picture of an error message
        gets committed as evidence."""
        assert png_dimensions(self.png(*PLACEHOLDER_SIZE)) == PLACEHOLDER_SIZE


class TestDsnLabel:
    def test_records_the_host_without_the_password(self) -> None:
        """Result files are committed."""
        label = _dsn_label("postgresql://transitpulse:transitpulse@localhost:5433/transitpulse")
        assert label == "localhost:5433"
        assert "transitpulse:transitpulse" not in label

    def test_copes_with_a_dsn_carrying_no_credentials(self) -> None:
        assert _dsn_label("postgresql://localhost:5432/db") == "localhost:5432"

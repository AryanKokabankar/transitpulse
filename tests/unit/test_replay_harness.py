"""Tests for the replay harness: corpus loading, fan-out, and the report.

The assertions here are mostly about honesty rather than mechanics. A load
harness is the one tool in a project that is trusted to grade the project, so
the properties worth pinning down are the ones whose failure would produce a
plausible-looking number that is wrong: clones that are not really distinct
vehicles, timestamps that make every message dedupe, a reported rate taken
from submissions rather than acknowledgements, and a shortfall attributed to
the wrong component.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from tests.gtfs_fixtures import FIXED_EPOCH, VehicleEntitySpec, build_vehicle_feed
from tools.replay.corpus import EmptyCorpusError, LoadedCorpus, fan_out, load_corpus
from tools.replay.harness import ReplayConfig, ReplayHarness, ReplayReport
from transitpulse_core.models import VehiclePing
from transitpulse_core.snapshots import LocalSnapshotArchive

AGENCY = "mta-nyct"
FETCHED_AT = datetime(2026, 10, 15, 14, 30, 5, tzinfo=UTC)


@pytest.fixture
def archive(tmp_path: Path) -> LocalSnapshotArchive:
    return LocalSnapshotArchive(tmp_path)


def seed(
    archive: LocalSnapshotArchive,
    *specs: VehicleEntitySpec,
    fetched_at: datetime = FETCHED_AT,
) -> str:
    return archive.put_payload(build_vehicle_feed(*specs), agency=AGENCY, fetched_at=fetched_at)


def make_ping(vehicle_id: str = "bus-1", **overrides: object) -> VehiclePing:
    defaults: dict[str, object] = {
        "agency": AGENCY,
        "vehicle_id": vehicle_id,
        "latitude": 40.7128,
        "longitude": -74.0060,
        "event_time": FETCHED_AT,
        "ingest_time": FETCHED_AT,
    }
    defaults.update(overrides)
    return VehiclePing(**defaults)  # type: ignore[arg-type]


def corpus_of(*pings: VehiclePing) -> LoadedCorpus:
    times = [ping.event_time for ping in pings]
    return LoadedCorpus(
        pings=list(pings),
        snapshot_count=1,
        quarantined_count=0,
        event_time_span=max(times) - min(times),
        duplicate_key_count=len(pings) - len({p.idempotency_key for p in pings}),
    )


class TestCorpusLoading:
    def test_decodes_archived_snapshots_into_pings(self, archive: LocalSnapshotArchive) -> None:
        seed(
            archive,
            VehicleEntitySpec(entity_id="1", vehicle_id="bus-1"),
            VehicleEntitySpec(entity_id="2", vehicle_id="bus-2"),
        )

        corpus = load_corpus(archive, agency=AGENCY)

        assert corpus.size == 2
        assert corpus.snapshot_count == 1
        assert {ping.vehicle_id for ping in corpus.pings} == {"bus-1", "bus-2"}

    def test_reads_every_snapshot(self, archive: LocalSnapshotArchive) -> None:
        seed(archive, VehicleEntitySpec(vehicle_id="bus-1"), fetched_at=FETCHED_AT)
        seed(
            archive,
            VehicleEntitySpec(vehicle_id="bus-2"),
            fetched_at=FETCHED_AT + timedelta(seconds=15),
        )

        corpus = load_corpus(archive, agency=AGENCY)

        assert corpus.snapshot_count == 2
        assert corpus.size == 2

    def test_counts_garbage_instead_of_refusing_to_load(
        self, archive: LocalSnapshotArchive
    ) -> None:
        """A corpus containing garbage is a corpus doing its job; refusing it
        would make the harness unusable against real captures."""
        seed(
            archive,
            VehicleEntitySpec(entity_id="1", vehicle_id="good"),
            VehicleEntitySpec(entity_id="2", vehicle_id="bad", latitude=0.0, longitude=0.0),
        )

        corpus = load_corpus(archive, agency=AGENCY)

        assert corpus.size == 1
        assert corpus.quarantined_count == 1

    def test_an_empty_corpus_explains_how_to_fill_it(self, archive: LocalSnapshotArchive) -> None:
        with pytest.raises(EmptyCorpusError, match="capture"):
            load_corpus(archive, agency=AGENCY)

    def test_a_corpus_with_no_usable_pings_is_rejected(self, archive: LocalSnapshotArchive) -> None:
        # Pacing a run against zero messages would report a throughput of
        # nothing and look like a pipeline failure.
        seed(archive, VehicleEntitySpec(vehicle_id="bad", latitude=0.0, longitude=0.0))

        with pytest.raises(EmptyCorpusError, match="no valid pings"):
            load_corpus(archive, agency=AGENCY)

    def test_infers_the_agency_from_the_path(self, archive: LocalSnapshotArchive) -> None:
        """The archive layout puts the agency first, so a corpus is
        self-describing and can be replayed without being told what it is."""
        seed(archive, VehicleEntitySpec(vehicle_id="bus-1"))

        corpus = load_corpus(archive)

        assert corpus.pings[0].agency == AGENCY

    def test_reports_pings_that_share_an_idempotency_key(
        self, archive: LocalSnapshotArchive
    ) -> None:
        """Agencies re-send pings they have already sent, so a multi-tick
        corpus holds the same observation twice. Replaying it is faithful, but
        that share of a run deduplicates instead of being written -- and a
        throughput figure that quietly included it would overstate the work
        the pipeline actually did.
        """
        for tick in range(2):
            archive.put_payload(
                build_vehicle_feed(VehicleEntitySpec(vehicle_id="bus-1", timestamp=FIXED_EPOCH)),
                agency=AGENCY,
                fetched_at=FETCHED_AT + timedelta(seconds=15 * tick),
            )

        corpus = load_corpus(archive, agency=AGENCY)

        assert corpus.size == 2
        assert corpus.duplicate_key_count == 1
        assert corpus.duplicate_key_fraction == pytest.approx(0.5)

    def test_reports_no_duplicates_for_a_clean_corpus(self, archive: LocalSnapshotArchive) -> None:
        for tick in range(2):
            stamp = FETCHED_AT + timedelta(seconds=15 * tick)
            archive.put_payload(
                build_vehicle_feed(
                    VehicleEntitySpec(vehicle_id="bus-1", timestamp=int(stamp.timestamp()))
                ),
                agency=AGENCY,
                fetched_at=stamp,
            )

        corpus = load_corpus(archive, agency=AGENCY)

        assert corpus.duplicate_key_count == 0
        assert corpus.duplicate_key_fraction == 0.0

    def test_honours_a_snapshot_limit(self, archive: LocalSnapshotArchive) -> None:
        for index in range(4):
            seed(
                archive,
                VehicleEntitySpec(vehicle_id=f"bus-{index}"),
                fetched_at=FETCHED_AT + timedelta(seconds=15 * index),
            )

        assert load_corpus(archive, agency=AGENCY, limit=2).snapshot_count == 2


class TestFanOut:
    def test_a_factor_of_one_is_the_identity(self) -> None:
        pings = [make_ping("bus-1"), make_ping("bus-2")]
        assert fan_out(pings, 1) == pings

    def test_multiplies_the_corpus(self) -> None:
        expanded = fan_out([make_ping("bus-1"), make_ping("bus-2")], 5)
        assert len(expanded) == 10

    def test_clones_are_distinct_vehicles(self) -> None:
        """The property the measurement depends on.

        Clones sharing a vehicle id would share an ordering key and an
        idempotency key, so the run would measure the deduplication path while
        appearing to measure the write path.
        """
        expanded = fan_out([make_ping("bus-1")], 20)

        assert len({ping.vehicle_id for ping in expanded}) == 20
        assert len({ping.idempotency_key for ping in expanded}) == 20

    def test_the_original_ping_survives_untouched(self) -> None:
        original = make_ping("bus-1")
        expanded = fan_out([original], 4)
        assert expanded[0] == original

    def test_clones_are_moved_but_stay_plausible(self) -> None:
        original = make_ping("bus-1")
        clones = fan_out([original], 10)[1:]

        for clone in clones:
            assert clone.latitude != original.latitude
            assert abs(clone.latitude - original.latitude) < 0.02
            assert abs(clone.longitude - original.longitude) < 0.02

    def test_is_reproducible_for_a_given_seed(self) -> None:
        """A benchmark whose input changes between runs cannot detect a
        regression."""
        first = fan_out([make_ping("bus-1")], 8, seed=7)
        second = fan_out([make_ping("bus-1")], 8, seed=7)
        assert [p.latitude for p in first] == [p.latitude for p in second]

    def test_a_different_seed_moves_the_clones(self) -> None:
        first = fan_out([make_ping("bus-1")], 8, seed=1)
        second = fan_out([make_ping("bus-1")], 8, seed=2)
        assert [p.latitude for p in first] != [p.latitude for p in second]

    def test_a_long_vehicle_id_stays_within_the_contract(self) -> None:
        # Appending a suffix to a 128-character id would otherwise fail
        # validation partway through a run.
        expanded = fan_out([make_ping("v" * 128)], 3)
        assert all(len(ping.vehicle_id) <= 128 for ping in expanded)
        assert len({ping.vehicle_id for ping in expanded}) == 3

    def test_rejects_a_factor_below_one(self) -> None:
        with pytest.raises(ValueError, match="at least 1"):
            fan_out([make_ping()], 0)


class TestConfig:
    def test_expected_messages_is_rate_times_duration(self) -> None:
        config = ReplayConfig(rate_per_second=500, duration=timedelta(seconds=10))
        assert config.expected_messages == 5000

    def test_batch_size_is_derived_from_the_rate(self) -> None:
        """Sized so each pacing sleep is tens of milliseconds: finer than that
        and the limiter's accuracy is dominated by OS sleep granularity."""
        config = ReplayConfig(rate_per_second=500, duration=timedelta(seconds=1))
        assert config.resolved_batch_size() == 25

    def test_an_explicit_batch_size_wins(self) -> None:
        config = ReplayConfig(rate_per_second=500, duration=timedelta(seconds=1), batch_size=7)
        assert config.resolved_batch_size() == 7

    def test_a_tiny_rate_still_gets_one_message_per_batch(self) -> None:
        config = ReplayConfig(rate_per_second=2, duration=timedelta(seconds=1))
        assert config.resolved_batch_size() == 1


class TestDryRun:
    def test_paces_without_a_publisher(self) -> None:
        """Answers the first question anyone should ask of a load harness: is
        the limiter itself accurate, independent of the pipeline?"""
        config = ReplayConfig(rate_per_second=2000, duration=timedelta(seconds=0.5), dry_run=True)
        report = ReplayHarness(config, corpus_of(make_ping())).run()

        assert report.dry_run is True
        assert report.published > 0
        assert report.achieved_rate == pytest.approx(2000, rel=0.2)
        assert report.failed == 0

    def test_respects_the_duration(self) -> None:
        config = ReplayConfig(rate_per_second=1000, duration=timedelta(seconds=0.3), dry_run=True)
        report = ReplayHarness(config, corpus_of(make_ping())).run()

        assert report.duration_seconds == pytest.approx(0.3, abs=0.15)


class TestTimestampHandling:
    def harness(self, *pings: VehiclePing, **config: object) -> ReplayHarness:
        defaults: dict[str, object] = {
            "rate_per_second": 100,
            "duration": timedelta(seconds=0.2),
            "dry_run": True,
            "batch_size": len(pings) or 1,
        }
        defaults.update(config)
        return ReplayHarness(
            ReplayConfig(**defaults),  # type: ignore[arg-type]
            corpus_of(*pings),
        )

    def test_rebases_event_times_by_default(self) -> None:
        """A corpus replayed with its original times floods the lag SLI with
        values describing the corpus rather than the pipeline, and dedupes
        against rows an earlier run already wrote."""
        stale = make_ping("bus-1", event_time=datetime(2026, 10, 1, tzinfo=UTC))
        batch, _, _ = self.harness(stale)._next_batch(0, 0)

        assert batch[0].event_time > stale.event_time
        assert batch[0].ingest_time > stale.ingest_time

    def test_never_dates_an_event_into_the_future(self) -> None:
        """Regression test for a flaw in the first version of this harness.

        Cycles were disambiguated by shifting the window forward in time, which
        on the second pass produced event times later than the moment of
        ingestion -- a negative ingest lag, fed straight into the histogram the
        project's one SLO is written against.
        """
        pings = [make_ping("bus-1"), make_ping("bus-2")]
        harness = self.harness(*pings)

        cursor, cycle = 0, 0
        for _ in range(6):
            batch, cursor, cycle = harness._next_batch(cursor, cycle)
            for ping in batch:
                assert ping.ingest_lag_seconds >= 0.0

        assert cycle >= 2, "the corpus should have been cycled more than once"

    def test_preserves_event_times_when_asked(self) -> None:
        """True replay semantics, for reprocessing a window after a fix."""
        original = make_ping("bus-1", event_time=datetime(2026, 10, 1, tzinfo=UTC))
        batch, _, _ = self.harness(original, preserve_timestamps=True)._next_batch(0, 0)

        assert batch[0].event_time == original.event_time
        assert batch[0].idempotency_key == original.idempotency_key

    def test_successive_cycles_do_not_reuse_idempotency_keys(self) -> None:
        """A run longer than the corpus wraps around. Reusing the first pass's
        keys would make the rest of the run measure deduplication."""
        harness = self.harness(make_ping("bus-1"), make_ping("bus-2"))

        first, cursor, cycle = harness._next_batch(0, 0)
        second, _, _ = harness._next_batch(cursor, cycle)

        assert {p.idempotency_key for p in first}.isdisjoint({p.idempotency_key for p in second})

    def test_a_later_cycle_emits_fresh_vehicle_ids(self) -> None:
        """Identity, not time, is what distinguishes a repeat pass."""
        harness = self.harness(make_ping("bus-1"))

        first, cursor, cycle = harness._next_batch(0, 0)
        second, _, _ = harness._next_batch(cursor, cycle)

        assert first[0].vehicle_id == "bus-1"
        assert second[0].vehicle_id.startswith("bus-1#c")
        assert len(second[0].vehicle_id) <= 128

    def test_rebasing_keeps_relative_skew(self) -> None:
        """Shifted as a window, not flattened onto one instant: the spread
        between two vehicles' event times is real data the pipeline should
        see, and it drives the enrichment's speed calculation."""
        early = make_ping("bus-1", event_time=FETCHED_AT)
        late = make_ping("bus-2", event_time=FETCHED_AT + timedelta(seconds=9))

        batch, _, _ = self.harness(early, late)._next_batch(0, 0)

        assert batch[1].event_time - batch[0].event_time == timedelta(seconds=9)

    def test_the_newest_corpus_ping_lands_on_now(self) -> None:
        early = make_ping("bus-1", event_time=FETCHED_AT - timedelta(seconds=30))
        newest = make_ping("bus-2", event_time=FETCHED_AT)

        batch, _, _ = self.harness(early, newest)._next_batch(0, 0)

        assert batch[1].event_time == batch[1].ingest_time
        assert batch[0].ingest_lag_seconds == pytest.approx(30.0)


class TestReport:
    def report(self, **overrides: object) -> ReplayReport:
        defaults: dict[str, object] = {
            "target_rate": 500.0,
            "achieved_rate": 498.0,
            "duration_seconds": 10.0,
            "expected_messages": 5000,
            "published": 4980,
            "failed": 0,
            "batches": 200,
            "batch_size": 25,
            "workers": 4,
            "fanout": 1,
            "corpus_snapshots": 2,
            "corpus_pings": 800,
            "distinct_vehicles": 800,
            "corpus_cycles": 2,
            "corpus_duplicate_keys": 0,
            "limiter_wait_seconds": 9.4,
        }
        defaults.update(overrides)
        return ReplayReport(**defaults)  # type: ignore[arg-type]

    def test_accuracy_is_achieved_over_target(self) -> None:
        assert self.report().rate_accuracy == pytest.approx(0.996)

    def test_a_paced_run_is_not_called_publisher_bound(self) -> None:
        assert self.report().publisher_bound is False

    def test_a_shortfall_with_no_waiting_is_attributed_to_the_publisher(self) -> None:
        """The distinction the whole report exists to make: a limiter that
        barely waited was never the constraint."""
        report = self.report(achieved_rate=120.0, published=1200, limiter_wait_seconds=0.1)
        assert report.publisher_bound is True

    def test_a_shortfall_while_the_limiter_waited_is_not_the_publishers(self) -> None:
        report = self.report(achieved_rate=120.0, published=1200, limiter_wait_seconds=8.0)
        assert report.publisher_bound is False

    def test_renders_both_target_and_achieved(self) -> None:
        # Never the achieved rate alone: a single number with no target next to
        # it cannot be checked.
        rendered = self.report().render()
        assert "target rate" in rendered
        assert "achieved rate" in rendered
        assert "500.0 msg/s" in rendered

    def test_renders_the_saturation_note(self) -> None:
        rendered = self.report(
            achieved_rate=120.0, published=1200, limiter_wait_seconds=0.1
        ).render()
        assert "publisher could not keep up" in rendered

    def test_serialises_for_a_regression_gate(self) -> None:
        """A later phase fails a build on these numbers, so they have to be
        machine-readable without parsing the rendered text."""
        payload = self.report().as_dict()
        assert payload["achieved_rate"] == 498.0
        assert payload["rate_accuracy"] == 0.996
        assert payload["publisher_bound"] is False
        assert "generated_at" in payload

    def test_reports_failures_separately_from_successes(self) -> None:
        rendered = self.report(failed=20).render()
        assert "failed" in rendered

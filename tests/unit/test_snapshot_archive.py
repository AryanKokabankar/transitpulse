"""Tests for the replay corpus archive.

The local backend is exercised directly against a temporary directory. Its
object paths are asserted rather than treated as opaque, because the GCS
backend derives its blob names from the same function: if the layout drifts,
a corpus captured in the cloud stops being readable by a harness pointed at
disk, and the "local and cloud differ only by config" claim quietly stops
being true.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from transitpulse_core.config import Settings
from transitpulse_core.models import FeedSnapshot, QuarantineReason, QuarantineRecord
from transitpulse_core.snapshots import (
    GcsSnapshotArchive,
    LocalSnapshotArchive,
    SnapshotArchive,
    build_snapshot_archive,
    snapshot_object_path,
)

AGENCY = "mta-nyct"
FETCHED_AT = datetime(2026, 10, 15, 14, 30, 5, tzinfo=UTC)
PAYLOAD = b"\x0a\x03\x32\x2e\x30\x80\x81\xfe"  # not valid UTF-8, on purpose


@pytest.fixture
def archive(tmp_path: Path) -> LocalSnapshotArchive:
    return LocalSnapshotArchive(tmp_path)


class TestObjectPath:
    def test_is_date_hierarchical(self) -> None:
        """Replaying one day must list one prefix, not scan the corpus."""
        path = snapshot_object_path(AGENCY, FETCHED_AT, "abc123def456789")
        assert path.startswith(f"{AGENCY}/2026/10/15/")
        assert "143005" in path

    def test_includes_a_content_hash(self) -> None:
        path = snapshot_object_path(AGENCY, FETCHED_AT, "abc123def456789")
        assert path.endswith("abc123def456")

    def test_is_normalised_to_utc(self) -> None:
        # A corpus whose folders followed the collector's local timezone would
        # interleave two days' snapshots after a deploy to another region.
        from datetime import timedelta, timezone

        tokyo = FETCHED_AT.astimezone(timezone(timedelta(hours=9)))
        assert snapshot_object_path(AGENCY, tokyo, "x" * 16) == snapshot_object_path(
            AGENCY, FETCHED_AT, "x" * 16
        )


class TestPayloadStorage:
    def test_round_trips_the_exact_bytes(self, archive: LocalSnapshotArchive) -> None:
        """Byte-for-byte: the corpus is worthless if it is not what arrived."""
        path = archive.put_payload(PAYLOAD, agency=AGENCY, fetched_at=FETCHED_AT)
        assert archive.load_payload(path) == PAYLOAD

    def test_creates_the_directory_tree(self, archive: LocalSnapshotArchive) -> None:
        path = archive.put_payload(PAYLOAD, agency=AGENCY, fetched_at=FETCHED_AT)
        assert (archive.root / path).is_file()

    def test_uses_the_pb_suffix(self, archive: LocalSnapshotArchive) -> None:
        path = archive.put_payload(PAYLOAD, agency=AGENCY, fetched_at=FETCHED_AT)
        assert path.endswith(".pb")

    def test_the_same_bytes_land_on_the_same_path(self, archive: LocalSnapshotArchive) -> None:
        """Re-archiving a payload overwrites rather than accumulating a
        near-duplicate that would be replayed twice."""
        first = archive.put_payload(PAYLOAD, agency=AGENCY, fetched_at=FETCHED_AT)
        second = archive.put_payload(PAYLOAD, agency=AGENCY, fetched_at=FETCHED_AT)
        assert first == second
        assert len(archive.list_payloads()) == 1

    def test_different_bytes_land_on_different_paths(self, archive: LocalSnapshotArchive) -> None:
        first = archive.put_payload(PAYLOAD, agency=AGENCY, fetched_at=FETCHED_AT)
        second = archive.put_payload(b"different", agency=AGENCY, fetched_at=FETCHED_AT)
        assert first != second
        assert len(archive.list_payloads()) == 2


class TestMetadata:
    def snapshot(self, path: str) -> FeedSnapshot:
        return FeedSnapshot(
            agency=AGENCY,
            fetched_at=FETCHED_AT,
            feed_timestamp=FETCHED_AT,
            entity_count=3,
            payload_bytes=len(PAYLOAD),
            object_path=path,
            content_sha256=FeedSnapshot.sha256_of(PAYLOAD),
        )

    def test_round_trips(self, archive: LocalSnapshotArchive) -> None:
        path = archive.put_payload(PAYLOAD, agency=AGENCY, fetched_at=FETCHED_AT)
        archive.put_metadata(self.snapshot(path))

        loaded = archive.load_metadata(path)
        assert loaded is not None
        assert loaded.entity_count == 3
        assert loaded.content_sha256 == FeedSnapshot.sha256_of(PAYLOAD)
        assert loaded.fetched_at == FETCHED_AT

    def test_is_absent_until_written(self, archive: LocalSnapshotArchive) -> None:
        """Metadata is written after decoding, so a payload archived moments
        before a decode crash legitimately has none."""
        path = archive.put_payload(PAYLOAD, agency=AGENCY, fetched_at=FETCHED_AT)
        assert archive.load_metadata(path) is None

    def test_sits_beside_the_payload(self, archive: LocalSnapshotArchive) -> None:
        path = archive.put_payload(PAYLOAD, agency=AGENCY, fetched_at=FETCHED_AT)
        archive.put_metadata(self.snapshot(path))
        assert (archive.root / path.replace(".pb", ".json")).is_file()

    def test_metadata_is_not_mistaken_for_a_payload(self, archive: LocalSnapshotArchive) -> None:
        path = archive.put_payload(PAYLOAD, agency=AGENCY, fetched_at=FETCHED_AT)
        archive.put_metadata(self.snapshot(path))
        assert archive.list_payloads() == [path]


class TestQuarantineSidecar:
    def records(self) -> list[QuarantineRecord]:
        return [
            QuarantineRecord(
                reason=QuarantineReason.NULL_ISLAND,
                detail="position is the (0, 0) GPS failure value",
                agency=AGENCY,
                vehicle_id="bus-1",
                raw_payload=b"\x80\x81\xfe",  # arbitrary protobuf, not UTF-8
            ),
            QuarantineRecord(
                reason=QuarantineReason.SCHEMA_VALIDATION,
                detail="bearing: input should be less than or equal to 360",
                agency=AGENCY,
                vehicle_id="bus-2",
            ),
        ]

    def test_writes_binary_payloads_without_failing(self, archive: LocalSnapshotArchive) -> None:
        """A quarantined entity's raw protobuf is not valid UTF-8, and it is
        the single most useful field on the record."""
        path = archive.put_payload(PAYLOAD, agency=AGENCY, fetched_at=FETCHED_AT)
        archive.put_quarantine(path, self.records())

        sidecar = archive.root / path.replace(".pb", ".quarantine.json")
        assert sidecar.is_file()
        assert "NULL_ISLAND" in sidecar.read_text(encoding="utf-8")

    def test_round_trips_through_the_model(self, archive: LocalSnapshotArchive) -> None:
        import json

        path = archive.put_payload(PAYLOAD, agency=AGENCY, fetched_at=FETCHED_AT)
        archive.put_quarantine(path, self.records())

        raw = json.loads(
            (archive.root / path.replace(".pb", ".quarantine.json")).read_text(encoding="utf-8")
        )
        restored = [QuarantineRecord.model_validate(entry) for entry in raw]
        assert [r.reason for r in restored] == [
            QuarantineReason.NULL_ISLAND,
            QuarantineReason.SCHEMA_VALIDATION,
        ]
        assert restored[0].raw_payload == b"\x80\x81\xfe"


class TestListing:
    def test_is_empty_for_a_corpus_that_does_not_exist(self, tmp_path: Path) -> None:
        assert LocalSnapshotArchive(tmp_path / "nothing-here").list_payloads() == []

    def test_is_ordered_oldest_first(self, archive: LocalSnapshotArchive) -> None:
        """The path layout sorts chronologically, which is what lets the
        harness replay a window in the order it happened."""
        stamps = [
            datetime(2026, 10, 15, 8, 0, 0, tzinfo=UTC),
            datetime(2026, 10, 15, 9, 30, 0, tzinfo=UTC),
            datetime(2026, 10, 16, 7, 0, 0, tzinfo=UTC),
        ]
        for index, stamp in enumerate(stamps):
            archive.put_payload(f"payload-{index}".encode(), agency=AGENCY, fetched_at=stamp)

        listed = archive.list_payloads()
        assert listed == sorted(listed)
        assert "2026/10/15/080000" in listed[0]
        assert "2026/10/16/070000" in listed[2]

    def test_filters_by_agency(self, archive: LocalSnapshotArchive) -> None:
        archive.put_payload(b"a", agency="mta-nyct", fetched_at=FETCHED_AT)
        archive.put_payload(b"b", agency="wmata", fetched_at=FETCHED_AT)

        assert len(archive.list_payloads()) == 2
        assert len(archive.list_payloads(agency="wmata")) == 1
        assert archive.list_payloads(agency="wmata")[0].startswith("wmata/")

    def test_honours_a_limit(self, archive: LocalSnapshotArchive) -> None:
        for index in range(5):
            archive.put_payload(f"p{index}".encode(), agency=AGENCY, fetched_at=FETCHED_AT)
        assert len(archive.list_payloads(limit=2)) == 2


class TestBackendSelection:
    def test_local_environment_resolves_to_disk(self, tmp_path: Path) -> None:
        archive = build_snapshot_archive(
            Settings(env="local", snapshot_local_dir=str(tmp_path))  # type: ignore[call-arg]
        )
        assert isinstance(archive, LocalSnapshotArchive)
        assert archive.name == "local"

    def test_an_explicit_local_backend_wins_outside_local(self, tmp_path: Path) -> None:
        archive = build_snapshot_archive(
            Settings(  # type: ignore[call-arg]
                env="prod", snapshot_backend="local", snapshot_local_dir=str(tmp_path)
            )
        )
        assert isinstance(archive, LocalSnapshotArchive)

    def test_auto_outside_local_resolves_to_gcs(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Deliberately not inferred from whether a bucket name is set: the
        bucket has a default, so inference would make a laptop upload."""
        constructed: dict[str, str] = {}

        def fake_init(self: GcsSnapshotArchive, bucket: str) -> None:
            constructed["bucket"] = bucket
            self.name = "gcs"

        monkeypatch.setattr(GcsSnapshotArchive, "__init__", fake_init)
        archive = build_snapshot_archive(
            Settings(env="prod", snapshot_bucket="tp-prod-snapshots")  # type: ignore[call-arg]
        )
        assert constructed["bucket"] == "tp-prod-snapshots"
        assert archive.name == "gcs"


class TestProtocolConformance:
    def test_the_local_backend_satisfies_the_interface(self, archive: LocalSnapshotArchive) -> None:
        # The collector depends on the protocol, not the class, so this is what
        # keeps the GCS backend substitutable without a second set of tests.
        assert isinstance(archive, SnapshotArchive)

"""The replay corpus: every raw feed response, kept exactly as it arrived.

A GTFS-Realtime feed serves only the present. An agency has no endpoint for
"what you were publishing at 08:14 last Tuesday", so a bug in decoding or
enrichment that is discovered on Friday cannot be reproduced against the data
that triggered it -- unless the bytes were kept. Archiving them turns two
otherwise impossible things into routine ones: fixing a decode bug and
replaying the affected window, and load-testing against real payloads with
real skew and real garbage instead of synthetic data that is too clean to
exercise the quarantine path.

Two properties are load-bearing:

**The bytes are stored before anything parses them.** :meth:`put_payload` is
called on the raw HTTP response body, so a payload that crashes the decoder is
in the corpus rather than lost with it -- which is precisely the payload worth
having. Metadata is written afterwards, once decoding has counted entities.

**Local and cloud differ only by configuration.** Both backends implement
:class:`SnapshotArchive`, so the collector contains no branch on environment
and the replay harness reads a corpus captured on a laptop and one captured in
Cloud Run through the same calls.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol, runtime_checkable

import structlog

from transitpulse_core.config import Settings, get_settings
from transitpulse_core.models import FeedSnapshot, QuarantineRecord

log = structlog.get_logger(__name__)

PAYLOAD_SUFFIX = ".pb"
METADATA_SUFFIX = ".json"
QUARANTINE_SUFFIX = ".quarantine.json"


def snapshot_object_path(agency: str, fetched_at: datetime, content_sha256: str) -> str:
    """Date-hierarchical key for one snapshot, without a suffix.

    The ``YYYY/MM/DD`` prefix means replaying one day lists one prefix instead
    of scanning the corpus, which matters once it holds months of 15-second
    ticks. The content hash in the leaf makes the path a pure function of the
    payload: re-archiving the same bytes from the same second overwrites rather
    than accumulating a near-duplicate.
    """
    stamp = fetched_at.astimezone(UTC)
    return f"{agency}/{stamp:%Y/%m/%d}/{stamp:%H%M%S}-{content_sha256[:12]}"


@runtime_checkable
class SnapshotArchive(Protocol):
    """Where raw feed responses are kept."""

    name: str

    def put_payload(self, payload: bytes, *, agency: str, fetched_at: datetime) -> str:
        """Store unmodified bytes. Returns the object path, suffix included."""
        ...

    def put_metadata(self, snapshot: FeedSnapshot) -> None:
        """Store the metadata record beside its payload."""
        ...

    def put_quarantine(self, payload_path: str, records: list[QuarantineRecord]) -> None:
        """Store the entities this snapshot's decode refused, beside it.

        The collector deliberately has no database -- it stays useful while
        Postgres is down, and the quarantine table belongs to the processor --
        but "nothing is dropped silently" has to hold here too. Rejections land
        next to the bytes that produced them, which is also where a reprocessing
        run would look for them.
        """
        ...

    def list_payloads(self, *, agency: str | None = None, limit: int | None = None) -> list[str]:
        """Payload paths in corpus order, oldest first."""
        ...

    def load_payload(self, path: str) -> bytes: ...

    def load_metadata(self, path: str) -> FeedSnapshot | None:
        """Metadata for a payload path, or ``None`` if it was never written."""
        ...


def _metadata_path(payload_path: str) -> str:
    return payload_path.removesuffix(PAYLOAD_SUFFIX) + METADATA_SUFFIX


def _quarantine_path(payload_path: str) -> str:
    return payload_path.removesuffix(PAYLOAD_SUFFIX) + QUARANTINE_SUFFIX


def _quarantine_json(records: list[QuarantineRecord]) -> str:
    return json.dumps([json.loads(record.model_dump_json()) for record in records], indent=2)


class LocalSnapshotArchive:
    """Filesystem corpus, used for local development and in tests.

    Identical layout to the GCS backend -- the object path is used verbatim as
    a relative file path -- so a corpus can be copied in either direction and
    the harness cannot tell the difference.
    """

    name = "local"

    def __init__(self, root: str | Path) -> None:
        self._root = Path(root)

    @property
    def root(self) -> Path:
        return self._root

    def put_payload(self, payload: bytes, *, agency: str, fetched_at: datetime) -> str:
        path = snapshot_object_path(agency, fetched_at, FeedSnapshot.sha256_of(payload))
        path += PAYLOAD_SUFFIX
        target = self._root / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(payload)
        log.debug("snapshot.stored", backend=self.name, path=path, bytes=len(payload))
        return path

    def put_metadata(self, snapshot: FeedSnapshot) -> None:
        target = self._root / _metadata_path(snapshot.object_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(snapshot.model_dump_json(indent=2), encoding="utf-8")

    def put_quarantine(self, payload_path: str, records: list[QuarantineRecord]) -> None:
        target = self._root / _quarantine_path(payload_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(_quarantine_json(records), encoding="utf-8")

    def list_payloads(self, *, agency: str | None = None, limit: int | None = None) -> list[str]:
        base = self._root / agency if agency else self._root
        if not base.exists():
            return []
        found = sorted(
            p.relative_to(self._root).as_posix() for p in base.rglob(f"*{PAYLOAD_SUFFIX}")
        )
        return found[:limit] if limit is not None else found

    def load_payload(self, path: str) -> bytes:
        return (self._root / path).read_bytes()

    def load_metadata(self, path: str) -> FeedSnapshot | None:
        target = self._root / _metadata_path(path)
        if not target.exists():
            return None
        return FeedSnapshot.model_validate(json.loads(target.read_text(encoding="utf-8")))


class GcsSnapshotArchive:
    """Google Cloud Storage corpus, used by the deployed collector.

    The client is constructed eagerly so that a missing credential or bucket
    fails at service start rather than fifteen seconds later on the first
    scheduler tick, when the error surfaces as a lost snapshot instead of a
    failed deploy.
    """

    name = "gcs"

    def __init__(self, bucket: str) -> None:
        import google.cloud.storage

        self._client = google.cloud.storage.Client()
        self._bucket = self._client.bucket(bucket)
        self._bucket_name = bucket

    @property
    def bucket_name(self) -> str:
        return self._bucket_name

    def put_payload(self, payload: bytes, *, agency: str, fetched_at: datetime) -> str:
        path = snapshot_object_path(agency, fetched_at, FeedSnapshot.sha256_of(payload))
        path += PAYLOAD_SUFFIX
        blob = self._bucket.blob(path)
        blob.upload_from_string(payload, content_type="application/x-protobuf")
        log.debug("snapshot.stored", backend=self.name, path=path, bytes=len(payload))
        return path

    def put_metadata(self, snapshot: FeedSnapshot) -> None:
        blob = self._bucket.blob(_metadata_path(snapshot.object_path))
        blob.upload_from_string(snapshot.model_dump_json(indent=2), content_type="application/json")

    def put_quarantine(self, payload_path: str, records: list[QuarantineRecord]) -> None:
        blob = self._bucket.blob(_quarantine_path(payload_path))
        blob.upload_from_string(_quarantine_json(records), content_type="application/json")

    def list_payloads(self, *, agency: str | None = None, limit: int | None = None) -> list[str]:
        blobs = self._client.list_blobs(
            self._bucket, prefix=f"{agency}/" if agency else None, max_results=limit
        )
        return sorted(b.name for b in blobs if b.name.endswith(PAYLOAD_SUFFIX))

    def load_payload(self, path: str) -> bytes:
        data: bytes = self._bucket.blob(path).download_as_bytes()
        return data

    def load_metadata(self, path: str) -> FeedSnapshot | None:
        blob = self._bucket.blob(_metadata_path(path))
        if not blob.exists():
            return None
        return FeedSnapshot.model_validate_json(blob.download_as_bytes())


def build_snapshot_archive(settings: Settings | None = None) -> SnapshotArchive:
    """Pick a backend from configuration alone.

    ``auto`` resolves by environment rather than by whether a bucket name is
    present, because the bucket name has a default: inferring from it would
    make a local run with no GCP account attempt a real upload.
    """
    cfg = settings or get_settings()
    backend = cfg.snapshot_backend
    if backend == "auto":
        backend = "local" if cfg.is_local else "gcs"

    if backend == "gcs":
        return GcsSnapshotArchive(cfg.snapshot_bucket)
    return LocalSnapshotArchive(cfg.snapshot_local_dir)


__all__ = [
    "GcsSnapshotArchive",
    "LocalSnapshotArchive",
    "SnapshotArchive",
    "build_snapshot_archive",
    "snapshot_object_path",
]

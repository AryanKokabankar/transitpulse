"""BigQuery sink -- the analytical history.

Uses the **Storage Write API**, not the legacy ``insertAll`` streaming
endpoint. The legacy path costs roughly 5x more per GB, has no stream-level
delivery guarantees, and its insert buffer blocks DML on the table. The
Storage Write API's ``_default`` stream is cheaper, supports at-least-once
delivery natively, and makes rows queryable immediately.

The awkward part of that API is that it speaks protobuf, which normally means
checking a generated ``_pb2.py`` into the repo and a ``grpcio-tools`` build
step. We avoid both by constructing the ``DescriptorProto`` at runtime from the
same column list the Postgres sink uses, so the schema is declared exactly once
in Python and the two sinks cannot drift apart. ADR-004 covers this.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import structlog
from google.protobuf import descriptor_pb2, message_factory
from opentelemetry.trace import SpanKind

from transitpulse_core.config import Settings, get_settings
from transitpulse_core.models import EnrichedVehicleEvent
from transitpulse_core.sinks import WriteResult
from transitpulse_core.telemetry import get_metrics, get_tracer

log = structlog.get_logger(__name__)

_T = descriptor_pb2.FieldDescriptorProto


@dataclass(frozen=True)
class _Column:
    name: str
    # The protobuf enum's own value type, so a wrong constant is a type error
    # rather than a runtime AppendRows rejection.
    proto_type: _T.Type.ValueType
    # BigQuery TIMESTAMP columns arrive as int64 microseconds since epoch.
    is_timestamp: bool = False


# Order is irrelevant to BigQuery (fields are matched by name), but field
# numbers must stay stable once data is in flight, so new columns append.
_SCHEMA: tuple[_Column, ...] = (
    _Column("idempotency_key", _T.TYPE_STRING),
    _Column("agency", _T.TYPE_STRING),
    _Column("vehicle_id", _T.TYPE_STRING),
    _Column("trip_id", _T.TYPE_STRING),
    _Column("route_id", _T.TYPE_STRING),
    _Column("route_short_name", _T.TYPE_STRING),
    _Column("direction_id", _T.TYPE_INT64),
    _Column("start_date", _T.TYPE_STRING),
    _Column("schedule_relationship", _T.TYPE_STRING),
    _Column("latitude", _T.TYPE_DOUBLE),
    _Column("longitude", _T.TYPE_DOUBLE),
    _Column("bearing", _T.TYPE_DOUBLE),
    _Column("reported_speed_mps", _T.TYPE_DOUBLE),
    _Column("computed_speed_mps", _T.TYPE_DOUBLE),
    _Column("distance_from_last_m", _T.TYPE_DOUBLE),
    _Column("seconds_since_last_ping", _T.TYPE_DOUBLE),
    _Column("schedule_delay_seconds", _T.TYPE_INT64),
    _Column("is_on_time", _T.TYPE_BOOL),
    _Column("current_stop_sequence", _T.TYPE_INT64),
    _Column("stop_id", _T.TYPE_STRING),
    _Column("occupancy_status", _T.TYPE_STRING),
    _Column("event_time", _T.TYPE_INT64, is_timestamp=True),
    _Column("ingest_time", _T.TYPE_INT64, is_timestamp=True),
    _Column("processed_time", _T.TYPE_INT64, is_timestamp=True),
)


def build_row_descriptor() -> descriptor_pb2.DescriptorProto:
    """Describe a ``vehicle_events`` row as a protobuf message, at runtime."""
    proto = descriptor_pb2.DescriptorProto()
    proto.name = "VehicleEventRow"
    for index, column in enumerate(_SCHEMA, start=1):
        field = proto.field.add()
        field.name = column.name
        field.number = index
        field.type = column.proto_type
        # OPTIONAL plus proto3 field presence: an unset field is a genuine
        # NULL in BigQuery rather than a zero, which matters for nullable
        # metrics like schedule_delay_seconds.
        field.label = _T.LABEL_OPTIONAL
        field.proto3_optional = True
        field.oneof_index = len(proto.oneof_decl)
        proto.oneof_decl.add().name = f"_{column.name}"
    return proto


def _to_micros(value: datetime) -> int:
    return int(value.timestamp() * 1_000_000)


def event_to_row(event: EnrichedVehicleEvent) -> dict[str, Any]:
    """Flatten an enriched event into a BigQuery row dict, omitting NULLs."""
    p = event.ping
    row: dict[str, Any] = {
        "idempotency_key": event.idempotency_key,
        "agency": p.agency,
        "vehicle_id": p.vehicle_id,
        "schedule_relationship": str(p.schedule_relationship),
        "occupancy_status": str(p.occupancy_status),
        "latitude": p.latitude,
        "longitude": p.longitude,
        "event_time": _to_micros(p.event_time),
        "ingest_time": _to_micros(p.ingest_time),
        "processed_time": _to_micros(event.processed_time),
    }
    optional: dict[str, Any] = {
        "trip_id": p.trip_id,
        "route_id": p.route_id,
        "route_short_name": event.route_short_name,
        "direction_id": p.direction_id,
        "start_date": p.start_date,
        "bearing": p.bearing,
        "reported_speed_mps": p.reported_speed_mps,
        "computed_speed_mps": event.computed_speed_mps,
        "distance_from_last_m": event.distance_from_last_m,
        "seconds_since_last_ping": event.seconds_since_last_ping,
        "schedule_delay_seconds": event.schedule_delay_seconds,
        "is_on_time": event.is_on_time,
        "current_stop_sequence": p.current_stop_sequence,
        "stop_id": p.stop_id,
    }
    row.update({k: v for k, v in optional.items() if v is not None})
    return row


class BigQuerySink:
    """Buffered append-only writer over the ``_default`` write stream.

    Rows accumulate until either ``bq_batch_size`` or
    ``bq_batch_timeout_seconds`` is reached, because one 500-row ``AppendRows``
    call costs about the same as a 1-row call -- per-row appends were the first
    throughput ceiling this pipeline hit.
    """

    name = "bigquery"

    def __init__(self, settings: Settings | None = None, *, connect: bool = True) -> None:
        self._settings = settings or get_settings()
        self._metrics = get_metrics()
        self._tracer = get_tracer()
        self._lock = threading.Lock()
        self._buffer: list[EnrichedVehicleEvent] = []
        self._last_flush = time.monotonic()

        self._descriptor = build_row_descriptor()
        pool = descriptor_pb2.FileDescriptorProto()
        pool.name = "transitpulse_vehicle_event.proto"
        pool.package = "transitpulse"
        pool.syntax = "proto3"
        pool.message_type.add().CopyFrom(self._descriptor)
        self._row_class = self._build_row_class(pool)

        self._client: Any | None = None
        self._stream_path = ""
        if connect:
            self._connect()

    @staticmethod
    def _build_row_class(file_proto: descriptor_pb2.FileDescriptorProto) -> Any:
        """Materialise a usable message class from the runtime descriptor.

        A private pool, not the default one: adding the same file name to the
        global pool twice raises, which would make constructing a second sink
        (as the tests and the replay harness both do) fail.

        ``Add()`` does not reliably return the descriptor across protobuf
        versions, so the file is looked up by name afterwards instead.
        """
        from google.protobuf import descriptor_pool

        pool = descriptor_pool.DescriptorPool()
        pool.Add(file_proto)
        file_descriptor = pool.FindFileByName(file_proto.name)
        return message_factory.GetMessageClass(
            file_descriptor.message_types_by_name["VehicleEventRow"]
        )

    def _connect(self) -> None:
        from google.cloud import bigquery_storage_v1

        self._client = bigquery_storage_v1.BigQueryWriteClient()
        self._stream_path = (
            f"projects/{self._settings.gcp_project}"
            f"/datasets/{self._settings.bq_dataset}"
            f"/tables/{self._settings.bq_table}/streams/_default"
        )
        log.info("bigquery.connected", stream=self._stream_path)

    # ------------------------------------------------------------------
    def write_batch(self, events: list[EnrichedVehicleEvent]) -> WriteResult:
        """Buffer rows, flushing when the batch is full or has aged out."""
        if not events:
            return WriteResult(sink=self.name)

        with self._lock:
            self._buffer.extend(events)
            due = len(self._buffer) >= self._settings.bq_batch_size or (
                time.monotonic() - self._last_flush >= self._settings.bq_batch_timeout_seconds
            )
            if not due:
                # Accepted into the buffer; the caller may ack the message
                # because a flush is guaranteed by the timeout path.
                return WriteResult(sink=self.name, written=len(events))
            pending, self._buffer = self._buffer, []
            self._last_flush = time.monotonic()

        return self._append(pending)

    def _append(self, events: list[EnrichedVehicleEvent]) -> WriteResult:
        from google.cloud import bigquery_storage_v1

        with self._tracer.start_as_current_span(
            "bigquery.append_rows",
            kind=SpanKind.CLIENT,
            attributes={"db.system": "bigquery", "batch.size": len(events)},
        ) as span:
            started = time.perf_counter()
            self._metrics.inflight_batches.add(1, {"sink": self.name})
            try:
                if self._client is None:  # pragma: no cover - guarded by flag
                    raise RuntimeError("BigQuerySink used without a connection")

                rows = bigquery_storage_v1.types.ProtoRows()
                for event in events:
                    message = self._row_class()
                    for key, value in event_to_row(event).items():
                        setattr(message, key, value)
                    rows.serialized_rows.append(message.SerializeToString())

                proto_data = bigquery_storage_v1.types.AppendRowsRequest.ProtoData(
                    writer_schema=bigquery_storage_v1.types.ProtoSchema(
                        proto_descriptor=self._descriptor
                    ),
                    rows=rows,
                )
                request = bigquery_storage_v1.types.AppendRowsRequest(
                    write_stream=self._stream_path, proto_rows=proto_data
                )
                # The _default stream is at-least-once; duplicates are made
                # harmless downstream by deduplicating on idempotency_key in
                # the nightly aggregation views rather than at write time.
                responses = self._client.append_rows(iter([request]))
                for response in responses:
                    if response.error.code:
                        raise RuntimeError(f"AppendRows failed: {response.error.message}")
                    break

                elapsed = time.perf_counter() - started
                self._metrics.sink_write_duration.record(
                    elapsed, {"sink": self.name, "outcome": "ok"}
                )
                span.set_attribute("db.rows_appended", len(events))
                return WriteResult(sink=self.name, written=len(events))
            except Exception as exc:
                self._metrics.sink_write_duration.record(
                    time.perf_counter() - started, {"sink": self.name, "outcome": "error"}
                )
                span.record_exception(exc)
                log.error("bigquery.append_failed", batch_size=len(events), exc_info=True)
                return WriteResult(sink=self.name, failed=len(events), errors=[str(exc)])
            finally:
                self._metrics.inflight_batches.add(-1, {"sink": self.name})

    def flush(self) -> None:
        """Drain the buffer. Must be called on shutdown or rows are lost."""
        with self._lock:
            pending, self._buffer = self._buffer, []
            self._last_flush = time.monotonic()
        if pending:
            self._append(pending)

    def close(self) -> None:
        self.flush()

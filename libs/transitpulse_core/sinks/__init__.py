"""Storage sinks.

The pipeline writes each event to two stores because they answer different
questions and have opposite access patterns:

* :class:`~transitpulse_core.sinks.postgres.PostgresSink` -- the serving layer.
  One hot row per vehicle, upserted, indexed for point lookups. OLTP.
* :class:`~transitpulse_core.sinks.bigquery.BigQuerySink` -- the history.
  Append-only, date-partitioned, route-clustered, batched. OLAP.

Both implement :class:`EventSink`, so the processor does not know or care how
many sinks are enabled; adding a third is a registration, not a rewrite.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol, runtime_checkable

from transitpulse_core.models import EnrichedVehicleEvent


@dataclass(frozen=True)
class WriteResult:
    """Outcome of one batch write.

    ``deduped`` is reported separately from ``written`` because a duplicate is
    a success, not a failure -- the agency re-sending a ping it already sent is
    normal, and conflating the two makes the delivery-rate metric meaningless.
    """

    sink: str
    written: int = 0
    deduped: int = 0
    failed: int = 0
    errors: list[str] = field(default_factory=list)

    @property
    def accepted(self) -> int:
        return self.written + self.deduped

    @property
    def ok(self) -> bool:
        return self.failed == 0


@runtime_checkable
class EventSink(Protocol):
    """Minimal contract every sink satisfies."""

    name: str

    def write_batch(self, events: list[EnrichedVehicleEvent]) -> WriteResult:
        """Persist a batch idempotently. Must tolerate being called twice."""
        ...

    def flush(self) -> None:
        """Force any buffered rows out. Called on shutdown."""
        ...

    def close(self) -> None: ...


__all__ = ["EventSink", "WriteResult"]

"""The load harness: replays the corpus at a target rate and reports what it
actually achieved.

This is the only thing in the project allowed to produce a throughput number,
so it is written to make an inflated one hard:

**Achieved rate is measured, never assumed.** It is acknowledged messages
divided by elapsed monotonic time, where "acknowledged" means Pub/Sub accepted
the publish -- not that the loop handed the message to a client library. A
harness that counted submissions would report the speed of a buffer.

**The report says who the bottleneck was.** If the limiter spent almost no time
waiting, the loop never reached its target under its own steam, and the
shortfall is the publisher's rather than the pacing's. Reporting the target
alongside the achieved rate and the limiter's wait time makes that
distinguishable instead of leaving a single flattering number.

**Elapsed time includes the drain.** In-flight batches are waited for before
the clock is read, so the last second of a run cannot be published for free.
"""

from __future__ import annotations

import math
import time
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

import structlog

from tools.replay.corpus import LoadedCorpus, fan_out, suffixed_vehicle_id
from tools.replay.limiter import TokenBucket
from transitpulse_core.models import VehiclePing
from transitpulse_core.pubsub import EventPublisher

log = structlog.get_logger(__name__)

# Enough batches per second that each pacing sleep is tens of milliseconds.
# Finer than this and the limiter's accuracy is dominated by the operating
# system's sleep granularity, which on Windows is coarse enough to matter.
TARGET_BATCHES_PER_SECOND = 20


@dataclass(frozen=True)
class ReplayConfig:
    rate_per_second: float
    duration: timedelta
    fanout: int = 1
    workers: int = 4
    seed: int = 0
    preserve_timestamps: bool = False
    batch_size: int | None = None
    dry_run: bool = False

    @property
    def expected_messages(self) -> int:
        return int(self.rate_per_second * self.duration.total_seconds())

    def resolved_batch_size(self) -> int:
        if self.batch_size is not None:
            return max(1, self.batch_size)
        return max(1, round(self.rate_per_second / TARGET_BATCHES_PER_SECOND))


@dataclass
class ReplayReport:
    """The numbers a benchmark may quote, and the context that qualifies them."""

    target_rate: float
    achieved_rate: float
    duration_seconds: float
    expected_messages: int
    published: int
    failed: int
    batches: int
    batch_size: int
    workers: int
    fanout: int
    corpus_snapshots: int
    corpus_pings: int
    distinct_vehicles: int
    corpus_cycles: int
    corpus_duplicate_keys: int
    limiter_wait_seconds: float
    publish_latency_ms: dict[str, float] = field(default_factory=dict)
    timestamps: str = "rebased"
    dry_run: bool = False

    @property
    def rate_accuracy(self) -> float:
        """Achieved over target. Below 1.0 means the run fell short."""
        return self.achieved_rate / self.target_rate if self.target_rate else 0.0

    @property
    def publisher_bound(self) -> bool:
        """Whether the shortfall belongs to the publisher rather than the pacing.

        A limiter that barely waited was never the constraint: the loop was
        already running flat out and still missed the target.
        """
        return self.rate_accuracy < 0.98 and self.limiter_wait_seconds < (
            0.05 * self.duration_seconds
        )

    def as_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["rate_accuracy"] = round(self.rate_accuracy, 4)
        payload["publisher_bound"] = self.publisher_bound
        payload["generated_at"] = datetime.now(UTC).isoformat()
        return payload

    def render(self) -> str:
        lines = [
            "",
            "  replay results",
            "  " + "-" * 52,
            f"  target rate        {self.target_rate:,.1f} msg/s",
            f"  achieved rate      {self.achieved_rate:,.1f} msg/s"
            f"   ({self.rate_accuracy:.1%} of target)",
            f"  duration           {self.duration_seconds:.2f} s",
            f"  published          {self.published:,} of {self.expected_messages:,} expected",
        ]
        if self.failed:
            lines.append(f"  failed             {self.failed:,}")
        lines.extend(
            [
                f"  batches            {self.batches:,} x {self.batch_size}"
                f" across {self.workers} worker(s)",
                f"  corpus             {self.corpus_pings:,} pings from"
                f" {self.corpus_snapshots} snapshot(s), fanout x{self.fanout}"
                f" -> {self.distinct_vehicles:,} vehicles",
                f"  corpus cycles      {self.corpus_cycles:,}"
                f" (each cycle emits fresh synthetic vehicle ids)",
                f"  limiter wait       {self.limiter_wait_seconds:.2f} s",
                f"  event timestamps   {self.timestamps}",
            ]
        )
        if self.publish_latency_ms:
            percentiles = "  ".join(
                f"{name}={value:.1f}ms" for name, value in self.publish_latency_ms.items()
            )
            lines.append(f"  publish latency    {percentiles}")
        if self.corpus_duplicate_keys:
            lines.append(
                f"  NOTE: {self.corpus_duplicate_keys:,} corpus pings share an"
                " idempotency key with another; that share of the run"
                " deduplicates rather than being written"
            )
        if self.dry_run:
            lines.append("  NOTE: dry run -- nothing was published, pacing only")
        elif self.publisher_bound:
            lines.append(
                "  NOTE: the limiter was not the constraint; the publisher could"
                " not keep up with the target"
            )
        lines.append("")
        return "\n".join(lines)


class ReplayHarness:
    """Drives one replay run.

    The publisher is injected so the harness can be exercised against the
    emulator in integration tests and against a stub in unit tests, and so a
    dry run can measure the pacing with no broker at all.
    """

    def __init__(
        self,
        config: ReplayConfig,
        corpus: LoadedCorpus,
        *,
        publisher: EventPublisher | None = None,
        limiter: TokenBucket | None = None,
    ) -> None:
        self._config = config
        self._corpus = corpus
        self._publisher = publisher
        self._batch_size = config.resolved_batch_size()
        # Capacity has to hold at least one batch or acquiring one would be
        # impossible; above that it stays small to keep the opening burst down.
        self._limiter = limiter or TokenBucket(
            config.rate_per_second,
            capacity=max(float(self._batch_size), config.rate_per_second * 0.1),
        )

        self._pings = fan_out(corpus.pings, config.fanout, seed=config.seed)
        if not self._pings:  # pragma: no cover - load_corpus rejects this first
            raise ValueError("nothing to replay")

        # The corpus's own newest event time anchors re-basing: every emitted
        # ping keeps its age relative to that instant, so a snapshot's internal
        # skew survives while the window as a whole lands on the present.
        self._newest_event_time = max(ping.event_time for ping in self._pings)
        self._latencies_ms: list[float] = []

    @property
    def batch_size(self) -> int:
        return self._batch_size

    def run(self) -> ReplayReport:
        config = self._config
        deadline_seconds = config.duration.total_seconds()
        published = 0
        failed = 0
        batches = 0

        cursor = 0
        cycle = 0
        start = time.monotonic()

        with ThreadPoolExecutor(max_workers=config.workers) as pool:
            inflight: set[Future[tuple[int, int, float]]] = set()
            while time.monotonic() - start < deadline_seconds:
                self._limiter.acquire(self._batch_size)

                batch, cursor, cycle = self._next_batch(cursor, cycle)
                batches += 1

                if config.dry_run:
                    published += len(batch)
                    continue

                inflight.add(pool.submit(self._publish, batch))
                # Bounded in-flight work: letting batches pile up would
                # decouple the loop from the broker and report a rate the
                # pipeline never absorbed.
                if len(inflight) >= config.workers * 2:
                    completed, inflight = wait(inflight, return_when=FIRST_COMPLETED)
                    ok, bad = self._collect(completed)
                    published += ok
                    failed += bad

            # The drain is inside the measured window on purpose: excluding it
            # would credit the run with messages that were still in flight.
            for future in inflight:
                ok, bad = self._collect({future})
                published += ok
                failed += bad

        elapsed = time.monotonic() - start

        report = ReplayReport(
            target_rate=config.rate_per_second,
            achieved_rate=published / elapsed if elapsed > 0 else 0.0,
            duration_seconds=elapsed,
            expected_messages=config.expected_messages,
            published=published,
            failed=failed,
            batches=batches,
            batch_size=self._batch_size,
            workers=config.workers,
            fanout=config.fanout,
            corpus_snapshots=self._corpus.snapshot_count,
            corpus_pings=self._corpus.size,
            distinct_vehicles=len({ping.vehicle_id for ping in self._pings}),
            corpus_cycles=cycle + 1,
            corpus_duplicate_keys=self._corpus.duplicate_key_count,
            limiter_wait_seconds=round(self._limiter.stats.wait_seconds, 4),
            publish_latency_ms=self._percentiles(),
            timestamps="preserved" if config.preserve_timestamps else "rebased",
            dry_run=config.dry_run,
        )
        log.info("replay.completed", **report.as_dict())
        return report

    def _next_batch(self, cursor: int, cycle: int) -> tuple[list[VehiclePing], int, int]:
        batch: list[VehiclePing] = []
        now = datetime.now(UTC)

        for _ in range(self._batch_size):
            if cursor >= len(self._pings):
                cursor = 0
                cycle += 1
            batch.append(self._stamp(self._pings[cursor], cycle, now))
            cursor += 1

        return batch, cursor, cycle

    def _stamp(self, ping: VehiclePing, cycle: int, now: datetime) -> VehiclePing:
        """Give a corpus ping the identity and timestamps this emission carries.

        Re-basing is expressed as an *age*: the corpus's newest ping maps onto
        now, and the rest keep their offsets behind it. Shifting the window
        forward by a cycle instead -- the obvious way to keep idempotency keys
        unique on a second pass -- would date events into the future and feed
        negative values into the lag histogram, which is the one metric this
        pipeline is judged on.

        So a repeat pass disambiguates by identity rather than by time: cycle
        *n* emits a fresh set of synthetic vehicle ids. A run that cycles the
        corpus is by definition generating more traffic than the corpus holds,
        and representing that as more synthetic vehicles is truthful, where
        time travel is not.

        ``model_copy`` rather than reconstruction: every field replaced here is
        already valid, and full re-validation per message would show up in the
        achieved rate -- the one number that must not be an artefact of the
        harness. Fan-out, which invents coordinates, does validate.
        """
        if self._config.preserve_timestamps:
            return ping.model_copy(update={"ingest_time": now})

        update: dict[str, object] = {
            "event_time": now - (self._newest_event_time - ping.event_time),
            "ingest_time": now,
        }
        if cycle:
            update["vehicle_id"] = suffixed_vehicle_id(ping.vehicle_id, f"#c{cycle}")
        return ping.model_copy(update=update)

    def _publish(self, batch: list[VehiclePing]) -> tuple[int, int, float]:
        if self._publisher is None:  # pragma: no cover - guarded by dry_run
            raise RuntimeError("no publisher configured; use dry_run to measure pacing only")
        started = time.monotonic()
        published = self._publisher.publish_batch(batch)
        return published, len(batch), (time.monotonic() - started) * 1000.0

    def _collect(self, futures: set[Future[tuple[int, int, float]]]) -> tuple[int, int]:
        published = 0
        failed = 0
        for future in futures:
            try:
                count, requested, latency_ms = future.result()
            except Exception as exc:  # noqa: BLE001 - one bad batch must not end the run
                failed += self._batch_size
                log.error("replay.batch_failed", error=str(exc))
                continue
            published += count
            failed += requested - count
            # Per message, so the figure is comparable across batch sizes.
            self._latencies_ms.append(latency_ms / max(1, count))
        return published, failed

    def _percentiles(self) -> dict[str, float]:
        if not self._latencies_ms:
            return {}
        ordered = sorted(self._latencies_ms)

        def at(fraction: float) -> float:
            index = min(len(ordered) - 1, max(0, math.ceil(fraction * len(ordered)) - 1))
            return round(ordered[index], 3)

        return {"p50": at(0.50), "p95": at(0.95), "p99": at(0.99)}

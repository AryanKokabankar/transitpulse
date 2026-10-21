"""Measurement primitives shared by the chaos scenarios.

Everything here exists to make a scenario's conclusion a count rather than an
opinion, so the helpers are deliberately dull: build pings, publish them,
drive the processor, count rows, read percentiles out of SQL.

Two of them carry reasoning worth keeping.

:func:`distinct_pings` filters the corpus down to one ping per idempotency key
before anything is published. Without it the zero-loss assertion is unwinnable:
roughly a third of a multi-snapshot capture re-sends an observation it has
already sent (ADR-006), those messages deduplicate at the sink by design, and
``rows == published`` would therefore fail on a pipeline that lost nothing at
all. Experiment 4 does the opposite on purpose.

:func:`latency_percentiles` reads the three timestamps back out of
``vehicle_events`` instead of asking Prometheus. The histogram's buckets were
placed around the 2s objective and are far too coarse to quote a p99 from --
the gap between the 1.5s and 2s boundaries is wider than the whole processing
latency being measured. SQL over the rows the run actually wrote is exact, and
it is a query a reader can re-run against their own database.
"""

from __future__ import annotations

import asyncio
import os
import secrets
import shutil
import socket
import subprocess
import sys
import threading
import time
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import httpx
import psycopg
import structlog
from google.cloud import pubsub_v1

from tools.pushbridge import BridgeReport, bridge
from tools.replay.corpus import load_corpus, suffixed_vehicle_id
from tools.replay.limiter import TokenBucket
from transitpulse_core.config import Settings, get_settings
from transitpulse_core.models import VehiclePing
from transitpulse_core.pubsub import EventPublisher
from transitpulse_core.snapshots import build_snapshot_archive

log = structlog.get_logger(__name__)

REPO_ROOT = Path(__file__).resolve().parents[2]
RESULTS_DIR = REPO_ROOT / "docs" / "benchmarks"

PROMETHEUS_URL = os.environ.get("TP_PROMETHEUS_URL", "http://localhost:9090")


def run_token() -> str:
    """A short tag that makes one run's rows findable and unique.

    Not a security context -- it labels synthetic bus positions -- but
    ``secrets`` is the module that gives a collision-free token without seeding
    a global RNG that a scenario might also want deterministic.
    """
    return secrets.token_hex(3)


def chaos_agency(scenario: str) -> str:
    """One agency per run, so every count below is of this run's rows only."""
    return f"chaos-{scenario}-{run_token()}"


def free_port() -> int:
    """A port nothing is listening on, bound and released rather than guessed."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


# ----------------------------------------------------------------------
# building load out of the corpus
# ----------------------------------------------------------------------
@dataclass(frozen=True)
class ChaosLoad:
    """Pings to publish, plus the provenance that qualifies their timestamps."""

    pings: list[VehiclePing]
    agency: str
    corpus_pings: int
    corpus_distinct_keys: int
    # Spread of the corpus's own event times, mapped onto the present by
    # re-basing. It lands inside every end-to-end lag figure measured from a
    # replay -- an event that was already 90 seconds old when it was captured
    # is 90 seconds old when it is replayed -- so it is reported here to be
    # subtracted rather than left to inflate a latency claim.
    event_time_span_seconds: float
    timestamps: str = "rebased"

    @property
    def size(self) -> int:
        return len(self.pings)


def distinct_pings(pings: Sequence[VehiclePing]) -> list[VehiclePing]:
    """One ping per idempotency key, keeping the first occurrence."""
    seen: set[str] = set()
    out: list[VehiclePing] = []
    for ping in pings:
        key = ping.idempotency_key
        if key in seen:
            continue
        seen.add(key)
        out.append(ping)
    return out


def build_load(
    settings: Settings,
    *,
    agency: str,
    count: int,
    now: datetime | None = None,
) -> ChaosLoad:
    """Real corpus pings, re-identified under this run's agency.

    One ``now`` for the whole batch, not one per sub-batch as the load harness
    uses. Re-basing maps the corpus's newest event onto ``now`` and keeps every
    other ping's offset behind it, which is injective in the original event
    time -- so with a single anchor, keys stay exactly as distinct as
    :func:`distinct_pings` made them. A moving anchor could map two different
    original times onto one, silently reintroducing the duplicates this
    function just removed.
    """
    archive = build_snapshot_archive(settings)
    corpus = load_corpus(archive, agency=settings.feed_agency)
    unique = distinct_pings(corpus.pings)
    anchor = now or datetime.now(UTC)
    newest = max(ping.event_time for ping in unique)

    pings: list[VehiclePing] = []
    for index in range(count):
        base = unique[index % len(unique)]
        cycle = index // len(unique)
        update: dict[str, Any] = {
            "agency": agency,
            "event_time": anchor - (newest - base.event_time),
            "ingest_time": anchor,
        }
        if cycle:
            # Same device as the load harness: a pass beyond the corpus's size
            # is more synthetic vehicles, never events dated into the future.
            update["vehicle_id"] = suffixed_vehicle_id(base.vehicle_id, f"#x{cycle}")
        pings.append(base.model_copy(update=update))

    span = max(p.event_time for p in pings) - min(p.event_time for p in pings)
    return ChaosLoad(
        pings=pings,
        agency=agency,
        corpus_pings=corpus.size,
        corpus_distinct_keys=len(unique),
        event_time_span_seconds=round(span.total_seconds(), 3),
    )


def publish(settings: Settings, pings: Sequence[VehiclePing], *, batch: int = 200) -> int:
    """Publish and return what the broker acknowledged, never what was sent."""
    publisher = EventPublisher(settings)
    try:
        return sum(
            publisher.publish_batch(pings[offset : offset + batch])
            for offset in range(0, len(pings), batch)
        )
    finally:
        publisher.close()


@dataclass
class PacedPublish:
    """What a paced publisher managed, measured rather than requested."""

    requested: int
    published: int
    seconds: float
    batches: int

    @property
    def rate(self) -> float:
        return self.published / self.seconds if self.seconds > 0 else 0.0

    def as_dict(self) -> dict[str, Any]:
        return {
            "requested": self.requested,
            "published": self.published,
            "batches": self.batches,
            "seconds": round(self.seconds, 3),
            "achieved_per_second": round(self.rate, 1),
        }


def publish_paced(
    settings: Settings,
    pings: Sequence[VehiclePing],
    *,
    rate: float,
    batch: int = 50,
) -> PacedPublish:
    """Publish at a target rate, stamping the timestamps batch by batch.

    This is what makes a latency figure mean anything. :func:`build_load`
    stamps one ``ingest_time`` across the whole batch, which is correct for a
    throughput run and useless for a latency one: every message then claims to
    have been decoded at the same instant, so ``processed_time - ingest_time``
    reports how long the queue took to drain rather than how long the pipeline
    took on any single event. Measured that way, a larger backlog looks like
    worse latency on an identically fast pipeline.

    Re-stamping per batch is what the collector actually does -- one tick
    decodes one batch and stamps it -- so each batch's interval is a real
    one. The re-basing is the load harness's, for the same reason: an event is
    shifted by its age rather than into the future, because the lag histogram
    this feeds is the metric the SLO is written against.
    """
    publisher = EventPublisher(settings)
    limiter = TokenBucket(rate, capacity=max(float(batch), rate * 0.1))
    newest = max(ping.event_time for ping in pings)
    published = 0
    batches = 0
    started = time.monotonic()
    try:
        for offset in range(0, len(pings), batch):
            window = pings[offset : offset + batch]
            limiter.acquire(len(window))
            now = datetime.now(UTC)
            stamped = [
                ping.model_copy(
                    update={
                        "event_time": now - (newest - ping.event_time),
                        "ingest_time": now,
                    }
                )
                for ping in window
            ]
            published += publisher.publish_batch(stamped)
            batches += 1
    finally:
        publisher.close()
    return PacedPublish(
        requested=len(pings),
        published=published,
        seconds=time.monotonic() - started,
        batches=batches,
    )


def publish_raw(settings: Settings, payloads: Sequence[bytes]) -> int:
    """Publish bodies the contract would reject, as a bad producer would.

    ``EventPublisher`` takes a validated ping and so cannot express this, which
    is the right division: the quarantine path exists for producers that are
    not this codebase.
    """
    client = pubsub_v1.PublisherClient()
    topic = client.topic_path(settings.gcp_project, settings.topic_vehicle_events)
    futures = [client.publish(topic, data=payload) for payload in payloads]
    published = 0
    for future in futures:
        future.result(timeout=60)
        published += 1
    return published


# ----------------------------------------------------------------------
# driving the consumer
# ----------------------------------------------------------------------
def deliver(
    settings: Settings,
    *,
    seconds: float,
    max_messages: int | None = None,
    concurrency: int = 16,
    url: str | None = None,
) -> BridgeReport:
    """Run the push bridge once, in-process or against a running processor."""
    return asyncio.run(
        bridge(
            settings,
            duration=timedelta(seconds=seconds),
            max_messages=max_messages,
            concurrency=concurrency,
            url=url,
        )
    )


@contextmanager
def processor_process(
    settings: Settings,
    *,
    port: int | None = None,
    env: dict[str, str] | None = None,
) -> Iterator[ProcessorHandle]:
    """A real processor in its own OS process, so it can be killed abruptly.

    The in-process ASGI path the bridge uses by default cannot be killed
    without taking the experiment down with it, and a processor that exits
    cleanly on request is not the failure being tested: a Cloud Run instance
    disappears mid-request without running its shutdown hooks. ``terminate()``
    on Windows is ``TerminateProcess``, which is exactly that.
    """
    handle = ProcessorHandle(port or free_port(), env=env)
    try:
        handle.start()
        yield handle
    finally:
        handle.stop()


class ProcessorHandle:
    """Start, kill and restart the processor as a separate process."""

    def __init__(self, port: int, *, env: dict[str, str] | None = None) -> None:
        self.port = port
        self.starts = 0
        self.kills = 0
        # Applied at start, not at request time: the processor builds its sink
        # and its connection pool once, in its lifespan, so anything that
        # changes those has to be in the environment before it boots.
        self._env = env or {}
        self._process: subprocess.Popen[bytes] | None = None

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    @property
    def alive(self) -> bool:
        return self._process is not None and self._process.poll() is None

    def start(self, *, timeout: float = 60.0) -> None:
        if self.alive:
            return
        self._process = subprocess.Popen(  # noqa: S603 - fixed argv, no shell
            [
                sys.executable,
                "-m",
                "uvicorn",
                "services.processor.main:app",
                "--host",
                "127.0.0.1",
                "--port",
                str(self.port),
                "--no-access-log",
            ],
            cwd=str(REPO_ROOT),
            env={**os.environ, **self._env},
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        self.starts += 1
        self._await_ready(timeout)

    def _await_ready(self, timeout: float) -> None:
        """Poll /readyz, which only answers once the database pool is open.

        Waiting on the port alone would be enough to connect and not enough to
        be served: uvicorn binds before the lifespan has built the sinks, and
        the first delivery would meet an uninitialised pool.
        """
        deadline = time.monotonic() + timeout
        last = "no attempt made"
        while time.monotonic() < deadline:
            if not self.alive:
                raise RuntimeError("the processor exited during startup")
            try:
                response = httpx.get(f"{self.url}/readyz", timeout=2.0)
                if response.status_code == 200:
                    return
                last = f"HTTP {response.status_code}"
            except httpx.HTTPError as exc:
                last = str(exc)
        raise TimeoutError(f"the processor was not ready within {timeout}s ({last})")

    def kill(self) -> None:
        """Abrupt, with no chance to flush or drain. That is the point."""
        if self._process is None or not self.alive:
            return
        self._process.terminate()
        try:
            self._process.wait(timeout=15)
        except subprocess.TimeoutExpired:  # pragma: no cover - belt and braces
            self._process.kill()
            self._process.wait(timeout=15)
        self.kills += 1

    def stop(self) -> None:
        self.kill()
        self._process = None


# ----------------------------------------------------------------------
# counting what came out
# ----------------------------------------------------------------------
@contextmanager
def database(settings: Settings) -> Iterator[psycopg.Connection]:
    with psycopg.connect(settings.pg_dsn_str, autocommit=True, connect_timeout=10) as conn:
        yield conn


def count_events(conn: psycopg.Connection, agency: str) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM vehicle_events WHERE agency = %s", (agency,))
        row = cur.fetchone()
    return int(row[0]) if row else 0


def count_live(conn: psycopg.Connection, agency: str) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM vehicle_live WHERE agency = %s", (agency,))
        row = cur.fetchone()
    return int(row[0]) if row else 0


def count_quarantine_since(conn: psycopg.Connection, since: datetime) -> int:
    """Quarantine rows written after an instant.

    By time rather than by agency because the rows that matter most carry
    neither: a payload that fails validation never yields a ping, so the
    processor has no agency to record on the row it writes.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM quarantine WHERE quarantined_at >= %s", (since,))
        row = cur.fetchone()
    return int(row[0]) if row else 0


def delete_agency_rows(conn: psycopg.Connection, agency: str) -> None:
    with conn.cursor() as cur:
        cur.execute("DELETE FROM vehicle_events WHERE agency = %s", (agency,))
        cur.execute("DELETE FROM vehicle_live WHERE agency = %s", (agency,))
        cur.execute("DELETE FROM quarantine WHERE agency = %s", (agency,))


def delete_agency_snapshots(settings: Settings, agency: str) -> int:
    """Remove the snapshots a scenario's synthetic agency archived.

    The collector archives before it decodes, so a feed-path experiment leaves
    its injected payload in the replay corpus under its own agency. Harmless to
    the corpus -- every reader filters by agency -- but it accumulates one
    directory per run, so each run removes its own.
    """
    archive = build_snapshot_archive(settings)
    root = getattr(archive, "root", None)
    if root is None:  # pragma: no cover - GCS archive, not reachable locally
        return 0
    target = root / agency
    if not target.exists():
        return 0
    removed = sum(1 for _ in target.rglob("*") if _.is_file())
    shutil.rmtree(target)
    return removed


def drain_subscriptions(settings: Settings) -> dict[str, int]:
    """Empty the processor and dead-letter subscriptions; report what was there.

    Acked, not handed back: a returned message is available again immediately,
    so a drain that nacked would leave the run measuring the previous one.
    """
    subscriber = pubsub_v1.SubscriberClient()
    drained: dict[str, int] = {}
    try:
        for name in (settings.subscription_processor, settings.subscription_dlq):
            path = subscriber.subscription_path(settings.gcp_project, name)
            total = 0
            while True:
                pulled = subscriber.pull(
                    request={"subscription": path, "max_messages": 1000}, timeout=15.0
                )
                if not pulled.received_messages:
                    break
                total += len(pulled.received_messages)
                subscriber.acknowledge(
                    request={
                        "subscription": path,
                        "ack_ids": [m.ack_id for m in pulled.received_messages],
                    }
                )
            drained[name] = total
    finally:
        subscriber.close()
    return drained


def dead_letter_depth(settings: Settings) -> int:
    """How many messages are sitting on the dead-letter subscription.

    Counted by pulling and handing straight back, because Pub/Sub exposes no
    depth to a client -- which is why the dashboard's original "DLQ depth"
    gauge could never have had a value (ADR-007).
    """
    subscriber = pubsub_v1.SubscriberClient()
    path = subscriber.subscription_path(settings.gcp_project, settings.subscription_dlq)
    seen: set[str] = set()
    try:
        while True:
            pulled = subscriber.pull(
                request={"subscription": path, "max_messages": 1000}, timeout=15.0
            )
            fresh = [m for m in pulled.received_messages if m.message.message_id not in seen]
            if pulled.received_messages:
                subscriber.modify_ack_deadline(
                    request={
                        "subscription": path,
                        "ack_ids": [m.ack_id for m in pulled.received_messages],
                        "ack_deadline_seconds": 0,
                    }
                )
            if not fresh:
                return len(seen)
            seen.update(m.message.message_id for m in fresh)
    finally:
        subscriber.close()


def _dsn_label(dsn: str) -> str:
    """Host and port of a DSN, so a result file says which database it used.

    Parsed rather than split on ``@``: the whole DSN carries a password and
    these files are committed, and splitting gets the no-credentials form
    wrong in a way that is invisible until someone reads the result file.
    """
    parsed = urlsplit(dsn)
    host = parsed.hostname or "unknown"
    return f"{host}:{parsed.port}" if parsed.port else host


@dataclass
class DrainResult:
    """One timed pass of the consumer: publish a backlog, then empty it."""

    concurrency: int
    pool_max: int
    backlog: int
    published: int
    processed: int
    rows: int
    publish_seconds: float
    drain_seconds: float
    latency: dict[str, Any] | None
    dsn_label: str = "default"
    processor: str = "in-process"

    @property
    def rate(self) -> float:
        return self.processed / self.drain_seconds if self.drain_seconds > 0 else 0.0

    def as_dict(self) -> dict[str, Any]:
        return {
            "concurrency": self.concurrency,
            "pg_pool_max": self.pool_max,
            "pg_dsn_host": self.dsn_label,
            "processor": self.processor,
            "backlog": self.backlog,
            "published": self.published,
            "processed": self.processed,
            "rows_written": self.rows,
            "publish_seconds": round(self.publish_seconds, 3),
            "drain_seconds": round(self.drain_seconds, 3),
            "drain_per_second": round(self.rate, 1),
            "latency": self.latency,
        }


def drain_backlog(
    settings: Settings,
    *,
    backlog: int,
    concurrency: int,
    pool_max: int,
    dsn: str | None = None,
    processor_url: str | None = None,
    keep_rows: bool = False,
) -> DrainResult:
    """Publish a backlog under a fresh agency, then time emptying it.

    ``dsn`` points the whole write path at a different database -- through
    the environment, because the processor builds its own sink inside its
    lifespan -- and the row counting follows it, so the pass that wrote the
    rows is the pass that counts them.
    """
    agency = chaos_agency("drain")
    target = settings if dsn is None else settings.model_copy(update={"pg_dsn": dsn})
    drain_subscriptions(settings)

    load = build_load(settings, agency=agency, count=backlog)
    with database(target) as conn:
        delete_agency_rows(conn, agency)

    publish_started = time.monotonic()
    published = publish(settings, load.pings)
    publish_seconds = time.monotonic() - publish_started

    # The pool size has to be in place before the bridge enters the processor's
    # lifespan, because that is where the sink -- and its pool -- is built.
    if processor_url is not None:
        # The processor owns its own pool and its own database in this mode;
        # overriding them here would change nothing and would misreport what
        # the run used.
        report = deliver(
            settings,
            seconds=1800.0,
            max_messages=published,
            concurrency=concurrency,
            url=processor_url,
        )
    else:
        overrides = {"TP_PG_POOL_MAX": str(pool_max), "TP_PG_POOL_MIN": "2"}
        if dsn is not None:
            overrides["TP_PG_DSN"] = dsn
        with settings_override(**overrides):
            report = deliver(
                settings,
                seconds=1800.0,
                max_messages=published,
                concurrency=concurrency,
            )

    with database(target) as conn:
        rows = count_events(conn, agency)
        profile = latency_percentiles(conn, agency)
        if not keep_rows:
            delete_agency_rows(conn, agency)

    result = DrainResult(
        concurrency=concurrency,
        pool_max=pool_max,
        backlog=backlog,
        published=published,
        processed=report.processed,
        rows=rows,
        publish_seconds=publish_seconds,
        drain_seconds=report.seconds,
        latency=profile.as_dict() if profile else None,
        dsn_label=_dsn_label(target.pg_dsn_str),
        processor="separate process" if processor_url else "in-process",
    )
    log.info("chaos.drain", **result.as_dict())
    return result


# ----------------------------------------------------------------------
# latency, read back out of the rows
# ----------------------------------------------------------------------
_PERCENTILES = (0.5, 0.95, 0.99)

# Exact distributions over the rows one run wrote. `percentile_cont` rather
# than `percentile_disc` so a percentile between two samples interpolates
# instead of snapping to a neighbour, which matters at these row counts.
_LATENCY_SQL = """
    SELECT
        count(*),
        percentile_cont(%(ps)s) WITHIN GROUP (
            ORDER BY extract(epoch FROM processed_time - ingest_time)),
        percentile_cont(%(ps)s) WITHIN GROUP (
            ORDER BY extract(epoch FROM processed_time - event_time)),
        percentile_cont(%(ps)s) WITHIN GROUP (
            ORDER BY extract(epoch FROM ingest_time - event_time)),
        max(extract(epoch FROM processed_time - ingest_time)),
        max(extract(epoch FROM processed_time - event_time))
    FROM vehicle_events
    WHERE agency = %(agency)s
"""


@dataclass(frozen=True)
class LatencyProfile:
    """The two latencies that answer different questions, and the gap between.

    ``processing`` is ``processed_time - ingest_time``: time the pipeline spent,
    and the only one of the three that is purely a property of this machine.

    ``end_to_end`` is ``processed_time - event_time``, which is what the SLO is
    written against. From a replayed corpus it is the sum of the processing
    time and ``corpus_age`` below, because re-basing preserves how old each
    ping already was when it was captured.

    ``corpus_age`` is ``ingest_time - event_time``: entirely a property of the
    capture, nothing to do with the pipeline, reported so the other two can be
    reconciled instead of taken on trust.
    """

    rows: int
    processing: dict[str, float]
    end_to_end: dict[str, float]
    corpus_age: dict[str, float]
    processing_max: float
    end_to_end_max: float

    def as_dict(self) -> dict[str, Any]:
        return {
            "rows": self.rows,
            "processing_seconds": self.processing,
            "end_to_end_seconds": self.end_to_end,
            "corpus_age_seconds": self.corpus_age,
            "processing_max_seconds": self.processing_max,
            "end_to_end_max_seconds": self.end_to_end_max,
        }


def latency_percentiles(conn: psycopg.Connection, agency: str) -> LatencyProfile | None:
    """Percentiles of all three intervals, or ``None`` if the run wrote nothing."""
    with conn.cursor() as cur:
        cur.execute(_LATENCY_SQL, {"ps": list(_PERCENTILES), "agency": agency})
        row = cur.fetchone()
    if row is None or not row[0]:
        return None

    def labelled(values: Sequence[float] | None) -> dict[str, float]:
        if values is None:
            return {}
        return {
            f"p{int(p * 100)}": round(float(v), 6)
            for p, v in zip(_PERCENTILES, values, strict=True)
        }

    return LatencyProfile(
        rows=int(row[0]),
        processing=labelled(row[1]),
        end_to_end=labelled(row[2]),
        corpus_age=labelled(row[3]),
        processing_max=round(float(row[4]), 6),
        end_to_end_max=round(float(row[5]), 6),
    )


# ----------------------------------------------------------------------
# Prometheus, for the series the dashboards and alerts read
# ----------------------------------------------------------------------
def prometheus_value(expr: str) -> float | None:
    """Evaluate one instant query, or ``None`` if it selects nothing.

    Returned rather than raised on an empty result, because "no series" is a
    real answer here and frequently the interesting one -- an unincremented
    counter has no series at all, which is the failure mode the dashboard tests
    exist to catch.
    """
    try:
        response = httpx.get(f"{PROMETHEUS_URL}/api/v1/query", params={"query": expr}, timeout=15.0)
        response.raise_for_status()
        payload = response.json()
    except (httpx.HTTPError, ValueError) as exc:
        log.warning("chaos.prometheus_unreachable", error=str(exc), expr=expr)
        return None
    if payload.get("status") != "success":
        return None
    result = payload["data"]["result"]
    if not result:
        return None
    return float(result[0]["value"][1])


# ----------------------------------------------------------------------
# standing in for the upstream agency
# ----------------------------------------------------------------------
@dataclass
class ServedFeed:
    """A loopback feed server that counts the requests it was actually sent.

    The hit count is the measurement that makes the circuit-breaker experiment
    more than a state assertion: a breaker that reports itself open while
    requests keep arriving is not doing its job, and only the far end can say
    whether they arrived.
    """

    url: str
    hits: int = 0
    status_code: int = 200
    body: bytes = b""
    delay_seconds: float = 0.0


@contextmanager
def serve_feed(
    *,
    body: bytes = b"",
    status_code: int = 200,
    delay_seconds: float = 0.0,
) -> Iterator[ServedFeed]:
    """Serve one response over loopback for the duration of a block.

    The collector fetches over HTTP and handing it bytes directly would mean
    replacing its client, which is where the retry, timeout and failure-metric
    code lives -- the code these experiments exist to exercise.
    """
    import http.server

    state = ServedFeed(url="", status_code=status_code, body=body, delay_seconds=delay_seconds)

    class Handler(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_GET(self) -> None:
            state.hits += 1
            if state.delay_seconds:
                # Longer than the client's timeout, so the client gives up
                # first. The connection is held rather than refused, which is
                # the failure mode that costs a caller its whole timeout
                # budget instead of failing fast.
                time.sleep(state.delay_seconds)
            if state.status_code >= 400:
                self.send_error(state.status_code, "chaos: upstream is unwell")
                return
            self.send_response(200)
            self.send_header("Content-Type", "application/x-protobuf")
            self.send_header("Content-Length", str(len(state.body)))
            self.end_headers()
            self.wfile.write(state.body)

        def log_message(self, *_args: Any) -> None:
            """Silence the default stderr log; structlog owns output here."""

        def handle_error(self, *_args: Any) -> None:
            """Also silence the traceback a timed-out client leaves behind.

            The hung-upstream case abandons the connection while the handler is
            still sleeping, so writing the response raises. That is the
            scenario working, not a fault, and the default handler prints a
            traceback per request.
            """

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = True
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    state.url = f"http://127.0.0.1:{server.server_address[1]}/VehiclePositions.pb"
    try:
        yield state
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


@contextmanager
def settings_override(**overrides: str) -> Iterator[None]:
    """Change TP_ settings for the duration of a block, then put them back.

    Through the environment and the settings cache rather than by passing a
    modified ``Settings`` around, because the things being driven build their
    own: the collector's lifespan constructs the feed client, and the
    processor's constructs the sink and its connection pool. A hand-built
    client or sink would exercise none of that, and those lifespans are where
    the behaviour under test lives.

    Restored afterwards -- including the cache -- so one scenario cannot leak
    its configuration into the next.
    """
    previous = {key: os.environ.get(key) for key in overrides}
    os.environ.update(overrides)
    get_settings.cache_clear()
    try:
        yield
    finally:
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        get_settings.cache_clear()


def await_metric_export(*, seconds: float = 13.0) -> None:
    """Give the export interval and one scrape time to land.

    OTEL_METRIC_EXPORT_INTERVAL is 5s locally and prometheus.yml scrapes every
    5s, so a counter incremented now is not queryable for up to ten seconds.
    Reading it sooner reports the value from before the fault, which looks
    exactly like a fault that produced no metrics.
    """
    time.sleep(seconds)

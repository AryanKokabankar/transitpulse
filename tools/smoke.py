"""Drive every signal the dashboard reads, once, through its real owner.

A dashboard panel has three ways to show nothing, and they look identical: the
query is wrong, the metric is never emitted, or the thing it measures simply
has not happened yet. Only the third is acceptable, and the only way to tell
them apart is to make all of them happen on purpose and then look.

So this is not a load test -- ``tools.replay`` is the load test, and it is the
only thing allowed to produce a throughput number. This runs at trivial volume
and aims for *coverage*: one success and one failure of each kind, so that
every panel and every alert rule has a value behind it. Each signal is produced
by the component that owns it in production -- the real ``FeedClient`` against
a real socket, the real publisher, the real processor handler, the real
partition job -- because a driver that called ``.record()`` on the instruments
directly would make the dashboard pass while the pipeline emitted nothing,
which is the exact failure being guarded against.

What it deliberately provokes, and why each one is needed:

* a **failed** upstream fetch against a closed port, because a feed-failure
  panel and the circuit-breaker alert have nothing to show on a healthy day;
* a **successful** fetch and decode of an archived snapshot served over
  loopback, so fetch latency is not composed entirely of error paths;
* the same ping published **twice**, because deduplication is normal traffic
  (agencies re-send constantly) and the panel that separates it from loss is
  worthless if it is empty;
* a **malformed** payload published straight to the topic, bypassing the
  publisher, because that is what a bad producer does and it is the only way
  to see the quarantine path from outside the processor;
* a **partition maintenance** run, for the headroom gauge and the job
  freshness stamp.

    python tasks.py smoke
    python tasks.py smoke -- --json
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import http.server
import json
import socket
import sys
import threading
import time
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import structlog
from google.api_core import exceptions as gcp_exceptions
from google.cloud import pubsub_v1
from opentelemetry import metrics

from services.collector.feed import FeedClient, FeedError, decode_feed
from services.jobs import partition_maintenance
from tools.pushbridge import bridge
from tools.replay.corpus import EmptyCorpusError, load_corpus
from transitpulse_core.config import Settings, get_settings
from transitpulse_core.models import VehiclePing
from transitpulse_core.pubsub import EventPublisher
from transitpulse_core.snapshots import build_snapshot_archive
from transitpulse_core.telemetry import setup_telemetry

log = structlog.get_logger(__name__)

SERVICE_NAME = "transitpulse-smoke"

# Enough pings to put a few points in every histogram bucket that matters,
# small enough that the whole run is quicker than one Grafana refresh.
_PING_SAMPLE = 60

# A little longer than the 5s scrape_interval in observability/prometheus.yml.
# Coupling this driver to that file is deliberate: its whole job is to produce
# signals Prometheus can actually see, and two observations inside one scrape
# interval are indistinguishable from one.
_SCRAPE_SPACING_SECONDS = 6.0


@dataclass
class SmokeReport:
    """What was actually provoked. Counted from results, never from intent."""

    fetch_ok: bool = False
    fetch_failed_as_intended: bool = False
    published: int = 0
    delivered: int = 0
    processed: int = 0
    quarantined: int = 0
    deduped_candidates: int = 0
    partitions_created: int = 0
    headroom_days: int = 0
    seconds: float = 0.0

    def render(self) -> str:
        return "\n".join(
            [
                "",
                "  observability smoke results",
                "  " + "-" * 52,
                f"  upstream fetch ok      {self.fetch_ok}",
                f"  upstream failure seen  {self.fetch_failed_as_intended}",
                f"  published              {self.published:,}",
                f"  delivered              {self.delivered:,}",
                f"  processed              {self.processed:,}",
                f"  quarantined            {self.quarantined:,}",
                f"  duplicate pings sent   {self.deduped_candidates:,}",
                f"  partitions created     {self.partitions_created}",
                f"  partition headroom     {self.headroom_days} day(s)",
                f"  elapsed                {self.seconds:,.2f}s",
                "",
                "  Metrics reach Prometheus one export interval later; give it",
                "  OTEL_METRIC_EXPORT_INTERVAL plus one scrape before looking.",
                "",
            ]
        )

    def as_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = asdict(self)
        payload["seconds"] = round(self.seconds, 3)
        return payload


def _free_port() -> int:
    """A port nothing is listening on.

    Bound and released rather than picked by hand: a hardcoded "surely nothing
    uses this" port that turns out to be in use would turn the intended
    connection failure into a successful fetch of something else entirely, and
    the run would pass while proving nothing.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@contextlib.contextmanager
def _serve_payload(payload: bytes) -> Any:
    """Serve one archived feed body over loopback for the duration of a block.

    The collector fetches over HTTP and there is no way to hand it bytes
    directly without replacing the client, which would stop exercising the
    retry, timeout and metric code that matters here. An archived snapshot is
    the right body to serve: it is what the agency actually sent.
    """

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            self.send_response(200)
            self.send_header("Content-Type", "application/x-protobuf")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, *_args: Any) -> None:
            """Silence the default stderr access log; structlog owns output."""

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}/feed.pb"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


async def _fetch_once(settings: Settings) -> bool:
    """One fetch through the real client. Returns whether it succeeded."""
    client = FeedClient(settings)
    try:
        response = await client.fetch()
    except FeedError as exc:
        log.info("smoke.fetch_failed_as_intended", kind=str(exc.kind), error=str(exc))
        return False
    else:
        decoded = decode_feed(
            response.payload, agency=settings.feed_agency, ingest_time=datetime.now(UTC)
        )
        log.info("smoke.fetch_ok", pings=len(decoded.pings), bytes=len(response.payload))
        return True
    finally:
        await client.aclose()


def _rebase(ping: VehiclePing, now: datetime) -> VehiclePing:
    """Move a corpus ping's timestamps to now, keeping its identity.

    Simpler than the replay harness's re-basing, and deliberately so: the
    harness has to preserve the corpus's age *distribution* because it is
    measuring a latency percentile, whereas this needs one plausible value per
    histogram. The identity is left alone precisely so that publishing the
    same ping twice collides on the idempotency key.
    """
    return ping.model_copy(update={"event_time": now, "ingest_time": now})


def _publish_malformed(settings: Settings) -> None:
    """Publish a body the processor cannot possibly accept.

    Published with the raw client rather than ``EventPublisher``, because
    ``EventPublisher`` takes a validated ``VehiclePing`` and so cannot express
    this. That is the right division: the quarantine path exists for producers
    that are not this codebase, and the only honest way to test it is to be
    one.
    """
    client = pubsub_v1.PublisherClient()
    topic = client.topic_path(settings.gcp_project, settings.topic_vehicle_events)
    future = client.publish(topic, data=b'{"schema_version": 1, "agency": "smoke"}')
    future.result(timeout=30)


async def _settle_metrics() -> None:
    """Export what has been recorded so far, then let one scrape go by.

    Both halves are needed. The flush puts the current cumulative values in
    front of the Collector immediately instead of up to an export interval
    later; the wait lets Prometheus actually scrape them, so the next phase's
    increment lands as a *second*, larger sample rather than being merged into
    the first. Without the pause there is one value to rate() over, and
    one value is not a rate.
    """
    provider = metrics.get_meter_provider()
    flush = getattr(provider, "force_flush", None)
    if flush is not None:
        flush(timeout_millis=10_000)
    await asyncio.sleep(_SCRAPE_SPACING_SECONDS)


async def _run(settings: Settings) -> SmokeReport:
    report = SmokeReport()
    started = time.monotonic()

    # --- upstream: one failure, then successes, deliberately spaced -----
    dead = settings.model_copy(update={"feed_url": f"http://127.0.0.1:{_free_port()}/feed.pb"})
    report.fetch_failed_as_intended = not await _fetch_once(dead)
    await _settle_metrics()

    archive = build_snapshot_archive(settings)
    payloads = archive.list_payloads(agency=settings.feed_agency, limit=1)
    if payloads:
        with _serve_payload(archive.load_payload(payloads[0])) as url:
            fed = settings.model_copy(update={"feed_url": url})
            # Twice, a scrape apart, and not out of thoroughness. A latency
            # percentile is histogram_quantile over rate(), rate() needs the
            # counter to have *increased* between two scrapes, and a histogram
            # observed in a single burst holds the same cumulative value at
            # every scrape thereafter -- so a perfectly correct query returns
            # NaN and the panel reads "No data". The collector ticking every
            # 15s in production never meets this; a verification run that
            # fires everything at once meets it every time.
            report.fetch_ok = await _fetch_once(fed)
            await _settle_metrics()
            report.fetch_ok = await _fetch_once(fed) and report.fetch_ok
            await _settle_metrics()
    else:
        log.warning("smoke.no_corpus_snapshot", hint="run `python tasks.py capture` first")

    # --- publish: a sample, then the same sample again ------------------
    corpus = load_corpus(archive, agency=settings.feed_agency, limit=None)
    now = datetime.now(UTC)
    sample = [_rebase(ping, now) for ping in corpus.pings[:_PING_SAMPLE]]

    publisher = EventPublisher(settings)
    try:
        report.published = publisher.publish_batch(sample)
        # The duplicate pass. Same keys, so every one of these is a
        # deduplication once the processor writes it.
        report.published += publisher.publish_batch(sample)
        report.deduped_candidates = len(sample)
    finally:
        publisher.close()

    _publish_malformed(settings)
    report.published += 1

    # --- consume: through the real handler ------------------------------
    # Twice over, sequentially rather than in one pass: a duplicate only
    # deduplicates if the original landed first, and one concurrent pass could
    # interleave the pair so that both inserts raced and neither conflicted.
    for _ in range(2):
        bridged = await bridge(
            settings,
            duration=timedelta(seconds=60),
            max_messages=len(sample) + 1,
            concurrency=1,
            url=None,
        )
        report.delivered += bridged.delivered
        report.processed += bridged.processed
        report.quarantined += bridged.quarantined

    # --- jobs ------------------------------------------------------------
    outcome = partition_maintenance.run(settings)
    report.partitions_created = len(outcome.created)
    report.headroom_days = outcome.headroom_days

    report.seconds = time.monotonic() - started
    return report


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="smoke",
        description="Provoke every metric the dashboard and the alert rules read.",
    )
    parser.add_argument("--json", action="store_true", help="emit the report as JSON on stdout")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    # Its own service name: this is a client of the pipeline, not part of it,
    # and metrics it provokes should not be attributed to a service that was
    # never running.
    setup_telemetry(SERVICE_NAME, instrument_libraries=False)
    settings = get_settings()

    try:
        report = asyncio.run(_run(settings))
    except EmptyCorpusError as exc:
        print(f"error: {exc}; run `python tasks.py capture` first", file=sys.stderr)
        return 2
    except gcp_exceptions.NotFound:
        print(
            "error: the topic or subscription does not exist; run `python tasks.py topology` first",
            file=sys.stderr,
        )
        return 2

    if args.json:
        print(json.dumps(report.as_dict(), indent=2))
    else:
        print(report.render())
    return 0


if __name__ == "__main__":  # pragma: no cover - module entry point
    raise SystemExit(main())

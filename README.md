# TransitPulse

[![ci](https://github.com/AryanKokabankar/transitpulse/actions/workflows/ci.yml/badge.svg)](https://github.com/AryanKokabankar/transitpulse/actions/workflows/ci.yml)

A cloud-native streaming pipeline for real-time transit telemetry. It ingests
live [GTFS-Realtime](https://gtfs.org/realtime/reference/) vehicle feeds,
enriches each position in flight with motion and schedule-adherence data, and
writes to two stores chosen for their opposite access patterns: PostgreSQL for
live serving, BigQuery for analytical history.

The point of the project is **operability**, not the data. Every event is
traceable end to end across a message broker, every rejection is kept and
replayable, and the service-level objective is defined, measured and alerted on
as code.

> **Status: Phases 0–3 and 6 complete.** The pipeline runs end to end locally —
> feed to broker to Postgres — with one trace spanning the broker, a dashboard
> whose every panel is verified against a live Prometheus, and the SLO defined
> and alerted on as rules. Five chaos experiments are scripted and repeatable,
> and every performance figure is measured rather than claimed:
> [BENCHMARKS.md](BENCHMARKS.md). Nothing is deployed to GCP yet; Terraform and
> Cloud Monitoring are Phase 4. See [Roadmap](#roadmap) for exactly what does
> and does not run today.

---

## Architecture

```
                    ┌─────────────────┐
  Cloud Scheduler──▶│  collector      │   Cloud Run service
   (every 15s)      │  - fetch feed   │   - protobuf decode
                    │  - normalise    │   - raw snapshot ──▶ GCS (replay corpus)
                    └────────┬────────┘
                             │ publish, ordering_key=vehicle_id
                             │ attributes: traceparent, schema_version
                             ▼
                    ┌─────────────────┐
                    │ Pub/Sub topic   │──▶ DLQ (after 5 delivery attempts)
                    │ vehicle-events  │         │
                    └────────┬────────┘         └──▶ dlq-replayer (Cloud Run Job)
                             │ push subscription
                             ▼
                    ┌─────────────────────────────────┐
                    │  processor  (Cloud Run)         │
                    │  1. validate  → quarantine on failure
                    │  2. dedupe    → idempotency key
                    │  3. enrich    → speed, schedule delay
                    │  4. dual write                  │
                    └───────┬──────────────┬──────────┘
                            │              │
              ┌─────────────▼───┐   ┌──────▼──────────────────┐
              │ Cloud SQL       │   │ BigQuery                │
              │ (PostgreSQL 16) │   │ vehicle_events          │
              │ - vehicle_live  │   │ - PARTITION BY DATE     │
              │ - vehicle_events│   │ - CLUSTER BY route_id   │
              │   (partitioned) │   │ - Storage Write API     │
              │ - quarantine    │   └─────────────────────────┘
              └─────────────┬───┘
                            ▼
                    ┌─────────────────┐
                    │  serving API    │  Cloud Run
                    │  /vehicles/live │
                    │  /routes/{id}/reliability
                    └─────────────────┘

  All four services export OTLP to a Collector → Cloud Trace + Cloud Monitoring
  (locally: → Jaeger + Prometheus + Grafana)
```

### Three decisions worth explaining

**Trace context crosses the broker.** Pub/Sub is a process boundary that normal
auto-instrumentation cannot see across, so the publisher's trace and the
consumer's trace end up unrelated. The collector injects the W3C `traceparent`
into Pub/Sub *message attributes* and the processor extracts it, producing one
trace spanning `fetch → decode → publish → consume → enrich → Postgres →
BigQuery`. This is what makes "where did the latency go" answerable.

**Two sinks, because the questions differ.** `vehicle_live` is one hot row per
vehicle, upserted, for sub-50ms point lookups. BigQuery is append-only,
date-partitioned and route-clustered, for scanning 90 days of history. Serving
a live map from the partitioned history would mean a scan per request; running
reliability analytics against the serving table would mean an index that
doesn't help.

**Nothing is dropped silently.** A malformed event goes to the `quarantine`
table with its raw payload and trace id. A message that fails five deliveries
goes to a dead-letter topic and is replayed by a job once the bug is fixed. A
pipeline that drops bad rows quietly looks perfectly healthy while losing data.

---

## Quick start

Requires Python 3.11+ and Docker. No GCP account, no cloud spend.

```bash
python tasks.py bootstrap     # .venv + dependencies + .env
python tasks.py up            # Postgres, Pub/Sub emulator, OTel, Jaeger, Prometheus, Grafana
python tasks.py migrate       # apply the schema
python tasks.py topology      # create topics, subscription and DLQ
python tasks.py check         # lint + mypy --strict + unit tests
```

Then collect something. The default feed is MBTA's vehicle positions, which
needs no API key, so this works on a fresh clone:

```bash
python tasks.py capture -- --count 5 --interval 15   # build a replay corpus
python tasks.py replay  -- --rate 500 --duration 10s # replay it into Pub/Sub
python tasks.py bridge  -- --duration 30s            # deliver it to the processor
```

`bridge` is a local stand-in for the push subscription. In production Pub/Sub
POSTs each message to the processor's Cloud Run URL; locally the broker is an
emulator in a container and the processor is a process on the host, which is the
one thing the emulator cannot reach. The bridge pulls from the same
subscription and performs the same POST, carrying the broker's own delivery
count, so the processor code on the path is the production code.

To put a value behind every panel and alert rule in one command:

```bash
python tasks.py smoke    # one of every signal, through the real components
```

A scheduler tick can also be triggered by hand, which is how the collector runs
in production (Cloud Scheduler calls the same endpoint):

```bash
TP_SERVICE=collector python -m uvicorn services.collector.main:app --port 8080
curl -XPOST localhost:8080/collect
```

| Service | URL |
|---|---|
| Jaeger (traces) | http://localhost:16686 |
| Prometheus (metrics) | http://localhost:9090 |
| Grafana (dashboards) | http://localhost:3000 — `admin` / `admin` |
| Postgres | `localhost:5432` — `transitpulse` / `transitpulse` |
| Pub/Sub emulator | `localhost:8085` |

`python tasks.py help` lists every task. A `Makefile` with the same targets is
included for Linux and CI; `tasks.py` exists because `make` is not present on a
stock Windows box and this project is developed on one.

---

## Layout

```
services/
  collector/    main.py (POST /collect) + feed.py (fetch, retry, decode)
  processor/    validate, dedupe, enrich, dual-write
  api/          serving reads over vehicle_live and the history
  jobs/         gtfs_static_loader, partition_maintenance,
                dlq_replayer, nightly_aggregate
libs/transitpulse_core/
  models.py     the data contract (three timestamps, idempotency key)
  telemetry.py  OTel bootstrap + cross-broker trace propagation
  pubsub.py     ordered publishing, push-delivery parsing
  resilience.py circuit breaker guarding the upstream feed
  snapshots.py  the replay corpus: one interface over disk and GCS
  geo.py        haversine, bearing, the implausible-speed gate
  sinks/        postgres.py (serving) + bigquery.py (history)
tools/
  capture.py    one-shot fetch-and-archive, to build a corpus
  replay/       load harness: replays the corpus at any rate
  pushbridge.py local stand-in for the push subscription
  smoke.py      provokes one of every metric, for dashboard verification
  chaos/        five scripted failures, each ending in a counted assertion
  durability.py the same load against fsync=off and fsync=on Postgres
  screenshots.py dashboard panels as PNGs, verified not to be error images
infra/terraform/ Pub/Sub, Cloud SQL, BigQuery, Cloud Run, IAM, WIF, SLOs,
                canary traffic splitting (validated, never applied)
observability/  collector config, Prometheus config and rules,
                Grafana dashboards and provisioning (all in git)
migrations/     hand-written Alembic migrations
docs/adr/       why things are the way they are
docs/benchmarks/ raw measurements, written by the runs that made them
docs/dashboards/ panel screenshots captured during a chaos run
```

---

## The collector, and what a tick does

`POST /collect` is one scheduler tick. The order of work inside it is the part
worth knowing:

1. **Fetch** with a timeout, retried on transient failures with exponential
   backoff and jitter, all inside a circuit breaker. Retries ride out a dropped
   connection; the breaker stops the service spending every tick on an upstream
   that has been dead for ten minutes. A tick arriving at an open breaker costs
   one rejection instead of four timeouts, and returns 503 with `Retry-After`.
2. **Archive the raw bytes — before decoding.** The payload most worth having
   is the one that breaks the decoder, and it exists in memory only during this
   tick. See [ADR-006](docs/adr/006-replay-corpus-as-the-only-benchmark.md).
3. **Decode and partition.** Valid entities become `VehiclePing`s; the rest
   become `QuarantineRecord`s with a reason — `NULL_ISLAND` for a (0, 0)
   coordinate, `SCHEMA_VALIDATION` for a 400-degree bearing — written beside
   the payload that produced them. An unrecognised GTFS enum value degrades to
   `UNKNOWN` rather than rejecting a position that is otherwise fine.
4. **Publish** through the ordered publisher, one batch per tick, and report
   only what the broker acknowledged.

The whole tick runs inside one span, so a single trace covers
`fetch → decode → publish` with the `traceparent` that leaves on each message
belonging to that tick.

## Load testing

The harness is the only thing in the project allowed to produce a throughput
number, and it is built to make an inflated one hard:

```bash
python tasks.py replay -- --rate 500 --duration 10s
python tasks.py replay -- --rate 2000 --duration 2m --fanout 20
python tasks.py replay -- --rate 500 --duration 10s --dry-run   # pacing only
```

- **Achieved rate is measured, never assumed** — acknowledged publishes over
  elapsed monotonic time, with the in-flight drain inside the measured window.
- **The report names the bottleneck.** Target, achieved rate and the limiter's
  total wait are printed together: a limiter that barely waited was never the
  constraint, so a shortfall belongs to the publisher.
- **It says what will deduplicate.** Agencies re-send pings they have already
  sent, and that share of a run exercises the dedupe path rather than the write
  path, so the corpus's duplicate-key count is reported.
- **`--dry-run`** runs the identical loop with the publish removed, which is how
  the limiter's own accuracy is established independently of the pipeline.
- **`--fanout K`** clones each ping into K synthetic vehicles with distinct ids
  and jittered positions, so volume is a parameter while the data stays real.
- **It exits non-zero when the target was missed**, below `--min-accuracy`
  (default 0.95), so a performance gate can fail a build without parsing
  output. `--json` emits the whole report for the same reason.

## Chaos experiments

Every resilience claim this project makes is a claim about behaviour under a
fault that does not occur on a healthy day — the queue absorbs a backlog,
nothing is dropped silently, the breaker stops calling a dead upstream, a
replay is free. A README can assert all four. The only way to know is to cause
the fault and count what came out, so each one is a committed script rather
than a write-up:

```bash
python tasks.py chaos -- --list
python tasks.py chaos -- all              # all five, in order
python -m tools.chaos.kill_processor --messages 2000    # or one at a time
```

Each run ends in checks, not prose: `expected 2000, measured 2000`. Each writes
its raw measurements to `docs/benchmarks/<name>.json`, and exits non-zero if any
check failed, so the set can gate a build. Three rules keep them honest — each
run invents its own agency so every count is of rows that run created, the
corpus is filtered to one ping per idempotency key so "zero loss" is a
reachable assertion rather than a defeat by deduplication, and the result file
is written by the run itself so the prose can be checked against it.

Two of the five earned their shape by first being wrong. The kill experiment
originally refused deliveries for eight seconds against a 1,600-message backlog
and dead-lettered nothing — a refused connection costs about two seconds on
loopback and the broker hands out fresh messages rather than the same ones, so
every message collected one refusal and none came near five. It reported zero
loss while never testing the path that loses data. It now holds back a bounded
number on purpose and refuses until the subscription has emptied into the
dead-letter topic, and a check insists that it did.

## Benchmarks

Measured on an Intel Core Ultra 7 155H (16 cores, 15.5 GiB) under Docker
Desktop, replaying a 7-snapshot MBTA corpus. Every figure names the command
that produced it, and each run's raw output is committed in `docs/benchmarks/`.

| Question | Measured | From |
|---|---|---|
| How fast can it publish? | **981 msg/s** — the Pub/Sub *emulator*'s ceiling, not Pub/Sub's | `chaos ramp` |
| How fast can it consume? | **~50 events/s** (49.5–58 across four runs), peaking at concurrency 16 | `chaos ramp` |
| What is the pipeline's ceiling? | **49.5 events/s — consumer-bound, by a factor of 20** | `chaos ramp` |
| How long does the pipeline take per event? | p50 **0.72 s**, p95 1.16 s, p99 **1.34 s** (`ingest → processed`, steady state) | `chaos ramp` |
| And end to end? | p50 **147.9 s** — of which **147.0 s** is how old MBTA's own reports already were | `chaos ramp` |
| Does the harness outrun what it measures? | Yes, 16,014/s with the publish removed — 16× the publisher | `chaos ramp` |
| Is a backlog lost when the processor dies? | **No. 2,000 published, 2,000 rows** — including 40 recovered from the DLQ | `chaos kill-processor` |
| Is a bad record ever dropped silently? | **No. 294 published + 73 quarantined = 367 entities**, exactly | `chaos malformed-feed` |
| Does the breaker really stop calling upstream? | **0 requests reached it while open**, counted at the far end | `chaos feed-failure` |
| What does replaying a duplicate batch cost? | **0 rows added**, 1,000 counted as deduplicated | `chaos duplicate-batch` |
| How much is `fsync=off` worth here? | **Nothing resolvable** — a 3.6/s gap against a 6.4/s noise floor | `durability` |

The three caveats that would make any of these misleading quoted alone: the
local Postgres runs without `fsync`, a rate the limiter was still pacing is not
a ceiling, and "end-to-end lag" from a replayed corpus includes the age each
event already had when it was captured. [BENCHMARKS.md](BENCHMARKS.md) states
all three next to the numbers they qualify, with the methodology and the
hardware.

## Observability

Metrics are named so each answers an operational question:

| Metric | Question |
|---|---|
| `transitpulse.ingest.lag` | Is it keeping up? *(the SLI)* |
| `transitpulse.events.quarantined` | Is it losing data, and why? |
| `transitpulse.events.deduped` | How much upstream traffic is repeats? |
| `transitpulse.events.dead_lettered` | Has anything been lost? |
| `transitpulse.events.replayed` | Has anything been recovered? |
| `transitpulse.sink.write.duration` | Which sink is slow? |
| `transitpulse.processing.duration` | Is the latency ours or upstream's? |
| `transitpulse.partitions.headroom` | How many days of writes are left? |
| `transitpulse.job.last_success_timestamp` | Is the schedule still firing? |

Those names are not the names queries are written against. The Collector's
Prometheus exporter rewrites every one: dots become underscores, counters gain
`_total`, and the unit becomes a suffix — so `transitpulse.ingest.lag` with
`unit="s"` is queried as `transitpulse_ingest_lag_seconds_bucket`. Changing a
unit silently renames a series and empties a panel, which is why the mapping is
derived from the registry and asserted rather than remembered.

### The SLO

**99% of events ingested within 2s**, where lag is `processed_time −
event_time`, over a rolling 30 days.

The 2s is derived: Cloud Scheduler ticks the collector every 15s, so a 2s
ingest budget means ingestion is never the reason a position is stale — the
feed's own cadence is, by an order of magnitude. The lag histogram's buckets
are placed around that boundary rather than left at SDK defaults. **The 99% is
a chosen target, not a measurement** — there is no production traffic to derive
an achievable percentile from, and saying otherwise would be the kind of
unearned number this project exists not to publish.

It is defined once, as recording rules in `observability/prometheus/rules/`,
and both the dashboard and the alerts read those recorded series. That is
partly cost — the dashboard was recomputing three histogram quantiles on every
ten-second refresh, per open tab — and mostly so the tile claiming compliance
and the rule that pages about it cannot drift apart.

### Alerting

Nine rules, covering what this pipeline has actually hit or deliberately
designed for. Every threshold in the files is either derived from something or
labelled as a guess.

| Alert | Fires on | Severity |
|---|---|---|
| `IngestLagBurnFast` | 14.4x budget burn over 1h **and** 5m | page |
| `IngestLagBurnSlow` | 6x burn over 6h **and** 30m | page |
| `IngestLagP95High` | p95 above the 2s objective for 10m | ticket |
| `EventsDeadLettered` | anything dead-lettered at all | page |
| `QuarantineRateSpike` | >5% of events rejected *(a guess)* | ticket |
| `FeedCircuitOpen` | the collector has stopped calling upstream | page |
| `FeedFailing` | upstream failing but not yet circuit-breaking | ticket |
| `PartitionHeadroomLow` | under 3 days of partitions left | page |
| `PartitionJobStale` | partition job last succeeded over 26h ago | ticket |

The rules are mounted into Prometheus, which runs with `--web.enable-lifecycle`,
so editing one takes effect on `curl -XPOST localhost:9090/-/reload`. The
integration tests compare the rules Prometheus is actually holding against the
files on disk, which is how a rules directory that failed to mount gets caught
rather than being syntactically perfect and completely absent.

Burn-rate alerting pairs a long window with a short one that must agree,
because the long window alone keeps paging after recovery and the short one
alone pages for every blip. Each alert carries a `runbook` annotation saying
what to look at first; the two capacity rules exist because partitions really
did run out mid-development and every insert was refused with no warning signal
at all. [ADR-007](docs/adr/007-slo-and-alerting-as-code.md) has the reasoning,
including what the local rules cannot express and why Phase 4 restates the same
objective in Cloud Monitoring.

### Why the dashboard is tested

It was written before a single metric had ever been emitted, so its PromQL was
composed against assumed series names — and the SLO tile, the one number the
project is judged on, selected `le="2"` for a bucket Prometheus stores as
`le="2.0"` and had been empty since the day it was committed. A "No data" panel
is worse than a missing one, because the dashboard still looks finished.

So two tests hold it in place. A unit test derives the series names the registry
can actually produce and fails if a panel queries anything else — it needs no
Docker, so it runs on every push. An integration test reads the expressions out
of the committed JSON, drives real load, and fails if any one of them selects
nothing. Neither restates a query: a test that duplicated the PromQL would pass
while the dashboard was broken, which is the exact failure being guarded
against.

Deduplicated events are counted separately from quarantined ones: an agency
re-sending a ping it already sent is normal traffic, not loss, and conflating
the two makes the delivery-rate metric meaningless. For the same reason every
counter whose label values are known at startup is declared at zero — an
unincremented counter has no series, and "no series" looks exactly like zero on
a dashboard while meaning that the process never started.

---

## Testing

```bash
python tasks.py test              # unit, no Docker needed
python tasks.py test-integration  # against the running stack
```

Integration tests run against **real** Postgres and a **real** Pub/Sub
emulator, in CI too — the bugs worth catching live in the SQL and the broker
semantics, and a mock of either one would have the bug as well. CI additionally
asserts that every migration rolls back and re-applies cleanly.

The decoder is tested against synthetic protobuf built with the real bindings,
including the payloads that matter: null-island coordinates, out-of-range
bearings, missing timestamps, enum values newer than this contract, an HTML
error page served with HTTP 200, and a non-compliant producer that omits the
proto2-`required` coordinates. The upstream feed is stubbed throughout — a test
that depended on a live agency endpoint would fail for reasons unrelated to
this code.

---

## Roadmap

- [x] **Phase 0** — tooling, data contract, telemetry library, sinks, schema,
      local stack, Docker image, CI
- [x] **Phase 1** — collector handler, snapshot archiving, replay harness
- [x] **Phase 2** — processor handler, enrichment, static GTFS loader
- [x] **Phase 3** — dashboards and SLO alerting wired end to end
- [x] **Phase 6** — chaos experiments, DLQ replayer, `BENCHMARKS.md`
- [x] **Phase 4** — Terraform, Workload Identity Federation — *written and
      validated, never applied*
- [x] **Phase 5** — canary releases, nightly performance regression gate

Phase 6 ran ahead of 4 and 5 deliberately: a performance regression gate needs
a measured baseline to regress against, and there was none.

**Phases 4 and 5 are the one place this project cannot show its working.**
There is no GCP project behind this repository and no spend budget, so the
infrastructure has never been through `terraform apply`. It is `fmt`-checked,
initialised against the real providers and `validate`-d on every push — which
caught a dependency cycle between the push subscription and the Cloud Run
service it delivers to — but validation cannot see a quota, an org policy, or
a field the API only rejects at apply time. The deploy workflow is
`workflow_dispatch` only for the same reason: an automatic trigger would fail
on every push and teach the reader to ignore a red badge.
[`infra/terraform/README.md`](infra/terraform/README.md) says what that does
and does not establish; [ADR-008](docs/adr/008-deploy-without-keys-and-release-without-rebuilding.md)
covers the keyless auth and the canary.

Every figure in [BENCHMARKS.md](BENCHMARKS.md) names the command that produced
it, and the raw output of each run is committed beside it in
`docs/benchmarks/`. **This README does not claim a throughput or latency figure
that the harness has not measured** — and where a number is bounded by the
measuring tool, by a limiter, or by a database running without `fsync`, it says
so next to the number rather than in a footnote.

---

## License

MIT

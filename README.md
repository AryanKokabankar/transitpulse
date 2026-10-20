# TransitPulse

A cloud-native streaming pipeline for real-time transit telemetry. It ingests
live [GTFS-Realtime](https://gtfs.org/realtime/reference/) vehicle feeds,
enriches each position in flight with motion and schedule-adherence data, and
writes to two stores chosen for their opposite access patterns: PostgreSQL for
live serving, BigQuery for analytical history.

The point of the project is **operability**, not the data. Every event is
traceable end to end across a message broker, every rejection is kept and
replayable, and the service-level objective is defined, measured and alerted on
as code.

> **Status: Phase 1 complete.** The collector fetches, archives and publishes
> a real agency feed; the replay harness paces load from the archived corpus.
> The processor is Phase 2, so nothing consumes the topic yet and no rows are
> written. See [Roadmap](#roadmap) for exactly what does and does not run
> today.

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
infra/terraform/ Pub/Sub, Cloud SQL, BigQuery, Cloud Run, IAM, SLOs
observability/  collector config, Prometheus config, Grafana dashboards (in git)
migrations/     hand-written Alembic migrations
docs/adr/       why things are the way they are
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

## Observability

Metrics are named so each answers an operational question:

| Metric | Question |
|---|---|
| `transitpulse.ingest.lag` | Is it keeping up? *(the SLI)* |
| `transitpulse.events.quarantined` | Is it losing data, and why? |
| `transitpulse.events.deduped` | How much upstream traffic is repeats? |
| `transitpulse.sink.write.duration` | Which sink is slow? |
| `transitpulse.processing.duration` | Is the latency ours or upstream's? |
| `transitpulse.dlq.depth` | Is anything stuck? |

**SLO: p95 end-to-end lag < 2s**, measured as `processed_time − event_time`
over a rolling 1h window. The lag histogram's buckets are chosen around that
boundary rather than left at SDK defaults, and the Grafana dashboard
(`observability/grafana/dashboards/`) is committed as JSON so it can be read
without running anything.

Deduplicated events are counted separately from quarantined ones: an agency
re-sending a ping it already sent is normal traffic, not loss, and conflating
the two makes the delivery-rate metric meaningless.

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
- [ ] **Phase 2** — processor handler, enrichment, static GTFS loader
- [ ] **Phase 3** — dashboards and SLO alerting wired end to end
- [ ] **Phase 4** — Terraform, GCP deploy, Workload Identity Federation
- [ ] **Phase 5** — canary deploys, nightly performance regression gate
- [ ] **Phase 6** — chaos experiments, `BENCHMARKS.md`

Performance numbers will be published in `BENCHMARKS.md` with methodology and
hardware, generated by the replay harness in Phase 6. **This README will not
claim a throughput or latency figure that the harness has not measured.**

---

## License

MIT

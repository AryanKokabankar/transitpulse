# Benchmarks

Measured numbers for TransitPulse, with the commands that produced them and
the caveats that qualify them.

Before this document existed the honest answer to "how fast is it" was *nobody
has measured*. The README has always promised that no throughput or latency
figure would be claimed unless the harness measured it, which left the figure
unclaimed. This is that measurement.

It is written to be audited rather than believed. Every number below names the
command that produced it; every run writes its raw output to
[`docs/benchmarks/`](docs/benchmarks/), committed, so a reader can compare the
prose against the measurement instead of trusting the transcription. Where a
number is bounded by something other than the pipeline — the measuring tool, a
rate limiter, a database running without `fsync` — that is stated beside the
number and not in a footnote.

---

## Read this before quoting any number

Three things make a figure from this document misleading if it travels alone.

**1. Postgres here runs with `fsync=off` and `synchronous_commit=off.**
That is set in [`docker-compose.yml`](docker-compose.yml) and it removes the
single largest cost on the write path: waiting for the disk to confirm. Every
consumer-side figure is inflated by it. Rather than leave that as an unbounded
caveat, the same load is run against a second Postgres with durability left on
and both are reported — see [Durability](#durability-what-fsyncoff-is-worth).

**2. A rate the limiter was still pacing is not a ceiling.** The load harness
reports `publisher_bound` for exactly this reason: if the limiter barely
waited, the loop was running flat out and the shortfall belongs to the
publisher; if the limiter did the waiting, the run only shows that the
requested rate was achievable, not that a higher one was not. Ladders below
are climbed until a rung *fails*, and a ladder whose top rung still met its
target is reported as a floor, not a ceiling. The ramp experiment asserts this
rather than leaving it to the reader.

**3. "End-to-end lag" from a replayed corpus is not the pipeline's latency.**
The replay harness re-bases event times by default: the corpus's newest ping is
mapped onto now and every other ping keeps its offset behind it. That is the
right behaviour — the alternative dates events into the future and feeds
negative values into the SLI — but it means each replayed event is as old as it
was when captured. So:

```
end_to_end (event_time  -> processed_time)   =   corpus age  +  processing
processing (ingest_time -> processed_time)   =   the pipeline's own work
corpus age (event_time  -> ingest_time)      =   a property of the capture
```

All three are reported separately below, and they reconcile. With
`--preserve-timestamps` the harness keeps the original event times instead, and
then "end-to-end lag" measures *the age of the corpus* and is meaningless as a
latency figure. **Every latency number in this document comes from a rebased
run**; the mode is stated per table.

There is a fourth, subtler version of the same trap, and this document walked
into it before walking back out. In a **backlog drain** — publish *N*, then
empty the queue — every message shares one `ingest_time`, so
`processed_time - ingest_time` measures how long the queue took to empty and
grows with the backlog on an identically fast pipeline. Latency is therefore
measured only in the **steady-state** stage, where the publisher and the
consumer run together at a rate the consumer can hold and each batch is stamped
as a collector tick would stamp it.

---

## Hardware and environment

Captured from the machine that ran these, not described generically.

| | |
|---|---|
| CPU | Intel Core Ultra 7 155H — 16 physical cores (6 P + 8 E + 2 LP-E), 22 logical, 1.40 GHz base |
| Memory | 15.52 GiB |
| OS | Windows 11 Home, build 26300 |
| Python | 3.11.0 (MSC v.1933, 64-bit), in `.venv` |
| Docker | Docker Desktop 29.8.2 (WSL 2 backend) |
| Docker VM | 22 CPUs, 7.51 GiB (8,069,238,784 bytes) available to containers |

**Container resource limits: none are set.** `docker inspect` reports
`NanoCpus=0` and `Memory=0` for every service, so each container may use the
whole 22-CPU / 7.51 GiB VM and they compete rather than being isolated. This
matters for interpretation: the broker, the database and the observability
stack were all running during every measurement, and nothing was pinned.

Everything runs on one host. There is no network between the stages — the
publisher, the broker, the processor and the database are all on loopback —
so no figure here includes a realistic network hop.

### Software under test

| Component | Version |
|---|---|
| PostgreSQL | 16.15 (`postgres:16-alpine`) |
| Pub/Sub | Google Cloud CLI **emulator** (`cloudsdktool/google-cloud-cli:emulators`) |
| OpenTelemetry Collector | `otel/opentelemetry-collector-contrib:0.111.0` |
| Prometheus | `prom/prometheus:v2.55.0` |
| Grafana | `grafana/grafana:11.3.0` |
| Jaeger | `jaegertracing/all-in-one:1.62.0` |

| Library | Version |
|---|---|
| pydantic | 2.14.1 |
| psycopg (binary) / psycopg-pool | 3.3.6 / 3.3.3 |
| google-cloud-pubsub | 2.43.0 |
| protobuf / gtfs-realtime-bindings | 7.36.2 / 3.0.0 |
| fastapi / uvicorn | 0.143.2 / 0.54.0 |
| httpx | 0.28.1 |
| orjson | 3.13.0 |
| opentelemetry-sdk | 1.45.1 |

Postgres runs with `fsync=off`, `synchronous_commit=off`, `shared_buffers=256MB`,
`max_connections=200`, `pg_stat_statements` loaded. The application's
connection pool is `TP_PG_POOL_MAX=10` unless a run says otherwise.

**The Pub/Sub figures are the emulator's, not Pub/Sub's.** The emulator is a
single-process Java service on loopback with no quota enforcement, no
replication and no cross-zone anything. It is the wrong shape to extrapolate
from in either direction, and the publisher ceiling below should be read as
"what this local stand-in does", not as a statement about the managed service.

### The corpus

All load is replayed from archived real MBTA feed responses
([ADR-006](docs/adr/006-replay-corpus-as-the-only-benchmark.md)); nothing is
synthetic except the fan-out clones and the deliberately corrupted entities.

| | |
|---|---|
| Snapshots | 7 |
| Total bytes | 307,887 (mean 43,983 per snapshot) |
| Pings decoded | 2,573 |
| Distinct vehicles | 373 |
| Distinct idempotency keys | 1,925 |
| Pings sharing a key with another | 648 (**25.2%**) |
| Event-time span | 1,827 s (30.5 min) |

Two of those rows do real work in the sections below.

The **25.2% duplicate share** is the agency re-sending observations it has
already sent, which is normal traffic. Those messages deduplicate at the sink
by design, so a throughput figure that counted them would overstate the work
done, and a "rows written equals events published" assertion would fail on a
pipeline that lost nothing at all. Every experiment that counts rows therefore
filters the corpus to one ping per key first; experiment 4 does the opposite on
purpose.

The **1,827-second event-time span** is not capture duration — the seven
snapshots were taken over about four minutes. It is the spread of the
timestamps the agency itself reported: MBTA's feed carries vehicles whose last
position report is half an hour old. That spread lands inside every end-to-end
lag measurement taken from this corpus, which is the single most important
reason the three intervals above are reported separately.

---

## How to reproduce all of it

```bash
python tasks.py up                      # stack: Postgres, Pub/Sub, OTel, Prometheus, Grafana
python tasks.py migrate
python tasks.py topology
python tasks.py capture -- --count 7 --interval 15   # build a corpus of your own
python tasks.py chaos -- all            # the five experiments, in order
python tasks.py durability              # the fsync comparison (see below)
```

Each experiment is also runnable on its own, which is what to reach for when
checking a single claim:

```bash
python tasks.py chaos -- --list
python -m tools.chaos.kill_processor --messages 2000
python -m tools.chaos.ramp --rates 500,1000,2000,4000
```

Every run writes `docs/benchmarks/<name>.json` containing its checks, its raw
measurements and its caveats, and exits non-zero if any check failed. The
numbers quoted in this document are transcribed from those files.

---

## Resilience: the five experiments

Each is a committed script under [`tools/chaos/`](tools/chaos/) that causes a
fault and counts the result. The raw output of every run, including the checks
and their expected-versus-measured pairs, is in `docs/benchmarks/`.

### 1. Kill the processor mid-stream

```bash
python -m tools.chaos.kill_processor --messages 2000
```

Publish 2,000 events, consume all but a held-back remainder, terminate the
processor abruptly, keep delivering into the hole until the broker gives up,
restart, drain, then replay the dead letters.

| | |
|---|---|
| Published | 2,000 |
| Consumed before the kill | 1,960 |
| Held on the queue when it died | 40 |
| Deliveries refused while it was down | **200** (= 40 × 5 attempts, exactly the budget) |
| Dead-lettered by the broker | 40 |
| Recovered by `dlq_replayer` | 40 |
| Left on the dead-letter topic | 0 |
| **Rows in `vehicle_events`** | **2,000** |
| Rows in `vehicle_live` | 448 (= distinct vehicles) |
| Quarantined | 0 |

**Zero loss, counted: 2,000 published, 2,000 rows.** Every one of the 40
messages the broker gave up on came back through the replayer and landed.

The first version of this experiment dead-lettered *nothing* — it refused
deliveries against the whole 1,600-message backlog for eight seconds, and
because a refused connection costs about two seconds on loopback and the broker
hands out fresh messages rather than the same ones, every message collected one
refusal and none came near five. It reported zero loss while never exercising
the path that loses data. Holding back a bounded number and refusing until the
subscription empties is what makes the budget genuinely run out, and a check
now insists that it did.

*Caveat:* the local bridge retries a refused delivery immediately with no
backoff, so the delivery budget is consumed far faster here than a real push
subscription would consume it. The outage **duration** is not comparable to a
production one; the counts are.

### 2. Malformed protobuf at a known rate

```bash
python -m tools.chaos.malformed_feed --corrupt-every 5
```

A real archived snapshot, with every 5th otherwise-valid entity corrupted in
place, re-serialised, served over loopback and driven through the real
`POST /collect`. Three corruptions are rotated so a run exercises all of them.

| | |
|---|---|
| Vehicle entities in the feed | 367 |
| Valid in the baseline | 367 |
| **Injected corruptions** | **73** (25 null-island, 24 bearing > 360°, 24 position missing) |
| Collector published | 294 |
| Collector quarantined | **73** |
| Archive sidecar records | 73 (`NULL_ISLAND` 25, `SCHEMA_VALIDATION` 48) |
| Rows written for the valid events | 294 |
| Bad payloads published straight to the topic | 3 → **3** quarantine rows |

**The identity holds: 294 + 73 = 367.** Nothing was unaccounted for. The
injected count equals the rise in rejections, the valid entities in the same
batches still landed, and a null-island coordinate is still labelled as a GPS
fault rather than collapsed into a generic schema failure.

Only entities the decoder accepted in the baseline are corrupted — breaking one
that was already being rejected would change no count and the experiment would
pass having measured nothing.

The feed path and the broker path are checked against different stores on
purpose: the collector deliberately has no database
([ADR-006](docs/adr/006-replay-corpus-as-the-only-benchmark.md)), so its
rejections land in the snapshot archive beside the payload that produced them,
while a bad third-party producer's payloads reach the processor and land in the
`quarantine` table.

### 3. Upstream failure and the circuit breaker

```bash
python -m tools.chaos.feed_failure
```

The upstream is a loopback server **that counts the requests it is sent**. That
count, not the breaker's reported state, is the measurement: a breaker that
reports itself open while requests keep going out is not doing its job, and
only the far end can say.

| | 503s | Hung connections |
|---|---|---|
| Ticks until the breaker opened | 5 (the threshold) | 5 |
| Upstream requests consumed getting there | 20 (= 5 ticks × 4 retries) | 5 (1 attempt per tick) |
| Failure kind recorded | `http_error` ×5 | `timeout` ×5 |
| Further ticks attempted while open | 5 | 5 |
| **Upstream requests while open** | **0** | **0** |
| Ticks rejected outright by the breaker | 5 | 5 |

Recovery needed no intervention: after the cooldown the half-open probe was
admitted — **exactly one** request reached upstream — it succeeded, and the
breaker returned to `CLOSED`.

End to end through the collector's own handler: five ticks returned **502**
while the feed was merely failing, and the sixth returned **503 with
`Retry-After: 2`**, which is what lets Cloud Scheduler wait out the cooldown
rather than guess at it.

*Caveat:* the breaker cooldown is shortened from 60 s to 3 s, and the retry
budget to 1 attempt in the timeout and service parts, so a run takes seconds.
Neither changes what is measured — the breaker counts whole calls, not retry
attempts — only how long observing it takes.

### 4. An exact duplicate batch

```bash
python -m tools.chaos.duplicate_batch --messages 1000
```

The same 1,000 events published twice and delivered twice, sequentially. Built
once and reused: re-deriving the batch would risk a fresh timestamp somewhere
and make the keys differ, which is the bug this is looking for.

| | After pass 1 | After pass 2 |
|---|---|---|
| Delivered | 1,000 | 1,000 |
| Rows in `vehicle_events` | 1,000 | **1,000** |
| Rows in `vehicle_live` | 368 | **368** |

**The duplicate batch added 0 rows.** It was not skipped — all 1,000 were
delivered and processed — and the `events_deduped` counter rose by exactly
**1,000**.

That last number is the point. Flat row counts alone cannot distinguish
deduplication from silent loss; a pipeline that threw the second pass away
would also show flat counts. The two are counted separately for exactly this
reason, and only the counter tells them apart.

### 5. Ramp until something breaks

```bash
python -m tools.chaos.ramp --backlog 3000
```

Three ladders, because "the pipeline does N events a second" means nothing
without knowing which stage set N.

#### Pacing — can the measuring tool outrun what it measures?

With the publish removed (`--dry-run` in the harness, which exists for exactly
this question):

| Target | Achieved | Accuracy |
|---|---|---|
| 1,000/s | 1,008.5/s | 100.8% |
| 4,000/s | 4,040.0/s | 101.0% |
| 16,000/s | **16,014.4/s** | 100.1% |

The ladder **never broke**, so 16,014/s is a floor on the harness, not a
ceiling — and that is the useful shape. It is 16× the publisher's ceiling
below, so nothing further down is measuring the tool.

#### Publishing — the emulator's ceiling

| Target | Achieved | Accuracy | publish p50 | publish p99 |
|---|---|---|---|---|
| 250/s | 250.2/s | 100.1% | 7.83 ms | 20.83 ms |
| 500/s | 497.7/s | 99.5% | 6.28 ms | 12.52 ms |
| 750/s | 748.5/s | 99.8% | 4.53 ms | 8.63 ms |
| 1,000/s | **981.2/s** | 98.1% | 3.76 ms | 7.82 ms |
| 1,500/s | 903.9/s | **60.3%** — broke | 4.17 ms | 7.71 ms |

**Publisher ceiling: ~981 msg/s**, and the ladder broke, so it is a ceiling
rather than a floor.

Corroborated independently: four workers each completing one message's share
of a batch every 3.76 ms implies 958.5/s, which agrees with the measured 981.2/s
to within **6%**. That cross-check matters because the harness's own
`publisher_bound` flag reads `False` here — it requires a limiter that barely
waited, and the limiter waited 18.1 s of 20. Both can be true: the token bucket
banks only 0.1 s of credit, so time lost while a batch is in flight cannot be
made up afterwards, and a bursty publisher produces waiting *and* a shortfall.
The flag is sufficient, not necessary.

> **This is the emulator, not Pub/Sub.** A single-process Java service on
> loopback with no quota enforcement and no replication. Do not extrapolate in
> either direction.

#### Consuming — where the pipeline actually gives out

A fixed backlog published then drained, timed with `max_messages` set to its
exact size so the bridge stops on the last message instead of spending a final
five-second long poll inside the measured window. Against a processor in its
own OS process, which is the shape production runs in.

| Delivery concurrency | Drain rate | Zero loss |
|---|---|---|
| 4 | 30.0/s | 3,000 / 3,000 |
| **16** | **49.5/s** | 3,000 / 3,000 |
| 32 | 42.0/s | 3,000 / 3,000 |
| 64 | 35.8/s | 3,000 / 3,000 |

**Consumer ceiling: ~50 events/s**, peaking at concurrency 16 and *degrading*
beyond it. Every rung wrote every row.

#### The answer

| Leg | Rate |
|---|---|
| Harness pacing | 16,014/s (floor) |
| Publisher + broker | 981/s |
| **Consumer (processor + Postgres)** | **49.5/s** |
| **Pipeline ceiling** | **49.5 events/s — consumer-bound, by a factor of 20** |

The broker is nowhere near being the constraint. The processor handles one
event per HTTP request — a push subscription delivers one message per request,
so there is no batch to amortise — and makes several database round trips per
event for enrichment and the dual write. That per-event cost, not the broker
and not the disk, is what sets the ceiling.

#### What is *not* the constraint

| `TP_PG_POOL_MAX` | Drain rate |
|---|---|
| 2 | 18.4/s |
| 10 (configured) | 53.2/s |
| 30 | 66.8/s |

Raising the pool from 10 to 30 bought **1.26×**, which sits right on the 1.25×
bar this experiment uses to call something the binding constraint — and repeat
runs landed at 1.14× and 1.26× across that line. **The honest reading is that
the pool is one contributor among several, not the constraint.** The ladder is
sensitive enough to detect a pool that does bind: at 2 connections the same
load ran 2.9× slower.

Draining through the bridge's *in-process* processor instead reached 73.8/s —
**1.5× faster** than the separate process. Over loopback the HTTP round trip
and the JSON envelope cost more than the GIL contention they remove. The
figures above use the separate process anyway, because that is production's
shape; the in-process path is a local convenience that happens to be quicker.

### Run-to-run variance, and one measured cause

Four full ramps on this machine produced publisher ceilings of 853, 1,051, 463
and 981 msg/s, and consumer ceilings between 49.5 and 58.0/s. Quoting any
single figure to three significant figures would be false precision; **treat
the publisher as ~1,000/s and the consumer as ~50–58/s.**

Part of that spread has a concrete cause. The Pub/Sub emulator keeps every
published message in memory, and after a session of runs its container had
grown to **1.32 GiB** — at which point the publish ladder broke at 500/s and
the latency-derived cross-check disagreed with the measured ceiling by 53%.
Restarting it dropped the container to **189 MiB**, and the same ladder then
ran cleanly to 981/s with the cross-check agreeing to 6%.

**So the emulator degrades as it accumulates state, and a publisher figure
measured on a long-running emulator is not trustworthy.** The committed run
was taken on a freshly restarted one. This is a property of the local
stand-in; nothing is implied about Pub/Sub.

---

## Latency

Measured in the steady-state stage: publisher and consumer running together at
**24.7/s**, half the measured consumer ceiling, so the subscription stays near
empty and each event's interval is its own time in the pipeline rather than its
wait behind a backlog. Timestamps **rebased** (the harness default). 3,000
events, all written.

| Interval | p50 | p95 | p99 | max |
|---|---|---|---|---|
| **Processing** (`ingest_time` → `processed_time`) | **0.72 s** | **1.16 s** | **1.34 s** | 1.51 s |
| Corpus age (`event_time` → `ingest_time`) | 147.0 s | 197.0 s | 283.1 s | — |
| End-to-end (`event_time` → `processed_time`) | 147.9 s | 197.8 s | 284.0 s | — |

**They reconcile, and that is the point of reporting all three.** End-to-end
minus corpus age is 0.9 s at p50 — the processing figure. The 147 seconds is
not the pipeline being slow; it is how old MBTA's own position reports already
were when they were captured.

Two consequences follow, and both matter more than the numbers.

**The SLO is unmeetable against replayed data, by construction.** The objective
is 99% of events ingested within 2 s of `event_time`, and a replayed event is
~147 s old before the pipeline touches it. The Grafana SLO tile reads **0.00%**
during these runs, and that is *correct* rather than a bug — the SLI is
measuring exactly what it claims to. It is also why that tile cannot be used to
judge this pipeline from a replay: the figure to look at is processing latency,
whose p99 of 1.34 s sits inside the 2 s budget with room to spare.

**A latency measured from a backlog drain would have been nonsense.** In a
drain every message shares one `ingest_time`, so the interval reports how long
the queue took to empty and grows with the backlog on an identically fast
pipeline. The same code path measured that way reported a processing p50 of
**8.8 s** — twelve times the real figure — purely because 1,000 messages were
stamped at one instant and written over fifteen seconds. That is why latency
comes only from the steady-state stage, and why the stage paces below capacity.

---

## Durability: what `fsync=off` is worth

```bash
docker compose --profile durable up -d postgres-durable
python tasks.py durability
```

The same backlog through the same broker and the same processor, against two
Postgres instances differing only in durability. Both configurations are read
back from the running databases (`SHOW fsync`) and recorded, and the run
refuses to report a ratio if they do not actually differ.

| Pass | Configuration | Drain rate |
|---|---|---|
| 1 | development (`fsync=off`) | 32.3/s |
| 2 | durable (`fsync=on`) | 34.2/s |
| 3 | durable (`fsync=on`) | 40.6/s |
| 4 | development (`fsync=off`) | 35.3/s |

| | Mean | Spread |
|---|---|---|
| `fsync=off`, `synchronous_commit=off` | **33.8/s** | 2.9/s |
| `fsync=on`, `synchronous_commit=on` | **37.4/s** | 6.4/s |

**No difference was resolved.** The 3.6/s gap between the means is smaller than
the 6.4/s spread within a single configuration, so this measurement cannot tell
the two apart — and the sign is backwards from expectation anyway, which is
what a difference lost in noise looks like.

That is the expected result at this throughput rather than a surprising one.
The handler makes several database round trips per event — enrichment lookups
plus the dual write — and is bound by those, so the commit cost `fsync` adds is
small against what the pipeline is already spending per event. **It does not
mean `fsync` is free.** It means this workload is not where it shows up; a bulk
load or a higher-throughput write path on the same database would look very
different.

**The `fsync=off` caveat on every other figure in this document stands
regardless.** What this section establishes is how much that caveat is worth
*for this workload on this machine*, which turns out to be less than one would
assume — and that is a far more useful thing to know than an unbounded
disclaimer.

The first attempt at this comparison reported `fsync=on` as **1.27× faster**,
which is not a thing that happens. Two confounds: the development database had
absorbed every experiment of the session and was carrying the dead tuples to
prove it, while the comparison database was nearly fresh; and a single pass
each cannot resolve a difference smaller than the run-to-run variance. The run
above vacuums both before every pass and alternates the order — `development,
durable, durable, development` — so warm caches and drift land on both equally.

---

## Dashboard screenshots

```bash
docker compose --profile rendering up -d renderer
python tasks.py screenshots --window 45m
```

Captured panels are in [`docs/dashboards/`](docs/dashboards/), with a
`manifest.json` recording the time range, theme and scale of each — a
screenshot with no window attached is an assertion rather than evidence.

Grafana cannot rasterise a panel on its own. Asked to, it answers **HTTP 200**
with a PNG reading "No image renderer available/installed", which a naive
capture script saves happily, leaving a picture of an error message in the
repository where the evidence should be. So the renderer is a profiled compose
service, and every capture is checked against the placeholder's fixed 478×208
before the file is written.

Two honest limitations:

- **`quarantined-by-reason.png` is close to unreadable.** The barchart renders,
  and the data behind it is real, but at the series cardinality these runs
  produce it is a wall of overlapping labels. That is a dashboard problem this
  phase surfaced rather than one it fixed — the panel passes both dashboard
  tests, because those check that it queries a real series and selects
  something, not that it is legible.
- The captures were taken after the benchmark runs rather than during a chaos
  run, so the short-window rate panels show the tail of that activity rather
  than a fault in progress.

# ADR-007: One SLO, defined in rules and verified against a live Prometheus

**Status:** accepted · **Date:** 2026-10-20

## Context

The Grafana dashboard was committed in Phase 0, before a single metric had ever
been emitted. Its PromQL was therefore written against *assumed* series names,
and four of its assumptions were wrong in ways that produce no error anywhere:

- **The SLO tile selected the wrong histogram bucket.** It matched `le="2"`.
  Prometheus stores that boundary as `le="2.0"`, so the one number the project
  is judged on had been empty since the day it was written. The cause is worth
  recording: the Collector's Prometheus exporter serves `le="2"` in Prometheus
  text format and `le="2.0"` in OpenMetrics, and `enable_open_metrics: true`
  means Prometheus negotiates OpenMetrics. Inspecting the endpoint with `curl`
  returns the spelling that does not work.
- **One panel queried an instrument nothing sets.** `transitpulse.dlq.depth`
  was in the registry, so it looked implemented, but no code path ever wrote to
  it — Pub/Sub performs the dead-letter move and reports it to nobody.
- **One annotation queried a metric that was never in the registry at all**
  (`transitpulse_build_info`), with an expression (`changes(...) > 0`) that
  could not have fired even if it had been.
- **Every panel pointed at a datasource that did not exist.** The dashboards
  reference datasource uid `Prometheus`; a provisioned datasource without an
  explicit `uid` is assigned a generated one, so each panel rendered a
  datasource error rather than data.

A "No data" panel is worse than a missing one, because the dashboard still
looks finished. All four failures are silent, and this project has already been
caught twice by silent failures — a structlog misconfiguration that broke
thirteen tests in unrelated files, and partitions that ran out with no warning
signal whatsoever.

## Decision

**The SLO is: 99% of events ingested within 2s**, where ingest lag is
`processed_time - event_time`, over a rolling 30 days.

It lives in `observability/prometheus/rules/` as recording rules, and the
dashboard and the alerts both read those recorded series rather than restating
the objective. Alert rules cover the failure modes this pipeline has actually
hit or deliberately designed for, with every threshold either derived from
something or labelled in the file as a guess.

Two tests hold it in place. `tests/unit/test_dashboard_contract.py` derives the
series names the registry can produce — by building the instruments against an
in-memory reader and applying the exporter's naming rule — and fails if a panel
queries anything else. `tests/integration/test_dashboard_panels.py` reads the
expressions out of the committed JSON, drives real load through the pipeline,
and fails if any one of them selects nothing.

## Why

**2s is derived; 99% is chosen.** Cloud Scheduler ticks the collector every
15s, so a 2s ingest budget means ingestion is never the reason a position on
the map is stale — the feed's own cadence is, by an order of magnitude. The
99% is a target, not a measurement: there is no production traffic to derive an
achievable percentile from, and the local figure is whatever the laptop was
doing. It is written here as a choice so that nobody later mistakes it for a
finding.

**Burn rate, not a bare threshold.** A rule on `p95 > 2s` fires for every slow
minute and says nothing about whether the month is in trouble. Multi-window
multi-burn-rate alerting pages on the rate the error budget is being spent:
14.4x over 1h (budget gone in two days) and 6x over 6h (gone in five). Each
pairs with a short window that must agree, because the long window alone keeps
paging long after recovery and the short window alone pages for every blip. The
multipliers come from the Google SRE workbook's standard table for a 30-day
budget — a convention worth inheriting rather than re-deriving. The bare p95
rule is kept as well, but at ticket severity: it restates the objective in the
units an operator thinks in.

The workbook's slower pairs (3x over 1d, 1x over 3d) are deliberately absent.
Local Prometheus retains six hours, so a one-day window cannot be computed at
all, and a rule that silently never fires is worse than no rule.

**Recording rules, for two reasons.** The dashboard refreshes every ten
seconds, and it was recomputing three histogram quantiles over a five-minute
window on every refresh for every open tab. More importantly, the objective now
appears exactly once: the tile claiming compliance and the rule that pages
about it read the same series and cannot drift apart, which they would the
moment either expression were edited alone.

**No `clamp_min` guarding the SLI's denominator.** Guarding the division makes
an idle pipeline report 0% compliance instead of no figure, which is a page for
a system that is doing nothing wrong. Plain division yields NaN on zero
traffic, every comparison against NaN is false, and the alerts stay quiet.

**Counters are declared at zero.** An unincremented counter has no series, and
"no series" renders on a dashboard exactly like zero while meaning something
entirely different — a quiet pipeline is indistinguishable from a processor
that never started. Every label value these counters can take is known at
startup (the quarantine reasons, the feed failure kinds, the sink names), so
each is declared at zero when the process boots and the panels show a real
zero. `partitions_ensured` and `partitions_dropped` are added even when a run
creates nothing, for the same reason: otherwise the panel reads "No data" on
precisely the days nothing was wrong.

**A dead-letter counter replaces the dead-letter depth gauge.** Queue depth is
a broker-side quantity; in GCP it is
`pubsub.googleapis.com/subscription/num_undelivered_messages`, which no process
in this repository can observe and the emulator does not expose at all. What
the processor *does* own is the moment it gives up: a message on its final
delivery attempt whose quarantine write also failed will be moved to the DLQ by
the broker, and that is the only point at which the fact becomes knowable from
inside. `transitpulse.events.dead_lettered` counts exactly that, and "depth
above zero" becomes "anything dead-lettered in the last ten minutes", which is
the same operational question. Phase 4 adds the true broker gauge from Cloud
Monitoring, where it belongs.

**Partition headroom is contiguous days, not furthest-day-minus-today.** The
obvious arithmetic is wrong: a single stray partition dated years ahead — left
by a test, or created by hand during an incident — reports years of runway
across a gap that refuses every insert the moment the real window ends.
Measured against the development database the two readings were 14 days and
1,209. This matters because the alert exists for a real outage: mid-development
the daily partitions of `vehicle_events` ran out and every insert began failing
with `no partition of relation "vehicle_events" found for row`, with no signal
at all — the maintenance job reported success because it had nothing left to
create.

**Job freshness is alerted on separately from its consequence.** Headroom
catches the outage three days out; the freshness stamp catches the cause a
fortnight earlier. A job that stopped being *triggered* produces no failing run
to notice, only an absence.

## Consequences

- **A verification driver is now part of the project.** `tools/smoke.py`
  provokes one of every signal — a failed fetch and a successful one, a
  duplicate ping, a malformed payload, a partition run — through the component
  that owns each in production. It is what CI drives and what a person runs
  before opening Grafana, so the thing CI proves and the thing a human checks
  cannot diverge. It is explicitly *not* a load test: `tools/replay` remains
  the only component permitted to produce a throughput figure.

- **Local parity needed a push stand-in.** The processor is a Pub/Sub *push*
  target, and the one thing the emulator cannot do is reach a process on the
  host. `tools/pushbridge.py` pulls from the same subscription and performs the
  POST Pub/Sub would, with the broker's own delivery-attempt count and the
  status code still acting as the acknowledgement. Before it, the local stack
  published into a topic nothing consumed, which is why six of eleven panels
  had never had a value.

- **The Collector's `metric_expiration` is raised to an hour.** At the
  five-minute default, a one-shot job's headroom gauge and success timestamp
  vanish minutes after it exits. The cloud deployment needs no equivalent:
  Cloud Monitoring keeps the point that was written.

- **The freshness alert cannot catch a long-dead job locally.** Once the
  expiry window passes with no run, the series is gone rather than stale, and
  local Prometheus cannot cleanly distinguish that from a stack that has only
  just started. `absent()` would fire on every fresh stack. Phase 4 adds the
  absence condition in Cloud Monitoring.

- **These rules are the local expression of an objective Phase 4 will codify
  in Cloud Monitoring.** The same 2s, the same 99%, the same burn-rate windows,
  stated twice because the two systems cannot share a definition — which is
  itself a reason to keep the definition to one line of YAML per window.

## Rejected

**Fixing the SLO tile by editing it to `le="2.0"` and stopping there.** It
would have worked, and the next person to add a panel would have had the same
accident. The expression was never the real defect; the absence of anything
that would notice was.

**`or vector(0)` on the panels that could legitimately be empty.** It renders
nicely and it lies: it makes "nothing has been dead-lettered" and "the
processor is not running" look identical. Declaring the counters at zero
produces the same tile from a fact rather than from a default.

**Asserting values rather than non-emptiness in the integration test.** Which
numbers are plausible depends on the machine and the moment, so the assertion
would be either vacuous or flaky. What must hold is that every query selects
something — that the names, labels, units and recording rules line up.

**Running the processor as a compose service with an emulator push
subscription.** Closer parity, and it would have removed the bridge. It also
needs the emulator to reach back into the host network, which is the part that
does not work reliably across platforms, and this phase was not the place to
find out. The bridge keeps the processor's own code on the real path.

# ADR-002: Two stores — Postgres for serving, BigQuery for history

**Status:** accepted · **Date:** 2026-10-15

## Context

Two very different read patterns exist against the same event stream:

1. *"Where is every vehicle on route Q right now?"* — tiny result, must be
   fast, called constantly.
2. *"What was on-time performance by route for the last 90 days?"* — scans
   hundreds of millions of rows, called occasionally.

## Decision

Write every event to both stores from the same handler invocation.

- **Cloud SQL (PostgreSQL 16)** holds `vehicle_live` (one upserted row per
  vehicle), the day-partitioned recent `vehicle_events`, the `quarantine`
  table, and the static GTFS reference tables that enrichment joins against.
- **BigQuery** holds the full append-only history, date-partitioned and
  clustered by `route_id`, written through the Storage Write API.

## Why

A single store serves one of these patterns badly. Postgres alone would mean
90-day aggregations scanning a table it has no useful index for. BigQuery alone
would mean every live-map request paying BigQuery's per-query latency and
minimum billing — and BigQuery cannot upsert a hot row cheaply, so "current
position per vehicle" becomes a windowed scan on every call.

Clustering by `route_id` matters specifically because every analytical query
filters on route; it lets BigQuery skip blocks rather than scan the partition.
Partitioning by date is what makes the time filter cheap. Those two choices
together are the difference between an affordable query and an expensive one.

## Consequences

- **The dual write is not atomic.** This is the real cost, and it is handled by
  idempotency rather than pretended away. See
  [ADR-005](005-idempotency-over-transactions.md).
- Two schemas must stay in step. Mitigated by deriving both from one Python
  column list — see [ADR-004](004-runtime-protobuf-descriptor.md).
- Local development has no BigQuery emulator, so `TP_BQ_ENABLED` defaults to
  false and the local pipeline writes to Postgres only.

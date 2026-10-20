# ADR-005: Idempotent retry instead of a distributed transaction

**Status:** accepted · **Date:** 2026-10-15

## Context

Each event is written to two independent systems: Cloud SQL and BigQuery
([ADR-002](002-dual-sink-storage.md)). There is no transaction spanning both.
A handler can therefore succeed against Postgres and fail against BigQuery, or
be redelivered by Pub/Sub after both writes already succeeded.

Pub/Sub delivery is at-least-once, and the BigQuery `_default` write stream is
also at-least-once, so duplicates are not an edge case — they are the normal
operating condition.

## Decision

Make every write idempotent on a deterministic key and let retries happen
freely. No two-phase commit, no saga, no outbox.

The key is `blake2b(agency | vehicle_id | event_time_epoch)`, computed from the
agency's own data:

- `vehicle_events` — `ON CONFLICT (idempotency_key, event_time) DO NOTHING`
- `vehicle_live` — `ON CONFLICT (agency, vehicle_id) DO UPDATE ... WHERE
  excluded.event_time > vehicle_live.event_time`
- BigQuery — duplicates are tolerated on write and collapsed by
  `idempotency_key` in the aggregation views

`ingest_time` is deliberately **excluded** from the key, because it differs on
every replay. That exclusion is what makes replaying the archived corpus safe,
and it is asserted by a unit test.

## Why

**Two-phase commit across Cloud SQL and BigQuery is not available**, and
emulating it with a saga or an outbox table adds a coordinator, its own
failure modes, and latency — to solve a problem that an idempotency key solves
for the cost of one index.

**The data has a natural identity.** An agency reporting that vehicle 7801 was
at a position at 14:30:00 is the same fact however many times it is delivered.
When a natural key exists, deduplication at the sink is strictly simpler than
coordination between sinks.

**The `WHERE excluded.event_time >` guard handles the harder problem.** Out-of-order
delivery is more damaging than duplication: without that predicate a late stale
ping would overwrite a fresher position and the live map would jump backwards.
Ordering keys make this rare; the predicate makes it impossible.

## Consequences

- **Postgres and BigQuery can be transiently inconsistent**, for as long as a
  retry takes. Accepted and documented: the serving layer is the source of
  truth for "now", BigQuery for history, and nothing reads both to answer one
  question.
- Partial batch failure retries the whole batch. Safe by construction, at the
  cost of redundant work on the already-written rows.
- Analytical queries must deduplicate on `idempotency_key`. This is a real
  constraint on anyone writing new SQL against `vehicle_events` in BigQuery,
  and it belongs in the views rather than in each query.
- Quarantine writes are *not* idempotent — a repeatedly failing message can
  produce several quarantine rows. Accepted: duplicate diagnostic rows are
  harmless, and `delivery_attempt` records which attempt produced each one.

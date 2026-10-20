# ADR-001: Pub/Sub rather than self-managed Kafka

**Status:** accepted · **Date:** 2026-10-15

## Context

The pipeline needs a durable buffer between ingestion and processing so the two
can fail and scale independently. The realistic options were Google Cloud
Pub/Sub, a self-managed Kafka cluster, and Confluent Cloud.

## Decision

Cloud Pub/Sub, with per-vehicle ordering keys and a dead-letter topic.

## Why

**Operational cost dominates at this scale.** Kafka's advantages — replay from
arbitrary offsets, very high per-partition throughput, a mature stream-processing
ecosystem — are real, but they are paid for with broker operations: partition
sizing, rebalancing, retention tuning, upgrades. For a pipeline whose steady
state is a few thousand events per second, that cost buys nothing the project
needs.

**Replay is solved differently here, and better.** The usual reason to want
Kafka's log is to re-run history after fixing a bug. This pipeline archives
every raw feed response to object storage and replays *that*, which has a
property the Kafka log does not: it replays the original upstream bytes,
before any of our own parsing, so a decode bug can be fixed and the original
payload re-processed. Retention is then an object-storage lifecycle rule, not a
broker setting.

**Ordering keys fit the data model exactly.** Correctness here requires that
pings for *one vehicle* stay in order; it does not require global ordering.
Pub/Sub's `ordering_key` gives exactly that, so parallelism stays proportional
to the number of vehicles. Kafka would express the same thing as
`partition_key`, at equivalent cost — no advantage either way.

**Scale-to-zero matters for a project funded by a student.** Pub/Sub costs
nothing when idle. An always-on three-broker cluster does not.

## Consequences

- No arbitrary-offset consumption. Accepted: the GCS corpus covers replay.
- Vendor coupling to GCP. Accepted: the project is explicitly a GCP project,
  and `EventPublisher` is a thin seam that could be reimplemented.
- The `_default` BigQuery write stream is at-least-once, so duplicates are
  possible and are handled by the idempotency key rather than by the broker.
  See [ADR-005](005-idempotency-over-transactions.md).

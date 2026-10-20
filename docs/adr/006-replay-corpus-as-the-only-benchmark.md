# ADR-006: Archive every raw response, and measure load only from it

**Status:** accepted · **Date:** 2026-10-15

## Context

A GTFS-Realtime feed serves only the present. There is no endpoint for "what
you were publishing at 08:14 last Tuesday", so a decode or enrichment bug found
on Friday cannot be reproduced against the data that caused it, and a load test
has nothing realistic to replay.

Synthetic load is the usual answer and it is a bad one here. Generated
positions are uniform, well-formed and free of the (0, 0) coordinates,
400-degree bearings, missing timestamps and re-sent pings that a real agency
emits constantly — so they exercise neither the quarantine path nor the
deduplication path, which is most of what this pipeline does.

## Decision

The collector writes every raw response to an archive **before anything parses
it**, and the load harness is the only component permitted to produce a
throughput or latency figure. Both local disk and GCS implement one
`SnapshotArchive` interface, so the corpus a laptop captures and the corpus
Cloud Run captures are read through identical calls.

## Why

**Archiving precedes decoding** because the payload most worth having is the one
that breaks the decoder, and it exists in memory only during that tick. A
decode failure that discarded its own evidence would be diagnosable only by
waiting for it to happen again.

**The collector gets no database.** It stays useful while Postgres is down, and
the quarantine table belongs to the processor. Entities the decode rejects are
written as a sidecar beside the payload that produced them, which is also where
a reprocessing run would look for them. "Nothing is dropped silently" has to
hold in a service with nowhere to write.

**One interface, selected by configuration alone** — and resolved from the
environment rather than inferred from whether a bucket name happens to be set,
because the bucket name has a default and inference would make a laptop upload
to a real bucket.

## Consequences

- **The harness re-bases event times by identity, not by time.** A run longer
  than the corpus wraps around, and the obvious way to keep idempotency keys
  unique on the second pass — shifting the window forward — dates events into
  the future and feeds negative values into the lag histogram the SLO is
  written against. Each cycle therefore emits a fresh set of synthetic vehicle
  ids instead: a run that cycles the corpus is by definition generating more
  traffic than the corpus holds, and more synthetic vehicles is a truthful way
  to say so.
- **The report names the bottleneck.** Achieved rate counts acknowledged
  publishes over elapsed monotonic time, with the drain inside the measured
  window, and it is printed next to the target and the limiter's total wait. A
  limiter that barely waited was never the constraint, so a shortfall is
  attributable instead of being left as one flattering number.
- **It reports what will deduplicate.** Agencies re-send observations they have
  already sent: roughly 29% of a six-snapshot MBTA capture shares an
  idempotency key with another ping in the same corpus. Replaying that is
  faithful, but that share of a run exercises the dedupe path rather than the
  write path, and a throughput figure that quietly included it would overstate
  the work done.
- **`--dry-run` runs the loop with the publish removed**, so the limiter's own
  accuracy can be established before any number is attributed to the pipeline.
- The corpus costs storage: ~44 KB per MBTA snapshot, about 250 MB per month at
  one tick every fifteen seconds. Bucket lifecycle rules are Phase 4's problem;
  the date-hierarchical layout exists so that expiring or replaying one day is
  a prefix operation.

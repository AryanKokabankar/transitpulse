# ADR-004: Build the BigQuery row descriptor at runtime

**Status:** accepted · **Date:** 2026-10-15

## Context

The BigQuery Storage Write API speaks protobuf: appending rows requires a
`DescriptorProto` describing the row type. The conventional approach is to
write a `.proto` file, compile it with `grpcio-tools` into a `_pb2.py`, and
check the generated module into the repository.

## Decision

Construct the `DescriptorProto` at runtime from the same Python column list the
rest of the sink uses, and materialise the message class with
`message_factory.GetMessageClass` against a private `DescriptorPool`.

## Why

**It removes a build step and a generated artifact.** No `grpcio-tools` in the
image, no `protoc` in CI, no generated file that someone must remember to
regenerate after editing the schema.

**It makes schema drift structurally impossible.** The column list is declared
once. A new column appears in the Postgres row tuple and the BigQuery
descriptor from the same edit. With a checked-in `_pb2.py`, the two can diverge
silently and the failure surfaces as a rejected append in production.

A private pool rather than the default one matters: adding the same file name
to the global descriptor pool twice raises, which would break constructing a
second sink — something both the tests and the replay harness do.

## Consequences

- **A typo becomes a runtime error rather than a compile error.** This is the
  real cost. It is bought back by unit tests that assert field numbers are
  unique and contiguous, that every field declares proto3 presence, and that
  every key `event_to_row` emits exists in the descriptor.
- Field numbers must stay stable once data is in flight, so new columns append
  to the end of `_SCHEMA` and existing entries are never reordered. Enforced
  by the contiguity test and stated in a comment at the list.
- `proto3_optional` on every field is deliberate: an unset field becomes a
  genuine `NULL` instead of a zero, which matters because a first ping has no
  computed speed and a `0.0` there would drag every speed average down.

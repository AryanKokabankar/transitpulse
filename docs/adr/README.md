# Architecture decision records

Short notes on decisions that were not obvious, written when the decision was
made. Each one states the alternative that was rejected and why, because the
rejected option is usually the interesting part.

| ADR | Decision |
|---|---|
| [001](001-pubsub-over-kafka.md) | Pub/Sub rather than self-managed Kafka |
| [002](002-dual-sink-storage.md) | Two stores: Postgres for serving, BigQuery for history |
| [003](003-raw-psycopg-over-orm.md) | Raw psycopg3 on the write path, not an ORM |
| [004](004-runtime-protobuf-descriptor.md) | Build the BigQuery row descriptor at runtime |
| [005](005-idempotency-over-transactions.md) | Idempotent retry instead of a distributed transaction |
| [006](006-replay-corpus-as-the-only-benchmark.md) | Archive every raw response, and measure load only from it |
| [007](007-slo-and-alerting-as-code.md) | One SLO, defined in rules and verified against a live Prometheus |

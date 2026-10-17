---
name: streaming-cdc
description: Designs and reviews Kafka, Debezium CDC, Spark Structured Streaming, and streaming/lakehouse pipelines. Use for offsets, event ordering, duplicate delivery, checkpoints, schema evolution, replay, and sink guarantees.
---

# Streaming and CDC Engineering

1. Trace the actual source → connector → topic/stream → consumer/transform → sink flow from configs and code. Identify where offsets and checkpoints live.
2. Clarify event identity, primary key, operation type, source timestamp, event time, transaction/order metadata, and schema version where available.
3. Distinguish at-most-once, at-least-once, and exactly-once claims at each boundary. Never claim end-to-end exactly-once based only on Kafka producer settings or Spark checkpointing.
4. Explain duplicate handling at the sink: stable event/business keys, upsert/merge behavior, deduplication window, and what happens after replay or restart.
5. Treat offsets/checkpoints as durable state. Explain reset/recovery consequences and never delete production checkpoints or offset state without explicit approval.
6. Consider ordering per key/partition, rebalances, retries, poison records, dead-letter handling, late events, and backpressure where relevant.
7. For CDC, inspect snapshot/stream transition, delete/tombstone semantics, primary-key changes, schema evolution, and initial-load consistency.
8. For lakehouse sinks, verify transaction/commit semantics and idempotency in the actual table format/runtime version. Do not assume Hudi/Iceberg/Delta options are interchangeable.
9. Do not recommend arbitrary partition keys or partition counts without event-volume and key-distribution evidence.
10. Provide a failure/recovery matrix for meaningful changes: crash before write, crash after write before offset/checkpoint commit, restart, replay, schema mismatch, and late event.
11. Avoid running commands that reset offsets, wipe checkpoints, truncate tables, or drop topics without explicit approval.

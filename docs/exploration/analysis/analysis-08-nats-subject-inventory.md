# NATS and JetStream Subject Inventory

Scope
- Identify core subjects/streams and how namespacing is applied.

Method
- rg for events.* and system.schemas.*; inspect environment and publisher code.

Namespacing
- Subjects are prefixed with environment name via SinexEnvironment::nats_subject (dev.*, staging.*, prod.*) (crate/lib/sinex-core/src/environment.rs:230-246).

Core subjects (logical, before env prefix)
- events.raw.<source>.<event_type> from node publishers (crate/lib/sinex-node-sdk/src/nats_publisher.rs:121-127).
- events.confirmations.<event_id> from ingestd confirmations stream (topology uses events.confirmations.> filter) (crate/core/sinex-ingestd/src/jetstream_consumer.rs:93-110).
- events.dlq.<component> for dead letter messages, including ingestd publish to events.dlq.ingestd (crate/core/sinex-ingestd/src/jetstream_consumer.rs:103-110).
- system.schemas.active for schema broadcast snapshots (crate/core/sinex-ingestd/src/service.rs:528-549; crate/lib/sinex-node-sdk/src/runtime/stream/mod.rs:160-166).

Streams
- Raw stream base name supplied to JetStreamTopology; confirmations stream is base + _CONFIRMATIONS, DLQ stream is base + _DLQ (crate/core/sinex-ingestd/src/jetstream_consumer.rs:93-107).

Consumers
- DLQ retry consumes filter_subject events.dlq.> (crate/lib/sinex-node-sdk/src/dlq_retry.rs:70-85).

Observations
- Publishers normalize dots in source/event_type when forming raw subjects, so subject names may not match payload values exactly (crate/lib/sinex-node-sdk/src/nats_publisher.rs:121-127).
- Namespacing is centralized and consistently applied, but some tests and docs still reference raw (non-namespaced) subjects.

Follow-ups
- Document the dot-to-underscore normalization and ensure any subscriber filters match the normalized form.
- Consider adding a central registry list of subjects for ops tools.

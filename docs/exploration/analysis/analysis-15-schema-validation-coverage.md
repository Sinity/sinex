# Schema Validation Coverage

Scope
- Determine where schema validation is enforced and where it is skipped.

Method
- Inspect ingestd validator, node SDK schema listener, and event emitters.

Ingestd enforcement
- ValidationResult::should_accept treats Valid, Skipped, NoSchema, and SchemaNotFound as accept states (crate/core/sinex-ingestd/src/validator.rs:133-158).
- JetStream consumer validates event envelope and payload schema, but accepts NoSchema and SchemaNotFound with warnings (crate/core/sinex-ingestd/src/jetstream_consumer.rs:778-807).

Node-side validation
- Schema validation in nodes is optional and depends on schema broadcast + KV availability; in edge mode it is skipped entirely (crate/lib/sinex-node-sdk/src/runtime/stream/mod.rs:150-180).
- EventEmitter only validates when a validator is present; otherwise events are sent without schema checks (crate/lib/sinex-node-sdk/src/runtime/stream/handles.rs:92-102).

Observations
- In practice, schema validation coverage is best-effort. Events with unknown schemas still persist unless validation is configured to be strict.

Follow-ups
- Add a strict validation mode for production pipelines where schema absence is treated as an error.
- Surface schema coverage metrics: % of events with known schema, NoSchema counts, and SchemaNotFound counts.

# Taint Analysis (Input -> Sensitive Sinks)

Scope
- Trace untrusted inputs (NATS messages, RPC requests, CLI args) to sinks (SQL, filesystem, logs) and note validation boundaries.

Method
- rg for path validation utilities and ingress points; inspect ingestd validation and filesystem helpers.

Input sources and validation
- JetStream RawEvent ingestion validates event id/source/type and payload schema; NoSchema and SchemaNotFound are accepted with warnings (crate/core/sinex-ingestd/src/jetstream_consumer.rs:778-807).
- Filesystem paths are validated via SanitizedPath or VerifiedPath before file IO (crate/lib/sinex-node-sdk/src/annex/path_validator.rs:12-58; crate/nodes/sinex-document-ingestor/src/lib.rs:72-126).
- Processor CLI resolves work_dir via SanitizedPath, but falls back to new_unchecked if validation fails on namespaced defaults (crate/lib/sinex-processor-runtime/src/cli.rs:912-918).

Sensitive sinks
- SQL queries use bind parameters for dynamic values; dynamic SQL is limited to static table names or session settings (crate/lib/sinex-core/src/db/repositories/common.rs:120-176; crate/lib/sinex-core/src/db/mod.rs:251-268).
- File writes in node exporters use SanitizedPath as input (e.g., desktop/system export_data paths) and path validation is part of node config.

Observations
- The taint boundary for events is primarily schema validation; acceptance of NoSchema/SchemaNotFound means untyped payloads can reach persistence.
- Path validation is centralized, with a small escape hatch via SanitizedPath::new_unchecked for default paths.

Follow-ups
- Consider stricter mode in ingestd where NoSchema is a hard failure for selected sources.
- Audit any future use of SanitizedPath::new_unchecked to ensure it does not accept user input.

# Ingest Service

`service.rs` implements the gRPC server that satellites talk to. It handles the
`IngestService` protobuf methods, applies validation, and delegates to database
repositories.

- Wraps the tonic-generated stubs in tracing and retry guards.
- Batches writes to reduce contention.
- Emits structured logs with ULIDs for provenance tracking.

Refer to `docs/current/architecture/Core_Architecture.md` for the event flow diagram and
queue interactions.

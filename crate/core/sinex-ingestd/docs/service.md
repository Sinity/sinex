# Ingest Service

`service.rs` orchestrates ingestd startup and the `JetStream` consumers that
receive events and source material from nodes. It applies validation,
delegates persistence to repositories, and drives material assembly.

- Runs `JetStream` consumers for events and material slices.
- Batches writes to reduce contention.
- Emits structured logs with `UUIDv7` IDs for provenance tracking.

Refer to `docs/architecture.md` for the event flow diagram and
queue interactions.

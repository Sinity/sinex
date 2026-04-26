# NATS Subject Registry

This document provides a canonical reference for all NATS subjects and streams used in the Sinex system.

## Stream Configuration Ownership

Stream configuration has two sources of truth depending on deployment mode:

- **NixOS deployment** (production): The NixOS module (`nixos/modules/nats.nix`) owns stream configuration. The Rust bootstrap code respects the `SINEX_NATS_STREAMS_MANAGED_EXTERNALLY=true` environment variable (set by the NixOS module when `bootstrapStreams.enable` is true) and skips its own `create_or_update_stream` calls.
- **Local dev** (default): The Rust `bootstrap_streams()` functions in `jetstream_consumer.rs` and `material_assembler/pipeline.rs` create streams on startup. The env var is not set, so bootstrap runs normally.

The dual path exists because local dev should not require a NixOS rebuild or the `nats` CLI tool, while deployed systems need deterministic stream config that survives service restarts without racing on stream creation.

Set `SINEX_NATS_STREAMS_MANAGED_EXTERNALLY=true` to suppress Rust-side bootstrap in any context (e.g., containerized deployments, test harnesses with pre-provisioned NATS).

## Environment Namespacing

All subjects are prefixed with the environment name (`dev`, `staging`, `prod`) via `SinexEnvironment::nats_subject()`. For example, `events.raw.fs.file.created` becomes `dev.events.raw.fs.file.created` in development.

Reference: `crate/lib/sinex-primitives/src/environment.rs`

## Subject Naming

**Collision-free token encoding**: publishers encode `source` and `event_type` into a single subject token each, so dots stop colliding with underscores. For example:

- Source `fs.watcher` becomes `fs_d_watcher` in the subject
- Source `fs_watcher` becomes `fs_u_watcher` in the subject
- Event type `file.created` becomes `file_d_created` in the subject

This keeps the fixed `events.raw.<source>.<event_type>` hierarchy while preserving a one-to-one mapping from logical identifiers to NATS subject tokens.

Reference: `crate/lib/sinex-node-sdk/src/nats_publisher.rs:121-127`

## Core Subjects

| Subject Pattern | Purpose | Publisher |
|-----------------|---------|-----------|
| `events.raw.<source>.<event_type>` | Raw events from ingestor nodes | Node SDK via `NatsPublisher` |
| `events.confirmations.<event_id>` | Persistence acknowledgments | ingestd |
| `events.confirmation_retries.<event_id>` | Confirmation retry processing | ingestd |
| `events.processing_failures.<component>` | Processing failure envelopes | ingestd |
| `events.dlq.<component>` | Dead letter queue messages | ingestd, failed consumers |
| `sinex.derived.invalidation` | Scope invalidation signals for automata | ingestd |
| `system.schemas.active` | Schema broadcast snapshots | ingestd |

## JetStream Streams

Stream names are derived from a configurable base name (typically `SINEX_EVENTS`):

| Stream | Filter | Purpose |
|--------|--------|---------|
| `<base>` | `events.raw.>` | Primary event storage |
| `<base>_CONFIRMATIONS` | `events.confirmations.>` | Persistence confirmations |
| `<base>_CONFIRMATION_RETRIES` | `events.confirmation_retries.>` | Confirmation retry queue |
| `<base>_PROCESSING_FAILURES` | `events.processing_failures.>` | Processing failure envelopes |
| `<base>_DLQ` | `events.dlq.>` | Dead letter queue |
| `<base>_DERIVED_INVALIDATIONS` | `sinex.derived.invalidation` | Scope invalidation signals |

Reference: `crate/core/sinex-ingestd/src/jetstream_consumer.rs:93-107`

## Consumers

| Consumer | Subject Filter | Purpose |
|----------|----------------|---------|
| Ingest consumer | `events.raw.>` | Batch ingestion to PostgreSQL |
| DLQ retry | `events.dlq.>` | Retry failed messages |

Reference: `crate/lib/sinex-node-sdk/src/dlq_retry.rs:70-85`

## Subject Examples

Fully-qualified subject names in development environment:

```
dev.events.raw.fs_d_watcher.file_d_created             # File creation event
dev.events.raw.terminal_u_kitty.shell_d_command        # Shell command event
dev.events.confirmations.01HXYZ...                     # Confirmation for event ID
dev.events.confirmation_retries.01HXYZ...               # Confirmation retry for event ID
dev.events.processing_failures.ingestd                  # Processing failure from ingestd
dev.events.dlq.ingestd                                  # DLQ message from ingestd
dev.sinex.derived.invalidation                          # Scope invalidation signal
dev.system.schemas.active                               # Schema broadcast
```

## Implementation References

- Environment namespacing: `sinex-primitives/src/environment.rs`
- Event publishing: `sinex-node-sdk/src/nats_publisher.rs`
- Stream topology: `sinex-ingestd/src/jetstream_consumer.rs`
- Schema broadcast: `sinex-ingestd/src/service.rs`
- DLQ handling: `sinex-node-sdk/src/dlq_retry.rs`

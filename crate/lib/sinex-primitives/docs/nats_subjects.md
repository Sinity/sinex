# NATS Subject Registry

This document provides a canonical reference for all NATS subjects and streams used in the Sinex system.

## Environment Namespacing

All subjects are prefixed with the environment name (`dev`, `staging`, `prod`) via `SinexEnvironment::nats_subject()`. For example, `events.raw.fs.file.created` becomes `dev.events.raw.fs.file.created` in development.

Reference: `crate/lib/sinex-primitives/src/environment.rs`

## Subject Naming

**Dot-to-underscore normalization**: Publishers normalize dots in `source` and `event_type` values when forming subject names. For example:

- Source `fs.watcher` becomes `fs_watcher` in the subject
- Event type `file.created` becomes `file_created` in the subject

This ensures subject names don't conflict with the NATS subject hierarchy while preserving the original values in the payload.

Reference: `crate/lib/sinex-node-sdk/src/nats_publisher.rs:121-127`

## Core Subjects

| Subject Pattern | Purpose | Publisher |
|-----------------|---------|-----------|
| `events.raw.<source>.<event_type>` | Raw events from ingestor nodes | Node SDK via `NatsPublisher` |
| `events.confirmations.<event_id>` | Persistence acknowledgments | ingestd |
| `events.dlq.<component>` | Dead letter queue messages | ingestd, failed consumers |
| `system.schemas.active` | Schema broadcast snapshots | ingestd |

## JetStream Streams

Stream names are derived from a configurable base name (typically `SINEX_EVENTS`):

| Stream | Filter | Purpose |
|--------|--------|---------|
| `<base>` | `events.raw.>` | Primary event storage |
| `<base>_CONFIRMATIONS` | `events.confirmations.>` | Persistence confirmations |
| `<base>_DLQ` | `events.dlq.>` | Dead letter queue |

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
dev.events.raw.fs_watcher.file_created       # File creation event
dev.events.raw.terminal_kitty.shell_command  # Shell command event
dev.events.confirmations.01HXYZ...           # Confirmation for event ID
dev.events.dlq.ingestd                       # DLQ message from ingestd
dev.system.schemas.active                    # Schema broadcast
```

## Implementation References

- Environment namespacing: `sinex-primitives/src/environment.rs`
- Event publishing: `sinex-node-sdk/src/nats_publisher.rs`
- Stream topology: `sinex-ingestd/src/jetstream_consumer.rs`
- Schema broadcast: `sinex-ingestd/src/service.rs`
- DLQ handling: `sinex-node-sdk/src/dlq_retry.rs`

# Architecture Documentation

Canonical architecture references for the current Sinex system.

## Core

- [Core_Architecture.md](./Core_Architecture.md) — End-to-end flow and component map
- [SystemOperations_And_Integrity_Architecture.md](./SystemOperations_And_Integrity_Architecture.md) — Operational invariants and integrity controls
- [UserInteraction_And_Query_Architecture.md](./UserInteraction_And_Query_Architecture.md) — Query/read-path architecture

## Data and Lifecycle

- [current-state-tracking.md](./current-state-tracking.md) — Continuous aggregates and current-state views
- [data-lifecycle.md](./data-lifecycle.md) — Live/archive/tombstone lifecycle semantics
- [gateway-coordination.md](./gateway-coordination.md) — Replay/lifecycle gateway coordination
- [nats-subjects.md](./nats-subjects.md) — Subject taxonomy and transport contracts

## Cross-Cutting Patterns

- [type-system-patterns.md](./type-system-patterns.md) — Domain typing, IDs, and API boundaries
- [distributed-patterns.md](./distributed-patterns.md) — Eventing, idempotency, and concurrency patterns
- [observability.md](./observability.md) — Health/telemetry patterns
- [extensibility.md](./extensibility.md) — Extension points for nodes/events/RPC
- [security-architecture.md](./security-architecture.md) — Threat model and controls

## Crate-Level References

- `crate/lib/sinex-node-sdk/docs/` — Node runtime and provenance internals
- `crate/lib/sinex-db/docs/` — Repository and persistence patterns
- `crate/core/sinex-ingestd/docs/` — Ingestion pipeline details
- `crate/core/sinex-gateway/docs/` — Gateway/RPC internals

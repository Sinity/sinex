# Architecture Documentation

Cross-cutting architecture references for the current Sinex system. Detailed subsystem doctrine now lives with the owning crates.

## Top-Level Owners

- [Core_Architecture.md](./Core_Architecture.md) — End-to-end flow and component map
- [SystemOperations_And_Integrity_Architecture.md](./SystemOperations_And_Integrity_Architecture.md) — Operational invariants, deployment policy, and integrity controls

## Rehomed To Crate Docs

- Query/read path and gateway coordination → `crate/core/sinex-gateway/docs/interaction_and_query.md`, `crate/core/sinex-gateway/docs/coordination.md`
- Current-state tracking → `crate/lib/sinex-services/docs/current_state_tracking.md`
- Data lifecycle → `crate/lib/sinex-db/docs/data_lifecycle.md`
- Type system and NATS subject contracts → `crate/lib/sinex-primitives/docs/type_system_patterns.md`, `crate/lib/sinex-primitives/docs/nats_subjects.md`
- Distributed runtime patterns, observability, extensibility → `crate/lib/sinex-node-sdk/docs/distributed_patterns.md`, `crate/lib/sinex-node-sdk/docs/observability.md`, `crate/lib/sinex-node-sdk/docs/extensibility.md`

## Crate-Level Reference Points

- `crate/core/sinex-gateway/docs/` — RPC surface, native messaging, replay orchestration, and coordination
- `crate/lib/sinex-services/docs/` — read-model and query service behavior
- `crate/lib/sinex-db/docs/` — persistence and lifecycle doctrine
- `crate/lib/sinex-primitives/docs/` — types, event contracts, and transport naming
- `crate/lib/sinex-node-sdk/docs/` — distributed runtime, checkpoints, observability, and extension patterns
- `crate/core/sinex-ingestd/docs/` — ingestion pipeline details

## Workspace Map (14 Workspace Members)

### What to import from where

| You need... | Import from | Key types |
|-------------|-------------|-----------|
| Types, errors, IDs, domain enums | `sinex_primitives::prelude::*` | `Event`, `Id<T>`, `SinexError`, `Timestamp`, `EventSource`, `EventType`, `Uuid` |
| Event creation | `sinex_primitives::events::payloads::*` | `EventPayload` trait, typed payload structs |
| Dynamic events | `sinex_primitives::events::{DynamicPayload, builder::EventBuilder}` | For runtime source/type |
| DB access | `sinex_db::DbPoolExt` | `pool.events()`, `pool.blobs()`, `pool.source_materials()` etc. |
| Node SDK | `sinex_node_sdk::*` | `IngestorNode`, `NodeConfig`, `node_entrypoint!`, runtime adapters |
| Derived nodes | `sinex_node_sdk::{TransducerNode, WindowedNode, ScopeReconcilerNode}` | Via `DerivedNodeAdapter<N>` |
| Privacy | `sinex_primitives::privacy::*` | `privacy::engine()`, `ProcessingContext` |
| Domain enums | `sinex_primitives::domain::*` | `OperationStatus`, `HealthStatus`, `DataTier`, `NodeType` etc. |
| Event field enums | `sinex_primitives::events::enums::*` | `FileModificationType`, `ShutdownReason`, etc. |
| Test utilities | `xtask::sandbox::prelude::*` | `TestContext`, `sinex_test`, `Timeouts` |

### Project Layout

```
crate/
  lib/
    sinex-primitives/    Foundation: types, validation, errors, IDs, privacy engine
    sinex-db/            Database pools, repositories, COPY protocol, query helpers
    sinex-schema/        DB schema definitions + declarative convergence engine
    sinex-macros/        #[derive(EventPayload)]
    sinex-node-sdk/      Node runtime: lifecycle, checkpoints, replay, CLI framework
  core/
    sinex-ingestd/       Ingestion daemon: NATS consumer -> batch writes -> confirmations
    sinex-gateway/       API gateway: JSON-RPC, SSE subscriptions, native messaging
    sinex-source-worker/ Unified source-unit host; parser/input-shape adapters live under
                         `src/sources/` instead of per-domain ingestor crates
    sinex-process/       Consolidated automata (#944): canonicalizer, analytics,
                         health, session-detector, hourly/daily summarizers,
                         entity/relation shadow-lane workers
  cli/
    sinexctl               Unified CLI: query, trace, telemetry, context, report, import
  tests/
    vm-suite               NixOS VM test binary
tests/e2e/                 End-to-end integration tests
tests/workspace/           Workspace-level test harness
xtask/                     Build automation, sandbox test infra, dev-loop tooling
```

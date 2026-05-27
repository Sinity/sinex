## Workspace Map (9 Workspace Members)

### What to import from where

| You need... | Import from | Key types |
|-------------|-------------|-----------|
| Types, errors, IDs, domain enums | `sinex_primitives::prelude::*` | `Event`, `Id<T>`, `SinexError`, `Timestamp`, `EventSource`, `EventType`, `Uuid` |
| Event creation | `sinex_primitives::events::payloads::*` | `EventPayload` trait, typed payload structs |
| Dynamic events | `sinex_primitives::events::{DynamicPayload, builder::EventBuilder}` | For runtime source/type |
| DB access | `sinex_db::DbPoolExt` | `pool.events()`, `pool.blobs()`, `pool.source_materials()` etc. |
| DB schema | `sinex_db::schema` | Schema definitions + declarative convergence engine |
| Node SDK | `sinex_node_sdk::*` | `SourceUnit`, `NodeConfig`, `node_entrypoint!`, runtime adapters |
| Derived nodes | `sinex_node_sdk::{Transducer, Windowed, ScopeReconciler}` | Via `AutomatonRuntime<N>` |
| Privacy | `sinex_primitives::privacy::*` | `privacy::engine()`, `ProcessingContext` |
| Domain enums | `sinex_primitives::domain::*` | `OperationStatus`, `HealthStatus`, `DataTier`, `NodeType` etc. |
| Event field enums | `sinex_primitives::events::enums::*` | `FileModificationType`, `ShutdownReason`, etc. |
| Test utilities | `xtask::sandbox::prelude::*` | `TestContext`, `sinex_test`, `Timeouts` |

### Project Layout

```
crate/
  sinex-primitives/    Foundation: types, validation, errors, IDs, privacy engine
  sinex-db/            Database pools, repositories, COPY protocol, query helpers,
                       schema definitions + declarative convergence (sinex_db::schema)
  sinex-macros/        #[derive(EventPayload)]
  sinex-node-sdk/      Node runtime: lifecycle, checkpoints, replay, CLI framework
  sinexd/              Unified daemon; internal modules:
    sinexd::event_engine   NATS consumer -> batch writes -> confirmations (was sinex-ingestd)
    sinexd::api            JSON-RPC, SSE subscriptions, native messaging (was sinex-gateway)
    sinexd::sources        Source-unit host; parser/input-shape adapters (was sinex-source-worker)
    sinexd::automata       Consolidated automata: canonicalizer, analytics, health,
                           session-detector, hourly/daily summarizers, entity/relation workers
    sinexd::supervisor     Module orchestrator: startup ordering, health gate, shutdown
  sinexctl/            Unified CLI: query, trace, telemetry, context, report, import
  sinex-e2e-tests/     End-to-end integration tests
  sinex-vm-suite/      NixOS VM test binary
  sinex-workspace-tests/ Workspace-level test harness
xtask/                 Build automation, sandbox test infra, dev-loop tooling
```

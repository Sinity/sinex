## Workspace Map (21 Workspace Members)

### What to import from where

| You need... | Import from | Key types |
|-------------|-------------|-----------|
| Types, errors, IDs, domain enums | `sinex_primitives::prelude::*` | `Event`, `Id<T>`, `SinexError`, `Timestamp`, `EventSource`, `EventType`, `Uuid` |
| Event creation | `sinex_primitives::events::payloads::*` | `EventPayload` trait, typed payload structs |
| Dynamic events | `sinex_primitives::events::{DynamicPayload, builder::EventBuilder}` | For runtime source/type |
| DB access | `sinex_db::DbPoolExt` | `pool.events()`, `pool.blobs()`, `pool.source_materials()` etc. |
| Node SDK | `sinex_node_sdk::*` | `IngestorNode`, `AutomatonNode`, `NodeConfig`, `node_entrypoint!` |
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
    sinex-services/      Business logic: PkmService (entity graph), ContentService (blobs)
  core/
    sinex-ingestd/       Ingestion daemon: NATS consumer -> batch writes -> confirmations
    sinex-gateway/       API gateway: JSON-RPC, SSE subscriptions, native messaging
  nodes/
    sinex-fs-ingestor/            file.created/modified/deleted
    sinex-terminal-ingestor/      shell.command, shell.history
    sinex-desktop-ingestor/       window.focused/closed, clipboard.*
    sinex-system-ingestor/        systemd.*, device.*, login.*
    sinex-document-ingestor/      document.parsed, document.extracted
    sinex-terminal-command-canonicalizer/  command.canonical (Transducer)
    sinex-analytics-automaton/    analytics.insight (Windowed)
    sinex-health-automaton/       health.aggregated_report (ScopeReconciler)
    sinex-session-detector/       activity.session.boundary (Windowed, not deployed)
  cli/
    sinexctl               Unified CLI: query, trace, telemetry, context, report, import
  tests/
    vm-suite               NixOS VM test binary
tests/e2e/                 End-to-end integration tests
xtask/                     Build automation (64K lines, sandbox test infra)
```

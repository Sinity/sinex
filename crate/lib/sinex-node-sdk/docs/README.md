# Sinex Node SDK

The Sinex Node SDK is the framework for building Sinex **ingestors** and
**derived nodes**. It provides the shared runtime pieces around lifecycle,
checkpointing, confirmation handling, replay participation, coordination, and
health/self-observation.

Unless a page is filed under **Vision & Roadmap**, the docs in this directory
describe the current runtime and public authoring surface. Historical rollout
language and future ideas belong in explicit history/vision pages, not in the
main architecture descriptions.

## 🧭 Navigation

### Current Runtime Model
- [**Overview**](overview.md) – Runtime shape, shared lifecycle phases, and how ingestors and derived nodes fit together.
- [**Stream Processing Runtime**](stream_runtime.md) – Deep dive into the derived-node traits (`TransducerNode`, `WindowedNode`, `ScopeReconcilerNode`), `IngestorNode`, and their adapters.
- [**Trait Selection**](trait-selection.md) – Decision flowchart for choosing the right node pattern.
- [**Node Patterns**](patterns.md) – Current capture-node and derived-node runtime patterns.
- [**Ingestion & Provenance**](provenance.md) – Rules for sensor/ingestor separation and Stage-as-You-Go patterns.

### Implementation Guides
- [**Ingestor Startup**](ingestor_startup.md) – Three-phase lifecycle (snapshot → gap-fill → continuous), crash recovery, source-material input-shape adapters.
- [**Distributed Coordination**](coordination.md) – Leadership election, handoff protocol, and strict lock ordering rules for reliability.
- [**Material Content Store**](content_store.md) – Hybrid local-CAS/large-object storage, backend-aware verification, and deduplication.
- [**Stage-as-You-Go**](stage_as_you_go.md) – Real-time provenance tracking for streaming data.
- [**Record Source Framework**](record_source.md) – Common source acquisition API for `SQLite` rows, append-only histories, and observation streams.
- [**Preflight Verification**](preflight.md) – Fail-fast safety checks for deployment readiness.
- [**Health Monitoring**](health_monitoring_integration.md) – Automatic success/error rate tracking and status emission.
- [**Distributed Patterns**](distributed_patterns.md) – Eventing, backpressure, idempotency, and runtime concurrency doctrine.
- [**Observability**](observability.md) – Journald-first monitoring and checkpoint telemetry.
- [**Extensibility**](extensibility.md) – Extension patterns for nodes, events, and runtime composition.

### Vision & Roadmap
- [**SDK Vision**](vision.md) – Non-current ideas and future-facing design work.

## 🛠️ Key Runtime Entry Points

- **Initialization**: `NodeInitContext::into_runtime()` yields a `NodeRuntimeState` with ergonomic accessors for acquisition, lifecycle, and coordination.
- **Replay**: Replay is gateway-orchestrated. Nodes participate via `scan_historical()` / `NodeScanCommand` but don't own replay lifecycle.
- **Testing**: Use `xtask::sandbox::TestRuntimeBuilder` and related sandbox helpers to provision ephemeral NATS and `PostgreSQL` environments.

## 📐 Design Principles

1. **Durable Lifecycle**: Shutdown persists checkpoint state to local files and
   NATS KV. Files are for fast local restart handoff; NATS is for durable
   recovery.
2. **Cooperative Shutdown**: Use `CancellationToken` and `WatcherHandle` for coordinated cleanup; avoid abrupt task aborts.
3. **Confirmed-Event Synthesis**: Derived nodes consume confirmations, checkpoint confirmed progress, and emit synthesis events with parent provenance.
4. **Privacy-by-Design**: Telemetry stays local via the "Self-Observation" pattern (metrics as events).

## 📚 See Also

- **Global Architecture**: `README.md#architecture`
- **Event Taxonomy**: `crate/lib/sinex-schema/docs/event-taxonomy.md`
- **Security Guardrails**: `README.md#security`

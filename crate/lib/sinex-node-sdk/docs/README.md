# Sinex Node SDK

The Sinex Node SDK is the framework for building Sinex **ingestors** and
**derived nodes**. It provides the shared runtime pieces around lifecycle,
checkpointing, confirmation handling, replay participation, coordination, and
health/self-observation.

## 🧭 Navigation

### Core Architecture
- [**Overview**](overview.md) – Runtime shape, shared lifecycle phases, and how ingestors and derived nodes fit together.
- [**Stream Processing Runtime**](stream_runtime.md) – Deep dive into the derived-node traits (`TransducerNode`, `WindowedNode`, `ScopeReconcilerNode`), `IngestorNode`, and their adapters.
- [**Trait Selection**](trait-selection.md) – Decision flowchart for choosing the right node pattern.
- [**Node Patterns**](patterns.md) – Higher-level runtime and deployment patterns.
- [**Ingestion & Provenance**](provenance.md) – Rules for sensor/ingestor separation and Stage-as-You-Go patterns.

### Implementation Guides
- [**Distributed Coordination**](coordination.md) – Leadership election, handoff protocol, and strict lock ordering rules for reliability.
- [**Material Content Store**](content_store.md) – Hybrid local-CAS/large-object storage, dual-hash verification, and deduplication.
- [**Stage-as-You-Go**](stage_as_you_go.md) – Real-time provenance tracking for streaming data.
- [**Record Source Framework**](record_source.md) – Common source acquisition API for `SQLite` rows, append-only histories, and observation streams.
- [**Preflight Verification**](preflight.md) – Fail-fast safety checks for deployment readiness.
- [**Health Monitoring**](health_monitoring_integration.md) – Automatic success/error rate tracking and status emission.
- [**Distributed Patterns**](distributed_patterns.md) – Eventing, backpressure, idempotency, and runtime concurrency doctrine.
- [**Observability**](observability.md) – Journald-first monitoring and checkpoint telemetry.
- [**Extensibility**](extensibility.md) – Extension patterns for nodes, events, and runtime composition.

### Vision & Roadmap
- [**SDK Vision**](vision.md) – Hot reload, Seamless Developer Experience, and the Prompt-to-Node development workflow.

## 🛠️ Key Runtime Entry Points

- **Initialization**: `NodeInitContext::into_runtime()` yields a `NodeRuntimeState` with ergonomic accessors for acquisition, lifecycle, and coordination.
- **Replay**: Replay is gateway-orchestrated. Nodes participate via `scan_historical()` / `NodeScanCommand` but don't own replay lifecycle.
- **Testing**: Use `xtask::sandbox::TestRuntimeBuilder` and related sandbox helpers to provision ephemeral NATS and `PostgreSQL` environments.

## 📐 Design Principles

1. **Best-Effort Lifecycle**: Shutdown attempts dual-checkpointing (File + NATS KV). File is for fast hot-rebuilds; NATS is for durable recovery.
2. **Cooperative Shutdown**: Use `CancellationToken` and `WatcherHandle` for coordinated cleanup; avoid abrupt task aborts.
3. **Optimistic Processing**: Automata can process provisional events with built-in rollback support if confirmation fails.
4. **Privacy-by-Design**: Telemetry stays local via the "Self-Observation" pattern (metrics as events).

## 📚 See Also

- **Global Architecture**: `README.md#architecture`
- **Event Taxonomy**: `crate/lib/sinex-schema/docs/event-taxonomy.md`
- **Security Guardrails**: `README.md#security`

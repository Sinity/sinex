# Sinex Node SDK

The Sinex Node SDK is the unified framework for building **Ingestors** (sensors that capture external data) and **Automata** (nodes that synthesize new insights from existing events). It implements a **Unified Node Architecture**, where all nodes are stateful stream nodes.

## 🧭 Navigation

### Core Architecture
- [**Overview**](overview.md) – The Unified Node Architecture, distributed service architecture, and the three-phase startup pattern.
- [**Stream Processing Runtime**](stream_runtime.md) – Deep dive into the `AutomatonNode` and `IngestorNode` abstractions (Gen2 patterns).
- [**Node Patterns**](patterns.md) – Distinguishing between "Edge" (Stream Processors) and "Core" (Automatons) deployment models.
- [**Ingestion & Provenance**](provenance.md) – Rules for sensor/ingestor separation and Stage-as-You-Go patterns.

### Implementation Guides
- [**Distributed Coordination**](coordination.md) – Leadership election, handoff protocol, and strict lock ordering rules for reliability.
- [**Annex Subsystem**](annex.md) – Large file management via git-annex, dual-hash verification, and deduplication.
- [**Stage-as-You-Go**](stage_as_you_go.md) – Real-time provenance tracking for streaming data.
- [**Preflight Verification**](preflight.md) – Fail-fast safety checks for deployment readiness.
- [**Health Monitoring**](health_monitoring_integration.md) – Automatic success/error rate tracking and status emission.

### Vision & Roadmap
- [**SDK Vision**](vision.md) – Hot reload, Seamless Developer Experience, and the Prompt-to-Node development workflow.

## 🛠️ Key Runtime Entry Points

- **Initialization**: `NodeInitContext::into_runtime()` yields a `NodeRuntimeState` with ergonomic accessors for acquisition, lifecycle, and coordination.
- **Replay**: `replay::ReplayService::from_runtime` is the canonical way to construct replay pipelines.
- **Testing**: Use `sinex_test_utils::TestRuntimeBuilder` to provision ephemeral NATS and PostgreSQL environments.

## 📐 Design Principles

1. **Best-Effort Lifecycle**: Shutdown attempts dual-checkpointing (File + NATS KV). File is for fast hot-rebuilds; NATS is for durable recovery.
2. **Cooperative Shutdown**: Use `CancellationToken` and `WatcherHandle` for coordinated cleanup; avoid abrupt task aborts.
3. **Optimistic Processing**: Automata can process provisional events with built-in rollback support if confirmation fails.
4. **Privacy-by-Design**: Telemetry stays local via the "Self-Observation" pattern (metrics as events).

## 📚 See Also

- **Global Architecture**: `docs/current/architecture/`
- **Event Taxonomy**: `crate/lib/sinex-schema/docs/event-taxonomy.md`
- **Security Guardrails**: `docs/current/security.md`

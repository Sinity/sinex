# Stream Processing Runtime

This document describes the current node authoring traits and the runtime
behavior around them. It is not a design-history page: the goal is to explain
what a contributor should implement against today.

## 🧱 The Abstractions

### 1. Derived Node Traits

Derived nodes consume confirmed events and emit derived events. The shared
`AutomatonRuntime` handles the common runtime work for all three trait
families:

- **`Transducer`**: 1:1 stateless event transformation
  - Simple filtering/enrichment without complex state
  - Example: command canonicalizer

- **`Windowed`**: Time-window aggregation
  - Accumulate events over time buckets, emit summaries
  - Example: analytics aggregator, metrics summarizer

- **`ScopeReconciler`**: Per-scope state tracking
  - Maintain distinct state per scope (source, device, etc.)
  - Example: health monitor, scope-aware reconciler

**Shared adapter responsibilities**
- **Checkpointing**: state and stream progress persist through the normal SDK
  checkpoint surfaces
- **Invalidation handling**: replay invalidations are consumed and reflected in
  adapter state
- **Health/self-observation**: per-run counters and error-rate signals are
  emitted through the shared telemetry paths
- **Drain and shutdown**: the adapter can stop intake, finish buffered work,
  persist state, and exit cleanly

### 2. `SourceUnit`

Ingestors capture from external sources such as files, sockets, journals, or
APIs.

- **Authoring surface**: implement `scan_snapshot`, `scan_historical`, and
  `run_continuous`
- **Continuous ownership**: the node owns its source-specific watch/tail logic
- **Provenance responsibility**: emitted events are material-provenance events
  rooted in source material
- **Checkpointing**: in-memory state is flushed to NATS KV and local files

### 3. `NodeRunner` and Adapters

The low-level `NodeRunner` provides lifecycle orchestration, while the adapters
turn the high-level traits into runnable services. Most node implementations
should use the trait + adapter surface instead of implementing low-level
runtime behavior themselves.

## 🔄 Processing Pipeline

The runtime follows a provisional/confirmed pattern:

1. Nodes publish provisional events to NATS.
2. ingestd validates and persists events to `PostgreSQL`.
3. ingestd publishes confirmations.
4. Automata consume confirmed events and advance checkpoints.

That pipeline-level provisional state is only about transport durability. It is
not a semantic license to emit "tentative" derived events whenever sibling
sources might arrive later. Late-arrival coordination uses the normal derived
node models:

- `Transducer` stays eager and 1:1;
- `Windowed` performs bounded waiting when window closure is part of the
  domain truth;
- `ScopeReconciler` handles late correction through scope invalidation,
  recomputation, and replacement relations.

## 💾 State Persistence Pattern

State is stored using a dual-destination strategy:

| Destination | Role | Rationale |
| :--- | :--- | :--- |
| **NATS KV** | Primary | Distributed durability for crash recovery. |
| **Local File** | Secondary | Fast local restart handoff during shutdown/restart. |

> [!IMPORTANT]
> Local files take precedence during startup. If a file-based checkpoint exists,
> the node resumes from that state before falling back to the distributed
> checkpoint store.

## 🛑 Drain and Cooperative Shutdown

Nodes support an orderly drain/shutdown path:

1.  **Signal**: The runtime receives drain or shutdown.
2.  **Stop intake**: Background watchers/consumers stop accepting new work.
3.  **Flush in-flight work**: Watchers finish their current slice; derived nodes
    drain buffered confirmed events and invalidations.
4.  **Persist state**: Final state is written to disk and NATS KV.
5.  **Exit**: The process terminates after the runtime reports drain/shutdown
    completion.

## 🛡️ Path Validation

All filesystem operations must pass through the `VerifiedPath` type. This prevents:
- **Directory Traversal**: Patterns like `../../etc/passwd` are rejected at the type level.
- **Symlink Attacks**: Predictable temp filenames are avoided via `create_secure_temp_path`.

## 🚦 Error Settlement

Error handling uses `FailurePolicy::settle()` with `DefaultFailurePolicy`, which
maps `ErrorClass` to `Settlement` variants with backoff and retry budgets:

- `Settlement::Commit`: Benign error — continue processing.
- `Settlement::SendToProcessingFailure`: Route the event to the processing-failure queue.
- `Settlement::Retry`: Retry with exponential backoff and budget.
- `Settlement::Park`: Retry budget exhausted — park for later inspection.
- `Settlement::HaltNode`: Fatal error — halt the node.
- `Settlement::DrainRuntimeUnit`: Drain the runtime unit and restart.

## Historical context

Two retired design branches are worth keeping as explicit history so they do
not drift back in as supposed current doctrine:

- **The actual `Windowed` API delegates window-completion to the implementor.** Earlier design documents describe a `WindowPolicy` / `WindowAction` enum-based API that was never implemented. The current trait (see `derived_node/traits.rs`) requires `accumulate`, `window_complete(&self, state) -> bool`, and `emit`, with `recompute_window` as a default-impl hook. Don't reintroduce the policy-enum shape — it lost against delegating the completion predicate to the implementor.
- **`AutomatonNode` has been fully removed** in favor of the derived-node model family (`Transducer` / `Windowed` / `ScopeReconciler`). `PersistedState` still exists as a standalone type in the derived-node module. `ErrorAction` has been replaced by `DefaultFailurePolicy::settle()` which maps `ErrorClass` to `Settlement` variants with backoff and retry budgets.

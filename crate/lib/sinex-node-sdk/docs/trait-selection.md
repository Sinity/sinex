# Node Trait Selection Guide

Which trait should you implement for a new node? This document provides a decision
flowchart and comparison matrix.

## Quick Decision Flowchart

```
Does your node capture data from the external world?
  ├── YES → Does it handle large binary content (files, documents)?
  │           ├── YES → IngestorNode + StageAsYouGoContext
  │           └── NO  → IngestorNode
  └── NO → Does it transform/filter existing events?
            ├── YES → Is it 1:1 stateless transformation?
            │           ├── YES → TransducerNode
            │           └── NO  → Does it aggregate over time windows?
            │                       ├── YES → WindowedNode
            │                       └── NO  → ScopeReconcilerNode
            └── NO → Node (direct impl, rare)
```

## Trait Comparison Matrix

| Aspect | IngestorNode | TransducerNode | WindowedNode | ScopeReconcilerNode | StageAsYouGoContext | Node (base) |
|--------|-------------|--------|---------|-----------|-----------|-------------|
| **Purpose** | External world → events | 1:1 event transformation | Time-windowed aggregation | State reconciliation | Binary content staging | Runtime dispatch |
| **Input** | External sources | Single event → output | Event stream over window | Scope state updates | Raw bytes + metadata | Checkpoint + TimeHorizon |
| **Output** | `ScanReport` (batched) | `Option<Output>` | Windowed aggregate | Reconciled state events | Material-linked events | `ScanReport` |
| **Scan modes** | Snapshot, historical, continuous | Continuous only | Continuous only | Continuous only | N/A (helper pattern) | All (delegated) |
| **State type** | Custom `S` in `IngestorState<S>` | Custom `S` in `PersistedState<S>` | Custom `S` in `PersistedState<S>` | Custom `S` in `PersistedState<S>` | N/A | Via adapter |
| **Checkpoint** | Explicit (user returns in state) | Automatic (every N events / M secs) | Automatic (window-based) | Automatic (scope-driven) | Via enclosing ingestor | In `ScanReport` |
| **Error handling** | User responsibility | Auto DLQ/skip/retry | Auto DLQ/skip/retry | Auto DLQ/skip/retry | User responsibility | N/A |
| **Config** | User-defined type | Built-in `NodeAdapterConfig` | Built-in `NodeAdapterConfig` | Built-in `NodeAdapterConfig` | N/A | Generic |
| **Boilerplate** | ~50 lines | ~15 lines | ~20 lines | ~25 lines | Used within ingestors | ~100+ lines |
| **Health monitoring** | Manual | Automatic (if NATS available) | Automatic (if NATS available) | Automatic (if NATS available) | N/A | Via adapter |

## When to Use Each Trait

### IngestorNode — "I capture data from the world"

Use when your node watches or polls an external source and produces events.

**Examples:** filesystem watcher, shell history reader, system journal scanner,
desktop activity tracker, document parser.

**Key characteristics:**
- Three scan modes: snapshot (full scan), historical (time range), continuous (live)
- User controls the live event loop in `run_continuous(ContinuousStart, ...)`
- Continuous mode receives a live-tail resume cursor, not permission to perform
  historical import; startup snapshot and gap-fill belong to the SDK runner
- State is checkpointed to file + NATS KV automatically via `IngestorNodeAdapter`
- Exploration hooks available (`get_source_state`, `get_ingestion_history`, `export_data`)

### TransducerNode — "I perform 1:1 event transformation"

Use when your node transforms individual events in a stateless manner.

**Examples:** command canonicalizer, event enricher, format converter.

**Key characteristics:**
- Pure 1:1 transformation: one input event → zero or one output event
- Minimal state (mostly event-independent)
- Return `Some(output)` to emit, `None` to filter/skip
- Error handling via DLQ, retry, or skip actions
- Lowest boilerplate for simple transformations
- Do not use as an implicit waiting or cross-source reconciliation mechanism

### WindowedNode — "I aggregate events over time windows"

Use when your node combines multiple events within a time or count window.

**Examples:** analytics aggregator, metrics summarizer, trend detector.

**Key characteristics:**
- Processes events within sliding time windows or event-count buckets
- Emits aggregate results at window boundaries
- Maintains window state across multiple input events
- Automatic checkpoint at window completion
- Return aggregated `Some(output)` per window boundary
- Correct choice when bounded waiting is part of the domain truth

### ScopeReconcilerNode — "I track and reconcile scope state"

Use when your node maintains per-scope state and emits reconciliation events.

**Examples:** health monitor, state tracker, scope-aware aggregator.

**Key characteristics:**
- Tracks distinct scopes (e.g., per-source, per-device)
- Maintains and evolves state per scope
- Emits reconciliation events when scope state changes
- Handles scope creation, updates, and cleanup
- Automatic DLQ/retry for failed reconciliations
- Correct choice when late-arriving evidence may require recomputing the current
  best output for a scope

## Late-Arrival Decision Rule

When sources for the same logical activity arrive with different latencies:

- keep raw ingestors eager;
- keep simple 1:1 normalization as `TransducerNode`;
- use `WindowedNode` only for real bounded windows;
- use `ScopeReconcilerNode` when the output may need replacement after the
  scope's working set changes.

Do not introduce a general-purpose derived-event provisional/final model just
because the runtime transport itself is provisional-before-confirmed.

### StageAsYouGoContext — "I need streaming content capture"

Not a node trait — a **context helper** used within `IngestorNode` implementations.

Use when your ingestor handles large binary content that should be staged
progressively into JetStream rather than buffered in memory.

**Workflow:**
1. `register_in_flight()` → get material ID immediately
2. `emit_event_with_provenance()` → emit events linking to material
3. `finalize_source_material()` → complete with full content

**Examples:** file content ingestor, document parser with streaming extraction.

### Node (direct impl) — "The adapters don't fit my use case"

Almost never needed. Both `IngestorNodeAdapter` and `DerivedNodeAdapter` implement
this trait for you. Only implement directly if you need custom scan dispatching or
a node type that doesn't fit the ingestor/derived-node model.

## Real Implementations

| Node | Trait | Adapter | Crate |
|------|-------|---------|-------|
| sinex-fs-ingestor | `IngestorNode` + `StageAsYouGoContext` | `IngestorNodeAdapter` | `crate/nodes/sinex-fs-ingestor` |
| sinex-terminal-ingestor | `IngestorNode` | `IngestorNodeAdapter` | `crate/nodes/sinex-terminal-ingestor` |
| sinex-desktop-ingestor | `IngestorNode` | `IngestorNodeAdapter` | `crate/nodes/sinex-desktop-ingestor` |
| sinex-system-ingestor | `IngestorNode` | `IngestorNodeAdapter` | `crate/nodes/sinex-system-ingestor` |
| sinex-document-ingestor | `IngestorNode` + `StageAsYouGoContext` | `IngestorNodeAdapter` | `crate/nodes/sinex-document-ingestor` |
| sinex-analytics-automaton | `WindowedNode` | `WindowedNodeAdapter` | `crate/nodes/sinex-analytics-automaton` |
| sinex-terminal-command-canonicalizer | `TransducerNode` | `TransducerNodeAdapter` | `crate/nodes/sinex-terminal-command-canonicalizer` |
| sinex-health-automaton | `ScopeReconcilerNode` | `ScopeReconcilerNodeAdapter` | `crate/nodes/sinex-health-automaton` |

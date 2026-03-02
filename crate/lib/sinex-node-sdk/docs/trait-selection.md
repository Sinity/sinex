# Node Trait Selection Guide

Which trait should you implement for a new node? This document provides a decision
flowchart and comparison matrix.

## Quick Decision Flowchart

```
Does your node capture data from the external world?
  ├── YES → Does it handle large binary content (files, documents)?
  │           ├── YES → IngestorNode + StageAsYouGoNode pattern
  │           └── NO  → IngestorNode
  └── NO → Does it transform/filter existing events?
            ├── YES → AutomatonNode
            └── NO  → Node (direct impl, rare)
```

## Trait Comparison Matrix

| Aspect | IngestorNode | AutomatonNode | StageAsYouGoNode | Node (base) |
|--------|-------------|---------------|------------------|-------------|
| **Purpose** | External world → events | Event → derived event | Binary content staging | Runtime dispatch |
| **Input** | External sources | Single typed event | Raw bytes + metadata | Checkpoint + TimeHorizon |
| **Output** | `ScanReport` (batched) | `Option<Output>` per event | `StageAsYouGoResult` | `ScanReport` |
| **Scan modes** | Snapshot, historical, continuous | Continuous only | N/A (helper pattern) | All (delegated) |
| **State type** | Custom `S` in `IngestorState<S>` | Custom `S` in `PersistedState<S>` | N/A | Via adapter |
| **Checkpoint** | Explicit (user returns in state) | Automatic (every N events / M secs) | Via enclosing ingestor | In `ScanReport` |
| **Error handling** | User responsibility | Auto DLQ/skip/retry | User responsibility | N/A |
| **Config** | User-defined type | Built-in `NodeAdapterConfig` | N/A | Generic |
| **Boilerplate** | ~50 lines | ~10 lines | Used within ingestors | ~100+ lines |
| **Health monitoring** | Manual | Automatic (if NATS available) | N/A | Via adapter |

## When to Use Each Trait

### IngestorNode — "I capture data from the world"

Use when your node watches or polls an external source and produces events.

**Examples:** filesystem watcher, shell history reader, system journal scanner,
desktop activity tracker, document parser.

**Key characteristics:**
- Three scan modes: snapshot (full scan), historical (time range), continuous (live)
- User controls the event loop in `run_continuous()`
- State is checkpointed to file + NATS KV automatically via `IngestorNodeAdapter`
- Exploration hooks available (`get_source_state`, `get_coverage_analysis`)

### AutomatonNode — "I process events from the pipeline"

Use when your node subscribes to events and optionally emits derived events.

**Examples:** command canonicalizer, analytics aggregator, health monitor,
content classifier.

**Key characteristics:**
- Minimal boilerplate — implement `process()` and a few metadata methods
- Automatic batching, checkpointing, and health reporting
- Return `Some(output)` to emit, `None` to filter/skip
- Error handling via `handle_error()` → `ErrorAction::{Retry, SendToDLQ, Skip}`
- Designed to be LLM-friendly (constrained enough for reliable code generation)

### StageAsYouGoNode — "I need streaming content capture"

Not a standalone node — a **pattern** used within `IngestorNode` implementations.

Use when your ingestor handles large binary content that should be staged
progressively into JetStream rather than buffered in memory.

**Workflow:**
1. `register_in_flight()` → get material ID immediately
2. `emit_event_with_provenance()` → emit events linking to material
3. `finalize_source_material()` → complete with full content

**Examples:** file content ingestor, document parser with streaming extraction.

### Node (direct impl) — "The adapters don't fit my use case"

Almost never needed. Both `IngestorNodeAdapter` and `AutomatonNodeAdapter` implement
this trait for you. Only implement directly if you need custom scan dispatching or
a node type that doesn't fit the ingestor/automaton dichotomy.

## Real Implementations

| Node | Trait | Crate |
|------|-------|-------|
| sinex-fs-ingestor | `IngestorNode` + `StageAsYouGoNode` | `crate/nodes/sinex-fs-ingestor` |
| sinex-terminal-ingestor | `IngestorNode` | `crate/nodes/sinex-terminal-ingestor` |
| sinex-desktop-ingestor | `IngestorNode` | `crate/nodes/sinex-desktop-ingestor` |
| sinex-system-ingestor | `IngestorNode` | `crate/nodes/sinex-system-ingestor` |
| sinex-document-ingestor | `IngestorNode` + `StageAsYouGoNode` | `crate/nodes/sinex-document-ingestor` |
| sinex-analytics-automaton | `AutomatonNode` | `crate/nodes/sinex-analytics-automaton` |
| sinex-terminal-command-canonicalizer | `AutomatonNode` | `crate/nodes/sinex-terminal-command-canonicalizer` |
| sinex-health-automaton | `AutomatonNode` | `crate/nodes/sinex-health-automaton` |

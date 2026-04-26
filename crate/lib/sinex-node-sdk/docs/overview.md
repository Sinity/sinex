# Sinex Node SDK: Architecture Overview

This document describes the current Sinex node runtime. It is about the code
that exists today: the traits, adapters, and runtime responsibilities that
ingestors and derived nodes actually use. Vision-only ideas live in
[`vision.md`](vision.md).

The SDK does **not** collapse ingestors and derived nodes into one authoring
model. They share infrastructure where the system benefits from uniformity, and
they keep separate traits where their work is genuinely different.

## 📐 Current Runtime Shape

The runtime has two authoring families built on shared plumbing:

### Ingestors

Use `IngestorNode` when a node reads from an external source and emits
material-provenance events.

- Owns the three-phase capture lifecycle: snapshot, historical catch-up, and
  continuous processing
- Uses SDK helpers for source acquisition, material staging, checkpointing, and
  shutdown
- Publishes provisional events that ingestd later persists and confirms

### Derived Nodes

Use `TransducerNode`, `WindowedNode`, or `ScopeReconcilerNode` when a node
consumes confirmed events and emits synthesis-provenance events.

- `TransducerNode`: stateless 1:1 transformation or filtering
- `WindowedNode`: accumulate state until the implementation decides the window
  is complete
- `ScopeReconcilerNode`: keep per-scope state and reconcile it into outputs

All three run through `DerivedNodeAdapter`, which handles checkpointing,
confirmation consumption, invalidation handling, health emission, and orderly
shutdown/drain behavior.

### Shared Runtime Infrastructure

Both families reuse the same operational pieces:

1. **Shared lifecycle phases**: snapshot, historical catch-up, and continuous processing.
2. **Shared checkpointing**: durable checkpoint/state management across node restarts.
3. **Shared runtime plumbing**: NATS transport, confirmations, health reporting, coordination, and shutdown handling.
4. **Reusable source adapters**: common input shapes like append-only UTF-8
   tail sources, checkpointed `SQLite` history readers, and incremental
   file-import roots live in the SDK so new ingestors extend the normal runtime
   plane instead of rebuilding bespoke readers or direct-import paths.
5. **Reusable material writers**: high-volume logical records can use
   SDK-managed rotating append streams, including buffered writers that
   coalesce records while returning exact per-record byte anchors.

## 🛰️ Runtime Boundaries

Sinex nodes communicate via a distributed event bus powered by NATS `JetStream`.

```text
┌─────────────────────┐     ┌─────────────────────┐     ┌─────────────────────┐
│   External World    │────▶│    Sinex Nodes      │────▶│   Data Substrate    │
│                     │     │ (Stateful Streams)  │     │                     │
│ • Files             │     │ • fs-ingestor       │     │ • core.events       │
│ • Terminal          │     │ • session-detector  │     │ • core.blobs        │
│ • Desktop           │     │ • health-automaton  │     │ • source materials  │
└─────────────────────┘     └──────────┬──────────┘     └──────────┬──────────┘
                                       │                           │
                            ┌──────────▼──────────┐                │
                            │   NATS JetStream    │◀───────────────┘
                            │                     │
                            │ • events.raw.*      │ (Submitted)
                            │ • events.confirm.*  │ (Persisted)
                            │ • events.dlq.*      │ (Failures)
                            └─────────────────────┘
```

### 🛡️ Data Integrity: The Single-Writer Pattern

Nodes do not write directly to `core.events`.

- Nodes publish provisional events to NATS.
- `sinex-ingestd` validates and persists them.
- `sinex-ingestd` publishes confirmations.
- Derived nodes consume confirmations, not speculative provisional events.

Replay is gateway-orchestrated. Nodes participate through snapshot and
historical scan surfaces, but replay planning and lifecycle control are not
owned by the node SDK.

## 🔄 Three-Phase Startup Pattern

The runtime exposes snapshot, historical, and continuous phases, but those
phases matter differently to the two node families:

### Ingestors

Ingestors use all three phases to keep capture complete:

1.  **Phase 1: Snapshot**
    *   Captures the *current* state of the external system.
    *   *Example*: Filesystem node scans all existing files.
2.  **Phase 2: Gap-Filling (Historical)**
    *   Processes events that occurred while the node was offline.
    *   Uses `TimeHorizon::Historical { end_time }`.
3.  **Phase 3: Continuous Processing**
    *   Real-time event monitoring and streaming.
    *   Receives `ContinuousStart`, whose checkpoint is only a live-tail resume cursor.
    *   Continues indefinitely until shutdown.

### Derived Nodes

Derived nodes run as long-lived consumers over confirmed event streams. They
still participate in lifecycle, checkpointing, invalidation, and shutdown, but
they do not implement "snapshot the outside world" authoring logic.

## 💾 Checkpoint & State Management

Nodes use a dual-destination persistence strategy:
- **Primary (NATS KV)**: Durable, distributed storage for crash recovery.
- **Secondary (Local File)**: Fast state serialization during shutdown so local
  restarts can resume without waiting on the distributed store.

Checkpoints support multiple anchoring strategies:
- `Timestamp`: Resuming based on wall-clock time (Journald).
- `Sequence`: Resuming based on internal `UUIDv7` IDs (NATS).
- `External`: Opaque cursor data for custom integrations.

## 🧬 Provenance & Lineage

The SDK automatically enforces data lineage:
- **Ingested Events**: Linked to `SourceMaterial` via byte offsets and hashes.
- **Synthesized Events**: Linked to parent events via `source_event_ids`.
- **Content Verification**: Material content is verified with BLAKE3 plus backend-aware digests when the storage backend exposes them.

For row-like or metadata-only observations, use the Record Source framework:
`RecordSources::*` for checkpointed reads, `BufferedRecordSourceHarness` for
the default read/process/materialize loop, and `BufferedRecordMaterializer` for
push-only observation streams. These APIs batch adjacent logical records into
fewer physical source-material slices, rotate through the normal SDK policy, and
return `SourceRecordAnchor` values for event provenance. This keeps events
byte-addressable without creating one tiny material or one fsync-heavy slice per
observation.

## 🚦 Error Handling, Raw DLQ, and Recovery

- **Automatic Retries**: SDK handles transient NATS, source-material, and database connectivity failures where the owning runtime path can retry safely.
- **Raw-Ingest DLQ**: Raw ingest/material failures are routed through the
  operator-facing DLQ stream/subjects for inspection and retry.
- **Processing-Failure Queue**: Derived/runtime processing failures are routed
  to a separate processing-failure stream instead of masquerading as raw ingest.
- **Recovery Spool**: If publishing that processing-failure payload fails, the
  node writes a per-work-dir recovery spool file for honest local recovery.
- **Backpressure**: The confirmation buffer applies backpressure via NAKs if the node falls too far behind.

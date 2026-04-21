# Sinex Node SDK: Architecture Overview

The Sinex Node SDK is the foundational library for building Sinex ingestors and
derived nodes. It does not erase the distinction between those roles; it gives
them shared runtime infrastructure where that sharing is useful and separate
authoring traits where their responsibilities differ.

## 📐 Core Runtime Shape

The SDK exposes a low-level `Node` runtime surface plus higher-level traits and
adapters for the common cases:

1. **Shared lifecycle phases**: snapshot, historical catch-up, and continuous processing.
2. **Shared checkpointing**: durable checkpoint/state management across node restarts.
3. **Shared runtime plumbing**: NATS transport, confirmations, health reporting, coordination, and shutdown handling.
4. **Different authoring traits**: `IngestorNode` for external capture, and `TransducerNode` / `WindowedNode` / `ScopeReconcilerNode` for synthesis.
5. **Reusable source adapters**: common input shapes like append-only UTF-8 tail sources, checkpointed `SQLite` history readers, and incremental file-import roots live in the SDK so future ingestors can extend the normal node/runtime plane instead of rebuilding bespoke readers or direct-import paths.

## 🛰️ Distributed Service Architecture

Sinex nodes communicate via a distributed event bus powered by NATS `JetStream`.

```text
┌─────────────────────┐     ┌─────────────────────┐     ┌─────────────────────┐
│   External World    │────▶│    Sinex Nodes      │────▶│   Data Substrate    │
│                     │     │ (Stateful Streams)  │     │                     │
│ • Files             │     │ • fs-ingestor       │     │ • core.events       │
│ • Terminal          │     │ • session-detector  │     │ • core.blobs        │
│ • Desktop           │     │ • health-automaton  │     │ • core.checkpoints  │
└─────────────────────┘     └──────────┬──────────┘     └──────────┬──────────┘
                                       │                           │
                            ┌──────────▼──────────┐                │
                            │   NATS JetStream    │◀───────────────┘
                            │                     │
                            │ • events.raw.*      │ (Provisional)
                            │ • events.confirm.*  │ (Canonical ID)
                            │ • events.dlq.*      │ (Failures)
                            └─────────────────────┘
```

### 🛡️ Data Integrity: The Single-Writer Pattern
Nodes do not write directly to the `core.events` table. They submit provisional
events to NATS. `sinex-ingestd` acts as the single persistence writer, commits
the validated events to Postgres, and publishes confirmations back onto the
runtime bus.

## 🔄 Three-Phase Startup Pattern

All nodes follow a consistent startup sequence to ensure data completeness:

1.  **Phase 1: Snapshot**
    *   Captures the *current* state of the external system.
    *   *Example*: Filesystem node scans all existing files.
2.  **Phase 2: Gap-Filling (Historical)**
    *   Processes events that occurred while the node was offline.
    *   Uses `TimeHorizon::Historical { end_time }`.
3.  **Phase 3: Continuous Processing**
    *   Real-time event monitoring and streaming.
    *   Continues indefinitely until shutdown.

## 💾 Checkpoint & State Management

Nodes utilize a dual-destination persistence strategy:
- **Primary (NATS KV)**: Durable, distributed storage for crash recovery.
- **Secondary (Local File)**: Ultra-fast state serialization during SIGTERM to support seamless **Hot Reload**.

Checkpoints support multiple anchoring strategies:
- `Timestamp`: Resuming based on wall-clock time (Journald).
- `Sequence`: Resuming based on internal `UUIDv7` IDs (NATS).
- `External`: Opaque cursor data for custom integrations.

## 🧬 Provenance & Lineage

The SDK automatically enforces data lineage:
- **Ingested Events**: Linked to `SourceMaterial` via byte offsets and hashes.
- **Synthesized Events**: Linked to parent events via `source_event_ids`.
- **Dual-Hash Verification**: Large files are verified using both BLAKE3 (Sinex-native) and SHA256 (Git-annex native) to detect tampering.

## 🚦 Error Handling & DLQ

- **Automatic Retries**: SDK handles transient NATS and gRPC connectivity issues.
- **Dead Letter Queue (DLQ)**: Failed events are routed to `events.dlq.<source>` for manual inspection and operator-triggered retry.
- **Backpressure**: The confirmation buffer applies backpressure via NAKs if the node falls too far behind.

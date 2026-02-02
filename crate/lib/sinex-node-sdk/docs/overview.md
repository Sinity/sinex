# Sinex Node SDK: Architecture Overview

The Sinex Node SDK is the foundational library for building all Sinex services. It implements a **Unified Node Architecture** where the distinction between data capturers (Ingestors) and data processors (Automata) is eliminated—both are modeled as **Stateful Stream Processors**.

## 📐 Core Vision: Unified Architecture

Every node in the system implements the unified `Node` trait. This ensures architectural consistency across the system:

1.  **Unified Interface**: Both Ingestors and Automata use the same `scan(from: Checkpoint, until: TimeHorizon)` primitive.
2.  **Unified Checkpoints**: Resumption logic is identical whether tracking a file offset (Ingestor) or a NATS sequence number (Automaton).
3.  **Unified Deployment**: Nodes can be deployed as lightweight "Edge" processors (NATS-only) or "Core" automatons (Postgres-heavy).

## 🛰️ Distributed Service Architecture

Sinex nodes communicate via a distributed event bus powered by NATS `JetStream`.

```text
┌─────────────────────┐     ┌─────────────────────┐     ┌─────────────────────┐
│   External World    │────▶│    Sinex Nodes      │────▶│   Data Substrate    │
│                     │     │ (Stateful Streams)  │     │                     │
│ • Files             │     │ • fs-ingestor       │     │ • core.events       │
│ • Terminal          │     │ • pkm-automaton     │     │ • core.blobs        │
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
Nodes never write directly to the `core.events` database table. They submit provisional events to NATS. The `sinex-ingestd` daemon acts as the **Single Writer**, persisting events to Postgres and emitting a **Confirmation** with the canonical Database ID.

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
- `Sequence`: Resuming based on internal ULIDs (NATS).
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
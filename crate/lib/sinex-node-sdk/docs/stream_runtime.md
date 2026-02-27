# Stream Processing Runtime (Gen2)

The Sinex SDK provides high-level abstractions—`AutomatonNode` and `IngestorNode`—that reduce boilerplate and enable LLM-friendly development. These "Gen2" patterns automate state management, checkpointing, and lifecycle transitions.

## 🧱 The Abstractions

### 1. `AutomatonNode` (Automata)
Designed for processing event streams and synthesizing new events.
- **Auto-State**: State is automatically persisted to NATS KV.
- **Runtime-Integrated**: Composes with `NodeRunner` and node-specific processing bridges.
- **Health**: Integrates with `HealthReporter` for automatic error rate monitoring.

### 2. `IngestorNode` (Sensors)
Tailored for capturing data from external sources (Files, APIs, Sockets).
- **Control**: Manages its own continuous loop (sensor mode).
- **Symmetry**: Implements `scan_snapshot`, `scan_historical`, and `run_continuous`.
- **Checkpointing**: In-memory state is flushed to NATS KV and local files.

## 🔄 Processing Pipeline

The runtime follows a provisional/confirmed pattern:

1. Nodes publish provisional events to NATS.
2. ingestd validates and persists events to PostgreSQL.
3. ingestd publishes confirmations.
4. Automata consume confirmed events and advance checkpoints.

## 💾 State Persistence Pattern

State is stored using a dual-destination strategy:

| Destination | Role | Rationale |
| :--- | :--- | :--- |
| **NATS KV** | Primary | Distributed durability for crash recovery. |
| **Local File** | Secondary | Ultra-fast serialization for **Hot Reload** restarts. |

> [!IMPORTANT]
> Local files take precedence during startup. If a file-based checkpoint exists, the node assumes it was just rebuilt and resumes immediately.

## 🛑 Cooperative Shutdown

Unlike "Gen1" nodes that used forceful task aborts, Gen2 nodes use **Cooperative Cancellation**:

1.  **Signal**: Node receives SIGTERM.
2.  **Broadcast**: `watch::channel` notifies all background watchers.
3.  **Finalize**: Watchers finish their current slice and finalize `SourceMaterial`.
4.  **Checkpoint**: Final state is written to disk and NATS.
5.  **Exit**: Process terminates cleanly.

## 🛡️ Path Validation

All filesystem operations must pass through the `VerifiedPath` type. This prevents:
- **Directory Traversal**: Patterns like `../../etc/passwd` are rejected at the type level.
- **Symlink Attacks**: Predictable temp filenames are avoided via `create_secure_temp_path`.

## 🚦 Error Actions

Nodes define their behavior via the `ErrorAction` enum:
- `Retry`: NAK the message for redelivery.
- `SendToDLQ`: Log failure and move message to the Dead Letter Queue.
- `Skip`: Continue processing without further action.

# Sinex Satellite SDK

Shared library for building Sinex satellite services (event sources and automata).

This crate provides:
- Common traits and interfaces
- JetStream helpers for publishing to `events.raw.*` and consuming confirmations/DLQs
- Configuration management
- Lifecycle management and graceful shutdown
- State persistence and checkpointing
- Historical replay capabilities

## Core Architecture: Deep Symmetry

All satellites implement the [`StatefulStreamProcessor`] trait, achieving "deep symmetry"
between ingestors and automata. This unified interface enables consistent behavior
across all data capture and processing mechanisms.

## Satellite Constellation Architecture

Sinex uses a satellite constellation pattern where independent services communicate via
NATS JetStream. Each satellite implements `StatefulStreamProcessor` with a
unified interface for consistent behavior across all data capture and processing mechanisms.

```text
┌─────────────────────┐     ┌─────────────────────┐     ┌─────────────────────┐
│   External World    │────▶│    Satellites       │────▶│   Data Substrate    │
│                     │     │                     │     │                     │
│ • Files             │     │ • fs-watcher        │     │ • core.events       │
│ • Terminal          │     │ • terminal          │     │ • source_material   │
│ • Desktop           │     │ • desktop           │     │ • knowledge_graph   │
│ • System            │     │ • system            │     │ • checkpoints       │
└─────────────────────┘     └──────────┬──────────┘     └──────────┬──────────┘
│                            │
┌──────────▼──────────┐                 │
│   NATS JetStream   │                 │
│                     │                 │
│ • events.raw.*      │◀────────────────┘  (ingested payloads)
│ • events.confirmations.*               (canonical IDs after persistence)
│ • events.dlq.*                         (automatic dead-letter subjects)
└──────────┬──────────┘
│
┌──────────▼──────────┐
│     Automata        │
│                     │
│ • canonicalizer     │
│ • health-aggregator │
│ • synthesis engines │
└─────────────────────┘
```

Satellites publish provisional events to `events.raw.<source>` with `Nats-Msg-Id` headers for idempotency. After ingestd persists the event, it emits confirmations on `events.confirmations.<event_id>` so automata can wait for canonical IDs, and routes failures to `events.dlq.<source>` for deterministic recovery.

### Satellite Roles

Each satellite can serve one or more roles:
- **Ingestor Role**: Capture external data, create raw events
- **Automaton Role**: Process events, create synthesis events
- **Actuator Role**: Act on instructional events (planned)

### Key Implementation Patterns

#### Event Symmetry (Active Inference)
Same event types serve as both observations and instructions:
```json
// Observation (what happened)
{
"source": "ingestor.hyprland",
"event_type": "desktop.workspace.switched",
"payload": { "workspace_id": 3 }
}

// Instruction (what should happen)
{
"source": "user.cli",
"event_type": "desktop.workspace.switched",
"payload": { "workspace_id": 3 }
}
```

## Three-Phase Startup Pattern

All satellites follow a consistent startup sequence that ensures complete data capture:

### Phase 1: Snapshot
- Captures current state of the external system
- Uses `TimeHorizon::Snapshot` for instantaneous data capture
- Example: Filesystem satellite scans existing files

### Phase 2: Gap-filling (Historical)
- Processes events that occurred while offline
- Uses `TimeHorizon::Historical { end_time }` for bounded scanning
- Only runs if processor supports historical scanning
- Ensures no events are lost during service restarts

### Phase 3: Continuous Processing
- Real-time event monitoring and streaming
- Uses `TimeHorizon::Continuous` for unbounded operation
- Continues indefinitely until shutdown

## Archive and Replace Pattern

The system never loses data but allows evolution of interpretations:
- Original interpretations archived with full audit trail
- New interpretations created with updated logic
- Complete provenance chain maintained via `source_event_ids`

## Checkpoint Management

Satellites share a common checkpoint representation:

```rust
pub struct Checkpoint {
    pub processor_name: String,
    pub position: CheckpointPosition,
    pub metadata: Option<JsonValue>,
}
```

`CheckpointPosition` captures the resume handle (timestamp, cursor, offset, custom
payload). Typical usage:

- **Timestamp-based** – desktop, clipboard, health satellites.
- **Cursor-based** – journald/system satellites.
- **Custom** – filesystem (per-path state), bespoke automata.

Checkpoints are persisted before processing begins, and satellites resume from the
latest successful position after restarts.

## Event Submission & Provenance

Satellites never write directly to Postgres. They submit events or source material
via the SDK, which publishes to NATS JetStream and is ultimately handled by
`sinex-ingestd`:

```rust
let event = build_event(...);
sdk.ingest().submit(event).await?;
```

Provenance is tracked via `source_event_ids` (internal chains) and
`associated_blob_ids` (material attachments). This ensures the processing graph is
recoverable end-to-end.

## Sensor Rules & Enforcement

- Only `sinex-sensd` captures source material. Satellites consume material slices
  via `MaterialConsumer`.
- Satellite crates expose `StatefulStreamProcessor` implementations, not capture
  APIs.
- Helper macros assert the processor role so sensor capabilities cannot leak into
  satellites.

## Implementation Examples

- **Filesystem Satellite**
  - Scanner mode walks directory trees.
  - Sensor mode uses inotify/FSEvents for realtime monitoring.
  - Custom JSON checkpoint per path.
- **Terminal Satellite**
  - Recording, scrollback, and Atuin import modes.
  - Captures full sessions or command history.
- **Desktop Satellite**
  - Clipboard polling, window focus tracking, workspace changes.
  - Timestamp checkpoints for deduplication.
- **System Satellite**
  - Journald cursor tracking, D-Bus monitoring, udev hardware events.

## Configuration Management

The SDK unifies configuration handling:

```toml
[satellite]
name = "filesystem-watcher"
checkpoint_interval_secs = 60

[ingestd]
endpoint = "http://localhost:50051"

[nats]
url = "nats://localhost:4222"

[satellite.custom]
scan_paths = ["/home/user/Documents"]
```

Environment overrides (`SINEX_SATELLITE_NAME`, `SINEX_INGESTD_ENDPOINT`,
`SINEX_NATS_URL`) respect deployment differences.

## Error Handling & Resilience

- Automatic reconnection for NATS and gRPC clients.
- Exponential backoff for transient failures.
- Checkpoint-based recovery ensures replay from the last good position.
- Graceful shutdown: handle SIGTERM/SIGINT, finish in-flight work, persist final
  checkpoint.

## Performance Considerations

- Configurable batching balances latency and throughput for ingest.
- Bounded buffers prevent unbounded memory growth.
- Heartbeats and metrics are emitted periodically to track health.

## Future Enhancements

- Multi-stage processing pipelines and richer event correlation.
- Distributed checkpoint coordination and compaction.
- Runtime configuration updates and feature-flag integration.

# Sinex Satellite SDK

Shared library for building Sinex satellite services (event sources and automata).

This crate provides:
- Common traits and interfaces
- gRPC client for communicating with sinex-ingestd
- NATS JetStream client for message bus communication
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
gRPC and NATS JetStream. Each satellite implements `StatefulStreamProcessor` with a
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
│ • Event streams     │◀────────────────┘
│ • Consumer groups   │
│ • Event filtering   │
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

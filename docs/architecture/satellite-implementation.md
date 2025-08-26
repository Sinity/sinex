# Satellite Implementation Architecture

## Overview

This document describes the implementation architecture for Sinex satellites - the distributed components that capture data from external sources and process events. All satellites follow consistent patterns based on the `StatefulStreamProcessor` trait.

## Core Concepts

### Deep Symmetry

All satellites (both event sources and automata) implement the same `StatefulStreamProcessor` trait, providing:
- Unified lifecycle management
- Consistent checkpoint handling
- Standard event emission patterns
- Common configuration structure

### Three-Phase Startup Pattern

Every satellite follows this startup sequence:

1. **Snapshot Phase**
   - Captures current state of external system
   - Uses `TimeHorizon::Snapshot` 
   - Examples:
     - Filesystem: Scans existing files
     - Terminal: Captures shell history
     - System: Reads current journal entries

2. **Gap-filling Phase** 
   - Processes events missed while offline
   - Uses `TimeHorizon::Historical { end_time }`
   - Only runs if:
     - Processor supports historical scanning
     - Previous checkpoint exists
   - Examples:
     - Hyprland: Replays IPC event log
     - Journal: Reads entries since last cursor

3. **Continuous Phase**
   - Real-time monitoring and streaming
   - Uses `TimeHorizon::Continuous`
   - Runs indefinitely until shutdown
   - Examples:
     - Filesystem: inotify watches
     - Clipboard: Polling for changes

## Checkpoint Management

### Checkpoint Structure
```rust
pub struct Checkpoint {
    pub processor_name: String,
    pub position: CheckpointPosition,
    pub metadata: Option<JsonValue>,
}

pub enum CheckpointPosition {
    Beginning,
    Timestamp(DateTime<Utc>),
    Cursor(String),
    Offset(u64),
    Custom(JsonValue),
}
```

### Position Types by Satellite

- **Timestamp-based**: Desktop, Clipboard, Health
  - Natural for time-series data
  - Easy gap detection

- **Cursor-based**: System (journald), Hyprland
  - Opaque position markers
  - Platform-specific resume points

- **Custom**: Filesystem (per-path state)
  - Complex state tracking
  - Path-specific positions

## Event Creation Patterns

### Direct Database Insertion
Most satellites use direct database insertion for performance:
```rust
let event_id = Ulid::new();
let raw_event = RawEvent {
    event_id,
    source: sources::FILESYSTEM_WATCHER,
    event_type: event_types::FILE_CREATED,
    // ...
};
EventQueries::insert_event(&pool, &raw_event).await?;
```

### Provenance Tracking
Events maintain relationships via `source_event_ids`:
- Scanner events reference discovered items
- Derived events link to originals
- Processing chains preserved

## Implementation Examples

### Filesystem Satellite
- **Scanner Mode**: Walks directory trees, creates file/directory events
- **Sensor Mode**: Uses inotify/FSEvents for real-time monitoring
- **Checkpoint**: Custom JSON tracking per-path scan state

### Terminal Satellite  
- **Recording Mode**: Captures full terminal sessions via script/asciinema
- **Scrollback Mode**: Extracts terminal buffer history
- **Atuin Mode**: Imports command history with rich metadata

### Desktop Satellite
- **Clipboard**: Polls for content changes with deduplication
- **Window Events**: Tracks focus changes and workspace switches
- **Checkpoint**: Timestamp of last processed event

### System Satellite
- **Journal**: Follows systemd journal with cursor-based resume
- **D-Bus**: Monitors system bus for service events (future)
- **Udev**: Tracks hardware changes (future)

## Configuration Management

### Unified Config Structure
```toml
[satellite]
name = "filesystem-watcher"
mode = "unified"
checkpoint_interval_secs = 60

[ingestd]
endpoint = "http://localhost:50051"

[nats]
url = "nats://localhost:4222"

[satellite.custom]
# Satellite-specific settings
scan_paths = ["/home/user/Documents"]
```

### Environment Variables
- `SINEX_SATELLITE_NAME`: Override configured name
- `SINEX_INGESTD_ENDPOINT`: Override ingestd location
- `SINEX_NATS_URL`: Override NATS connection

## Error Handling and Resilience

### Automatic Reconnection
- gRPC clients reconnect on connection loss
- NATS clients handle transient failures
- Exponential backoff for retries

### Checkpoint Recovery
- Checkpoints persisted before processing
- Automatic resume from last known good state
- Manual checkpoint override via CLI

### Graceful Shutdown
- SIGTERM/SIGINT handling
- Finish current batch before exit
- Save final checkpoint

## Performance Considerations

### Batching
- Events batched for database insertion
- Configurable batch sizes and timeouts
- Balance latency vs throughput

### Resource Management
- Connection pooling for database
- Bounded memory usage for buffers
- Rate limiting for high-volume sources

### Monitoring
- Heartbeat events every 30 seconds
- Processing metrics via events
- Health status reporting

## Future Enhancements

### Pipeline Architecture
- Multi-stage processing pipelines
- Intermediate transformation steps
- Complex event correlation

### Advanced Checkpointing
- Distributed checkpoint coordination
- Checkpoint compaction strategies
- Point-in-time recovery

### Dynamic Configuration
- Runtime config updates
- Feature flag integration
- A/B testing support

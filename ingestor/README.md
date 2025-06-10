# Sinex Ingestors

This directory contains the unified event collector for the Sinex system.

## Structure

```
ingestor/
├── shared/               # Shared libraries and utilities
│   ├── src/             # Source code
│   └── tests/           # Unit tests for shared components
└── unified-collector/    # Unified collector for all event sources
    ├── src/             # Source code
    │   ├── main.rs      # Entry point
    │   ├── lib.rs       # Library exports
    │   ├── collector.rs # UnifiedCollector implementation
    │   └── config.rs    # Configuration
    └── tests/           # Integration tests

```

## Architecture

The unified collector uses an event-centric architecture:
1. **Events are primary** - Event types declare which sources produce them
2. **Sources are secondary** - EventSource implementations are just streaming details
3. **Single binary** - One collector manages all enabled event sources
4. **SimpleIngestor pattern** - Uses `IngestorRuntime` for lifecycle management

## Event Sources

Event sources are defined in `crate/sinex-events/`:
- `FilesystemMonitor` - Monitors file system changes
- `KittySocketListener` - Captures terminal commands from Kitty
- `HyprlandIPCMonitor` - Real-time window manager events
- `HyprlandStateSnapshotter` - Periodic state snapshots

## Configuration

The unified collector uses event-centric configuration:
```toml
enabled_events = [
    "file.created",
    "file.modified", 
    "command.executed",
    "window.focused",
    "state.snapshot"
]

[event.files]
watch_patterns = ["~/Documents/**/*"]
ignore_patterns = ["*.tmp"]

[event.state_snapshot]
interval_secs = 300
```

## Testing

Run tests with:
```bash
# All tests
cargo test --workspace

# Unified collector tests
cargo test --package unified-collector

# Event sources tests
cargo test --package sinex-events

# Integration tests
cargo test --test integration ingestor::
```
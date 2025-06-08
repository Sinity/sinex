# Hyprland Ingestor

Captures window manager events from Hyprland via IPC socket connection.

## Features

- Direct socket2 integration for real-time events
- Captures 40+ Hyprland event types
- Configurable event augmentation
- State snapshots for comprehensive tracking
- Multi-instance support

## Usage

```bash
# Run with default config
cargo run --bin hyprland-ingestor

# Dry run (logs events to console)
cargo run --bin hyprland-ingestor -- --dry-run

# Output to file instead of database
cargo run --bin hyprland-ingestor -- --output-file events.json

# Use custom config
cargo run --bin hyprland-ingestor -- --config config/hyprland/production.toml

# Show current configuration
cargo run --bin hyprland-ingestor -- config

# Check database connection
cargo run --bin hyprland-ingestor -- check
```

## Configuration

Configuration uses TOML format. See `config/hyprland/` for examples.

Key settings:
- `window_augmentation` - Level of detail for window events (none/basic/detailed/full)
- `workspace_tracking` - Workspace event detail (events/with_windows/with_state)
- `state_snapshot_interval_secs` - How often to capture full state
- `hyprctl_cache_ms` - Cache duration for hyprctl results
- `ignore_events` - Event types to skip

## Events Captured

### Window Events
- `activewindow`/`activewindowv2` - Focus changes
- `openwindow`/`closewindow` - Window lifecycle
- `movewindow`/`movewindowv2` - Window movement
- `windowtitle`/`windowtitlev2` - Title changes
- `fullscreen`, `changefloatingmode`, `urgent`, `minimized`, `pin`

### Workspace Events
- `workspace`/`workspacev2` - Workspace switches
- `createworkspace`/`destroyworkspace` - Workspace lifecycle
- `moveworkspace`/`renameworkspace` - Workspace changes

### Monitor Events
- `focusedmon`/`monitoradded`/`monitorremoved`

### System Events
- `configreloaded` - Configuration changes
- `activelayout` - Keyboard layout changes
- `state_snapshot` - Periodic full state capture

## Event Augmentation

The ingestor can enrich raw events with additional context:

**Basic**: Adds window details to focus events
```json
{
  "event_type": "activewindow",
  "payload": {
    "window_class": "firefox",
    "window_title": "GitHub",
    "window_details": {
      "address": "0x1234567",
      "pid": 12345,
      "workspace": {"id": 1, "name": "1"},
      "at": [100, 100],
      "size": [1920, 1080]
    }
  }
}
```

**Detailed**: Includes context on window open/close
**Full**: Captures focus history and neighboring windows

## Requirements

- Hyprland window manager
- `HYPRLAND_INSTANCE_SIGNATURE` environment variable
- `hyprctl` command available

## Architecture

Uses the SimpleIngestor pattern - the ingestor captures events while IngestorRuntime handles:
- Heartbeats
- Error recovery and retries
- Dead letter queue
- Graceful shutdown
- Automatic reconnection
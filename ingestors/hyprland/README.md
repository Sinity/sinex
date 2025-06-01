# Hyprland Event Ingestor

A comprehensive event capture system for Hyprland window manager using direct IPC socket2 connection.

## Features

### Direct Socket2 Integration

- Connects directly to `$XDG_RUNTIME_DIR/hypr/$HYPRLAND_INSTANCE_SIGNATURE/.socket2.sock`
- Captures ALL 40+ Hyprland event types in real-time
- No buffering - events inserted immediately as they arrive
- Automatic reconnection on socket failures

### Comprehensive Event Capture

**Workspace Events:**
- `workspace`/`workspacev2` - Workspace changes
- `createworkspace`/`createworkspacev2` - Workspace creation
- `destroyworkspace`/`destroyworkspacev2` - Workspace destruction  
- `moveworkspace`/`moveworkspacev2` - Workspace moved between monitors
- `renameworkspace` - Workspace renamed
- `activespecial`/`activespecialv2` - Special workspace changes

**Window Events:**
- `activewindow`/`activewindowv2` - Window focus changes (augmented)
- `openwindow`/`closewindow` - Window lifecycle
- `movewindow`/`movewindowv2` - Window moved between workspaces
- `windowtitle`/`windowtitlev2` - Window title changes
- `fullscreen` - Fullscreen state changes
- `changefloatingmode` - Floating mode changes
- `urgent`/`minimized`/`pin` - Window state changes

**Monitor Events:**
- `focusedmon`/`focusedmonv2` - Monitor focus changes
- `monitoradded`/`monitoraddedv2` - Monitor connected
- `monitorremoved`/`monitorremovedv2` - Monitor disconnected

**System Events:**
- `activelayout` - Keyboard layout changes
- `submap` - Keybind submap changes
- `configreloaded` - Configuration reloaded
- `screencast`/`bell` - System events

### Intelligent Event Augmentation

Four levels of window augmentation:

1. **None** - Raw socket events only
2. **Basic** - Augment active window changes with full window details
3. **Detailed** - Also capture context on window open/close
4. **Full** - Augment all window events, capture focus history and neighbor windows

Three levels of workspace tracking:

1. **Events** - Just workspace change events
2. **WithWindows** - Include window summaries when workspace changes
3. **WithState** - Full workspace state including geometries

### State Snapshots

Periodic comprehensive snapshots include:
- All Hyprland instances
- Version and monitor configuration
- All workspaces and windows
- Input devices and layer clients
- System state (locked, cursor, errors)
- Descriptions (configurable frequency)

### Performance Features

- **Caching**: Configurable hyprctl result caching (default 100ms)
- **Parallel Processing**: Augment events in parallel vs sequential
- **Event Filtering**: Ignore specific event types to reduce load
- **Batch Snapshots**: Efficient batch hyprctl commands
- **Focus History**: Configurable depth (default 3 previous windows)

## Configuration

### Basic Configuration

```toml
[database]
url = "postgresql://localhost/sinex"

[hyprland]
# Basic window focus tracking
window_augmentation = "basic"
# Just workspace events
workspace_tracking = "events"
```

### Detailed Configuration

```toml
[hyprland]
# State snapshots every 30 minutes
state_snapshot_interval_secs = 1800

# Capture descriptions every 4 hours  
descriptions_interval_hours = 4

# Capture rolling log on config reload
rolling_log_on_reload = true

# Detailed window tracking
window_augmentation = "detailed"

# Workspace changes with window lists
workspace_tracking = "with_windows"

# Performance tuning
hyprctl_cache_ms = 100
parallel_augmentation = true

# Focus history
track_focus_history = true
focus_history_depth = 3

# Event filtering
ignore_events = ["screencast", "bell"]
```

See `config-examples/` for complete configuration examples:
- `minimal.toml` - Basic setup
- `detailed.toml` - Balanced detail/performance  
- `comprehensive.toml` - Maximum data capture
- `performance.toml` - Minimal overhead

## Usage

```bash
# Run with default configuration
hyprland-ingestor run

# Use specific config file
hyprland-ingestor -c config.toml run

# Check connections
hyprland-ingestor check

# Show current configuration  
hyprland-ingestor config

# Generate example config
hyprland-ingestor generate-config -o config.toml
```

## Database Schema

Events are stored using the Phase 2 schema with rich payloads:

```sql
-- Example window focus event
{
  "id": "01HXYZ123...",           -- ULID
  "source": "hyprland", 
  "event_type": "activewindow",
  "ts_orig": "2024-01-01T12:00:00Z",
  "host": "desktop",
  "payload": {
    "window_class": "kitty",
    "window_title": "nvim",
    "window_details": {           -- Augmented data
      "address": "0x1234567",
      "pid": 12345,
      "workspace": {"id": 1, "name": "1"},
      "at": [100, 100],
      "size": [800, 600],
      // ... full hyprctl activewindow output
    }
  }
}

-- Example state snapshot
{
  "source": "hyprland",
  "event_type": "state_snapshot", 
  "payload": {
    "snapshots": [{
      "instance_info": {
        "instance": "...",
        "pid": 12345
      },
      "state": {
        "version": {...},
        "monitors": [...],
        "workspaces": [...],
        "clients": [...],
        // ... complete Hyprland state
      }
    }],
    "includes_descriptions": true
  }
}
```

## Multi-Instance Support

- Automatically detects multiple Hyprland instances
- Uses `hyprctl -i N` for instance-specific commands
- Each snapshot includes instance identification
- Handles instance failures gracefully

## Error Handling & Reliability

- **Automatic Reconnection**: Reconnects to socket2 on failures
- **Retry Logic**: Configurable retry attempts for database operations
- **Dead Letter Queue**: Failed events written to DLQ files
- **Graceful Degradation**: Event processing continues on augmentation failures
- **Health Monitoring**: Regular heartbeats and agent status events

## Performance Characteristics

### Typical Load (Basic Configuration)
- ~50-200 events/minute during normal usage
- <1% CPU usage
- <50MB memory
- Minimal disk I/O

### High Detail Configuration  
- ~200-500 events/minute with full augmentation
- 1-3% CPU usage
- <100MB memory
- Database writes are the primary bottleneck

### Optimization Tips

1. **Reduce Augmentation**: Use `window_augmentation = "none"` for minimal overhead
2. **Filter Events**: Add noisy events to `ignore_events` list
3. **Increase Cache**: Set `hyprctl_cache_ms = 500` for less frequent calls
4. **Reduce Snapshots**: Increase `state_snapshot_interval_secs`
5. **Disable Features**: Set `track_focus_history = false`

## Requirements

- Hyprland window manager running
- `HYPRLAND_INSTANCE_SIGNATURE` environment variable set
- PostgreSQL database with Phase 2 schema
- `hyprctl` command available in PATH

## Development

The implementation uses:
- `tokio::net::UnixStream` for async socket handling
- Event parsing for all Hyprland IPC event types
- Configurable caching and augmentation strategies
- Phase 2 database schema with ULIDs and structured events
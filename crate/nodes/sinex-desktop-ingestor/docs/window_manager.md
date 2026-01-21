# Window Manager Module

Window manager watcher with Stage-as-you-go source material capture

Monitors window manager events (focus, open, close, move) and captures them as source
material for later event creation with proper provenance tracking.

## Supported Window Managers

**Currently Supported:**
- Hyprland (via IPC socket)

**Not Yet Supported:**
- Sway/i3 (would require i3 IPC protocol)
- GNOME (would require D-Bus org.gnome.Shell interface)
- KDE Plasma (would require KWin D-Bus interface)
- X11 window managers (would require EWMH/X11 protocol)

**Note:** This module is currently limited to Hyprland users. Support for other window
managers is planned but not yet implemented. The node will fail to start if Hyprland
is not running.

## Architecture

This module follows the Stage-as-you-go pattern:
1. **Source Material Capture**: Window manager events → raw.source_material_registry
2. **Temporal Ledger**: Precise timing → raw.temporal_ledger
3. **Event Generation**: Material processing → events with Provenance::Material

## Hyprland Integration

- Real-time IPC via socket2 for event stream
- State augmentation via hyprctl queries
- Automatic reconnection with exponential backoff
- Comprehensive window and workspace metadata capture

## Configuration Constants

The following hardcoded values control behavior:

- `HYPRLAND_INITIAL_BACKOFF_MS`: 500ms - Initial reconnection delay
- `HYPRLAND_MAX_BACKOFF`: 60s - Maximum reconnection delay
- `WINDOW_STATE_TTL`: 48 hours - How long to keep window state in memory
- `HYPRLAND_SOCKET_READ_TIMEOUT`: 30s - Timeout for socket read operations
- `STATE_SNAPSHOT_INTERVAL`: 300s (5 minutes) - How often to capture full state

These values are not currently configurable but may become so in future versions.

# Window Manager Module

Window manager watcher with Stage-as-you-go source material capture

Monitors window manager events (focus, open, close, move) and captures them as source
material for later event creation with proper provenance tracking.

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

# Desktop node Overview

Desktop node with Stage-as-you-go Source Material Capture

Coordinates multiple desktop event sources using the Stage-as-you-go pipeline:
- Clipboard events (copy/cut/paste) → source material first
- Window manager events (Hyprland focus, movement, workspaces) → source material first

## Architecture

This node uses the Stage-as-you-go pattern for ALL desktop data:
1. **Source Material Capture**: Desktop activity → raw.source_material_registry
2. **Temporal Ledger**: Precise timing → raw.temporal_ledger
3. **Event Generation**: Material processing → events with Provenance::Material

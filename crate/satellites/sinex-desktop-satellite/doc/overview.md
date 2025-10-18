# Desktop Satellite Overview

Desktop Satellite with sensd Source Material Capture

Coordinates multiple desktop event sources using the sensd pattern:
- Clipboard events (copy/cut/paste) → source material first
- Window manager events (Hyprland focus, movement, workspaces) → source material first

## Architecture

This satellite uses the sensd pattern for ALL desktop data:
1. **Source Material Capture**: Desktop activity → raw.source_material_registry
2. **Temporal Ledger**: Precise timing → raw.temporal_ledger
3. **Event Generation**: Material processing → events with Provenance::Material

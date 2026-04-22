# Desktop node Overview

Desktop node with Stage-as-you-go Source Material Capture

Coordinates multiple desktop event sources using the Stage-as-you-go pipeline:
- Clipboard events (copy/cut/paste) → source material first
- Window manager events (Hyprland focus, movement, workspaces) → source material first
- `ActivityWatch` historical rows → source material first

## Architecture

This node uses the Stage-as-you-go pattern for ALL desktop data:
1. **Source Material Capture**: Desktop activity → `raw.source_material_registry`
2. **Temporal Ledger**: Precise timing → `raw.temporal_ledger`
3. **Event Generation**: Material processing → events with `Provenance::Material`

`ActivityWatch` historical proof is covered by
`scan_historical_persists_activitywatch_through_node_runtime`, which exercises
the `SQLite` source through `NodeRunner`, NATS, `sinex-ingestd`, and persisted
`core.events` material provenance.

# Desktop sensd Integration

Desktop sensd integration module

This module provides integration between desktop satellite and sensd for
source material capture and event generation with proper provenance.

## Architecture

Following the fs-watcher pattern:
1. **Source Material Capture**: Desktop data → raw.source_material_registry
2. **Temporal Ledger**: Precise timing → raw.temporal_ledger  
3. **Event Generation**: Material processing → events with Provenance::Material

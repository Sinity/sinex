# Stage-as-you-go Pattern

Stage-as-You-Go pattern implementation for real-time provenance tracking

This module provides helpers for implementing the Stage-as-You-Go pattern where
source material is registered in-flight as events are being created, enabling
real-time provenance tracking without waiting for full ingestion completion.

This critical architectural pattern ensures zero provenance gaps for real-time streams.
It solves the fundamental problem of maintaining data lineage when events are being
processed and emitted before the complete source material is available.

## The Problem

Traditional approaches face a dilemma:
- **Option 1**: Wait for complete ingestion before emitting events (high latency)
- **Option 2**: Emit events immediately without provenance (broken lineage)

## The Solution

Stage-as-You-Go allows immediate event emission with full provenance by:

```rust
// 1. Create in-flight source material record on startup
let blob_id = source_material_registry.create_in_flight().await?;

// 2. Emit events immediately with provenance
let event = Event {
source_material_id: Some(blob_id),
// ... events flow in real-time
};

// 3. Periodically finalize chunks (e.g., every 5 minutes)
source_material_registry.finalize_chunk(blob_id).await?;
```

## Key Benefits

- **Real-time Processing**: No delay for event emission
- **Complete Provenance**: Every event linked to its source
- **Incremental Updates**: Source material details filled in as available
- **Crash Recovery**: In-flight records can be resumed or finalized

## Implementation Pattern

1. **Register In-Flight**: Create an in-flight source material record with initial metadata
2. **Process & Emit**: Process data and emit events with source_material_id
3. **Finalize**: Update source material with complete details (size, checksum, etc.)

## Example Use Cases

- **Log Tailing**: Emit log events as lines arrive, finalize after rotation
- **Terminal Sessions**: Track commands immediately, finalize on session end
- **Network Streams**: Process packets in real-time, finalize on connection close

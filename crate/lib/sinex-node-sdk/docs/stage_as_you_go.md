# Stage-as-you-go Pattern

This module provides helpers for implementing the Stage-as-You-Go pattern where source material is registered in-flight as events are being created, enabling real-time provenance tracking without waiting for full ingestion completion.

This pattern ensures zero provenance gaps for real-time streams by maintaining data lineage even when events are processed before the complete source material is available.

## Implementation Pattern

1.  **Register In-Flight**: Create an in-flight source material record with initial metadata to get a stable ID.
2.  **Process & Emit**: Process data and emit events referencing this `source_material_id`.
3.  **Finalize**: Update the source material record with complete details (size, hashes) once the stream ends.

For append-style sources, prefer `AppendStreamAcquirer` instead of hand-managed
begin/append/finalize loops. When producers emit many small logical records,
wrap it in `BufferedAppendStreamWriter`: callers still receive exact byte
anchors for each record, while the SDK owns batching, rotation, and finalization.

## Design Principles

### 1. Per-Material Isolation
Fine-grained locking (via material ID) ensures that concurrent operations on different materials proceed without global contention.

### 2. Resource Management
The system employs concurrency limits (via semaphores) to ensure predictable resource usage during high-volume assembly operations.

High-cardinality metadata observations should be represented as records in a
bounded append stream, not as fresh zero-byte source materials. This preserves
material provenance while keeping lifecycle-frame count and fsync pressure tied
to stream batches instead of event cardinality.

### 3. State Reconciliation
A background task periodically cleans up staging resources associated with completed or abandoned operations based on activity timestamps.

## Key Benefits

- **Real-time Processing**: No delay for event emission.
- **Complete Provenance**: Every event linked to its source from the start.
- **Incremental Updates**: Source material details filled in as available.

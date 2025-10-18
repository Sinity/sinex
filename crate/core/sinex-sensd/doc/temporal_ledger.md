# Temporal Ledger

`temporal_ledger.rs` tracks checkpoints for each sensor stream so restarts can
resume from the last processed item without duplication.

- Persists ledger entries via `sinex-core::db` helpers.
- Supports pruning and compaction routines.
- Coordinates tightly with checkpoint ingestion in `sinex-satellite-sdk`.

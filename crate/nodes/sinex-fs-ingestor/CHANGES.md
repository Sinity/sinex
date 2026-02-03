# Changes to sinex-fs-ingestor (LOW Priority Fixes)

## File Modified
- `crate/nodes/sinex-fs-ingestor/src/unified_processor.rs`

## New Types Added

### EventMetrics (Lines 139-187)
```rust
struct EventMetrics {
    events_processed: AtomicU64,
    events_created: AtomicU64,
    events_modified: AtomicU64,
    events_deleted: AtomicU64,
    events_moved: AtomicU64,
    processing_errors: AtomicU64,
}

impl EventMetrics {
    fn new() -> Arc<Self>;
    fn record_created(&self);
    fn record_modified(&self);
    fn record_deleted(&self);
    fn record_moved(&self);
    fn record_error(&self);
}
```

## Modified Structures

### WatchContext (Line 196)
Added field:
```rust
metrics: Arc<EventMetrics>,
```

### FilesystemProcessor (Line 206)
Added field:
```rust
metrics: Arc<EventMetrics>,
```

## Modified Functions

### FilesystemProcessor::new() (Line 218)
Added initialization:
```rust
metrics: EventMetrics::new(),
```

### FilesystemProcessor::with_config() (Line 230)
Added initialization:
```rust
metrics: EventMetrics::new(),
```

### FilesystemProcessor::build_watch_contexts() (Line 324)
Added metrics to WatchContext:
```rust
metrics: Arc::clone(&self.metrics),
```

### FilesystemProcessor::spawn_watchers() (Lines 285-320)
**Issue 86 Fix**: Added retry logic with exponential backoff
- Maximum 5 initialization attempts
- Delays: 1s, 2s, 4s, 8s, 16s
- Structured logging for retry operations

### FilesystemProcessor::get_source_state() (Lines 562-597)
**Issue 24 Fix**: Added metrics exposure
```rust
"events_processed" → events_processed counter
"events_created" → events_created counter
"events_modified" → events_modified counter
"events_deleted" → events_deleted counter
"events_moved" → events_moved counter
"processing_errors" → processing_errors counter
```

### watch_path() (Line 647)
Added error counting:
```rust
ctx.metrics.record_error();
```

### handle_file_created() (Line 742)
Added success tracking:
```rust
ctx.metrics.record_created();
```

### handle_file_modified() (Line 793)
Added success tracking:
```rust
ctx.metrics.record_modified();
```

### handle_file_deleted() (Line 817)
Added success tracking:
```rust
ctx.metrics.record_deleted();
```

### handle_file_moved() (Line 846)
Added success tracking:
```rust
ctx.metrics.record_moved();
```

### capture_material_from_file_inner() (Lines 971-976)
**Issue 92 Documentation**: Added comment explaining TOCTOU elimination:
```rust
// Issue 92: TOCTOU race eliminated by opening file first, then getting metadata
// from the open handle. This ensures atomic operations:
// 1. File is opened and locked by OS
// 2. Metadata retrieved from open file descriptor (no path lookup)
// 3. Size checked before any read
// 4. Cumulative tracking during streaming prevents growing file issues
```

### Test: handle_file_created_emits_event() (Line 1156)
Added metrics to test WatchContext:
```rust
metrics: EventMetrics::new(),
```

## Line Count Changes
- Lines added: ~150
- Lines modified: ~20
- Net increase: ~150 lines

## Breaking Changes
None - all changes are additive or internal

## API Surface Changes
- `ExplorationProvider::get_source_state()` now exposes 6 additional metrics
- All other APIs unchanged

## Configuration Changes
None - no new configuration parameters required

## Dependencies Added
None - uses existing `std::sync::atomic::AtomicU64`

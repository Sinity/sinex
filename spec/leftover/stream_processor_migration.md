# Stream Processor Migration Guide

## Note
The key architectural concepts from this migration guide have been extracted to:
- `/crate/sinex-satellite-sdk/src/stream_processor.rs` - Architecture overview and checkpoint types
- `/crate/sinex-satellite-sdk/examples/README.md` - Implementation patterns

This document contains the detailed migration steps for converting old EventSource/Automaton code to the new architecture.

## Migration Steps

### 1. Replace EventSource with StatefulStreamProcessor

**Before (EventSource):**
```rust
#[async_trait]
impl EventSource for MySource {
    async fn initialize(&mut self, ctx: EventSourceContext) -> SatelliteResult<()> { ... }
    async fn start_streaming(&mut self) -> SatelliteResult<()> { ... }
    async fn run_scanner(&mut self, args: ScannerArgs) -> SatelliteResult<ScanReport> { ... }
    fn source_name(&self) -> &str { "my-source" }
    fn supports_scanner(&self) -> bool { true }
}
```

**After (StatefulStreamProcessor):**
```rust
#[async_trait]
impl StatefulStreamProcessor for MyProcessor {
    async fn initialize(&mut self, ctx: StreamProcessorContext) -> SatelliteResult<()> { ... }
    
    async fn scan(
        &mut self,
        from: Checkpoint,
        until: TimeHorizon,
        args: ScanArgs,
    ) -> SatelliteResult<ScanReport> {
        match until {
            TimeHorizon::Snapshot => { /* take current state snapshot */ }
            TimeHorizon::Historical { end_time } => { /* scan from checkpoint to end_time */ }
            TimeHorizon::Continuous => { /* start continuous monitoring */ }
        }
    }
    
    fn processor_name(&self) -> &str { "my-processor" }
    fn processor_type(&self) -> ProcessorType { ProcessorType::Ingestor }
    fn capabilities(&self) -> ProcessorCapabilities { ... }
    async fn current_checkpoint(&self) -> SatelliteResult<Checkpoint> { ... }
}
```

### 2. Update Checkpoint Management

**Before (Legacy):**
```rust
CheckpointState {
    last_processed_id: Some("stream-id-123".to_string()),
    processed_count: 42,
    // ...
}
```

**After (Unified):**
```rust
CheckpointState {
    checkpoint: Checkpoint::stream("stream-id-123", Some(event_ulid)),
    processed_count: 42,
    // ...
}

// Or for ingestors:
CheckpointState {
    checkpoint: Checkpoint::external(
        serde_json::json!({"file_offset": 1024}),
        "file.log:1024"
    ),
    // ...
}
```

### 3. Implement Exploration Provider

Add diagnostic capabilities for the explore subcommand:

```rust
impl ExplorationProvider for MyProcessor {
    fn get_source_state(&self) -> Result<SourceState, Box<dyn std::error::Error>> {
        Ok(SourceState {
            description: "Current processor state".to_string(),
            last_updated: Utc::now(),
            total_items: Some(self.count_items()?),
            metadata: HashMap::new(),
            healthy: true,
            recent_activity: vec![],
        })
    }
    
    fn get_coverage_analysis(&self, time_range: Option<(DateTime<Utc>, DateTime<Utc>)>) 
        -> Result<CoverageAnalysis, Box<dyn std::error::Error>> {
        // Compare source data with Sinex events
        // Return coverage percentage and missing items
    }
    
    // ... other methods
}
```

### 4. Update CLI Structure

**Before (manual CLI):**
```rust
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = MyArgs::parse();
    let mut runner = EventSourceRunner::new(MySource::new(), ingest_client);
    runner.run().await
}
```

**After (unified CLI):**
```rust
use sinex_satellite_sdk::processor_main;

processor_main!(MyProcessor);
```

### 5. Service Mode Integration

The unified processor automatically handles the startup sequence:

1. **Snapshot Phase**: Capture current state (if supported)
2. **Gap-Fill Phase**: Process missed data since last checkpoint
3. **Continuous Phase**: Start real-time processing

## Time Horizon Examples

### Snapshot Mode
```bash
# Take current state snapshot
my-processor scan --until snapshot

# CLI usage
my-processor explore --source-state
```

### Historical Mode
```bash
# Process data from checkpoint to specific time
my-processor scan --from "2024-01-01T00:00:00Z" --until "2024-01-02T00:00:00Z"

# Coverage analysis
my-processor explore --coverage-analysis
```

### Continuous Mode
```bash
# Start continuous processing (service mode)
my-processor service

# Or explicit continuous scan
my-processor scan --until continuous
```

## Checkpoint Types

### External Checkpoints (Ingestors)
```rust
// File position
Checkpoint::external(
    serde_json::json!({"path": "/var/log/app.log", "offset": 1024}),
    "app.log:1024"
)

// Database cursor
Checkpoint::external(
    serde_json::json!({"table": "events", "last_id": 12345}),
    "events:12345"
)

// Timestamp-based
Checkpoint::timestamp(
    DateTime::parse_from_rfc3339("2024-01-01T12:00:00Z")?.into(),
    Some(serde_json::json!({"source": "api"}))
)
```

### Internal Checkpoints (Automata)
```rust
// Event-based
Checkpoint::internal(event_ulid, message_count)

// Stream-based
Checkpoint::stream("1641024000000-0", Some(event_ulid))
```

## Migration Checklist

- [ ] Replace `EventSource` trait with `StatefulStreamProcessor`
- [ ] Implement unified `scan()` method handling all TimeHorizon cases
- [ ] Update checkpoint management to use unified `Checkpoint` enum
- [ ] Add `ExplorationProvider` implementation for diagnostics
- [ ] Update CLI to use standardized subcommands
- [ ] Test startup sequence (Snapshot → Gap-Fill → Continuous)
- [ ] Add processor-specific configuration schema
- [ ] Update documentation and examples

## Common Patterns

### File-based Ingestor
```rust
async fn scan(&mut self, from: Checkpoint, until: TimeHorizon, args: ScanArgs) -> SatelliteResult<ScanReport> {
    let start_offset = match from {
        Checkpoint::External { position, .. } => {
            position.get("offset").and_then(|v| v.as_u64()).unwrap_or(0)
        }
        Checkpoint::None => 0,
        _ => return Err(SatelliteError::Checkpoint("Invalid checkpoint type".to_string())),
    };
    
    match until {
        TimeHorizon::Snapshot => self.scan_file_current(start_offset).await,
        TimeHorizon::Historical { end_time } => self.scan_file_historical(start_offset, end_time).await,
        TimeHorizon::Continuous => self.scan_file_continuous(start_offset).await,
    }
}
```

### Event-based Automaton
```rust
async fn scan(&mut self, from: Checkpoint, until: TimeHorizon, args: ScanArgs) -> SatelliteResult<ScanReport> {
    let start_event_id = match from {
        Checkpoint::Internal { event_id, .. } => Some(event_id),
        Checkpoint::Stream { event_id, .. } => event_id,
        Checkpoint::None => None,
        _ => return Err(SatelliteError::Checkpoint("Invalid checkpoint type".to_string())),
    };
    
    match until {
        TimeHorizon::Historical { end_time } => {
            self.process_events_historical(start_event_id, end_time).await
        }
        TimeHorizon::Continuous => {
            self.process_events_continuous(start_event_id).await
        }
        TimeHorizon::Snapshot => {
            Err(SatelliteError::Automaton("Automata don't support snapshot mode".to_string()))
        }
    }
}
```

## Benefits

1. **Unified Architecture**: Single interface eliminates artificial distinctions
2. **Flexible Checkpointing**: Supports any checkpoint format
3. **Intelligent Startup**: Automatic gap-filling and state recovery
4. **Rich Diagnostics**: Built-in exploration and coverage analysis
5. **Standardized CLI**: Consistent interface across all processors
6. **Type Safety**: Proper processor type distinction at runtime

This migration enables the "Deep Symmetry" vision where sensing is just continuous scanning, and both ingestors and automata share the same fundamental patterns.
# Satellite Development Guide

This guide provides best practices and patterns for developing new Sinex satellites (event sources and automata).

## Architecture Overview

Satellites implement the `StatefulStreamProcessor` trait from `sinex-satellite-sdk`, providing:
- Unified interface for all data capture and processing
- State persistence and checkpointing
- Historical replay capabilities
- Graceful shutdown handling
- Three-phase startup pattern

## Development Workflow

### 1. Define Your Satellite Type

```rust
use sinex_satellite_sdk::{StatefulStreamProcessor, ProcessorConfig, TimeHorizon};

pub struct MySatellite {
    config: ProcessorConfig,
    state: MyState,
    // Add your fields
}

impl StatefulStreamProcessor for MySatellite {
    type Config = MyConfig;
    type State = MyState;
    type Error = MyError;
    
    // Implement required methods
}
```

### 2. Configuration Management

```rust
#[derive(Debug, Clone, Deserialize)]
pub struct MyConfig {
    #[serde(default = "default_scan_interval")]
    pub scan_interval: Duration,
    
    #[serde(default)]
    pub filters: Vec<PathPattern>,
    
    // Add configuration fields
}

fn default_scan_interval() -> Duration {
    Duration::from_secs(60)
}
```

### 3. State Management

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MyState {
    pub last_scan_time: Option<DateTime<Utc>>,
    pub processed_items: HashSet<String>,
    // Track your state
}

impl Default for MyState {
    fn default() -> Self {
        Self {
            last_scan_time: None,
            processed_items: HashSet::new(),
        }
    }
}
```

### 4. Main Processing Loop

```rust
async fn process_stream(
    &mut self,
    time_horizon: TimeHorizon,
) -> Result<ProcessingStats, Self::Error> {
    match time_horizon {
        TimeHorizon::Snapshot => {
            // Capture current state
            self.capture_snapshot().await
        }
        TimeHorizon::Historical { end_time } => {
            // Process historical data
            self.process_historical(end_time).await
        }
        TimeHorizon::Continuous => {
            // Real-time monitoring
            self.monitor_realtime().await
        }
    }
}
```

## Best Practices

### Error Handling

```rust
use anyhow::{Context, Result};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum MySatelliteError {
    #[error("Configuration error: {0}")]
    Config(String),
    
    #[error("Processing failed: {0}")]
    Processing(#[from] anyhow::Error),
    
    #[error("State persistence error: {0}")]
    State(#[from] std::io::Error),
}

// Use context for rich error messages
async fn process_item(&self, item: &Item) -> Result<()> {
    self.validate_item(item)
        .context("Failed to validate item")?;
    
    self.submit_event(item)
        .await
        .context("Failed to submit event")?;
    
    Ok(())
}
```

### Logging and Observability

```rust
use tracing::{debug, info, warn, error, instrument};

#[instrument(skip(self))]
async fn process_stream(&mut self, horizon: TimeHorizon) -> Result<Stats> {
    info!(?horizon, "Starting stream processing");
    
    let start = Instant::now();
    let stats = self.do_process(horizon).await?;
    
    info!(
        duration = ?start.elapsed(),
        events = stats.events_processed,
        "Stream processing completed"
    );
    
    Ok(stats)
}
```

### Performance Optimization

1. **Batch Processing**: Submit events in batches rather than individually
2. **Concurrent Scanning**: Use `tokio::task::spawn` for parallel processing
3. **Efficient State Storage**: Only persist changed state fields
4. **Resource Limits**: Implement backpressure and rate limiting

```rust
// Batch event submission
let mut batch = Vec::with_capacity(BATCH_SIZE);
for item in items {
    let event = self.create_event(item)?;
    batch.push(event);
    
    if batch.len() >= BATCH_SIZE {
        self.submit_batch(&batch).await?;
        batch.clear();
    }
}

// Submit remaining
if !batch.is_empty() {
    self.submit_batch(&batch).await?;
}
```

### Testing Strategy

1. **Unit Tests**: Test individual components
2. **Integration Tests**: Use test utilities from `sinex-test-utils`
3. **Property Tests**: Use `proptest` for edge cases
4. **System Tests**: Full end-to-end validation

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use sinex_test_utils::{TestHarness, MockIngestClient};
    
    #[tokio::test]
    async fn test_snapshot_processing() {
        let harness = TestHarness::new().await;
        let mut satellite = MySatellite::new(test_config());
        
        let stats = satellite
            .process_stream(TimeHorizon::Snapshot)
            .await
            .unwrap();
        
        assert!(stats.events_processed > 0);
        assert_eq!(stats.errors, 0);
    }
}
```

## Common Patterns

### Archive and Replace
For data that changes over time, use the archive-and-replace pattern:
1. Mark old events as superseded
2. Create new events with updated data
3. Maintain provenance chain

### Idempotent Processing
Ensure operations can be safely retried:
- Use deterministic event IDs
- Check for duplicates before processing
- Make state updates atomic

### Graceful Degradation
Handle partial failures gracefully:
- Continue processing other items on individual failures
- Track and report errors without stopping
- Implement retry logic with exponential backoff

## Deployment

### NixOS Module Integration
See [NixOS Module Documentation](../../nixos/modules/README.md) for service configuration patterns.

### Resource Requirements
- Memory: 256-512MB typical
- CPU: Single core sufficient for most satellites
- Disk: Minimal (state checkpoints)
- Network: Low bandwidth, bursty

### Monitoring
- Export Prometheus metrics
- Emit structured logs
- Implement health check endpoints
- Track processing statistics

## References

- [Satellite SDK Documentation](../../crate/sinex-satellite-sdk/src/lib.rs)
- [StatefulStreamProcessor Trait](../../crate/sinex-satellite-sdk/src/stage_as_you_go.rs)
- [Example Satellites](../../crate/)
- [Testing Utilities](../../crate/sinex-test-utils/)
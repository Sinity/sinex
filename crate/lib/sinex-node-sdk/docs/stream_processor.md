# Stream Processor Architecture

Unified Stream Processor Architecture for Sinex

This module implements the "Deep Symmetry" vision from Part 16 of the design discussion,
unifying ingestors and automata as both being "Stateful Stream Processors" with a single
scan(from: Checkpoint, until: TimeHorizon) interface.

## Architecture Overview

The unified architecture eliminates the artificial distinction between ingestors and automata:

- **Single Interface**: Both implement `StatefulStreamProcessor`
- **Unified Checkpoints**: Support external positions (files, APIs) and internal event IDs
- **Time Horizons**: Three modes replace sensor/scanner split:
  - `Snapshot`: Capture current state
  - `Historical`: Process bounded time range
  - `Continuous`: Real-time streaming
- **Startup Sequence**: Automatic Snapshot → Gap-Fill → Continuous progression
- **CLI Structure**: Standardized service/scan/explore subcommands

## Checkpoint Types

### External Checkpoints (Ingestors)
```rust
// File position
Checkpoint::external(
json!({"path": "/var/log/app.log", "offset": 1024}),
"app.log:1024"
)
```

### Internal Checkpoints (Automata)
```rust
// Event-based
Checkpoint::internal(event_ulid, message_count)
```

## Implementing New nodes

To implement a new node service using this SDK:

### 1. Implement StatefulStreamProcessor

```rust,ignore
use sinex_node_sdk::prelude::*;

#[derive(Debug)]
pub struct MyProcessor {
checkpoint_manager: CheckpointManager,
work_tracker: Arc<RwLock<WorkTracker>>,
}

#[async_trait]
impl StatefulStreamProcessor for MyProcessor {
async fn scan(
&self,
from: Checkpoint,
horizon: TimeHorizon,
event_sender: EventSender,
) -> nodeResult<ScanReport> {
// Track work for graceful shutdown
let tracker = self.work_tracker.read().await;
tracker.start_operation();

// Process events based on horizon - implementation specific
let result = match horizon {
TimeHorizon::Snapshot => self.scan_current_state(&event_sender).await,
TimeHorizon::Historical { end_time } => {
self.scan_historical(from, end_time, &event_sender).await
}
TimeHorizon::Continuous => {
self.scan_continuous(from, &event_sender).await
}
};

tracker.finish_operation();
result
}

fn processor_type(&self) -> ProcessorType {
ProcessorType::Ingestor // or ProcessorType::Automaton
}

fn capabilities(&self) -> ProcessorCapabilities {
ProcessorCapabilities {
supports_snapshot: true,
supports_historical: true,
supports_continuous: true,
supports_replay: false,
}
}
}
```

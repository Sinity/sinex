# Automaton Implementation Analysis: Current vs Target Architecture

## Executive Summary

This analysis examines the current automaton implementations in Sinex and their relationship to the planned StatefulStreamProcessor architecture. The analysis reveals a dual architecture in transition:

1. **Current HotlogAutomaton System**: Event-driven Redis stream consumption with `process_event()` method
2. **Target StatefulStreamProcessor System**: Time-driven unified interface with `scan()` method for both ingestors and automata

## Current Automaton Architecture

### 1. HotlogAutomaton Trait (Current System)

**Location**: `crate/sinex-satellite-sdk/src/automaton.rs`

The current automaton system is built around the `HotlogAutomaton` trait:

```rust
#[async_trait]
pub trait HotlogAutomaton: Send + Sync {
    async fn initialize(&mut self, ctx: HotlogAutomatonContext) -> SatelliteResult<()>;
    
    // Core event processing method (EVENT-DRIVEN)
    async fn process_event(
        &mut self,
        event: HotlogAutomatonEvent,
    ) -> SatelliteResult<ProcessingResult>;
    
    async fn process_batch(
        &mut self,
        events: Vec<HotlogAutomatonEvent>,
    ) -> SatelliteResult<Vec<ProcessingResult>>;
    
    fn event_filters(&self) -> Vec<EventFilter>;
    fn automaton_name(&self) -> &str;
}
```

**Key Characteristics:**
- **Event-driven**: Processes individual events as they arrive from Redis streams
- **Client-side filtering**: Events are consumed from `sinex:streams:hotlog` and filtered locally
- **Reactive processing**: Processing is triggered by event arrival, not time boundaries

### 2. HotlogAutomatonRunner Pattern

**Location**: `crate/sinex-satellite-sdk/src/automaton.rs:358`

The runner manages the automaton lifecycle and Redis stream consumption:

```rust
pub struct HotlogAutomatonRunner<T: HotlogAutomaton> {
    automaton: T,
    context: Option<HotlogAutomatonContext>,
}

impl<T: HotlogAutomaton> HotlogAutomatonRunner<T> {
    pub async fn run(&mut self) -> SatelliteResult<()> {
        // Main processing loop consuming from "sinex:streams:hotlog"
        self.process_hotlog_events("sinex:streams:hotlog").await
    }
}
```

**Processing Flow:**
1. Creates consumer group for `sinex:streams:hotlog` Redis stream
2. Reads messages from stream in batches (up to 10 at a time, 5s timeout)
3. Parses Redis messages into `HotlogAutomatonEvent` objects
4. Applies client-side filtering based on `event_filters()`
5. Processes filtered events via `process_event()` or `process_batch()`
6. ACKs successful messages to Redis

### 3. Redis Stream Architecture

**Stream Name**: `sinex:streams:hotlog`
**Message Format**: 
```json
{
  "data": "<serialized RawEvent JSON>"
}
```

**Consumer Groups**: Each automaton creates its own consumer group for parallel processing
**Checkpointing**: Uses Redis Stream message IDs for progress tracking

### 4. Event Filtering System

Automata define their interests through `EventFilter` objects:

```rust
pub struct EventFilter {
    pub source: Option<String>,        // e.g., "journald", "shell.kitty"
    pub event_type: Option<String>,    // e.g., "satellite.heartbeat"
    pub host: Option<String>,
    pub payload_filters: Vec<PayloadFilter>,
}
```

**Filtering Approach**: 
- Consume all events from hotlog stream
- Apply filters client-side after consumption
- Only process matching events
- ACK non-matching events immediately

## Target StatefulStreamProcessor Architecture

### 1. StatefulStreamProcessor Trait (Target System)

**Location**: `crate/sinex-satellite-sdk/src/stream_processor.rs`

The target unified interface for both ingestors and automata:

```rust
#[async_trait]
pub trait StatefulStreamProcessor: Send + Sync {
    async fn initialize(&mut self, ctx: StreamProcessorContext) -> SatelliteResult<()>;
    
    // Core unified scanning method (TIME-DRIVEN)
    async fn scan(
        &mut self,
        from: Checkpoint,
        until: TimeHorizon, 
        args: ScanArgs,
    ) -> SatelliteResult<ScanReport>;
    
    fn processor_name(&self) -> &str;
    fn processor_type(&self) -> ProcessorType; // Ingestor or Automaton
    async fn current_checkpoint(&self) -> SatelliteResult<Checkpoint>;
}
```

**Key Characteristics:**
- **Time-driven**: Processing is bounded by time ranges and checkpoints
- **Unified interface**: Same trait for ingestors (External → Events) and automata (Events → Synthesis)
- **Bounded operations**: Clear start/end points with explicit completion

### 2. TimeHorizon and Checkpoint System

**TimeHorizon Types:**
```rust
pub enum TimeHorizon {
    Historical { end_time: DateTime<Utc> },  // Process from checkpoint to end_time
    Continuous,                              // Process from checkpoint indefinitely
    Snapshot,                                // Instantaneous state capture
}
```

**Checkpoint Types:**
```rust  
pub enum Checkpoint {
    None,                                    // Start from beginning
    External { position: Value, ... },      // For ingestors (file offsets, etc.)
    Internal { event_id: Ulid, ... },       // For automata (event ULIDs)
    Stream { message_id: String, ... },     // For Redis streams
    Timestamp { timestamp: DateTime<Utc> },  // For time-based processing
}
```

### 3. Service Lifecycle Pattern

The target pattern implements a three-phase service startup:

1. **Snapshot Phase**: Capture current state (`TimeHorizon::Snapshot`)
2. **Gap-filling Phase**: Historical processing from last checkpoint (`TimeHorizon::Historical`)
3. **Continuous Phase**: Real-time processing (`TimeHorizon::Continuous`)

## Current Automaton Implementations

### 1. Health Aggregator Automaton

**Location**: `crate/sinex-health-aggregator/src/automaton.rs`

**Purpose**: Processes satellite heartbeat events and generates system health summaries

**Event Filters:**
```rust
vec![
    EventFilter::new(Some("journald".to_string()), Some("satellite.heartbeat".to_string())),
    EventFilter::new(Some("sinex".to_string()), None),
]
```

**Processing Logic:**
- Processes heartbeat events from journald source
- Maintains component health state (healthy, degraded, failed, missing)
- Generates `system_health_summary` synthesis events
- Uses batch processing to aggregate multiple heartbeats

**Key Methods:**
- `process_heartbeat_event()`: Extracts health data from events
- `generate_health_summary()`: Creates synthesis events for system status
- `process_batch()`: Implements custom batch aggregation logic

### 2. Terminal Command Canonicalizer

**Location**: `crate/sinex-terminal-command-canonicalizer/src/lib.rs`

**Purpose**: Creates canonical command synthesis events from multiple terminal sources

**Event Filters:**
```rust
vec![
    EventFilter::new(Some("shell.kitty".to_string()), Some("command.executed".to_string())),
    EventFilter::new(Some("shell.atuin".to_string()), Some("command.imported".to_string())),
    EventFilter::new(Some("shell.history".to_string()), Some("command.imported".to_string())),
    // ... more terminal sources
]
```

**Processing Logic:**
- Deduplication: Uses time windows to find existing canonical commands
- Enrichment: Updates existing commands with additional data
- Synthesis: Creates new `command.canonical` events when no duplicate found
- Database integration: Queries and updates `core.events` table directly

**Key Methods:**
- `find_existing_canonical_command()`: Database lookup for deduplication
- `create_canonical_command()`: Synthesis event creation
- `enrich_canonical_command()`: Updates existing events with new data

### 3. Analytics Service Automaton

**Location**: `crate/sinex-analytics-automaton/src/lib.rs`

**Purpose**: Provides analytics capabilities as RPC-style request/response automaton

**Event Filters:**
```rust  
vec![
    EventFilter::new(Some("rpc.analytics".to_string()), Some("request".to_string())),
]
```

**Processing Logic:**
- Request/response pattern: Processes analytics RPC requests  
- Service delegation: Uses `AnalyticsService` for actual analytics operations
- Methods supported: `analytics.event_count_by_source`, `analytics.activity_heatmap`
- Response handling: Currently logs responses (needs synthesis integration)

## Checkpoint Management System

### 1. Unified Checkpoint Storage

**Location**: `crate/sinex-satellite-sdk/src/checkpoint.rs`

**Database Table**: `core.automaton_checkpoints`

**Schema Fields:**
- `automaton_name`: Processor identifier
- `consumer_group`: Redis consumer group
- `consumer_name`: Instance identifier (hostname + PID)
- `checkpoint_data`: JSON-serialized unified checkpoint (v2+)
- `last_processed_id`: Legacy field for Redis Stream ID (v1 compatibility)

### 2. Version Migration System

**Version 1 (Legacy)**: String-based `last_processed_id` field
**Version 2 (Current)**: JSON-serialized `Checkpoint` enum in `checkpoint_data`

The `CheckpointManager` automatically migrates v1 checkpoints to v2 format:

```rust
impl CheckpointManager {
    pub async fn load_checkpoint(&self) -> SatelliteResult<CheckpointState> {
        // Loads existing checkpoint, migrates v1->v2 if needed
        // Falls back to Checkpoint::None on corruption
    }
    
    pub async fn save_checkpoint(&self, state: &CheckpointState) -> SatelliteResult<()> {
        // Atomic upsert with ON CONFLICT handling
        // Maintains backward compatibility with last_processed_id field
    }
}
```

### 3. Checkpoint Types and Usage

**For Current Automata (HotlogAutomaton)**:
- Uses `Checkpoint::Stream` with Redis message IDs
- Tracks processed message count for verification
- Saves checkpoint data after successful ACK to Redis

**For Target Automata (StatefulStreamProcessor)**:
- Uses `Checkpoint::Internal` with event ULIDs  
- Represents last processed event from `core.events` table
- Enables time-based resumption and gap-filling

## Architecture Comparison

### Current HotlogAutomaton vs Target StatefulStreamProcessor

| Aspect | HotlogAutomaton (Current) | StatefulStreamProcessor (Target) |
|--------|---------------------------|-----------------------------------|
| **Processing Model** | Event-driven, reactive | Time-driven, bounded |
| **Core Method** | `process_event(HotlogAutomatonEvent)` | `scan(Checkpoint, TimeHorizon, ScanArgs)` |
| **Data Source** | Redis Stream (`sinex:streams:hotlog`) | Database (`core.events` table) |
| **Filtering** | Client-side after consumption | Server-side via database queries |
| **Checkpointing** | Stream message IDs | Event ULIDs and timestamps |
| **Time Handling** | Event arrival time | Explicit time boundaries |
| **Batch Processing** | Optional batch method | Built-in via scan ranges |
| **Completion** | Never completes (continuous) | Clear completion for bounded operations |

### Key Differences in Implementation

1. **Event Access Pattern**:
   - **Current**: Stream consumption with ACK/NACK semantics
   - **Target**: Database queries with SQL predicates for filtering

2. **Progress Tracking**:
   - **Current**: Redis Stream message IDs (`1234567890-0`)
   - **Target**: Event ULIDs from database with time ordering

3. **Error Handling**:
   - **Current**: Retry via stream semantics, skip/retry/fail per event
   - **Target**: Checkpoint rollback, bounded retry with explicit scopes

4. **Scalability**:
   - **Current**: Consumer groups for parallel processing
   - **Target**: Partitioned scanning by time ranges or event ID ranges

## Migration Path: Current → Target

### 1. Automata Transformation Requirements

To migrate current automata to StatefulStreamProcessor:

```rust
// CURRENT: Event-driven processing
async fn process_event(&mut self, event: HotlogAutomatonEvent) -> SatelliteResult<ProcessingResult>

// TARGET: Time-driven scanning  
async fn scan(&mut self, from: Checkpoint, until: TimeHorizon, args: ScanArgs) -> SatelliteResult<ScanReport>
```

**Migration Steps:**
1. Replace `process_event()` with `scan()` implementation
2. Convert `EventFilter` logic to database queries with WHERE clauses
3. Change checkpointing from stream message IDs to event ULIDs
4. Implement time boundary handling for Historical/Continuous modes
5. Replace Redis stream consumption with database pagination

### 2. Health Aggregator Migration Example

**Current Implementation:**
```rust
async fn process_event(&mut self, event: HotlogAutomatonEvent) -> ProcessingResult {
    if let Some(component_health) = self.process_heartbeat_event(&event)? {
        // Process individual heartbeat
        return Ok(ProcessingResult::Success { checkpoint_data: Some(...) });
    }
    Ok(ProcessingResult::Skip { reason: "Not a heartbeat".to_string() })
}
```

**Target Implementation:**
```rust
async fn scan(&mut self, from: Checkpoint, until: TimeHorizon, args: ScanArgs) -> ScanReport {
    let events = self.query_heartbeat_events(&from, &until).await?;
    let mut processed = 0;
    
    for batch in events.chunks(100) {
        let health_summary = self.aggregate_health_batch(batch).await?;
        self.emit_synthesis_event(health_summary).await?;
        processed += batch.len();
    }
    
    ScanReport {
        events_processed: processed,
        final_checkpoint: Checkpoint::internal(last_event_id, processed),
        // ...
    }
}
```

### 3. Command Canonicalizer Migration Example

**Current Implementation:**
- Processes events one-by-one with database lookups for deduplication
- Uses Redis stream events as triggers for processing

**Target Implementation:**
- Scans time ranges of terminal command events from database
- Groups by command text and time windows for deduplication
- Processes in batches with fewer database round-trips
- Clear completion criteria for bounded scans

## Implementation Considerations

### 1. Performance Implications

**Current System Benefits:**
- Stream-based processing with natural backpressure
- Consumer group parallelism
- Low-latency event-driven processing

**Target System Benefits:**
- Server-side filtering reduces network traffic
- Batch processing reduces database overhead
- Clear completion semantics for bounded work

**Migration Challenges:**
- Database query performance for event filtering
- Checkpoint complexity for time-based resumption
- Ensuring exactly-once processing without stream semantics

### 2. Operational Differences

**Monitoring:**
- **Current**: Redis stream consumer lag metrics
- **Target**: Time-based processing lag, scan completion rates

**Debugging:**
- **Current**: Redis stream message inspection, consumer group status
- **Target**: Database checkpoint queries, scan progress tracking

**Scaling:**
- **Current**: Consumer group multiplication
- **Target**: Time range or event ID range partitioning

### 3. Data Consistency

**Current System:**
- Redis stream ordering guarantees
- At-least-once processing with ACK semantics
- Stream message IDs provide ordering

**Target System:**
- Database ULID ordering for event sequencing
- Checkpoint-based exactly-once processing
- Time-based consistency with explicit boundaries

## Concrete Implementation Examples

### 1. Current Satellites Using StatefulStreamProcessor

**Terminal Satellite**: `crate/sinex-terminal-satellite/src/unified_processor.rs`

Already implements the target architecture:

```rust
impl StatefulStreamProcessor for TerminalProcessor {
    async fn scan(&mut self, from: Checkpoint, until: TimeHorizon, args: ScanArgs) -> ScanReport {
        match until {
            TimeHorizon::Snapshot => self.take_snapshot().await,
            TimeHorizon::Historical { end_time } => self.scan_historical(from, end_time, args).await,
            TimeHorizon::Continuous => self.run_continuous(from, args).await,
        }
    }
    
    fn processor_type(&self) -> ProcessorType { ProcessorType::Ingestor }
}
```

**Key Features:**
- Three-phase startup sequence (snapshot, gap-fill, continuous)
- External checkpointing for file positions and timestamps
- Bounded historical scanning with clear completion
- Interactive exploration support

### 2. Current CLI Integration

**Processor CLI**: `crate/sinex-satellite-sdk/src/cli.rs`

Provides unified command-line interface for both architectures:

```bash
# Current usage with satellites
terminal-satellite service    # Continuous processing 
terminal-satellite scan       # Bounded scanning
terminal-satellite explore    # Interactive analysis

# Future usage with automata  
health-aggregator service     # Three-phase startup
health-aggregator scan --from=yesterday --until=now
command-canonicalizer explore --show-duplicates
```

## Recommendations

### 1. Migration Priority

1. **High Priority**: Health Aggregator (simple aggregation pattern)
2. **Medium Priority**: Analytics Service (request/response pattern)
3. **Low Priority**: Command Canonicalizer (complex deduplication logic)

### 2. Implementation Strategy

1. **Parallel Implementation**: Maintain current HotlogAutomaton while building StatefulStreamProcessor versions
2. **Feature Parity**: Ensure equivalent functionality before cutover
3. **Gradual Migration**: Switch automata one at a time with rollback capability

### 3. Architecture Decisions

1. **Hybrid Approach**: Keep Redis streams for real-time notifications, use database for bulk processing
2. **Checkpoint Migration**: Automatic v1→v2 checkpoint migration in production
3. **Monitoring**: Dual metrics during transition period

## Conclusion

The Sinex project has two well-designed automaton architectures serving different use cases:

- **HotlogAutomaton**: Excellent for real-time, event-driven processing with Redis stream semantics
- **StatefulStreamProcessor**: Superior for bounded operations, historical analysis, and unified ingestor/automaton interfaces

The migration path is clear but requires careful attention to:
1. Performance implications of database-centric processing
2. Maintaining exactly-once processing semantics 
3. Preserving operational visibility and debugging capabilities

Both architectures have merit, and the choice depends on specific use case requirements for latency, consistency, and operational characteristics.
# Channel Capacity Analysis for Sinex

## Current Channel Usage

### 1. Terminal Satellite Channel (1000 capacity)
**Location**: `crate/satellites/sinex-terminal-satellite/src/sensd_integration.rs:739`

**Data Flow**:
1. **Initial Ingestion**: Processes shell history files
   - `.bash_history`: 500 lines (my system)
   - `.zsh_history`: 32,102 lines (my system) 
   - `.local/share/fish/fish_history`: Variable
   - Atuin database: Could be 100,000+ commands

2. **Processing Pattern**:
   ```rust
   // From sensd_integration.rs:498-534
   for (line_num, line) in data_str.lines().enumerate() {
       // Creates ONE EVENT PER LINE
       events.push(event);
   }
   ```

3. **Batch Processing**: 
   - Config shows `batch_size: 100` (line 46)
   - History files configured with `batch_size: 50` (line 137)
   - BUT the code creates events for ALL lines in a slice at once!

**PROBLEM IDENTIFIED**: 
- Channel capacity: 1000
- Shell history: 32,000+ lines
- Each line becomes an event
- **Result**: Channel will block after 1000 commands!

### 2. Desktop Satellite Channel (1000 capacity)
**Location**: `crate/satellites/sinex-desktop-satellite/src/desktop_sensd_integration.rs:464`

**Data Flow**: Desktop events (window changes, app launches)
- Lower volume than terminal
- 1000 probably sufficient

### 3. Document Ingestor Channel (1000 capacity)
**Location**: `crate/satellites/sinex-document-ingestor/src/lib.rs:521`

**Data Flow**: Document processing
- Depends on document count
- Could be problematic for large directories

### 4. GRPC Server Channel (100 capacity)
**Location**: `crate/core/sinex-sensd/src/grpc_server.rs:79`

**Data Flow**: Streaming material slices over network
- 100 is reasonable for network backpressure
- Properly bounded for streaming

## The Real Problem

The issue isn't just capacity - it's the **processing model**:

1. **No Pagination in Event Generation**:
   ```rust
   // sensd_integration.rs:230
   let events = self.slice_to_events(slice).await?;  // Creates ALL events at once
   
   for event in events {  // Then tries to send them all
       self.event_sender.send(event).await  // BLOCKS when channel full!
   }
   ```

2. **Existing Solutions NOT Used**:
   - Slices are batched from database (LIMIT 100)
   - But events from slices aren't batched!

## Solutions

### Option 1: Increase Channel Capacity (Band-aid)
```rust
// Change from:
mpsc::channel(1000)
// To:
mpsc::channel(100_000)  // Accommodate large histories
```
**Problems**: 
- Memory usage (100K events × size of Event struct)
- Still fails for very large histories

### Option 2: Batch Event Processing (Proper Fix)
```rust
// Instead of creating all events at once:
async fn process_history_file_slice(&self, slice: &MaterialSlice) {
    let data_str = String::from_utf8_lossy(&slice.data);
    
    // Process in chunks
    for chunk in data_str.lines().chunks(100) {
        let mut events = Vec::new();
        for line in chunk {
            // Create event
            events.push(event);
        }
        
        // Send this batch
        for event in events {
            self.event_sender.send(event).await?;
        }
        
        // Allow other tasks to run
        tokio::task::yield_now().await;
    }
}
```

### Option 3: Use Existing Pagination
The system already has pagination at the slice level:
- `batch_size: 100` in config
- Database queries use LIMIT

**But**: A single slice might contain thousands of lines!

The fix: Make slices smaller OR process lines within slices in batches.

### Option 4: Stream Processing (Best)
Don't create intermediate Vec of events:
```rust
// Stream events directly instead of collecting
async fn process_history_file_slice(&self, slice: &MaterialSlice) -> impl Stream<Item = Event> {
    // Return a stream that yields events one by one
    // Channel never gets overwhelmed
}
```

## Existing Mitigations

1. **Outbox Pattern**: System has `core.transactional_outbox` for durability
   - Events are persisted before publishing
   - Could handle overflow to database

2. **Backpressure**: `send().await` blocks when full
   - System won't lose data, just slows down
   - But this defeats the purpose of async!

3. **Database Pagination**: Already limits slice queries
   - But doesn't help when one slice = thousands of events

## Recommendation

**Immediate**: Keep channel at 1000, but fix the event generation to stream/batch properly.

**Why Not Just Increase Capacity?**
1. **32K+ events in memory is wasteful** 
2. **Doesn't solve the root cause** - unbounded event generation
3. **Will still fail** for users with larger histories (100K+ commands)
4. **Hides the real issue** - synchronous processing of async data

**The Real Fix**: 
The channels aren't the problem. The problem is creating thousands of events synchronously before sending them. The solution is already partially there (batch_size configs) but not properly implemented at the event generation level.

## Code Location for Fix

File: `crate/satellites/sinex-terminal-satellite/src/sensd_integration.rs`
- Line 230: `slice_to_events()` creates all events at once
- Line 498-534: Loop that creates events for every line
- Line 233: Loop that sends all events

The fix should paginate or stream at the event creation level, not just at the slice level.
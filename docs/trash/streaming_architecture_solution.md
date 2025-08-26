# Streaming Architecture Solution for Sinex

## Executive Summary

This document describes a comprehensive solution to Sinex's channel capacity and streaming problems. Rather than completely inverting the architecture (which would break important guarantees), we propose a hybrid approach that:

1. Uses NATS JetStream for satellite→ingestd communication (replacing problematic channels)
2. Preserves the database as the single source of truth
3. Maintains the transactional outbox pattern for reliability
4. Provides natural backpressure without arbitrary capacity decisions

## The Problem

### Current Pain Points

The current architecture forces us into arbitrary decisions about channel capacities:

```rust
// Terminal satellite - current broken implementation
let (tx, rx) = mpsc::channel(1000);  // Why 1000? What if we have 32K events?

// Creates ALL events at once
let events = slice_to_events(slice).await?;  // Could be 32,000 events!

// Then tries to push through limited channel
for event in events {
    tx.send(event).await;  // BLOCKS after 1000!
}
```

### Root Cause Analysis

The fundamental issue isn't the channel capacity - it's the **synchronous collection** of unbounded data before asynchronous processing:

1. **Shell history**: 32,000+ lines loaded at once
2. **Vec collection**: All events created in memory
3. **Channel bottleneck**: Limited capacity causes blocking
4. **No natural backpressure**: Producer doesn't know consumer's pace

## Current Architecture

### Data Flow
```
Satellites → Channels(?) → Ingestd → Database → Outbox → NATS → Consumers
                ↑                         ↓
            Problem Here!          Single Source of Truth
```

### Guarantees Provided
1. **Durability**: Every event on NATS is persisted in database
2. **Atomicity**: Database write and NATS publish succeed or fail together
3. **Ordering**: Events maintain temporal ordering via ULID keys
4. **Recovery**: Outbox ensures eventual delivery after crashes

## Proposed Solution: Hybrid Streaming Architecture

### New Data Flow
```
Satellites → NATS Staging → Ingestd → Database → Outbox → NATS Events → Consumers
                  ↓                          ↓                    ↓
            Buffer/Transport          Source of Truth      Notifications
```

### Key Components

#### 1. NATS Staging Stream
A temporary buffering layer for high-throughput ingestion:

```yaml
streams:
  staging:
    subjects: ["staging.>"]
    retention: 1 hour          # Short-lived
    max_age: 3600s
    max_msgs: 10_000_000       # High capacity
    storage: file              # Disk-backed
    discard: old               # Drop old if full
```

Purpose:
- Replace internal channels
- Handle bursts (32K shell history)
- Provide natural backpressure
- Allow replay on failure

#### 2. NATS Events Stream (Existing)
Authoritative event notifications:

```yaml
streams:
  events:
    subjects: ["events.>"]
    retention: 7 days          # Longer retention
    max_age: 604800s
    storage: file
    num_replicas: 1
    discard: none              # Never drop
```

Purpose:
- Notify consumers of persisted events
- Guarantee every event is in database
- Support replay for recovery
- Maintain event ordering

#### 3. Transactional Outbox (Preserved)
Ensures atomic persistence and publishing:

```sql
-- Same pattern, still necessary!
BEGIN;
  INSERT INTO core.events (...);
  INSERT INTO core.transactional_outbox (...);
COMMIT;
-- Background process publishes from outbox to NATS Events
```

## Implementation Details

### Phase 1: Satellite Streaming Refactor

Replace channel-based event collection with streaming:

```rust
// OLD: Problematic channel-based approach
impl TerminalProcessor {
    async fn process_history(&self) -> Result<()> {
        let (tx, rx) = mpsc::channel(1000);  // Arbitrary capacity!
        
        let history = load_history_file().await?;
        let events = parse_all_events(history);  // ALL at once!
        
        for event in events {
            tx.send(event).await;  // Can block!
        }
    }
}

// NEW: Direct streaming to NATS
impl TerminalProcessor {
    async fn process_history(&self) -> Result<()> {
        let js = self.nats.jetstream();
        let file = BufReader::new(File::open(HISTORY_PATH)?);
        
        // Stream lines one-by-one
        for line in file.lines() {
            let event = create_event(line?);
            
            // Publish to staging - natural backpressure
            js.publish("staging.terminal.history", &event).await?;
            // Publisher waits for JetStream acknowledgment
        }
        
        Ok(())
    }
}
```

Benefits:
- No Vec collection of 32K events
- No channel capacity decisions
- Natural backpressure from NATS
- Automatic retry on failure

### Phase 2: Ingestd Consumer Implementation

Add staging stream consumer to ingestd:

```rust
impl IngestService {
    /// Start consumer for staging stream
    async fn start_staging_consumer(&self) -> Result<()> {
        // Create durable consumer
        let consumer = self.jetstream
            .consumer("staging", ConsumerConfig {
                durable_name: Some("ingestd-consumer".to_string()),
                ack_policy: AckPolicy::Explicit,
                ack_wait: Duration::from_secs(30),
                max_deliver: 3,
                filter_subject: "staging.>".to_string(),
                ..Default::default()
            })
            .await?;
        
        loop {
            // Pull sustainable batch
            let messages = consumer.batch()
                .max_messages(self.config.batch_size)
                .expires(Duration::from_secs(1))
                .fetch()
                .await?;
            
            if messages.is_empty() {
                continue;
            }
            
            // Convert to events
            let events: Vec<Event> = messages
                .iter()
                .filter_map(|msg| self.parse_staging_message(msg).ok())
                .collect();
            
            // Process through existing pipeline (with outbox!)
            match self.batch_write_to_db(&events).await {
                Ok(_) => {
                    // Acknowledge only after successful persistence
                    for msg in messages {
                        msg.ack().await?;
                    }
                }
                Err(e) => {
                    error!("Failed to persist batch: {}", e);
                    // Don't ack - messages will be redelivered
                }
            }
        }
    }
}
```

### Phase 3: Stream Processing Utilities

Add async-stream for elegant stream handling:

```toml
# Cargo.toml additions
async-stream = "0.3"
tokio-stream = "0.2"
futures = "0.3"
```

Utility functions for stream processing:

```rust
use async_stream::stream;
use tokio_stream::{Stream, StreamExt};

/// Convert material slice to event stream
fn slice_to_event_stream(slice: &MaterialSlice) -> impl Stream<Item = Event> + '_ {
    stream! {
        let data_str = String::from_utf8_lossy(&slice.data);
        
        for (line_num, line) in data_str.lines().enumerate() {
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            
            let event = Event::new(
                EventSource::from("terminal"),
                EventType::from("command"),
                json!({ "command": line, "line_num": line_num }),
            );
            
            yield event;
        }
    }
}

/// Process stream with automatic batching
async fn process_event_stream<S>(
    stream: S,
    batch_size: usize,
    nats: &JetStream,
) -> Result<()> 
where
    S: Stream<Item = Event>,
{
    let mut stream = Box::pin(stream);
    let mut batch = Vec::with_capacity(batch_size);
    
    while let Some(event) = stream.next().await {
        batch.push(event);
        
        if batch.len() >= batch_size {
            // Publish batch to staging
            for event in batch.drain(..) {
                nats.publish("staging.events", &event).await?;
            }
        }
    }
    
    // Flush remaining
    for event in batch {
        nats.publish("staging.events", &event).await?;
    }
    
    Ok(())
}
```

## Migration Strategy

### Step 1: Deploy NATS Staging Stream
```bash
# Create staging stream via NATS CLI
nats stream add staging \
  --subjects "staging.>" \
  --retention=limits \
  --max-age=1h \
  --max-msgs=10000000 \
  --storage=file \
  --discard=old
```

### Step 2: Update Satellites Incrementally
1. Start with terminal-satellite (highest volume)
2. Test with real 32K+ line history files
3. Monitor NATS metrics
4. Roll out to other satellites

### Step 3: Add Ingestd Consumer
1. Deploy consumer alongside existing gRPC endpoint
2. Monitor both paths in parallel
3. Gradually shift traffic to NATS path
4. Deprecate channel-based code

### Step 4: Cleanup
1. Remove channel creation code
2. Delete Vec collection patterns
3. Remove arbitrary capacity constants

## Performance Characteristics

### Before (Channel-Based)
- **Memory**: O(n) for all events in flight
- **Latency**: Blocking on channel capacity
- **Throughput**: Limited by channel size
- **Recovery**: Lost events on crash

### After (NATS Staging)
- **Memory**: O(1) - streaming one event at a time
- **Latency**: Non-blocking with backpressure
- **Throughput**: Limited only by NATS/disk
- **Recovery**: Automatic replay from JetStream

## Monitoring and Observability

### Key Metrics

```rust
// Add to satellites
metrics! {
    counter!("staging.events.published", 1);
    histogram!("staging.publish.latency", latency);
}

// Add to ingestd
metrics! {
    counter!("staging.events.consumed", batch.len());
    gauge!("staging.consumer.lag", consumer.pending());
    histogram!("staging.batch.size", batch.len());
}
```

### NATS Monitoring
```bash
# Monitor staging stream
nats stream info staging

# Check consumer lag
nats consumer info staging ingestd-consumer

# Watch message rates
nats stream view staging
```

## Why This Solution Works

### Addresses Root Causes
1. **No arbitrary capacities**: NATS handles buffering dynamically
2. **No synchronous collection**: Events streamed one-by-one
3. **Natural backpressure**: Publishers wait for acknowledgments
4. **Graceful degradation**: Staging can drop old messages if needed

### Preserves Guarantees
1. **Database remains authoritative**: No architectural inversion
2. **Outbox ensures atomicity**: Write+publish still atomic
3. **Event ordering maintained**: ULID keys preserve time order
4. **Crash recovery**: Staging replays, outbox retries

### Implementation Simplicity
1. **Uses existing infrastructure**: NATS already configured
2. **Incremental migration**: Can run both paths in parallel
3. **Minimal code changes**: Mostly replacing channel sends with publishes
4. **Battle-tested patterns**: JetStream is production-ready

## Alternative Approaches Considered

### 1. Just Increase Channel Capacity
**Rejected because**:
- Still requires guessing capacity
- Wastes memory (100K events × size)
- Doesn't solve root cause
- Still fails for very large datasets

### 2. Full NATS-First Architecture
**Rejected because**:
- Would break persistence guarantees
- Database would no longer be authoritative
- Complex duplicate handling needed
- Query consistency problems

### 3. Timely Dataflow
**Rejected because**:
- Overkill for current needs
- Complex to integrate
- Better suited for analytics than ingestion
- Steep learning curve

### 4. Direct Database Streaming
**Rejected because**:
- Satellites shouldn't know about database
- Would couple components too tightly
- Loses buffering capability
- No replay on failure

## Future Enhancements

### Near Term
1. **Compression**: Enable NATS message compression
2. **Batching**: Implement smart batching in staging consumer
3. **Metrics**: Add comprehensive observability
4. **Rate Limiting**: Implement per-satellite rate limits

### Long Term
1. **Sharding**: Partition staging stream by source
2. **Priority**: Different retention for different event types
3. **Deduplication**: Use NATS deduplication features
4. **Stream Processing**: Consider Timely for complex analytics

## Conclusion

This hybrid approach solves Sinex's channel capacity problems without sacrificing architectural guarantees. By using NATS JetStream as a transport layer while maintaining the database as the source of truth, we get:

- **Unlimited buffering** without memory exhaustion
- **Natural backpressure** without arbitrary limits
- **Preserved guarantees** about persistence and atomicity
- **Simple migration** path from current architecture

The key insight: **NATS staging replaces channels, not the database**. The transactional outbox pattern remains essential for ensuring that the main event stream only contains persisted events.

This is not a theoretical solution - all components (NATS JetStream, async-stream, SQLx streaming) are production-ready and already available in the codebase.
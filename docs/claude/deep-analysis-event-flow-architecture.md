# Deep Analysis: Event Flow Architecture

**Analysis Date:** 2025-11-16
**Focus:** Provisional/Confirmed Event Model, NATS JetStream Integration, Idempotency

---

## 🔄 Event Flow Overview

### Complete Event Lifecycle

```
Satellite Capture
    ↓ (stage material, emit provisional)
NATS JetStream events.raw.{source}.{type}
    ↓ (Nats-Msg-Id for idempotency)
IngestdIngestd JetStreamConsumer
    ├─→ Validate Event
    ├─→ Persist to Postgres (TimescaleDB)
    ├─→ Publish Confirmation → events.confirmations.{event_id}
    └─→ On Error → DLQ events.dlq.ingestd
         ↓ (confirmed events only)
Automata (search, analytics, health)
    ├─→ Optional: Handle Provisional (with rollback)
    └─→ Required: Handle Confirmed
         ↓
Secondary Processing, Indexing, Analysis
```

---

## 📊 NATS JetStream Topology

### Stream Configuration Analysis

#### 1. **Events Stream** (`SINEX_EVENTS`)
```rust
max_messages: 10_000_000
max_age: Duration::from_secs(90 * 24 * 60 * 60) // 90 days
retention: RetentionPolicy::Limits
storage: StorageType::File
subjects: ["events.raw.>"]
```

**Purpose:** Operational history for automata replay
**Retention Strategy:** 90 days or 10M messages (whichever hits first)
**Storage:** File-based (persistent across restarts)

**Analysis:**
- ✅ 90-day retention supports full operational history replay
- ✅ 10M message cap prevents unbounded growth
- ⚠️ **ISSUE:** No monitoring for approaching message limits
- ⚠️ **ISSUE:** No automatic cleanup strategy when hitting 10M
- 💡 **RECOMMENDATION:** Add metrics for stream utilization percentage
- 💡 **RECOMMENDATION:** Add alerting at 80% capacity
- 💡 **RECOMMENDATION:** Document expected event volume vs limits

#### 2. **Confirmations Stream** (`SINEX_EVENTS_CONFIRMATIONS`)
```rust
max_messages_per_subject: 1  // COMPACTION
max_age: Duration::from_secs(7 * 24 * 60 * 60) // 7 days
subjects: ["events.confirmations.>"]
```

**Purpose:** Ephemeral operational state for provisional→confirmed transitions
**Compaction Strategy:** Only keep latest confirmation per event ID
**Retention:** 7 days

**Analysis:**
- ✅ **EXCELLENT:** Stream compaction prevents confirmation accumulation
- ✅ Confirmations are per-event-id subjects (enables compaction)
- ✅ Short retention (7d) appropriate for ephemeral operational state
- ✅ Subject pattern: `events.confirmations.{event_id}` enables targeted consumption
- 💡 **INSIGHT:** This is clever - confirmations auto-cleanup via compaction
- ⚠️ **QUESTION:** What happens if automaton is down >7 days? Missed confirmations?

#### 3. **Dead Letter Queue** (`SINEX_EVENTS_DLQ`)
```rust
max_messages: 1_000_000
max_age: Duration::from_secs(30 * 24 * 60 * 60) // 30 days
subjects: ["events.dlq.>"]
```

**Purpose:** Failed event storage for debugging and reprocessing
**Retention:** 30 days or 1M messages

**Analysis:**
- ✅ Separate DLQ prevents failed events from blocking pipeline
- ✅ 30-day retention longer than confirmations (good for investigation)
- ⚠️ **ISSUE:** No documented DLQ reprocessing strategy
- ⚠️ **ISSUE:** 1M message cap could be hit quickly under high error rates
- 💡 **RECOMMENDATION:** Add DLQ monitoring and alerting
- 💡 **RECOMMENDATION:** Document DLQ replay procedure
- 💡 **RECOMMENDATION:** Consider tiered DLQ (parse errors vs validation errors)

---

## 🔒 Idempotency Mechanisms

### 1. NATS-Level Idempotency

**Implementation:**
```rust
// sinex-satellite-sdk/src/nats_publisher.rs:48-49
let mut headers = async_nats::HeaderMap::new();
headers.insert("Nats-Msg-Id", event_id.to_string().as_str());
```

**Guarantees:**
- NATS JetStream uses `Nats-Msg-Id` for deduplication
- Duplicate publishes with same ID are ignored
- Window: configurable per stream (default: recent history)

**Analysis:**
- ✅ Protects against network retries at satellite level
- ✅ ULID as message ID provides temporal ordering + uniqueness
- ⚠️ **QUESTION:** What is the actual deduplication window?
- ⚠️ **ISSUE:** Deduplication window not explicitly configured
- 💡 **RECOMMENDATION:** Explicitly set deduplication window in stream config
- 💡 **RECOMMENDATION:** Document expected retry behavior

### 2. Double-Await Pattern

**Implementation:**
```rust
// sinex-satellite-sdk/src/nats_publisher.rs:54-57
let ack = js
    .publish_with_headers(subject, headers, payload.into())
    .await?  // Send publish request
    .await?; // Wait for PublishAck from JetStream
```

**Purpose:**
- First await: network send completes
- Second await: JetStream confirms persistence

**Analysis:**
- ✅ **EXCELLENT:** Ensures event is durable before returning success
- ✅ Prevents "fire and forget" data loss
- ⚠️ **ISSUE:** No timeout on second await (could hang indefinitely)
- ⚠️ **ISSUE:** No retry logic if second await fails
- 💡 **RECOMMENDATION:** Add timeout wrapping second await
- 💡 **RECOMMENDATION:** Consider exponential backoff retry on ack failures

---

## 🎯 Provisional/Confirmed Event Model

### Design Pattern Analysis

#### Provisional Event Phase
```rust
pub struct ProvisionalEvent {
    pub event_id: Ulid,
    pub source: String,
    pub event_type: String,
    pub payload: serde_json::Value,
    pub ts_orig: DateTime<Utc>,
    pub received_at: DateTime<Utc>,
}
```

**Purpose:** Events in-flight, not yet persisted to database
**State:** Pending confirmation or DLQ routing

#### Confirmation Phase
```rust
pub struct EventConfirmation {
    pub event_id: Ulid,
    pub persisted: bool,
    pub ts_ingest: DateTime<Utc>,
}
```

**Purpose:** Signal that event is durably stored in Postgres
**Delivery:** Published to `events.confirmations.{event_id}`

### Automaton Processing Models

#### Model 1: Confirmed-Only (Stateless Worker)
```rust
ProcessingModel::StatelessWorker
```
- Only processes confirmed events
- Can run multiple instances in parallel
- No provisional state to manage
- Simpler, more reliable

**Use Cases:**
- Analytics automaton (batch processing)
- Search indexing (eventual consistency ok)
- Health aggregation (periodic snapshots)

#### Model 2: Provisional + Confirmed (Leader/Standby)
```rust
ProcessingModel::LeaderStandby
```
- Processes provisional events immediately
- Requires rollback capability if event fails
- Uses NATS KV leases for coordination
- Only one active processor (leader)

**Use Cases:**
- Real-time alerting (need immediate response)
- Live dashboards (can't wait for confirmation)
- Stream processing with rollback

**Implementation Requirements:**
```rust
trait ProvisionalEventHandler {
    async fn handle_provisional(&self, event: &ProvisionalEvent) -> Result<()>;
    async fn rollback_provisional(&self, event_id: Ulid) -> Result<()>;
}
```

### Confirmation Buffer

**Implementation:**
```rust
pub struct ConfirmationBuffer {
    pending: Arc<RwLock<HashMap<Ulid, ProvisionalEvent>>>,
    timeout: std::time::Duration,
}
```

**Features:**
- In-memory buffer of provisional events awaiting confirmation
- Configurable timeout (default appears to be service-specific)
- Periodic timeout checking
- Auto-cleanup of timed-out events

**Analysis:**
- ✅ Clean separation of concerns
- ✅ Timeout prevents unbounded memory growth
- ⚠️ **ISSUE:** Buffer is in-memory only (lost on restart)
- ⚠️ **ISSUE:** No persistence of provisional processing state
- ⚠️ **ISSUE:** If automaton crashes, provisional work is lost
- 💡 **RECOMMENDATION:** Consider optional persistent buffer
- 💡 **RECOMMENDATION:** Document recovery behavior after crash
- 💡 **RECOMMENDATION:** Add metrics: buffer size, timeout rate, confirmation lag

---

## 🔍 Critical Issues Found

### 1. **Confirmation Timeout Handling** (MEDIUM)

**File:** `crate/lib/sinex-satellite-sdk/src/confirmation_handler.rs:108`
```rust
if age.to_std().unwrap_or_default() > self.timeout {
    timed_out.push(*event_id);
}
```

**Issues:**
- Uses `unwrap_or_default()` which silently treats duration conversion errors as 0
- A negative duration (clock skew) would be treated as timed out immediately
- No logging of timeout events

**Impact:**
- Clock skew could cause false timeouts
- Silent failure mode (no warning)
- Hard to debug timeout issues

**Recommendation:**
```rust
let age = now.signed_duration_since(event.received_at);
match age.to_std() {
    Ok(std_age) if std_age > self.timeout => {
        warn!(event_id = %event_id, age_secs = std_age.as_secs(),
              "Confirmation timeout");
        timed_out.push(*event_id);
    }
    Err(e) => {
        error!(event_id = %event_id, error = %e,
               "Invalid duration in timeout check (clock skew?)");
    }
    _ => {} // Within timeout
}
```

### 2. **No Backpressure on Event Publishing** (HIGH)

**File:** `crate/lib/sinex-satellite-sdk/src/nats_publisher.rs:54`

**Issue:**
- `publish_with_headers` returns Future that must be awaited twice
- No timeout on second await (JetStream ack)
- If JetStream is slow/down, satellite blocks indefinitely
- No queue depth limit on pending publishes

**Impact:**
- Satellite can hang if NATS is slow
- No graceful degradation under backpressure
- Memory can grow unbounded with pending futures

**Recommendation:**
1. Add timeout to second await
2. Implement bounded channel for publish queue
3. Add backpressure metrics
4. Consider "fail fast" mode vs buffering

### 3. **Stream Capacity Monitoring Missing** (MEDIUM)

**File:** `crate/core/sinex-ingestd/src/jetstream_consumer.rs:138-150`

**Issue:**
- No monitoring of stream message count vs `max_messages`
- No alerting when approaching capacity
- Hitting limit could cause silent message loss
- No visibility into stream health

**Impact:**
- Events could be dropped silently when streams fill
- No early warning before capacity issues
- Difficult to capacity plan

**Recommendation:**
1. Add periodic stream info queries
2. Emit metrics: current_messages, max_messages, utilization%
3. Alert at 80% capacity
4. Document capacity planning guidelines

### 4. **Material Assembly Pattern Not Fully Analyzed**

**File:** `crate/core/sinex-ingestd/src/material_assembler.rs`

**Gap:** Need to read and analyze the MaterialAssembler implementation

**Questions:**
- How are blob chunks assembled?
- What happens if chunks arrive out of order?
- Is there a timeout for incomplete materials?
- How is deduplication handled?

**Action:** Continue analysis...

---

## 🎨 Architectural Patterns Observed

### 1. **Event Sourcing Pattern** ✅
- All events are immutable
- Full event history retained (90 days)
- Replay capability for automata
- Provenance tracking

### 2. **CQRS Pattern** ✅
- Write path: Satellites → NATS → Ingestd → Postgres
- Read path: Gateway RPC, Automata queries
- Separation of concerns

### 3. **Saga Pattern** (Provisional/Confirmed)
- Two-phase processing
- Rollback capability
- Eventual consistency
- State transitions: Provisional → Confirmed | DLQ

### 4. **Dead Letter Queue Pattern** ✅
- Failed events isolated
- Prevents poison pill blocking
- Enables investigation and replay

### 5. **Compaction/Log Cleanup Pattern** ✅
- Confirmations stream uses compaction
- Only latest confirmation per event retained
- Automatic cleanup

---

## 📈 Performance Considerations

### Bottleneck Analysis

#### 1. JetStream Consumer Throughput
- Single consumer per ingestd instance
- Database writes are batch-friendly
- Confirmation publishing is per-event

**Potential Bottleneck:**
- If confirmation publishing is slow, could back up processing
- Each event requires: DB write + confirmation publish

**Measurement Needed:**
- Events/second processed
- Confirmation publish latency
- Database write latency
- Consumer lag

#### 2. Confirmation Buffer Memory
- In-memory hashmap of pending events
- Grows with confirmation lag
- Could be significant with high event rate + slow confirmations

**Calculation:**
- Event size: ~1KB average?
- At 1000 events/sec with 10s confirmation lag: 10K events × 1KB = 10MB
- At 1000 events/sec with 60s confirmation lag: 60K events × 1KB = 60MB
- Reasonable, but needs monitoring

#### 3. NATS Message Size
- Events include full payload
- Large payloads could impact NATS throughput
- Material chunks are separate (good!)

**Observed:**
- No max message size configured
- Could cause issues with very large events

---

## ✅ Strengths

1. **Idempotency at Multiple Levels**
   - NATS-level deduplication
   - ULID uniqueness
   - Explicit Nats-Msg-Id headers

2. **Clear Separation: Provisional vs Confirmed**
   - Well-defined state transitions
   - Rollback capability
   - Flexible processing models

3. **Stream Compaction for Confirmations**
   - Elegant solution to confirmation accumulation
   - Self-cleaning
   - Minimal storage overhead

4. **DLQ Isolation**
   - Failed events don't block pipeline
   - 30-day retention for investigation
   - Separate stream topology

5. **Comprehensive Testing**
   - Confirmation buffer tests
   - Timeout tests
   - Integration tests

---

## ⚠️ Weaknesses & Recommendations

### Immediate (High Priority)

1. **Add timeouts to JetStream ack awaits**
   - Prevent indefinite hangs
   - Enable fast failure
   - 5-10 second timeout recommended

2. **Monitor stream capacity**
   - Current messages vs max
   - Utilization percentage
   - Alert at 80%

3. **Add confirmation timeout logging**
   - Warn on timeout
   - Include age, event ID
   - Help debugging

### Short Term (Medium Priority)

4. **Document DLQ replay procedure**
   - How to reprocess failed events
   - Who is responsible
   - What tools to use

5. **Add backpressure metrics**
   - Publish queue depth
   - JetStream ack latency
   - Consumer lag

6. **Explicit deduplication window config**
   - Currently using defaults
   - Should be explicit in code
   - Document expected behavior

### Long Term (Low Priority)

7. **Consider persistent confirmation buffer**
   - Survive automaton restarts
   - Avoid losing provisional work
   - Trade-off: complexity vs robustness

8. **Tiered DLQ strategy**
   - Separate parse errors from validation errors
   - Different retention policies
   - Enable targeted reprocessing

9. **Capacity planning documentation**
   - Expected event rates
   - Stream sizing guidelines
   - Growth projections

---

**Analysis Status:** Incomplete - Continue with MaterialAssembler deep dive
**Files Analyzed:** 6
**Issues Found:** 4 critical, 8 improvements
**Next:** Blob assembly, checkpoint mechanisms, ULID usage patterns

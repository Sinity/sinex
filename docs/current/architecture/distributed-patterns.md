# Distributed Systems Patterns

Sinex's distributed systems patterns for event sourcing, concurrency, and operations.

## Event Sourcing & CQRS

### Event Sourcing

- All events immutable and retained
- Full replay capability
- Provenance tracking

### CQRS (Command Query Responsibility Segregation)

- Write: Nodes → NATS → Ingestd → Postgres
- Read: Gateway RPC, Automata queries
- Clear separation

### Saga Pattern (Provisional → Confirmed)

- Two-phase event processing
- Compensating transactions (rollback)
- Eventual consistency

### Dead Letter Queue

- Failed events isolated
- 30-day retention
- Prevents poison pill blocking

### Stream Compaction

- Confirmations auto-deduplicate
- Only latest per subject retained
- Log-structured storage

### Leader/Standby HA

- PostgreSQL advisory locks for coordination
- Automatic failover
- Exactly-once processing

---

## Concurrency Patterns

### CoordinationPrimitive

**File:** `crate/lib/sinex-node-sdk/src/coordination.rs`

Custom lock-free synchronization abstraction unifying:

- Event counting (like a semaphore)
- Boolean signaling (like an event)
- Barrier synchronization
- Progress tracking

```rust
pub struct CoordinationPrimitive {
    state: AtomicUsize,
    notify: Arc<Notify>,
    threshold: usize,
    generation: AtomicUsize,  // Prevents ABA problem in barrier reuse
    reset_behavior: ResetBehavior,
}

impl CoordinationPrimitive {
    pub fn add(&self, delta: usize) {
        let new_state = self.state.fetch_add(delta, Ordering::AcqRel) + delta;
        self.check_threshold_and_notify(new_state);
    }

    pub async fn wait_for(&self, value: usize, timeout: Duration) -> bool {
        let initial_generation = self.generation.load(Ordering::Acquire);
        let deadline = Instant::now() + timeout;

        loop {
            let current = self.state.load(Ordering::Acquire);
            let current_gen = self.generation.load(Ordering::Acquire);

            // Check if condition met OR generation changed (barrier opened)
            if current >= value || current_gen > initial_generation {
                return true;
            }

            match tokio::time::timeout_at(deadline.into(), self.notify.notified()).await {
                Ok(_) => continue,
                Err(_) => return false,
            }
        }
    }
}
```

**Strengths:**

- Lock-free atomic operations (AtomicUsize + tokio::sync::Notify)
- Generation counter prevents ABA problem in barrier reuse
- Timeout-based waiting (no indefinite hangs)
- Flexible reset behavior (Manual, Automatic, Never)

### Lock Selection Patterns

**Lock Inventory:**

| Lock Type | Use Case | Location |
|-----------|----------|----------|
| `std::sync::Mutex` | Simple, blocking, infrequent access | ServiceStatus |
| `tokio::sync::RwLock` | Async hot paths, read-heavy | WorkTracker, Assembler state |
| `parking_lot::Mutex` | Fast uncontended, no poisoning | Heartbeat metrics |
| `AtomicUsize` | Lock-free counters | In-flight operations, events processed |

**Pattern:** Right tool for the job - std::Mutex for simplicity, tokio::RwLock for async + read-heavy, parking_lot::Mutex for hot paths, atomics for counters.

### Spawn Management

**Background Task Pattern:**

```rust
let handle = tokio::spawn(async move {
    let mut interval = tokio::time::interval(Duration::from_secs(interval_seconds));
    loop {
        interval.tick().await;
        // Emit heartbeat metrics
    }
});
self.heartbeat_handle = Some(handle);
```

**Cleanup Pattern:**

```rust
tokio::select! {
    result = &mut begin_handle => {
        slices_handle.abort();
        end_handle.abort();
        return handle_task_exit("begin consumer", result);
    }
    // ...
}
```

---

## UUIDv7 Infrastructure

### UUIDv7 as Distributed ID System

```
 01AN4Z07BY      79KA1307SR9X4MV3
|----------|    |----------------|
 Timestamp          Randomness
   48bits              80bits
```

**Properties:**

- Time-ordered (lexicographically sortable)
- Globally unique (128-bit, cryptographically random)
- Compact (26 chars vs UUID's 36)
- PostgreSQL native support via `pg_uuidv7` extension
- Embeds creation timestamp (perfect for TimescaleDB partitioning)

**Multi-Level Generation:**

| Level | Usage | Benefits |
|-------|-------|----------|
| Database | `id UUIDv7 PRIMARY KEY DEFAULT uuidv7()` | Consistent timestamp source |
| Application | `let event_id = Uuid::new();` | When ID needed before insert |
| NATS | `headers.insert("Nats-Msg-Id", event_id)` | Deduplication |

**Type-Safe Wrappers:**

```rust
pub struct Event<T>;
pub type EventId = Id<Event<JsonValue>>;

pub struct SourceMaterial;
pub type SourceMaterialId = Id<SourceMaterial>;

// Compile-time prevention of mixing ID types
fn process_event(event_id: Id<Event>) { /* ... */ }
```

### Leader/Standby Coordination

```
┌──────────────────────────────────────────────┐
│         Postgres Advisory Locks              │
│  ┌─────────────┐  ┌─────────────┐          │
│  │ Lock: fs-01 │  │ Lock: fs-02 │          │
│  └─────────────┘  └─────────────┘          │
└──────────────────────────────────────────────┘
         ↑                  ↑
         │ Acquire          │ Attempt
         │ SUCCESS          │ BLOCKED
    ┌────────────┐    ┌────────────┐
    │ Instance A │    │ Instance B │
    │  (LEADER)  │    │ (STANDBY)  │
    └────────────┘    └────────────┘
```

**State Machine:**

```
Startup → Standby ⇄ Transitioning → Leader
                         ↓
                    Draining (graceful shutdown)
```

**Advantages:**

- Automatic cleanup (lock released on connection drop)
- Fast (in-memory locks)
- No separate coordination service (etcd, Zookeeper, Consul)
- Exactly-once leadership guarantee

---

## Operational Patterns

### Idempotency (Three-Layer Defense)

#### Layer 1: NATS Message Deduplication

```rust
let msg_id = format!("{}:{}", node_id, event.id);
headers.insert("Nats-Msg-Id", msg_id);
```

JetStream maintains a deduplication window (default 2 minutes).

#### Layer 2: Database-Level Idempotency

```rust
builder.push(" ON CONFLICT (id) DO NOTHING RETURNING id::uuid");
```

Duplicate UUIDv7 insertions silently ignored, not errored.

#### Layer 3: Confirmation Stream Compaction

```rust
StreamConfig {
    max_msgs_per_subject: 1,  // Compacts to latest confirmation
}
```

Prevents automata from seeing duplicate confirmations.

**Why Exemplary:** Defense in depth achieves exactly-once semantics without distributed transactions.

### Backpressure (Four-Layer Strategy)

| Layer | Mechanism | Config |
|-------|-----------|--------|
| Gateway | ConcurrencyLimit, Timeout, RateLimit | 100 concurrent, 30s timeout, 100/s |
| JetStream | max_ack_pending, ack_wait, max_deliver | Flow control, 30s ack, 10 retries |
| Database | max_connections, connect_timeout | 10 connections, 30s timeout |
| Channels | mpsc::channel(100) | Bounded internal queues |

**Why Well-Designed:** Backpressure propagates from database all the way to gateway.

### Graceful Shutdown

**Signal Handling:**

```rust
let mut sigterm = signal(SignalKind::terminate())?;
let mut sigint = signal(SignalKind::interrupt())?;

tokio::select! {
    _ = sigterm.recv() => { /* shutdown */ }
    _ = sigint.recv() => { /* shutdown */ }
}
```

**Shutdown Sequence:**

1. Signal received
2. Cancellation token triggered
3. In-flight messages completed (or NAK'd for redelivery)
4. Checkpoint saved to NATS KV
5. Connections closed

**Why Well-Designed:**

- No busy polling (100% channel-driven)
- Clean checkpoint saves before exit
- NATS redelivery handles interrupted batches

### Configuration Precedence

```rust
Figment::new()
    .merge(Toml::file("config.toml"))       // 1. Config file (lowest)
    .merge(Env::prefixed("SINEX_"))         // 2. Environment variables
    .merge(Serialized::defaults(&cli_args)) // 3. CLI args (highest)
```

| Service | Prefix | Example |
|---------|--------|---------|
| Gateway | `SINEX_` | `SINEX_RPC_PORT` |
| Ingestd | `SINEX_INGESTD_` | `SINEX_INGESTD_BATCH_SIZE` |
| Nodes | `SINEX_<SERVICE>_` | `SINEX_FS_WATCHER_LOG_LEVEL` |

---

## Critical Path: Ingestion Hot Path

```
NATS JetStream
    │ pull_batch(100)
    ↓
┌─────────────────────┐
│   process_batch()   │
│   ├── Deserialize   │
│   ├── Validate      │
│   ├── Parse UUIDv7    │
│   └── Build batch   │
└─────────────────────┘
    │
    ↓
┌─────────────────────────────┐
│ persist_batch_optimized()   │
│ └── Multi-row INSERT        │
│     ON CONFLICT DO NOTHING  │
└─────────────────────────────┘
    │ AFTER commit
    ↓
┌─────────────────────────────┐
│ publish_confirmations()     │
│ └── To sinex.events.{id}    │
└─────────────────────────────┘
    │
    ↓
┌─────────────────────┐
│      ack_all()      │
└─────────────────────┘
```

**Critical Invariant:** Confirmations published AFTER commit, ACKs AFTER confirmations.

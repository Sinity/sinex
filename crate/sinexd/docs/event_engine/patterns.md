# EventEngine Patterns

> Maintained event-engine-side pattern note for event sourcing, idempotency, and backpressure.

## Event Sourcing Architecture

**Key Patterns:**

### 1. Immutable Event Log
- All events immutable and retained (90 days)
- UUIDv7 primary keys (time-ordered, distributed-safe)
- Full operational history for replay
- TimescaleDB hypertable for time-series optimization

### 2. Raw/Confirmed Delivery Model
```
RuntimeModule Capture
    ↓ (stage material, emit raw event intent)
NATS JetStream events.raw.{source}.{type}
    ↓ (Nats-Msg-Id for idempotency)
EventEngine JetStreamConsumer
    ├─→ Validate Event
    ├─→ Persist to Postgres (TimescaleDB)
    ├─→ Publish confirmed event → events.confirmed.{provenance}.{source}.{type}
    └─→ On Error → DLQ events.dlq.event_engine
         ↓ (confirmed events only)
Automata (search, analytics, health)
```

### 3. Bounded Confirmed-Events Bus
- Confirmed-events stream carries the full post-redaction `Event<JsonValue>`
- The stream uses Limits retention with `discard: Old`
- PostgreSQL is the archive; consumers that fall past the tail catch up from DB

## Idempotency Patterns

Three-layer defense achieving exactly-once semantics:

### Layer 1: NATS Message Deduplication
```rust
let msg_id = format!("{}:{}", producer_id, event.id);
headers.insert("Nats-Msg-Id", msg_id);
```

### Layer 2: Database-Level Idempotency
```rust
builder.push(" ON CONFLICT (id) DO NOTHING RETURNING id::uuid");
```

### Layer 3: Confirmed-Event Publish Gate

The event engine ACKs the raw JetStream message only after the confirmed-event
publish succeeds. If confirmed-event publish retries are exhausted after the DB
commit, the consumer stops and leaves the raw message unacked for redelivery.

## Backpressure Mechanisms

Four-layer coordination:

1. **Gateway Layer**: Concurrency limit (100), timeout (30s), rate limit (100/s)
2. **JetStream Consumer**: `max_ack_pending`, ack_wait, max_deliver
3. **Database Pool**: max_connections (10), connect_timeout (30s)
4. **Internal Channel Bounds**: `mpsc::channel(100)`

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
│ publish_confirmed_event()   │
│ └── To events.confirmed.*   │
└─────────────────────────────┘
    │
    ↓
┌─────────────────────┐
│      ack_all()      │
└─────────────────────┘
```

**Critical Invariant:** confirmed events are published AFTER commit, raw ACKs AFTER confirmed-event publish.

## Provenance Enforcement

XOR constraint: every event has EITHER material OR derived provenance:

```rust
fn validate_provenance(raw_event: &RawEvent) -> Result<PreparedProvenance> {
    match (&raw_event.material_id, &raw_event.source_event_ids) {
        (Some(material_id), None) => Ok(PreparedProvenance::Material { ... }),
        (None, Some(source_ids)) if !source_ids.is_empty() => Ok(PreparedProvenance::Derived { ... }),
        _ => Err(SinexError::validation("Event must have exactly one of: material_id XOR source_event_ids")),
    }
}
```

## See Also

- Pipeline design: [pipeline-design.md](./pipeline-design.md)
- Architecture: [architecture.md](./architecture.md)

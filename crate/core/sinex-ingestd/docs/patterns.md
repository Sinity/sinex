# Ingestd Patterns

> Extracted from `docs/current/architecture/advanced-implementation-patterns.md` (Parts 1.3, 16)

## Event Sourcing Architecture

**Key Patterns:**

### 1. Immutable Event Log
- All events immutable and retained (90 days)
- ULID primary keys (time-ordered, distributed-safe)
- Full operational history for replay
- TimescaleDB hypertable for time-series optimization

### 2. Provisional/Confirmed Model (Saga Pattern)
```
Node Capture
    ↓ (stage material, emit provisional)
NATS JetStream events.raw.{source}.{type}
    ↓ (Nats-Msg-Id for idempotency)
Ingestd JetStreamConsumer
    ├─→ Validate Event
    ├─→ Persist to Postgres (TimescaleDB)
    ├─→ Publish Confirmation → events.confirmations.{event_id}
    └─→ On Error → DLQ events.dlq.ingestd
         ↓ (confirmed events only)
Automata (search, analytics, health)
```

### 3. Stream Compaction for Confirmations
- Confirmations stream uses `max_messages_per_subject: 1`
- Only latest confirmation per event retained
- Self-cleaning confirmation architecture

## Idempotency Patterns

Three-layer defense achieving exactly-once semantics:

### Layer 1: NATS Message Deduplication
```rust
let msg_id = format!("{}:{}", node_id, event.id);
headers.insert("Nats-Msg-Id", msg_id);
```

### Layer 2: Database-Level Idempotency
```rust
builder.push(" ON CONFLICT (id) DO NOTHING RETURNING id::uuid");
```

### Layer 3: Confirmation Stream Compaction
```rust
StreamConfig {
    max_msgs_per_subject: 1,  // Compacts to latest
    ...
}
```

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
│   ├── Parse ULID    │
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

## Provenance Enforcement

XOR constraint: every event has EITHER material OR synthesis provenance:

```rust
fn validate_provenance(raw_event: &RawEvent) -> Result<PreparedProvenance> {
    match (&raw_event.material_id, &raw_event.source_event_ids) {
        (Some(material_id), None) => Ok(PreparedProvenance::Material { ... }),
        (None, Some(source_ids)) if !source_ids.is_empty() => Ok(PreparedProvenance::Synthesis { ... }),
        _ => Err(SinexError::validation("Event must have exactly one of: material_id XOR source_event_ids")),
    }
}
```

## See Also

- Full patterns analysis: `docs/current/architecture/advanced-implementation-patterns.md`
- Pipeline design: [pipeline-design.md](./pipeline-design.md)
- Architecture: [architecture.md](./architecture.md)

# sinexd Event Engine Architecture Diagrams

> Note: these diagrams are conceptual. For exact stream/consumer settings,
> use `crate/sinexd/src/event_engine/jetstream_consumer.rs`.

## Event Sourcing & CQRS Flow

### Provisional/Confirmed Model (Saga Pattern)

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                        EVENT SOURCING ARCHITECTURE                            │
│                      Provisional → Confirmed Pipeline                         │
└──────────────────────────────────────────────────────────────────────────────┘

PHASE 1: CAPTURE (RuntimeModule)
────────────────────────────
  ┌──────────────────────────┐
  │   RuntimeModule detects           │
  │   filesystem change      │
  └────────────┬─────────────┘
               │
               ↓
  ┌──────────────────────────┐
  │ Stage source material     │
  │ (copy to content store)   │
  └────────────┬─────────────┘
               │
               ↓
  ┌──────────────────────────┐
  │ Emit PROVISIONAL event    │
  │ - event_id: UUIDv7          │
  │ - source_material_id      │
  │ - payload: {path, size}   │
  │ - provenance: Material    │
  └────────────┬─────────────┘
               │
               │ Publish to NATS
               ↓
═══════════════════════════════════════════════════════════════════════════════

PHASE 2: TRANSPORT (NATS JetStream)
────────────────────────────────────
  events.raw.filesystem.file_created
               │
               │ Headers:
               │ - Nats-Msg-Id: <event_id>  (idempotency)
               │ - Nats-Expected-Stream: events.raw
               │
               ↓
  ┌──────────────────────────────────────────────┐
  │  JetStream Guarantees:                       │
  │  - Exactly-once delivery (deduplication)     │
  │  - Persistent storage (file/memory)          │
  │  - Stream retention (by time/size/messages)  │
  │  - Consumer acknowledgment required          │
  └──────────────────────────────────────────────┘
               │
               ↓
═══════════════════════════════════════════════════════════════════════════════

PHASE 3: INGESTION (`sinexd::event_engine`)
───────────────────────────────────
  ┌──────────────────────────┐
  │ JetStreamConsumer        │
  │ - Batch fetch (50 msgs)  │
  │ - Ack timeout: 30s       │
  └────────────┬─────────────┘
               │
               ↓
  ┌──────────────────────────┐
  │ Validation               │
  │ - Schema validation      │
  │ - Security checks        │
  │ - Payload size limit     │
  └────────────┬─────────────┘
               │
               ├─────────────────────────┐
               │                         │
               ↓                         ↓ (validation failure)
  ┌──────────────────────────┐  ┌─────────────────────────┐
  │ Persist to Postgres       │  │ Route to DLQ            │
  │ - INSERT INTO core.events │  │ events.dlq.event_engine │
  │ - ts_coided = NOW()       │  │ - Original message      │
  │ - RETURNING *             │  │ - Error details         │
  └────────────┬─────────────┘  │ - Retry count           │
               │                 └─────────────────────────┘
               ↓
  ┌──────────────────────────┐
  │ Publish CONFIRMED EVENT   │
  │ events.confirmed.         │
  │   {prov}.{source}.{type}  │
  │                           │
  │ Payload:                  │
  │ - full Event<JsonValue>   │
  │ - persisted/redacted body │
  │ - event_id header         │
  └────────────┬─────────────┘
               │
               ↓
  ┌──────────────────────────┐
  │ ACK to NATS               │
  │ (message processed)       │
  └──────────────────────────┘
               │
               ↓
═══════════════════════════════════════════════════════════════════════════════

PHASE 4: CONSUMPTION (Automata)
────────────────────────────────
  ┌──────────────────────────────────────┐
  │ Automata subscribe to:               │
  │   events.confirmed.>                 │
  │                                       │
  │ Why? Ensures atomicity:               │
  │ - Event in DB → confirmed event sent │
  │ - No confirmed event → raw not ACKed │
  └────────────┬─────────────────────────┘
               │
               ↓
  ┌──────────────────────────┐
  │ search-automata           │
  │ - Index for FTS           │
  │ - Checkpoint: event_id    │
  └────────────┬─────────────┘
               │
  ┌────────────┼─────────────┐
  │ analytics-automata        │
  │ - Aggregate metrics       │
  │ - Checkpoint: event_id    │
  └────────────┬─────────────┘
               │
  ┌────────────┼─────────────┐
  │ health-aggregator         │
  │ - Service health          │
  │ - Checkpoint: event_id    │
  └───────────────────────────┘
```

## NATS JetStream Topology

### Stream Architecture

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                         NATS JETSTREAM TOPOLOGY                               │
│                    3 Streams + Key-Value Buckets                              │
└──────────────────────────────────────────────────────────────────────────────┘

STREAM 1: events.raw (Raw Events from Sources)
═══════════════════════════════════════════════════════════════════════════════

  Subjects: events.raw.{source}.{event_type}
  Examples:
    - events.raw.filesystem.file_created
    - events.raw.shell.kitty.command_executed
    - events.raw.desktop.hyprland.window_focus_changed

  ┌─────────────────────────────────────────────────────────────────────┐
  │  Stream: events.raw                                                  │
  │  ┌─────────────────────────────────────────────────────────────────┐│
  │  │ Configuration:                                                   ││
  │  │ - Subjects: events.raw.>                                         ││
  │  │ - Storage: File                                                  ││
  │  │ - Retention: Limits                                               ││
  │  │ - Max Age: 90 days                                                ││
  │  │ - Max Msgs: 10,000,000                                           ││
  │  │ - Max Bytes: (not explicitly limited here)                        ││
  │  │ - Replicas: 1 (single NATS server)                               ││
  │  │ - Discard: Old (FIFO when full)                                  ││
  │  └─────────────────────────────────────────────────────────────────┘│
  │                                                                       │
  │  Consumers:                                                           │
  │  ┌────────────────────────────────────────────┐                     │
  │  │ Consumer: event-engine-begin                    │                     │
  │  │ - Filter: events.raw.>.material_begin      │                     │
  │  │ - Batch: configurable (default 100)       │                     │
  │  │ - Ack Wait: 30s                            │                     │
  │  │ - Max Deliver: 3                           │                     │
  │  └────────────────────────────────────────────┘                     │
  │  ┌────────────────────────────────────────────┐                     │
  │  │ Consumer: event-engine-slices                   │                     │
  │  │ - Filter: events.raw.>.material_slice      │                     │
  │  │ - Batch: configurable (default 100)        │                     │
  │  │ - Ack Wait: 30s                            │                     │
  │  └────────────────────────────────────────────┘                     │
  │  ┌────────────────────────────────────────────┐                     │
  │  │ Consumer: event-engine-end                      │                     │
  │  │ - Filter: events.raw.>.material_end        │                     │
  │  │ - Batch: configurable (default 100)       │                     │
  │  └────────────────────────────────────────────┘                     │
  └───────────────────────────────────────────────────────────────────────┘

═══════════════════════════════════════════════════════════════════════════════

STREAM 2: events.confirmed (Confirmed Event Delivery)
═══════════════════════════════════════════════════════════════════════════════

  Subjects: events.confirmed.{provenance}.{source}.{event_type}

  ┌─────────────────────────────────────────────────────────────────────┐
  │  Stream: events.confirmed                                            │
  │  ┌─────────────────────────────────────────────────────────────────┐│
  │  │ Configuration:                                                   ││
  │  │ - Subjects: events.confirmed.>                                   ││
  │  │ - Storage: File                                                  ││
  │  │ - Retention: Limits                                              ││
  │  │ - Max Age: 3 days                                                 ││
  │  │ - Replicas: 1                                                    ││
  │  │ - Discard: Old (bounded delivery bus; DB is archive)             ││
  │  └─────────────────────────────────────────────────────────────────┘│
  │                                                                       │
  │  Payload:                                                             │
  │    Full post-redaction Event<JsonValue> exactly as persisted          │
  │    No provisional buffer, watermark subject, or DB refetch            │
  │                                                                       │
  │  Consumers:                                                           │
  │  ┌────────────────────────────────────────────┐                     │
  │  │ Consumer: search-automata                  │                     │
  │  │ - Filter: events.confirmed.>               │                     │
  │  │ - Deliver: New + DB catch-up on startup    │                     │
  │  │ - Ack Wait: 60s                            │                     │
  │  └────────────────────────────────────────────┘                     │
  │  ┌────────────────────────────────────────────┐                     │
  │  │ Consumer: analytics-automata               │                     │
  │  │ - Filter: events.confirmed.>               │                     │
  │  └────────────────────────────────────────────┘                     │
  │  ┌────────────────────────────────────────────┐                     │
  │  │ Consumer: health-aggregator                │                     │
  │  │ - Filter: events.confirmed.>               │                     │
  │  └────────────────────────────────────────────┘                     │
  └───────────────────────────────────────────────────────────────────────┘

═══════════════════════════════════════════════════════════════════════════════

STREAM 3: events.dlq (Raw-Ingest Dead Letter Queue)
═══════════════════════════════════════════════════════════════════════════════

  Subjects: events.dlq.{component}

  ┌─────────────────────────────────────────────────────────────────────┐
  │  Stream: events.dlq                                                  │
  │  ┌─────────────────────────────────────────────────────────────────┐│
  │  │ Configuration:                                                   ││
  │  │ - Subjects: events.dlq.>                                         ││
  │  │ - Storage: File                                                  ││
  │  │ - Retention: Limits                                              ││
  │  │ - Max Age: 30 days                                               ││
  │  │ - Max Msgs: 1,000,000                                            ││
  │  │ - Replicas: 1                                                    ││
  │  └─────────────────────────────────────────────────────────────────┘│
  │                                                                       │
  │  Message Format:                                                      │
  │  {                                                                    │
  │    "original_message": { ... },                                       │
  │    "error": "Validation failed: payload too large",                   │
  │    "component": "event_engine",                                            │
  │    "timestamp": "2025-01-15T12:00:00Z",                               │
  │    "retry_count": 3,                                                  │
  │    "metadata": { ... }                                                │
  │  }                                                                    │
  │                                                                       │
  │  Manual Intervention:                                                 │
  │    Ops team reviews DLQ, fixes issues, replays if needed              │
  └───────────────────────────────────────────────────────────────────────┘

═══════════════════════════════════════════════════════════════════════════════

KEY-VALUE BUCKETS
═══════════════════════════════════════════════════════════════════════════════

  Bucket: sinex_checkpoints
  ┌─────────────────────────────────────────────────────────────────────┐
  │  Key format: {module_name}.{consumer_group}.{consumer_name}       │
  │  Value: JSON checkpoint state                                        │
  │                                                                       │
  │  {                                                                    │
  │    "checkpoint": {                                                    │
  │      "Internal": {                                                    │
  │        "event_id": "01HK...",                                         │
  │        "message_count": 12345                                         │
  │      }                                                                 │
  │    },                                                                 │
  │    "processed_count": 12345,                                          │
  │    "last_activity": "2025-01-15T12:00:00Z",                           │
  │    "version": 2                                                       │
  │  }                                                                    │
  │                                                                       │
  │  Operations:                                                          │
  │  - Atomic per-key updates                                             │
  │  - Last write wins                                                    │
  │  - Used for crash recovery                                            │
  └───────────────────────────────────────────────────────────────────────┘

  Bucket: sinex_locks (Advisory Locks)
  ┌─────────────────────────────────────────────────────────────────────┐
  │  Key format: lock.{service_name}                                      │
  │  Value: {instance_id, acquired_at, ttl}                               │
  │                                                                       │
  │  Used for leader election (alternative to Postgres advisory locks)    │
  └───────────────────────────────────────────────────────────────────────┘
```

## Error Scenarios

```
═══════════════════════════════════════════════════════════════════════════════

ROLLBACK / ERROR SCENARIOS
═══════════════════════════════════════════════════════════════════════════════

Scenario 1: Validation Failure
  Provisional Event → DLQ
  No confirmation sent
  Automata never see it ✓

Scenario 2: DB Insert Failure
  Provisional Event → DB insert fails → Exception
  No ACK to NATS → Message redelivered ✓
  No confirmation sent → Automata don't process ✓

Scenario 3: Confirmation Publish Failure
  Event persisted → Confirmation publish fails
  ACK not sent → NATS redelivers
  Idempotency: event_id already exists (CONFLICT)
  Retry publishes confirmation ✓

Scenario 4: Duplicate Message (Idempotency)
  Same event_id arrives twice
  DB: UNIQUE constraint on event_id
  INSERT fails with CONFLICT
  Check if event exists → Yes → Publish confirmation again
  ACK message ✓
```

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

Critical Invariant: confirmed events published AFTER commit, raw ACKs AFTER confirmed publish.
```

## See Also

- Patterns: [patterns.md](./patterns.md)
- Architecture: [architecture.md](./architecture.md)
- Database diagrams: `crate/sinex-db/docs/diagrams.md`

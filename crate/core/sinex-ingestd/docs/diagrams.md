# Ingestd Architecture Diagrams

> Note: these diagrams are conceptual. For exact stream/consumer settings,
> use `crate/core/sinex-ingestd/src/jetstream_consumer.rs`.

## Event Sourcing & CQRS Flow

### Provisional/Confirmed Model (Saga Pattern)

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                        EVENT SOURCING ARCHITECTURE                            │
│                      Provisional → Confirmed Pipeline                         │
└──────────────────────────────────────────────────────────────────────────────┘

PHASE 1: CAPTURE (Node)
────────────────────────────
  ┌──────────────────────────┐
  │   Node detects           │
  │   filesystem change      │
  └────────────┬─────────────┘
               │
               ↓
  ┌──────────────────────────┐
  │ Stage source material     │
  │ (copy to annex)           │
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

PHASE 3: INGESTION (sinex-ingestd)
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
  │ - INSERT INTO core.events │  │ events.dlq.ingestd      │
  │ - ts_ingest = NOW()       │  │ - Original message      │
  │ - RETURNING *             │  │ - Error details         │
  └────────────┬─────────────┘  │ - Retry count           │
               │                 └─────────────────────────┘
               ↓
  ┌──────────────────────────┐
  │ Publish CONFIRMATION      │
  │ events.confirmations.     │
  │   {event_id}              │
  │                           │
  │ Payload:                  │
  │ - event_id                │
  │ - confirmed_at            │
  │ - ingestor_version        │
  │ - db_insert_duration_ms   │
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
  │ Automata subscribe ONLY to:          │
  │   events.confirmations.>             │
  │                                       │
  │ Why? Ensures atomicity:               │
  │ - Event in DB → Confirmation sent    │
  │ - No confirmation → Event not in DB  │
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

STREAM 1: events.raw (Raw Events from Nodes)
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
  │  │ - Replicas: 1 (single-node)                                      ││
  │  │ - Discard: Old (FIFO when full)                                  ││
  │  └─────────────────────────────────────────────────────────────────┘│
  │                                                                       │
  │  Consumers:                                                           │
  │  ┌────────────────────────────────────────────┐                     │
  │  │ Consumer: ingestd-begin                    │                     │
  │  │ - Filter: events.raw.>.material_begin      │                     │
  │  │ - Batch: configurable (default 100)       │                     │
  │  │ - Ack Wait: 30s                            │                     │
  │  │ - Max Deliver: 3                           │                     │
  │  └────────────────────────────────────────────┘                     │
  │  ┌────────────────────────────────────────────┐                     │
  │  │ Consumer: ingestd-slices                   │                     │
  │  │ - Filter: events.raw.>.material_slice      │                     │
  │  │ - Batch: configurable (default 100)        │                     │
  │  │ - Ack Wait: 30s                            │                     │
  │  └────────────────────────────────────────────┘                     │
  │  ┌────────────────────────────────────────────┐                     │
  │  │ Consumer: ingestd-end                      │                     │
  │  │ - Filter: events.raw.>.material_end        │                     │
  │  │ - Batch: configurable (default 100)       │                     │
  │  └────────────────────────────────────────────┘                     │
  └───────────────────────────────────────────────────────────────────────┘

═══════════════════════════════════════════════════════════════════════════════

STREAM 2: events.confirmations (Event Persistence Confirmations)
═══════════════════════════════════════════════════════════════════════════════

  Subjects: events.confirmations.{event_id}

  ┌─────────────────────────────────────────────────────────────────────┐
  │  Stream: events.confirmations                                        │
  │  ┌─────────────────────────────────────────────────────────────────┐│
  │  │ Configuration:                                                   ││
  │  │ - Subjects: events.confirmations.>                               ││
  │  │ - Storage: File                                                  ││
  │  │ - Retention: Limits (not WorkQueue!)                             ││
  │  │ - Max Age: 7 days                                                 ││
  │  │ - Max Msgs Per Subject: 1  ← STREAM COMPACTION                  ││
  │  │ - Replicas: 1                                                    ││
  │  │ - Discard: New (keep latest per event_id)                        ││
  │  └─────────────────────────────────────────────────────────────────┘│
  │                                                                       │
  │  Stream Compaction:                                                   │
  │    Only latest confirmation per event_id retained                     │
  │    Old confirmations auto-deleted                                     │
  │    Self-cleaning architecture ✓                                       │
  │                                                                       │
  │  Consumers:                                                           │
  │  ┌────────────────────────────────────────────┐                     │
  │  │ Consumer: search-automata                  │                     │
  │  │ - Filter: events.confirmations.>           │                     │
  │  │ - Deliver: All (replay from beginning)     │                     │
  │  │ - Ack Wait: 60s                            │                     │
  │  └────────────────────────────────────────────┘                     │
  │  ┌────────────────────────────────────────────┐                     │
  │  │ Consumer: analytics-automata               │                     │
  │  │ - Filter: events.confirmations.>           │                     │
  │  └────────────────────────────────────────────┘                     │
  │  ┌────────────────────────────────────────────┐                     │
  │  │ Consumer: health-aggregator                │                     │
  │  │ - Filter: events.confirmations.>           │                     │
  │  └────────────────────────────────────────────┘                     │
  └───────────────────────────────────────────────────────────────────────┘

═══════════════════════════════════════════════════════════════════════════════

STREAM 3: events.dlq (Dead Letter Queue)
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
  │    "component": "ingestd",                                            │
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
  │  Key format: {node_name}.{consumer_group}.{consumer_name}       │
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
│ publish_confirmations()     │
│ └── To events.confirmations.{id} │
└─────────────────────────────┘
    │
    ↓
┌─────────────────────┐
│      ack_all()      │
└─────────────────────┘

Critical Invariant: Confirmations published AFTER commit, ACKs AFTER confirmations.
```

## See Also

- Patterns: [patterns.md](./patterns.md)
- Architecture: [architecture.md](./architecture.md)
- Database diagrams: `crate/lib/sinex-db/docs/diagrams.md`

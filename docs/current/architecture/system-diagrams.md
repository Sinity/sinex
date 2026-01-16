Status: canonical
Last Verified: 2025-01-15 (moved from exploration)
> **Purpose:** Comprehensive visual reference with ASCII art diagrams for all major Sinex architectural components. Living document - update as architecture evolves.

# System Architecture Diagrams

---

## Table of Contents

1. [Overall System Architecture](#1-overall-system-architecture)
2. [Event Sourcing & CQRS Flow](#2-event-sourcing--cqrs-flow)
3. [NATS JetStream Topology](#3-nats-jetstream-topology)
4. [Leader/Standby Coordination](#4-leaderstandby-coordination)
5. [Monitoring & Observability](#5-monitoring--observability)
6. [Database Architecture](#6-database-architecture)
7. [Testing Infrastructure](#7-testing-infrastructure)
8. [Type System Architecture](#8-type-system-architecture)
9. [Concurrency Patterns](#9-concurrency-patterns)
10. [Checkpoint System](#10-checkpoint-system)

---

## 1. Overall System Architecture

### High-Level Component View

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           SINEX ARCHITECTURE                                  │
│                        Event-Sourced Observability                            │
└─────────────────────────────────────────────────────────────────────────────┘

┌───────────────────────┐         ┌──────────────────────────────────────────┐
│   CAPTURE LAYER       │         │         INGESTION LAYER                  │
│   (nodes)        │         │                                          │
│                       │         │  ┌────────────────────────────────────┐  │
│  ┌─────────────────┐ │         │  │     sinex-ingestd                  │  │
│  │  fs-watcher     │ ├────────▶│  │  ┌──────────────────────────────┐  │  │
│  │  (inotify)      │ │  NATS   │  │  │  MaterialAssembler           │  │  │
│  └─────────────────┘ │  Events │  │  │  - Begin/Slices/End          │  │  │
│                       │         │  │  │  - State machine             │  │  │
│  ┌─────────────────┐ │         │  │  │  - Temp file assembly        │  │  │
│  │  terminal-sat   │ ├────────▶│  │  └──────────────────────────────┘  │  │
│  │  (kitty/fish)   │ │         │  │                                      │  │
│  └─────────────────┘ │         │  │  ┌──────────────────────────────┐  │  │
│                       │         │  │  │  JetStreamConsumer           │  │  │
│  ┌─────────────────┐ │         │  │  │  - Batch processing          │  │  │
│  │  desktop-sat    │ ├────────▶│  │  │  - Idempotency (Nats-Msg-Id) │  │  │
│  │  (hyprland)     │ │         │  │  │  - DLQ routing               │  │  │
│  └─────────────────┘ │         │  │  └──────────────────────────────┘  │  │
│                       │         │  │                                      │  │
│  ┌─────────────────┐ │         │  │  ┌──────────────────────────────┐  │  │
│  │  system-sat     │ ├────────▶│  │  │  Repository Layer            │  │  │
│  │  (metrics)      │ │         │  │  │  - EventRepository           │  │  │
│  └─────────────────┘ │         │  │  │  - SourceMaterialRepository  │  │  │
│                       │         │  │  │  - BlobRepository            │  │  │
│  ┌─────────────────┐ │         │  │  └──────────────────────────────┘  │  │
│  │  journald-sat   │ ├────────▶│  │                    ↓                 │  │
│  │  (systemd logs) │ │         │  └────────────────────┼─────────────────┘  │
│  └─────────────────┘ │         │                       ↓                    │
└───────────────────────┘         └───────────────────────┼────────────────────┘
                                                          ↓
        ┌─────────────────────────────────────────────────┼─────────────────┐
        │                    PERSISTENCE LAYER            ↓                 │
        │                                                                    │
        │  ┌──────────────────────────────────────────────────────────────┐│
        │  │           PostgreSQL + TimescaleDB                           ││
        │  │                                                               ││
        │  │  ┌────────────────┐  ┌────────────────┐  ┌────────────────┐││
        │  │  │ core.events    │  │ core.source_   │  │ core.blobs     │││
        │  │  │ (hypertable)   │  │   materials    │  │                │││
        │  │  │                │  │                │  │                │││
        │  │  │ Partitioned by │  │ Git Annex for  │  │ Large binary   │││
        │  │  │ ULID timestamp │  │ large files    │  │ storage        │││
        │  │  └────────────────┘  └────────────────┘  └────────────────┘││
        │  │                                                               ││
        │  │  Indexing: GIN (JSONB), BTREE (ts_ingest), GiST (temporal)  ││
        │  └──────────────────────────────────────────────────────────────┘│
        └─────────────────────────────────────────────────────────────────┘
                                        ↑
                                        │ Read Path
        ┌───────────────────────────────┼──────────────────────────────────┐
        │                 QUERY LAYER   │                                   │
        │                               │                                   │
        │  ┌─────────────────────────────────────────────────────────────┐ │
        │  │              sinex-gateway (RPC Server)                      │ │
        │  │                                                               │ │
        │  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐ │ │
        │  │  │ Auth Guard  │  │ Rate Limiter│  │ Query Router        │ │ │
        │  │  │ (Bearer)    │  │ (per-token) │  │ - EventQuery        │ │ │
        │  │  └─────────────┘  └─────────────┘  │ - MaterialQuery     │ │ │
        │  │                                     │ - HealthQuery       │ │ │
        │  │                                     └─────────────────────┘ │ │
        │  └─────────────────────────────────────────────────────────────┘ │
        └───────────────────────────────────────────────────────────────────┘
                                        ↓
        ┌───────────────────────────────┼──────────────────────────────────┐
        │              AUTOMATA LAYER   │   (Event Processors)              │
        │                               │                                   │
        │  ┌────────────────┐  ┌───────────────┐  ┌────────────────────┐  │
        │  │ search-automata│  │ analytics-    │  │ health-aggregator  │  │
        │  │ (FTS indexing) │  │  automata     │  │ (metrics)          │  │
        │  └────────────────┘  └───────────────┘  └────────────────────┘  │
        │                                                                   │
        │  All automata:                                                    │
        │  - Consume confirmed events                                       │
        │  - Maintain checkpoints (NATS KV)                                 │
        │  - Leader/standby HA (advisory locks)                             │
        │  - Graceful shutdown (WorkTracker)                                │
        └───────────────────────────────────────────────────────────────────┘
```

---

## 2. Event Sourcing & CQRS Flow

### Provisional/Confirmed Model (Saga Pattern)

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                        EVENT SOURCING ARCHITECTURE                            │
│                      Provisional → Confirmed Pipeline                         │
└──────────────────────────────────────────────────────────────────────────────┘

PHASE 1: CAPTURE (node)
────────────────────────────
  ┌──────────────────────────┐
  │   node detects       │
  │   filesystem change       │
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
  │ - event_id: ULID          │
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

═══════════════════════════════════════════════════════════════════════════════

ROLLBACK / ERROR SCENARIOS
───────────────────────────

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

---

## 3. NATS JetStream Topology

### Stream Architecture

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                         NATS JETSTREAM TOPOLOGY                               │
│                    3 Streams + Key-Value Buckets                              │
└──────────────────────────────────────────────────────────────────────────────┘

STREAM 1: events.raw (Raw Events from nodes)
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
  │  │ - Retention: WorkQueue (delete after consumption)                ││
  │  │ - Max Age: 7 days (safety buffer)                                ││
  │  │ - Max Msgs: 10,000,000                                           ││
  │  │ - Max Bytes: 10 GB                                               ││
  │  │ - Replicas: 1 (single-node)                                      ││
  │  │ - Discard: Old (FIFO when full)                                  ││
  │  └─────────────────────────────────────────────────────────────────┘│
  │                                                                       │
  │  Consumers:                                                           │
  │  ┌────────────────────────────────────────────┐                     │
  │  │ Consumer: ingestd-begin                    │                     │
  │  │ - Filter: events.raw.>.material_begin      │                     │
  │  │ - Batch: 50 messages                       │                     │
  │  │ - Ack Wait: 30s                            │                     │
  │  │ - Max Deliver: 3                           │                     │
  │  └────────────────────────────────────────────┘                     │
  │  ┌────────────────────────────────────────────┐                     │
  │  │ Consumer: ingestd-slices                   │                     │
  │  │ - Filter: events.raw.>.material_slice      │                     │
  │  │ - Batch: 200 messages                      │                     │
  │  │ - Ack Wait: 30s                            │                     │
  │  └────────────────────────────────────────────┘                     │
  │  ┌────────────────────────────────────────────┐                     │
  │  │ Consumer: ingestd-end                      │                     │
  │  │ - Filter: events.raw.>.material_end        │                     │
  │  │ - Batch: 50 messages                       │                     │
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
  │  │ - Max Age: 24 hours                                              ││
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
  │  │ - Max Msgs: 100,000                                              ││
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
  │  Key format: {processor_name}/{consumer_group}/{consumer_name}       │
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

---

## 4. Leader/Standby Coordination

### PostgreSQL Advisory Locks Architecture

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                      LEADER/STANDBY COORDINATION                              │
│              PostgreSQL Advisory Locks + WorkTracker                          │
└──────────────────────────────────────────────────────────────────────────────┘

COORDINATION INFRASTRUCTURE
═══════════════════════════════════════════════════════════════════════════════

  ┌─────────────────────────────────────────────────────────────────────┐
  │                     PostgreSQL Advisory Locks                        │
  │                                                                       │
  │  ┌───────────────────────────────────────────────────────────────┐  │
  │  │                     Lock Registry                              │  │
  │  │                                                                 │  │
  │  │  Lock ID: hash("fs-watcher-01")                                │  │
  │  │  ┌──────────────────┐                                          │  │
  │  │  │ Owner: conn_425  │  ← Instance A connection                 │  │
  │  │  │ Acquired: T0     │                                          │  │
  │  │  └──────────────────┘                                          │  │
  │  │                                                                 │  │
  │  │  Lock ID: hash("terminal-sat-01")                              │  │
  │  │  ┌──────────────────┐                                          │  │
  │  │  │ Owner: conn_531  │  ← Instance B connection                 │  │
  │  │  │ Acquired: T1     │                                          │  │
  │  │  └──────────────────┘                                          │  │
  │  │                                                                 │  │
  │  │  Guarantees:                                                    │  │
  │  │  - In-memory (fast)                                             │  │
  │  │  - Per-connection (auto-release on disconnect)                  │  │
  │  │  - Atomic acquire (pg_try_advisory_lock)                        │  │
  │  │  - No deadlock detection needed (non-blocking)                  │  │
  │  └───────────────────────────────────────────────────────────────┘  │
  └─────────────────────────────────────────────────────────────────────┘

INSTANCE STATE MACHINE
═══════════════════════════════════════════════════════════════════════════════

  ┌──────────┐
  │ Startup  │  Initial state on launch
  └────┬─────┘
       │
       │ Check for existing lock
       ↓
  ┌─────────────────┐
  │    Standby      │  Wait for leader to fail/release
  │                 │
  │ Actions:        │
  │ - Poll lock     │  Every 5s: SELECT pg_try_advisory_lock(id)
  │ - Monitor DB    │  Check for failure signals
  │ - Idle          │  No event processing
  └────┬─────┬──────┘
       │     ↑
       │     │ Lock acquisition failed
       │     │
       │ Lock acquired!
       ↓     │
  ┌─────────┴───────┐
  │ Transitioning   │  Brief state during handoff
  │                 │
  │ Actions:        │
  │ - Verify lock   │  Re-check lock ownership
  │ - Initialize    │  Set up consumers, load checkpoints
  └────┬────────────┘
       │
       │ Initialization complete
       ↓
  ┌──────────────────────────────────────────────────────────┐
  │                      Leader                               │
  │                                                           │
  │  ┌───────────────────────────────────────────────────┐   │
  │  │            Active Event Processing                 │   │
  │  │                                                     │   │
  │  │  ┌──────────────────────────────────────────────┐ │   │
  │  │  │ NATS Consumers                                │ │   │
  │  │  │ - Batch fetch events                          │ │   │
  │  │  │ - Process & persist                           │ │   │
  │  │  │ - ACK messages                                │ │   │
  │  │  └──────────────────────────────────────────────┘ │   │
  │  │                                                     │   │
  │  │  ┌──────────────────────────────────────────────┐ │   │
  │  │  │ WorkTracker                                   │ │   │
  │  │  │ - in_flight_operations: AtomicUsize           │ │   │
  │  │  │ - shutdown_requested: CoordinationPrimitive   │ │   │
  │  │  └──────────────────────────────────────────────┘ │   │
  │  │                                                     │   │
  │  │  ┌──────────────────────────────────────────────┐ │   │
  │  │  │ Heartbeat Emitter                             │ │   │
  │  │  │ - Emit every 60s to journald                  │ │   │
  │  │  │ - Status: Healthy/Degraded/Failed             │ │   │
  │  │  └──────────────────────────────────────────────┘ │   │
  │  └───────────────────────────────────────────────────┘   │
  │                                                           │
  │  Failure Detection:                                       │
  │  - DB connection lost → Lock auto-released                │
  │  - Process crash → Lock auto-released                     │
  │  - Signal (SIGTERM) → Graceful shutdown initiated         │
  └────────────┬──────────────────────────────────────────────┘
               │
               │ Graceful shutdown requested
               ↓
  ┌──────────────────────────────────────────────────────────┐
  │                     Draining                              │
  │                                                           │
  │  ┌───────────────────────────────────────────────────┐   │
  │  │ Shutdown Protocol:                                 │   │
  │  │                                                     │   │
  │  │ 1. request_shutdown()                              │   │
  │  │    - Set shutdown_requested flag                   │   │
  │  │    - Stop accepting new work                       │   │
  │  │                                                     │   │
  │  │ 2. Wait for in_flight_operations → 0              │   │
  │  │    - Timeout: 30 seconds                           │   │
  │  │    - Poll every 100ms                              │   │
  │  │                                                     │   │
  │  │ 3. Checkpoint state                                │   │
  │  │    - Save to NATS KV                               │   │
  │  │    - Flush pending writes                          │   │
  │  │                                                     │   │
  │  │ 4. Release advisory lock                           │   │
  │  │    - pg_advisory_unlock(lock_id)                   │   │
  │  │                                                     │   │
  │  │ 5. Close DB connection                             │   │
  │  │    - pool.close().await                            │   │
  │  └───────────────────────────────────────────────────┘   │
  └──────────────────────────────────────────────────────────┘
               │
               ↓
         ┌──────────┐
         │ Shutdown │  Process exits cleanly
         └──────────┘

SEQUENCE DIAGRAMS
═══════════════════════════════════════════════════════════════════════════════

Normal Operation (2 Instances)
───────────────────────────────

  Instance A          PostgreSQL           Instance B
      │                   │                     │
      │  Startup          │                     │  Startup
      │                   │                     │
      ├──try_lock()──────>│                     │
      │<─────OK───────────┤                     │
      │                   │                     │
      │  LEADER mode      │                     │
      │  Process events   │                     ├──try_lock()──────>
      │                   │                     │<─────FAIL─────────┤
      │                   │                     │
      │                   │                     │  STANDBY mode
      │                   │                     │  Sleep 5s
      │                   │                     │
      │                   │                     │  (retry loop)
      │                   │                     ├──try_lock()──────>
      │                   │                     │<─────FAIL─────────┤
      │                   │                     │

Leader Failure (Automatic Failover)
────────────────────────────────────

  Instance A          PostgreSQL           Instance B
      │                   │                     │
      │  LEADER           │                     │  STANDBY
      │                   │                     │
      │  ──X              │                     │  (polling)
      │  [Crash!]         │                     │
      │                   │                     │
      │                   │  Lock auto-released │
      │                   │  (connection closed)│
      │                   │                     │
      │                   │                     ├──try_lock()──────>
      │                   │                     │<─────OK───────────┤
      │                   │                     │
      │                   │                     │  LEADER mode
      │                   │                     │  Load checkpoint
      │                   │                     │  Resume processing

Graceful Upgrade (Zero-Downtime)
─────────────────────────────────

  Instance A (v1.0)   PostgreSQL           Instance B (v1.1)
      │                   │                     │
      │  LEADER           │                     │  [Deploy]
      │                   │                     │  Startup
      │                   │                     │
      │                   │                     ├──try_lock()──────>
      │                   │                     │<─────FAIL─────────┤
      │                   │                     │
      │  [SIGTERM]        │                     │  STANDBY
      │  Start draining   │                     │  (waiting)
      │                   │                     │
      │  Wait for work    │                     │
      │  to complete...   │                     │
      │  (30s timeout)    │                     │
      │                   │                     │
      │  Save checkpoint  │                     │
      │  Release lock ────┤                     │
      │                   │  Lock released      │
      │  Exit cleanly     │                     │
      │                   │                     ├──try_lock()──────>
      │                   │                     │<─────OK───────────┤
      │                   │                     │
      │                   │                     │  LEADER mode
      │                   │                     │  Load checkpoint
      │                   │                     │  Resume at last event
```

---

## 5. Monitoring & Observability

### Journald-First Architecture

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                    MONITORING & OBSERVABILITY ARCHITECTURE                    │
│                           Self-Hosting via Events                             │
└──────────────────────────────────────────────────────────────────────────────┘

HEARTBEAT EMISSION (All Services)
═══════════════════════════════════════════════════════════════════════════════

  ┌─────────────────────────────────────────────────────────────────────┐
  │                     fs-watcher (Example)                             │
  │                                                                       │
  │  Every 60 seconds:                                                    │
  │  ┌───────────────────────────────────────────────────────────────┐  │
  │  │  HeartbeatEmitter::emit()                                      │  │
  │  │                                                                 │  │
  │  │  1. Collect metrics:                                            │  │
  │  │     - events_processed (since last heartbeat)                   │  │
  │  │     - errors_count (since last heartbeat)                       │  │
  │  │     - memory_usage_mb (VmRSS from /proc/self/status)            │  │
  │  │     - cpu_usage_percent (getrusage delta)                       │  │
  │  │     - uptime_seconds                                            │  │
  │  │                                                                 │  │
  │  │  2. Determine status:                                           │  │
  │  │     errors > 50  → Status::Failed                               │  │
  │  │     errors > 10  → Status::Degraded                             │  │
  │  │     else         → Status::Healthy                              │  │
  │  │                                                                 │  │
  │  │  3. Serialize to JSON                                           │  │
  │  │                                                                 │  │
  │  │  4. println!("{}", json)  ← To stdout                           │  │
  │  └───────────────────────────────────────────────────────────────┘  │
  └─────────────────┬───────────────────────────────────────────────────┘
                    │
                    ↓ stdout
  ┌─────────────────────────────────────────────────────────────────────┐
  │                           systemd                                    │
  │                                                                       │
  │  Unit file: fs-watcher.service                                       │
  │  StandardOutput=journal                                              │
  │  StandardError=journal                                               │
  │                                                                       │
  │  All stdout → journald automatically                                 │
  └─────────────────┬───────────────────────────────────────────────────┘
                    │
                    ↓ journald logs
  ┌─────────────────────────────────────────────────────────────────────┐
  │                      journald Storage                                │
  │                                                                       │
  │  Logs stored: /var/log/journal/{machine-id}/                        │
  │  Format: Binary, indexed by timestamp, unit, priority               │
  │                                                                       │
  │  Heartbeat entry example:                                            │
  │  {                                                                    │
  │    "__REALTIME_TIMESTAMP": "1705324800000000",                       │
  │    "_SYSTEMD_UNIT": "fs-watcher.service",                            │
  │    "MESSAGE": "{                                                     │
  │      \"service_name\": \"fs-watcher\",                               │
  │      \"status\": \"Healthy\",                                        │
  │      \"events_processed\": 142,                                      │
  │      \"memory_usage_mb\": 45,                                        │
  │      \"cpu_usage_percent\": 2.3,                                     │
  │      ...                                                             │
  │    }"                                                                 │
  │  }                                                                    │
  └─────────────────┬───────────────────────────────────────────────────┘
                    │
                    ↓ journald-node reads
  ┌─────────────────────────────────────────────────────────────────────┐
  │               journald-node (Event Capture)                     │
  │                                                                       │
  │  ┌───────────────────────────────────────────────────────────────┐  │
  │  │ JournalReader:                                                 │  │
  │  │                                                                 │  │
  │  │ 1. journalctl --follow --output=json                           │  │
  │  │    --unit=*.service                                            │  │
  │  │                                                                 │  │
  │  │ 2. Filter: MESSAGE matches heartbeat pattern                   │  │
  │  │    (contains "service_name", "status", etc.)                   │  │
  │  │                                                                 │  │
  │  │ 3. Parse JSON from MESSAGE field                               │  │
  │  │                                                                 │  │
  │  │ 4. Emit as Sinex event:                                        │  │
  │  │    - source: "journald"                                        │  │
  │  │    - event_type: "heartbeat"                                   │  │
  │  │    - payload: {parsed heartbeat}                               │  │
  │  └───────────────────────────────────────────────────────────────┘  │
  └─────────────────┬───────────────────────────────────────────────────┘
                    │
                    ↓ NATS events.raw.journald.heartbeat
  ┌─────────────────────────────────────────────────────────────────────┐
  │                        sinex-ingestd                                 │
  │                                                                       │
  │  Standard ingestion path:                                            │
  │  - Validate heartbeat schema                                         │
  │  - INSERT INTO core.events                                           │
  │  - Publish confirmation                                              │
  └─────────────────┬───────────────────────────────────────────────────┘
                    │
                    ↓ events.confirmations.{event_id}
  ┌─────────────────────────────────────────────────────────────────────┐
  │                   health-aggregator Automaton                        │
  │                                                                       │
  │  ┌───────────────────────────────────────────────────────────────┐  │
  │  │ Processing:                                                    │  │
  │  │                                                                 │  │
  │  │ 1. Subscribe to: events.confirmations.>                        │  │
  │  │    Filter: event_type = "heartbeat"                            │  │
  │  │                                                                 │  │
  │  │ 2. Aggregate metrics per service:                              │  │
  │  │    - Latest status                                             │  │
  │  │    - Uptime                                                    │  │
  │  │    - Event throughput (events/sec)                             │  │
  │  │    - Error rate                                                │  │
  │  │    - Resource usage trends                                     │  │
  │  │                                                                 │  │
  │  │ 3. Detect anomalies:                                           │  │
  │  │    - Status: Healthy → Failed                                  │  │
  │  │    - Missing heartbeats (>90s gap)                             │  │
  │  │    - Resource spikes (memory +50%, CPU +80%)                   │  │
  │  │                                                                 │  │
  │  │ 4. Store aggregated metrics:                                   │  │
  │  │    - INSERT INTO core.service_health                           │  │
  │  │    - Time-series data for dashboard                            │  │
  │  └───────────────────────────────────────────────────────────────┘  │
  └─────────────────┬───────────────────────────────────────────────────┘
                    │
                    ↓ Query via gateway
  ┌─────────────────────────────────────────────────────────────────────┐
  │                      sinex-gateway (RPC)                             │
  │                                                                       │
  │  Endpoints:                                                           │
  │  - GET /health/services              (all services)                  │
  │  - GET /health/services/{name}       (specific service)              │
  │  - GET /health/history/{name}        (time-series)                   │
  │  - GET /health/alerts                (active alerts)                 │
  └─────────────────┬───────────────────────────────────────────────────┘
                    │
                    ↓ HTTP/JSON
  ┌─────────────────────────────────────────────────────────────────────┐
  │                    Dashboard / Monitoring UI                         │
  │                                                                       │
  │  Real-time view:                                                     │
  │  ┌──────────────────────────────────────────────────────────────┐   │
  │  │ Service          Status      Uptime   Events/s   Mem   CPU   │   │
  │  │ fs-watcher       ✅ Healthy  2d 4h    23.5       45MB  2.3%  │   │
  │  │ terminal-sat     ✅ Healthy  2d 4h    8.2        32MB  1.1%  │   │
  │  │ desktop-sat      ⚠️  Degraded 1d 2h    5.1        78MB  3.8%  │   │
  │  │ ingestd          ✅ Healthy  2d 4h    31.8       125MB 8.2%  │   │
  │  └──────────────────────────────────────────────────────────────┘   │
  └───────────────────────────────────────────────────────────────────────┘

═══════════════════════════════════════════════════════════════════════════════

BENEFITS OF JOURNALD-FIRST APPROACH
═══════════════════════════════════════════════════════════════════════════════

  ✅ Zero Configuration
     - Works out-of-box with systemd
     - No Prometheus, Grafana, Datadog setup needed

  ✅ Unified Storage
     - Heartbeats are events, stored in core.events
     - Query with same API as application events
     - Time-travel debugging

  ✅ Self-Hosting
     - No external monitoring dependencies
     - System monitors itself
     - Observability built into event model

  ✅ Historical Analysis
     - Full heartbeat history in database
     - SQL queries for complex analysis
     - Replay monitoring data

  ✅ Integration
     - Heartbeats flow through same pipeline as app events
     - Can correlate service health with event processing
     - Single source of truth
```

---

## 6. Database Architecture

### TimescaleDB Hypertable + Repository Pattern

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                        DATABASE ARCHITECTURE                                  │
│              PostgreSQL 14+ with TimescaleDB Extension                        │
└──────────────────────────────────────────────────────────────────────────────┘

SCHEMA OVERVIEW
═══════════════════════════════════════════════════════════════════════════════

  Schemas:
    - core                  (main tables)
    - sinex_schemas         (schema registry)
    - sinex_internal        (migrations, metadata)

  ┌─────────────────────────────────────────────────────────────────────┐
  │                       core.events (Hypertable)                       │
  │                                                                       │
  │  Columns:                                                             │
  │  ┌───────────────────────────────────────────────────────────────┐  │
  │  │ id                    ULID PRIMARY KEY                         │  │
  │  │ source                TEXT NOT NULL                            │  │
  │  │ event_type            TEXT NOT NULL                            │  │
  │  │ host                  TEXT NOT NULL                            │  │
  │  │ payload               JSONB NOT NULL                           │  │
  │  │ ts_orig               TIMESTAMPTZ                              │  │
  │  │ ts_ingest             TIMESTAMPTZ NOT NULL DEFAULT NOW()       │  │
  │  │ source_material_id    ULID                                     │  │
  │  │ anchor_byte           BIGINT                                   │  │
  │  │ offset_start          BIGINT                                   │  │
  │  │ offset_end            BIGINT                                   │  │
  │  │ offset_kind           TEXT                                     │  │
  │  │ source_event_ids      ULID[]                                   │  │
  │  │ associated_blob_ids   ULID[]                                   │  │
  │  │ payload_schema_id     ULID                                     │  │
  │  │ ingestor_version      TEXT                                     │  │
  │  └───────────────────────────────────────────────────────────────┘  │
  │                                                                       │
  │  Partitioning:                                                        │
  │  ┌───────────────────────────────────────────────────────────────┐  │
  │  │ TimescaleDB Hypertable:                                        │  │
  │  │ - Partition by: ulid_to_timestamptz(id)                        │  │
  │  │ - Chunk interval: 7 days (default)                             │  │
  │  │ - Automatic chunk creation                                     │  │
  │  │ - Partition pruning on time-range queries                      │  │
  │  │                                                                 │  │
  │  │ Chunks (auto-created):                                         │  │
  │  │   _hyper_1_1_chunk  (2025-01-01 to 2025-01-08)                 │  │
  │  │   _hyper_1_2_chunk  (2025-01-08 to 2025-01-15)                 │  │
  │  │   _hyper_1_3_chunk  (2025-01-15 to 2025-01-22)  ← active       │  │
  │  └───────────────────────────────────────────────────────────────┘  │
  │                                                                       │
  │  Indexes:                                                             │
  │  ┌───────────────────────────────────────────────────────────────┐  │
  │  │ PRIMARY KEY (id)                                               │  │
  │  │ CREATE INDEX idx_events_ts_ingest ON core.events(ts_ingest)   │  │
  │  │ CREATE INDEX idx_events_source ON core.events(source)         │  │
  │  │ CREATE INDEX idx_events_event_type ON core.events(event_type) │  │
  │  │ CREATE INDEX idx_events_payload_gin ON core.events            │  │
  │  │   USING GIN (payload jsonb_path_ops)                          │  │
  │  │ CREATE INDEX idx_events_source_material                        │  │
  │  │   ON core.events(source_material_id) WHERE source_material_id  │  │
  │  │   IS NOT NULL                                                  │  │
  │  └───────────────────────────────────────────────────────────────┘  │
  └───────────────────────────────────────────────────────────────────────┘

  ┌─────────────────────────────────────────────────────────────────────┐
  │                    core.source_materials                             │
  │                                                                       │
  │  Purpose: Tracks large source files (logs, command output, etc.)     │
  │                                                                       │
  │  Columns:                                                             │
  │  - id (ULID)                                                          │
  │  - material_type (text, binary, structured)                           │
  │  - content_hash (SHA256)                                              │
  │  - size_bytes                                                         │
  │  - storage_path (Git Annex key)                                       │
  │  - created_at                                                         │
  │                                                                       │
  │  Storage Backend: Git Annex                                           │
  │  - Large files (>1MB) stored in annex                                 │
  │  - Deduplication via content hash                                     │
  │  - Symlinks in .git/annex/objects/                                    │
  └───────────────────────────────────────────────────────────────────────┘

  ┌─────────────────────────────────────────────────────────────────────┐
  │                        core.blobs                                     │
  │                                                                       │
  │  Purpose: Binary data attached to events (screenshots, recordings)    │
  │                                                                       │
  │  Columns:                                                             │
  │  - id (ULID)                                                          │
  │  - mime_type                                                          │
  │  - size_bytes                                                         │
  │  - content_hash                                                       │
  │  - storage_backend (postgres, filesystem, s3)                         │
  │  - data (BYTEA, nullable)  ← Small blobs inline                       │
  │  - external_path           ← Large blobs external                     │
  └───────────────────────────────────────────────────────────────────────┘

═══════════════════════════════════════════════════════════════════════════════

REPOSITORY PATTERN ARCHITECTURE
═══════════════════════════════════════════════════════════════════════════════

  ┌─────────────────────────────────────────────────────────────────────┐
  │                    sinex-core/db/repositories/                       │
  │                                                                       │
  │  Base Trait: Repository<'a>                                          │
  │  ┌───────────────────────────────────────────────────────────────┐  │
  │  │ trait Repository<'a> {                                         │  │
  │  │     fn pool(&self) -> &'a PgPool;                              │  │
  │  │     fn new(pool: &'a PgPool) -> Self;                          │  │
  │  │ }                                                               │  │
  │  └───────────────────────────────────────────────────────────────┘  │
  │                                                                       │
  │  Concrete Repositories:                                               │
  │  ┌────────────────────────────────────────────────────────────┬────┐│
  │  │ EventRepository<'a>                                        │ ⭐ ││
  │  │ - insert(event) -> Event                                   │    ││
  │  │ - insert_batch(events) -> Vec<Event>                       │    ││
  │  │ - get_by_id(id) -> Option<Event>                           │    ││
  │  │ - search(filters) -> Vec<Event>                            │    ││
  │  │ - get_events_over_time(range, interval) -> Vec<Bucket>     │    ││
  │  └────────────────────────────────────────────────────────────┴────┘│
  │  ┌────────────────────────────────────────────────────────────┐    ││
  │  │ SourceMaterialRepository<'a>                               │    ││
  │  │ - insert(material) -> SourceMaterial                       │    ││
  │  │ - get_by_id(id) -> Option<SourceMaterial>                  │    ││
  │  │ - get_by_hash(hash) -> Option<SourceMaterial>              │    ││
  │  └────────────────────────────────────────────────────────────┘    ││
  │  ┌────────────────────────────────────────────────────────────┐    ││
  │  │ CheckpointRepository<'a>                                   │    ││
  │  │ - get_latest(processor) -> Option<Checkpoint>              │    ││
  │  │ - save(checkpoint) -> ()                                   │    ││
  │  └────────────────────────────────────────────────────────────┘    ││
  │                                                                       │
  │  DbPoolExt Trait (Ergonomic Access):                                 │
  │  ┌───────────────────────────────────────────────────────────────┐  │
  │  │ impl DbPoolExt for PgPool {                                   │  │
  │  │     fn events(&self) -> EventRepository<'_> { ... }           │  │
  │  │     fn source_materials(&self) -> SourceMaterialRepository { }│  │
  │  │     fn checkpoints(&self) -> CheckpointRepository { ... }     │  │
  │  │ }                                                              │  │
  │  │                                                                │  │
  │  │ Usage:                                                         │  │
  │  │   let event = pool.events().get_by_id(id).await?;             │  │
  │  │   let materials = pool.source_materials().search(q).await?;   │  │
  │  └───────────────────────────────────────────────────────────────┘  │
  └─────────────────────────────────────────────────────────────────────┘

═══════════════════════════════════════════════════════════════════════════════

SQLX COMPILE-TIME VALIDATION
═══════════════════════════════════════════════════════════════════════════════

  Flow during `cargo build`:
  ┌───────────────────────────────────────────────────────────────────┐
  │ 1. SQLX Macros (sqlx::query!, query_as!)                          │
  │    ↓                                                               │
  │ 2. Connect to DATABASE_URL at compile-time                        │
  │    ↓                                                               │
  │ 3. Execute PREPARE query                                          │
  │    - Check syntax                                                  │
  │    - Check table/column existence                                  │
  │    - Infer result types                                            │
  │    ↓                                                               │
  │ 4. Generate Rust struct matching result                           │
  │    ↓                                                               │
  │ 5. Type-check bindings                                             │
  │    - Parameter types match                                         │
  │    - Nullability correct                                           │
  │    ↓                                                               │
  │ 6. Compile succeeds OR fails with helpful error                   │
  └───────────────────────────────────────────────────────────────────┘

  Example:
  ```rust
  // This will NOT compile if:
  // - core.events doesn't exist
  // - Column 'source' doesn't exist
  // - Type mismatch (e.g., binding i32 to TEXT column)

  let events = sqlx::query_as!(
      EventRecord,
      r#"
      SELECT id::uuid as "id!: Ulid",
             source as "source!",
             event_type as "event_type!",
             payload as "payload!"
      FROM core.events
      WHERE source = $1
      ORDER BY ts_ingest DESC
      LIMIT 100
      "#,
      source_filter  // Type-checked against TEXT
  )
  .fetch_all(&pool)
  .await?;
  ```

  Benefits:
  ✅ Typos caught at compile-time (not runtime!)
  ✅ Schema changes break build (immediate feedback)
  ✅ Nullability enforced (no unexpected NULLs)
  ✅ Zero runtime overhead (all validation done at build time)

═══════════════════════════════════════════════════════════════════════════════

TIMESCALEDB FEATURES UTILIZED
═══════════════════════════════════════════════════════════════════════════════

  1. Hypertable Partitioning
     - Automatic chunk creation based on time
     - Partition pruning (query only relevant chunks)
     - Efficient time-range queries

  2. time_bucket() Function
     ```sql
     SELECT time_bucket('1 hour', ts_ingest) as hour,
            COUNT(*) as event_count
     FROM core.events
     WHERE ts_ingest >= NOW() - INTERVAL '24 hours'
     GROUP BY hour
     ORDER BY hour;
     ```
     - Aggregates events into time buckets
     - Used for dashboards, analytics

  3. Continuous Aggregates (Future)
     - Pre-compute materialized views
     - Automatically refresh
     - Fast dashboard queries

  4. Compression (Future)
     - Compress old chunks
     - Save storage
     - Still queryable

  5. Retention Policies (Planned)
     ```sql
     SELECT add_retention_policy('core.events', INTERVAL '90 days');
     ```
     - Auto-delete old chunks
     - Enforce 90-day retention
```

---

## 7. Testing Infrastructure

### 64-Database Parallel Test Pool

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                      TESTING INFRASTRUCTURE                                   │
│                 Parallel Test Execution at Scale                              │
└──────────────────────────────────────────────────────────────────────────────┘

TEST DATABASE POOL ARCHITECTURE
═══════════════════════════════════════════════════════════════════════════════

  ┌─────────────────────────────────────────────────────────────────────┐
  │                       PostgreSQL Server                              │
  │                                                                       │
  │  ┌────────────────────────────────────────────────────────────────┐ │
  │  │              Template Database                                  │ │
  │  │  test_db_template                                               │ │
  │  │                                                                  │ │
  │  │  - All migrations applied                                       │ │
  │  │  - Schema matches production                                    │ │
  │  │  - Migration fingerprint stored: SHA256(migrations)             │ │
  │  │  - Used as template for fast cloning                            │ │
  │  └────────────────────────────────────────────────────────────────┘ │
  │                                                                       │
  │  ┌────────────────────────────────────────────────────────────────┐ │
  │  │              Test Database Pool (64 databases)                  │ │
  │  │                                                                  │ │
  │  │  test_db_00  ←─ Advisory Lock: 1000                            │ │
  │  │  test_db_01  ←─ Advisory Lock: 1001                            │ │
  │  │  test_db_02  ←─ Advisory Lock: 1002                            │ │
  │  │  ...                                                            │ │
  │  │  test_db_63  ←─ Advisory Lock: 1063                            │ │
  │  │                                                                  │ │
  │  │  Each database:                                                  │ │
  │  │  - Cloned from test_db_template (fast!)                         │ │
  │  │  - Isolated (no cross-test pollution)                           │ │
  │  │  - Coordinated via advisory locks                               │ │
  │  └────────────────────────────────────────────────────────────────┘ │
  └───────────────────────────────────────────────────────────────────────┘

POOL ACQUISITION FLOW
═══════════════════════════════════════════════════════════════════════════════

  Test Process                 DatabasePool               PostgreSQL
      │                             │                          │
      │ acquire_slot()              │                          │
      ├────────────────────────────>│                          │
      │                             │                          │
      │                             │ Check migration hash     │
      │                             ├─────────────────────────>│
      │                             │ SELECT fingerprint       │
      │                             │   FROM test_db_template  │
      │                             │<─────────────────────────┤
      │                             │                          │
      │                             │ Hash matches?            │
      │                             │ YES: Use template        │
      │                             │ NO:  Rebuild template    │
      │                             │                          │
      │                             │ Try slot 0               │
      │                             ├─────────────────────────>│
      │                             │ CONNECT test_db_00       │
      │                             │<─────────────────────────┤
      │                             │                          │
      │                             │ pg_try_advisory_lock(1000)
      │                             ├─────────────────────────>│
      │                             │<─────── true ────────────┤
      │                             │ LOCK ACQUIRED ✓          │
      │                             │                          │
      │<─ TestDatabase(slot=0) ─────┤                          │
      │                             │                          │
      │ Run test...                 │                          │
      │ INSERT/UPDATE/DELETE        │                          │
      │ Test assertions             │                          │
      │                             │                          │
      │ Drop TestDatabase           │                          │
      │ (automatic cleanup)         │                          │
      ├────────────────────────────>│                          │
      │                             │ pg_advisory_unlock(1000) │
      │                             ├─────────────────────────>│
      │                             │ LOCK RELEASED            │
      │                             │                          │
      │                             │ pool.close()             │
      │                             ├─────────────────────────>│
      │                             │ CONNECTION CLOSED        │
      │<─ () ───────────────────────┤                          │

PARALLEL TEST EXECUTION
═══════════════════════════════════════════════════════════════════════════════

  ┌─────────────────────────────────────────────────────────────────────┐
  │                     cargo nextest run (64 threads)                   │
  │                                                                       │
  │  Thread 1 → acquire_slot() → test_db_00 ─┐                          │
  │  Thread 2 → acquire_slot() → test_db_01  │                          │
  │  Thread 3 → acquire_slot() → test_db_02  │  All tests run           │
  │  ...                                      ├─ in parallel             │
  │  Thread 63 → acquire_slot() → test_db_62 │  No interference         │
  │  Thread 64 → acquire_slot() → test_db_63─┘                          │
  │                                                                       │
  │  Thread 65 → acquire_slot() → [WAIT]                                 │
  │                  │                                                    │
  │                  │ Sleep 50ms, retry...                              │
  │                  │                                                    │
  │                  ↓ (Thread 1 finishes)                               │
  │               test_db_00 released                                    │
  │                  │                                                    │
  │                  ↓                                                    │
  │  Thread 65 → acquire_slot() → test_db_00 ✓                          │
  └───────────────────────────────────────────────────────────────────────┘

  Benefits:
  ✅ Up to 64 tests in parallel (vs 1 with shared DB)
  ✅ No test pollution (isolated databases)
  ✅ Fast startup (template cloning ~100ms)
  ✅ Automatic cleanup (advisory locks)
  ✅ No manual teardown needed

MIGRATION FINGERPRINTING
═══════════════════════════════════════════════════════════════════════════════

  Purpose: Detect when migrations change, trigger template rebuild

  ┌───────────────────────────────────────────────────────────────────┐
  │ 1. Hash all migration files                                        │
  │    SHA256(m001.sql + m002.sql + ... + m050.sql)                   │
  │    → "a3f9c2e1..."                                                 │
  │                                                                    │
  │ 2. Store in test_db_template                                       │
  │    CREATE TABLE _test_metadata (                                   │
  │      migration_fingerprint TEXT                                    │
  │    );                                                              │
  │    INSERT VALUES ('a3f9c2e1...');                                  │
  │                                                                    │
  │ 3. On next test run:                                               │
  │    - Compute current hash                                          │
  │    - Compare with template hash                                    │
  │    - Match?    → Use template (fast)                               │
  │    - Mismatch? → Rebuild template (one-time cost)                  │
  │                                                                    │
  │ 4. Rebuild procedure:                                              │
  │    DROP DATABASE IF EXISTS test_db_template;                       │
  │    CREATE DATABASE test_db_template;                               │
  │    \c test_db_template                                             │
  │    -- Apply all migrations                                         │
  │    -- Store new fingerprint                                        │
  │                                                                    │
  │ Result: Template always matches current schema                     │
  └───────────────────────────────────────────────────────────────────┘

FIXTURE MANAGEMENT
═══════════════════════════════════════════════════════════════════════════════

  ┌─────────────────────────────────────────────────────────────────────┐
  │              Global Fixture Registry (Singleton)                     │
  │                                                                       │
  │  static FIXTURE_REGISTRY: OnceCell<Arc<Mutex<FixtureRegistry>>>     │
  │                                                                       │
  │  ┌───────────────────────────────────────────────────────────────┐  │
  │  │ FixtureRegistry:                                              │  │
  │  │                                                                │  │
  │  │ cache:      HashMap<FixtureKey, Arc<dyn Any>>                 │  │
  │  │ ref_counts: HashMap<FixtureKey, usize>                        │  │
  │  │ cleanups:   HashMap<CleanupKey, CleanupTask>                  │  │
  │  │                                                                │  │
  │  │ FixtureKey = (type_name, params)                              │  │
  │  │   e.g., ("TestDatabase", "test_db_05")                        │  │
  │  │        ("TestContext", "{config_json}")                       │  │
  │  └───────────────────────────────────────────────────────────────┘  │
  │                                                                       │
  │  Reference Counting:                                                  │
  │  ┌───────────────────────────────────────────────────────────────┐  │
  │  │ Test A calls: test_database("mydb")                           │  │
  │  │   → Create fixture, ref_count = 1                             │  │
  │  │                                                                │  │
  │  │ Test B calls: test_database("mydb")  (same key!)              │  │
  │  │   → Return cached fixture, ref_count = 2                      │  │
  │  │                                                                │  │
  │  │ Test A drops fixture                                           │  │
  │  │   → ref_count = 1 (no cleanup yet)                            │  │
  │  │                                                                │  │
  │  │ Test B drops fixture                                           │  │
  │  │   → ref_count = 0 → Run cleanup → Remove from cache           │  │
  │  └───────────────────────────────────────────────────────────────┘  │
  │                                                                       │
  │  Cleanup Tasks:                                                       │
  │  - Database: pool.close(), remove advisory lock                      │
  │  - Temp files: fs::remove_dir_all(path)                              │
  │  - NATS connections: conn.close()                                    │
  └───────────────────────────────────────────────────────────────────────┘

PROPERTY-BASED TESTING
═══════════════════════════════════════════════════════════════════════════════

  Strategy Builders:
  ┌───────────────────────────────────────────────────────────────────┐
  │ SinexStrategies::event_source()                                    │
  │ → Generates: "filesystem", "shell.kitty", "", "a"                  │
  │                                                                    │
  │ SinexStrategies::json_payload()                                    │
  │ → Generates: null, strings, objects, arrays (0-10 elements)        │
  │                                                                    │
  │ SinexStrategies::malicious_payload()  ← ADVERSARIAL                │
  │ → Generates:                                                       │
  │   - SQL injection: "'; DROP TABLE events; --"                     │
  │   - XSS: "<script>alert('xss')</script>"                          │
  │   - Path traversal: "../../../../etc/passwd"                      │
  │   - DoS: 1MB-2MB strings                                          │
  │   - Deeply nested JSON (100 levels)                               │
  │   - Integer overflow: i64::MAX                                    │
  └───────────────────────────────────────────────────────────────────┘

  Usage:
  ```rust
  proptest! {
      #[test]
      fn event_insertion_preserves_fields(
          source in SinexStrategies::event_source(),
          payload in SinexStrategies::json_payload()
      ) {
          let ctx = TestContext::new().await?;
          let event = Event::new(source, "test", payload.clone());

          event.insert(&ctx.db.pool()).await?;
          let retrieved = Event::get_by_id(&ctx.db.pool(), event.id).await?;

          prop_assert_eq!(retrieved.payload, payload);
      }
  }
  ```

  Runs 100 random test cases, shrinks failures to minimal repro
```

---

## 8. Type System Architecture

### Phantom Types & Zero-Cost Abstractions

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                         TYPE SYSTEM ARCHITECTURE                              │
│                  Compile-Time Safety, Zero Runtime Cost                       │
└──────────────────────────────────────────────────────────────────────────────┘

ULID-BASED TYPE-SAFE IDS
═══════════════════════════════════════════════════════════════════════════════

  Generic Id<T> with Phantom Type:
  ┌─────────────────────────────────────────────────────────────────────┐
  │ #[repr(transparent)]                      ← No runtime overhead     │
  │ #[derive(Clone, Copy, PartialEq, Eq, Hash)]                         │
  │ pub struct Id<T> {                                                   │
  │     ulid: Ulid,                                                      │
  │     _phantom: PhantomData<T>,  ← Zero-sized, compile-time only      │
  │ }                                                                    │
  │                                                                      │
  │ Concrete Types:                                                      │
  │   pub type EventId = Id<Event<JsonValue>>;                          │
  │   pub type SourceMaterialId = Id<SourceMaterial>;                   │
  │   pub type BlobId = Id<Blob>;                                       │
  │   pub type ProcessorId = Id<Processor>;                             │
  │   pub type OperationId = Id<Operation>;                             │
  │                                                                      │
  │ Compile-Time Enforcement:                                            │
  │   fn process_event(event_id: EventId) { ... }                       │
  │   fn delete_blob(blob_id: BlobId) { ... }                           │
  │                                                                      │
  │   let event_id: EventId = ...;                                       │
  │   let blob_id: BlobId = ...;                                         │
  │                                                                      │
  │   process_event(event_id);  ✅ Compiles                              │
  │   process_event(blob_id);   ❌ Compile error: type mismatch          │
  │                             Expected EventId, found BlobId           │
  └─────────────────────────────────────────────────────────────────────┘

  Benefits:
  ✅ Impossible to pass wrong ID type
  ✅ Self-documenting function signatures
  ✅ Zero runtime overhead (newtype pattern)
  ✅ Refactoring safe (compiler catches all ID mismatches)

NEWTYPE PATTERN (35+ Types)
═══════════════════════════════════════════════════════════════════════════════

  Domain-Specific Types:
  ┌─────────────────────────────────────────────────────────────────────┐
  │ pub struct EventSource(String);                                      │
  │ pub struct EventType(String);                                        │
  │ pub struct HostName(String);                                         │
  │ pub struct ServiceName(String);                                      │
  │ pub struct ConsumerGroup(String);                                    │
  │ pub struct ProcessorName(String);                                    │
  │ pub struct NatsSubject(String);                                      │
  │ pub struct FilePath(PathBuf);                                        │
  │ pub struct CommandLine(String);                                      │
  │ pub struct ContentHash(String);  // SHA256 hex                       │
  │ pub struct GitHash(String);      // Git commit SHA                   │
  │ pub struct IpAddress(String);    // Validated IP                     │
  │ pub struct Port(u16);            // 1-65535                          │
  │ ...  (35+ total newtypes)                                            │
  │                                                                      │
  │ Smart Constructors (Validation):                                     │
  │   impl EventSource {                                                 │
  │       pub fn new(s: &str) -> Result<Self, ValidationError> {        │
  │           validate_event_source(s)?;  // Check format               │
  │           Ok(EventSource(s.to_string()))                             │
  │       }                                                               │
  │                                                                      │
  │       pub fn from_static(s: &'static str) -> Self {                 │
  │           // Trusted, no validation (used in tests/hardcoded)       │
  │           EventSource(s.to_string())                                 │
  │       }                                                               │
  │   }                                                                  │
  │                                                                      │
  │ Usage:                                                               │
  │   let source = EventSource::new("filesystem")?;  // Validated       │
  │   let source = EventSource::from_static("filesystem");  // Trusted  │
  │                                                                      │
  │   fn filter_events(source: EventSource) { ... }                     │
  │                                                                      │
  │   filter_events(source);          ✅ Type-safe                       │
  │   filter_events("filesystem");    ❌ Compile error                   │
  │   filter_events(event_type);      ❌ Compile error (different type)  │
  └─────────────────────────────────────────────────────────────────────┘

  Benefits:
  ✅ Prevents string confusion (no mixing source with event_type)
  ✅ Validation at boundaries (untrusted input)
  ✅ No validation in internal code (trusted types)
  ✅ Self-documenting APIs

IMPOSSIBLE STATES VIA TYPE SYSTEM
═══════════════════════════════════════════════════════════════════════════════

  Example 1: NonEmptyVec<T>
  ┌─────────────────────────────────────────────────────────────────────┐
  │ pub struct NonEmptyVec<T> {                                          │
  │     head: T,                                                         │
  │     tail: Vec<T>,                                                    │
  │ }                                                                    │
  │                                                                      │
  │ impl<T> NonEmptyVec<T> {                                             │
  │     pub fn new(head: T) -> Self {                                    │
  │         NonEmptyVec { head, tail: Vec::new() }                       │
  │     }                                                                 │
  │                                                                      │
  │     pub fn push(&mut self, item: T) {                                │
  │         self.tail.push(item);                                        │
  │     }                                                                 │
  │                                                                      │
  │     pub fn first(&self) -> &T {                                      │
  │         &self.head  // No Option<>, always exists!                   │
  │     }                                                                 │
  │                                                                      │
  │     pub fn iter(&self) -> impl Iterator<Item = &T> {                 │
  │         std::iter::once(&self.head).chain(self.tail.iter())          │
  │     }                                                                 │
  │ }                                                                    │
  │                                                                      │
  │ Usage:                                                               │
  │   pub enum Provenance {                                              │
  │       Synthesis {                                                    │
  │           source_event_ids: NonEmptyVec<EventId>,  // ← Guaranteed  │
  │       }                                                               │
  │   }                                                                  │
  │                                                                      │
  │   fn process_synthesis(prov: Provenance) {                           │
  │       if let Provenance::Synthesis { source_event_ids } = prov {     │
  │           let first_id = source_event_ids.first();  // No unwrap!   │
  │           // Compiler guarantees at least 1 element                  │
  │       }                                                               │
  │   }                                                                  │
  └─────────────────────────────────────────────────────────────────────┘

  Example 2: Provenance Enum
  ┌─────────────────────────────────────────────────────────────────────┐
  │ pub enum Provenance {                                                │
  │     Material {                                                       │
  │         id: SourceMaterialId,                                        │
  │         anchor_byte: i64,        // NOT Option<i64>!                 │
  │         offset_start: Option<i64>,                                   │
  │         offset_end: Option<i64>,                                     │
  │     },                                                               │
  │     Synthesis {                                                      │
  │         source_event_ids: NonEmptyVec<EventId>,  // NOT Vec!        │
  │         operation_id: Option<OperationId>,                           │
  │     },                                                               │
  │ }                                                                    │
  │                                                                      │
  │ Impossible States:                                                   │
  │   ❌ Material with missing anchor_byte → Prevented by type           │
  │   ❌ Synthesis with empty source_event_ids → Prevented by NonEmpty   │
  │   ❌ Event with no provenance → Enum exhaustiveness                  │
  └─────────────────────────────────────────────────────────────────────┘

TYPE-STATE PATTERN
═══════════════════════════════════════════════════════════════════════════════

  Example: Event<T> with Type-Level Payload
  ┌─────────────────────────────────────────────────────────────────────┐
  │ pub struct Event<T> {                                                │
  │     pub id: Option<EventId>,  // None before insert, Some after     │
  │     pub source: EventSource,                                         │
  │     pub event_type: EventType,                                       │
  │     pub payload: T,  ← Generic payload type                          │
  │     // ... other fields                                              │
  │ }                                                                    │
  │                                                                      │
  │ Concrete Types:                                                      │
  │   Event<JsonValue>       // Dynamic JSON                             │
  │   Event<FileCreatedPayload>  // Typed struct                         │
  │   Event<CommandExecutedPayload>                                      │
  │                                                                      │
  │ Type-Safe Builders:                                                  │
  │   impl Event<JsonValue> {                                            │
  │       pub fn dynamic(                                                │
  │           source: EventSource,                                       │
  │           event_type: EventType,                                     │
  │           payload: JsonValue                                         │
  │       ) -> Self { ... }                                              │
  │   }                                                                  │
  │                                                                      │
  │   impl<T: Serialize> Event<T> {                                      │
  │       pub fn typed(                                                  │
  │           source: EventSource,                                       │
  │           event_type: EventType,                                     │
  │           payload: T                                                 │
  │       ) -> Self { ... }                                              │
  │   }                                                                  │
  │                                                                      │
  │ Usage:                                                               │
  │   let event: Event<FileCreatedPayload> = Event::typed(              │
  │       EventSource::from_static("filesystem"),                        │
  │       EventType::from_static("file.created"),                        │
  │       FileCreatedPayload { path: "/tmp/foo", size: 1024 }            │
  │   );                                                                 │
  │                                                                      │
  │   // Compiler enforces payload type                                  │
  │   println!("{}", event.payload.path);  ✅ Works                      │
  └─────────────────────────────────────────────────────────────────────┘

COMPILE-TIME GUARANTEES ACHIEVED
═══════════════════════════════════════════════════════════════════════════════

  ✅ No ID confusion (EventId ≠ BlobId ≠ MaterialId)
  ✅ No string confusion (EventSource ≠ EventType ≠ HostName)
  ✅ No empty collections where non-empty required (NonEmptyVec)
  ✅ No missing required fields (Material provenance requires anchor_byte)
  ✅ No wrong payload types (Event<T> enforces payload structure)
  ✅ No invalid states (enum exhaustiveness, type-state pattern)

  Bug Classes Eliminated at Compile-Time:
  ❌ Passing wrong ID to function
  ❌ Mixing up string-typed values
  ❌ Accessing first element of empty Vec (panic)
  ❌ Missing required provenance fields
  ❌ Type confusion in event payloads
```

---

## 9. Concurrency Patterns

### CoordinationPrimitive & Lock Strategy

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                         CONCURRENCY ARCHITECTURE                              │
│                Custom Primitives + Careful Lock Selection                     │
└──────────────────────────────────────────────────────────────────────────────┘

COORDINATIONPRIMITIVE (Custom Lock-Free Abstraction)
═══════════════════════════════════════════════════════════════════════════════

  Design: Unifies multiple synchronization patterns
  ┌─────────────────────────────────────────────────────────────────────┐
  │ pub struct CoordinationPrimitive {                                   │
  │     state: Arc<AtomicUsize>,       // Counter                        │
  │     notify: Arc<Notify>,           // tokio::sync::Notify            │
  │     threshold: usize,               // Trigger point                 │
  │     generation: Arc<AtomicUsize>,  // ABA prevention                │
  │     reset_behavior: ResetBehavior, // Manual/Automatic/Never         │
  │     name: String,                   // Debugging                     │
  │ }                                                                    │
  │                                                                      │
  │ Patterns Unified:                                                    │
  │ 1. Event Counter (Semaphore-like)                                    │
  │    - add(delta) / subtract(delta)                                    │
  │    - get() → current value                                           │
  │                                                                      │
  │ 2. Boolean Signal (Event-like)                                       │
  │    - signal() → set to 1                                             │
  │    - reset() → set to 0                                              │
  │    - wait_for(1) → wait until signaled                               │
  │                                                                      │
  │ 3. Barrier Synchronization                                           │
  │    - wait_for(threshold)                                             │
  │    - Auto-reset when threshold reached                               │
  │    - Generation counter prevents ABA                                 │
  │                                                                      │
  │ 4. Progress Tracker                                                  │
  │    - Track in-flight operations                                      │
  │    - Wait for drain (value → 0)                                      │
  └─────────────────────────────────────────────────────────────────────┘

  Implementation Details:
  ┌─────────────────────────────────────────────────────────────────────┐
  │ pub fn add(&self, delta: usize) {                                    │
  │     let new_state = self.state.fetch_add(delta, Ordering::AcqRel)   │
  │                     + delta;                                         │
  │     self.check_threshold_and_notify(new_state);                      │
  │ }                                                                    │
  │                                                                      │
  │ pub async fn wait_for(&self, value: usize, timeout: Duration)       │
  │     -> bool                                                          │
  │ {                                                                    │
  │     let initial_generation = self.generation.load(Ordering::Acquire);│
  │     let deadline = Instant::now() + timeout;                         │
  │                                                                      │
  │     loop {                                                           │
  │         let current = self.state.load(Ordering::Acquire);            │
  │         let current_gen = self.generation.load(Ordering::Acquire);   │
  │                                                                      │
  │         // Condition met OR generation changed (barrier opened)      │
  │         if current >= value || current_gen > initial_generation {    │
  │             return true;                                             │
  │         }                                                            │
  │                                                                      │
  │         match tokio::time::timeout_at(                               │
  │             deadline.into(),                                         │
  │             self.notify.notified()                                   │
  │         ).await {                                                    │
  │             Ok(_) => continue,  // Woken up, check again             │
  │             Err(_) => return false,  // Timeout                      │
  │         }                                                            │
  │     }                                                                │
  │ }                                                                    │
  │                                                                      │
  │ fn check_threshold_and_notify(&self, new_state: usize) {            │
  │     if new_state >= self.threshold {                                 │
  │         match self.reset_behavior {                                  │
  │             ResetBehavior::Automatic => {                            │
  │                 // Barrier pattern                                   │
  │                 self.state.store(0, Ordering::Release);              │
  │                 self.generation.fetch_add(1, Ordering::AcqRel);      │
  │             }                                                        │
  │             _ => {}                                                  │
  │         }                                                            │
  │         self.notify.notify_waiters();  // Wake ALL waiters           │
  │     }                                                                │
  │ }                                                                    │
  └─────────────────────────────────────────────────────────────────────┘

  Usage Example: WorkTracker
  ┌─────────────────────────────────────────────────────────────────────┐
  │ pub struct WorkTracker {                                             │
  │     in_flight_operations: Arc<CoordinationPrimitive>,                │
  │     shutdown_requested: Arc<CoordinationPrimitive>,                  │
  │ }                                                                    │
  │                                                                      │
  │ impl WorkTracker {                                                   │
  │     pub fn start_operation(&self) {                                  │
  │         self.in_flight_operations.add(1);                            │
  │     }                                                                │
  │                                                                      │
  │     pub fn finish_operation(&self) {                                 │
  │         self.in_flight_operations.subtract(1);                       │
  │     }                                                                │
  │                                                                      │
  │     pub fn request_shutdown(&self) {                                 │
  │         self.shutdown_requested.signal();                            │
  │     }                                                                │
  │                                                                      │
  │     pub async fn wait_for_drain(&self, timeout: Duration) -> bool { │
  │         self.in_flight_operations.wait_for(0, timeout).await         │
  │     }                                                                │
  │                                                                      │
  │     pub fn is_shutdown_requested(&self) -> bool {                    │
  │         self.shutdown_requested.get() > 0                            │
  │     }                                                                │
  │ }                                                                    │
  └─────────────────────────────────────────────────────────────────────┘

LOCK SELECTION STRATEGY
═══════════════════════════════════════════════════════════════════════════════

  ┌───────────────────────────────────────────────────────────────────┐
  │                       Lock Type Decision Tree                      │
  │                                                                    │
  │  Need atomicity for counter/flag?                                 │
  │    YES → AtomicUsize / CoordinationPrimitive                      │
  │    NO  ↓                                                          │
  │                                                                    │
  │  Async code (tokio runtime)?                                      │
  │    YES → tokio::sync::RwLock or tokio::sync::Mutex               │
  │    NO  ↓                                                          │
  │                                                                    │
  │  Read-heavy workload (10:1 read:write)?                           │
  │    YES → tokio::sync::RwLock                                      │
  │    NO  ↓                                                          │
  │                                                                    │
  │  Hot path (frequent lock contention)?                             │
  │    YES → parking_lot::Mutex (faster, no poisoning)               │
  │    NO  ↓                                                          │
  │                                                                    │
  │  Simple, infrequent access?                                        │
  │    YES → std::sync::Mutex (simple, stdlib)                        │
  └───────────────────────────────────────────────────────────────────┘

  Lock Inventory in Codebase:
  ┌───────────────────────────────────────────────────────────────────┐
  │ Lock Type              Use Case                  Location           │
  │ ─────────────────────  ────────────────────────  ────────────────  │
  │ AtomicUsize            Counters, flags           CoordinationPrim   │
  │ CoordinationPrimitive  In-flight ops, barriers   WorkTracker        │
  │ tokio::sync::RwLock    Assembler state HashMap   MaterialAssembler │
  │ tokio::sync::RwLock    Rotation state            AcquisitionMgr    │
  │ parking_lot::Mutex     Heartbeat metrics         HeartbeatEmitter  │
  │ parking_lot::Mutex     CPU sample                HeartbeatEmitter  │
  │ std::sync::Mutex       Service status            Lifecycle         │
  └───────────────────────────────────────────────────────────────────┘

SPAWN MANAGEMENT PATTERNS
═══════════════════════════════════════════════════════════════════════════════

  Pattern 1: Background Task with JoinHandle
  ┌───────────────────────────────────────────────────────────────────┐
  │ pub struct SomeService {                                           │
  │     heartbeat_handle: Option<JoinHandle<()>>,                     │
  │ }                                                                  │
  │                                                                    │
  │ impl SomeService {                                                 │
  │     pub fn start_heartbeat(&mut self) {                            │
  │         let handle = tokio::spawn(async move {                    │
  │             let mut interval = tokio::time::interval(             │
  │                 Duration::from_secs(60)                            │
  │             );                                                     │
  │                                                                    │
  │             loop {                                                 │
  │                 interval.tick().await;                             │
  │                 // Emit heartbeat                                  │
  │             }                                                      │
  │         });                                                        │
  │         self.heartbeat_handle = Some(handle);                     │
  │     }                                                              │
  │                                                                    │
  │     pub async fn shutdown(&mut self) {                             │
  │         if let Some(handle) = self.heartbeat_handle.take() {      │
  │             handle.abort();  // Or send shutdown signal            │
  │         }                                                          │
  │     }                                                              │
  │ }                                                                  │
  └───────────────────────────────────────────────────────────────────┘

  Pattern 2: Coordinated Shutdown with tokio::select!
  ┌───────────────────────────────────────────────────────────────────┐
  │ pub async fn run_consumers(&self) -> Result<()> {                  │
  │     let mut begin_handle = self.spawn_begin_consumer();            │
  │     let mut slices_handle = self.spawn_slices_consumer();          │
  │     let mut end_handle = self.spawn_end_consumer();                │
  │                                                                    │
  │     tokio::select! {                                               │
  │         result = &mut begin_handle => {                            │
  │             // One consumer exited, abort others                   │
  │             slices_handle.abort();                                 │
  │             end_handle.abort();                                    │
  │             self.handle_task_exit("begin consumer", result)        │
  │         }                                                          │
  │         result = &mut slices_handle => {                           │
  │             begin_handle.abort();                                  │
  │             end_handle.abort();                                    │
  │             self.handle_task_exit("slices consumer", result)       │
  │         }                                                          │
  │         result = &mut end_handle => {                              │
  │             begin_handle.abort();                                  │
  │             slices_handle.abort();                                 │
  │             self.handle_task_exit("end consumer", result)          │
  │         }                                                          │
  │     }                                                              │
  │ }                                                                  │
  └───────────────────────────────────────────────────────────────────┘

  Pattern 3: Signal Handler with shutdown_receiver
  ┌───────────────────────────────────────────────────────────────────┐
  │ let (shutdown_sender, shutdown_receiver) =                         │
  │     tokio::sync::oneshot::channel();                               │
  │                                                                    │
  │ tokio::spawn(async move {                                          │
  │     let mut sigterm = signal(SignalKind::terminate()).unwrap();   │
  │     let mut sigint = signal(SignalKind::interrupt()).unwrap();    │
  │                                                                    │
  │     tokio::select! {                                               │
  │         _ = sigterm.recv() => {                                    │
  │             info!("Received SIGTERM");                             │
  │         }                                                          │
  │         _ = sigint.recv() => {                                     │
  │             info!("Received SIGINT");                              │
  │         }                                                          │
  │         _ = shutdown_receiver => {                                 │
  │             info!("Received shutdown signal");                     │
  │         }                                                          │
  │     }                                                              │
  │                                                                    │
  │     shutdown_flag.store(true, Ordering::Relaxed);                 │
  │ });                                                                │
  └───────────────────────────────────────────────────────────────────┘

RACE CONDITION PREVENTION
═══════════════════════════════════════════════════════════════════════════════

  Pattern: Atomic Check-and-Insert with Entry API
  ┌───────────────────────────────────────────────────────────────────┐
  │ // ❌ WRONG: TOCTOU Race                                           │
  │ let mut states = self.assembler_state.write().await;              │
  │ if states.contains_key(&material_id) {                            │
  │     return Ok(());  // Already exists                             │
  │ }                                                                  │
  │ states.insert(material_id, new_state);  // ← Race here!           │
  │                                                                    │
  │ // ✅ RIGHT: Atomic with Entry API                                 │
  │ let mut states = self.assembler_state.write().await;              │
  │ states.entry(material_id)                                          │
  │     .or_insert_with(|| new_state);  // Atomic check-and-insert    │
  └───────────────────────────────────────────────────────────────────┘

  Pattern: RAII Guard for Paired Operations
  ┌───────────────────────────────────────────────────────────────────┐
  │ // Problem: Manual pairing error-prone                            │
  │ work_tracker.start_operation();                                    │
  │ // ... do work ...                                                 │
  │ work_tracker.finish_operation();  // Might forget or panic!       │
  │                                                                    │
  │ // Solution: RAII Guard                                            │
  │ pub struct OperationGuard<'a> {                                    │
  │     tracker: &'a WorkTracker,                                      │
  │ }                                                                  │
  │                                                                    │
  │ impl Drop for OperationGuard<'_> {                                 │
  │     fn drop(&mut self) {                                           │
  │         self.tracker.finish_operation();  // Automatic!           │
  │     }                                                              │
  │ }                                                                  │
  │                                                                    │
  │ impl WorkTracker {                                                 │
  │     pub fn start_operation(&self) -> OperationGuard<'_> {          │
  │         self.in_flight_operations.add(1);                          │
  │         OperationGuard { tracker: self }                           │
  │     }                                                              │
  │ }                                                                  │
  │                                                                    │
  │ // Usage:                                                          │
  │ let _guard = work_tracker.start_operation();                       │
  │ // ... do work ...                                                 │
  │ // Automatic cleanup on drop (even if panic!)                      │
  └───────────────────────────────────────────────────────────────────┘
```

---

## 10. Checkpoint System

### Unified Abstraction for All Processors

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                          CHECKPOINT ARCHITECTURE                              │
│                   Type-Safe, Versioned, Persistent State                      │
└──────────────────────────────────────────────────────────────────────────────┘

CHECKPOINT TYPE HIERARCHY
═══════════════════════════════════════════════════════════════════════════════

  ┌─────────────────────────────────────────────────────────────────────┐
  │ pub enum Checkpoint {                                                │
  │     /// No checkpoint (initial state)                                │
  │     None,                                                            │
  │                                                                      │
  │     /// Internal event ID (automata processing confirmed events)     │
  │     Internal {                                                       │
  │         event_id: Ulid,                                              │
  │         message_count: u64,                                          │
  │     },                                                               │
  │                                                                      │
  │     /// External position (ingestors, custom state)                  │
  │     External {                                                       │
  │         position: serde_json::Value,  // Flexible schema             │
  │     },                                                               │
  │                                                                      │
  │     /// Stream message ID (NATS JetStream sequence)                  │
  │     Stream {                                                         │
  │         message_id: String,                                          │
  │         event_id: Option<Ulid>,                                      │
  │     },                                                               │
  │                                                                      │
  │     /// Timestamp-based checkpoint (time-series processing)          │
  │     Timestamp {                                                      │
  │         timestamp: DateTime<Utc>,                                    │
  │     },                                                               │
  │ }                                                                    │
  │                                                                      │
  │ pub struct CheckpointState {                                         │
  │     pub checkpoint: Checkpoint,                                      │
  │     pub processed_count: u64,                                        │
  │     pub last_activity: DateTime<Utc>,                                │
  │     pub data: Option<serde_json::Value>,  // Processor-specific     │
  │     pub version: u32,  // Schema version (current: 2)                │
  │ }                                                                    │
  └─────────────────────────────────────────────────────────────────────┘

  Type Safety Benefits:
  ✅ Impossible to mix checkpoint types
  ✅ Compiler enforces correct variant usage
  ✅ Extensible (add new variants without breaking existing)

STORAGE ARCHITECTURE (NATS KV)
═══════════════════════════════════════════════════════════════════════════════

  NATS KV Bucket: sinex_checkpoints
  ┌─────────────────────────────────────────────────────────────────────┐
  │ Key Format: {processor_name}/{consumer_group}/{consumer_name}       │
  │                                                                      │
  │ Examples:                                                            │
  │   search-automata/default/instance-01                                │
  │   analytics-automata/default/instance-02                             │
  │   health-aggregator/default/instance-01                              │
  │   ingestd-begin/default/worker-01                                    │
  │                                                                      │
  │ Value (JSON):                                                        │
  │ {                                                                    │
  │   "checkpoint": {                                                    │
  │     "Internal": {                                                    │
  │       "event_id": "01HK9Z...",                                       │
  │       "message_count": 12345                                         │
  │     }                                                                 │
  │   },                                                                 │
  │   "processed_count": 12345,                                          │
  │   "last_activity": "2025-01-15T12:00:00Z",                           │
  │   "data": null,                                                      │
  │   "version": 2                                                       │
  │ }                                                                    │
  │                                                                      │
  │ Operations:                                                          │
  │ - PUT (upsert): Atomic per-key, last write wins                     │
  │ - GET: Retrieve latest checkpoint                                    │
  │ - DELETE: Remove checkpoint (rare)                                   │
  │ - WATCH: Subscribe to checkpoint changes (monitoring)                │
  └─────────────────────────────────────────────────────────────────────┘

CHECKPOINT LIFECYCLE
═══════════════════════════════════════════════════════════════════════════════

  Automaton Startup:
  ┌───────────────────────────────────────────────────────────────────┐
  │ 1. Load Checkpoint                                                 │
  │    key = "search-automata/default/instance-01"                     │
  │    checkpoint = nats_kv.get(key).await?                            │
  │                                                                    │
  │    if checkpoint.is_none() {                                       │
  │        // First run, start from beginning                          │
  │        checkpoint = CheckpointState {                              │
  │            checkpoint: Checkpoint::None,                           │
  │            processed_count: 0,                                     │
  │            last_activity: Utc::now(),                              │
  │            data: None,                                             │
  │            version: 2,                                             │
  │        };                                                          │
  │    }                                                               │
  │                                                                    │
  │ 2. Resume from Checkpoint                                          │
  │    match checkpoint.checkpoint {                                   │
  │        Checkpoint::Internal { event_id, .. } => {                  │
  │            // Subscribe to confirmations AFTER event_id            │
  │            consumer.subscribe_after(event_id).await?               │
  │        }                                                           │
  │        Checkpoint::None => {                                       │
  │            // Subscribe from beginning                             │
  │            consumer.subscribe_from_start().await?                  │
  │        }                                                           │
  │        _ => { /* Other variants */ }                               │
  │    }                                                               │
  │                                                                    │
  │ 3. Process Events                                                  │
  │    loop {                                                          │
  │        let msg = consumer.next().await?;                           │
  │        process_event(msg).await?;                                  │
  │                                                                    │
  │        // Update checkpoint every N events or M seconds            │
  │        if should_checkpoint() {                                    │
  │            save_checkpoint(last_event_id).await?;                  │
  │        }                                                           │
  │    }                                                               │
  └───────────────────────────────────────────────────────────────────┘

  Checkpointing Strategy:
  ┌───────────────────────────────────────────────────────────────────┐
  │ Frequency:                                                         │
  │ - Every 100 events processed                                       │
  │ - OR every 30 seconds (whichever comes first)                      │
  │ - On graceful shutdown (always)                                    │
  │                                                                    │
  │ Atomic Update:                                                     │
  │   checkpoint_state.checkpoint = Checkpoint::Internal {             │
  │       event_id: last_processed_event_id,                           │
  │       message_count: checkpoint_state.processed_count,             │
  │   };                                                               │
  │   checkpoint_state.last_activity = Utc::now();                     │
  │                                                                    │
  │   nats_kv.put(key, serde_json::to_vec(&checkpoint_state)?).await?;│
  │                                                                    │
  │ Trade-offs:                                                        │
  │ - Frequent checkpoints: Lower replay on crash, higher overhead    │
  │ - Infrequent checkpoints: Higher replay, lower overhead           │
  │ - Current: Good balance (100 events / 30s)                        │
  └───────────────────────────────────────────────────────────────────┘

SCHEMA EVOLUTION (v1 → v2)
═══════════════════════════════════════════════════════════════════════════════

  Legacy v1 Checkpoint:
  ┌───────────────────────────────────────────────────────────────────┐
  │ struct LegacyCheckpointState {                                     │
  │     last_processed_id: Option<String>,  // Mixed ULID/stream ID   │
  │     processed_count: u64,                                          │
  │     last_activity: DateTime<Utc>,                                  │
  │     data: Option<JsonValue>,                                       │
  │ }                                                                  │
  └───────────────────────────────────────────────────────────────────┘

  Migration (Automatic):
  ┌───────────────────────────────────────────────────────────────────┐
  │ impl From<LegacyCheckpointState> for CheckpointState {             │
  │     fn from(legacy: LegacyCheckpointState) -> Self {               │
  │         let checkpoint = match legacy.last_processed_id {          │
  │             Some(id) => {                                          │
  │                 // Try parsing as ULID                             │
  │                 if let Ok(ulid) = id.parse::<Ulid>() {             │
  │                     Checkpoint::Internal {                         │
  │                         event_id: ulid,                            │
  │                         message_count: legacy.processed_count,     │
  │                     }                                              │
  │                 } else {                                           │
  │                     // Fallback: treat as stream ID                │
  │                     Checkpoint::Stream {                           │
  │                         message_id: id,                            │
  │                         event_id: None,                            │
  │                     }                                              │
  │                 }                                                  │
  │             }                                                      │
  │             None => Checkpoint::None,                              │
  │         };                                                         │
  │                                                                    │
  │         CheckpointState {                                          │
  │             checkpoint,                                            │
  │             processed_count: legacy.processed_count,               │
  │             last_activity: legacy.last_activity,                   │
  │             data: legacy.data,                                     │
  │             version: 2,  // Upgraded!                              │
  │         }                                                          │
  │     }                                                              │
  │ }                                                                  │
  │                                                                    │
  │ // Usage:                                                          │
  │ let checkpoint: CheckpointState = match version {                  │
  │     1 => {                                                         │
  │         let legacy: LegacyCheckpointState =                        │
  │             serde_json::from_slice(&data)?;                        │
  │         legacy.into()  // Automatic migration                      │
  │     }                                                              │
  │     2 => serde_json::from_slice(&data)?,                           │
  │     _ => return Err("Unknown checkpoint version"),                 │
  │ };                                                                 │
  └───────────────────────────────────────────────────────────────────┘

CHECKPOINT USE CASES
═══════════════════════════════════════════════════════════════════════════════

  Use Case 1: Automata (Internal Checkpoints)
  ┌───────────────────────────────────────────────────────────────────┐
  │ search-automata:                                                   │
  │   Checkpoint::Internal { event_id, message_count }                 │
  │                                                                    │
  │   Resume: Subscribe to events.confirmations.> AFTER event_id      │
  │   Guarantees: No event processed twice, no event skipped           │
  └───────────────────────────────────────────────────────────────────┘

  Use Case 2: Ingestd (Stream Checkpoints)
  ┌───────────────────────────────────────────────────────────────────┐
  │ ingestd-begin consumer:                                            │
  │   Checkpoint::Stream {                                             │
  │       message_id: "1234567890",  // NATS sequence number          │
  │       event_id: Some(ulid),      // Optional event correlation    │
  │   }                                                                │
  │                                                                    │
  │   Resume: JetStream delivers from sequence 1234567891+            │
  └───────────────────────────────────────────────────────────────────┘

  Use Case 3: External Integration (External Checkpoints)
  ┌───────────────────────────────────────────────────────────────────┐
  │ kafka-bridge (hypothetical):                                       │
  │   Checkpoint::External {                                           │
  │       position: json!({                                            │
  │           "topic": "sinex-events",                                 │
  │           "partition": 3,                                          │
  │           "offset": 9876543                                        │
  │       })                                                           │
  │   }                                                                │
  │                                                                    │
  │   Resume: Kafka consumer seeks to partition 3, offset 9876543     │
  └───────────────────────────────────────────────────────────────────┘

  Use Case 4: Time-Series Aggregator (Timestamp Checkpoints)
  ┌───────────────────────────────────────────────────────────────────┐
  │ daily-aggregator:                                                  │
  │   Checkpoint::Timestamp {                                          │
  │       timestamp: 2025-01-14T23:59:59Z                              │
  │   }                                                                │
  │                                                                    │
  │   Resume: Query events WHERE ts_ingest > '2025-01-14 23:59:59'    │
  └───────────────────────────────────────────────────────────────────┘

BENEFITS
═══════════════════════════════════════════════════════════════════════════════

  ✅ Crash Recovery: Resume exactly where you left off
  ✅ No Data Loss: At-least-once processing guaranteed
  ✅ No Duplication: Idempotency handles replays
  ✅ Type Safety: Enum prevents checkpoint type confusion
  ✅ Schema Evolution: Automatic v1→v2 migration
  ✅ Flexibility: External variant for any checkpoint format
  ✅ Observability: last_activity for staleness detection
```

---

## Conclusion

This visual reference provides comprehensive ASCII art diagrams for all major architectural components of Sinex. These diagrams should be updated as the architecture evolves to maintain accuracy and utility for onboarding, debugging, and system understanding.

**Last Updated:** 2025-01-15
**Maintainers:** Keep these diagrams in sync with code changes
**Usage:** Reference during design reviews, onboarding, incident response

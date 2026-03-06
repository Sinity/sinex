Status: canonical
Last Verified: 2025-09-06 (manual review)
> **Purpose:** Canonical reference for the end-to-end system architecture and pointers to deeper component docs.
# Core Architecture

This is the consolidated architecture overview. It links to and summarizes the canonical documents.

## Mission
- Build a long-lived, user-controlled local event and knowledge system that preserves context, supports fast recall/automation, and remains privacy-preserving by default.

## Key Principles
- User sovereignty and local operation by default
- Single writer + immutable event log with strict provenance
- Open, hackable architecture; graceful evolution via versioned migrations
- Observability by default (journald heartbeat; traceable command/response)

## System Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           SINEX ARCHITECTURE                                  │
│                        Event-Sourced Observability                            │
└─────────────────────────────────────────────────────────────────────────────┘

┌───────────────────────┐         ┌──────────────────────────────────────────┐
│   CAPTURE LAYER       │         │         INGESTION LAYER                  │
│   (nodes)             │         │                                          │
│                       │         │  ┌────────────────────────────────────┐  │
│  ┌─────────────────┐ │         │  │     sinex-ingestd                  │  │
│  │  fs-watcher     │ ├────────▶│  │  ┌──────────────────────────────┐  │  │
│  │  (inotify)      │ │  NATS   │  │  │  MaterialAssembler           │  │  │
│  └─────────────────┘ │  Events │  │  │  - Begin/Slices/End          │  │  │
│                       │         │  │  │  - State machine             │  │  │
│  ┌─────────────────┐ │         │  │  │  - Temp file assembly        │  │  │
│  │  terminal-node  │ ├────────▶│  │  └──────────────────────────────┘  │  │
│  │  (kitty/fish)   │ │         │  │                                      │  │
│  └─────────────────┘ │         │  │  ┌──────────────────────────────┐  │  │
│                       │         │  │  │  JetStreamConsumer           │  │  │
│  ┌─────────────────┐ │         │  │  │  - Batch processing          │  │  │
│  │  desktop-node   │ ├────────▶│  │  │  - Idempotency (Nats-Msg-Id) │  │  │
│  │  (hyprland)     │ │         │  │  │  - DLQ routing               │  │  │
│  └─────────────────┘ │         │  │  └──────────────────────────────┘  │  │
│                       │         │  │                                      │  │
│  ┌─────────────────┐ │         │  │  ┌──────────────────────────────┐  │  │
│  │  system-node    │ ├────────▶│  │  │  Repository Layer            │  │  │
│  │  (metrics)      │ │         │  │  │  - EventRepository           │  │  │
│  └─────────────────┘ │         │  │  │  - SourceMaterialRepository  │  │  │
│                       │         │  │  │  - BlobRepository            │  │  │
│  ┌─────────────────┐ │         │  │  └──────────────────────────────┘  │  │
│  │  journald-node  │ ├────────▶│  │                    ↓                 │  │
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
        │  │  │ UUIDv7 timestamp │  │ large files    │  │ storage        │││
        │  │  └────────────────┘  └────────────────┘  └────────────────┘││
        │  │                                                               ││
        │  │  Indexing: GIN (JSONB), BTREE (ts_coided), GiST (temporal)  ││
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
        │              AUTOMATA LAYER   │   (Event Nodes)                   │
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

## Flow
- nodes → NATS `JetStream` → sinex-ingestd → Postgres (`core.events`) → Automata → Gateway (JSON‑RPC) → CLI.

Data Substrate
- Storage: `PostgreSQL` (+ `TimescaleDB`)
- IDs: UUIDv7 IDs for ordering and distribution
- Event store: `core.events` with strict provenance
- Schema: see `crate/lib/sinex-schema/docs/overview.md` for table details

Streaming & Ingestion
- Messaging: NATS `JetStream` (subjects, durable consumers, explicit acks)
- Backpressure: bounded batches, ack timeouts, lag monitoring
- Ingestion: validation, persistence, idempotency, single writer
- See also: `docs/current/architecture/provenance.md` (Stage-as-you-go + provenance rules) and `docs/vision/streaming-architecture.md` (backpressure guidance)

Security & Operations
- Security model, threat mitigation: `docs/current/architecture/security-architecture.md`
- Ops & integrity: backups, invariants, journald-based observability: `docs/current/architecture/SystemOperations_And_Integrity_Architecture.md`

Schema & Taxonomy
- Schema notes: `crate/lib/sinex-schema/docs/overview.md`
- Event taxonomy: `crate/lib/sinex-schema/docs/event-taxonomy.md`

Implementation Guides
- nodes SDK and patterns: `crate/lib/sinex-node-sdk/docs/overview.md`
- Gateway/CLI: see repository README and `crate/cli` (`sinexctl`)

## Deep Dives

**Pattern Documentation:**
- [type-system-patterns.md](./type-system-patterns.md) — Newtypes, validated types, state machines, compile-time safety
- [distributed-patterns.md](./distributed-patterns.md) — Event sourcing, CQRS, concurrency, idempotency, backpressure
- [observability.md](./observability.md) — Journald monitoring, checkpoint system

**Crate-Specific Diagrams:**
- Ingestd: `crate/core/sinex-ingestd/docs/diagrams.md` — Event sourcing & NATS topology
- Database: `crate/lib/sinex-db/docs/diagrams.md` — Schema & repository architecture
- Testing: `xtask/docs/sandbox/diagrams.md` — Parallel test pool
- Primitives: `crate/lib/sinex-primitives/docs/diagrams.md` — Type system & validation

See also: [Ingestion & Provenance Patterns](provenance.md) for sensor layering, Stage-as-you-go guidance, and timestamp taxonomy.

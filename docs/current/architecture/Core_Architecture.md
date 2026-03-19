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
- Open, hackable architecture with declarative schema convergence and no compatibility shims
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
│  │ system-ingestor │ ├────────▶│  │  │  Repository Layer            │  │  │
│  │ (dbus/journal/  │ │         │  │  │  - EventRepository           │  │  │
│  │  systemd/udev)  │ │         │  │  │  - SourceMaterialRepository  │  │  │
│  └─────────────────┘ │         │  │  │  - BlobRepository            │  │  │
│                       │         │  │  └──────────────────────────────┘  │  │
│                       │         │  └────────────────────┼─────────────────┘  │
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
        │  │  │ core.events    │  │ raw.source_    │  │ core.blobs     │││
        │  │  │ (hypertable)   │  │ material_      │  │                │││
        │  │  │                │  │                │  │                │││
        │  │  │ Partitioned by │  │ registry +      │  │ Large binary   │││
        │  │  │ UUIDv7 timestamp │  │ temporal ledger │  │ storage        │││
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
        │  │ terminal-cmd   │  │ analytics-    │  │ health-automaton   │  │
        │  │ canonicalizer  │  │  automaton    │  │ (health checks)    │  │
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
- IDs: `UUIDv7` IDs for ordering and distribution
- Event store: `core.events` with strict provenance
- Schema: see `crate/lib/sinex-schema/docs/overview.md` for table details

Streaming & Ingestion
- Messaging: NATS `JetStream` (subjects, durable consumers, explicit acks)
- Backpressure: bounded batches, ack timeouts, lag monitoring
- Ingestion: validation, persistence, idempotency, single writer
- See also: `crate/lib/sinex-node-sdk/docs/provenance.md` (Stage-as-you-go + provenance rules), `/realm/project/sinex-target-vision/analysis/foundation/current-state-cartography.md` (JetStream-first pipeline state), and `/realm/project/sinex-target-vision/analysis/collations/design-decisions.md` (message-bus / backpressure design decisions)

Security & Operations
- Security model, threat mitigation: `docs/current/security.md`
- Ops & integrity: backups, invariants, journald-based observability: `docs/current/architecture/SystemOperations_And_Integrity_Architecture.md`

Schema & Taxonomy
- Schema notes: `crate/lib/sinex-schema/docs/overview.md`
- Event taxonomy: `crate/lib/sinex-schema/docs/event-taxonomy.md`

Implementation Guides
- nodes SDK and patterns: `crate/lib/sinex-node-sdk/docs/overview.md`
- Gateway/query surface: `crate/core/sinex-gateway/docs/interaction_and_query.md`; CLI surface: `crate/cli/docs/README.md`

## Deep Dives

**Pattern Documentation:**
- `crate/lib/sinex-primitives/docs/type_system_patterns.md` — Newtypes, validated types, state machines, compile-time safety
- `crate/lib/sinex-node-sdk/docs/distributed_patterns.md` — Event sourcing, CQRS, concurrency, idempotency, backpressure
- `crate/lib/sinex-node-sdk/docs/observability.md` — Journald monitoring, checkpoint system

**Crate-Specific Diagrams:**
- Ingestd: `crate/core/sinex-ingestd/docs/diagrams.md` — Event sourcing & NATS topology
- Database: `crate/lib/sinex-db/docs/diagrams.md` — Schema & repository architecture
- Testing: `xtask/docs/sandbox/diagrams.md` — Parallel test pool
- Primitives: `crate/lib/sinex-primitives/docs/diagrams.md` — Type system & validation

See also: `crate/lib/sinex-node-sdk/docs/provenance.md` for sensor layering, Stage-as-you-go guidance, and timestamp taxonomy.

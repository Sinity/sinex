Status: canonical
Last Verified: 2025-09-06 (manual review)
> **Purpose:** Canonical reference for the live end-to-end system shape and the downward links to owning crate docs.
# Core Architecture

This document owns the cross-cutting architecture of the running system. Detailed subsystem doctrine lives in crate docs.

## System Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           SINEX ARCHITECTURE                               │
│                        Event-Sourced Observability                         │
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
└───────────────────────┘         └───────────────────────┼─────────────────┘
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
        │  └──────────────────────────────────────────────────────────────┘│
        └─────────────────────────────────────────────────────────────────┘
                                        ↑
                                        │ Read Path
        ┌───────────────────────────────┼──────────────────────────────────┐
        │                 QUERY LAYER   │                                   │
        │                               │                                   │
        │  ┌─────────────────────────────────────────────────────────────┐ │
        │  │              sinex-gateway (RPC Server)                    │ │
        │  │                                                           │ │
        │  │  Auth + RBAC + limits + JSON-RPC dispatch                 │ │
        │  └─────────────────────────────────────────────────────────────┘ │
        └───────────────────────────────────────────────────────────────────┘
                                        ↓
        ┌───────────────────────────────┼──────────────────────────────────┐
        │              DERIVED LAYER    │                                   │
        │                               │                                   │
        │  terminal-cmd canonicalizer, analytics automaton, health         │
        │  automaton, and other derived nodes consume confirmed events      │
        │  and emit new ones with replay metadata and checkpoints.          │
        └───────────────────────────────────────────────────────────────────┘
```

## End-To-End Flow

- nodes emit events onto NATS `JetStream`
- `sinex-ingestd` validates, assembles, and persists canonical events into `core.events`
- derived nodes consume confirmed events, maintain checkpoints, and emit synthetic events
- `sinex-gateway` exposes the query/control surface over JSON-RPC
- `sinexctl` is the primary operator and developer client

## Cross-Cutting Invariants

- canonical event persistence flows through `sinex-ingestd`
- `core.events` is append-only; corrections become new events with provenance
- derived events carry source, temporal, and replay metadata
- `UUIDv7` IDs provide ordering; `ts_orig` and `ts_coided` have different meanings and are both load-bearing
- gateway is the default query/control boundary; direct DB access is diagnostic, not the primary interface

## Core Substrates

Data substrate
- `PostgreSQL` + `TimescaleDB`
- `core.events` as the canonical event store
- `raw.source_material*` plus temporal ledger for material provenance

Streaming substrate
- NATS `JetStream` with durable consumers and explicit ack flow
- bounded batch processing, lag monitoring, idempotent ingestion

Read/control substrate
- `sinex-gateway` for RPC and native messaging
- `sinex-services` for query/read-model logic
- `sinexctl` for operator workflows

## Canonical Downward Links

- schema and event taxonomy: `crate/lib/sinex-schema/docs/overview.md`, `crate/lib/sinex-schema/docs/event-taxonomy.md`
- type system and transport contracts: `crate/lib/sinex-primitives/docs/type_system_patterns.md`, `crate/lib/sinex-primitives/docs/nats_subjects.md`
- runtime/distributed behavior: `crate/lib/sinex-node-sdk/docs/distributed_patterns.md`
- observability and checkpoints: `crate/lib/sinex-node-sdk/docs/observability.md`
- provenance and capture layering: `crate/lib/sinex-node-sdk/docs/provenance.md`
- gateway/query surface: `crate/core/sinex-gateway/docs/interaction_and_query.md`
- operations/integrity policy: `docs/current/architecture/SystemOperations_And_Integrity_Architecture.md`
- current security posture: `docs/current/security.md`

Status: canonical
# Sinex Architecture Documentation

This directory contains comprehensive technical architecture documentation for the Sinex system.

Note: Internal messaging uses NATS JetStream. In the current implementation, satellites ingest via gRPC to `sinex-ingestd`, which persists to Postgres and fans out over JetStream. The planned end‑state (see `docs/plan_v3.txt`) is NATS‑native ingestion where satellites publish directly to JetStream and `ingestd` acts as an archiver/consumer.

## Core Architecture Documents

See also: the high-level map in `../Architecture.md` and definitions in `../GLOSSARY.md`.

### System Architecture
- **[Core Architecture](./Core_Architecture.md)** - Consolidated architecture (flow, messaging, ingestion, and data substrate)
- **[System Operations Architecture](./SystemOperations_And_Integrity_Architecture.md)** - Operational concerns, monitoring, backup, and integrity
- **[User Interaction Architecture](./UserInteraction_And_Query_Architecture.md)** - Query interfaces, CLI, and future UI systems
- **[Event Taxonomy](./event-taxonomy.md)** - Canonical event families and minimal payload contracts

- **Satellite SDK Reference**: `crate/lib/sinex-satellite-sdk/doc/overview.md` (shared traits, checkpoints, configuration)
- Event Relations and Tagging System are tracked under roadmap; historical design notes are in `../archive/architecture/`.

### Deprecated/Consolidated
- Metrics & Telemetry: see NixOS/module docs and service logs; a dedicated `MONITORING.md` will be added when available
- Query & Operations: see `UserInteraction_And_Query_Architecture.md`

## Architecture Principles

### 1. Satellite Constellation
- Independent systemd services for each data source
- Unified communication via NATS JetStream
- Checkpoint-based processing with exactly-once semantics
- StatefulStreamProcessor interface for all components

### 2. Data Substrate
- PostgreSQL + TimescaleDB for time-series storage
- ULID primary keys for distributed ordering
- NATS JetStream for real-time event distribution
- Git-annex for large file management

### 3. Event Processing
- Immutable event storage in `core.events`
- Provenance tracking via `source_event_ids`
- JSON Schema validation for all payloads
- Parallel processing with consumer groups

## Reading Order

1. Start with **Data Substrate Architecture** for system overview
2. Read **Ingestion Architecture** for event flow understanding
3. Review **System Operations** for deployment and monitoring
4. Check **User Interaction** for interface design

## Implementation Status

These documents reflect the current operational state of Sinex:
- ✅ Core substrate operational
- ✅ 25+ satellite services deployed
- ✅ Unified event processing pipeline
- ✅ CLI and query interfaces complete
- 🚧 Web UI in planning phase
- 📋 Advanced features documented for future implementation

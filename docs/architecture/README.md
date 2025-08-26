# Sinex Architecture Documentation

This directory contains comprehensive technical architecture documentation for the Sinex system.

Note: Internal messaging uses NATS JetStream. Any references to Redis Streams in older documents are historical and superseded by the streaming design in `streaming-architecture.md`.

## Core Architecture Documents

### System Architecture
- **[Data Substrate Architecture](DataSubstrate_Architecture.md)** - Foundation of Sinex including PostgreSQL, TimescaleDB, NATS JetStream, and the satellite constellation
- **[Streaming Architecture & Backpressure](streaming-architecture.md)** - NATS JetStream staging/events, transactional outbox, and producer/consumer guidance
- **[Ingestion Architecture](IngestionArchitecture_And_TelemetrySources.md)** - Event sources, telemetry patterns, and ingestion pipeline
- **[System Operations Architecture](SystemOperations_And_Integrity_Architecture.md)** - Operational concerns, monitoring, backup, and integrity
- **[User Interaction Architecture](UserInteraction_And_Query_Architecture.md)** - Query interfaces, CLI, and future UI systems

### Implementation Patterns
- **[Satellite Implementation](satellite-implementation.md)** - Patterns for building new satellites
- **[Event Relations](event-relations.md)** - Design for event relationship tracking (planned)
- **[Tagging System](tagging-system.md)** - Comprehensive tagging architecture (planned)

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

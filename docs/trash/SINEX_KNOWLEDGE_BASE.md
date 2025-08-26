# Sinex Codebase Knowledge Base

## Project Overview
- **Purpose**: Event-driven data capture system that records everything happening on a computer
- **Architecture**: Satellite-based distributed system with PostgreSQL + TimescaleDB storage
- **Language**: Rust with async/await patterns
- **Version**: 0.4.2

## Core Architecture
- Satellites → ingestd → PostgreSQL → NATS JetStream → Automata
- ULID-based primary keys for time-ordered, distributed-safe IDs
- Immutable event storage pattern
- JSON Schema validation for event payloads

## Recent Major Changes
- Event<T> type refactoring to unified Event model (commits 4eded568 through 69e252d9)
- Telemetry system removal (~5000 lines deleted)
- Migration from Redis Streams to NATS JetStream

## Key Questions to Investigate
1. ✅ Why was Event<T> refactored to Event? What problems did it solve?
   - Eliminated dangerous Event::new() without provenance
   - Unified constructor pattern with mandatory provenance
   - Improved type safety with NonEmptyVec for synthesis events
   - Simplified mental model (no more RawEvent alias confusion)
2. How do satellites communicate with ingestd?
3. What is the role of StatefulStreamProcessor?
4. How does the checkpoint/replay system work?
5. What patterns ensure event immutability?
6. How is schema validation enforced?
7. What's the relationship between automata and satellites?

## Event Model Architecture (DISCOVERED)
- **Unified Constructor**: Event::create() with mandatory provenance
- **Provenance Types**: Material (with anchor_byte) or Synthesis (with NonEmptyVec parent IDs)
- **Type Conversions**: to_json_event() for erasure, to_typed<T>() for recovery
- **Builder Pattern**: Typestate pattern ensures provenance before build()
- **Default Type**: Event<JsonValue> replaces old RawEvent alias

## Workspace Structure
- 30 workspace members
- Core services: ingestd, gateway, rpc-dispatcher, sensd
- Libraries: sinex-core, sinex-schema, sinex-macros, sinex-satellite-sdk
- Satellites: fs-watcher, terminal, desktop, system, health-aggregator
- Automata: analytics, content, pkm, search

## Technical Patterns Observed
- Builder pattern with bon crate
- Property-based testing with proptest
- Parallel test execution with database pool isolation
- gRPC for satellite communication
- Figment for configuration management

## StatefulStreamProcessor Architecture (DISCOVERED)
- **Unified Interface**: Single trait unifying ingestors and automata
- **Deep Symmetry**: Both read and write event streams
- **TimeHorizon**: Replaces sensor/scanner modes (Snapshot/Historical/Continuous)
- **Checkpoint System**: PostgreSQL-backed state persistence with multiple types
- **Configuration**: Type-safe with validation via validator crate
- **Lifecycle**: initialize → scan → shutdown with optional process_event_batch
- **Context**: Provides db_pool, gRPC client, checkpoint manager

## Sensd Integration Pattern (DISCOVERED from fs-watcher)
- **Material-Based**: Events derived from temporal_ledger material slices
- **TreeWatch Jobs**: Submit sensor_jobs to sensd for monitoring
- **Material Stream**: Query temporal_ledger for material slices
- **Provenance**: Material provenance with anchor_byte from offset_start
- **Processing Loop**: Poll for completed jobs, process materials into events

## Database Architecture (DISCOVERED)
### Schema Organization
- **core**: Canonical synthesized data (events table as TimescaleDB hypertable)
- **raw**: Immutable acquisition records (source_material_registry, temporal_ledger)
- **audit**: Archive of superseded records (archived_events)
- **sinex_schemas**: Event payload schema management
- **metrics**: Materialized views and analytics

### Key Tables
1. **core.events**: Single source of truth, ULID PK, TimescaleDB hypertable
2. **raw.source_material_registry**: Birth certificate for external data
3. **raw.temporal_ledger**: Append-only capture-time provenance
4. **raw.sensor_jobs**: Sensd job tracking (TreeWatch, etc.)
5. **core.processor_checkpoints**: Unified checkpoint storage for all processors
6. **core.outbox**: Transactional outbox for NATS publishing

### Provenance Model
- **XOR Invariant**: Events have EITHER material OR synthesis provenance
- **Material Provenance**: source_material_id + anchor_byte + offsets
- **Synthesis Provenance**: source_event_ids (NonEmptyVec enforced)
- **Idempotency**: Unique constraint on (material_id, anchor_byte, id)

## Satellite Ecosystem (DISCOVERED)
### Ingestor Satellites
- **fs-watcher**: File system events via sensd TreeWatch
- **terminal-satellite**: Shell history, Atuin, terminal recordings
- **desktop-satellite**: Clipboard, window manager events
- **system-satellite**: D-Bus, systemd, udev events
- **document-ingestor**: Document processing and indexing

### Automaton Satellites
- **analytics-automaton**: Frequency analysis, pattern detection
- **content-automaton**: Text analysis, classification, similarity
- **pkm-automaton**: Knowledge extraction, learning sessions
- **search-automaton**: Full-text indexing, search analytics
- **terminal-command-canonicalizer**: Command standardization, safety analysis
- **health-aggregator**: System health monitoring and alerts

## NATS JetStream Integration (DISCOVERED)
- **Outbox Pattern**: PostgreSQL → outbox table → NATS for durability
- **Five Streams**: RAW_EVENTS, PROCESSED_EVENTS, METRICS, ALERTS, SATELLITE_CONTROL
- **Environment Namespacing**: dev.sinex.* vs prod.sinex.*
- **Pull Consumers**: Durable consumers with batch processing
- **Migration from Redis**: 2-3x performance improvement, better persistence

## Checkpoint & Replay Systems (DISCOVERED)
- **Five Checkpoint Types**: None, External, Internal, Stream, Timestamp
- **Atomic Operations**: PostgreSQL-backed with optimistic concurrency
- **Corruption Recovery**: Graceful fallback to Checkpoint::None
- **Replay Modes**: Time range, source-based, event type, custom filters
- **Progress Tracking**: Real-time ETA with pause/resume capabilities
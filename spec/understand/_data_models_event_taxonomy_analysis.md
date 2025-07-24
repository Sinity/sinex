# Data Models, Event Taxonomy, and Information Architecture Analysis

## Executive Summary

Sinex implements a sophisticated event-driven architecture centered around immutable event capture, type-safe processing, and knowledge synthesis. The system uses a hierarchical event taxonomy across 7 primary domains, with 27 strongly-typed payload structures ensuring data integrity. The architecture balances flexibility with type safety through a three-layer data model: raw event storage, knowledge graph extraction, and analytics aggregation.

## Event Taxonomy

### Primary Event Domains

#### 1. System Events (`sinex.*`)
Internal system lifecycle and operational events:
- **Automaton lifecycle**: `automaton.startup`, `automaton.shutdown`, `automaton.heartbeat`, `automaton.error`
- **Scanner operations**: `scan.started`, `scan.completed`
- **Process management**: `process.started`, `process.heartbeat`, `process.shutdown`
- **Sensor control**: `sensor.activated`, `sensor.deactivated`
- **Health monitoring**: `system.health.summary`

#### 2. Filesystem Events (`file.*`, `dir.*`)
File and directory operations with full provenance:
- **File operations**: `file.created`, `file.modified`, `file.deleted`, `file.moved`
- **Directory operations**: `dir.created`, `dir.deleted`
- **Payload structure**: Includes path, size, timestamps, permissions, and modification types

#### 3. Terminal/Shell Events (`command.*`, `session.*`)
Command execution and shell session tracking:
- **Command lifecycle**: `command.executed`, `command.completed`, `command.failed`, `command.imported`
- **Session management**: `session.started`, `session.ended`
- **Recording**: `recording.started`, `recording.ended`
- **Terminal UI**: `tab.created`, `tab.focused`, `tab.closed`
- **Historical imports**: `shell.command.historical`, `entry.imported`

#### 4. Window Manager Events (`window.*`, `workspace.*`)
Desktop environment state tracking:
- **Window lifecycle**: `window.opened`, `window.closed`, `window.focused`, `window.moved`, `window.resized`
- **Workspace management**: `workspace.switched`, `workspace.created`, `workspace.destroyed`
- **Display events**: `display.connected`, `display.disconnected`, `monitor.focused`
- **State capture**: `state.captured`

#### 5. Clipboard Events (`clipboard.*`)
Clipboard content tracking with privacy considerations:
- **Operations**: `clipboard.copied`, `clipboard.selected`
- **Payload includes**: content_type, content_size, text_preview, content_hash, source_app

#### 6. System Integration Events
Events from system services and D-Bus:
- **D-Bus signals**: `signal.received`, `method.called`, `notification.sent`
- **Device management**: `device.connected`, `device.disconnected`, `device.changed`
- **State changes**: Media, power, network, bluetooth, session, screensaver states
- **Systemd units**: `unit.started`, `unit.stopped`, `unit.changed`
- **Journal entries**: `entry.written`, `sync.completed`

#### 7. Metrics Events (`metrics.*`)
System telemetry and performance tracking:
- **Blob storage**: `metrics.blob_storage.operation`, `metrics.blob_storage.statistics`
- **Performance metrics**: Custom metrics per processor

### Event Naming Conventions

1. **Hierarchical dot notation**: `domain.object.action` (e.g., `file.created`, `window.focused`)
2. **Consistent verb tenses**: Past tense for completed actions, present progressive for ongoing
3. **Source prefixes**: Multi-level sources like `shell.kitty`, `wm.hyprland`
4. **Alternative patterns**: Some legacy events use underscores (e.g., `window_manager.window.focused`)

## Core Data Models

### 1. Event Storage Layer

#### Primary Event Table (`core.events`)
```sql
- event_id: ULID (time-ordered, distributed-safe)
- ts_ingest: TIMESTAMPTZ (auto-generated from ULID)
- event_type: TEXT (dot-notation taxonomy)
- source: TEXT (processor that created the event)
- ts_orig: TIMESTAMPTZ (conceptual timestamp from source)
- host: TEXT
- payload: JSONB (validated against schema)
- payload_schema_id: ULID (reference to schema registry)
- source_event_ids: ULID[] (provenance chain)
- source_material_id: ULID (external data reference)
- associated_blob_ids: ULID[] (related binary data)
```

**Key Design Decisions:**
- ULID primary keys provide time-ordering without coordination
- Immutable events - no updates allowed after creation
- Full provenance tracking via source_event_ids array
- TimescaleDB hypertable partitioning by ULID timestamp

#### Source Material Registry (`raw.source_material_registry`)
Tracks external data sources:
- Files, streams, API responses that events are derived from
- Checksum tracking (BLAKE3) for deduplication
- Retention policies and archival status
- Content preview for debugging

#### Processor Manifests (`core.processor_manifests`)
Registry of all event producers:
- Tracks ingestors, automata, and system processors
- Version and configuration management
- Lifecycle tracking (start/end times)
- Enables processor-specific query filtering

### 2. Knowledge Graph Layer

#### Entities (`core.entities`)
Extracted semantic entities:
- Type-based classification (person, file, command, process, etc.)
- Canonical names with aliases
- Merge tracking for entity resolution
- Creation provenance from events

#### Entity Relations (`core.entity_relations`)
Semantic relationships:
- Directional relationships with types
- Strength scoring (0-1)
- Temporal validity (valid_from/valid_until)
- Provenance tracking to source events

#### Knowledge Management (`km.*` schema)
Advanced knowledge extraction:
- **Concepts**: Semantic concepts with vector embeddings
- **Relations**: Typed relationships between concepts
- **Event Annotations**: Links events to concepts
- **Artifacts**: Knowledge documents with versioning
- **LLM Interactions**: Track AI-assisted knowledge extraction

### 3. Analytics Layer

#### Materialized Views
Pre-aggregated metrics for performance:
- `event_counts_by_type_1h`: Hourly event volume analysis
- `process_heartbeats_1h`: Process health aggregations
- Refresh via `metrics.refresh_materialized_views()`

#### Real-time Views
Dynamic analytics without materialization:
- `event_processing_lag`: Ingestion latency analysis
- `system_health`: Live process health dashboard
- `event_throughput`: Per-minute throughput metrics

### 4. Coordination Layer

#### Satellite Coordination
Distributed processor management:
- **Satellite Instances**: Registry of all running processors
- **Service Leadership**: Singleton service coordination
- **Satellite Signals**: Inter-processor messaging
- Automatic cleanup of stale instances

#### Checkpointing System (`core.automaton_checkpoints`)
Reliable event processing:
- Per-automaton processing state
- Consumer group support for parallel processing
- Checkpoint versioning for schema evolution
- Last processed event tracking

## Information Flow Architecture

### 1. Ingestion Pipeline
```
External Sources → Satellites → Validation → PostgreSQL → Redis Streams → Automata
```

**Key Characteristics:**
- Satellites are stateless ingestors capturing raw events
- JSON Schema validation at ingestion boundary
- Immediate persistence to PostgreSQL for durability
- Redis streams for real-time processing distribution

### 2. Processing Pipeline
```
Raw Events → Automata → Synthesis Events → Knowledge Extraction → Analytics
```

**Processing Patterns:**
- **Stateful Stream Processing**: Automata maintain checkpoints for reliable processing
- **Event Synthesis**: Automata create higher-level events from raw events
- **Provenance Preservation**: All synthesis tracks source_event_ids
- **Concurrent Processing**: Multiple automata can process same events

### 3. Query Pipeline
```
Applications → QueryBuilder → Type Conversion → PostgreSQL → Result Mapping
```

**Query Capabilities:**
- Automatic ULID ↔ UUID conversion
- Type-safe parameter binding
- Time-range and source filtering
- Provenance chain traversal

## Query Patterns and Optimization

### 1. Time-Range Queries
Primary access pattern for event data:
```rust
QueryBuilder::select(tables::EVENTS)
    .where_gte("ts_ingest", timestamp)
    .order_by("ts_ingest", "DESC")
    .limit(1000)
```

**Optimizations:**
- TimescaleDB chunk pruning via ULID partitioning
- Descending indexes on ts_ingest and ts_orig
- Composite indexes for source+time queries

### 2. Source-Based Filtering
Efficient filtering by event producer:
```rust
QueryBuilder::select(tables::EVENTS)
    .where_eq("source", "fs-watcher")
    .where_eq("event_type", "file.modified")
```

**Index Strategy:**
- `(source, event_type, ts_ingest DESC)` composite index
- Separate indexes for high-cardinality sources

### 3. Provenance Queries
Tracing event lineage:
```rust
// Find synthesis events from specific raw events
QueryBuilder::select(tables::EVENTS)
    .where_op("source_event_ids", "@>", ulid_array)
```

**GIN Index Support:**
- Array containment queries via GIN indexes
- Efficient provenance chain traversal

### 4. Entity-Centric Queries
Knowledge graph navigation:
```sql
-- Find all events related to an entity
SELECT e.* FROM core.events e
JOIN km.event_annotations a ON e.event_id = a.event_id
WHERE a.concept_id = $1
```

### 5. Real-time Monitoring
System health and metrics:
```sql
-- Active processors in last 5 minutes
SELECT DISTINCT source, host, MAX(ts_ingest) as last_seen
FROM core.events
WHERE event_type = 'process.heartbeat'
  AND ts_ingest > NOW() - INTERVAL '5 minutes'
GROUP BY source, host
```

## Schema Evolution Strategy

### 1. JSON Schema Versioning
- Each event type has versioned JSON schemas
- Schemas stored in `sinex_schemas.event_payload_schemas`
- pg_jsonschema extension for runtime validation
- GitOps integration for schema deployment

### 2. Compatibility Tracking
- Forward/backward compatibility matrix
- Migration strategies stored with schemas
- Validation cache for performance

### 3. Evolution Patterns
- **Additive Changes**: New optional fields (backward compatible)
- **Type Widening**: int → float, string → string[] (forward compatible)
- **Breaking Changes**: New schema version with migration automaton
- **Deprecation**: Mark schemas inactive with reason and timeline

## Data Integrity and Constraints

### 1. Database Constraints
- ULID primary keys ensure uniqueness
- Foreign key relationships for provenance
- Check constraints on enum-like fields
- NOT NULL constraints on critical fields

### 2. Application-Level Validation
- Strongly-typed event payloads in Rust
- JSON Schema validation at boundaries
- Type-safe QueryBuilder prevents SQL injection
- Compile-time payload type checking

### 3. Temporal Integrity
- Immutable events after creation
- Append-only event log
- Checkpoint versioning for replay safety
- Temporal validity on relationships

## Performance Characteristics

### 1. Write Performance
- Bulk insert optimization for high-volume ingestion
- Async I/O with connection pooling
- Minimal indexes on write path
- Deferred constraint checking where safe

### 2. Read Performance
- TimescaleDB chunk pruning for time queries
- Materialized views for common aggregations
- Query result caching in application layer
- Connection pooling and prepared statements

### 3. Storage Efficiency
- JSONB compression for payloads
- TimescaleDB compression policies
- Automatic data retention/archival
- Deduplicated blob storage

## Recommendations

### 1. Schema Improvements
- **Standardize event naming**: Migrate legacy underscore patterns to dots
- **Payload normalization**: Extract common fields (user, session) to top level
- **Schema inheritance**: Base schemas for event categories
- **Validation tooling**: CLI for schema testing before deployment

### 2. Performance Optimizations
- **Continuous aggregates**: Replace materialized views with TimescaleDB continuous aggregates
- **Parallel ingestion**: Implement partitioned Redis streams for higher throughput
- **Query caching**: Add Redis-based query result caching
- **Index advisor**: Implement automated index recommendation system

### 3. Data Quality Enhancements
- **Entity resolution**: Implement fuzzy matching for entity deduplication
- **Anomaly detection**: Add statistical anomaly detection on event streams
- **Data lineage UI**: Visual provenance tracking interface
- **Quality metrics**: Dashboard for schema validation failures

### 4. Extensibility Patterns
- **Plugin system**: Dynamic automaton loading without recompilation
- **Custom event types**: Self-service event type registration
- **Webhook integrations**: External system notifications
- **GraphQL API**: Flexible query interface for analytics

## Conclusion

Sinex's information architecture demonstrates sophisticated design patterns for event-driven systems. The combination of immutable event storage, strongly-typed processing, and flexible knowledge extraction creates a powerful platform for comprehensive digital activity capture and analysis. The system successfully balances performance, flexibility, and data integrity while maintaining clear extension points for future growth.
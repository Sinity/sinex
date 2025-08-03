# Sinex Database Schema

## Overview
The Sinex database uses PostgreSQL with TimescaleDB for time-series optimization and pgvector for embeddings. All tables use ULID (Universally Unique Lexicographically Sortable Identifier) as primary keys for time-ordered, distributed-safe identification.

**Current Implementation Status**:
- ✅ Core event storage and processing (core schema)
- ✅ Source material tracking (raw schema)
- ✅ Schema registry and validation (sinex_schemas)
- ✅ System telemetry (metrics schema)
- ✅ Knowledge management in core schema (artifacts, entities, relations)
- ❌ Event relations not yet implemented (planned)
- ❌ Artifact management abandoned (architecture changed)

## Architectural Decisions

### ULID Primary Keys (ADR-009)
- Time-ordered for efficient range queries
- Lexicographically sortable for natural ordering
- Distributed-safe without coordination
- Generated via pgx_ulid extension

### TimescaleDB for Events (ADR-002)
- Automatic time-based partitioning
- Efficient retention policies
- Optimized time-series queries
- Compression for older partitions

### pgvector for Embeddings (ADR-005, ADR-007)
- IVFFlat indexes for CPU efficiency
- 1536-dimensional vectors for modern models
- Cosine similarity for semantic search
- Local-first approach avoiding external vector DBs

## Schemas
- **core**: Main event storage and entity management ✅
- **raw**: Source material registry ✅
- **metrics**: System telemetry ✅
- **sinex**: Legacy compatibility (deprecated) ⚠️
- **sinex_schemas**: Schema registry and validation ✅

## Core Tables

### core.events (TimescaleDB Hypertable)
Main event storage table, partitioned by time using ULID-based partitioning. This is the heart of Sinex - an immutable, append-only event store.

**Key Columns:**
- **id**: ULID (PK) - Unique event identifier (renamed from event_id in original design)
- **ts_ingest**: TIMESTAMPTZ - Ingestion timestamp (generated from ULID)
- **event_type**: TEXT - Type of event (e.g., 'desktop.window_focused', 'filesystem.file_modified')
- **source**: TEXT - Processor that created this event (e.g., 'ingestor.hyprland', 'automaton.canonicalizer')
- **ts_orig**: TIMESTAMPTZ - Original timestamp from source system
- **host**: TEXT - Host where event originated
- **payload**: JSONB - Event data (validated against schema)
- **ingestor_version**: TEXT - Version of the ingestor that created this event
- **payload_schema_id**: ULID - Reference to schema used for validation
- **payload_schema_name**: TEXT - Denormalized schema name for queries
- **payload_schema_version**: TEXT - Denormalized schema version

**Provenance Tracking:**
- **source_material_id**: ULID (FK) - Link to raw.source_material_registry
- **source_material_offset_start**: BIGINT - Start position in source material
- **source_material_offset_end**: BIGINT - End position in source material
- **anchor_byte**: BIGINT - Primary offset for precise location
- **source_event_ids**: ULID[] - Internal provenance chain (for derived events)
- **associated_blob_ids**: ULID[] - Large files associated with this event
- **processor_manifest_id**: INTEGER (FK) - Which processor instance created this

**Design Principles:**
- Append-only (no UPDATE/DELETE allowed)
- Schema-validated payloads via pg_jsonschema
- Complete provenance tracking
- Time-based partitioning via TimescaleDB

### core.processor_manifests
Registry of event processors (ingestors and automata). Tracks which processor instance created each event.

**Implementation Status**: ✅ Fully implemented

- **manifest_id**: SERIAL (PK) - Auto-incrementing ID
- **processor_name**: TEXT - Name of the processor (e.g., 'fs-watcher', 'canonicalizer')
- **processor_version**: TEXT - Version for debugging
- **processor_type**: TEXT - CHECK IN ('ingestor', 'automaton', 'system')
  - ingestor: Captures raw events
  - automaton: Processes/enriches events
  - system: Internal Sinex operations
- **hostname**: TEXT - Where processor is running
- **start_time**: TIMESTAMPTZ - Session start
- **end_time**: TIMESTAMPTZ - Session end (NULL if running)
- **config**: JSONB - Runtime configuration
- **metadata**: JSONB - Additional context
- **created_at**: TIMESTAMPTZ

**Usage**: Each processor registers a manifest on startup. Events reference via processor_manifest_id.

### core.entities
Knowledge graph nodes representing canonical concepts. Part of the implemented entity system (not obsolete).

**Implementation Status**: ✅ Exists in migration m20240101_000001_core_infrastructure

- **id**: ULID (PK)
- **type**: TEXT - Entity type (people, projects, technologies, organizations, etc.)
- **name**: TEXT - Display name
- **canonical_name**: TEXT - Normalized name for matching
- **aliases**: TEXT[] - Alternative names
- **description**: TEXT - Human-readable description
- **metadata**: JSONB - Flexible properties
- **merged_into_id**: ULID (FK) - For entity resolution/deduplication
- **created_at**: TIMESTAMPTZ
- **updated_at**: TIMESTAMPTZ
- **created_from_event_id**: ULID - Provenance tracking

**Design Intent**:
- Supports entity resolution through merge chains
- Aliases enable matching variations of names
- Type system is extensible via TEXT (not enum)

### core.entity_relations
Relationships between entities with temporal validity. Part of the implemented entity system (not obsolete).

**Implementation Status**: ✅ Exists and actively used

- **id**: ULID (PK)
- **from_entity_id**: ULID (FK) - Source entity
- **to_entity_id**: ULID (FK) - Target entity
- **relation_type**: TEXT - Semantic type (e.g., 'works_for', 'authored_by', 'member_of')
- **strength**: DOUBLE PRECISION (0-1) - Confidence or importance
- **metadata**: JSONB - Additional properties
- **valid_from**: TIMESTAMPTZ - When relationship started
- **valid_until**: TIMESTAMPTZ - When relationship ended (NULL if current)
- **created_at**: TIMESTAMPTZ
- **updated_at**: TIMESTAMPTZ
- **created_from_event_id**: ULID - Which event established this relation

**Note**: Event relations (event_relations table) were planned but not implemented. The design considered a unified _relations table for both events and entities.

### core.processor_checkpoints
Processing state for all event processors (ingestors, automata, system).
- **id**: ULID (PK)
- **processor_name**: TEXT
- **consumer_group**: TEXT
- **consumer_name**: TEXT
- **last_processed_id**: ULID
- **last_processed_ts**: TIMESTAMPTZ
- **processed_count**: BIGINT
- **checkpoint_data**: JSONB
- **state_data**: JSONB
- **checkpoint_version**: INTEGER
- **last_activity**: TIMESTAMPTZ
- **created_at**: TIMESTAMPTZ
- **updated_at**: TIMESTAMPTZ

### core.operations_log
Audit trail for administrative operations.
- **operation_id**: ULID (PK)
- **operation_ts**: TIMESTAMPTZ (generated from ULID)
- **operation_type**: TEXT
- **operator**: TEXT
- **target_table**: TEXT
- **target_id**: TEXT
- **operation_data**: JSONB
- **result_status**: TEXT - CHECK IN ('success', 'failure', 'partial')
- **result_message**: TEXT
- **duration_ms**: INTEGER
- **metadata**: JSONB
- **created_at**: TIMESTAMPTZ

### core.satellite_instances
Registry of active satellite services. Used for monitoring and coordination.

**Implementation Status**: ✅ Implemented in migration m20240101_000001_core_infrastructure

- **instance_id**: TEXT (PK) - Unique instance identifier
- **satellite_name**: TEXT - Type of satellite (fs-watcher, terminal, etc.)
- **instance_type**: TEXT - CHECK IN ('sensor', 'scanner')
  - sensor: Continuous monitoring mode
  - scanner: Batch processing mode
- **hostname**: TEXT - Where satellite is running
- **status**: TEXT - CHECK IN ('running', 'stopped', 'error')
- **last_heartbeat**: TIMESTAMPTZ - Liveness indicator
- **started_at**: TIMESTAMPTZ - Instance start time
- **config**: JSONB - Runtime configuration
- **metadata**: JSONB - Additional properties

**Usage**: Satellites register on startup and send periodic heartbeats. Used by orchestration for health monitoring.

### core.satellite_signals
Control signals for satellite coordination. Enables command and control of satellite fleet.

**Implementation Status**: ✅ Implemented in migration m20240101_000001_core_infrastructure

- **signal_id**: ULID (PK)
- **signal_type**: TEXT - CHECK IN ('scan_request', 'control', 'config_update')
  - scan_request: Trigger batch scan of specific paths
  - control: Start/stop/restart commands
  - config_update: Runtime configuration changes
- **target_satellite**: TEXT - Which satellite(s) to target
- **payload**: JSONB - Signal-specific data
- **status**: TEXT - CHECK IN ('pending', 'acknowledged', 'completed', 'failed')
- **created_at**: TIMESTAMPTZ - When signal was created
- **acknowledged_at**: TIMESTAMPTZ - When satellite received it
- **completed_at**: TIMESTAMPTZ - When processing finished
- **result**: JSONB - Execution result/errors

**Usage Pattern**: Control plane writes signals, satellites poll and update status.

### core.service_leadership
Distributed service coordination using PostgreSQL advisory locks.

**Implementation Status**: ✅ Implemented for high-availability deployments

- **service_name**: TEXT (PK) - Service requiring leadership
- **instance_id**: TEXT - Current leader instance
- **acquired_at**: TIMESTAMPTZ - When leadership acquired
- **expires_at**: TIMESTAMPTZ - TTL for automatic expiry
- **metadata**: JSONB - Leader-specific data

**Implementation Details**:
- Uses pg_advisory_lock for atomic leadership acquisition
- Heartbeat extends expires_at to maintain leadership
- On expiry, any instance can claim leadership
- Prevents split-brain in multi-instance deployments

## Raw Data Tables

### raw.source_material_registry
External data provenance tracking. Critical for maintaining the chain of custody from raw inputs to processed events.

**Implementation Status**: ✅ Fully implemented

- **blob_id**: ULID (PK) - Unique identifier
- **material_type**: TEXT - Type of source (file, api_response, stream_capture, etc.)
- **source_uri**: TEXT - Original location (file path, URL, etc.)
- **ingestion_time**: TIMESTAMPTZ - When ingested into Sinex
- **file_size_bytes**: BIGINT - Size for capacity planning
- **checksum_blake3**: TEXT - Content integrity verification
- **mime_type**: TEXT - Content type identification
- **encoding**: TEXT - Character encoding if applicable
- **metadata**: JSONB - Source-specific metadata
- **content_preview**: TEXT - First N bytes/chars for quick inspection
- **is_archived**: BOOLEAN - Whether moved to cold storage
- **archive_time**: TIMESTAMPTZ - When archived
- **retention_policy**: TEXT - How long to keep
- **created_at**: TIMESTAMPTZ
- **updated_at**: TIMESTAMPTZ

**Usage Pattern**:
- Ingestors register source material before processing
- Events reference via source_material_id
- Enables full provenance from event back to original source

## Knowledge Management Tables

Knowledge management functionality is integrated into the core schema alongside event processing.

### core.artifacts
Central registry of all knowledge artifacts in the system.

**Implementation Status**: ✅ Fully implemented

- **id**: ULID (PK)
- **type**: TEXT - artifact type (note, webpage, email, file, document, code, media, pkm_note, task_item)
- **title**: TEXT - Human-readable title
- **source_url**: TEXT - Original URL if applicable
- **original_path**: TEXT - Original file path if applicable
- **mime_type**: TEXT - MIME type of content
- **size_bytes**: BIGINT - File size if applicable
- **checksum**: TEXT - Content hash for integrity
- **metadata**: JSONB - Type-specific properties
- **created_at**: TIMESTAMPTZ
- **updated_at**: TIMESTAMPTZ
- **deleted_at**: TIMESTAMPTZ - Soft delete support
- **created_from_event_id**: ULID (FK) - Event that created this artifact
- **blob_id**: ULID (FK) - Reference to core.blobs for large files

### core.artifact_contents
Versioned content storage for artifacts.

**Implementation Status**: ✅ Fully implemented

- **id**: ULID (PK)
- **artifact_id**: ULID (FK) - Parent artifact
- **version**: INTEGER - Sequential version number
- **content**: TEXT - Actual text content
- **content_type**: TEXT - Content format (text/plain, text/markdown, etc.)
- **extracted_text**: TEXT - Searchable text from binary formats
- **word_count**: INTEGER - For reading time estimates
- **char_count**: INTEGER - Character count
- **metadata**: JSONB - Version-specific metadata
- **created_at**: TIMESTAMPTZ
- **created_from_event_id**: ULID (FK) - Event that created this version

**Constraints**:
- UNIQUE(artifact_id, version)

### core.blobs
Registry of git-annex managed binary files.

**Implementation Status**: ✅ Fully implemented

- **id**: ULID (PK)
- **annex_key**: TEXT (UNIQUE) - Git-annex key
- **original_filename**: TEXT - Original file name
- **size_bytes**: BIGINT - File size
- **mime_type**: TEXT - MIME type
- **checksum_sha256**: TEXT - SHA256 hash
- **checksum_blake3**: TEXT - BLAKE3 hash
- **storage_backend**: TEXT - Storage type (default: 'git-annex')
- **metadata**: JSONB - Additional properties
- **created_at**: TIMESTAMPTZ
- **last_verified_at**: TIMESTAMPTZ - Last integrity check
- **verification_status**: TEXT - Status (pending, verified, missing, corrupted)

### core.tags
Hierarchical tagging system.

**Implementation Status**: ✅ Fully implemented

- **id**: ULID (PK)
- **name**: TEXT (UNIQUE) - Tag identifier
- **display_name**: TEXT - User-friendly name
- **color**: TEXT - Hex color for UI
- **icon**: TEXT - Icon identifier
- **parent_id**: ULID (FK) - Parent tag for hierarchy
- **metadata**: JSONB - Additional properties
- **created_at**: TIMESTAMPTZ

## Metrics Tables

### metrics.sinex_metrics
System telemetry data. Stores Prometheus-style metrics for internal observability.

**Implementation Status**: ✅ Implemented in migration m20240101_000001_core_infrastructure

- **metric_id**: ULID (PK)
- **metric_ts**: TIMESTAMPTZ - Metric timestamp
- **metric_name**: TEXT - Metric identifier (e.g., 'events_ingested_total')
- **metric_value**: DOUBLE PRECISION - Numeric value
- **metric_type**: TEXT - CHECK IN ('counter', 'gauge', 'histogram', 'summary')
  - counter: Monotonically increasing
  - gauge: Point-in-time value
  - histogram: Bucketed distribution
  - summary: Percentile distribution
- **labels**: JSONB - Prometheus-style labels
- **source**: TEXT - Which component emitted metric
- **host**: TEXT - Hostname for multi-host setups
- **created_at**: TIMESTAMPTZ

**Note**: This is a TimescaleDB hypertable partitioned by metric_ts for efficient time-series queries.

## Schema Registry Tables

### sinex_schemas.event_payload_schemas
JSON schema registry for event validation. Central to ensuring data quality.

**Implementation Status**: ✅ Fully implemented with pg_jsonschema integration

- **schema_id**: ULID (PK)
- **schema_name**: TEXT - Unique schema identifier
- **schema_version**: TEXT - Semantic version
- **schema_json**: JSONB - JSON Schema definition
- **created_at**: TIMESTAMPTZ
- **created_by**: TEXT - Who registered schema
- **is_active**: BOOLEAN - Whether schema accepts new events
- **deprecated_at**: TIMESTAMPTZ - When schema was deprecated

**Constraints**:
- UNIQUE (schema_name, schema_version)
- CHECK (schema_json validates as JSON Schema)

**Usage**: 
- Ingestors register schemas for their event types
- Validation happens via pg_jsonschema before insert
- Schema evolution tracked via version history

### sinex_schemas.schema_compatibility
Schema version compatibility tracking.
- **id**: ULID (PK)
- **schema_name**: TEXT
- **from_version**: TEXT
- **to_version**: TEXT
- **is_compatible**: BOOLEAN
- **compatibility_type**: TEXT
- **breaking_changes**: JSONB
- **tested_at**: TIMESTAMPTZ
- **tested_by**: TEXT

### sinex_schemas.validation_cache
Caches validation results for performance. Reduces repeated validation overhead for identical payloads.

- **cache_key**: TEXT (PK) - Hash of payload + schema for uniqueness
- **event_type**: TEXT - Type of event validated
- **schema_name**: TEXT - Name of schema used
- **schema_version**: TEXT - Version of schema used
- **payload_hash**: TEXT - BLAKE3 hash of the payload
- **is_valid**: BOOLEAN - Validation result
- **validation_errors**: JSONB - Array of validation errors if invalid
- **validated_at**: TIMESTAMPTZ - When validation occurred
- **expires_at**: TIMESTAMPTZ - Cache expiry for cleanup

**Design Note**: Uses TTL-based expiry to prevent unbounded growth. Background job periodically cleans expired entries.

## Migration History

The database schema is organized into three comprehensive migrations using the sea-orm-migration system:

1. **m20240101_000001_core_infrastructure**: Complete core infrastructure
   - PostgreSQL extensions (uuid-ossp, TimescaleDB, pg_jsonschema, ULID)
   - All database schemas (core, raw, audit, sinex_schemas, metrics)
   - Core tables with all columns from the start
   - Schema validation infrastructure with content hashing
   - All indexes and constraints
   - Updated_at triggers

2. **m20240102_000002_functions_and_views**: All functions and analytics
   - Query helper functions (get_recent_events, search_events, etc.)
   - Analytics materialized views for metrics
   - Schema management functions
   - Entity relationship queries
   - Test data generation utilities

3. **m20240103_000003_advanced_features**: LLM and advanced capabilities
   - Complete LLM infrastructure (models, prompts, interactions)
   - Vector embeddings for semantic search
   - Event annotations and clustering
   - Data retention policies
   - Processing pipeline definitions

## Key Design Patterns

1. **ULID Primary Keys**: All main tables use ULID for time-ordered, distributed-safe IDs
   - Generated via pgx_ulid extension's gen_ulid() function
   - Provides natural time ordering without separate timestamp index
   - Enables distributed ID generation without coordination

2. **TimescaleDB**: core.events is a hypertable partitioned by time
   - Automatic partitioning by ts_ingest (derived from ULID)
   - Compression for older partitions to save space
   - Continuous aggregates for performance (future)
   - Data retention policies for automatic cleanup

3. **Provenance Tracking**: Events track their source material and derivation chain
   - source_material_id links to original raw data
   - source_event_ids array tracks internal derivation
   - Byte offsets for precise location in source files
   - Complete audit trail from raw data to derived events

4. **Temporal Validity**: Relations and entities support time-based validity
   - valid_from/valid_until for entity relations
   - Enables historical queries and relationship evolution
   - Supports entity merging with temporal consistency

5. **Schema Evolution**: Comprehensive schema versioning and compatibility tracking
   - JSON Schema validation via pg_jsonschema
   - Version compatibility matrix in schema_compatibility table
   - Backward/forward compatibility testing
   - Migration path documentation

6. **Vector Embeddings**: pgvector with IVFFlat indexes for semantic search
   - 1536-dimensional vectors for modern embedding models
   - IVFFlat chosen over HNSW for build speed and memory efficiency
   - Cosine similarity for semantic matching
   - Content deduplication via BLAKE3 hashing

7. **Audit Trail**: Operations log tracks all administrative actions
   - Captures who, what, when for all admin operations
   - Result status and duration for performance analysis
   - JSONB metadata for flexible additional context

8. **Service Coordination**: PostgreSQL advisory locks for distributed coordination
   - Used for leadership election in satellite services
   - Prevents duplicate processing in multi-instance deployments
   - Heartbeat-based expiry for automatic failover

## Related Documentation

- **Architecture**: docs/architecture/STAD.md - System Technical Architecture
- **Development**: docs/development/satellite-development-guide.md - Building satellites
- **Features**: 
  - docs/roadmap/features/embeddings-and-semantic-search.md - Semantic search design
  - docs/roadmap/features/database-encryption-pgsodium.md - Field-level encryption
- **Operations**:
  - crate/sinex-db/src/queries/ - Query builders and database interface
  - crate/sinex-db/migration/src/ - Sea-ORM migration definitions

## Schema Validation

All event payloads are validated against JSON schemas stored in sinex_schemas.event_payload_schemas. Use the schema registry to:

1. Register new event types with their schemas
2. Validate payloads before insertion
3. Track schema evolution and compatibility
4. Cache validation results for performance

See crate/sinex-ingestd/src/validation.rs for the implementation.

## AI/LLM Infrastructure Tables

### core.llm_models
Registry of available LLM models and their capabilities.

**Implementation Status**: ✅ Fully implemented

- **id**: ULID (PK)
- **provider**: TEXT - Provider name (openai, anthropic, local, etc.)
- **model_name**: TEXT - Model identifier
- **model_version**: TEXT - Version if applicable
- **capabilities**: TEXT[] - Array of capabilities (chat, completion, embeddings, vision)
- **context_window**: INTEGER - Maximum context size
- **max_output_tokens**: INTEGER - Maximum response size
- **cost_per_1k_input_tokens**: DECIMAL(10,6) - Input cost
- **cost_per_1k_output_tokens**: DECIMAL(10,6) - Output cost
- **is_active**: BOOLEAN - Whether model is available
- **metadata**: JSONB - Additional properties
- **created_at**: TIMESTAMPTZ
- **deprecated_at**: TIMESTAMPTZ - When model was deprecated

### core.prompts
Reusable prompt templates.

**Implementation Status**: ✅ Fully implemented

- **id**: ULID (PK)
- **name**: TEXT (UNIQUE) - Template identifier
- **category**: TEXT - Category (summarization, extraction, analysis, etc.)
- **template**: TEXT - Template with {{variables}}
- **system_prompt**: TEXT - System prompt if applicable
- **variables**: JSONB - Expected variables and types
- **model_constraints**: JSONB - Model requirements
- **version**: INTEGER - Version number
- **is_active**: BOOLEAN - Whether template is active
- **metadata**: JSONB - Additional properties
- **created_at**: TIMESTAMPTZ
- **updated_at**: TIMESTAMPTZ

### core.embedding_cache
Cache for computed embeddings to avoid redundant API calls.

**Implementation Status**: ✅ Fully implemented

- **id**: ULID (PK)
- **text_hash**: TEXT - SHA256 hash of input text
- **embedding_model_id**: ULID (FK) - Model used
- **embedding**: vector(1536) - Computed embedding
- **text_sample**: TEXT - First 1000 chars for debugging
- **use_count**: INTEGER - Usage counter for LRU
- **created_at**: TIMESTAMPTZ
- **last_used_at**: TIMESTAMPTZ - For LRU eviction

**Constraints**:
- UNIQUE(text_hash, embedding_model_id)

**Indexes**:
- B-tree on text_hash
- B-tree on last_used_at for LRU
- IVFFlat on embedding for similarity search
# Sinex Database Schema

## Overview
The Sinex database uses PostgreSQL with TimescaleDB for time-series optimization and pgvector for embeddings. All tables use ULID (Universally Unique Lexicographically Sortable Identifier) as primary keys for time-ordered, distributed-safe identification.

**Current Implementation Status**:
- ✅ Core event storage and processing (core schema)
- ✅ Source material tracking (raw schema)
- ✅ Schema registry and validation (sinex_schemas)
- ✅ System telemetry (metrics schema)
- ⚠️ Knowledge management partially implemented (km schema - some tables may be obsolete)
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
- **km**: Knowledge management (concepts, relations, embeddings) - ⚠️ PARTIALLY IMPLEMENTED
  - ✅ concepts, relations, event_annotations - legitimate knowledge graph functionality
  - ⚠️ artifacts, artifact_revisions - potentially obsolete, architecture changed
  - ✅ embeddings, llm_interactions - implemented for AI integration
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

**Implementation Status**: ✅ Exists in migration 00000000000002_create_core_tables.sql

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

### core.automaton_checkpoints
Processing state for event automata.
- **id**: ULID (PK)
- **automaton_name**: TEXT
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

**Implementation Status**: ✅ Implemented in migration 00000000000008

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

**Implementation Status**: ✅ Implemented in migration 00000000000008

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

⚠️ **WARNING**: The km schema contains both legitimate tables and mistakenly added ones:
- ✅ **Legitimate**: concepts, relations, event_annotations, embeddings, llm_interactions (knowledge graph functionality)
- ❌ **Mistakenly Added**: artifacts, artifact_revisions (added during migration reorganization from TIM design docs, never intended for implementation)

### km.concepts
Knowledge graph concepts with embeddings. Central to the knowledge management system.

**Implementation Status**: ✅ Fully implemented

- **id**: ULID (PK)
- **concept_name**: TEXT - Human-readable name
- **concept_type**: TEXT - Type categorization (person, project, technology, etc.)
- **description**: TEXT - Detailed description
- **metadata**: JSONB - Flexible additional properties
- **embedding**: vector(1536) - For semantic search (uses pgvector)
- **created_at**: TIMESTAMPTZ
- **updated_at**: TIMESTAMPTZ
- **created_by**: TEXT - Actor who created this (user or automaton)

**Constraints**:
- UNIQUE (concept_name, concept_type) - Prevents duplicate concepts

**Indexes**:
- B-tree on concept_type for filtering
- B-tree on concept_name for lookups
- IVFFlat on embedding for vector search (100 lists)

### km.relations
Relationships between concepts in the knowledge graph.

**Implementation Status**: ✅ Fully implemented

- **id**: ULID (PK)
- **from_concept_id**: ULID (FK) - Source concept
- **to_concept_id**: ULID (FK) - Target concept
- **relation_type**: TEXT - Semantic relationship type
- **confidence**: REAL (0-1) - Relationship strength/confidence
- **metadata**: JSONB - Additional properties
- **created_at**: TIMESTAMPTZ
- **created_by**: TEXT - Who established this relation

**Constraints**:
- UNIQUE (from_concept_id, to_concept_id, relation_type) - No duplicate relations
- CHECK (from_concept_id != to_concept_id) - No self-relations

**Indexes**:
- B-tree on from_concept_id
- B-tree on to_concept_id
- B-tree on relation_type

### km.event_annotations
Links events to concepts. Enables semantic tagging and knowledge extraction from raw events.

**Implementation Status**: ✅ Implemented

- **id**: ULID (PK)
- **event_id**: ULID - Reference to core.events (no FK to avoid coupling)
- **concept_id**: ULID (FK) - Links to km.concepts
- **annotation_type**: TEXT - Type of annotation (tag, comment, summary, analysis)
- **confidence**: REAL (0-1) - For automated annotations
- **metadata**: JSONB - Additional annotation properties
- **created_at**: TIMESTAMPTZ
- **created_by**: TEXT - User or automaton that created annotation

**Design Note**: This links events to knowledge concepts. The TIM proposed direct text annotations without concept requirement, which could be a future enhancement.

### km.artifacts ⚠️ INCORRECTLY ADDED
**WARNING**: These tables were mistakenly added during migration reorganization (commit ffe3fdce) based on the TIM-CoreArtifactsSchema design document. They were never part of the actual implementation and appear to be AI-generated during documentation work. The architecture uses events as the primary data model, not artifacts.

Versioned knowledge documents (if implemented).
- **id**: ULID (PK)
- **artifact_type**: TEXT - Would have been: pkm_note, webpage_archive, email_message, pdf_document, task_item
- **title**: TEXT
- **uri**: TEXT - External URI if applicable
- **metadata**: JSONB - Type-specific properties
- **created_at**: TIMESTAMPTZ
- **updated_at**: TIMESTAMPTZ
- **created_by**: TEXT

**Original Design Intent** (from TIM-CoreArtifactsSchema):
- Separate metadata from content for efficiency
- Support multiple artifact types with type-specific metadata
- Integration with Yjs CRDTs for real-time collaboration (never implemented)

### km.artifact_revisions ⚠️ INCORRECTLY ADDED
**WARNING**: Like km.artifacts, this table was mistakenly added during migration reorganization and was never intended to be implemented.

Content versions for artifacts (if implemented).
- **revision_id**: ULID (PK)
- **artifact_id**: ULID (FK)
- **revision_number**: INTEGER - Sequential per artifact
- **content**: TEXT - Actual content or snapshot
- **content_hash**: TEXT - BLAKE3 for deduplication
- **metadata**: JSONB - Could include Yjs state vectors
- **created_at**: TIMESTAMPTZ
- **created_by**: TEXT

**Note**: The full TIM specification included canonical identifiers, tags denormalization, integration with core.blobs, and full-text search capabilities that were never implemented.

### km.embeddings
Vector embeddings for various content types. This differs from the TIM-KnowledgeGraphSchema design which proposed separate embeddings per table.

**Implementation Note**: Current implementation uses a generic embeddings cache with content hashing. The migration includes a different structure than the TIM proposed.

- **id**: ULID (PK)
- **content_hash**: TEXT (UNIQUE) - BLAKE3 hash for deduplication
- **content_type**: TEXT - Type of content that was embedded
- **embedding**: vector(1536) - pgvector type for semantic search
- **model_name**: TEXT - Which embedding model was used
- **metadata**: JSONB - Additional context about the embedding
- **created_at**: TIMESTAMPTZ

**Indexes**:
- B-tree on content_hash for fast lookups
- IVFFlat on embedding for vector similarity search

**Future Enhancements**: See docs/roadmap/features/embeddings-and-semantic-search.md for comprehensive semantic search design.

### km.llm_interactions
Tracks AI model interactions for auditing, cost tracking, and analysis.

**Implementation Status**: ✅ Implemented but schema differs from migration

**Migration Schema**:
- **id**: ULID (PK)
- **interaction_type**: TEXT - Type of interaction
- **model_name**: TEXT - Which LLM was used
- **model_version**: TEXT - Model version if known
- **prompt**: TEXT - Full prompt sent
- **response**: TEXT - Full response received
- **token_count**: INTEGER - Total tokens used
- **latency_ms**: INTEGER - Response time
- **metadata**: JSONB - Additional context
- **created_at**: TIMESTAMPTZ

**Note**: The SCHEMA.md version includes context_events and duration_ms which aren't in the migration. This suggests either documentation drift or planned enhancements.

## Metrics Tables

### metrics.sinex_metrics
System telemetry data. Stores Prometheus-style metrics for internal observability.

**Implementation Status**: ✅ Implemented in migration 00000000000006

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

The database schema has evolved through several migrations:

1. **00000000000001_initial_schema.sql**: Basic setup with pgx_ulid
2. **00000000000002_create_core_tables.sql**: Core event storage, entities
3. **00000000000003_create_domains.sql**: Domain-specific event tables
4. **00000000000004_create_knowledge_management.sql**: KM tables (contains mistakenly added artifact tables)
5. **00000000000005_create_sinex_schemas.sql**: Schema registry
6. **00000000000006_create_metrics_schema.sql**: Telemetry tables
7. **00000000000007_create_raw_schema.sql**: Source material tracking
8. **00000000000008_satellite_coordination.sql**: Service coordination
9. **00000000000009_add_processor_registry.sql**: Processor manifests
10. **00000000000010_add_ulid_indices.sql**: Performance optimizations

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
  - migrations/ - SQL migration files with detailed schema definitions

## Schema Validation

All event payloads are validated against JSON schemas stored in sinex_schemas.event_payload_schemas. Use the schema registry to:

1. Register new event types with their schemas
2. Validate payloads before insertion
3. Track schema evolution and compatibility
4. Cache validation results for performance

See crate/sinex-ingestd/src/validation.rs for the implementation.

## Original Migration Structure vs Current km Schema

### Original Tables (core schema from migrations 20250103120011-13):
1. **core.entities** - Knowledge graph nodes (persons, projects, topics, organizations)
   - Had: type, name, canonical_name, aliases[], description, metadata, merged_into_id
   - Types: person, project, topic, organization, location, concept, tool, event

2. **core.entity_relations** - Knowledge graph edges
   - Had: from_entity_id, to_entity_id, relation_type, strength, valid_from/until

3. **core.event_annotations** - User annotations on events
   - Had: event_id, annotation_type, content, metadata, created_by
   - Types: note, correction, context, importance

4. **core.artifacts** - Conceptual documents (PKM notes, web pages, emails)
   - Had: type, title, source_url, mime_type, blob_id references

5. **core.embedding_cache** - Deduplication cache
   - Had: text_hash, embedding_model_id, embedding vector, use_count

### Current km Schema (created during reorganization):
1. **km.concepts** - Similar to core.entities but:
   - Missing: aliases[], merged_into_id (for deduplication)
   - Added: embedding vector(1536) directly in table
   - Different focus: "concepts" vs "entities"

2. **km.relations** - Similar to core.entity_relations but:
   - Simplified: no temporal validity (valid_from/until)
   - Changed: confidence instead of strength
   - Missing: created_from_event_id provenance

3. **km.event_annotations** - Different from core.event_annotations:
   - Links to concepts (concept_id FK) instead of direct text annotations
   - Added: confidence score
   - Missing: ability to annotate without linking to a concept

4. **km.artifacts/artifact_revisions** - Simplified from core.artifacts:
   - Missing many fields from original design
   - References non-existent features (Yjs CRDTs)

5. **km.embeddings** - Different from core.embedding_cache:
   - Generic cache with content_hash instead of text_hash
   - Missing: use_count, last_used_at for LRU
   - Different structure than TIM-EmbeddingGenerationModels design

### Summary
The km schema appears to be a reimplementation created during migration reorganization (commit ffe3fdce) that:
- Took concepts from the original core.* tables
- Simplified or modified many aspects
- Added AI/ML features (embeddings in concepts, llm_interactions)
- Lost some important features (entity deduplication, temporal validity, direct annotations)
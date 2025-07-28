-- Create core event storage tables with unified architecture

-- Create helper function for updated_at triggers
CREATE OR REPLACE FUNCTION set_current_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

--
-- Technical Implementation Module: TimescaleDB Configuration
--
-- Maturity Level: L4 - Implemented
-- Implementation: 85% (TimescaleDB hypertable creation and basic configuration working, compression pending)
-- Dependencies: TimescaleDB PostgreSQL extension, NixOS PostgreSQL configuration
-- Blocks: Time-series event storage, efficient time-based queries, data compression
--
-- ## Overview
--
-- This migration configures TimescaleDB for managing the core.events table as a hypertable,
-- optimized for time-series data. TimescaleDB is used due to its ability to efficiently
-- partition large time-series tables, provide performant time-based queries, and offer
-- features like native compression.
--
-- ## Key Configuration Decisions
--
-- 1. Partitioning Strategy: Uses ULID-based partitioning via ulid_to_timestamptz function
--    - Leverages time-ordering properties of ULIDs
--    - Automatic chunk management based on time ranges
--
-- 2. Chunk Interval: Default 1 day (configured at runtime)
--    - Aim for chunks to be 10-25% of available RAM
--    - Adjust based on actual daily volume
--
-- 3. Compression Strategy (to be configured):
--    - Enable compression for chunks older than 7 days
--    - Uses columnar compression with segmentby on source, host
--    - Can achieve 90-95% storage reduction
--
-- ## Optimization Guidelines
--
-- - For high volume (>10-20GB/day): Use shorter intervals (6-12 hours)
-- - For low volume: Can extend to 7 days
-- - Extract frequently queried JSONB fields to native columns for better compression
-- - Monitor chunk sizes via timescaledb_information.chunks
--
-- ## Required Configuration
--
-- 1. Enable TimescaleDB in postgresql.conf:
--    shared_preload_libraries = 'timescaledb'
--
-- 2. Run timescaledb_tune for optimal settings based on system resources
--
-- 3. Configure compression policy after initial data load:
--    SELECT add_compression_policy('core.events', INTERVAL '7 days');

-- Create processor manifests table for tracking event producers
CREATE TABLE IF NOT EXISTS core.processor_manifests (
    manifest_id SERIAL PRIMARY KEY,
    processor_name TEXT NOT NULL,
    processor_version TEXT NOT NULL,
    processor_type TEXT NOT NULL CHECK (processor_type IN ('ingestor', 'automaton', 'system')),
    hostname TEXT NOT NULL,
    start_time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    end_time TIMESTAMPTZ,
    config JSONB,
    metadata JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT unique_processor_instance UNIQUE (processor_name, processor_version, hostname, start_time)
);

CREATE INDEX idx_processor_manifests_active ON core.processor_manifests (processor_name, hostname) WHERE end_time IS NULL;
CREATE INDEX idx_processor_manifests_time_range ON core.processor_manifests (start_time, end_time);

-- Create source material registry for external data provenance
CREATE TABLE IF NOT EXISTS raw.source_material_registry (
    blob_id ULID PRIMARY KEY DEFAULT gen_ulid(),
    material_type TEXT NOT NULL,
    source_uri TEXT,
    ingestion_time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    file_size_bytes BIGINT,
    checksum_blake3 TEXT,
    mime_type TEXT,
    encoding TEXT,
    metadata JSONB NOT NULL DEFAULT '{}',
    content_preview TEXT,
    is_archived BOOLEAN NOT NULL DEFAULT FALSE,
    archive_time TIMESTAMPTZ,
    retention_policy TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_source_material_type_time ON raw.source_material_registry (material_type, ingestion_time DESC);
CREATE INDEX idx_source_material_uri ON raw.source_material_registry (source_uri) WHERE source_uri IS NOT NULL;
CREATE INDEX idx_source_material_checksum ON raw.source_material_registry (checksum_blake3) WHERE checksum_blake3 IS NOT NULL;

-- Create the main events table with unified architecture
--
-- ## Architectural Decision: ULID Primary Key with TimescaleDB (ADR-009)
--
-- We use a generated column approach to maintain ULID as the sole primary key
-- while enabling TimescaleDB hypertable partitioning:
--
-- 1. event_id ULID remains the sole primary key (architectural purity)
-- 2. ts_ingest is GENERATED from the ULID timestamp (no redundancy)
-- 3. TimescaleDB partitions by ULID using custom partition function
--
-- This approach was chosen after benchmarking showed it provides:
-- - Same query performance as composite keys
-- - Only 8 bytes storage overhead per row
-- - No timestamp extraction overhead in queries
-- - Enables index-only scans on time ranges
--
-- Alternative approaches considered but rejected:
-- - Composite key (event_id, ts_ingest): Violates single PK principle
-- - ULID-only partitioning: 3-13x slower aggregation queries
-- - Native PostgreSQL partitioning: Lacks TimescaleDB features
--
CREATE TABLE IF NOT EXISTS core.events (
    event_id ULID PRIMARY KEY DEFAULT gen_ulid(),
    ts_ingest TIMESTAMPTZ NOT NULL GENERATED ALWAYS AS (event_id::timestamp) STORED,
    
    -- The Interpretation
    event_type TEXT NOT NULL,
    source TEXT NOT NULL,  -- The processor that created this interpretation
    ts_orig TIMESTAMPTZ,   -- The conceptual timestamp from source material
    host TEXT NOT NULL,
    payload JSONB NOT NULL,
    
    -- Schema tracking
    ingestor_version TEXT,
    payload_schema_id ULID,
    payload_schema_name TEXT,
    payload_schema_version TEXT,
    
    -- Provenance Links
    source_material_id ULID REFERENCES raw.source_material_registry(blob_id),
    source_material_offset_start BIGINT,
    source_material_offset_end BIGINT,
    anchor_byte BIGINT,  -- Primary offset for precise location
    source_event_ids ULID[],  -- Internal provenance chain
    
    -- Associated data
    associated_blob_ids ULID[],
    processor_manifest_id INTEGER REFERENCES core.processor_manifests(manifest_id),
    
    -- Constraints
    CONSTRAINT events_event_type_check CHECK (length(TRIM(BOTH FROM event_type)) > 0),
    CONSTRAINT events_host_check CHECK (length(TRIM(BOTH FROM host)) > 0),
    CONSTRAINT events_source_check CHECK (length(TRIM(BOTH FROM source)) > 0)
);

-- Create ULID to timestamp conversion function for partitioning
CREATE OR REPLACE FUNCTION ulid_to_timestamptz(ulid_val ULID) 
RETURNS TIMESTAMPTZ AS $$
BEGIN
    RETURN ulid_val::timestamp;
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE;

-- Convert events table to TimescaleDB hypertable
SELECT create_hypertable(
    'core.events',
    by_range('event_id', partition_func => 'ulid_to_timestamptz'::regproc)
);

-- Create comprehensive indexes
CREATE INDEX idx_core_events_ts_ingest ON core.events (ts_ingest DESC);
CREATE INDEX idx_core_events_ts_orig ON core.events (ts_orig DESC) WHERE ts_orig IS NOT NULL;
CREATE INDEX idx_core_events_source_ts ON core.events (source, ts_ingest DESC);
CREATE INDEX idx_core_events_source_type_ts ON core.events (source, event_type, ts_ingest DESC);
CREATE INDEX idx_core_events_host_ts ON core.events (host, ts_ingest DESC);
CREATE INDEX idx_core_events_schema_name ON core.events (payload_schema_name) WHERE payload_schema_name IS NOT NULL;
CREATE INDEX idx_core_events_source_material ON core.events (source_material_id) WHERE source_material_id IS NOT NULL;
CREATE INDEX idx_core_events_provenance ON core.events USING GIN (source_event_ids) WHERE source_event_ids IS NOT NULL;
CREATE INDEX idx_core_events_raw_events ON core.events (ts_ingest DESC) WHERE source_event_ids IS NULL;
CREATE INDEX idx_core_events_synthesis_events ON core.events (ts_ingest DESC) WHERE source_event_ids IS NOT NULL;
CREATE INDEX idx_core_events_associated_blobs ON core.events USING GIN (associated_blob_ids) WHERE associated_blob_ids IS NOT NULL;
CREATE INDEX idx_core_events_payload_gin ON core.events USING GIN (payload jsonb_path_ops);

-- Create archived events table for data lifecycle management
CREATE TABLE IF NOT EXISTS core.archived_events (
    LIKE core.events INCLUDING ALL,
    archived_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    archive_reason TEXT
);

-- Create automaton checkpoints table
CREATE TABLE IF NOT EXISTS core.automaton_checkpoints (
    id ULID PRIMARY KEY DEFAULT gen_ulid(),
    automaton_name TEXT NOT NULL,
    consumer_group TEXT NOT NULL DEFAULT 'default',
    consumer_name TEXT NOT NULL DEFAULT 'default',
    last_processed_id ULID,
    last_processed_ts TIMESTAMPTZ,
    processed_count BIGINT NOT NULL DEFAULT 0,
    checkpoint_data JSONB,
    state_data JSONB,
    checkpoint_version INTEGER NOT NULL DEFAULT 1,
    last_activity TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT unique_automaton_consumer UNIQUE (automaton_name, consumer_group, consumer_name)
);

CREATE INDEX idx_automaton_checkpoints_updated ON core.automaton_checkpoints (updated_at DESC);
CREATE INDEX idx_automaton_checkpoints_automaton ON core.automaton_checkpoints (automaton_name);
CREATE INDEX idx_automaton_checkpoints_consumer ON core.automaton_checkpoints (consumer_group, consumer_name);

-- Create operations log for audit trail
CREATE TABLE IF NOT EXISTS core.operations_log (
    operation_id ULID PRIMARY KEY DEFAULT gen_ulid(),
    operation_ts TIMESTAMPTZ NOT NULL GENERATED ALWAYS AS (operation_id::timestamp) STORED,
    operation_type TEXT NOT NULL,
    operator TEXT NOT NULL,
    target_table TEXT NOT NULL,
    target_id TEXT,
    operation_data JSONB NOT NULL,
    result_status TEXT NOT NULL CHECK (result_status IN ('success', 'failure', 'partial')),
    result_message TEXT,
    duration_ms INTEGER,
    metadata JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_operations_log_ts ON core.operations_log (operation_ts DESC);
CREATE INDEX idx_operations_log_type_ts ON core.operations_log (operation_type, operation_ts DESC);
CREATE INDEX idx_operations_log_target ON core.operations_log (target_table, target_id) WHERE target_id IS NOT NULL;

-- Create metrics table for system telemetry (in metrics schema)
CREATE TABLE IF NOT EXISTS metrics.sinex_metrics (
    metric_id ULID PRIMARY KEY DEFAULT gen_ulid(),
    metric_ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    metric_name TEXT NOT NULL,
    metric_value DOUBLE PRECISION NOT NULL,
    metric_type TEXT NOT NULL CHECK (metric_type IN ('counter', 'gauge', 'histogram', 'summary')),
    labels JSONB NOT NULL DEFAULT '{}',
    source TEXT NOT NULL,
    host TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_sinex_metrics_name_ts ON metrics.sinex_metrics (metric_name, metric_ts DESC);
CREATE INDEX idx_sinex_metrics_source_ts ON metrics.sinex_metrics (source, metric_ts DESC);
CREATE INDEX idx_sinex_metrics_labels ON metrics.sinex_metrics USING GIN (labels);

-- Create legacy sinex.metrics table for compatibility with sinex-metrics-lib
CREATE TABLE IF NOT EXISTS sinex.metrics (
    id UUID PRIMARY KEY,
    metric_name TEXT NOT NULL,
    metric_type TEXT NOT NULL,
    value DOUBLE PRECISION NOT NULL,
    labels JSONB NOT NULL DEFAULT '{}',
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    namespace TEXT NOT NULL DEFAULT 'sinex',
    subsystem TEXT NOT NULL,
    CONSTRAINT valid_metric_type CHECK (metric_type IN ('counter', 'gauge', 'histogram', 'summary'))
);

CREATE INDEX idx_sinex_metrics_legacy_name_time ON sinex.metrics (metric_name, timestamp DESC);
CREATE INDEX idx_sinex_metrics_legacy_namespace ON sinex.metrics (namespace, subsystem, timestamp DESC);

-- Create entities table for knowledge graph
--
-- ## Knowledge Graph Schema (TIM-KnowledgeGraphSchema)
-- 
-- Central registry for all canonical "things" or "concepts" (nodes) in Sinex.
-- Supports entity resolution, relationship discovery, and graph queries.
--
-- ### Entity Types
-- - people: Individuals mentioned or involved in events
-- - projects: Software projects, repositories, tasks
-- - artifacts: Files, documents, media
-- - topics: Subjects, tags, categories
-- - locations: Physical or virtual locations
-- - organizations: Companies, teams, groups
--
-- ### Future Enhancements (Not Implemented)
-- - Entity embeddings with pgvector (768-dimensional vectors)
-- - Semantic similarity search across entities
-- - Automated entity extraction from events
-- - Entity resolution using embeddings
-- - Confidence scoring for relationships
CREATE TABLE IF NOT EXISTS core.entities (
    id ULID PRIMARY KEY DEFAULT gen_ulid(),
    type TEXT NOT NULL,
    name TEXT NOT NULL,
    canonical_name TEXT,
    aliases TEXT[],
    description TEXT,
    metadata JSONB NOT NULL DEFAULT '{}',
    merged_into_id ULID REFERENCES core.entities(id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_from_event_id ULID,
    CONSTRAINT unique_entity_name_type UNIQUE (name, type)
);

CREATE INDEX idx_entities_type ON core.entities (type);
CREATE INDEX idx_entities_name ON core.entities (name);
CREATE INDEX idx_entities_canonical ON core.entities (canonical_name) WHERE canonical_name IS NOT NULL;
CREATE INDEX idx_entities_created_from ON core.entities (created_from_event_id) WHERE created_from_event_id IS NOT NULL;

-- Create entity relations table
--
-- ## Entity Relations (Knowledge Graph Edges)
--
-- Represents relationships between entities with temporal validity.
--
-- ### Relationship Types
-- - mentions: Entity A mentions Entity B
-- - works_on: Person works on Project
-- - depends_on: Project A depends on Project B
-- - links_to: Document links to another entity
-- - authored_by: Artifact authored by Person
-- - located_at: Entity located at Location
-- - member_of: Person member of Organization
--
-- ### Temporal Tracking
-- - valid_from/valid_until: When relationship was/is valid
-- - Allows historical relationship queries
--
-- ### Future Enhancements
-- - Relationship strength/confidence scoring
-- - Bidirectional relationship inference
-- - Graph traversal optimizations
-- - Path-finding algorithms support
CREATE TABLE IF NOT EXISTS core.entity_relations (
    id ULID PRIMARY KEY DEFAULT gen_ulid(),
    from_entity_id ULID NOT NULL REFERENCES core.entities(id) ON DELETE CASCADE,
    to_entity_id ULID NOT NULL REFERENCES core.entities(id) ON DELETE CASCADE,
    relation_type TEXT NOT NULL,
    strength DOUBLE PRECISION CHECK (strength >= 0 AND strength <= 1),
    metadata JSONB NOT NULL DEFAULT '{}',
    valid_from TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    valid_until TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_from_event_id ULID,
    CONSTRAINT unique_entity_relation UNIQUE (from_entity_id, to_entity_id, relation_type),
    CONSTRAINT no_self_relation CHECK (from_entity_id != to_entity_id)
);

CREATE INDEX idx_entity_relations_from ON core.entity_relations (from_entity_id);
CREATE INDEX idx_entity_relations_to ON core.entity_relations (to_entity_id);
CREATE INDEX idx_entity_relations_type ON core.entity_relations (relation_type);
CREATE INDEX idx_entity_relations_created_from ON core.entity_relations (created_from_event_id) WHERE created_from_event_id IS NOT NULL;

-- Add table comments
COMMENT ON TABLE core.events IS 'Unified event storage for all captured and synthesized events with full provenance tracking';
COMMENT ON TABLE core.processor_manifests IS 'Registry of all event processors (ingestors and automata) with their configurations';
COMMENT ON TABLE raw.source_material_registry IS 'Registry of external source materials (files, streams, etc.) that events are derived from';
COMMENT ON TABLE core.automaton_checkpoints IS 'Processing state for event automata to enable reliable restarts';
COMMENT ON TABLE core.operations_log IS 'Audit log of all administrative operations performed on the system';
COMMENT ON TABLE metrics.sinex_metrics IS 'System telemetry and performance metrics';
COMMENT ON TABLE core.entities IS 'Knowledge graph entities extracted from events';
COMMENT ON TABLE core.entity_relations IS 'Relationships between entities in the knowledge graph';

-- ============================================================================
-- Git-Annex Blob Management
-- ============================================================================
--
-- Registry for large files managed by git-annex. This table tracks metadata
-- about binary content that's too large for direct database storage.
--
CREATE TABLE IF NOT EXISTS core.blobs (
    id ULID PRIMARY KEY DEFAULT gen_ulid(),
    annex_key TEXT UNIQUE NOT NULL,
    original_filename TEXT NOT NULL,
    size_bytes BIGINT NOT NULL,
    mime_type TEXT,
    checksum_sha256 TEXT NOT NULL,
    checksum_blake3 TEXT,
    storage_backend TEXT NOT NULL DEFAULT 'git-annex',
    metadata JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_verified_at TIMESTAMPTZ,
    verification_status TEXT CHECK (verification_status IN ('pending', 'verified', 'missing', 'corrupted'))
);

CREATE INDEX idx_blobs_annex_key ON core.blobs(annex_key);
CREATE INDEX idx_blobs_checksum_sha256 ON core.blobs(checksum_sha256);
CREATE INDEX idx_blobs_checksum_blake3 ON core.blobs(checksum_blake3) WHERE checksum_blake3 IS NOT NULL;
CREATE INDEX idx_blobs_verification ON core.blobs(verification_status, last_verified_at);

-- ============================================================================
-- Hierarchical Tagging System
-- ============================================================================
--
-- Flexible tagging/categorization that complements the formal knowledge graph.
-- Tags are user-defined labels that can be organized hierarchically.
--
CREATE TABLE IF NOT EXISTS core.tags (
    id ULID PRIMARY KEY DEFAULT gen_ulid(),
    name TEXT NOT NULL UNIQUE,
    display_name TEXT NOT NULL,
    color TEXT,
    icon TEXT,
    parent_id ULID REFERENCES core.tags(id),
    metadata JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_tags_parent ON core.tags(parent_id) WHERE parent_id IS NOT NULL;
CREATE INDEX idx_tags_name ON core.tags(name);

-- ============================================================================
-- Event Annotations
-- ============================================================================
--
-- ## Event Annotations Schema (TIM-EventAnnotationsSchema)
--
-- Provides flexible annotation system for events with:
-- - Multiple annotation types (tag, comment, summary, analysis)
-- - Actor tracking for provenance (user vs AI agent)
-- - Direct text annotations (no concept linking required)
-- - Structured metadata in JSONB format
--
-- This is the "human knowledge layer" that captures understanding not
-- present in raw event data.
--
CREATE TABLE IF NOT EXISTS core.event_annotations (
    id ULID PRIMARY KEY DEFAULT gen_ulid(),
    event_id ULID NOT NULL REFERENCES core.events(event_id) ON DELETE CASCADE,
    annotation_type TEXT NOT NULL,
    content TEXT NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by TEXT NOT NULL DEFAULT 'user'
);

CREATE INDEX idx_event_annotations_event ON core.event_annotations(event_id);
CREATE INDEX idx_event_annotations_type ON core.event_annotations(annotation_type);
CREATE INDEX idx_event_annotations_created ON core.event_annotations(created_at);
CREATE INDEX idx_event_annotations_search ON core.event_annotations USING gin(to_tsvector('english', content));

-- ============================================================================
-- Event Relations
-- ============================================================================
--
-- Captures relationships between events to understand causality, workflows,
-- and patterns. These relations can be discovered automatically or defined
-- manually.
--
CREATE TABLE IF NOT EXISTS core.event_relations (
    id ULID PRIMARY KEY DEFAULT gen_ulid(),
    from_event_id ULID NOT NULL REFERENCES core.events(event_id) ON DELETE CASCADE,
    to_event_id ULID NOT NULL REFERENCES core.events(event_id) ON DELETE CASCADE,
    relation_type TEXT NOT NULL,
    confidence FLOAT DEFAULT 1.0 CHECK (confidence >= 0 AND confidence <= 1),
    detected_by TEXT NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT no_self_event_relations CHECK (from_event_id != to_event_id),
    CONSTRAINT unique_event_relation UNIQUE(from_event_id, to_event_id, relation_type)
);

CREATE INDEX idx_event_relations_from ON core.event_relations(from_event_id);
CREATE INDEX idx_event_relations_to ON core.event_relations(to_event_id);
CREATE INDEX idx_event_relations_type ON core.event_relations(relation_type);
CREATE INDEX idx_event_relations_confidence ON core.event_relations(confidence) WHERE confidence < 1.0;

-- ============================================================================
-- Event Clusters
-- ============================================================================
--
-- Groups events into meaningful clusters like sessions, workflows, or
-- incidents. Clusters provide context for understanding event sequences.
--
CREATE TABLE IF NOT EXISTS core.event_clusters (
    id ULID PRIMARY KEY DEFAULT gen_ulid(),
    name TEXT NOT NULL,
    cluster_type TEXT NOT NULL,
    summary TEXT,
    time_start TIMESTAMPTZ NOT NULL,
    time_end TIMESTAMPTZ NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_event_clusters_type ON core.event_clusters(cluster_type);
CREATE INDEX idx_event_clusters_time ON core.event_clusters(time_start, time_end);

-- ============================================================================
-- Event Cluster Membership
-- ============================================================================
CREATE TABLE IF NOT EXISTS core.event_cluster_members (
    cluster_id ULID NOT NULL REFERENCES core.event_clusters(id) ON DELETE CASCADE,
    event_id ULID NOT NULL REFERENCES core.events(event_id) ON DELETE CASCADE,
    role TEXT,
    added_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    metadata JSONB NOT NULL DEFAULT '{}',
    PRIMARY KEY (cluster_id, event_id)
);

CREATE INDEX idx_event_cluster_members_event ON core.event_cluster_members(event_id);

-- Add triggers for updated_at
CREATE TRIGGER set_event_annotations_updated_at 
    BEFORE UPDATE ON core.event_annotations 
    FOR EACH ROW 
    EXECUTE FUNCTION set_current_timestamp();

CREATE TRIGGER set_event_clusters_updated_at 
    BEFORE UPDATE ON core.event_clusters 
    FOR EACH ROW 
    EXECUTE FUNCTION set_current_timestamp();

-- Add comments for new tables
COMMENT ON TABLE core.blobs IS 'Registry of git-annex managed binary files';
COMMENT ON TABLE core.tags IS 'Hierarchical tagging system for flexible categorization';
COMMENT ON TABLE core.event_annotations IS 'User annotations and notes on individual events';
COMMENT ON TABLE core.event_relations IS 'Discovered or defined relationships between events';
COMMENT ON TABLE core.event_clusters IS 'Grouped collections of related events';
COMMENT ON TABLE core.event_cluster_members IS 'Membership of events in clusters';
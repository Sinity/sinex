-- Just for documentation purposes, this file is not executed directly.

-- =============================================================================
-- SCRIPT:         Sinex Canonical Database Schema v7.1 (Part 1/3)
-- DESCRIPTION:    Core Infrastructure, Raw Data Substrate, and the Event Log.
--                 This script is idempotent and represents the final,
--                 architecturally sound design for the Sinex system's foundation.
-- =============================================================================

-- Set session parameters for a stable and robust migration.
SET client_min_messages = warning;
SET statement_timeout = '5m';
SET lock_timeout = '10s';

-- =============================================================================
-- I. EXTENSIONS & SCHEMAS
--
-- This section sets up the foundational PostgreSQL extensions and logical
-- namespaces (schemas) that organize the system's data and provide specialized
-- capabilities. These are non-negotiable dependencies.
-- =============================================================================

CREATE EXTENSION IF NOT EXISTS "ulid";          -- Provides the native ULID data type and gen_ulid() function, crucial for time-sortable, globally unique primary keys.
CREATE EXTENSION IF NOT EXISTS "timescaledb" CASCADE; -- Enables hypertables, turning core.events into a high-performance, auto-partitioned time-series table.
CREATE EXTENSION IF NOT EXISTS "pg_jsonschema"; -- Provides server-side JSON Schema validation, a critical backstop for data integrity.
CREATE EXTENSION IF NOT EXISTS "vector";        -- Enables storage and querying of vector embeddings for future AI/ML features like semantic search.

-- Create logical namespaces for data based on its role and mutability.
CREATE SCHEMA IF NOT EXISTS core;          -- For canonical, synthesized, and operational data. The system's "current understanding."
CREATE SCHEMA IF NOT EXISTS raw;           -- For immutable records related to raw data acquisition. The system's "sensory input."
CREATE SCHEMA IF NOT EXISTS audit;         -- For the immutable archive of superseded or deleted records. The system's "long-term memory."
CREATE SCHEMA IF NOT EXISTS sinex_schemas; -- For event payload schema management. The system's "data contracts."
CREATE SCHEMA IF NOT EXISTS metrics;       -- For materialized views and analytics functions.

-- =============================================================================
-- II. HELPER FUNCTIONS
--
-- Utility functions that provide critical functionality used throughout the database.
-- =============================================================================

-- Extracts the timestamp from a ULID. This is the core mechanism that allows
-- TimescaleDB to partition the `core.events` hypertable by time, while using a
-- non-chronological (but still time-sortable) ULID as the primary key.
CREATE OR REPLACE FUNCTION public.ulid_to_timestamptz(id_val ULID)
RETURNS TIMESTAMPTZ AS $$
BEGIN
    RETURN id_val::timestamp;
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE;
COMMENT ON FUNCTION public.ulid_to_timestamptz(ULID) IS 'Extracts the timestamp component from a ULID for TimescaleDB time-series partitioning.';

-- Trigger function to automatically update the `updated_at` column on any row modification.
-- This provides a simple, reliable audit trail for mutable records.
CREATE OR REPLACE FUNCTION public.set_current_timestamp_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION public.set_current_timestamp_updated_at() IS 'Generic trigger function to set updated_at to the current timestamp upon row update.';

-- =============================================================================
-- III. RAW DATA SUBSTRATE (GROUND TRUTH)
--
-- This section defines the tables that represent the system's immutable ground
-- truth: the records of what was captured and the precise temporal context of
-- that capture. These tables are exclusively managed by the `sensd` daemon.
-- =============================================================================

-- The manifest for all captured external data artifacts. A record here is the
-- "birth certificate" for any piece of information entering Sinex.
CREATE TABLE IF NOT EXISTS raw.source_material_registry (
    id                      ULID PRIMARY KEY DEFAULT gen_ulid(),
    material_kind           TEXT NOT NULL CHECK (material_kind IN ('annex', 'git')), -- The storage backend for the content.
    source_identifier       TEXT NOT NULL UNIQUE, -- A human-readable, unique identifier for the source (e.g., log file path, API endpoint).
    status                  TEXT NOT NULL CHECK (status IN ('sensing', 'completed', 'recovered_partial', 'failed')), -- Lifecycle status managed by sensd.
    timing_info_type        TEXT NOT NULL CHECK (timing_info_type IN ('realtime', 'intrinsic', 'inferred')), -- Primary mode of timestamping for this material.
    metadata                JSONB NOT NULL DEFAULT '{}',
    -- staged_at is removed; the ULID primary key's timestamp component serves this purpose.
    start_time              TIMESTAMPTZ NULL,
    end_time                TIMESTAMPTZ NULL,
    staged_by               TEXT, -- Identifier of the sensd sensor that staged this material.
    staged_on_host          TEXT,
    optional_blob_id        ULID NULL -- Foreign key to core.blobs, populated on finalization.
);
COMMENT ON TABLE raw.source_material_registry IS 'The manifest of all external data artifacts, representing ground truth. Managed by the `sensd` acquisition daemon.';
CREATE INDEX IF NOT EXISTS ix_sm_registry_id ON raw.source_material_registry (id DESC); -- Time-based index via ULID
CREATE INDEX IF NOT EXISTS ix_sm_registry_blob_id ON raw.source_material_registry (optional_blob_id) WHERE optional_blob_id IS NOT NULL;


-- The append-only ledger of capture-time provenance. This provides a high-precision,
-- immutable record of when each chunk of data was physically acquired, along with
-- rich metadata about the quality and source of the timing information.
CREATE TABLE IF NOT EXISTS raw.temporal_ledger (
    id                      ULID PRIMARY KEY DEFAULT gen_ulid(),
    source_material_id      ULID NOT NULL REFERENCES raw.source_material_registry(id) ON DELETE CASCADE,
    offset_start            BIGINT NOT NULL,
    offset_end              BIGINT NOT NULL,
    offset_kind             TEXT NOT NULL CHECK (offset_kind IN ('byte', 'line', 'rowid', 'logical')),
    ts_capture              TIMESTAMPTZ NOT NULL, -- The high-precision timestamp of when sensd physically captured this slice of data.
    -- Decomposed time_quality fields provide better performance and integrity than a single JSONB object.
    precision               TEXT NOT NULL CHECK (precision IN ('exact', 'bounded')),
    clock                   TEXT NOT NULL CHECK (clock IN ('monotonic', 'wall')),
    source_type             TEXT NOT NULL CHECK (source_type IN ('realtime_capture', 'intrinsic_content', 'inferred_mtime', 'inferred_user')),
    UNIQUE(source_material_id, offset_start)
);
COMMENT ON TABLE raw.temporal_ledger IS 'Append-only ledger of capture-time metadata for slices of Source Material, providing rich temporal provenance.';
COMMENT ON COLUMN raw.temporal_ledger.ts_capture IS 'High-precision timestamp recorded by `sensd` at the moment of data acquisition.';
CREATE INDEX IF NOT EXISTS ix_tl_material_offsets ON raw.temporal_ledger (source_material_id, offset_start, offset_end);
CREATE INDEX IF NOT EXISTS ix_tl_ts_source ON raw.temporal_ledger (ts_capture, source_type);

-- Trigger to enforce the append-only nature of the temporal ledger.
CREATE OR REPLACE FUNCTION raw.fn_temporal_ledger_append_only() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'raw.temporal_ledger is append-only (operation % is forbidden)', TG_OP;
END $$;
DROP TRIGGER IF EXISTS trg_tl_no_update_delete ON raw.temporal_ledger;
CREATE TRIGGER trg_tl_no_update_delete BEFORE UPDATE OR DELETE ON raw.temporal_ledger FOR EACH ROW EXECUTE FUNCTION raw.fn_temporal_ledger_append_only();


-- =============================================================================
-- IV. CORE EVENT LOG & ARCHIVE
-- =============================================================================

-- The single, unified log of all events. This is the heart of the Sinex system.
CREATE TABLE IF NOT EXISTS core.events (
    id                      ULID PRIMARY KEY DEFAULT gen_ulid(),
    source                  TEXT NOT NULL CHECK (length(TRIM(source)) > 0),
    event_type              TEXT NOT NULL CHECK (length(TRIM(event_type)) > 0),
    host                    TEXT NOT NULL,
    payload                 JSONB NOT NULL,
    ts_orig                 TIMESTAMPTZ NOT NULL,
    ts_ingest               TIMESTAMPTZ NOT NULL GENERATED ALWAYS AS (id::timestamp) STORED,

    -- External Provenance (for events derived from raw Source Material)
    source_material_id      ULID NULL REFERENCES raw.source_material_registry(id),
    anchor_byte             BIGINT NULL,
    offset_start            BIGINT NULL,
    offset_end              BIGINT NULL,
    offset_kind             TEXT NULL CHECK (offset_kind IN ('byte', 'line', 'rowid', 'logical')),

    -- Internal Provenance (for events derived from other events)
    source_event_ids        ULID[] NULL,

    -- Metadata
    payload_schema_id       ULID NULL, -- FK added later
    ingestor_version        TEXT NULL
);
COMMENT ON TABLE core.events IS 'The single source of truth: a unified, immutable log of all observations and syntheses.';

-- Convert to a TimescaleDB hypertable.
SELECT create_hypertable('core.events', by_range('id', partition_func => 'public.ulid_to_timestamptz'::regproc), if_not_exists => TRUE);

-- Enforce the Provenance XOR Invariant.
ALTER TABLE core.events DROP CONSTRAINT IF EXISTS events_provenance_xor;
ALTER TABLE core.events ADD CONSTRAINT events_provenance_xor CHECK ( (source_material_id IS NOT NULL AND source_event_ids IS NULL) OR (source_material_id IS NULL AND source_event_ids IS NOT NULL) );
COMMENT ON CONSTRAINT events_provenance_xor ON core.events IS 'Enforces that an event has either external (material) or internal (event) provenance, but not both.';

-- Enforce idempotency for ingestors.
CREATE UNIQUE INDEX IF NOT EXISTS ux_events_material_anchor ON core.events(source_material_id, anchor_byte) WHERE source_material_id IS NOT NULL;
COMMENT ON INDEX ux_events_material_anchor IS 'Ensures idempotency for events generated from the same anchor byte of the same source material.';

-- Create performance-critical indexes.
CREATE INDEX IF NOT EXISTS ix_events_ts_orig ON core.events (ts_orig DESC);
CREATE INDEX IF NOT EXISTS ix_events_source_type_ts ON core.events (source, event_type, ts_orig DESC);
CREATE INDEX IF NOT EXISTS ix_events_source_event_ids ON core.events USING GIN (source_event_ids) WHERE source_event_ids IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_events_payload_gin ON core.events USING GIN (payload jsonb_path_ops);

-- Immutable archive for superseded events.
CREATE TABLE IF NOT EXISTS audit.archived_events ( LIKE core.events INCLUDING ALL );
ALTER TABLE audit.archived_events ADD COLUMN IF NOT EXISTS archived_at TIMESTAMPTZ NOT NULL DEFAULT now();
ALTER TABLE audit.archived_events ADD COLUMN IF NOT EXISTS archived_by TEXT;
ALTER TABLE audit.archived_events ADD COLUMN IF NOT EXISTS archive_reason TEXT;
ALTER TABLE audit.archived_events ADD COLUMN IF NOT EXISTS superseded_by_event_id ULID NULL;
COMMENT ON TABLE audit.archived_events IS 'Immutable archive for events superseded by replay operations. Populated by a trigger.';
CREATE INDEX IF NOT EXISTS ix_archived_events_archived_at ON audit.archived_events (archived_at DESC);

-- Trigger to enforce the Archive-on-Delete invariant.
CREATE OR REPLACE FUNCTION core.fn_archive_before_delete() RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE
  op_id TEXT := current_setting('sinex.operation_id', true);
  sup_id ulid := NULLIF(current_setting('sinex.superseded_by_id', true), '');
  who TEXT := current_setting('sinex.archived_by', true);
  why TEXT := current_setting('sinex.archive_reason', true);
BEGIN
  IF op_id IS NULL OR op_id = '' THEN
    RAISE EXCEPTION 'DELETE on core.events requires sinex.operation_id to be set in this session';
  END IF;
  INSERT INTO audit.archived_events SELECT OLD.*, now(), who, why, sup_id;
  RETURN OLD;
END $$;
DROP TRIGGER IF EXISTS trg_events_archive_before_delete ON core.events;
CREATE TRIGGER trg_events_archive_before_delete BEFORE DELETE ON core.events FOR EACH ROW EXECUTE FUNCTION core.fn_archive_before_delete();

-- =============================================================================
-- SCRIPT:         Sinex Canonical Database Schema v7.1 (Part 2/3)
-- DESCRIPTION:    Operational Metadata, Control Plane, and Content Storage.
--                 This script defines the tables that manage schemas,
--                 checkpoints, operational audits, and content-addressed storage.
-- =============================================================================

-- =============================================================================
-- V. SCHEMA & DATA CONTRACTS
-- =============================================================================

-- The central registry for all event payload JSON schemas. This table acts as the
-- data contract registry for the entire system. It is managed by the `sinex-schema`
-- tool and read by `ingestd` at runtime to enforce validation.
CREATE TABLE IF NOT EXISTS sinex_schemas.event_payload_schemas (
    id                      ULID PRIMARY KEY DEFAULT gen_ulid(),
    source                  TEXT NOT NULL, -- The event source this schema applies to (e.g., 'fs-watcher').
    event_type              TEXT NOT NULL, -- The event type this schema applies to (e.g., 'file.created').
    schema_version          TEXT NOT NULL, -- The semantic version of this schema (e.g., '1.0.0').
    schema_content          JSONB NOT NULL, -- The full JSON Schema document.
    content_hash            TEXT NOT NULL UNIQUE, -- SHA-256 of the canonical schema content, for idempotent synchronization.
    is_active               BOOLEAN NOT NULL DEFAULT true, -- Flag to enable/disable schemas without deleting them.
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(source, event_type, schema_version)
);
COMMENT ON TABLE sinex_schemas.event_payload_schemas IS 'Registry for event payload JSON schemas, synchronized from Rust code on ingestd startup.';

-- Add the foreign key from events to schemas now that the table exists.
-- This ensures that any event claiming to adhere to a schema is referencing a real, known schema.
ALTER TABLE core.events
    DROP CONSTRAINT IF EXISTS fk_events_payload_schema,
    ADD CONSTRAINT fk_events_payload_schema
        FOREIGN KEY (payload_schema_id)
        REFERENCES sinex_schemas.event_payload_schemas(id)
        ON DELETE SET NULL;


-- =============================================================================
-- VI. OPERATIONAL STATE & CONTROL
-- =============================================================================

-- The audit log for high-level, intentional system operations like replays and
-- archival. This table provides the "intent provenance" for the system.
CREATE TABLE IF NOT EXISTS core.operations_log (
    id                      ULID PRIMARY KEY DEFAULT gen_ulid(),
    operation_type          TEXT NOT NULL,
    operator                TEXT NOT NULL, -- Who or what initiated the action (e.g., 'user:admin', 'service:sinex-archiver').
    scope                   JSONB, -- The parameters of the operation (e.g., the processor and time range for a replay).
    result_status           TEXT NOT NULL CHECK (result_status IN ('success', 'failure', 'partial')),
    result_message          TEXT,
    preview_summary         JSONB, -- The output of the replay planner, stored for auditability.
    duration_ms             INTEGER
);
COMMENT ON TABLE core.operations_log IS 'The audit trail of system-level operations (e.g., replays, archives), providing intent provenance.';
-- The primary key ULID's timestamp serves as the creation/start time.
CREATE INDEX IF NOT EXISTS ix_operations_log_ts_type ON core.operations_log (id DESC, operation_type);


-- Checkpoint storage for all stateful processors. This table is the single
-- source of truth for the processing state of every satellite and automaton.
CREATE TABLE IF NOT EXISTS core.processor_checkpoints (
    id                      ULID PRIMARY KEY DEFAULT gen_ulid(),
    processor_name          TEXT NOT NULL,
    consumer_group          TEXT NOT NULL DEFAULT 'default', -- For NATS consumer groups.
    consumer_name           TEXT NOT NULL DEFAULT 'default', -- For unique instances within a group.
    last_processed_id       ULID NULL REFERENCES core.events(id) ON DELETE SET NULL, -- For automata tracking event stream position.
    processed_count         BIGINT NOT NULL DEFAULT 0,
    checkpoint_data         JSONB, -- For ingestors storing external state (e.g., file offsets, API cursors).
    last_activity           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (processor_name, consumer_group, consumer_name)
);
COMMENT ON TABLE core.processor_checkpoints IS 'Stateful progress tracking for all satellites and automata.';
DROP TRIGGER IF EXISTS trg_processor_checkpoints_updated_at ON core.processor_checkpoints;
CREATE TRIGGER trg_processor_checkpoints_updated_at BEFORE UPDATE ON core.processor_checkpoints FOR EACH ROW EXECUTE FUNCTION public.set_current_timestamp_updated_at();



-- The Transactional Outbox. This is a critical component for ensuring the
-- "post-commit publish" invariant. `ingestd` writes to `core.events` and this
-- table in the same transaction. A separate, asynchronous poller then reliably
-- publishes messages from this table to NATS and deletes them.
CREATE TABLE IF NOT EXISTS core.transactional_outbox (
    id                      BIGSERIAL PRIMARY KEY,
    event_id                ULID NOT NULL,
    destination             TEXT NOT NULL, -- The destination for the message (e.g., a NATS subject).
    payload                 BYTEA NOT NULL, -- The serialized event payload for the message bus.
    status                  TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'processing', 'sent', 'failed')),
    retry_count             INTEGER NOT NULL DEFAULT 0,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processed_at            TIMESTAMPTZ
);
COMMENT ON TABLE core.transactional_outbox IS 'Transactional outbox for ensuring at-least-once, post-commit event publishing to the message bus.';
-- Index for the poller to efficiently find pending messages.
CREATE INDEX IF NOT EXISTS ix_outbox_pending ON core.transactional_outbox (created_at) WHERE status = 'pending';

-- =============================================================================
-- VII. CONTENT-ADDRESSED STORAGE METADATA
-- =============================================================================

-- Metadata for large binary objects stored externally in a content-addressed
-- store like git-annex. This table acts as a high-performance index and metadata
-- cache for the annex.
CREATE TABLE IF NOT EXISTS core.blobs (
    id                      ULID PRIMARY KEY DEFAULT gen_ulid(),
    -- Decomposed annex_key components provide performance and integrity.
    annex_backend           TEXT NOT NULL,    -- The hashing algorithm used by the annex (e.g., 'SHA256E').
    content_hash            TEXT NOT NULL,    -- The cryptographic content hash from the annex key.
    size_bytes              BIGINT NOT NULL,  -- The exact size of the content in bytes.
    -- A faster, non-cryptographic hash for high-speed deduplication checks.
    checksum_blake3         TEXT UNIQUE,
    -- Essential metadata about the original artifact.
    original_filename       TEXT NOT NULL,
    mime_type               TEXT,
    -- Rich, queryable intrinsic metadata extracted from the blob's content.
    metadata                JSONB NOT NULL DEFAULT '{}',
    -- Operational status for data integrity management.
    last_verified_at        TIMESTAMPTZ,
    verification_status     TEXT CHECK (verification_status IN ('pending', 'verified', 'corrupted')),
    UNIQUE(annex_backend, content_hash) -- This is the true natural key of the annexed content.
);
COMMENT ON TABLE core.blobs IS 'Metadata for large binary objects stored in the content-addressed store (e.g., git-annex).';
COMMENT ON COLUMN core.blobs.checksum_blake3 IS 'Fast, non-cryptographic hash (BLAKE3) for high-speed deduplication lookups.';
COMMENT ON COLUMN core.blobs.metadata IS 'Intrinsic metadata extracted from the blob''s content (e.g., EXIF for images, ID3 for audio).';
-- Add the foreign key constraint from the raw material registry to here.
ALTER TABLE raw.source_material_registry ADD CONSTRAINT fk_sm_blob FOREIGN KEY (optional_blob_id) REFERENCES core.blobs(id) ON DELETE SET NULL;

-- =============================================================================
-- SCRIPT:         Sinex Canonical Database Schema v7.1 (Part 3/3)
-- DESCRIPTION:    Derived State (Projections), Analytics Views, and Finalization.
--                 This script defines the knowledge graph, tagging system, and
--                 the final functions and triggers that enforce system-wide integrity.
-- =============================================================================

-- =============================================================================
-- VIII. DERIVED STATE (KNOWLEDGE GRAPH & TAGS - Projections of the Event Log)
--
-- These tables hold structured knowledge synthesized from the core.events log.
-- They are considered rebuildable caches and can be safely truncated and
-- repopulated by replaying the relevant automata. They are the physical
-- manifestation of the system's "understanding."
-- =============================================================================

-- The definitive table for Knowledge Graph entities. This version includes
-- richer metadata for provenance, confidence, and entity resolution (merging).
CREATE TABLE core.entities (
    id                      ULID PRIMARY KEY DEFAULT gen_ulid(),
    entity_type             TEXT NOT NULL,
    canonical_name          TEXT NOT NULL UNIQUE,
    aliases                 TEXT[] NOT NULL DEFAULT '{}',
    properties              JSONB NOT NULL DEFAULT '{}',
    source_event_ids        ULID[] NOT NULL, -- Provenance: which events led to this entity's creation/update.
    confidence_score        FLOAT NOT NULL DEFAULT 1.0 CHECK (confidence_score >= 0 AND confidence_score <= 1.0),
    is_merged               BOOLEAN NOT NULL DEFAULT false,
    merged_into_id          ULID NULL REFERENCES core.entities(id) ON DELETE SET NULL,
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
COMMENT ON TABLE core.entities IS 'Materialized projection of entities (nouns) synthesized from the event log, with rich metadata for provenance and confidence.';
CREATE INDEX IF NOT EXISTS ix_kg_entities_type ON core.entities (entity_type);
CREATE INDEX IF NOT EXISTS idx_kg_entities_aliases ON core.entities USING GIN (aliases);
CREATE INDEX IF NOT EXISTS ix_kg_entities_source_events ON core.entities USING GIN (source_event_ids);
CREATE INDEX IF NOT EXISTS idx_kg_entities_merged ON core.entities (merged_into_id) WHERE is_merged = true;
DROP TRIGGER IF EXISTS trg_entities_updated_at ON core.entities;
CREATE TRIGGER trg_entities_updated_at BEFORE UPDATE ON core.entities FOR EACH ROW EXECUTE FUNCTION public.set_current_timestamp_updated_at();


-- The definitive table for relationships between Knowledge Graph entities.
CREATE TABLE core.entity_relations (
    id                      ULID PRIMARY KEY DEFAULT gen_ulid(),
    from_entity_id          ULID NOT NULL REFERENCES core.entities(id) ON DELETE CASCADE,
    to_entity_id            ULID NOT NULL REFERENCES core.entities(id) ON DELETE CASCADE,
    relation_type           TEXT NOT NULL,
    properties              JSONB NOT NULL DEFAULT '{}',
    source_event_ids        ULID[] NOT NULL,
    confidence_score        FLOAT NOT NULL DEFAULT 1.0 CHECK (confidence_score >= 0 AND confidence_score <= 1.0),
    is_active               BOOLEAN NOT NULL DEFAULT true,
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(from_entity_id, to_entity_id, relation_type),
    CHECK (from_entity_id <> to_entity_id)
);
COMMENT ON TABLE core.entity_relations IS 'Materialized projection of relationships (verbs) between entities, synthesized from the event log.';
CREATE INDEX IF NOT EXISTS ix_kg_relations_from_type ON core.entity_relations (from_entity_id, relation_type);
CREATE INDEX IF NOT EXISTS ix_kg_relations_to ON core.entity_relations (to_entity_id);
CREATE INDEX IF NOT EXISTS ix_kg_relations_active ON core.entity_relations (from_entity_id, to_entity_id) WHERE is_active = true;
DROP TRIGGER IF EXISTS trg_entity_relations_updated_at ON core.entity_relations;
CREATE TRIGGER trg_entity_relations_updated_at BEFORE UPDATE ON core.entity_relations FOR EACH ROW EXECUTE FUNCTION public.set_current_timestamp_updated_at();

-- The central definition table for all tags. Using a surrogate ULID key is
-- crucial here to allow tag renaming without causing a massive cascade of
-- updates across the `tagged_items` junction table.
CREATE TABLE IF NOT EXISTS core.tags (
    id                      ULID PRIMARY KEY DEFAULT gen_ulid(),
    name                    TEXT NOT NULL UNIQUE,
    parent_tag_id           ULID NULL REFERENCES core.tags(id) ON DELETE SET NULL, -- Enables tag hierarchies.
    description             TEXT,
    color                   TEXT
);
COMMENT ON TABLE core.tags IS 'A hierarchical tagging system, with tags themselves derived from events or user curation.';

-- The many-to-many junction table for applying tags to various items in the system.
-- This implements the "Tags, not Hierarchies" philosophy.
CREATE TABLE IF NOT EXISTS core.tagged_items (
    tag_id                  ULID NOT NULL REFERENCES core.tags(id) ON DELETE CASCADE,
    item_id                 ULID NOT NULL, -- Polymorphic: can be an event_id, entity_id, blob_id, etc.
    item_type               TEXT NOT NULL, -- The type of item being tagged, e.g., 'event', 'entity'.
    tagged_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tag_id, item_id, item_type)
);
COMMENT ON TABLE core.tagged_items IS 'Junction table for applying tags to various items, derived from events.';
CREATE INDEX IF NOT EXISTS ix_tagged_items_item ON core.tagged_items (item_id, item_type);


-- =============================================================================
-- IX. ANALYTICS & REPORTING (Materialized Views)
--
-- These materialized views provide pre-aggregated data for high-performance
-- dashboarding and analytics. They are a form of controlled, denormalized cache.
-- =============================================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS metrics.event_counts_by_type_hourly AS
SELECT
    time_bucket('1 hour', ts_ingest) AS bucket,
    source,
    event_type,
    COUNT(*) as event_count,
    COUNT(DISTINCT host) as unique_hosts
FROM core.events
GROUP BY bucket, source, event_type
WITH NO DATA;
COMMENT ON MATERIALIZED VIEW metrics.event_counts_by_type_hourly IS 'Pre-aggregates event counts per source and type on an hourly basis for fast dashboarding.';
CREATE UNIQUE INDEX IF NOT EXISTS ix_event_counts_hourly_unique ON metrics.event_counts_by_type_hourly (bucket, source, event_type);

CREATE MATERIALIZED VIEW IF NOT EXISTS metrics.terminal_commands_daily AS
SELECT
    time_bucket('1 day', ts_orig) AS bucket,
    payload->>'command' as command,
    COUNT(*) as execution_count,
    COUNT(DISTINCT host) as unique_hosts,
    AVG((payload->>'duration_ms')::numeric) as avg_duration_ms
FROM core.events
WHERE event_type LIKE '%.command.executed' OR event_type = 'command.canonical'
GROUP BY bucket, command
WITH NO DATA;
COMMENT ON MATERIALIZED VIEW metrics.terminal_commands_daily IS 'Pre-aggregates terminal command usage statistics on a daily basis.';
CREATE UNIQUE INDEX IF NOT EXISTS ix_terminal_commands_daily_unique ON metrics.terminal_commands_daily (bucket, command);

-- Function to refresh all materialized views concurrently, suitable for a cron job.
CREATE OR REPLACE FUNCTION metrics.refresh_all_materialized_views()
RETURNS void AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY metrics.event_counts_by_type_hourly;
    REFRESH MATERIALIZED VIEW CONCURRENTLY metrics.terminal_commands_daily;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- X. FINALIZATION & DATABASE-LEVEL VALIDATION
--
-- These final functions and triggers act as a safety net, enforcing critical
-- application logic at the database level to guarantee integrity.
-- =============================================================================

-- This function provides a database-level API for validating an event's payload.
-- It is a crucial integrity backstop to the primary validation that occurs in `ingestd`.
CREATE OR REPLACE FUNCTION sinex_schemas.is_payload_valid(p_payload JSONB, p_schema_id ULID)
RETURNS BOOLEAN AS $$
DECLARE
    v_schema JSONB;
BEGIN
    SELECT schema_content INTO v_schema
    FROM sinex_schemas.event_payload_schemas
    WHERE id = p_schema_id;

    IF v_schema IS NULL THEN
        -- If the referenced schema doesn't exist, the payload is considered invalid.
        RETURN FALSE;
    END IF;

    -- Use the pg_jsonschema extension to perform the validation.
    -- The payload must be cast to `json` for the function.
    RETURN json_matches_schema(v_schema::json, p_payload::json);
EXCEPTION
    WHEN OTHERS THEN
        -- Any error during validation (e.g., malformed schema) results in failure.
        RAISE WARNING 'Schema validation function error for schema_id %: %', p_schema_id, SQLERRM;
        RETURN FALSE;
END;
$$ LANGUAGE plpgsql STABLE;
COMMENT ON FUNCTION sinex_schemas.is_payload_valid(JSONB, ULID) IS 'Validates a JSONB payload against a registered schema ID. Returns false if schema not found or validation fails.';

-- Trigger to apply the validation function automatically upon event insertion or update.
-- This makes schema validation an enforced property of the database itself.
DROP TRIGGER IF EXISTS trg_events_validate_payload ON core.events;
CREATE TRIGGER trg_events_validate_payload
BEFORE INSERT OR UPDATE ON core.events
FOR EACH ROW EXECUTE FUNCTION sinex_schemas.fn_validate_event_payload();

-- =============================================================================
-- SCRIPT:         Sinex Canonical Database Schema v7.2 (Part 4/4)
-- DESCRIPTION:    High-Level Functions, Analytics Views, and Coordination Tables.
--                 This script adds the application-specific database logic that
--                 enables advanced features like analytics, provenance tracing,
--                 and distributed satellite coordination.
-- =============================================================================

-- Set session parameters for stability.
SET client_min_messages = warning;
SET statement_timeout = '5m';
SET lock_timeout = '10s';

-- =============================================================================
-- IX. HIGH-LEVEL DATABASE APIs (Functions)
--
-- These functions encapsulate complex business logic directly within the database
-- for performance, atomicity, and to create a clean API for the application layer.
-- =============================================================================

-- Provides a safe, batch-oriented mechanism for data retention, moving old events
-- to the audit archive instead of permanently deleting them.
-- This is a conceptual implementation of a correct, cascading archive function.
CREATE OR REPLACE FUNCTION core.archive_events_older_than_cascading(
    cutoff_date TIMESTAMPTZ
) RETURNS BIGINT AS $$
DECLARE
    archived_count BIGINT;
BEGIN
    -- This operation requires a high-level operation_id to be set.
    PERFORM current_setting('sinex.operation_id');
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Archival requires sinex.operation_id to be set.';
    END IF;

    WITH RECURSIVE events_to_archive AS (
        -- 1. Base Case: Find all root events older than the cutoff.
        --    These are typically raw events (no source_event_ids).
        SELECT id FROM core.events
        WHERE ts_ingest < cutoff_date AND source_event_ids IS NULL

        UNION

        -- 2. Recursive Step: Find all events that have a parent in our set.
        SELECT e.id
        FROM core.events e
        JOIN events_to_archive eta ON e.source_event_ids @> ARRAY[eta.id]
    ),
    deleted AS (
        -- 3. Delete the entire identified dependency graph.
        --    The BEFORE DELETE trigger will handle moving them to the audit table.
        DELETE FROM core.events
        WHERE id IN (SELECT id FROM events_to_archive)
        RETURNING 1
    )
    -- 4. Count the results.
    SELECT count(*) INTO archived_count FROM deleted;

    RETURN archived_count;
END;
$$ LANGUAGE sql;

COMMENT ON FUNCTION core.archive_events_older_than_cascading IS 'Safely archives events older than a specified date, correctly cascading to archive all dependent synthesis events to maintain provenance integrity.';

-- Traces the full lineage of a synthesized event back to its raw, observational roots.
-- This is a critical function for fulfilling the "Explainability" and "Auditable Metacognition" principles.
CREATE OR REPLACE FUNCTION core.get_event_lineage(
    start_event_id ULID,
    max_depth INTEGER DEFAULT 10
) RETURNS TABLE (
    level INTEGER,
    id ULID,
    event_type TEXT,
    source TEXT,
    ts_orig TIMESTAMPTZ,
    parent_event_ids ULID[]
) AS $$
    WITH RECURSIVE lineage AS (
        -- Base case: the starting event
        SELECT
            0 as level, e.id, e.event_type, e.source, e.ts_orig, e.source_event_ids as parent_event_ids
        FROM core.events e
        WHERE e.id = start_event_id

        UNION ALL

        -- Recursive step: join with parent events from the source_event_ids array
        SELECT
            l.level + 1, e.id, e.event_type, e.source, e.ts_orig, e.source_event_ids as parent_event_ids
        FROM lineage l
        JOIN core.events e ON e.id = ANY(l.parent_event_ids)
        WHERE l.level < max_depth AND l.parent_event_ids IS NOT NULL
    )
    SELECT * FROM lineage ORDER BY level;
$$ LANGUAGE sql STABLE;
COMMENT ON FUNCTION core.get_event_lineage IS 'Recursively traces the provenance of a synthesized event back to its source events.';


-- Provides a high-level API for interacting with the operations log, ensuring that
-- long-running operations are correctly tracked.
CREATE OR REPLACE FUNCTION core.start_operation(p_operation_type TEXT, p_operator TEXT, p_scope JSONB)
RETURNS ULID AS $$
DECLARE
    v_operation_id ULID;
BEGIN
    v_operation_id := gen_ulid();
    INSERT INTO core.operations_log (id, operation_type, operator, scope, result_status)
    VALUES (v_operation_id, p_operation_type, p_operator, p_scope, 'running');
    RETURN v_operation_id;
END;
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION core.start_operation IS 'Creates a new entry in the operations_log with a ''running'' status and returns its ID.';

CREATE OR REPLACE FUNCTION core.complete_operation(p_operation_id ULID, p_summary JSONB)
RETURNS VOID AS $$
BEGIN
    UPDATE core.operations_log
    SET result_status = 'success',
        result_message = p_summary->>'message',
        duration_ms = EXTRACT(MILLISECONDS FROM (NOW() - (id::timestamp)))::integer,
        preview_summary = COALESCE(preview_summary, '{}'::jsonb) || p_summary
    WHERE id = p_operation_id;
END;
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION core.complete_operation IS 'Marks an operation as successfully completed, calculating its duration.';

CREATE OR REPLACE FUNCTION core.fail_operation(p_operation_id ULID, p_error JSONB)
RETURNS VOID AS $$
BEGIN
    UPDATE core.operations_log
    SET result_status = 'failure',
        result_message = p_error->>'error',
        duration_ms = EXTRACT(MILLISECONDS FROM (NOW() - (id::timestamp)))::integer,
        preview_summary = COALESCE(preview_summary, '{}'::jsonb) || p_error
    WHERE id = p_operation_id;
END;
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION core.fail_operation IS 'Marks an operation as failed, calculating its duration and storing error details.';

-- =============================================================================
-- X. SATELLITE COORDINATION & LEADERSHIP
--
-- These tables provide the backend for the distributed coordination, leadership
-- election, and graceful handoff mechanisms used by the satellite constellation.
-- =============================================================================

-- Tracks all running instances of all satellites for service discovery and version management.
CREATE TABLE IF NOT EXISTS core.satellite_instances (
    id                      ULID PRIMARY KEY DEFAULT gen_ulid(),
    service_name            TEXT NOT NULL,
    instance_id             TEXT NOT NULL UNIQUE, -- A unique identifier for the process instance (e.g., hostname:pid).
    version                 TEXT NOT NULL,
    start_time              TIMESTAMPTZ NOT NULL,
    last_heartbeat          TIMESTAMPTZ NOT NULL,
    host_name               TEXT NOT NULL,
    metadata                JSONB NOT NULL DEFAULT '{}'
);
COMMENT ON TABLE core.satellite_instances IS 'Registry of all active satellite instances for service discovery and coordination.';

-- A simple signaling mechanism for inter-satellite communication, used for things
-- like requesting a graceful leadership handoff.
CREATE TABLE IF NOT EXISTS core.satellite_signals (
    id                      BIGSERIAL PRIMARY KEY,
    target_instance         TEXT NOT NULL, -- Can be a specific instance_id or 'ALL'.
    signal_type             TEXT NOT NULL CHECK (signal_type IN ('handoff_request', 'leader_failure', 'handoff_ready', 'shutdown')),
    message                 TEXT,
    payload                 JSONB NOT NULL DEFAULT '{}',
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processed_at            TIMESTAMPTZ
);
COMMENT ON TABLE core.satellite_signals IS 'A lightweight message queue for inter-satellite communication and control signals.';
CREATE INDEX IF NOT EXISTS idx_satellite_signals_target_unprocessed ON core.satellite_signals(target_instance, created_at) WHERE processed_at IS NULL;

-- Tracks the current leader for each service, enforced by PostgreSQL advisory locks.
CREATE TABLE IF NOT EXISTS core.service_leadership (
    service_name            TEXT PRIMARY KEY,
    instance_id             TEXT NOT NULL UNIQUE REFERENCES core.satellite_instances(instance_id) ON DELETE CASCADE,
    acquired_at             TIMESTAMPTZ NOT NULL,
    last_heartbeat          TIMESTAMPTZ NOT NULL,
    version                 TEXT NOT NULL
);
COMMENT ON TABLE core.service_leadership IS 'Tracks the current leader for each service, enabling high-availability patterns.';
CREATE INDEX IF NOT EXISTS idx_service_leadership_heartbeat ON core.service_leadership(last_heartbeat);

-- Function to clean up stale satellite instance records.
CREATE OR REPLACE FUNCTION core.cleanup_old_satellite_instances()
RETURNS INTEGER AS $$
DECLARE
    deleted_count INTEGER;
BEGIN
    DELETE FROM core.satellite_instances
    WHERE last_heartbeat < NOW() - INTERVAL '24 hours';
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- XI. ANALYTICS & REPORTING (Materialized Views)
--
-- These materialized views provide pre-aggregated data for high-performance
-- dashboarding and analytics. They are considered disposable and rebuildable.
-- =============================================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS metrics.process_heartbeats_hourly AS
SELECT
    time_bucket('1 hour', ts_ingest) AS bucket,
    source as process_name,
    host,
    COUNT(*) as heartbeat_count,
    AVG((payload->>'uptime_seconds')::numeric) as avg_uptime_seconds,
    MAX((payload->>'memory_usage_mb')::numeric) as max_memory_mb
FROM core.events
WHERE event_type = 'system.heartbeat'
GROUP BY bucket, process_name, host
WITH NO DATA;
COMMENT ON MATERIALIZED VIEW metrics.process_heartbeats_hourly IS 'Hourly aggregation of service heartbeats for health and resource monitoring.';

CREATE MATERIALIZED VIEW IF NOT EXISTS metrics.file_activity_hourly AS
SELECT
    time_bucket('1 hour', ts_ingest) AS bucket,
    event_type,
    COUNT(*) as operation_count,
    COUNT(DISTINCT payload->>'path') as unique_files,
    SUM((payload->>'size')::numeric) as total_bytes
FROM core.events
WHERE source = 'fs-watcher' AND event_type IN ('file.created', 'file.modified', 'file.deleted')
GROUP BY bucket, event_type
WITH NO DATA;
COMMENT ON MATERIALIZED VIEW metrics.file_activity_hourly IS 'Hourly aggregation of filesystem activity.';

-- Create indexes on the materialized views to speed up dashboard queries.
CREATE UNIQUE INDEX IF NOT EXISTS ix_process_heartbeats_hourly_unique ON metrics.process_heartbeats_hourly (bucket, process_name, host);
CREATE UNIQUE INDEX IF NOT EXISTS ix_file_activity_hourly_unique ON metrics.file_activity_hourly (bucket, event_type);

-- Function to refresh all materialized views concurrently.
CREATE OR REPLACE FUNCTION metrics.refresh_all_materialized_views()
RETURNS void AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY metrics.event_counts_by_type_hourly;
    REFRESH MATERIALIZED VIEW CONCURRENTLY metrics.process_heartbeats_hourly;
    REFRESH MATERIALIZED VIEW CONCURRENTLY metrics.file_activity_hourly;
    REFRESH MATERIALIZED VIEW CONCURRENTLY metrics.terminal_commands_daily;
END;
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION metrics.refresh_all_materialized_views() IS 'Refreshes all analytics materialized views concurrently.';



-- =============================================================================
-- X. FINALIZATION & COMMENTS
-- =============================================================================

-- Final comments on schemas to document their purpose.
COMMENT ON SCHEMA core IS 'Contains the canonical event log and all data synthesized from it, as well as operational state tables.';
COMMENT ON SCHEMA raw IS 'Contains immutable, append-only records of raw data acquisition, representing the ground truth.';
COMMENT ON SCHEMA audit IS 'Contains an immutable archive of superseded or deleted records, preserving the system''s entire history.';
COMMENT ON SCHEMA sinex_schemas IS 'Contains metadata and validation schemas that act as the data contracts for the event system.';
COMMENT ON SCHEMA metrics IS 'Contains materialized views and functions for high-performance analytics and reporting.';
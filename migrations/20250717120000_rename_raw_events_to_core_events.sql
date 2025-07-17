-- Migration: Rename raw.events to core.events and add new unified architecture columns
-- 
-- This migration implements the unified architecture from plan.md:
-- 1. Moves raw.events table to core.events  
-- 2. Adds new provenance and correlation columns
-- 3. Renames primary key column from id to event_id
-- 4. Maps blob_id to associated_blob_ids array
-- 5. Updates all indexes, constraints, and triggers
-- 6. Updates archived_events table to match

-- =============================================================================
-- Part 1: Create core schema if it doesn't exist
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS core;

-- =============================================================================
-- Part 2: Create the new core.events table with the target schema
-- =============================================================================

-- Create the new core.events table with the target schema from plan.md
CREATE TABLE core.events (
    event_id ULID PRIMARY KEY,
    ts_ingest TIMESTAMPTZ NOT NULL GENERATED ALWAYS AS (event_id::timestamp) STORED,

    -- The Interpretation
    event_type TEXT NOT NULL,
    source TEXT NOT NULL,           -- The processor (ingestor/automaton) that created this interpretation
    ts_orig TIMESTAMPTZ,            -- The conceptual timestamp, derived from the source material
    host TEXT NOT NULL,
    payload JSONB NOT NULL,         -- The "pleasant" structured JSON data
    correlation_id ULID,            -- For end-to-end distributed tracing

    -- Schema fields (preserved from current implementation)
    ingestor_version TEXT,
    payload_schema_id ULID,
    payload_schema_name TEXT,
    payload_schema_version TEXT,

    -- Provenance Links (Dual-Layer)
    source_material_id ULID REFERENCES raw.source_material_registry(blob_id), -- External provenance (to the "whole blob")
    source_material_offset_start BIGINT, -- The "Anchor Byte" offset within the blob
    source_material_offset_end BIGINT,
    source_event_ids ULID[],         -- Internal provenance (to other events in this table)

    -- Convenience Link to Associated Data (e.g., a screenshot)
    associated_blob_ids ULID[],

    -- Constraints
    CONSTRAINT events_event_type_check CHECK (length(TRIM(BOTH FROM event_type)) > 0),
    CONSTRAINT events_host_check CHECK (length(TRIM(BOTH FROM host)) > 0),
    CONSTRAINT events_source_check CHECK (length(TRIM(BOTH FROM source)) > 0),
    
    -- The Natural Key makes a raw event's identity deterministic
    CONSTRAINT unique_raw_event_origin UNIQUE (source_material_id, source_material_offset_start)
);

-- Add comments
COMMENT ON TABLE core.events IS 'Universal log for all captured events (raw and synthesis). The single source of truth for event interpretations with full provenance tracking.';
COMMENT ON COLUMN core.events.event_id IS 'Globally unique identifier for the event using ULID (Universally Unique Lexicographically Sortable Identifier).';
COMMENT ON COLUMN core.events.source IS 'Canonical identifier for the event origin/producer (e.g., "desktop.hyprland.ipc_ingestor", "sinex.pkm.sync_agent").';
COMMENT ON COLUMN core.events.event_type IS 'Type string for the event, often namespaced by source (e.g., "window_focused", "note_updated", "agent.heartbeat").';
COMMENT ON COLUMN core.events.ts_ingest IS 'Timestamp of ingestion extracted from ULID (GENERATED column). Primary TimescaleDB partitioning key.';
COMMENT ON COLUMN core.events.ts_orig IS 'Original timestamp from the source system/sensor when the event occurred; best effort for accuracy.';
COMMENT ON COLUMN core.events.host IS 'Identifier of the machine or device where the event originated.';
COMMENT ON COLUMN core.events.payload IS 'Complete event data as JSONB. May contain a "_provenance" sub-object for lineage details.';
COMMENT ON COLUMN core.events.correlation_id IS 'Unique identifier for end-to-end distributed tracing of related events.';
COMMENT ON COLUMN core.events.source_material_id IS 'Reference to the external blob that was the source for this event interpretation.';
COMMENT ON COLUMN core.events.source_material_offset_start IS 'Starting byte offset within the source material blob (the "Anchor Byte").';
COMMENT ON COLUMN core.events.source_material_offset_end IS 'Ending byte offset within the source material blob.';
COMMENT ON COLUMN core.events.source_event_ids IS 'Provenance chain: NULL for raw events (direct observations), array of ULIDs for synthesis events (derived from other events).';
COMMENT ON COLUMN core.events.associated_blob_ids IS 'Array of blob IDs for additional data associated with this event (e.g., screenshots, recordings).';

-- =============================================================================
-- Part 3: Migrate data from raw.events to core.events
-- =============================================================================

-- Copy all data from raw.events to core.events, mapping columns appropriately
INSERT INTO core.events (
    event_id,
    -- ts_ingest is GENERATED from event_id
    event_type,
    source,
    ts_orig,
    host,
    payload,
    correlation_id,
    ingestor_version,
    payload_schema_id,
    payload_schema_name,
    payload_schema_version,
    source_material_id,
    source_material_offset_start,
    source_material_offset_end,
    source_event_ids,
    associated_blob_ids
)
SELECT 
    id,  -- id -> event_id
    event_type,
    source,
    ts_orig,
    host,
    payload,
    NULL,  -- correlation_id (new column, starts NULL)
    ingestor_version,
    payload_schema_id,
    payload_schema_name,
    payload_schema_version,
    NULL,  -- source_material_id (new column, starts NULL)  
    NULL,  -- source_material_offset_start (new column, starts NULL)
    NULL,  -- source_material_offset_end (new column, starts NULL)
    source_event_ids,
    CASE 
        WHEN blob_id IS NOT NULL THEN ARRAY[blob_id]  -- blob_id -> associated_blob_ids array
        ELSE NULL 
    END
FROM raw.events;

-- =============================================================================
-- Part 4: Recreate indexes on core.events
-- =============================================================================

-- Primary Key index is created automatically

-- Time-based indexes
CREATE INDEX IF NOT EXISTS idx_core_events_ts_ingest ON core.events (ts_ingest DESC);
CREATE INDEX IF NOT EXISTS idx_core_events_ts_orig_desc ON core.events (ts_orig DESC NULLS LAST);

-- Source and type indexes  
CREATE INDEX IF NOT EXISTS idx_core_events_source_ts ON core.events (source, ts_ingest DESC);
CREATE INDEX IF NOT EXISTS idx_core_events_source_type_ts ON core.events (source, event_type, ts_ingest DESC);
CREATE INDEX IF NOT EXISTS idx_core_events_host_ts ON core.events (host, ts_ingest DESC);

-- Schema-related indexes
CREATE INDEX IF NOT EXISTS idx_core_events_schema_id ON core.events (payload_schema_id) WHERE payload_schema_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_core_events_schema_name ON core.events (payload_schema_name) WHERE payload_schema_name IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_core_events_schema_version ON core.events (payload_schema_name, payload_schema_version) WHERE payload_schema_name IS NOT NULL AND payload_schema_version IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_core_events_no_schema ON core.events (ts_ingest) WHERE payload_schema_name IS NULL;

-- Provenance indexes
CREATE INDEX IF NOT EXISTS idx_core_events_provenance ON core.events USING GIN (source_event_ids) WHERE source_event_ids IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_core_events_source_material ON core.events (source_material_id) WHERE source_material_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_core_events_source_material_range ON core.events (source_material_id, source_material_offset_start) WHERE source_material_id IS NOT NULL;

-- Associated blob indexes
CREATE INDEX IF NOT EXISTS idx_core_events_associated_blobs ON core.events USING GIN (associated_blob_ids) WHERE associated_blob_ids IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_core_events_no_blobs ON core.events (event_id) WHERE associated_blob_ids IS NULL;

-- Correlation tracking
CREATE INDEX IF NOT EXISTS idx_core_events_correlation_id ON core.events (correlation_id) WHERE correlation_id IS NOT NULL;

-- JSONB payload index
CREATE INDEX IF NOT EXISTS idx_core_events_payload_gin_path_ops ON core.events USING GIN (payload jsonb_path_ops);

-- =============================================================================
-- Part 5: Add foreign key constraints
-- =============================================================================

-- Foreign key to schema registry (if it exists)
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_schema = 'sinex_schemas' 
        AND table_name = 'event_payload_schemas'
    ) THEN
        ALTER TABLE core.events 
        ADD CONSTRAINT fk_core_events_schema_id 
        FOREIGN KEY (payload_schema_id) 
        REFERENCES sinex_schemas.event_payload_schemas(id)
        ON DELETE SET NULL;
    END IF;
END $$;

-- Foreign key to blobs table (for associated_blob_ids - note: this will be enforced at application level due to array type)
-- No foreign key constraint for associated_blob_ids since it's an array - will be enforced in application code

-- =============================================================================
-- Part 6: Update audit.archived_events table to match new schema
-- =============================================================================

-- Drop dependent views first
DROP VIEW IF EXISTS audit.events_with_archive_status;

-- Drop the old archived_events table and recreate with new schema
DROP TABLE IF EXISTS audit.archived_events;

CREATE TABLE audit.archived_events (
    -- Archive metadata (added fields)
    archived_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    archived_by TEXT DEFAULT 'system',
    archive_reason TEXT,
    superseded_by_event_id ULID, -- Links to the new event that replaced this one
    
    -- Original events table structure (matching core.events exactly)
    event_id ULID NOT NULL,
    ts_ingest TIMESTAMPTZ,
    event_type TEXT NOT NULL,
    source TEXT NOT NULL,
    ts_orig TIMESTAMPTZ,
    host TEXT NOT NULL,
    payload JSONB NOT NULL,
    correlation_id ULID,
    ingestor_version TEXT,
    payload_schema_id ULID,
    payload_schema_name TEXT,
    payload_schema_version TEXT,
    source_material_id ULID,
    source_material_offset_start BIGINT,
    source_material_offset_end BIGINT,
    source_event_ids ULID[],
    associated_blob_ids ULID[],
    
    -- Archive table constraints
    CONSTRAINT archived_events_source_check CHECK (length(TRIM(BOTH FROM source)) > 0),
    CONSTRAINT archived_events_event_type_check CHECK (length(TRIM(BOTH FROM event_type)) > 0),
    CONSTRAINT archived_events_host_check CHECK (length(TRIM(BOTH FROM host)) > 0)
);

-- Create indexes for efficient archive queries
CREATE INDEX idx_archived_events_archived_at ON audit.archived_events (archived_at DESC);
CREATE INDEX idx_archived_events_original_id ON audit.archived_events (event_id);
CREATE INDEX idx_archived_events_superseded_by ON audit.archived_events (superseded_by_event_id) 
WHERE superseded_by_event_id IS NOT NULL;
CREATE INDEX idx_archived_events_source_type_ts ON audit.archived_events (source, event_type, ts_orig DESC);

-- Add comments
COMMENT ON TABLE audit.archived_events IS 
'Archive of all logically deleted events. Enables safe, auditable replay operations and provides complete data lineage.';

COMMENT ON COLUMN audit.archived_events.superseded_by_event_id IS 
'ULID of the event that replaced this archived event during a replay operation. NULL for deletions without replacement.';

-- =============================================================================
-- Part 7: Update the archive trigger function
-- =============================================================================

-- Drop and recreate the archive trigger function to handle new schema
DROP TRIGGER IF EXISTS trg_archive_deleted_events ON raw.events;
DROP FUNCTION IF EXISTS raw.archive_deleted_event();

-- Create updated archive trigger function for core.events
CREATE OR REPLACE FUNCTION core.archive_deleted_event()
RETURNS TRIGGER AS $$
BEGIN
    -- Insert the deleted event into the archive table
    INSERT INTO audit.archived_events (
        archived_at,
        archived_by,
        archive_reason,
        superseded_by_event_id,
        -- Original event data
        event_id,
        ts_ingest,
        event_type,
        source,
        ts_orig,
        host,
        payload,
        correlation_id,
        ingestor_version,
        payload_schema_id,
        payload_schema_name,
        payload_schema_version,
        source_material_id,
        source_material_offset_start,
        source_material_offset_end,
        source_event_ids,
        associated_blob_ids
    ) VALUES (
        NOW(),
        COALESCE(current_setting('sinex.archived_by', true), 'system'),
        COALESCE(current_setting('sinex.archive_reason', true), 'unspecified'),
        CASE 
            WHEN current_setting('sinex.superseded_by_event_id', true) = '' THEN NULL
            ELSE current_setting('sinex.superseded_by_event_id', true)::ULID
        END,
        -- Copy all original event data
        OLD.event_id,
        OLD.ts_ingest,
        OLD.event_type,
        OLD.source,
        OLD.ts_orig,
        OLD.host,
        OLD.payload,
        OLD.correlation_id,
        OLD.ingestor_version,
        OLD.payload_schema_id,
        OLD.payload_schema_name,
        OLD.payload_schema_version,
        OLD.source_material_id,
        OLD.source_material_offset_start,
        OLD.source_material_offset_end,
        OLD.source_event_ids,
        OLD.associated_blob_ids
    );
    
    -- Log the archival operation
    RAISE NOTICE 'Event % archived: % (reason: %)', 
        OLD.event_id, 
        COALESCE(current_setting('sinex.archived_by', true), 'system'),
        COALESCE(current_setting('sinex.archive_reason', true), 'unspecified');
    
    -- Allow the DELETE to proceed
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

-- Create the BEFORE DELETE trigger on core.events
CREATE TRIGGER trg_archive_deleted_events
    BEFORE DELETE ON core.events
    FOR EACH ROW
    EXECUTE FUNCTION core.archive_deleted_event();

-- Add comment explaining the trigger
COMMENT ON TRIGGER trg_archive_deleted_events ON core.events IS 
'Automatically archives events to audit.archived_events before deletion. This implements the "never truly delete" principle for data integrity.';

-- =============================================================================
-- Part 8: Update helper functions to use new schema
-- =============================================================================

-- Update the set_archive_metadata function to use core schema
DROP FUNCTION IF EXISTS raw.set_archive_metadata(TEXT, TEXT, ULID);

CREATE OR REPLACE FUNCTION core.set_archive_metadata(
    archived_by_param TEXT DEFAULT 'system',
    archive_reason_param TEXT DEFAULT 'unspecified',
    superseded_by_event_id_param ULID DEFAULT NULL
) RETURNS VOID AS $$
BEGIN
    PERFORM set_config('sinex.archived_by', archived_by_param, true);
    PERFORM set_config('sinex.archive_reason', archive_reason_param, true);
    PERFORM set_config('sinex.superseded_by_event_id', 
                      COALESCE(superseded_by_event_id_param::TEXT, ''), true);
END;
$$ LANGUAGE plpgsql;

-- Update the find_dependent_events function
DROP FUNCTION IF EXISTS raw.find_dependent_events(ULID);

CREATE OR REPLACE FUNCTION core.find_dependent_events(root_event_id ULID)
RETURNS TABLE(event_id ULID, dependency_depth INTEGER) AS $$
WITH RECURSIVE dependency_tree AS (
    -- Base case: events that directly depend on the root event
    SELECT event_id as event_id, 1 as dependency_depth
    FROM core.events 
    WHERE source_event_ids @> ARRAY[root_event_id]
    
    UNION ALL
    
    -- Recursive case: events that depend on events we've already found
    SELECT e.event_id as event_id, dt.dependency_depth + 1
    FROM core.events e
    JOIN dependency_tree dt ON e.source_event_ids && ARRAY[dt.event_id]
    WHERE dt.dependency_depth < 10 -- Prevent infinite recursion
)
SELECT event_id, dependency_depth FROM dependency_tree
ORDER BY dependency_depth DESC, event_id;
$$ LANGUAGE sql;

-- Update the find_root_events function
DROP FUNCTION IF EXISTS raw.find_root_events(ULID);

CREATE OR REPLACE FUNCTION core.find_root_events(synthesis_event_id ULID)
RETURNS TABLE(event_id ULID, dependency_depth INTEGER) AS $$
WITH RECURSIVE provenance_tree AS (
    -- Base case: the synthesis event itself
    SELECT synthesis_event_id as event_id, 0 as dependency_depth
    
    UNION ALL
    
    -- Recursive case: source events of events we've already found
    SELECT unnest(e.source_event_ids) as event_id, pt.dependency_depth + 1
    FROM core.events e
    JOIN provenance_tree pt ON e.event_id = pt.event_id
    WHERE e.source_event_ids IS NOT NULL
      AND pt.dependency_depth < 10 -- Prevent infinite recursion
)
SELECT event_id, dependency_depth FROM provenance_tree
WHERE event_id != synthesis_event_id
ORDER BY dependency_depth DESC, event_id;
$$ LANGUAGE sql;

-- Update the restore function
DROP FUNCTION IF EXISTS audit.restore_archived_event(ULID);

CREATE OR REPLACE FUNCTION audit.restore_archived_event(archived_event_id ULID)
RETURNS BOOLEAN AS $$
DECLARE
    restored_event audit.archived_events%ROWTYPE;
BEGIN
    -- Get the archived event
    SELECT * INTO restored_event 
    FROM audit.archived_events 
    WHERE event_id = archived_event_id;
    
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Archived event with ID % not found', archived_event_id;
    END IF;
    
    -- Restore to core.events (excluding archive metadata)
    INSERT INTO core.events (
        event_id, event_type, source, ts_orig, host, payload, correlation_id,
        ingestor_version, payload_schema_id, payload_schema_name, payload_schema_version,
        source_material_id, source_material_offset_start, source_material_offset_end,
        source_event_ids, associated_blob_ids
    ) VALUES (
        restored_event.event_id,
        restored_event.event_type,
        restored_event.source,
        restored_event.ts_orig,
        restored_event.host,
        restored_event.payload,
        restored_event.correlation_id,
        restored_event.ingestor_version,
        restored_event.payload_schema_id,
        restored_event.payload_schema_name,
        restored_event.payload_schema_version,
        restored_event.source_material_id,
        restored_event.source_material_offset_start,
        restored_event.source_material_offset_end,
        restored_event.source_event_ids,
        restored_event.associated_blob_ids
    );
    
    -- Remove from archive
    DELETE FROM audit.archived_events WHERE event_id = archived_event_id;
    
    RETURN TRUE;
EXCEPTION
    WHEN unique_violation THEN
        RAISE EXCEPTION 'Cannot restore event %: already exists in core.events', archived_event_id;
    WHEN OTHERS THEN
        RAISE;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- Part 9: Update views to use new schema
-- =============================================================================

-- Drop and recreate the events_with_archive_status view
DROP VIEW IF EXISTS audit.events_with_archive_status;

CREATE VIEW audit.events_with_archive_status AS
SELECT 
    event_id,
    source,
    event_type,
    ts_orig,
    ts_ingest,
    host,
    payload,
    source_event_ids,
    'active' as status,
    NULL::TIMESTAMPTZ as archived_at,
    NULL::TEXT as archive_reason
FROM core.events

UNION ALL

SELECT 
    event_id,
    source,
    event_type,
    ts_orig,
    ts_ingest,
    host,
    payload,
    source_event_ids,
    'archived' as status,
    archived_at,
    archive_reason
FROM audit.archived_events;

-- =============================================================================
-- Part 10: Update any triggers that reference raw.events
-- =============================================================================

-- Update metrics trigger if it exists
DO $$
BEGIN
    -- Drop existing trigger if it exists
    DROP TRIGGER IF EXISTS metrics_fanout ON raw.events;
    
    -- Recreate on core.events if the function exists
    IF EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'fanout' AND pronamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'metrics')) THEN
        CREATE TRIGGER metrics_fanout 
        AFTER INSERT ON core.events 
        FOR EACH ROW 
        EXECUTE FUNCTION metrics.fanout();
    END IF;
END $$;

-- Update schema validation trigger if it exists
DO $$
BEGIN
    -- Drop existing trigger if it exists  
    DROP TRIGGER IF EXISTS trg_validate_event_payload_schema ON raw.events;
    
    -- Recreate on core.events if the function exists
    IF EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'validate_event_payload_schema' AND pronamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'raw')) THEN
        -- Move the function to core schema
        ALTER FUNCTION raw.validate_event_payload_schema() SET SCHEMA core;
        
        CREATE TRIGGER trg_validate_event_payload_schema 
        BEFORE INSERT OR UPDATE OF payload, payload_schema_id ON core.events 
        FOR EACH ROW 
        EXECUTE FUNCTION core.validate_event_payload_schema();
    END IF;
END $$;

-- Update TimescaleDB insert blocker if it exists
DO $$
BEGIN
    -- Drop existing trigger if it exists
    DROP TRIGGER IF EXISTS ts_insert_blocker ON raw.events;
    
    -- Recreate on core.events if the function exists
    IF EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'insert_blocker' AND pronamespace = (SELECT oid FROM pg_namespace WHERE nspname = '_timescaledb_functions')) THEN
        CREATE TRIGGER ts_insert_blocker 
        BEFORE INSERT ON core.events 
        FOR EACH ROW 
        EXECUTE FUNCTION _timescaledb_functions.insert_blocker();
    END IF;
END $$;

-- =============================================================================
-- Part 11: Convert core.events to hypertable (TimescaleDB)
-- =============================================================================

-- Convert core.events to hypertable if TimescaleDB is available
DO $$
BEGIN
    -- Check if TimescaleDB extension is available
    IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'timescaledb') THEN
        -- Convert to hypertable
        PERFORM create_hypertable('core.events', 'ts_ingest', 
                                 chunk_time_interval => INTERVAL '1 day',
                                 if_not_exists => TRUE);
        
        RAISE NOTICE 'Converted core.events to TimescaleDB hypertable';
    ELSE
        RAISE NOTICE 'TimescaleDB not available, skipping hypertable conversion';
    END IF;
EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'Failed to convert to hypertable: %', SQLERRM;
END $$;

-- =============================================================================
-- Part 12: Update foreign key references
-- =============================================================================

-- Update all tables that reference raw.events to reference core.events
-- Note: This section handles the foreign key updates carefully

-- Update core.revisions
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'core' AND table_name = 'revisions') THEN
        -- Drop old constraint
        ALTER TABLE core.revisions DROP CONSTRAINT IF EXISTS artifact_contents_created_from_event_id_fkey;
        
        -- Add new constraint  
        ALTER TABLE core.revisions 
        ADD CONSTRAINT revisions_created_from_event_id_fkey 
        FOREIGN KEY (created_from_event_id) REFERENCES core.events(event_id);
    END IF;
END $$;

-- Update core.artifact_event_sources
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'core' AND table_name = 'artifact_event_sources') THEN
        ALTER TABLE core.artifact_event_sources DROP CONSTRAINT IF EXISTS artifact_event_sources_event_id_fkey;
        ALTER TABLE core.artifact_event_sources 
        ADD CONSTRAINT artifact_event_sources_event_id_fkey 
        FOREIGN KEY (event_id) REFERENCES core.events(event_id) ON DELETE CASCADE;
    END IF;
END $$;

-- Update core.artifact_tags
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'core' AND table_name = 'artifact_tags') THEN
        ALTER TABLE core.artifact_tags DROP CONSTRAINT IF EXISTS artifact_tags_tagged_from_event_id_fkey;
        ALTER TABLE core.artifact_tags 
        ADD CONSTRAINT artifact_tags_tagged_from_event_id_fkey 
        FOREIGN KEY (tagged_from_event_id) REFERENCES core.events(event_id);
    END IF;
END $$;

-- Update core.artifacts
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'core' AND table_name = 'artifacts') THEN
        ALTER TABLE core.artifacts DROP CONSTRAINT IF EXISTS artifacts_created_from_event_id_fkey;
        ALTER TABLE core.artifacts 
        ADD CONSTRAINT artifacts_created_from_event_id_fkey 
        FOREIGN KEY (created_from_event_id) REFERENCES core.events(event_id);
    END IF;
END $$;

-- Update core.entity_relations
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'core' AND table_name = 'entity_relations') THEN
        ALTER TABLE core.entity_relations DROP CONSTRAINT IF EXISTS entity_relations_created_from_event_id_fkey;
        ALTER TABLE core.entity_relations 
        ADD CONSTRAINT entity_relations_created_from_event_id_fkey 
        FOREIGN KEY (created_from_event_id) REFERENCES core.events(event_id);
    END IF;
END $$;

-- Update core.event_annotations
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'core' AND table_name = 'event_annotations') THEN
        ALTER TABLE core.event_annotations DROP CONSTRAINT IF EXISTS event_annotations_event_id_fkey;
        ALTER TABLE core.event_annotations 
        ADD CONSTRAINT event_annotations_event_id_fkey 
        FOREIGN KEY (event_id) REFERENCES core.events(event_id) ON DELETE CASCADE;
    END IF;
END $$;

-- Update core.event_artifact_refs
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'core' AND table_name = 'event_artifact_refs') THEN
        ALTER TABLE core.event_artifact_refs DROP CONSTRAINT IF EXISTS event_artifact_refs_event_id_fkey;
        ALTER TABLE core.event_artifact_refs 
        ADD CONSTRAINT event_artifact_refs_event_id_fkey 
        FOREIGN KEY (event_id) REFERENCES core.events(event_id) ON DELETE CASCADE;
    END IF;
END $$;

-- Update core.event_cluster_members
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'core' AND table_name = 'event_cluster_members') THEN
        ALTER TABLE core.event_cluster_members DROP CONSTRAINT IF EXISTS event_cluster_members_event_id_fkey;
        ALTER TABLE core.event_cluster_members 
        ADD CONSTRAINT event_cluster_members_event_id_fkey 
        FOREIGN KEY (event_id) REFERENCES core.events(event_id) ON DELETE CASCADE;
    END IF;
END $$;

-- Update core.event_embeddings
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'core' AND table_name = 'event_embeddings') THEN
        ALTER TABLE core.event_embeddings DROP CONSTRAINT IF EXISTS event_embeddings_event_id_fkey;
        ALTER TABLE core.event_embeddings 
        ADD CONSTRAINT event_embeddings_event_id_fkey 
        FOREIGN KEY (event_id) REFERENCES core.events(event_id) ON DELETE CASCADE;
    END IF;
END $$;

-- Update core.event_relations
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'core' AND table_name = 'event_relations') THEN
        ALTER TABLE core.event_relations DROP CONSTRAINT IF EXISTS event_relations_from_event_id_fkey;
        ALTER TABLE core.event_relations DROP CONSTRAINT IF EXISTS event_relations_to_event_id_fkey;
        
        ALTER TABLE core.event_relations 
        ADD CONSTRAINT event_relations_from_event_id_fkey 
        FOREIGN KEY (from_event_id) REFERENCES core.events(event_id) ON DELETE CASCADE;
        
        ALTER TABLE core.event_relations 
        ADD CONSTRAINT event_relations_to_event_id_fkey 
        FOREIGN KEY (to_event_id) REFERENCES core.events(event_id) ON DELETE CASCADE;
    END IF;
END $$;

-- =============================================================================
-- Part 13: Drop the old raw.events table
-- =============================================================================

-- Drop the old raw.events table (all data has been migrated)
DROP TABLE IF EXISTS raw.events CASCADE;

-- =============================================================================
-- Part 14: Grant appropriate permissions
-- =============================================================================

-- Grant permissions for the new schema and tables
GRANT USAGE ON SCHEMA core TO PUBLIC;
GRANT SELECT ON core.events TO PUBLIC;
GRANT USAGE ON SCHEMA audit TO PUBLIC;
GRANT SELECT ON audit.archived_events TO PUBLIC;
GRANT SELECT ON audit.events_with_archive_status TO PUBLIC;

-- =============================================================================
-- Part 15: Final success message
-- =============================================================================

DO $$
BEGIN
    RAISE NOTICE '✅ Successfully renamed raw.events to core.events with unified architecture';
    RAISE NOTICE '📊 Migration completed:';
    RAISE NOTICE '   - Renamed id column to event_id';
    RAISE NOTICE '   - Added correlation_id for distributed tracing';
    RAISE NOTICE '   - Added source_material_id and offset fields for external provenance';
    RAISE NOTICE '   - Converted blob_id to associated_blob_ids array';
    RAISE NOTICE '   - Added unique constraint for deterministic raw event identity';
    RAISE NOTICE '   - Updated all indexes, triggers, and foreign key references';
    RAISE NOTICE '   - Updated archived_events table to match new schema';
    RAISE NOTICE '🔄 Ready for unified architecture implementation';
END $$;
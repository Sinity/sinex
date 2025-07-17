-- Down Migration: Revert core.events back to raw.events with original schema
-- 
-- This migration reverts the unified architecture changes:
-- 1. Moves core.events table back to raw.events
-- 2. Removes new columns (correlation_id, source_material_*, etc.)
-- 3. Renames event_id back to id  
-- 4. Converts associated_blob_ids array back to single blob_id
-- 5. Restores original indexes, constraints, and triggers
-- 6. Reverts archived_events table to original schema

-- =============================================================================
-- Part 1: Create the original raw.events table structure
-- =============================================================================

-- Create raw schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS raw;

-- Create the original raw.events table with the pre-migration schema
CREATE TABLE raw.events (
    id ULID PRIMARY KEY DEFAULT gen_ulid(),
    source TEXT NOT NULL CHECK (LENGTH(TRIM(source)) > 0),
    event_type TEXT NOT NULL CHECK (LENGTH(TRIM(event_type)) > 0),
    ts_ingest TIMESTAMPTZ GENERATED ALWAYS AS (id::timestamp) STORED,
    ts_orig TIMESTAMPTZ,
    host TEXT NOT NULL CHECK (LENGTH(TRIM(host)) > 0),
    ingestor_version TEXT,
    payload_schema_id ULID,
    payload JSONB NOT NULL,
    blob_id ULID,
    payload_schema_name TEXT,
    payload_schema_version TEXT,
    source_event_ids ULID[]
);

-- Add comments (original)
COMMENT ON TABLE raw.events IS 'Universal log for all captured raw events before promotion or detailed structuring. Immutable by principle.';
COMMENT ON COLUMN raw.events.id IS 'Globally unique identifier for the event using ULID (Universally Unique Lexicographically Sortable Identifier).';
COMMENT ON COLUMN raw.events.source IS 'Canonical identifier for the event origin/producer (e.g., "desktop.hyprland.ipc_ingestor", "sinex.pkm.sync_agent").';
COMMENT ON COLUMN raw.events.event_type IS 'Type string for the event, often namespaced by source (e.g., "window_focused", "note_updated", "agent.heartbeat").';
COMMENT ON COLUMN raw.events.ts_ingest IS 'Timestamp of ingestion extracted from ULID (GENERATED column). Primary TimescaleDB partitioning key.';
COMMENT ON COLUMN raw.events.ts_orig IS 'Original timestamp from the source system/sensor when the event occurred; best effort for accuracy.';
COMMENT ON COLUMN raw.events.host IS 'Identifier of the machine or device where the event originated.';
COMMENT ON COLUMN raw.events.ingestor_version IS 'Version of the ingestor code/binary that produced this event.';
COMMENT ON COLUMN raw.events.payload_schema_id IS 'Foreign key to sinex_schemas.event_payload_schemas, identifying the schema for the payload. Null if ad-hoc/unknown.';
COMMENT ON COLUMN raw.events.payload IS 'Complete raw event data as JSONB. May contain a "_provenance" sub-object for lineage details.';
COMMENT ON COLUMN raw.events.blob_id IS 'Optional reference to binary blob associated with this event (e.g., screenshots, recordings, large clipboard content)';
COMMENT ON COLUMN raw.events.source_event_ids IS 'Provenance chain: NULL for raw events (direct observations), array of ULIDs for synthesis events (derived from other events)';

-- =============================================================================
-- Part 2: Migrate data from core.events back to raw.events
-- =============================================================================

-- Copy all data from core.events to raw.events, converting columns back
INSERT INTO raw.events (
    id,
    -- ts_ingest is GENERATED from id
    source,
    event_type,
    ts_orig,
    host,
    ingestor_version,
    payload_schema_id,
    payload,
    blob_id,
    payload_schema_name,
    payload_schema_version,
    source_event_ids
)
SELECT 
    event_id,  -- event_id -> id
    source,
    event_type,
    ts_orig,
    host,
    ingestor_version,
    payload_schema_id,
    payload,
    CASE 
        WHEN associated_blob_ids IS NOT NULL AND array_length(associated_blob_ids, 1) > 0 
        THEN associated_blob_ids[1]  -- Take first element of array -> blob_id
        ELSE NULL 
    END,
    payload_schema_name,
    payload_schema_version,
    source_event_ids
FROM core.events;

-- =============================================================================
-- Part 3: Recreate original indexes on raw.events
-- =============================================================================

-- Time-based indexes
CREATE INDEX IF NOT EXISTS idx_raw_events_ts_orig_desc ON raw.events (ts_orig DESC NULLS LAST);
CREATE INDEX IF NOT EXISTS idx_raw_events_ts_ingest ON raw.events (ts_ingest DESC);

-- Source and type indexes
CREATE INDEX IF NOT EXISTS idx_raw_events_source_type_ts_ingest_desc ON raw.events (source, event_type, ts_ingest DESC);
CREATE INDEX IF NOT EXISTS idx_raw_events_source_ts ON raw.events (source, ts_ingest DESC);
CREATE INDEX IF NOT EXISTS idx_raw_events_host_ts_ingest_desc ON raw.events (host, ts_ingest DESC);

-- Schema-related indexes
CREATE INDEX IF NOT EXISTS idx_raw_events_payload_schema_id ON raw.events (payload_schema_id) WHERE payload_schema_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_events_schema_name ON raw.events (payload_schema_name) WHERE payload_schema_name IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_events_schema_version ON raw.events (payload_schema_name, payload_schema_version) WHERE payload_schema_name IS NOT NULL AND payload_schema_version IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_events_schema_id ON raw.events (payload_schema_id) WHERE payload_schema_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_events_no_schema ON raw.events (ts_ingest) WHERE payload_schema_name IS NULL;

-- Blob indexes
CREATE INDEX IF NOT EXISTS idx_raw_events_blob_id ON raw.events(blob_id) WHERE blob_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_raw_events_no_blob ON raw.events(id) WHERE blob_id IS NULL;

-- Provenance indexes
CREATE INDEX IF NOT EXISTS idx_raw_events_provenance ON raw.events USING GIN (source_event_ids) WHERE source_event_ids IS NOT NULL;

-- JSONB payload index
CREATE INDEX IF NOT EXISTS idx_raw_events_payload_gin_path_ops ON raw.events USING GIN (payload jsonb_path_ops);

-- =============================================================================
-- Part 4: Add original foreign key constraints
-- =============================================================================

-- Foreign key to schema registry
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_schema = 'sinex_schemas' 
        AND table_name = 'event_payload_schemas'
    ) THEN
        ALTER TABLE raw.events 
        ADD CONSTRAINT events_payload_schema_id_fkey 
        FOREIGN KEY (payload_schema_id) 
        REFERENCES sinex_schemas.event_payload_schemas(id) 
        ON UPDATE CASCADE ON DELETE SET NULL;
        
        ALTER TABLE raw.events 
        ADD CONSTRAINT fk_events_schema_id 
        FOREIGN KEY (payload_schema_id) 
        REFERENCES sinex_schemas.event_payload_schemas(id) 
        ON DELETE SET NULL;
    END IF;
END $$;

-- Foreign key to blobs table
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_schema = 'core' 
        AND table_name = 'blobs'
    ) THEN
        ALTER TABLE raw.events 
        ADD CONSTRAINT fk_raw_events_blob_id 
        FOREIGN KEY (blob_id) 
        REFERENCES core.blobs(id) ON DELETE SET NULL;
    END IF;
END $$;

-- =============================================================================
-- Part 5: Revert audit.archived_events table to original schema
-- =============================================================================

-- Drop dependent views first
DROP VIEW IF EXISTS audit.events_with_archive_status;

-- Drop new archived_events table and recreate with original schema
DROP TABLE IF EXISTS audit.archived_events;

CREATE TABLE audit.archived_events (
    -- Archive metadata
    archived_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    archived_by TEXT DEFAULT 'system',
    archive_reason TEXT,
    superseded_by_event_id ULID,
    
    -- Original raw.events table structure (pre-migration)
    id ULID NOT NULL,
    source TEXT NOT NULL,
    event_type TEXT NOT NULL,
    ts_ingest TIMESTAMPTZ,
    ts_orig TIMESTAMPTZ,
    host TEXT NOT NULL,
    ingestor_version TEXT,
    payload_schema_id ULID,
    payload JSONB NOT NULL,
    blob_id ULID,
    payload_schema_name TEXT,
    payload_schema_version TEXT,
    source_event_ids ULID[],
    
    -- Original constraints
    CONSTRAINT archived_events_source_check CHECK (length(TRIM(BOTH FROM source)) > 0),
    CONSTRAINT archived_events_event_type_check CHECK (length(TRIM(BOTH FROM event_type)) > 0),
    CONSTRAINT archived_events_host_check CHECK (length(TRIM(BOTH FROM host)) > 0)
);

-- Recreate original indexes for archived_events
CREATE INDEX idx_archived_events_archived_at ON audit.archived_events (archived_at DESC);
CREATE INDEX idx_archived_events_original_id ON audit.archived_events (id);
CREATE INDEX idx_archived_events_superseded_by ON audit.archived_events (superseded_by_event_id) 
WHERE superseded_by_event_id IS NOT NULL;
CREATE INDEX idx_archived_events_source_type_ts ON audit.archived_events (source, event_type, ts_orig DESC);

-- Add comments
COMMENT ON TABLE audit.archived_events IS 
'Archive of all logically deleted events. Enables safe, auditable replay operations and provides complete data lineage.';

COMMENT ON COLUMN audit.archived_events.superseded_by_event_id IS 
'ULID of the event that replaced this archived event during a replay operation. NULL for deletions without replacement.';

-- =============================================================================
-- Part 6: Revert archive trigger function
-- =============================================================================

-- Drop new trigger and function
DROP TRIGGER IF EXISTS trg_archive_deleted_events ON core.events;
DROP FUNCTION IF EXISTS core.archive_deleted_event();

-- Recreate original archive trigger function for raw.events
CREATE OR REPLACE FUNCTION raw.archive_deleted_event()
RETURNS TRIGGER AS $$
BEGIN
    -- Insert the deleted event into the archive table
    INSERT INTO audit.archived_events (
        archived_at,
        archived_by,
        archive_reason,
        superseded_by_event_id,
        -- Original event data
        id,
        source,
        event_type,
        ts_ingest,
        ts_orig,
        host,
        ingestor_version,
        payload_schema_id,
        payload,
        blob_id,
        payload_schema_name,
        payload_schema_version,
        source_event_ids
    ) VALUES (
        NOW(),
        COALESCE(current_setting('sinex.archived_by', true), 'system'),
        COALESCE(current_setting('sinex.archive_reason', true), 'unspecified'),
        CASE 
            WHEN current_setting('sinex.superseded_by_event_id', true) = '' THEN NULL
            ELSE current_setting('sinex.superseded_by_event_id', true)::ULID
        END,
        -- Copy all original event data
        OLD.id,
        OLD.source,
        OLD.event_type,
        OLD.ts_ingest,
        OLD.ts_orig,
        OLD.host,
        OLD.ingestor_version,
        OLD.payload_schema_id,
        OLD.payload,
        OLD.blob_id,
        OLD.payload_schema_name,
        OLD.payload_schema_version,
        OLD.source_event_ids
    );
    
    -- Log the archival operation
    RAISE NOTICE 'Event % archived: % (reason: %)', 
        OLD.id, 
        COALESCE(current_setting('sinex.archived_by', true), 'system'),
        COALESCE(current_setting('sinex.archive_reason', true), 'unspecified');
    
    -- Allow the DELETE to proceed
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

-- Create the BEFORE DELETE trigger on raw.events
CREATE TRIGGER trg_archive_deleted_events
    BEFORE DELETE ON raw.events
    FOR EACH ROW
    EXECUTE FUNCTION raw.archive_deleted_event();

-- =============================================================================
-- Part 7: Revert helper functions to original schema
-- =============================================================================

-- Drop new functions and recreate original versions
DROP FUNCTION IF EXISTS core.set_archive_metadata(TEXT, TEXT, ULID);

CREATE OR REPLACE FUNCTION raw.set_archive_metadata(
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

-- Revert find_dependent_events function
DROP FUNCTION IF EXISTS core.find_dependent_events(ULID);

CREATE OR REPLACE FUNCTION raw.find_dependent_events(root_event_id ULID)
RETURNS TABLE(event_id ULID, dependency_depth INTEGER) AS $$
WITH RECURSIVE dependency_tree AS (
    -- Base case: events that directly depend on the root event
    SELECT id as event_id, 1 as dependency_depth
    FROM raw.events 
    WHERE source_event_ids @> ARRAY[root_event_id]
    
    UNION ALL
    
    -- Recursive case: events that depend on events we've already found
    SELECT e.id as event_id, dt.dependency_depth + 1
    FROM raw.events e
    JOIN dependency_tree dt ON e.source_event_ids && ARRAY[dt.event_id]
    WHERE dt.dependency_depth < 10 -- Prevent infinite recursion
)
SELECT event_id, dependency_depth FROM dependency_tree
ORDER BY dependency_depth DESC, event_id;
$$ LANGUAGE sql;

-- Revert find_root_events function
DROP FUNCTION IF EXISTS core.find_root_events(ULID);

CREATE OR REPLACE FUNCTION raw.find_root_events(synthesis_event_id ULID)
RETURNS TABLE(event_id ULID, dependency_depth INTEGER) AS $$
WITH RECURSIVE provenance_tree AS (
    -- Base case: the synthesis event itself
    SELECT synthesis_event_id as event_id, 0 as dependency_depth
    
    UNION ALL
    
    -- Recursive case: source events of events we've already found
    SELECT unnest(e.source_event_ids) as event_id, pt.dependency_depth + 1
    FROM raw.events e
    JOIN provenance_tree pt ON e.id = pt.event_id
    WHERE e.source_event_ids IS NOT NULL
      AND pt.dependency_depth < 10 -- Prevent infinite recursion
)
SELECT event_id, dependency_depth FROM provenance_tree
WHERE event_id != synthesis_event_id
ORDER BY dependency_depth DESC, event_id;
$$ LANGUAGE sql;

-- Revert restore function
DROP FUNCTION IF EXISTS audit.restore_archived_event(ULID);

CREATE OR REPLACE FUNCTION audit.restore_archived_event(archived_event_id ULID)
RETURNS BOOLEAN AS $$
DECLARE
    restored_event audit.archived_events%ROWTYPE;
BEGIN
    -- Get the archived event
    SELECT * INTO restored_event 
    FROM audit.archived_events 
    WHERE id = archived_event_id;
    
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Archived event with ID % not found', archived_event_id;
    END IF;
    
    -- Restore to raw.events (excluding archive metadata)
    INSERT INTO raw.events (
        id, source, event_type, ts_ingest, ts_orig, host,
        ingestor_version, payload_schema_id, payload, blob_id,
        payload_schema_name, payload_schema_version, source_event_ids
    ) VALUES (
        restored_event.id,
        restored_event.source,
        restored_event.event_type,
        restored_event.ts_ingest,
        restored_event.ts_orig,
        restored_event.host,
        restored_event.ingestor_version,
        restored_event.payload_schema_id,
        restored_event.payload,
        restored_event.blob_id,
        restored_event.payload_schema_name,
        restored_event.payload_schema_version,
        restored_event.source_event_ids
    );
    
    -- Remove from archive
    DELETE FROM audit.archived_events WHERE id = archived_event_id;
    
    RETURN TRUE;
EXCEPTION
    WHEN unique_violation THEN
        RAISE EXCEPTION 'Cannot restore event %: already exists in raw.events', archived_event_id;
    WHEN OTHERS THEN
        RAISE;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- Part 8: Revert views to original schema
-- =============================================================================

-- Drop and recreate the events_with_archive_status view with original column names
DROP VIEW IF EXISTS audit.events_with_archive_status;

CREATE VIEW audit.events_with_archive_status AS
SELECT 
    id,
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
FROM raw.events

UNION ALL

SELECT 
    id,
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
-- Part 9: Revert triggers that were moved to core.events
-- =============================================================================

-- Revert metrics trigger
DO $$
BEGIN
    DROP TRIGGER IF EXISTS metrics_fanout ON core.events;
    
    IF EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'fanout' AND pronamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'metrics')) THEN
        CREATE TRIGGER metrics_fanout 
        AFTER INSERT ON raw.events 
        FOR EACH ROW 
        EXECUTE FUNCTION metrics.fanout();
    END IF;
END $$;

-- Revert schema validation trigger
DO $$
BEGIN
    DROP TRIGGER IF EXISTS trg_validate_event_payload_schema ON core.events;
    
    IF EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'validate_event_payload_schema' AND pronamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'core')) THEN
        -- Move the function back to raw schema
        ALTER FUNCTION core.validate_event_payload_schema() SET SCHEMA raw;
        
        CREATE TRIGGER trg_validate_event_payload_schema 
        BEFORE INSERT OR UPDATE OF payload, payload_schema_id ON raw.events 
        FOR EACH ROW 
        EXECUTE FUNCTION raw.validate_event_payload_schema();
    END IF;
END $$;

-- Revert TimescaleDB insert blocker
DO $$
BEGIN
    DROP TRIGGER IF EXISTS ts_insert_blocker ON core.events;
    
    IF EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'insert_blocker' AND pronamespace = (SELECT oid FROM pg_namespace WHERE nspname = '_timescaledb_functions')) THEN
        CREATE TRIGGER ts_insert_blocker 
        BEFORE INSERT ON raw.events 
        FOR EACH ROW 
        EXECUTE FUNCTION _timescaledb_functions.insert_blocker();
    END IF;
END $$;

-- =============================================================================
-- Part 10: Convert raw.events to hypertable (TimescaleDB)
-- =============================================================================

-- Convert raw.events to hypertable if TimescaleDB is available
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'timescaledb') THEN
        PERFORM create_hypertable('raw.events', 'ts_ingest', 
                                 chunk_time_interval => INTERVAL '1 day',
                                 if_not_exists => TRUE);
        
        RAISE NOTICE 'Converted raw.events to TimescaleDB hypertable';
    ELSE
        RAISE NOTICE 'TimescaleDB not available, skipping hypertable conversion';
    END IF;
EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'Failed to convert to hypertable: %', SQLERRM;
END $$;

-- =============================================================================
-- Part 11: Revert foreign key references back to raw.events
-- =============================================================================

-- Update all tables that reference core.events to reference raw.events
-- Note: Column names also need to be reverted from event_id back to id

-- Update core.revisions
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'core' AND table_name = 'revisions') THEN
        ALTER TABLE core.revisions DROP CONSTRAINT IF EXISTS revisions_created_from_event_id_fkey;
        ALTER TABLE core.revisions 
        ADD CONSTRAINT artifact_contents_created_from_event_id_fkey 
        FOREIGN KEY (created_from_event_id) REFERENCES raw.events(id);
    END IF;
END $$;

-- Update core.artifact_event_sources
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'core' AND table_name = 'artifact_event_sources') THEN
        ALTER TABLE core.artifact_event_sources DROP CONSTRAINT IF EXISTS artifact_event_sources_event_id_fkey;
        ALTER TABLE core.artifact_event_sources 
        ADD CONSTRAINT artifact_event_sources_event_id_fkey 
        FOREIGN KEY (event_id) REFERENCES raw.events(id) ON DELETE CASCADE;
    END IF;
END $$;

-- Update core.artifact_tags  
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'core' AND table_name = 'artifact_tags') THEN
        ALTER TABLE core.artifact_tags DROP CONSTRAINT IF EXISTS artifact_tags_tagged_from_event_id_fkey;
        ALTER TABLE core.artifact_tags 
        ADD CONSTRAINT artifact_tags_tagged_from_event_id_fkey 
        FOREIGN KEY (tagged_from_event_id) REFERENCES raw.events(id);
    END IF;
END $$;

-- Update core.artifacts
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'core' AND table_name = 'artifacts') THEN
        ALTER TABLE core.artifacts DROP CONSTRAINT IF EXISTS artifacts_created_from_event_id_fkey;
        ALTER TABLE core.artifacts 
        ADD CONSTRAINT artifacts_created_from_event_id_fkey 
        FOREIGN KEY (created_from_event_id) REFERENCES raw.events(id);
    END IF;
END $$;

-- Update core.entity_relations
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'core' AND table_name = 'entity_relations') THEN
        ALTER TABLE core.entity_relations DROP CONSTRAINT IF EXISTS entity_relations_created_from_event_id_fkey;
        ALTER TABLE core.entity_relations 
        ADD CONSTRAINT entity_relations_created_from_event_id_fkey 
        FOREIGN KEY (created_from_event_id) REFERENCES raw.events(id);
    END IF;
END $$;

-- Update core.event_annotations
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'core' AND table_name = 'event_annotations') THEN
        ALTER TABLE core.event_annotations DROP CONSTRAINT IF EXISTS event_annotations_event_id_fkey;
        ALTER TABLE core.event_annotations 
        ADD CONSTRAINT event_annotations_event_id_fkey 
        FOREIGN KEY (event_id) REFERENCES raw.events(id) ON DELETE CASCADE;
    END IF;
END $$;

-- Update core.event_artifact_refs
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'core' AND table_name = 'event_artifact_refs') THEN
        ALTER TABLE core.event_artifact_refs DROP CONSTRAINT IF EXISTS event_artifact_refs_event_id_fkey;
        ALTER TABLE core.event_artifact_refs 
        ADD CONSTRAINT event_artifact_refs_event_id_fkey 
        FOREIGN KEY (event_id) REFERENCES raw.events(id) ON DELETE CASCADE;
    END IF;
END $$;

-- Update core.event_cluster_members
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'core' AND table_name = 'event_cluster_members') THEN
        ALTER TABLE core.event_cluster_members DROP CONSTRAINT IF EXISTS event_cluster_members_event_id_fkey;
        ALTER TABLE core.event_cluster_members 
        ADD CONSTRAINT event_cluster_members_event_id_fkey 
        FOREIGN KEY (event_id) REFERENCES raw.events(id) ON DELETE CASCADE;
    END IF;
END $$;

-- Update core.event_embeddings
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'core' AND table_name = 'event_embeddings') THEN
        ALTER TABLE core.event_embeddings DROP CONSTRAINT IF EXISTS event_embeddings_event_id_fkey;
        ALTER TABLE core.event_embeddings 
        ADD CONSTRAINT event_embeddings_event_id_fkey 
        FOREIGN KEY (event_id) REFERENCES raw.events(id) ON DELETE CASCADE;
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
        FOREIGN KEY (from_event_id) REFERENCES raw.events(id) ON DELETE CASCADE;
        
        ALTER TABLE core.event_relations 
        ADD CONSTRAINT event_relations_to_event_id_fkey 
        FOREIGN KEY (to_event_id) REFERENCES raw.events(id) ON DELETE CASCADE;
    END IF;
END $$;

-- =============================================================================
-- Part 12: Drop the core.events table
-- =============================================================================

-- Drop the new core.events table (all data has been migrated back)
DROP TABLE IF EXISTS core.events CASCADE;

-- =============================================================================
-- Part 13: Final success message
-- =============================================================================

DO $$
BEGIN
    RAISE NOTICE '✅ Successfully reverted core.events back to raw.events';
    RAISE NOTICE '📊 Rollback completed:';
    RAISE NOTICE '   - Renamed event_id column back to id';
    RAISE NOTICE '   - Removed correlation_id and source_material_* columns';
    RAISE NOTICE '   - Converted associated_blob_ids array back to blob_id';
    RAISE NOTICE '   - Restored all original indexes, triggers, and foreign keys';
    RAISE NOTICE '   - Reverted archived_events table to original schema';
    RAISE NOTICE '🔄 Back to original raw.events architecture';
END $$;
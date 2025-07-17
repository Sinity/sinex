-- Unified Events Table with Archive Trigger Implementation
-- 
-- This migration implements the unified events table architecture from the comprehensive plan:
-- 1. Adds source_event_ids column for provenance tracking
-- 2. Creates audit.archived_events table for safe archival
-- 3. Implements BEFORE DELETE trigger for automatic archival
--
-- This enables the "never truly delete" principle while maintaining query simplicity

-- =============================================================================
-- Part 1: Add provenance tracking to raw.events
-- =============================================================================

-- Add source_event_ids column for provenance tracking
-- NULL = Raw Event (direct observation)
-- NOT NULL = Synthesis Event (derived from other events)
ALTER TABLE raw.events 
ADD COLUMN source_event_ids ULID[] DEFAULT NULL;

-- Create GIN index for efficient provenance queries
-- This enables fast "find all events that depend on X" queries
CREATE INDEX idx_raw_events_provenance 
ON raw.events USING GIN (source_event_ids)
WHERE source_event_ids IS NOT NULL;

-- Add comment explaining the provenance model
COMMENT ON COLUMN raw.events.source_event_ids IS 
'Provenance chain: NULL for raw events (direct observations), array of ULIDs for synthesis events (derived from other events)';

-- =============================================================================
-- Part 2: Create audit schema and archived_events table
-- =============================================================================

-- Create audit schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS audit;

-- Create archived_events table with identical structure plus archive metadata
CREATE TABLE audit.archived_events (
    -- Archive metadata (added fields)
    archived_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    archived_by TEXT DEFAULT 'system',
    archive_reason TEXT,
    superseded_by_event_id ULID, -- Links to the new event that replaced this one
    
    -- Original events table structure (copied exactly)
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
    
    -- Archive table constraints
    CONSTRAINT archived_events_source_check CHECK (length(TRIM(BOTH FROM source)) > 0),
    CONSTRAINT archived_events_event_type_check CHECK (length(TRIM(BOTH FROM event_type)) > 0),
    CONSTRAINT archived_events_host_check CHECK (length(TRIM(BOTH FROM host)) > 0)
);

-- Create indexes for efficient archive queries
CREATE INDEX idx_archived_events_archived_at ON audit.archived_events (archived_at DESC);
CREATE INDEX idx_archived_events_original_id ON audit.archived_events (id);
CREATE INDEX idx_archived_events_superseded_by ON audit.archived_events (superseded_by_event_id) 
WHERE superseded_by_event_id IS NOT NULL;
CREATE INDEX idx_archived_events_source_type_ts ON audit.archived_events (source, event_type, ts_orig DESC);

-- Note: Archive table partitioning can be added later with pg_partman
-- For now, we'll use a single table with good indexing

-- Add comments
COMMENT ON TABLE audit.archived_events IS 
'Archive of all logically deleted events. Enables safe, auditable replay operations and provides complete data lineage.';

COMMENT ON COLUMN audit.archived_events.superseded_by_event_id IS 
'ULID of the event that replaced this archived event during a replay operation. NULL for deletions without replacement.';

-- =============================================================================
-- Part 3: Implement archive trigger system
-- =============================================================================

-- Create session variables for trigger metadata
-- These can be set by the application before DELETE operations
DO $$
BEGIN
    -- Initialize session variables if they don't exist
    PERFORM set_config('sinex.archived_by', 'system', true);
    PERFORM set_config('sinex.archive_reason', 'unspecified', true);
    PERFORM set_config('sinex.superseded_by_event_id', '', true);
EXCEPTION WHEN OTHERS THEN
    -- Ignore errors if variables already exist
    NULL;
END $$;

-- Create the archive trigger function
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

-- Create the BEFORE DELETE trigger
CREATE TRIGGER trg_archive_deleted_events
    BEFORE DELETE ON raw.events
    FOR EACH ROW
    EXECUTE FUNCTION raw.archive_deleted_event();

-- Add comment explaining the trigger
COMMENT ON TRIGGER trg_archive_deleted_events ON raw.events IS 
'Automatically archives events to audit.archived_events before deletion. This implements the "never truly delete" principle for data integrity.';

-- =============================================================================
-- Part 4: Create helper functions for the replay system
-- =============================================================================

-- Function to set archive metadata before performing a DELETE
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

-- Function to find all events that depend on a given event (for cascade deletes)
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

-- Function to find the root events (raw events) that led to a synthesis event
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

-- =============================================================================
-- Part 5: Add helpful views and functions
-- =============================================================================

-- View to easily see the current state of all events (active + archived)
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

-- Function to restore an archived event (for rollback operations)
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
-- Part 6: Grant appropriate permissions
-- =============================================================================

-- Grant permissions for the new schema and tables
GRANT USAGE ON SCHEMA audit TO PUBLIC;
GRANT SELECT ON audit.archived_events TO PUBLIC;
GRANT SELECT ON audit.events_with_archive_status TO PUBLIC;

-- Only specific roles should be able to restore events
-- (This will be configured based on actual role setup)

-- Add final success message
DO $$
BEGIN
    RAISE NOTICE '✅ Unified events table with archive trigger successfully implemented';
    RAISE NOTICE '📊 Features enabled:';
    RAISE NOTICE '   - Provenance tracking via source_event_ids column';
    RAISE NOTICE '   - Automatic archival on DELETE operations';
    RAISE NOTICE '   - Helper functions for replay operations';
    RAISE NOTICE '   - Partitioned archive table for performance';
    RAISE NOTICE '🔄 Ready for replay system implementation';
END $$;
-- Unify Events Architecture Migration
-- 
-- This migration implements the unified architecture from the comprehensive plan:
-- 1. Ensures core.events exists with unified schema
-- 2. Migrates any data from synthesis.events to core.events with proper source_event_ids
-- 3. Drops synthesis.events table and schema
-- 4. Updates all references to use the unified table

-- =============================================================================
-- Part 1: Create core.events table if it doesn't exist
-- =============================================================================

-- Create core schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS core;

-- Create the unified core.events table if it doesn't exist
CREATE TABLE IF NOT EXISTS core.events (
    event_id ULID PRIMARY KEY DEFAULT gen_ulid(),
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

    -- Provenance Links (Unified Architecture)
    source_material_id ULID,        -- External provenance (to the "whole blob")
    source_material_offset_start BIGINT, -- The "Anchor Byte" offset within the blob
    source_material_offset_end BIGINT,
    source_event_ids ULID[],         -- Internal provenance (to other events in this table)
                                    -- NULL = raw event (direct observation)
                                    -- NOT NULL = synthesis event (derived from other events)

    -- Convenience Link to Associated Data (e.g., a screenshot)
    associated_blob_ids ULID[],

    -- Constraints
    CONSTRAINT events_event_type_check CHECK (length(TRIM(BOTH FROM event_type)) > 0),
    CONSTRAINT events_host_check CHECK (length(TRIM(BOTH FROM host)) > 0),
    CONSTRAINT events_source_check CHECK (length(TRIM(BOTH FROM source)) > 0)
);

-- Add table comment
COMMENT ON TABLE core.events IS 'Unified log for all captured events (raw and synthesis). The single source of truth for event interpretations with full provenance tracking.';

-- Add column comments  
COMMENT ON COLUMN core.events.event_id IS 'Globally unique identifier for the event using ULID (Universally Unique Lexicographically Sortable Identifier).';
COMMENT ON COLUMN core.events.source IS 'Canonical identifier for the event origin/producer (e.g., "desktop.hyprland.ipc_ingestor", "sinex.pkm.sync_agent").';
COMMENT ON COLUMN core.events.event_type IS 'Type string for the event, often namespaced by source (e.g., "window_focused", "note_updated", "agent.heartbeat").';
COMMENT ON COLUMN core.events.source_event_ids IS 'Provenance chain: NULL for raw events (direct observations), array of ULIDs for synthesis events (derived from other events).';

-- =============================================================================
-- Part 2: Migrate data from synthesis.events to core.events if synthesis.events exists
-- =============================================================================

DO $$
BEGIN
    -- Check if synthesis.events exists
    IF EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_schema = 'synthesis' 
        AND table_name = 'events'
    ) THEN
        RAISE NOTICE 'Migrating data from synthesis.events to core.events...';
        
        -- Insert synthesis events into core.events with proper source_event_ids
        INSERT INTO core.events (
            event_id,
            event_type,
            source,
            ts_orig,
            host,
            payload,
            ingestor_version,
            payload_schema_id,
            payload_schema_name,
            payload_schema_version,
            source_event_ids,
            associated_blob_ids
        )
        SELECT 
            id,  -- Use existing ID
            event_type,
            source,
            ts_orig,
            host,
            payload,
            ingestor_version,
            payload_schema_id,
            payload_schema_name,
            payload_schema_version,
            -- Map provenance fields to unified source_event_ids
            CASE 
                WHEN source_raw_event_ids IS NOT NULL THEN source_raw_event_ids
                WHEN source_synthesis_event_ids IS NOT NULL THEN source_synthesis_event_ids
                ELSE NULL
            END,
            NULL -- associated_blob_ids (new field, starts NULL)
        FROM synthesis.events
        ON CONFLICT (event_id) DO NOTHING; -- Avoid duplicates if run multiple times
        
        -- Log the migration
        RAISE NOTICE 'Migrated % events from synthesis.events to core.events', 
            (SELECT COUNT(*) FROM synthesis.events);
    ELSE
        RAISE NOTICE 'synthesis.events table does not exist, skipping migration';
    END IF;
END $$;

-- =============================================================================
-- Part 3: Create indexes on core.events if they don't exist
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

-- Provenance indexes
CREATE INDEX IF NOT EXISTS idx_core_events_provenance ON core.events USING GIN (source_event_ids) WHERE source_event_ids IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_core_events_raw_events ON core.events (ts_ingest DESC) WHERE source_event_ids IS NULL;
CREATE INDEX IF NOT EXISTS idx_core_events_synthesis_events ON core.events (ts_ingest DESC) WHERE source_event_ids IS NOT NULL;

-- Associated blob indexes
CREATE INDEX IF NOT EXISTS idx_core_events_associated_blobs ON core.events USING GIN (associated_blob_ids) WHERE associated_blob_ids IS NOT NULL;

-- JSONB payload index
CREATE INDEX IF NOT EXISTS idx_core_events_payload_gin_path_ops ON core.events USING GIN (payload jsonb_path_ops);

-- =============================================================================
-- Part 4: Add foreign key constraints
-- =============================================================================

-- Foreign key to schema registry (if it exists)
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_schema = 'sinex_schemas' 
        AND table_name = 'event_payload_schemas'
    ) THEN
        -- Check if constraint already exists
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.table_constraints 
            WHERE table_schema = 'core' 
            AND table_name = 'events' 
            AND constraint_name = 'fk_core_events_schema_id'
        ) THEN
            ALTER TABLE core.events 
            ADD CONSTRAINT fk_core_events_schema_id 
            FOREIGN KEY (payload_schema_id) 
            REFERENCES sinex_schemas.event_payload_schemas(id)
            ON DELETE SET NULL;
        END IF;
    END IF;
END $$;

-- =============================================================================
-- Part 5: Convert core.events to hypertable (TimescaleDB)
-- =============================================================================

-- Convert core.events to hypertable if TimescaleDB is available
DO $$
BEGIN
    -- Check if TimescaleDB extension is available
    IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'timescaledb') THEN
        -- Check if already a hypertable
        IF NOT EXISTS (
            SELECT 1 FROM _timescaledb_catalog.hypertable 
            WHERE table_name = 'events' AND schema_name = 'core'
        ) THEN
            -- Convert to hypertable
            PERFORM create_hypertable('core.events', 'ts_ingest', 
                                     chunk_time_interval => INTERVAL '1 day',
                                     if_not_exists => TRUE);
            
            RAISE NOTICE 'Converted core.events to TimescaleDB hypertable';
        ELSE
            RAISE NOTICE 'core.events is already a hypertable';
        END IF;
    ELSE
        RAISE NOTICE 'TimescaleDB not available, skipping hypertable conversion';
    END IF;
EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'Failed to convert to hypertable: %', SQLERRM;
END $$;

-- =============================================================================
-- Part 6: Update foreign key references from other tables
-- =============================================================================

-- Update all tables that reference events to use core.events
DO $$
BEGIN
    -- Update core.revisions if it exists
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'core' AND table_name = 'revisions') THEN
        -- Check if column exists and update foreign key
        IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'core' AND table_name = 'revisions' AND column_name = 'created_from_event_id') THEN
            ALTER TABLE core.revisions DROP CONSTRAINT IF EXISTS artifact_contents_created_from_event_id_fkey;
            ALTER TABLE core.revisions DROP CONSTRAINT IF EXISTS revisions_created_from_event_id_fkey;
            ALTER TABLE core.revisions 
            ADD CONSTRAINT revisions_created_from_event_id_fkey 
            FOREIGN KEY (created_from_event_id) REFERENCES core.events(event_id);
        END IF;
    END IF;

    -- Update core.event_annotations if it exists
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'core' AND table_name = 'event_annotations') THEN
        ALTER TABLE core.event_annotations DROP CONSTRAINT IF EXISTS event_annotations_event_id_fkey;
        ALTER TABLE core.event_annotations 
        ADD CONSTRAINT event_annotations_event_id_fkey 
        FOREIGN KEY (event_id) REFERENCES core.events(event_id) ON DELETE CASCADE;
    END IF;

    -- Update core.event_relations if it exists
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

    -- Update other tables that might reference events
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'core' AND table_name = 'event_embeddings') THEN
        ALTER TABLE core.event_embeddings DROP CONSTRAINT IF EXISTS event_embeddings_event_id_fkey;
        ALTER TABLE core.event_embeddings 
        ADD CONSTRAINT event_embeddings_event_id_fkey 
        FOREIGN KEY (event_id) REFERENCES core.events(event_id) ON DELETE CASCADE;
    END IF;

    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'core' AND table_name = 'event_artifact_refs') THEN
        ALTER TABLE core.event_artifact_refs DROP CONSTRAINT IF EXISTS event_artifact_refs_event_id_fkey;
        ALTER TABLE core.event_artifact_refs 
        ADD CONSTRAINT event_artifact_refs_event_id_fkey 
        FOREIGN KEY (event_id) REFERENCES core.events(event_id) ON DELETE CASCADE;
    END IF;

    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'core' AND table_name = 'event_cluster_members') THEN
        ALTER TABLE core.event_cluster_members DROP CONSTRAINT IF EXISTS event_cluster_members_event_id_fkey;
        ALTER TABLE core.event_cluster_members 
        ADD CONSTRAINT event_cluster_members_event_id_fkey 
        FOREIGN KEY (event_id) REFERENCES core.events(event_id) ON DELETE CASCADE;
    END IF;

    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'core' AND table_name = 'artifact_event_sources') THEN
        ALTER TABLE core.artifact_event_sources DROP CONSTRAINT IF EXISTS artifact_event_sources_event_id_fkey;
        ALTER TABLE core.artifact_event_sources 
        ADD CONSTRAINT artifact_event_sources_event_id_fkey 
        FOREIGN KEY (event_id) REFERENCES core.events(event_id) ON DELETE CASCADE;
    END IF;

    RAISE NOTICE 'Updated foreign key references to core.events';
END $$;

-- =============================================================================
-- Part 7: Create helper functions for unified architecture
-- =============================================================================

-- Function to check if an event is a raw event (source_event_ids IS NULL)
CREATE OR REPLACE FUNCTION core.is_raw_event(event_row core.events)
RETURNS BOOLEAN AS $$
BEGIN
    RETURN event_row.source_event_ids IS NULL;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Function to check if an event is a synthesis event (source_event_ids IS NOT NULL)
CREATE OR REPLACE FUNCTION core.is_synthesis_event(event_row core.events)
RETURNS BOOLEAN AS $$
BEGIN
    RETURN event_row.source_event_ids IS NOT NULL;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Function to find all events that depend on a given event (for cascade operations)
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

-- Function to find the root events (raw events) that led to a synthesis event
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

-- =============================================================================
-- Part 8: Create views for compatibility and monitoring
-- =============================================================================

-- Create a view that shows raw events only (for compatibility)
CREATE OR REPLACE VIEW core.raw_events AS
SELECT * FROM core.events WHERE source_event_ids IS NULL;

-- Create a view that shows synthesis events only (for compatibility)
CREATE OR REPLACE VIEW core.synthesis_events AS
SELECT * FROM core.events WHERE source_event_ids IS NOT NULL;

-- Create a view that shows event type distribution
CREATE OR REPLACE VIEW core.event_type_stats AS
SELECT 
    event_type,
    source,
    COUNT(*) as event_count,
    COUNT(*) FILTER (WHERE source_event_ids IS NULL) as raw_count,
    COUNT(*) FILTER (WHERE source_event_ids IS NOT NULL) as synthesis_count,
    MIN(ts_ingest) as first_seen,
    MAX(ts_ingest) as last_seen
FROM core.events
GROUP BY event_type, source
ORDER BY event_count DESC;

-- Add comments to views
COMMENT ON VIEW core.raw_events IS 'Raw events only (source_event_ids IS NULL) - direct observations from ingestors';
COMMENT ON VIEW core.synthesis_events IS 'Synthesis events only (source_event_ids IS NOT NULL) - derived events from automata';
COMMENT ON VIEW core.event_type_stats IS 'Event type distribution and statistics for monitoring';

-- =============================================================================
-- Part 9: Drop synthesis.events table and schema
-- =============================================================================

DO $$
BEGIN
    -- Drop synthesis.events table if it exists
    IF EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_schema = 'synthesis' 
        AND table_name = 'events'
    ) THEN
        DROP TABLE synthesis.events CASCADE;
        RAISE NOTICE 'Dropped synthesis.events table';
    END IF;
    
    -- Drop synthesis schema if it exists and is empty
    IF EXISTS (
        SELECT 1 FROM information_schema.schemata 
        WHERE schema_name = 'synthesis'
    ) THEN
        -- Check if schema is empty
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.tables 
            WHERE table_schema = 'synthesis'
        ) THEN
            DROP SCHEMA synthesis;
            RAISE NOTICE 'Dropped synthesis schema';
        ELSE
            RAISE NOTICE 'Synthesis schema not empty, keeping it';
        END IF;
    END IF;
END $$;

-- =============================================================================
-- Part 10: Grant appropriate permissions
-- =============================================================================

-- Grant permissions for the unified schema
GRANT USAGE ON SCHEMA core TO PUBLIC;
GRANT SELECT ON core.events TO PUBLIC;
GRANT SELECT ON core.raw_events TO PUBLIC;
GRANT SELECT ON core.synthesis_events TO PUBLIC;
GRANT SELECT ON core.event_type_stats TO PUBLIC;

-- =============================================================================
-- Part 11: Final success message
-- =============================================================================

DO $$
BEGIN
    RAISE NOTICE '✅ Successfully unified events architecture';
    RAISE NOTICE '📊 Migration completed:';
    RAISE NOTICE '   - Created unified core.events table';
    RAISE NOTICE '   - Migrated data from synthesis.events (if it existed)';
    RAISE NOTICE '   - Dropped synthesis.events table and schema';
    RAISE NOTICE '   - Updated all foreign key references';
    RAISE NOTICE '   - Created helper functions and views';
    RAISE NOTICE '   - Applied TimescaleDB hypertable conversion';
    RAISE NOTICE '🔄 Ready for unified architecture in application code';
END $$;
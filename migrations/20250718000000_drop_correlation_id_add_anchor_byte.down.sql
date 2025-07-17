-- Rollback: Re-add correlation_id and remove anchor_byte column
--
-- This migration rolls back the changes from the up migration:
-- 1. Re-adds correlation_id column
-- 2. Removes anchor_byte column  
-- 3. Restores original unique constraint using source_material_offset_start

BEGIN;

-- =============================================================================
-- Part 1: Drop updated views
-- =============================================================================

-- Drop views that need to be recreated with correlation_id
DROP VIEW IF EXISTS core.raw_events;
DROP VIEW IF EXISTS core.synthesis_events;

-- =============================================================================
-- Part 2: Drop new unique constraint
-- =============================================================================

-- Drop the new constraint that uses anchor_byte
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.table_constraints 
        WHERE table_schema = 'core' 
        AND table_name = 'events' 
        AND constraint_name = 'unique_raw_event_origin_anchor'
    ) THEN
        ALTER TABLE core.events DROP CONSTRAINT unique_raw_event_origin_anchor;
        RAISE NOTICE 'Dropped unique_raw_event_origin_anchor constraint';
    ELSE
        RAISE NOTICE 'unique_raw_event_origin_anchor constraint does not exist';
    END IF;
END $$;

-- =============================================================================
-- Part 2: Drop anchor_byte index
-- =============================================================================

-- Drop the index on anchor_byte
DROP INDEX IF EXISTS idx_core_events_source_material_anchor;

-- =============================================================================
-- Part 3: Re-add correlation_id column
-- =============================================================================

-- Re-add correlation_id column if it doesn't exist
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_schema = 'core' 
        AND table_name = 'events' 
        AND column_name = 'correlation_id'
    ) THEN
        ALTER TABLE core.events ADD COLUMN correlation_id ULID;
        RAISE NOTICE 'Re-added correlation_id column to core.events';
    ELSE
        RAISE NOTICE 'correlation_id column already exists in core.events';
    END IF;
END $$;

-- =============================================================================
-- Part 4: Drop anchor_byte column
-- =============================================================================

-- Drop anchor_byte column if it exists
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_schema = 'core' 
        AND table_name = 'events' 
        AND column_name = 'anchor_byte'
    ) THEN
        ALTER TABLE core.events DROP COLUMN anchor_byte;
        RAISE NOTICE 'Dropped anchor_byte column from core.events';
    ELSE
        RAISE NOTICE 'anchor_byte column does not exist in core.events';
    END IF;
END $$;

-- =============================================================================
-- Part 5: Restore original unique constraint
-- =============================================================================

-- Restore the original unique constraint using source_material_offset_start
DO $$
BEGIN
    -- Only create constraint if it doesn't exist
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints 
        WHERE table_schema = 'core' 
        AND table_name = 'events' 
        AND constraint_name = 'unique_raw_event_origin'
    ) THEN
        ALTER TABLE core.events 
        ADD CONSTRAINT unique_raw_event_origin 
        UNIQUE (source_material_id, source_material_offset_start);
        RAISE NOTICE 'Restored unique_raw_event_origin constraint using source_material_offset_start';
    ELSE
        RAISE NOTICE 'unique_raw_event_origin constraint already exists';
    END IF;
END $$;

-- =============================================================================
-- Part 6: Recreate views with correlation_id column
-- =============================================================================

-- Recreate raw_events view with correlation_id
CREATE VIEW core.raw_events AS
SELECT 
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
FROM core.events WHERE source_event_ids IS NULL;

-- Recreate synthesis_events view with correlation_id
CREATE VIEW core.synthesis_events AS
SELECT 
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
FROM core.events WHERE source_event_ids IS NOT NULL;

-- Add comments to restored views
COMMENT ON VIEW core.raw_events IS 'Raw events only (source_event_ids IS NULL) - direct observations from ingestors';
COMMENT ON VIEW core.synthesis_events IS 'Synthesis events only (source_event_ids IS NOT NULL) - derived events from automata';

-- =============================================================================
-- Part 7: Final rollback message
-- =============================================================================

DO $$
BEGIN
    RAISE NOTICE '✅ Successfully rolled back core.events schema changes';
    RAISE NOTICE '📊 Rollback completed:';
    RAISE NOTICE '   - Re-added correlation_id column';
    RAISE NOTICE '   - Removed anchor_byte column';
    RAISE NOTICE '   - Restored original unique constraint using source_material_offset_start';
    RAISE NOTICE '   - Recreated views with correlation_id column';
    RAISE NOTICE '🔄 Reverted to previous schema state';
END $$;

COMMIT;
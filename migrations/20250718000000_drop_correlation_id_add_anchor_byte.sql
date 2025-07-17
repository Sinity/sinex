-- Drop correlation_id and add anchor_byte column
--
-- This migration implements the plan.md requirements:
-- 1. Drops correlation_id column as provenance removes the need for this mechanism
-- 2. Adds anchor_byte column separate from source_material_offset_start
-- 3. Updates unique constraint to use anchor_byte instead of source_material_offset_start

BEGIN;

-- =============================================================================
-- Part 1: Drop dependent views before dropping correlation_id column
-- =============================================================================

-- Drop views that depend on correlation_id column
DROP VIEW IF EXISTS core.raw_events;
DROP VIEW IF EXISTS core.synthesis_events;

-- =============================================================================
-- Part 2: Drop correlation_id column
-- =============================================================================

-- Check if correlation_id column exists and drop it
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_schema = 'core' 
        AND table_name = 'events' 
        AND column_name = 'correlation_id'
    ) THEN
        ALTER TABLE core.events DROP COLUMN correlation_id;
        RAISE NOTICE 'Dropped correlation_id column from core.events';
    ELSE
        RAISE NOTICE 'correlation_id column does not exist in core.events';
    END IF;
END $$;

-- =============================================================================
-- Part 2: Add anchor_byte column
-- =============================================================================

-- Add anchor_byte column if it doesn't exist
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_schema = 'core' 
        AND table_name = 'events' 
        AND column_name = 'anchor_byte'
    ) THEN
        ALTER TABLE core.events ADD COLUMN anchor_byte BIGINT;
        RAISE NOTICE 'Added anchor_byte column to core.events';
    ELSE
        RAISE NOTICE 'anchor_byte column already exists in core.events';
    END IF;
END $$;

-- =============================================================================
-- Part 3: Populate anchor_byte with existing offset_start values
-- =============================================================================

-- Initially set anchor_byte to the same value as source_material_offset_start
-- This ensures existing data has proper anchor_byte values
DO $$
DECLARE
    rows_updated INTEGER;
BEGIN
    -- Update anchor_byte to match source_material_offset_start for existing records
    UPDATE core.events 
    SET anchor_byte = source_material_offset_start 
    WHERE source_material_offset_start IS NOT NULL 
    AND anchor_byte IS NULL;
    
    -- Get count of updated records
    GET DIAGNOSTICS rows_updated = ROW_COUNT;
    
    RAISE NOTICE 'Updated % events with anchor_byte from source_material_offset_start', rows_updated;
END $$;

-- =============================================================================
-- Part 4: Update unique constraint to use anchor_byte
-- =============================================================================

-- Drop existing unique constraint that uses source_material_offset_start
DO $$
BEGIN
    -- Check if the constraint exists first
    IF EXISTS (
        SELECT 1 FROM information_schema.table_constraints 
        WHERE table_schema = 'core' 
        AND table_name = 'events' 
        AND constraint_name = 'unique_raw_event_origin'
    ) THEN
        ALTER TABLE core.events DROP CONSTRAINT unique_raw_event_origin;
        RAISE NOTICE 'Dropped existing unique_raw_event_origin constraint';
    ELSE
        RAISE NOTICE 'unique_raw_event_origin constraint does not exist';
    END IF;
END $$;

-- Create new unique constraint using anchor_byte instead of source_material_offset_start
DO $$
BEGIN
    -- Only create constraint if it doesn't exist
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints 
        WHERE table_schema = 'core' 
        AND table_name = 'events' 
        AND constraint_name = 'unique_raw_event_origin_anchor'
    ) THEN
        ALTER TABLE core.events 
        ADD CONSTRAINT unique_raw_event_origin_anchor 
        UNIQUE (source_material_id, anchor_byte);
        RAISE NOTICE 'Created unique_raw_event_origin_anchor constraint using anchor_byte';
    ELSE
        RAISE NOTICE 'unique_raw_event_origin_anchor constraint already exists';
    END IF;
END $$;

-- =============================================================================
-- Part 5: Add column comment for anchor_byte
-- =============================================================================

COMMENT ON COLUMN core.events.anchor_byte IS 'Immutable anchor byte offset within source material. Unlike source_material_offset_start, this value never changes even if offset_start is updated. Used in natural key constraint for deterministic raw event identity.';

-- =============================================================================
-- Part 6: Create index on anchor_byte for performance
-- =============================================================================

-- Create index on the new constraint columns for performance
CREATE INDEX IF NOT EXISTS idx_core_events_source_material_anchor 
ON core.events (source_material_id, anchor_byte) 
WHERE source_material_id IS NOT NULL AND anchor_byte IS NOT NULL;

-- =============================================================================
-- Part 7: Recreate views without correlation_id column
-- =============================================================================

-- Recreate raw_events view without correlation_id
CREATE VIEW core.raw_events AS
SELECT 
    event_id,
    ts_ingest,
    event_type,
    source,
    ts_orig,
    host,
    payload,
    ingestor_version,
    payload_schema_id,
    payload_schema_name,
    payload_schema_version,
    source_material_id,
    source_material_offset_start,
    source_material_offset_end,
    anchor_byte,
    source_event_ids,
    associated_blob_ids
FROM core.events WHERE source_event_ids IS NULL;

-- Recreate synthesis_events view without correlation_id
CREATE VIEW core.synthesis_events AS
SELECT 
    event_id,
    ts_ingest,
    event_type,
    source,
    ts_orig,
    host,
    payload,
    ingestor_version,
    payload_schema_id,
    payload_schema_name,
    payload_schema_version,
    source_material_id,
    source_material_offset_start,
    source_material_offset_end,
    anchor_byte,
    source_event_ids,
    associated_blob_ids
FROM core.events WHERE source_event_ids IS NOT NULL;

-- Add comments to updated views
COMMENT ON VIEW core.raw_events IS 'Raw events only (source_event_ids IS NULL) - direct observations from ingestors. Updated to exclude correlation_id.';
COMMENT ON VIEW core.synthesis_events IS 'Synthesis events only (source_event_ids IS NOT NULL) - derived events from automata. Updated to exclude correlation_id.';

-- =============================================================================
-- Part 8: Final success message
-- =============================================================================

DO $$
BEGIN
    RAISE NOTICE '✅ Successfully updated core.events schema';
    RAISE NOTICE '📊 Migration completed:';
    RAISE NOTICE '   - Dropped correlation_id column (provenance removes need for this)';
    RAISE NOTICE '   - Added anchor_byte column (immutable anchor for raw events)';
    RAISE NOTICE '   - Updated unique constraint to use anchor_byte instead of offset_start';
    RAISE NOTICE '   - Created performance index on (source_material_id, anchor_byte)';
    RAISE NOTICE '   - Recreated views without correlation_id column';
    RAISE NOTICE '🔄 Ready for updated application code to use anchor_byte';
END $$;

COMMIT;
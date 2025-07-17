-- Down Migration: Remove Source Material Registry Table
--
-- This migration reverses the creation of raw.source_material_registry table
-- and all associated indexes, functions, and constraints.

-- =============================================================================
-- Part 1: Drop helper functions
-- =============================================================================

DROP FUNCTION IF EXISTS raw.get_staging_statistics();
DROP FUNCTION IF EXISTS raw.find_source_material(TEXT, TEXT[], TEXT, INTEGER);

-- =============================================================================
-- Part 2: Drop the source material registry table
-- =============================================================================

-- This will automatically drop all indexes and constraints
DROP TABLE IF EXISTS raw.source_material_registry CASCADE;

-- =============================================================================
-- Part 3: Success notification
-- =============================================================================

DO $$
BEGIN
    RAISE NOTICE '🔄 Source Material Registry table and related objects removed';
    RAISE NOTICE '⚠️  All staged source material metadata has been permanently deleted';
END $$;
-- Create Source Material Registry Table
-- 
-- This migration implements the raw.source_material_registry table from plan.md,
-- which serves as the foundation for the unified architecture. This table acts as
-- the "data inbox & birth certificate" for all external source material that
-- enters the Sinex system.
--
-- The table captures the complete provenance of source material:
-- - User-provided context (the "human story")
-- - System ingestion metadata (the "system story") 
-- - Content-derived metadata (the "inferred story")
--
-- This enables the system to be fully auditable, reversible, and intelligible
-- by maintaining perfect knowledge of where data came from and why it was captured.

-- =============================================================================
-- Part 1: Create the source material registry table
-- =============================================================================

CREATE TABLE raw.source_material_registry (
    -- Core Identity & Deduplication
    blob_id ULID PRIMARY KEY,
    checksum TEXT NOT NULL UNIQUE,      -- blake3 hash of the content to prevent duplicate staging
    stage_batch_id UUID NOT NULL,       -- Groups files staged in a single `exo` command invocation

    -- User-Provided Context (The "Human Story")
    source_identifier TEXT NOT NULL,    -- User-defined name, e.g., 'old-laptop-bash', 'live-kitty-stream'
    user_comment TEXT,                  -- Free-text description from the user
    user_tags TEXT[],                   -- User-provided tags for grouping and filtering

    -- Ingestion Context (The "System Story")
    staged_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    staged_by_user TEXT,                -- The system user who ran the staging command
    staged_on_host TEXT NOT NULL,       -- The hostname where staging occurred
    staged_via_command TEXT,            -- The exact 'exo blob stage ...' command used

    -- Original Source File Metadata
    source_path TEXT,                   -- Original absolute path of the file
    source_mtime TIMESTAMPTZ,           -- Original modification time (crucial for ts_orig inference)
    source_size BIGINT,                 -- Original file size

    -- Content-Derived Metadata (The "Inferred Story")
    start_time TIMESTAMPTZ,             -- Earliest conceptual timestamp found *inside* the blob
    end_time TIMESTAMPTZ,               -- Latest conceptual timestamp found *inside* the blob
    timing_info_type TEXT NOT NULL CHECK (timing_info_type IN ('intrinsic', 'external_wrapper', 'inferred', 'none')),
    source_material_format TEXT NOT NULL DEFAULT 'raw',
    
    -- Processing State
    processing_status TEXT DEFAULT 'staged' CHECK (processing_status IN ('staged', 'processing', 'completed', 'failed', 'archived')),

    -- Validation constraints
    CONSTRAINT smr_source_identifier_not_empty CHECK (length(TRIM(BOTH FROM source_identifier)) > 0),
    CONSTRAINT smr_staged_on_host_not_empty CHECK (length(TRIM(BOTH FROM staged_on_host)) > 0),
    CONSTRAINT smr_checksum_not_empty CHECK (length(TRIM(BOTH FROM checksum)) > 0),
    CONSTRAINT smr_valid_time_range CHECK (start_time IS NULL OR end_time IS NULL OR start_time <= end_time),
    CONSTRAINT smr_valid_source_size CHECK (source_size IS NULL OR source_size >= 0)
);

-- =============================================================================
-- Part 2: Create performance indexes
-- =============================================================================

-- Primary access patterns for efficient querying
CREATE INDEX idx_smr_checksum ON raw.source_material_registry (checksum);
CREATE INDEX idx_smr_stage_batch_id ON raw.source_material_registry (stage_batch_id);
CREATE INDEX idx_smr_source_identifier ON raw.source_material_registry (source_identifier);
CREATE INDEX idx_smr_staged_at ON raw.source_material_registry (staged_at DESC);
CREATE INDEX idx_smr_processing_status ON raw.source_material_registry (processing_status);

-- Composite indexes for common query patterns
CREATE INDEX idx_smr_source_status ON raw.source_material_registry (source_identifier, processing_status);
CREATE INDEX idx_smr_status_staged_at ON raw.source_material_registry (processing_status, staged_at DESC);

-- Time-based queries (when timing metadata is available)
CREATE INDEX idx_smr_time_range ON raw.source_material_registry (start_time, end_time) 
WHERE start_time IS NOT NULL AND end_time IS NOT NULL;

-- Tag-based queries using GIN index for array operations
CREATE INDEX idx_smr_user_tags ON raw.source_material_registry USING GIN (user_tags)
WHERE user_tags IS NOT NULL;

-- Source path queries (when available)
CREATE INDEX idx_smr_source_path ON raw.source_material_registry (source_path)
WHERE source_path IS NOT NULL;

-- =============================================================================
-- Part 3: Add comprehensive table and column documentation
-- =============================================================================

COMMENT ON TABLE raw.source_material_registry IS 
'The foundational registry of all external source material in the Sinex system. This table serves as the "data inbox & birth certificate" capturing the complete provenance story of every external data source. It enables the system to be fully auditable, reversible, and intelligible by maintaining perfect knowledge of data origins and capture context.';

-- Core Identity & Deduplication
COMMENT ON COLUMN raw.source_material_registry.blob_id IS 
'Primary key ULID that uniquely identifies this source material blob in the system.';

COMMENT ON COLUMN raw.source_material_registry.checksum IS 
'Blake3 hash of the complete source material content. Used for deduplication to prevent staging the same content multiple times.';

COMMENT ON COLUMN raw.source_material_registry.stage_batch_id IS 
'UUID that groups multiple files staged together in a single `exo blob stage` command invocation. Enables atomic batch operations.';

-- User-Provided Context (The "Human Story")
COMMENT ON COLUMN raw.source_material_registry.source_identifier IS 
'User-defined descriptive name for this source material (e.g., "old-laptop-bash", "live-kitty-stream"). Provides human-readable context for data organization.';

COMMENT ON COLUMN raw.source_material_registry.user_comment IS 
'Free-text description provided by the user to explain the significance or context of this source material.';

COMMENT ON COLUMN raw.source_material_registry.user_tags IS 
'Array of user-provided tags for grouping, filtering, and organizing source material. Enables flexible categorization.';

-- Ingestion Context (The "System Story")  
COMMENT ON COLUMN raw.source_material_registry.staged_at IS 
'Timestamp when this source material was first staged into the system. Captures the moment of system awareness.';

COMMENT ON COLUMN raw.source_material_registry.staged_by_user IS 
'The system user account that executed the staging command. Important for audit trails and permission tracking.';

COMMENT ON COLUMN raw.source_material_registry.staged_on_host IS 
'Hostname of the machine where the staging operation occurred. Critical for distributed system provenance.';

COMMENT ON COLUMN raw.source_material_registry.staged_via_command IS 
'The exact `exo blob stage` command used to stage this material. Enables perfect reproduction of staging operations.';

-- Original Source File Metadata
COMMENT ON COLUMN raw.source_material_registry.source_path IS 
'Original absolute filesystem path of the source file before staging. May be NULL for non-file sources (e.g., stdin, network streams).';

COMMENT ON COLUMN raw.source_material_registry.source_mtime IS 
'Original modification timestamp of the source file. Crucial for inferring ts_orig values during event interpretation.';

COMMENT ON COLUMN raw.source_material_registry.source_size IS 
'Original size in bytes of the source file. Used for validation and storage planning.';

-- Content-Derived Metadata (The "Inferred Story")
COMMENT ON COLUMN raw.source_material_registry.start_time IS 
'Earliest conceptual timestamp found within the source material content. Inferred by analyzing timestamps inside the data.';

COMMENT ON COLUMN raw.source_material_registry.end_time IS 
'Latest conceptual timestamp found within the source material content. Defines the temporal scope of the data.';

COMMENT ON COLUMN raw.source_material_registry.timing_info_type IS 
'Method used to determine start_time and end_time: "intrinsic" (timestamps within content), "external_wrapper" (file metadata), "inferred" (heuristic analysis), "none" (no timing info available).';

COMMENT ON COLUMN raw.source_material_registry.source_material_format IS 
'Format descriptor for the source material (e.g., "raw", "json", "csv", "log"). Helps ingestors choose appropriate parsing strategies.';

-- Processing State
COMMENT ON COLUMN raw.source_material_registry.processing_status IS 
'Current processing status: "staged" (ready for processing), "processing" (currently being processed), "completed" (fully processed), "failed" (processing failed), "archived" (logically removed).';

-- =============================================================================
-- Part 4: Create helper functions for common operations
-- =============================================================================

-- Function to find source material by various criteria
CREATE OR REPLACE FUNCTION raw.find_source_material(
    p_source_identifier TEXT DEFAULT NULL,
    p_user_tags TEXT[] DEFAULT NULL,
    p_processing_status TEXT DEFAULT NULL,
    p_limit INTEGER DEFAULT 100
) RETURNS TABLE(
    blob_id ULID,
    source_identifier TEXT,
    staged_at TIMESTAMPTZ,
    processing_status TEXT,
    user_comment TEXT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        smr.blob_id,
        smr.source_identifier,
        smr.staged_at,
        smr.processing_status,
        smr.user_comment
    FROM raw.source_material_registry smr
    WHERE 
        (p_source_identifier IS NULL OR smr.source_identifier = p_source_identifier)
        AND (p_user_tags IS NULL OR smr.user_tags && p_user_tags)
        AND (p_processing_status IS NULL OR smr.processing_status = p_processing_status)
    ORDER BY smr.staged_at DESC
    LIMIT p_limit;
END;
$$ LANGUAGE plpgsql;

-- Function to get staging statistics
CREATE OR REPLACE FUNCTION raw.get_staging_statistics()
RETURNS TABLE(
    total_blobs BIGINT,
    total_size_bytes NUMERIC,
    by_status JSONB,
    by_format JSONB,
    recent_activity JSONB
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        COUNT(*)::BIGINT as total_blobs,
        COALESCE(SUM(source_size), 0) as total_size_bytes,
        CASE 
            WHEN COUNT(*) = 0 THEN '{}'::JSONB
            ELSE jsonb_object_agg(processing_status, status_count)
        END as by_status,
        CASE 
            WHEN COUNT(*) = 0 THEN '{}'::JSONB
            ELSE jsonb_object_agg(source_material_format, format_count)
        END as by_format,
        jsonb_build_object(
            'last_24h', COUNT(*) FILTER (WHERE staged_at > NOW() - INTERVAL '24 hours'),
            'last_week', COUNT(*) FILTER (WHERE staged_at > NOW() - INTERVAL '7 days'),
            'last_month', COUNT(*) FILTER (WHERE staged_at > NOW() - INTERVAL '30 days')
        ) as recent_activity
    FROM (
        SELECT 
            processing_status,
            source_material_format,
            source_size,
            staged_at,
            COUNT(*) OVER (PARTITION BY processing_status) as status_count,
            COUNT(*) OVER (PARTITION BY source_material_format) as format_count
        FROM raw.source_material_registry
    ) stats;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- Part 5: Grant appropriate permissions
-- =============================================================================

-- Grant basic read access to public (will be refined based on actual role setup)
GRANT SELECT ON raw.source_material_registry TO PUBLIC;

-- =============================================================================
-- Part 6: Final success notification
-- =============================================================================

DO $$
BEGIN
    RAISE NOTICE '✅ Source Material Registry table successfully created';
    RAISE NOTICE '📊 Features implemented:';
    RAISE NOTICE '   - Complete provenance tracking (human, system, and inferred stories)';
    RAISE NOTICE '   - Blake3-based deduplication via checksum uniqueness';
    RAISE NOTICE '   - Comprehensive indexing for efficient queries';
    RAISE NOTICE '   - Validation constraints for data integrity';
    RAISE NOTICE '   - Helper functions for common operations';
    RAISE NOTICE '🏗️  Foundation ready for unified architecture implementation';
    RAISE NOTICE '🔄 Next steps: Implement core.events table and replay system';
END $$;
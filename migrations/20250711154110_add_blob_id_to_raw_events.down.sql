-- Migration: Add blob_id column to raw.events for first-class blob attachments
-- Down Migration

-- Drop indexes first
DROP INDEX IF EXISTS idx_raw_events_no_blob;
DROP INDEX IF EXISTS idx_raw_events_blob_id;

-- Drop foreign key constraint
ALTER TABLE raw.events DROP CONSTRAINT IF EXISTS fk_raw_events_blob_id;

-- Remove blob_id column
ALTER TABLE raw.events DROP COLUMN IF EXISTS blob_id;
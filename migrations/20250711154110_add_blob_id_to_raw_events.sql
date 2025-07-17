-- Migration: Add blob_id column to raw.events for first-class blob attachments
-- Up Migration

-- Add blob_id column without constraints first (TimescaleDB hypertable limitation)
ALTER TABLE raw.events 
ADD COLUMN blob_id ULID;

-- Add foreign key constraint separately
ALTER TABLE raw.events 
ADD CONSTRAINT fk_raw_events_blob_id 
FOREIGN KEY (blob_id) REFERENCES core.blobs(id) ON DELETE SET NULL;

-- Add comment explaining the column
COMMENT ON COLUMN raw.events.blob_id IS 'Optional reference to binary blob associated with this event (e.g., screenshots, recordings, large clipboard content)';

-- Create index for efficient blob-related queries
CREATE INDEX IF NOT EXISTS idx_raw_events_blob_id 
ON raw.events(blob_id) 
WHERE blob_id IS NOT NULL;

-- Create partial index for finding events without blobs (for cleanup/analysis)
CREATE INDEX IF NOT EXISTS idx_raw_events_no_blob 
ON raw.events(id) 
WHERE blob_id IS NULL;
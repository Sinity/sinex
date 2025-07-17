-- Add schema identification fields to raw.events table
-- This supports the new schema management system specified in the satellite architecture

-- Add schema identification columns if they don't exist
DO $$
BEGIN
    -- Add payload_schema_name if it doesn't exist
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_schema = 'raw' 
        AND table_name = 'events' 
        AND column_name = 'payload_schema_name'
    ) THEN
        ALTER TABLE raw.events ADD COLUMN payload_schema_name TEXT;
    END IF;
    
    -- Add payload_schema_version if it doesn't exist
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_schema = 'raw' 
        AND table_name = 'events' 
        AND column_name = 'payload_schema_version'
    ) THEN
        ALTER TABLE raw.events ADD COLUMN payload_schema_version TEXT;
    END IF;
    
    -- Add payload_schema_id if it doesn't exist (denormalized FK for performance)
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_schema = 'raw' 
        AND table_name = 'events' 
        AND column_name = 'payload_schema_id'
    ) THEN
        ALTER TABLE raw.events ADD COLUMN payload_schema_id UUID;
    END IF;
END $$;

-- Create indexes for efficient schema-based queries
CREATE INDEX IF NOT EXISTS idx_events_schema_name 
ON raw.events (payload_schema_name) 
WHERE payload_schema_name IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_events_schema_version 
ON raw.events (payload_schema_name, payload_schema_version) 
WHERE payload_schema_name IS NOT NULL AND payload_schema_version IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_events_schema_id 
ON raw.events (payload_schema_id) 
WHERE payload_schema_id IS NOT NULL;

-- Create index for schemaless events (for monitoring)
CREATE INDEX IF NOT EXISTS idx_events_no_schema 
ON raw.events (ts_ingest) 
WHERE payload_schema_name IS NULL;

-- Add foreign key constraint to schema registry (if it exists)
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_schema = 'sinex_schemas' 
        AND table_name = 'event_payload_schemas'
    ) THEN
        -- Add foreign key constraint
        ALTER TABLE raw.events 
        ADD CONSTRAINT fk_events_schema_id 
        FOREIGN KEY (payload_schema_id) 
        REFERENCES sinex_schemas.event_payload_schemas(id)
        ON DELETE SET NULL;
    END IF;
END $$;

-- Comments for documentation
COMMENT ON COLUMN raw.events.payload_schema_name IS 'Name of the JSON schema for payload validation (e.g., shell.command.executed)';
COMMENT ON COLUMN raw.events.payload_schema_version IS 'Version of the JSON schema (e.g., 1.0.0)';
COMMENT ON COLUMN raw.events.payload_schema_id IS 'Denormalized foreign key to schema registry for performance';
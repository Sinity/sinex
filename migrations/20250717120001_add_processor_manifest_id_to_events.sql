-- Add processor_manifest_id foreign key to events table
-- This migration runs after the events table is created

-- Check if the events table exists and add the foreign key
DO $$ 
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'core' AND table_name = 'events') THEN
        ALTER TABLE core.events ADD COLUMN IF NOT EXISTS processor_manifest_id INTEGER 
            REFERENCES sinex_schemas.processor_manifests(id);
        
        CREATE INDEX IF NOT EXISTS idx_events_processor_manifest_id 
            ON core.events (processor_manifest_id);
            
        COMMENT ON COLUMN core.events.processor_manifest_id IS 'Foreign key to processor_manifests table for provenance tracking';
    END IF;
END $$;
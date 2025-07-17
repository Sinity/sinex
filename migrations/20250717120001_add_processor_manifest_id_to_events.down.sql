-- Remove processor_manifest_id foreign key from events table

-- Check if the events table exists and remove the foreign key
DO $$ 
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'core' AND table_name = 'events') THEN
        DROP INDEX IF EXISTS core.idx_events_processor_manifest_id;
        ALTER TABLE core.events DROP COLUMN IF EXISTS processor_manifest_id;
    END IF;
END $$;
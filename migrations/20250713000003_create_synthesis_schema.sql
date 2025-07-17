-- Transitional migration for synthesis schema
-- This migration is being transitioned to unified architecture
-- The synthesis schema is no longer needed in the unified architecture

-- Create synthesis schema temporarily for backward compatibility
-- This will be removed in the unified architecture migration
CREATE SCHEMA IF NOT EXISTS synthesis;
COMMENT ON SCHEMA synthesis IS 'Transitional schema - will be removed in unified architecture migration';

-- Create a placeholder table that will be migrated to core.events
CREATE TABLE IF NOT EXISTS synthesis.events (
    id ULID PRIMARY KEY DEFAULT gen_ulid(),
    source TEXT NOT NULL CHECK (LENGTH(TRIM(source)) > 0),
    event_type TEXT NOT NULL CHECK (LENGTH(TRIM(event_type)) > 0),
    ts_ingest TIMESTAMPTZ GENERATED ALWAYS AS (id::timestamp) STORED,
    ts_orig TIMESTAMPTZ,
    host TEXT NOT NULL CHECK (LENGTH(TRIM(host)) > 0),
    ingestor_version TEXT,
    payload_schema_id ULID,
    payload JSONB NOT NULL,
    payload_schema_name TEXT,
    payload_schema_version TEXT,
    source_raw_event_ids ULID[],
    source_synthesis_event_ids ULID[]
);

-- Add minimal indexes for transitional period
CREATE INDEX IF NOT EXISTS idx_synthesis_events_id ON synthesis.events (id);
CREATE INDEX IF NOT EXISTS idx_synthesis_events_source_type_ts ON synthesis.events (source, event_type, ts_ingest DESC);
CREATE INDEX IF NOT EXISTS idx_synthesis_events_ts_ingest ON synthesis.events (ts_ingest DESC);
CREATE INDEX IF NOT EXISTS idx_synthesis_source_raw_ids ON synthesis.events USING GIN (source_raw_event_ids);
CREATE INDEX IF NOT EXISTS idx_synthesis_source_synthesis_ids ON synthesis.events USING GIN (source_synthesis_event_ids);

-- Add foreign key to schema registry if it exists
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_schema = 'sinex_schemas' 
        AND table_name = 'event_payload_schemas'
    ) THEN
        ALTER TABLE synthesis.events 
        ADD CONSTRAINT events_payload_schema_id_fkey 
        FOREIGN KEY (payload_schema_id) 
        REFERENCES sinex_schemas.event_payload_schemas(id) 
        ON UPDATE CASCADE ON DELETE SET NULL;
    END IF;
END $$;

-- Add comments
COMMENT ON TABLE synthesis.events IS 'Transitional synthesis events table - will be merged into core.events in unified architecture migration';
COMMENT ON COLUMN synthesis.events.source_raw_event_ids IS 'Raw event ULIDs used as evidence for this synthesis - will be mapped to source_event_ids in unified architecture';
COMMENT ON COLUMN synthesis.events.source_synthesis_event_ids IS 'Synthesis event ULIDs used for cascading synthesis - will be mapped to source_event_ids in unified architecture';

-- Success message
DO $$
BEGIN
    RAISE NOTICE '✅ Created transitional synthesis schema';
    RAISE NOTICE '🔄 This will be migrated to unified architecture in a later migration';
END $$;
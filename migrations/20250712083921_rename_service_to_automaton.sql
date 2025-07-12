-- Rename service tables and columns to automaton terminology

-- Rename the main table
ALTER TABLE sinex_schemas.service_manifests RENAME TO automaton_manifests;

-- Rename columns in automaton_manifests  
ALTER TABLE sinex_schemas.automaton_manifests 
    RENAME COLUMN service_name TO automaton_name;

ALTER TABLE sinex_schemas.automaton_manifests 
    RENAME COLUMN service_type TO automaton_type;

-- Rename column in work_queue
ALTER TABLE sinex_schemas.work_queue 
    RENAME COLUMN target_service_name TO target_automaton_name;

-- Rename column in dlq_events 
DO $$ 
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_schema = 'sinex_schemas' 
        AND table_name = 'dlq_events' 
        AND column_name = 'service_name'
    ) THEN
        ALTER TABLE sinex_schemas.dlq_events RENAME COLUMN service_name TO automaton_name;
    END IF;
END $$;

-- Update indexes
DROP INDEX IF EXISTS sinex_schemas.idx_service_manifests_status_type;
CREATE INDEX IF NOT EXISTS idx_automaton_manifests_status_type 
    ON sinex_schemas.automaton_manifests (automaton_type, status);

-- Update comments
COMMENT ON TABLE sinex_schemas.automaton_manifests 
    IS 'Central registry for Sinex automata (event processors), their capabilities, configuration, and status.';

COMMENT ON COLUMN sinex_schemas.automaton_manifests.automaton_name 
    IS 'Unique automaton identifier, e.g., "FilesystemProcessor_v0.3.1"';

COMMENT ON COLUMN sinex_schemas.automaton_manifests.automaton_type 
    IS 'Automaton type: ingestor, processor, enricher, analytical, ui_backend, system_utility';
-- Rename agent tables and columns to service terminology

-- Rename the main table
ALTER TABLE sinex_schemas.agent_manifests RENAME TO service_manifests;

-- Rename columns in service_manifests
ALTER TABLE sinex_schemas.service_manifests 
    RENAME COLUMN agent_name TO service_name;

ALTER TABLE sinex_schemas.service_manifests 
    RENAME COLUMN agent_type TO service_type;

-- Rename column in work_queue
ALTER TABLE sinex_schemas.work_queue 
    RENAME COLUMN target_agent_name TO target_service_name;

-- Rename column in dlq_events (if it exists - some installations might not have it yet)
DO $$ 
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_schema = 'sinex_schemas' 
        AND table_name = 'dlq_events' 
        AND column_name = 'agent_name'
    ) THEN
        ALTER TABLE sinex_schemas.dlq_events RENAME COLUMN agent_name TO service_name;
    END IF;
END $$;

-- Update indexes
DROP INDEX IF EXISTS sinex_schemas.idx_agent_manifests_status_type;
CREATE INDEX IF NOT EXISTS idx_service_manifests_status_type 
    ON sinex_schemas.service_manifests (service_type, status);

-- Update comments
COMMENT ON TABLE sinex_schemas.service_manifests 
    IS 'Central registry for Sinex services (workers), their capabilities, configuration, and status.';

COMMENT ON COLUMN sinex_schemas.service_manifests.service_name 
    IS 'Unique service identifier, e.g., "HyprlandIngestor_v0.3.1"';

COMMENT ON COLUMN sinex_schemas.service_manifests.service_type 
    IS 'Service type: ingestor, worker, enricher, analytical, ui_backend, system_utility';
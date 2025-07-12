-- Revert service terminology back to agent

-- Rename columns in dlq_events
ALTER TABLE sinex_schemas.dlq_events 
    RENAME COLUMN service_name TO agent_name;

-- Rename column in work_queue
ALTER TABLE sinex_schemas.work_queue 
    RENAME COLUMN target_service_name TO target_agent_name;

-- Rename columns in service_manifests
ALTER TABLE sinex_schemas.service_manifests 
    RENAME COLUMN service_type TO agent_type;

ALTER TABLE sinex_schemas.service_manifests 
    RENAME COLUMN service_name TO agent_name;

-- Rename the main table
ALTER TABLE sinex_schemas.service_manifests RENAME TO agent_manifests;

-- Restore indexes
DROP INDEX IF EXISTS sinex_schemas.idx_service_manifests_status_type;
CREATE INDEX IF NOT EXISTS idx_agent_manifests_status_type 
    ON sinex_schemas.agent_manifests (agent_type, status);

-- Restore comments
COMMENT ON TABLE sinex_schemas.agent_manifests 
    IS 'Central registry for Sinex agents, their capabilities, configuration, and status.';

COMMENT ON COLUMN sinex_schemas.agent_manifests.agent_name 
    IS 'Unique, e.g., "HyprlandIngestor_Rust_v0.3.1"';

COMMENT ON COLUMN sinex_schemas.agent_manifests.agent_type 
    IS 'e.g., ''ingestor'', ''promoter'', ''enricher'', ''analytical'', ''ui_backend'', ''system_utility''';
-- Drop processor_manifests table and related components

-- Drop foreign key from events table
ALTER TABLE core.events DROP COLUMN IF EXISTS processor_manifest_id;

-- Drop indexes
DROP INDEX IF EXISTS sinex_schemas.idx_processor_manifests_name_type;
DROP INDEX IF EXISTS sinex_schemas.idx_processor_manifests_status;
DROP INDEX IF EXISTS sinex_schemas.idx_processor_manifests_heartbeat;
DROP INDEX IF EXISTS core.idx_events_processor_manifest_id;

-- Drop trigger
DROP TRIGGER IF EXISTS processor_manifests_updated_at ON sinex_schemas.processor_manifests;
DROP FUNCTION IF EXISTS sinex_schemas.update_processor_manifests_updated_at();

-- Drop table
DROP TABLE IF EXISTS sinex_schemas.processor_manifests;
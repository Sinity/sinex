-- Down migration for GitOps schema registry

-- Drop the view first
DROP VIEW IF EXISTS sinex_schemas.schema_deployment_status;

-- Drop functions
DROP FUNCTION IF EXISTS raw.validate_event_payload_with_registry();
DROP FUNCTION IF EXISTS sinex_schemas.validate_against_registry(TEXT, JSONB);
DROP FUNCTION IF EXISTS sinex_schemas.get_active_schema(TEXT);

-- Drop indexes
DROP INDEX IF EXISTS sinex_schemas.idx_schema_registry_deployed_at;
DROP INDEX IF EXISTS sinex_schemas.idx_schema_registry_version_active;
DROP INDEX IF EXISTS sinex_schemas.idx_schema_registry_schema_id_active;

-- Drop the table
DROP TABLE IF EXISTS sinex_schemas.schema_registry;
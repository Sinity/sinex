-- Down migration for 00000000000003_create_schema_registry.sql

-- Drop function
DROP FUNCTION IF EXISTS sinex_schemas.validate_event_payload(JSONB, TEXT, TEXT);

-- Drop indexes
DROP INDEX IF EXISTS idx_schema_changes_schema;
DROP INDEX IF EXISTS idx_schemas_event_types;
DROP INDEX IF EXISTS idx_schemas_active;

-- Drop tables (order matters due to foreign keys)
DROP TABLE IF EXISTS sinex_schemas.schema_change_log;
DROP TABLE IF EXISTS sinex_schemas.schema_compatibility;
DROP TABLE IF EXISTS sinex_schemas.event_payload_schemas;

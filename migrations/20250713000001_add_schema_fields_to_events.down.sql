-- Rollback schema fields from raw.events table

-- Drop foreign key constraint
ALTER TABLE raw.events DROP CONSTRAINT IF EXISTS fk_events_schema_id;

-- Drop indexes
DROP INDEX IF EXISTS raw.idx_events_schema_name;
DROP INDEX IF EXISTS raw.idx_events_schema_version;
DROP INDEX IF EXISTS raw.idx_events_schema_id;
DROP INDEX IF EXISTS raw.idx_events_no_schema;

-- Drop columns
ALTER TABLE raw.events DROP COLUMN IF EXISTS payload_schema_name;
ALTER TABLE raw.events DROP COLUMN IF EXISTS payload_schema_version;
ALTER TABLE raw.events DROP COLUMN IF EXISTS payload_schema_id;
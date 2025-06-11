-- Migration: Create core schemas for Sinex event substrate
-- Up Migration

-- 1. Core Schemas
CREATE SCHEMA IF NOT EXISTS raw;
COMMENT ON SCHEMA raw IS 'Schema for raw, immutable event data (raw.events).';

CREATE SCHEMA IF NOT EXISTS sinex_schemas;
COMMENT ON SCHEMA sinex_schemas IS 'Schema for Exocortex system schemas, like event payload definitions and agent manifests.';

CREATE SCHEMA IF NOT EXISTS core;
COMMENT ON SCHEMA core IS 'Schema for core structured data: artifacts, entities, blobs, tags, etc.';

-- Down Migration (commented out, will be in separate file for sqlx)
-- DROP SCHEMA IF EXISTS core CASCADE;
-- DROP SCHEMA IF EXISTS sinex_schemas CASCADE;
-- DROP SCHEMA IF EXISTS raw CASCADE;
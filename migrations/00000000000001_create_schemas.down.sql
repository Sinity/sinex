-- Down migration for 00000000000001_create_schemas.sql
-- Drops all schemas created in the up migration

-- Drop schemas in reverse order (newest first)
DROP SCHEMA IF EXISTS sinex_router CASCADE;
DROP SCHEMA IF EXISTS audit CASCADE;
DROP SCHEMA IF EXISTS synthesis CASCADE;
DROP SCHEMA IF EXISTS sinex CASCADE;
DROP SCHEMA IF EXISTS metrics CASCADE;
DROP SCHEMA IF EXISTS sinex_schemas CASCADE;
DROP SCHEMA IF EXISTS raw CASCADE;
DROP SCHEMA IF EXISTS core CASCADE;
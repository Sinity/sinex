-- Migration: Enable pgx_ulid extension
-- Up Migration

-- Enable the ULID extension (from pgx_ulid)
CREATE EXTENSION IF NOT EXISTS ulid;

COMMENT ON EXTENSION ulid IS 'ULID (Universally Unique Lexicographically Sortable Identifier) support via pgx_ulid';

-- Down Migration
-- DROP EXTENSION IF EXISTS ulid CASCADE;
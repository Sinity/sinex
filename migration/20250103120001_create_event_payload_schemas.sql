-- Migration: Create event payload schemas registry table
-- Up Migration

-- Note: This uses gen_ulid() from pgx_ulid extension which must be installed first
-- For MVP, we'll use custom ULID generation in application code if pgx_ulid not available

CREATE TABLE IF NOT EXISTS sinex_schemas.event_payload_schemas (
    id                      ULID PRIMARY KEY DEFAULT gen_ulid(),
    event_source            TEXT NOT NULL,
    event_type              TEXT NOT NULL,
    schema_version          TEXT NOT NULL, -- e.g., "v1.0", "v1.0.1", "v2.0-alpha"
    json_schema_definition  JSONB NOT NULL, -- The actual JSON Schema object
    description             TEXT,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    is_active               BOOLEAN NOT NULL DEFAULT TRUE, -- Flag for current/active schema version
    UNIQUE (event_source, event_type, schema_version)
);

COMMENT ON TABLE sinex_schemas.event_payload_schemas IS 'Registry for JSON Schema definitions of raw.events payloads.';
COMMENT ON COLUMN sinex_schemas.event_payload_schemas.is_active IS 'Indicates if this schema version is currently active and should be used for new events or validation.';

-- Indexes for common queries
CREATE INDEX idx_event_payload_schemas_source_type_active 
    ON sinex_schemas.event_payload_schemas (event_source, event_type, is_active) 
    WHERE is_active = TRUE;

CREATE INDEX idx_event_payload_schemas_created_at 
    ON sinex_schemas.event_payload_schemas (created_at DESC);
-- Migration: Add pg_jsonschema extension and validation constraint
-- Up Migration

-- Enable pg_jsonschema extension
CREATE EXTENSION IF NOT EXISTS pg_jsonschema;

-- Add CHECK constraint for JSON schema validation
ALTER TABLE raw.events
DROP CONSTRAINT IF EXISTS chk_payload_conforms_to_schema;

ALTER TABLE raw.events
ADD CONSTRAINT chk_payload_conforms_to_schema
CHECK (
    payload_schema_id IS NULL OR -- If no schema is specified, validation is skipped
    EXISTS (
        SELECT 1 
        FROM sinex_schemas.event_payload_schemas ps
        WHERE ps.id = raw.events.payload_schema_id
        AND ps.is_active = TRUE
        AND jsonb_matches_schema(
            ps.json_schema_definition,
            raw.events.payload
        )
    )
);

COMMENT ON CONSTRAINT chk_payload_conforms_to_schema ON raw.events
    IS 'Ensures that raw.events.payload conforms to the JSON schema specified by payload_schema_id, if that schema is active.';
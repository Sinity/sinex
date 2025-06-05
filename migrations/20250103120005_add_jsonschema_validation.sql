-- Migration: Add pg_jsonschema extension and validation constraint
-- Up Migration

-- Enable pg_jsonschema extension if available
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_available_extensions 
        WHERE name = 'pg_jsonschema'
    ) THEN
        CREATE EXTENSION IF NOT EXISTS pg_jsonschema;
    ELSE
        RAISE NOTICE 'pg_jsonschema extension not available, skipping JSON schema validation';
    END IF;
END $$;

-- Add CHECK constraint for JSON schema validation only if pg_jsonschema is available
DO $$
BEGIN
    -- Check if pg_jsonschema extension exists
    IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_jsonschema') THEN
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

        EXECUTE 'COMMENT ON CONSTRAINT chk_payload_conforms_to_schema ON raw.events IS ' ||
                quote_literal('Ensures that raw.events.payload conforms to the JSON schema specified by payload_schema_id, if that schema is active.');
    ELSE
        RAISE NOTICE 'pg_jsonschema not available, skipping payload validation constraint';
    END IF;
END $$;
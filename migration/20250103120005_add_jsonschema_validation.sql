-- Migration: Add pg_jsonschema extension and validation trigger
-- Up Migration

-- Enable pg_jsonschema extension (required)
CREATE EXTENSION IF NOT EXISTS pg_jsonschema;

-- Create a function to validate JSON schema
CREATE OR REPLACE FUNCTION raw.validate_event_payload_schema()
RETURNS TRIGGER AS $$
BEGIN
    -- If no schema is specified, validation is skipped
    IF NEW.payload_schema_id IS NULL THEN
        RETURN NEW;
    END IF;
    
    -- Check if the payload conforms to the schema
    IF NOT EXISTS (
        SELECT 1 
        FROM sinex_schemas.event_payload_schemas ps
        WHERE ps.id = NEW.payload_schema_id
        AND ps.is_active = TRUE
        AND jsonb_matches_schema(ps.json_schema_definition, NEW.payload)
    ) THEN
        RAISE EXCEPTION 'Event payload does not conform to schema % or schema is inactive', NEW.payload_schema_id;
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger to validate on insert/update
CREATE TRIGGER trg_validate_event_payload_schema
    BEFORE INSERT OR UPDATE OF payload, payload_schema_id
    ON raw.events
    FOR EACH ROW
    EXECUTE FUNCTION raw.validate_event_payload_schema();

COMMENT ON TRIGGER trg_validate_event_payload_schema ON raw.events IS 
    'Ensures that raw.events.payload conforms to the JSON schema specified by payload_schema_id, if that schema is active.';
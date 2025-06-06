-- Down migration for 20250103120005_add_jsonschema_validation

-- Drop the trigger and function
DROP TRIGGER IF EXISTS trg_validate_event_payload_schema ON raw.events;
DROP FUNCTION IF EXISTS raw.validate_event_payload_schema();

-- Note: We don't drop the extension as other objects might depend on it
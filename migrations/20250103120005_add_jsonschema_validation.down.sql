-- Down migration for 20250103120005_add_jsonschema_validation

ALTER TABLE raw.events DROP CONSTRAINT IF EXISTS chk_payload_conforms_to_schema;

-- Note: We don't drop the extension as other objects might depend on it
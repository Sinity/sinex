-- Down migration for 20250103120004_add_schema_change_trigger

DROP TRIGGER IF EXISTS trg_event_payload_schemas_after_insert_update ON sinex_schemas.event_payload_schemas;
DROP FUNCTION IF EXISTS sinex_schemas.log_schema_change_trigger_func();
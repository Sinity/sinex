-- Migration: Add trigger for schema change eventification
-- Up Migration

-- Create trigger function to log schema changes as events
CREATE OR REPLACE FUNCTION sinex_schemas.log_schema_change_trigger_func()
RETURNS TRIGGER AS $$
DECLARE
    v_change_type TEXT;
    v_payload JSONB;
BEGIN
    IF (TG_OP = 'INSERT') THEN
        v_change_type := 'created';
        IF NEW.is_active THEN
            v_change_type := 'created_and_activated';
        END IF;
    ELSIF (TG_OP = 'UPDATE') THEN
        IF OLD.is_active IS DISTINCT FROM NEW.is_active THEN
            v_change_type := CASE WHEN NEW.is_active THEN 'activated' ELSE 'deactivated' END;
        ELSE
            v_change_type := 'updated_metadata';
        END IF;
    ELSE
        RETURN NULL; -- Should not happen for this trigger configuration
    END IF;

    v_payload := jsonb_build_object(
        'schema_id', NEW.id::text,
        'event_source', NEW.event_source,
        'event_type', NEW.event_type,
        'schema_version', NEW.schema_version,
        'change_type', v_change_type,
        'description', NEW.description,
        '_provenance', jsonb_build_object(
            'event_source', 'schema_registry_trigger',
            'trigger_op', TG_OP,
            'schema_change_id', gen_random_uuid()::text
        )
    );

    INSERT INTO raw.events (source, event_type, host, payload, payload_schema_id)
    VALUES (
        'sinex.schema.registry_monitor',
        'definition_changed',
        coalesce(inet_server_addr()::text, 'localhost'),
        v_payload,
        NULL -- Meta-events don't have schemas yet
    );
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create the trigger
CREATE TRIGGER trg_event_payload_schemas_after_insert_update
AFTER INSERT OR UPDATE ON sinex_schemas.event_payload_schemas
FOR EACH ROW
EXECUTE FUNCTION sinex_schemas.log_schema_change_trigger_func();

COMMENT ON FUNCTION sinex_schemas.log_schema_change_trigger_func() IS 'Logs schema registry changes as events in raw.events for auditability.';
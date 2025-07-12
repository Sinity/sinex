-- Migration: Fix obsolete router triggers that reference removed tables
-- The old work_queue and agent_manifests tables have been removed in the satellite architecture

-- Drop the obsolete router trigger and function
DROP TRIGGER IF EXISTS trg_raw_events_route_after_insert ON raw.events;
DROP FUNCTION IF EXISTS raw.trigger_router_on_event_insert();
DROP FUNCTION IF EXISTS sinex_router.route_raw_event_to_work_queue(UUID);
DROP FUNCTION IF EXISTS sinex_router.route_raw_event_to_promotion_queue(UUID);

-- Drop the obsolete routing cache that depended on agent_manifests
DROP MATERIALIZED VIEW IF EXISTS sinex_schemas.routing_cache;
DROP TRIGGER IF EXISTS trg_agent_manifests_refresh_cache ON sinex_schemas.agent_manifests;
DROP TRIGGER IF EXISTS trg_agent_manifests_refresh_cache ON sinex_schemas.service_manifests;
DROP TRIGGER IF EXISTS trg_agent_manifests_refresh_cache ON sinex_schemas.automaton_manifests;
DROP FUNCTION IF EXISTS sinex_router.trigger_refresh_routing_cache();
DROP FUNCTION IF EXISTS sinex_router.refresh_routing_cache();

-- Drop the schema change logging trigger that tries to route events
DROP TRIGGER IF EXISTS trg_event_payload_schemas_after_insert_update ON sinex_schemas.event_payload_schemas;

-- Create a simplified schema change logging function that doesn't route events
CREATE OR REPLACE FUNCTION sinex_schemas.log_schema_change_trigger_func()
RETURNS TRIGGER AS $$
DECLARE
    v_action TEXT;
    v_payload JSONB;
BEGIN
    -- Determine the action
    IF TG_OP = 'INSERT' THEN
        v_action := 'created';
    ELSIF TG_OP = 'UPDATE' THEN
        v_action := 'updated';
    ELSE
        RETURN NEW;
    END IF;
    
    -- Build payload
    v_payload := jsonb_build_object(
        'action', v_action,
        'schema_id', NEW.id,
        'event_source', NEW.event_source,
        'event_type', NEW.event_type,
        'schema_version', NEW.schema_version,
        'is_active', NEW.is_active,
        'timestamp', CURRENT_TIMESTAMP
    );
    
    -- Log the change without routing (routing is handled by satellites now)
    -- This is just for audit trail
    INSERT INTO raw.events (
        source, 
        event_type, 
        host, 
        payload, 
        payload_schema_id
    )
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

-- Recreate the trigger with the simplified function
CREATE TRIGGER trg_event_payload_schemas_after_insert_update
AFTER INSERT OR UPDATE ON sinex_schemas.event_payload_schemas
FOR EACH ROW
EXECUTE FUNCTION sinex_schemas.log_schema_change_trigger_func();

-- Add comment explaining the satellite architecture
COMMENT ON FUNCTION sinex_schemas.log_schema_change_trigger_func() IS 
'Logs schema changes as events for audit trail. In the satellite architecture, 
event routing is handled by satellites via Redis streams, not database triggers.';

-- Clean up the obsolete router schema if empty
DO $$
BEGIN
    -- Check if sinex_router schema has any remaining objects
    IF NOT EXISTS (
        SELECT 1 
        FROM information_schema.routines 
        WHERE routine_schema = 'sinex_router'
        UNION
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'sinex_router'
        UNION
        SELECT 1
        FROM information_schema.views
        WHERE table_schema = 'sinex_router'
    ) THEN
        DROP SCHEMA IF EXISTS sinex_router;
    END IF;
END $$;
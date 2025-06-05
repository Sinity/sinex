-- Migration: Create event router schema and function
-- Up Migration

-- Create router schema
CREATE SCHEMA IF NOT EXISTS sinex_router;
COMMENT ON SCHEMA sinex_router IS 'Schema for event routing functions and infrastructure.';

-- Create routing function
CREATE OR REPLACE FUNCTION sinex_router.route_raw_event_to_promotion_queue(p_raw_event_id ULID)
RETURNS VOID AS $$
DECLARE
    v_event_source TEXT;
    v_event_type TEXT;
    v_agent_record RECORD;
BEGIN
    SELECT source, event_type INTO v_event_source, v_event_type
    FROM raw.events WHERE id = p_raw_event_id;

    IF NOT FOUND THEN RETURN; END IF;

    -- Find active agents subscribing to this (source, event_type)
    -- Assuming subscribes_to_event_types is like:
    -- { "desktop.hyprland.plugin": ["window_focused", "workspace_activated"], "app.neovim.plugin": ["file_saved"] }
    FOR v_agent_record IN
        SELECT am.agent_name
        FROM sinex_schemas.agent_manifests am
        WHERE am.status = 'running'
          AND am.subscribes_to_event_types IS NOT NULL
          AND am.subscribes_to_event_types ? v_event_source
          AND am.subscribes_to_event_types -> v_event_source @> to_jsonb(v_event_type)
    LOOP
        INSERT INTO sinex_schemas.promotion_queue (raw_event_id, target_agent_name)
        VALUES (p_raw_event_id, v_agent_record.agent_name)
        ON CONFLICT (raw_event_id, target_agent_name) DO NOTHING;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION sinex_router.route_raw_event_to_promotion_queue(ULID) IS 'Routes raw events to appropriate agents based on their subscription manifests.';

-- Create trigger function for routing
CREATE OR REPLACE FUNCTION raw.trigger_router_on_event_insert() 
RETURNS TRIGGER AS $$
BEGIN
  PERFORM sinex_router.route_raw_event_to_promotion_queue(NEW.id);
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger on raw.events
CREATE TRIGGER trg_raw_events_route_after_insert
AFTER INSERT ON raw.events
FOR EACH ROW EXECUTE FUNCTION raw.trigger_router_on_event_insert();

COMMENT ON TRIGGER trg_raw_events_route_after_insert ON raw.events IS 'Automatically routes new events to promotion queue based on agent subscriptions.';
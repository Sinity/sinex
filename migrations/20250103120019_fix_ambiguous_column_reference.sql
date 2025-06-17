-- Fix ambiguous column reference in route_raw_event_to_work_queue function
-- The variable 'event_type' conflicts with the column name 'event_type'

CREATE OR REPLACE FUNCTION sinex_router.route_raw_event_to_work_queue(event_id uuid)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    event_source text;
    event_type_val text;  -- Renamed to avoid conflict with column name
    agent_record record;
    event_types_for_source text[];
BEGIN
    -- Get the event details - explicitly qualify the column names
    SELECT e.source, e.event_type INTO event_source, event_type_val
    FROM raw.events e
    WHERE e.id = event_id::uuid::ulid;
    
    IF NOT FOUND THEN
        RETURN;
    END IF;
    
    -- Find agents that subscribe to this event type
    FOR agent_record IN
        SELECT agent_name
        FROM sinex_schemas.agent_manifests
        WHERE status = 'running'
        AND subscribes_to_event_types ? event_source
    LOOP
        -- Extract the event types array for this source
        SELECT ARRAY(SELECT jsonb_array_elements_text(subscribes_to_event_types->event_source))
        INTO event_types_for_source
        FROM sinex_schemas.agent_manifests
        WHERE agent_name = agent_record.agent_name;
        
        -- Check if this specific event type is subscribed
        IF event_type_val = ANY(event_types_for_source) THEN
            -- Insert into work_queue with only the columns that exist
            INSERT INTO sinex_schemas.work_queue (raw_event_id, target_agent_name)
            VALUES (event_id::uuid::ulid, agent_record.agent_name)
            ON CONFLICT (raw_event_id, target_agent_name) DO NOTHING;
        END IF;
    END LOOP;
END;
$$;

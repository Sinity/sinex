-- Fix batch_route_events function to use correct work_queue columns

CREATE OR REPLACE FUNCTION sinex_router.batch_route_events()
RETURNS bigint
LANGUAGE plpgsql
AS $$
DECLARE
    routed_count bigint := 0;
BEGIN
    -- Insert work queue items for events that match routing cache
    -- Only process events that haven't been routed yet
    WITH unrouted_events AS (
        SELECT DISTINCT e.id as event_id, CONCAT(e.source, ':', e.event_type) as full_event_type
        FROM raw.events e
        LEFT JOIN sinex_schemas.work_queue wq ON e.id = wq.raw_event_id
        WHERE wq.raw_event_id IS NULL
        AND e.ts_ingest > now() - interval '24 hours'  -- Only process recent events
    ),
    routing_matches AS (
        SELECT ue.event_id, rc.agent_id
        FROM unrouted_events ue
        JOIN sinex_schemas.routing_cache rc ON ue.full_event_type = rc.event_type
    )
    INSERT INTO sinex_schemas.work_queue (raw_event_id, target_agent_name)
    SELECT rm.event_id, rm.agent_id
    FROM routing_matches rm
    ON CONFLICT (raw_event_id, target_agent_name) DO NOTHING;
    
    GET DIAGNOSTICS routed_count = ROW_COUNT;
    RETURN routed_count;
END;
$$;

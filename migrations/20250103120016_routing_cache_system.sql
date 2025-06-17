-- Migration: Replace per-row trigger routing with materialized view routing cache
-- This implements a batch routing system for better performance

-- First, fix the broken reference in the existing routing function
-- The function still references promotion_queue instead of work_queue
DROP FUNCTION IF EXISTS sinex_router.route_raw_event_to_promotion_queue(uuid);

-- Create the corrected version (temporarily, will be deprecated)
CREATE OR REPLACE FUNCTION sinex_router.route_raw_event_to_work_queue(event_id uuid)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    event_source text;
    event_type text;
    agent_record record;
    event_types_for_source text[];
BEGIN
    -- Get the event details
    SELECT source, event_type INTO event_source, event_type
    FROM raw.events 
    WHERE id = event_id::uuid::ulid;
    
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
        IF event_type = ANY(event_types_for_source) THEN
            -- Insert into work_queue (avoiding duplicates)
            INSERT INTO sinex_schemas.work_queue (raw_event_id, target_agent_name, priority_score, attempts)
            VALUES (event_id::uuid::ulid, agent_record.agent_name, 3, 0)
            ON CONFLICT (raw_event_id, target_agent_name) DO NOTHING;
        END IF;
    END LOOP;
END;
$$;

-- Update the trigger to use the corrected function name
DROP TRIGGER IF EXISTS trg_raw_events_route_after_insert ON raw.events;
CREATE TRIGGER trg_raw_events_route_after_insert
    AFTER INSERT ON raw.events
    FOR EACH ROW
    EXECUTE FUNCTION raw.trigger_router_on_event_insert();

-- Update the trigger function to call the corrected routing function
CREATE OR REPLACE FUNCTION raw.trigger_router_on_event_insert()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    -- Call the corrected routing function
    PERFORM sinex_router.route_raw_event_to_work_queue(NEW.id::uuid);
    RETURN NEW;
END;
$$;

-- Now create the new routing cache system

-- Create materialized view for routing cache
-- This pre-computes which event types should go to which agents
CREATE MATERIALIZED VIEW sinex_schemas.routing_cache AS
SELECT 
    CONCAT(source_key, ':', event_type) AS event_type,
    agent_name AS agent_id
FROM sinex_schemas.agent_manifests am
CROSS JOIN LATERAL jsonb_each_text(am.subscribes_to_event_types) AS sources(source_key, event_types_json)
CROSS JOIN LATERAL jsonb_array_elements_text(event_types_json::jsonb) AS event_type
WHERE am.status = 'running';

-- Create unique index on routing cache for fast lookups
CREATE UNIQUE INDEX idx_routing_cache_event_agent ON sinex_schemas.routing_cache (event_type, agent_id);
CREATE INDEX idx_routing_cache_agent ON sinex_schemas.routing_cache (agent_id);
CREATE INDEX idx_routing_cache_event_type ON sinex_schemas.routing_cache (event_type);

-- Function to refresh the routing cache
CREATE OR REPLACE FUNCTION sinex_router.refresh_routing_cache()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY sinex_schemas.routing_cache;
END;
$$;

-- Function to batch route events based on routing cache
-- This is more efficient than per-row triggers
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
    INSERT INTO sinex_schemas.work_queue (raw_event_id, target_agent_name, priority_score, attempts)
    SELECT rm.event_id, rm.agent_id, 3, 0
    FROM routing_matches rm
    ON CONFLICT (raw_event_id, target_agent_name) DO NOTHING;
    
    GET DIAGNOSTICS routed_count = ROW_COUNT;
    RETURN routed_count;
END;
$$;

-- Function to auto-refresh routing cache when agent manifests change
CREATE OR REPLACE FUNCTION sinex_router.trigger_refresh_routing_cache()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    -- Refresh the routing cache when agent manifests are modified
    PERFORM sinex_router.refresh_routing_cache();
    RETURN COALESCE(NEW, OLD);
END;
$$;

-- Trigger to auto-refresh routing cache on agent manifest changes
DROP TRIGGER IF EXISTS trg_agent_manifests_refresh_cache ON sinex_schemas.agent_manifests;
CREATE TRIGGER trg_agent_manifests_refresh_cache
    AFTER INSERT OR UPDATE OR DELETE ON sinex_schemas.agent_manifests
    FOR EACH STATEMENT
    EXECUTE FUNCTION sinex_router.trigger_refresh_routing_cache();

-- Initial population of routing cache
SELECT sinex_router.refresh_routing_cache();

-- Create a function to gradually migrate from trigger-based to batch-based routing
-- This allows for a smooth transition
CREATE OR REPLACE FUNCTION sinex_router.migrate_to_batch_routing()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    -- Disable the per-row trigger (but keep the function for emergency fallback)
    DROP TRIGGER IF EXISTS trg_raw_events_route_after_insert ON raw.events;
    
    -- Run batch router to catch up on any missed events
    PERFORM sinex_router.batch_route_events();
    
    RAISE NOTICE 'Migration to batch routing complete. Per-row trigger disabled.';
END;
$$;

-- Add comments for documentation
COMMENT ON MATERIALIZED VIEW sinex_schemas.routing_cache IS 
'Pre-computed routing table showing which event types should be routed to which agents. Refreshed automatically when agent_manifests change.';

COMMENT ON FUNCTION sinex_router.batch_route_events() IS 
'Batch processes unrouted events and creates work queue entries based on routing cache. More efficient than per-row triggers.';

COMMENT ON FUNCTION sinex_router.refresh_routing_cache() IS 
'Refreshes the routing cache materialized view. Called automatically when agent manifests change.';

COMMENT ON FUNCTION sinex_router.migrate_to_batch_routing() IS 
'Disables per-row trigger routing and switches to batch-based routing. Call this when ready to migrate.';
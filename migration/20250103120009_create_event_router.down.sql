-- Down migration for 20250103120009_create_event_router

DROP TRIGGER IF EXISTS trg_raw_events_route_after_insert ON raw.events;
DROP FUNCTION IF EXISTS raw.trigger_router_on_event_insert();
DROP FUNCTION IF EXISTS sinex_router.route_raw_event_to_promotion_queue(UUID);
DROP SCHEMA IF EXISTS sinex_router CASCADE;
-- Down migration: This cannot fully restore the old routing system 
-- because the required tables (work_queue, agent_manifests) no longer exist

-- Drop the simplified trigger
DROP TRIGGER IF EXISTS trg_event_payload_schemas_after_insert_update ON sinex_schemas.event_payload_schemas;

-- Note: We cannot restore the old routing functions and triggers because they depend on:
-- - sinex_schemas.work_queue (removed)
-- - sinex_schemas.agent_manifests (removed)
-- - sinex_router schema objects (removed)

-- This is intentional as the satellite architecture has replaced this functionality
-- Remove deprecated work_queue table and related components
-- These are no longer needed in the satellite architecture

-- Drop work_queue related views
DROP VIEW IF EXISTS sinex_schemas.work_queue_stats;
DROP VIEW IF EXISTS sinex_schemas.work_queue_summary;

-- Drop foreign key constraints first
ALTER TABLE IF EXISTS sinex_schemas.work_queue DROP CONSTRAINT IF EXISTS work_queue_target_automaton_name_fkey;

-- Drop dependent objects first
DROP MATERIALIZED VIEW IF EXISTS sinex_schemas.routing_cache;

-- Drop work_queue related functions
DROP FUNCTION IF EXISTS sinex_schemas.get_pending_work(text);
DROP FUNCTION IF EXISTS sinex_schemas.claim_work_batch(text, integer);
DROP FUNCTION IF EXISTS sinex_schemas.mark_work_completed(uuid);
DROP FUNCTION IF EXISTS sinex_schemas.mark_work_failed(uuid, text);

-- Drop work_queue related triggers
DROP TRIGGER IF EXISTS work_queue_updated_at ON sinex_schemas.work_queue;
DROP FUNCTION IF EXISTS sinex_schemas.update_work_queue_updated_at();

-- Drop indexes
DROP INDEX IF EXISTS sinex_schemas.idx_work_queue_status;
DROP INDEX IF EXISTS sinex_schemas.idx_work_queue_claimed_by;
DROP INDEX IF EXISTS sinex_schemas.idx_work_queue_raw_event_id;
DROP INDEX IF EXISTS sinex_schemas.idx_work_queue_target_agent;
DROP INDEX IF EXISTS sinex_schemas.idx_work_queue_created_at;
DROP INDEX IF EXISTS sinex_schemas.idx_work_queue_claimed_at;

-- Drop the work_queue table itself
DROP TABLE IF EXISTS sinex_schemas.work_queue;

-- Remove old agent manifests table (replaced by automaton checkpoints)
DROP TABLE IF EXISTS sinex_schemas.agent_manifests;
DROP TABLE IF EXISTS sinex_schemas.service_manifests;
DROP TABLE IF EXISTS sinex_schemas.automaton_manifests;

-- Drop obsolete notification triggers
DROP FUNCTION IF EXISTS sinex_schemas.notify_work_queue_insert() CASCADE;
DROP FUNCTION IF EXISTS sinex_schemas.notify_work_queue_update() CASCADE;

-- Comments
COMMENT ON SCHEMA sinex_schemas IS 'Schema registry and automaton configuration (work queue removed in satellite architecture)';

-- Grant cleanup (remove grants that are no longer needed)
-- The sinex_user will only need access to core.automaton_checkpoints now
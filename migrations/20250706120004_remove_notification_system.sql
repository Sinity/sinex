-- Remove notification system completely
-- This eliminates the unused PostgreSQL LISTEN/NOTIFY infrastructure
-- that was never deployed in production

-- Remove triggers first
DROP TRIGGER IF EXISTS trigger_notify_event_inserted ON raw.events;
DROP TRIGGER IF EXISTS trigger_notify_work_queue_updated ON sinex_schemas.work_queue;

-- Remove notification functions
DROP FUNCTION IF EXISTS sinex_schemas.notify_event_inserted();
DROP FUNCTION IF EXISTS sinex_schemas.notify_work_queue_updated();
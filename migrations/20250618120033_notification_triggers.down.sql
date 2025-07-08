-- Rollback notification triggers and functions

-- Drop triggers
DROP TRIGGER IF EXISTS trigger_notify_event_inserted ON raw.events;
DROP TRIGGER IF EXISTS trigger_notify_work_queue_updated ON sinex_schemas.work_queue;

-- Drop functions
DROP FUNCTION IF EXISTS notify_event_inserted();
DROP FUNCTION IF EXISTS notify_work_queue_updated();
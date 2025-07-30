-- Down migration for 00000000000006_create_helper_functions.sql

-- Drop all helper functions
DROP FUNCTION IF EXISTS metrics.get_event_stats(INTERVAL);
DROP FUNCTION IF EXISTS core.get_event_lineage(ULID, INTEGER);
DROP FUNCTION IF EXISTS core.archive_events_older_than(TIMESTAMPTZ, INTEGER);
DROP FUNCTION IF EXISTS set_current_timestamp();
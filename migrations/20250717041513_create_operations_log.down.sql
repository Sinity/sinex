-- Rollback operations_log table and associated functions/triggers

-- Drop helper functions
DROP FUNCTION IF EXISTS core.start_operation(TEXT, TEXT, JSONB);
DROP FUNCTION IF EXISTS core.complete_operation(ULID, JSONB);
DROP FUNCTION IF EXISTS core.fail_operation(ULID, JSONB);

-- Drop trigger and trigger function
DROP TRIGGER IF EXISTS operations_log_duration_trigger ON core.operations_log;
DROP FUNCTION IF EXISTS core.calculate_operation_duration();

-- Drop indexes
DROP INDEX IF EXISTS core.idx_operations_log_type;
DROP INDEX IF EXISTS core.idx_operations_log_status;
DROP INDEX IF EXISTS core.idx_operations_log_started_at;
DROP INDEX IF EXISTS core.idx_operations_log_user;
DROP INDEX IF EXISTS core.idx_operations_log_monitoring;

-- Drop table
DROP TABLE IF EXISTS core.operations_log;
-- Create operations_log table for intent-level auditability
-- This table provides auditability for high-level operations like stage, replay, archive, restore, curate
-- It serves as the system's diary, logging user and system actions that cause data to change

CREATE TABLE IF NOT EXISTS core.operations_log (
    -- Primary identification
    operation_id ULID PRIMARY KEY,
    
    -- Operation categorization and status
    operation_type TEXT NOT NULL CHECK (operation_type IN ('stage', 'replay', 'archive', 'restore', 'curate')),
    status TEXT NOT NULL CHECK (status IN ('started', 'completed', 'failed')),
    
    -- Timing information
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    duration_ms BIGINT,
    
    -- User context and command tracking
    invoked_by_user TEXT,
    parameters JSONB NOT NULL, -- The exact command and flags used
    summary JSONB              -- Summary of the outcome (events created/archived, etc.)
);

-- Performance indexes on key query columns
CREATE INDEX IF NOT EXISTS idx_operations_log_type 
ON core.operations_log (operation_type, started_at DESC);

CREATE INDEX IF NOT EXISTS idx_operations_log_status 
ON core.operations_log (status, started_at DESC);

CREATE INDEX IF NOT EXISTS idx_operations_log_started_at 
ON core.operations_log (started_at DESC);

CREATE INDEX IF NOT EXISTS idx_operations_log_user 
ON core.operations_log (invoked_by_user, started_at DESC);

-- Composite index for common monitoring queries
CREATE INDEX IF NOT EXISTS idx_operations_log_monitoring 
ON core.operations_log (operation_type, status, started_at DESC);

-- Trigger function to automatically calculate duration_ms when operation completes
CREATE OR REPLACE FUNCTION core.calculate_operation_duration()
RETURNS TRIGGER AS $$
BEGIN
    -- Only calculate duration when status changes to completed or failed
    -- and completed_at is being set
    IF NEW.completed_at IS NOT NULL AND OLD.completed_at IS NULL THEN
        NEW.duration_ms = EXTRACT(EPOCH FROM (NEW.completed_at - NEW.started_at)) * 1000;
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger to automatically calculate duration
CREATE TRIGGER operations_log_duration_trigger
    BEFORE UPDATE ON core.operations_log
    FOR EACH ROW
    EXECUTE FUNCTION core.calculate_operation_duration();

-- Helper function to start a new operation
CREATE OR REPLACE FUNCTION core.start_operation(
    p_operation_type TEXT,
    p_invoked_by_user TEXT DEFAULT NULL,
    p_parameters JSONB DEFAULT '{}'::jsonb
) RETURNS ULID AS $$
DECLARE
    new_operation_id ULID;
BEGIN
    -- Validate operation type
    IF p_operation_type NOT IN ('stage', 'replay', 'archive', 'restore', 'curate') THEN
        RAISE EXCEPTION 'Invalid operation_type: %. Must be one of: stage, replay, archive, restore, curate', p_operation_type;
    END IF;
    
    -- Generate new ULID for the operation
    new_operation_id := gen_ulid();
    
    -- Insert the operation record
    INSERT INTO core.operations_log (
        operation_id,
        operation_type,
        status,
        started_at,
        invoked_by_user,
        parameters
    ) VALUES (
        new_operation_id,
        p_operation_type,
        'started',
        NOW(),
        p_invoked_by_user,
        p_parameters
    );
    
    RETURN new_operation_id;
END;
$$ LANGUAGE plpgsql;

-- Helper function to complete an operation successfully
CREATE OR REPLACE FUNCTION core.complete_operation(
    p_operation_id ULID,
    p_summary JSONB DEFAULT NULL
) RETURNS VOID AS $$
BEGIN
    UPDATE core.operations_log 
    SET 
        status = 'completed',
        completed_at = NOW(),
        summary = COALESCE(p_summary, summary)
    WHERE operation_id = p_operation_id
      AND status = 'started';
    
    -- Check if the operation was found and updated
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Operation % not found or not in started status', p_operation_id;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Helper function to fail an operation
CREATE OR REPLACE FUNCTION core.fail_operation(
    p_operation_id ULID,
    p_error_summary JSONB DEFAULT NULL
) RETURNS VOID AS $$
BEGIN
    UPDATE core.operations_log 
    SET 
        status = 'failed',
        completed_at = NOW(),
        summary = COALESCE(p_error_summary, summary)
    WHERE operation_id = p_operation_id
      AND status = 'started';
    
    -- Check if the operation was found and updated
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Operation % not found or not in started status', p_operation_id;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Grant necessary permissions
GRANT SELECT, INSERT, UPDATE ON core.operations_log TO sinex;
GRANT EXECUTE ON FUNCTION core.start_operation(TEXT, TEXT, JSONB) TO sinex;
GRANT EXECUTE ON FUNCTION core.complete_operation(ULID, JSONB) TO sinex;
GRANT EXECUTE ON FUNCTION core.fail_operation(ULID, JSONB) TO sinex;

-- Comprehensive documentation comments
COMMENT ON TABLE core.operations_log IS 'Intent-level auditability log for high-level user and system operations that modify data';
COMMENT ON COLUMN core.operations_log.operation_id IS 'Unique ULID identifier for the operation';
COMMENT ON COLUMN core.operations_log.operation_type IS 'Type of operation: stage (acquisition), replay (interpretation), archive (retraction), restore (recovery), curate (surgical editing)';
COMMENT ON COLUMN core.operations_log.status IS 'Current status of the operation: started, completed, or failed';
COMMENT ON COLUMN core.operations_log.started_at IS 'Timestamp when the operation was initiated';
COMMENT ON COLUMN core.operations_log.completed_at IS 'Timestamp when the operation finished (success or failure)';
COMMENT ON COLUMN core.operations_log.duration_ms IS 'Operation duration in milliseconds, automatically calculated on completion';
COMMENT ON COLUMN core.operations_log.invoked_by_user IS 'System user who initiated the operation';
COMMENT ON COLUMN core.operations_log.parameters IS 'JSON object containing the exact command parameters and flags used';
COMMENT ON COLUMN core.operations_log.summary IS 'JSON object summarizing the operation outcome (events created/archived, errors, statistics)';

COMMENT ON FUNCTION core.start_operation(TEXT, TEXT, JSONB) IS 'Creates a new operation record in started status and returns the operation ID';
COMMENT ON FUNCTION core.complete_operation(ULID, JSONB) IS 'Marks an operation as completed with optional summary data';
COMMENT ON FUNCTION core.fail_operation(ULID, JSONB) IS 'Marks an operation as failed with optional error summary data';
COMMENT ON FUNCTION core.calculate_operation_duration() IS 'Trigger function that automatically calculates duration_ms when completed_at is set';
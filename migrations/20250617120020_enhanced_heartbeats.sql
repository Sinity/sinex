-- Enhanced heartbeat tracking for unified health monitoring
-- This replaces state files with comprehensive heartbeat events

CREATE TABLE IF NOT EXISTS component_heartbeats (
    id ULID PRIMARY KEY,
    component_name TEXT NOT NULL,
    timestamp TIMESTAMPTZ DEFAULT NOW(),
    status TEXT NOT NULL CHECK (status IN ('healthy', 'degraded', 'failed')),
    
    -- Basic system metrics
    uptime_seconds BIGINT,
    memory_usage_mb INTEGER,
    cpu_usage_percent FLOAT,
    
    -- Component-specific metrics  
    events_processed_last_minute INTEGER DEFAULT 0,
    errors_last_hour INTEGER DEFAULT 0,
    last_error_message TEXT,
    
    -- Version tracking
    binary_version TEXT,
    git_hash TEXT,
    build_time TEXT,
    
    -- Extensible metrics storage
    metrics JSONB DEFAULT '{}'::jsonb
);

-- Performance indexes for heartbeat queries
CREATE INDEX IF NOT EXISTS idx_heartbeats_component_time 
ON component_heartbeats (component_name, timestamp DESC);

-- Removed time-based partial indexes due to NOW() not being immutable
-- These can be added manually if needed with specific timestamps
CREATE INDEX IF NOT EXISTS idx_heartbeats_status
ON component_heartbeats (status, timestamp DESC);

-- View for latest component status (most recent heartbeat per component)
CREATE OR REPLACE VIEW latest_component_health AS
SELECT DISTINCT ON (component_name)
    component_name,
    timestamp,
    status,
    uptime_seconds,
    memory_usage_mb,
    cpu_usage_percent,
    events_processed_last_minute,
    errors_last_hour,
    last_error_message,
    binary_version,
    git_hash,
    build_time,
    metrics
FROM component_heartbeats
ORDER BY component_name, timestamp DESC;

-- Function to get overall system health status
CREATE OR REPLACE FUNCTION get_system_health_status()
RETURNS TABLE (
    overall_status TEXT,
    healthy_components INTEGER,
    degraded_components INTEGER,
    failed_components INTEGER,
    total_components INTEGER,
    last_updated TIMESTAMPTZ
) 
LANGUAGE plpgsql
AS $$
DECLARE
    cutoff_time TIMESTAMPTZ := NOW() - INTERVAL '3 minutes';
    healthy_count INTEGER := 0;
    degraded_count INTEGER := 0;
    failed_count INTEGER := 0;
    total_count INTEGER := 0;
    overall_status_result TEXT;
BEGIN
    -- Count components by status (only recent heartbeats)
    SELECT 
        COUNT(*) FILTER (WHERE status = 'healthy'),
        COUNT(*) FILTER (WHERE status = 'degraded'), 
        COUNT(*) FILTER (WHERE status = 'failed'),
        COUNT(*)
    INTO healthy_count, degraded_count, failed_count, total_count
    FROM latest_component_health
    WHERE timestamp > cutoff_time;
    
    -- Determine overall status
    IF total_count = 0 THEN
        overall_status_result := 'unknown';
    ELSIF failed_count > 0 THEN
        overall_status_result := 'failed';
    ELSIF degraded_count > 0 THEN
        overall_status_result := 'degraded';
    ELSE
        overall_status_result := 'healthy';
    END IF;
    
    RETURN QUERY SELECT 
        overall_status_result,
        healthy_count,
        degraded_count, 
        failed_count,
        total_count,
        NOW();
END;
$$;

-- Cleanup function for old heartbeat data
CREATE OR REPLACE FUNCTION cleanup_old_heartbeats(retention_days INTEGER DEFAULT 7)
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
    deleted_count INTEGER;
    cutoff_time TIMESTAMPTZ := NOW() - (retention_days || ' days')::INTERVAL;
BEGIN
    DELETE FROM component_heartbeats 
    WHERE timestamp < cutoff_time;
    
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    
    RETURN deleted_count;
END;
$$;

-- Comment on the table for documentation
COMMENT ON TABLE component_heartbeats IS 
'Component health heartbeats replacing state files for unified health monitoring. Contains system metrics, version info, and component-specific data.';

COMMENT ON VIEW latest_component_health IS
'Latest heartbeat for each component, used for current system health assessment.';

COMMENT ON FUNCTION get_system_health_status() IS
'Returns aggregated system health status based on recent component heartbeats.';

COMMENT ON FUNCTION cleanup_old_heartbeats(INTEGER) IS
'Removes heartbeat records older than specified retention period. Default is 7 days.';
-- Multi-location storage tracking for Git-annex
-- This migration adds tables to track storage locations, health metrics, and sync status

-- Storage locations configuration
CREATE TABLE IF NOT EXISTS sinex_schemas.storage_locations (
    id TEXT PRIMARY KEY,
    description TEXT NOT NULL,
    remote_name TEXT NOT NULL UNIQUE,
    url TEXT NOT NULL,
    priority INTEGER NOT NULL CHECK (priority >= 1 AND priority <= 10),
    max_capacity_gb BIGINT,
    cost INTEGER NOT NULL DEFAULT 100 CHECK (cost >= 0 AND cost <= 1000),
    enabled BOOLEAN NOT NULL DEFAULT true,
    auto_sync BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Storage location health status
CREATE TABLE IF NOT EXISTS sinex_schemas.location_status (
    location_id TEXT PRIMARY KEY REFERENCES sinex_schemas.storage_locations(id) ON DELETE CASCADE,
    is_available BOOLEAN NOT NULL DEFAULT false,
    last_seen TIMESTAMPTZ,
    last_sync TIMESTAMPTZ,
    disk_usage_gb DOUBLE PRECISION,
    file_count BIGINT,
    health_score REAL NOT NULL DEFAULT 0.0 CHECK (health_score >= 0.0 AND health_score <= 1.0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Sync error tracking
CREATE TABLE IF NOT EXISTS sinex_schemas.sync_errors (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    location_id TEXT NOT NULL REFERENCES sinex_schemas.storage_locations(id) ON DELETE CASCADE,
    error_type TEXT NOT NULL,
    message TEXT NOT NULL,
    retry_count INTEGER NOT NULL DEFAULT 0,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Health alerts
CREATE TABLE IF NOT EXISTS sinex_schemas.health_alerts (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    alert_type TEXT NOT NULL,
    location_id TEXT REFERENCES sinex_schemas.storage_locations(id) ON DELETE CASCADE,
    message TEXT NOT NULL,
    severity TEXT NOT NULL,
    auto_resolved BOOLEAN NOT NULL DEFAULT false,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    resolved_at TIMESTAMPTZ
);

-- Storage metrics history
CREATE TABLE IF NOT EXISTS sinex_schemas.storage_metrics (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    total_locations INTEGER NOT NULL,
    available_locations INTEGER NOT NULL,
    healthy_locations INTEGER NOT NULL,
    total_capacity_gb DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    used_capacity_gb DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    replication_factor REAL NOT NULL DEFAULT 0.0,
    avg_health_score REAL NOT NULL DEFAULT 0.0,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_location_status_updated_at ON sinex_schemas.location_status(updated_at);
CREATE INDEX IF NOT EXISTS idx_sync_errors_location_timestamp ON sinex_schemas.sync_errors(location_id, timestamp);
CREATE INDEX IF NOT EXISTS idx_sync_errors_timestamp ON sinex_schemas.sync_errors(timestamp);
CREATE INDEX IF NOT EXISTS idx_health_alerts_severity ON sinex_schemas.health_alerts(severity, auto_resolved);
CREATE INDEX IF NOT EXISTS idx_health_alerts_location ON sinex_schemas.health_alerts(location_id, created_at);
CREATE INDEX IF NOT EXISTS idx_storage_metrics_recorded_at ON sinex_schemas.storage_metrics(recorded_at);

-- Create hypertable for storage metrics (time-series data)
SELECT create_hypertable('sinex_schemas.storage_metrics', 'recorded_at', if_not_exists => TRUE);
SELECT create_hypertable('sinex_schemas.sync_errors', 'timestamp', if_not_exists => TRUE);

-- Set up data retention policies
SELECT add_retention_policy('sinex_schemas.storage_metrics', INTERVAL '90 days', if_not_exists => TRUE);
SELECT add_retention_policy('sinex_schemas.sync_errors', INTERVAL '30 days', if_not_exists => TRUE);

-- Trigger to update location_status.updated_at
CREATE OR REPLACE FUNCTION update_location_status_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_update_location_status_timestamp ON sinex_schemas.location_status;
CREATE TRIGGER trigger_update_location_status_timestamp
    BEFORE UPDATE ON sinex_schemas.location_status
    FOR EACH ROW
    EXECUTE FUNCTION update_location_status_timestamp();

-- Trigger to update storage_locations.updated_at
DROP TRIGGER IF EXISTS trigger_update_storage_locations_timestamp ON sinex_schemas.storage_locations;
CREATE TRIGGER trigger_update_storage_locations_timestamp
    BEFORE UPDATE ON sinex_schemas.storage_locations
    FOR EACH ROW
    EXECUTE FUNCTION update_location_status_timestamp();

-- Function to clean up old resolved alerts
CREATE OR REPLACE FUNCTION cleanup_old_health_alerts()
RETURNS INTEGER AS $$
DECLARE
    deleted_count INTEGER;
BEGIN
    DELETE FROM sinex_schemas.health_alerts
    WHERE auto_resolved = TRUE 
      AND resolved_at < NOW() - INTERVAL '48 hours';
    
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;

-- Function to get current storage health summary
CREATE OR REPLACE FUNCTION get_storage_health_summary()
RETURNS TABLE(
    total_locations BIGINT,
    available_locations BIGINT,
    healthy_locations BIGINT,
    avg_health_score DOUBLE PRECISION,
    critical_alerts BIGINT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        COUNT(*)::BIGINT,
        COUNT(*) FILTER (WHERE ls.is_available = TRUE)::BIGINT,
        COUNT(*) FILTER (WHERE ls.health_score > 0.7)::BIGINT,
        COALESCE(AVG(ls.health_score) FILTER (WHERE ls.is_available = TRUE), 0.0),
        (SELECT COUNT(*)::BIGINT FROM sinex_schemas.health_alerts 
         WHERE auto_resolved = FALSE AND severity IN ('Critical', 'Emergency'))
    FROM sinex_schemas.storage_locations sl
    LEFT JOIN sinex_schemas.location_status ls ON sl.id = ls.location_id
    WHERE sl.enabled = TRUE;
END;
$$ LANGUAGE plpgsql;

-- Comments for documentation
COMMENT ON TABLE sinex_schemas.storage_locations IS 'Configuration for Git-annex storage locations';
COMMENT ON TABLE sinex_schemas.location_status IS 'Current health and status of each storage location';
COMMENT ON TABLE sinex_schemas.sync_errors IS 'Historical record of synchronization errors';
COMMENT ON TABLE sinex_schemas.health_alerts IS 'Active and resolved health alerts for storage system';
COMMENT ON TABLE sinex_schemas.storage_metrics IS 'Time-series storage system health metrics';

COMMENT ON FUNCTION get_storage_health_summary() IS 'Returns current storage system health summary';
COMMENT ON FUNCTION cleanup_old_health_alerts() IS 'Removes old resolved health alerts to prevent table bloat';
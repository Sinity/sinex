-- Create satellite coordination tables
-- Migration: 00000000000008_create_coordination_tables.sql

-- Table for tracking all satellite instances
CREATE TABLE IF NOT EXISTS core.satellite_instances (
    instance_id UUID PRIMARY KEY,
    service_name TEXT NOT NULL,
    version TEXT NOT NULL,
    start_time TIMESTAMPTZ NOT NULL,
    last_heartbeat TIMESTAMPTZ NOT NULL,
    host_name TEXT NOT NULL,
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Table for inter-satellite signaling
CREATE TABLE IF NOT EXISTS core.satellite_signals (
    id SERIAL PRIMARY KEY,
    target_instance TEXT NOT NULL, -- instance_id or 'ALL'
    signal_type TEXT NOT NULL,     -- 'handoff_request', 'leader_failure', 'handoff_ready'
    message TEXT,
    payload JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    processed_at TIMESTAMPTZ,
    processed_by UUID REFERENCES core.satellite_instances(instance_id)
);

-- Table for tracking current service leadership
CREATE TABLE IF NOT EXISTS core.service_leadership (
    service_name TEXT PRIMARY KEY,
    instance_id UUID NOT NULL REFERENCES core.satellite_instances(instance_id),
    acquired_at TIMESTAMPTZ NOT NULL,
    last_heartbeat TIMESTAMPTZ NOT NULL,
    version TEXT NOT NULL,
    metadata JSONB DEFAULT '{}'::jsonb
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_satellite_instances_service_version 
    ON core.satellite_instances(service_name, version DESC, start_time ASC);

CREATE INDEX IF NOT EXISTS idx_satellite_signals_target_unprocessed 
    ON core.satellite_signals(target_instance, created_at) 
    WHERE processed_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_service_leadership_heartbeat 
    ON core.service_leadership(last_heartbeat);

-- Function to cleanup old satellite instances (older than 24 hours)
CREATE OR REPLACE FUNCTION core.cleanup_old_satellite_instances()
RETURNS INTEGER AS $$
DECLARE
    deleted_count INTEGER;
BEGIN
    DELETE FROM core.satellite_instances 
    WHERE last_heartbeat < NOW() - INTERVAL '24 hours';
    
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;

-- Function to cleanup processed signals (older than 1 hour)
CREATE OR REPLACE FUNCTION core.cleanup_processed_signals()
RETURNS INTEGER AS $$
DECLARE
    deleted_count INTEGER;
BEGIN
    DELETE FROM core.satellite_signals 
    WHERE processed_at IS NOT NULL 
      AND processed_at < NOW() - INTERVAL '1 hour';
    
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;
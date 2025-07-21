-- Create analytics views and regular materialized views for metrics
-- Note: Continuous aggregates don't support custom partitioning functions,
-- so we use regular materialized views with refresh functions instead

-- Event count by type and time bucket
CREATE MATERIALIZED VIEW metrics.event_counts_by_type_1h AS
SELECT 
    time_bucket('1 hour', ts_ingest) AS bucket,
    source,
    event_type,
    COUNT(*) as event_count,
    COUNT(DISTINCT host) as unique_hosts
FROM core.events
GROUP BY bucket, source, event_type
WITH NO DATA;

-- Process heartbeat analysis
CREATE MATERIALIZED VIEW metrics.process_heartbeats_1h AS
SELECT 
    time_bucket('1 hour', ts_ingest) AS bucket,
    source as process_name,
    host,
    COUNT(*) as heartbeat_count,
    AVG((payload->>'uptime_seconds')::numeric) as avg_uptime_seconds,
    MAX((payload->>'memory_mb')::numeric) as max_memory_mb
FROM core.events
WHERE event_type = 'process.heartbeat'
GROUP BY bucket, process_name, host
WITH NO DATA;

-- Create indexes for performance
CREATE INDEX idx_event_counts_bucket ON metrics.event_counts_by_type_1h (bucket DESC);
CREATE INDEX idx_event_counts_source ON metrics.event_counts_by_type_1h (source, bucket DESC);
CREATE INDEX idx_heartbeats_bucket ON metrics.process_heartbeats_1h (bucket DESC);
CREATE INDEX idx_heartbeats_process ON metrics.process_heartbeats_1h (process_name, bucket DESC);

-- Create refresh function for materialized views
CREATE OR REPLACE FUNCTION metrics.refresh_materialized_views() RETURNS void AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY metrics.event_counts_by_type_1h;
    REFRESH MATERIALIZED VIEW CONCURRENTLY metrics.process_heartbeats_1h;
END;
$$ LANGUAGE plpgsql;

-- Event processing lag analysis
CREATE VIEW metrics.event_processing_lag AS
SELECT 
    source,
    event_type,
    AVG(EXTRACT(EPOCH FROM (ts_ingest - ts_orig))) as avg_lag_seconds,
    MAX(EXTRACT(EPOCH FROM (ts_ingest - ts_orig))) as max_lag_seconds,
    MIN(EXTRACT(EPOCH FROM (ts_ingest - ts_orig))) as min_lag_seconds,
    COUNT(*) as event_count
FROM core.events
WHERE ts_orig IS NOT NULL
  AND ts_ingest >= NOW() - INTERVAL '24 hours'
GROUP BY source, event_type;

-- System health dashboard view
CREATE VIEW metrics.system_health AS
WITH recent_heartbeats AS (
    SELECT DISTINCT ON (source, host)
        source as process_name,
        host,
        ts_ingest as last_seen,
        (payload->>'status')::text as status,
        (payload->>'uptime_seconds')::numeric as uptime_seconds
    FROM core.events
    WHERE event_type = 'process.heartbeat'
      AND ts_ingest >= NOW() - INTERVAL '10 minutes'
    ORDER BY source, host, ts_ingest DESC
)
SELECT 
    process_name,
    host,
    last_seen,
    status,
    uptime_seconds,
    CASE 
        WHEN last_seen >= NOW() - INTERVAL '2 minutes' THEN 'healthy'
        WHEN last_seen >= NOW() - INTERVAL '5 minutes' THEN 'warning'
        ELSE 'critical'
    END as health_status
FROM recent_heartbeats;

-- Event throughput analysis
CREATE VIEW metrics.event_throughput AS
SELECT 
    date_trunc('minute', ts_ingest) as minute,
    source,
    COUNT(*) as events_per_minute,
    COUNT(DISTINCT event_type) as unique_event_types,
    pg_size_pretty(SUM(pg_column_size(payload))) as payload_size
FROM core.events
WHERE ts_ingest >= NOW() - INTERVAL '1 hour'
GROUP BY minute, source
ORDER BY minute DESC;

-- Add comments
COMMENT ON MATERIALIZED VIEW metrics.event_counts_by_type_1h IS 'Hourly aggregation of event counts by source and type';
COMMENT ON MATERIALIZED VIEW metrics.process_heartbeats_1h IS 'Hourly aggregation of process heartbeat metrics';
COMMENT ON FUNCTION metrics.refresh_materialized_views IS 'Refreshes all metrics materialized views concurrently';
COMMENT ON VIEW metrics.event_processing_lag IS 'Analysis of lag between event occurrence and ingestion';
COMMENT ON VIEW metrics.system_health IS 'Real-time system health status based on heartbeats';
COMMENT ON VIEW metrics.event_throughput IS 'Per-minute event throughput analysis';
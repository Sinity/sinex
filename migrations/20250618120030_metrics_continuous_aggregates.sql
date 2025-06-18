-- Create continuous aggregates for efficient metrics queries

-- First ensure hypertables exist for raw.events
-- (This should already be done, but being safe)
SELECT create_hypertable('raw.events', 'ts_ingest', if_not_exists => true);

-- Create a view for metrics events with parsed data
CREATE OR REPLACE VIEW metrics_events AS
SELECT 
    id,
    ts_ingest,
    ts_orig,
    host,
    payload->'interval_seconds' as interval_seconds,
    payload->'uptime_seconds' as uptime_seconds,
    payload->'summary' as summary,
    payload->'sources' as sources,
    payload->'timeseries' as timeseries,
    payload->'context' as context
FROM raw.events
WHERE source = 'sinex.metrics.collector';

-- 1-minute continuous aggregate for metrics summary
CREATE MATERIALIZED VIEW IF NOT EXISTS metrics_1min
WITH (timescaledb.continuous) AS
SELECT 
    time_bucket('1 minute', ts_ingest) AS bucket,
    host,
    AVG((summary->>'avg_cpu_percent')::float) as avg_cpu_percent,
    MAX((summary->>'max_memory_mb')::int) as max_memory_mb,
    AVG((summary->>'events_per_second')::float) as avg_events_per_second,
    MAX((summary->>'total_events')::bigint) as total_events,
    MAX((summary->>'total_errors')::bigint) as total_errors,
    COUNT(*) as sample_count
FROM metrics_events
GROUP BY bucket, host;

-- 5-minute continuous aggregate
CREATE MATERIALIZED VIEW IF NOT EXISTS metrics_5min
WITH (timescaledb.continuous) AS
SELECT 
    time_bucket('5 minutes', ts_ingest) AS bucket,
    host,
    AVG((summary->>'avg_cpu_percent')::float) as avg_cpu_percent,
    MAX((summary->>'max_memory_mb')::int) as max_memory_mb,
    AVG((summary->>'events_per_second')::float) as avg_events_per_second,
    MAX((summary->>'total_events')::bigint) as total_events,
    MAX((summary->>'total_errors')::bigint) as total_errors,
    COUNT(*) as sample_count
FROM metrics_events
GROUP BY bucket, host;

-- 1-hour continuous aggregate
CREATE MATERIALIZED VIEW IF NOT EXISTS metrics_1h
WITH (timescaledb.continuous) AS
SELECT 
    time_bucket('1 hour', ts_ingest) AS bucket,
    host,
    AVG((summary->>'avg_cpu_percent')::float) as avg_cpu_percent,
    MAX((summary->>'max_memory_mb')::int) as max_memory_mb,
    AVG((summary->>'events_per_second')::float) as avg_events_per_second,
    MAX((summary->>'total_events')::bigint) as total_events,
    MAX((summary->>'total_errors')::bigint) as total_errors,
    COUNT(*) as sample_count
FROM metrics_events
GROUP BY bucket, host;

-- Event count aggregates by source (non-metrics events)
CREATE MATERIALIZED VIEW IF NOT EXISTS event_counts_1min
WITH (timescaledb.continuous) AS
SELECT 
    time_bucket('1 minute', ts_ingest) AS bucket,
    source,
    event_type,
    COUNT(*) as event_count,
    AVG(EXTRACT(EPOCH FROM COALESCE(ts_ingest - ts_orig, INTERVAL '0'))) as avg_lag_seconds
FROM raw.events
WHERE source NOT LIKE 'sinex.metrics.%'
GROUP BY bucket, source, event_type;

-- 5-minute event aggregates
CREATE MATERIALIZED VIEW IF NOT EXISTS event_counts_5min
WITH (timescaledb.continuous) AS
SELECT 
    time_bucket('5 minutes', ts_ingest) AS bucket,
    source,
    event_type,
    COUNT(*) as event_count,
    AVG(EXTRACT(EPOCH FROM COALESCE(ts_ingest - ts_orig, INTERVAL '0'))) as avg_lag_seconds
FROM raw.events
WHERE source NOT LIKE 'sinex.metrics.%'
GROUP BY bucket, source, event_type;

-- Refresh policies for continuous aggregates
SELECT add_continuous_aggregate_policy('metrics_1min',
    start_offset => INTERVAL '2 hours',
    end_offset => INTERVAL '1 minute',
    schedule_interval => INTERVAL '1 minute');

SELECT add_continuous_aggregate_policy('metrics_5min',
    start_offset => INTERVAL '1 day',
    end_offset => INTERVAL '5 minutes',
    schedule_interval => INTERVAL '5 minutes');

SELECT add_continuous_aggregate_policy('metrics_1h',
    start_offset => INTERVAL '7 days',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour');

SELECT add_continuous_aggregate_policy('event_counts_1min',
    start_offset => INTERVAL '2 hours',
    end_offset => INTERVAL '1 minute',
    schedule_interval => INTERVAL '1 minute');

SELECT add_continuous_aggregate_policy('event_counts_5min',
    start_offset => INTERVAL '1 day',
    end_offset => INTERVAL '5 minutes',
    schedule_interval => INTERVAL '5 minutes');

-- Retention policies for CONTINUOUS AGGREGATES ONLY
-- These remove old data from the aggregated views, NOT from raw.events
-- Raw events are preserved indefinitely (or according to separate retention policy)
SELECT add_retention_policy('metrics_1min', INTERVAL '30 days');
SELECT add_retention_policy('metrics_5min', INTERVAL '180 days');  -- 6 months
SELECT add_retention_policy('metrics_1h', INTERVAL '5 years');
SELECT add_retention_policy('event_counts_1min', INTERVAL '14 days');
SELECT add_retention_policy('event_counts_5min', INTERVAL '90 days');

-- Note: raw.events retention is managed separately and should be much longer
-- or indefinite for a personal exocortex system. To add retention to raw events:
-- SELECT add_retention_policy('raw.events', INTERVAL '10 years');
-- But for personal data, you probably want to keep events forever!

-- Helper function to extract time series data from metrics
CREATE OR REPLACE FUNCTION extract_metrics_timeseries(
    start_time TIMESTAMPTZ DEFAULT NOW() - INTERVAL '1 hour',
    end_time TIMESTAMPTZ DEFAULT NOW()
)
RETURNS TABLE (
    time TIMESTAMPTZ,
    host TEXT,
    cpu_percent FLOAT,
    memory_mb INT,
    events_count BIGINT,
    errors_count BIGINT,
    queue_depth INT,
    active_sources INT,
    db_pool_size INT,
    db_pool_idle INT
)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    WITH metrics_data AS (
        SELECT 
            ts_ingest,
            host as metric_host,
            jsonb_array_elements(timeseries->'datapoints') as datapoint
        FROM metrics_events
        WHERE ts_ingest BETWEEN start_time AND end_time
    )
    SELECT 
        (datapoint->>'ts')::timestamptz,
        metric_host,
        (datapoint->>'cpu')::float,
        (datapoint->>'mem')::int,
        (datapoint->>'events')::bigint,
        (datapoint->>'errors')::bigint,
        (datapoint->>'queue')::int,
        (datapoint->>'sources')::int,
        (datapoint->>'db_pool')::int,
        (datapoint->>'db_idle')::int
    FROM metrics_data
    ORDER BY time;
END;
$$;

-- Comments for documentation
COMMENT ON VIEW metrics_events IS 'Parsed metrics events from the collector with JSON fields extracted';
COMMENT ON MATERIALIZED VIEW metrics_1min IS 'One-minute aggregated metrics for efficient querying';
COMMENT ON MATERIALIZED VIEW metrics_5min IS 'Five-minute aggregated metrics for medium-term analysis';
COMMENT ON MATERIALIZED VIEW metrics_1h IS 'Hourly aggregated metrics for long-term trends';
COMMENT ON MATERIALIZED VIEW event_counts_1min IS 'One-minute event counts by source and type';
COMMENT ON MATERIALIZED VIEW event_counts_5min IS 'Five-minute event counts for trend analysis';
COMMENT ON FUNCTION extract_metrics_timeseries IS 'Extract full-resolution time series data from metrics events';
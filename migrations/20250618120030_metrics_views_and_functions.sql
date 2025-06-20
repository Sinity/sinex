-- Create views and functions for metrics (transactional part)

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

-- Continuous aggregates are created in a separate migration file

-- See next migration file for continuous aggregates





-- Helper function to extract time series data from metrics
CREATE OR REPLACE FUNCTION extract_metrics_timeseries(
    start_time TIMESTAMPTZ DEFAULT NOW() - INTERVAL '1 hour',
    end_time TIMESTAMPTZ DEFAULT NOW()
)
RETURNS TABLE (
    ts TIMESTAMPTZ,
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
    ORDER BY 1;
END;
$$;

-- Comments for documentation
COMMENT ON VIEW metrics_events IS 'Parsed metrics events from the collector with JSON fields extracted';
COMMENT ON FUNCTION extract_metrics_timeseries IS 'Extract full-resolution time series data from metrics events';
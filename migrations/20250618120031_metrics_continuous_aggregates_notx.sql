-- TimescaleDB continuous aggregates don't work with custom ULID partitioning
-- Use regular materialized views instead - these are fast enough for personal-scale data
-- and can be refreshed periodically

-- 1-minute metrics aggregation (regular materialized view)
CREATE MATERIALIZED VIEW IF NOT EXISTS metrics_1min AS
SELECT 
    date_trunc('minute', ts_ingest) AS bucket,
    host,
    AVG((payload->'summary'->>'avg_cpu_percent')::float) as avg_cpu_percent,
    MAX((payload->'summary'->>'max_memory_mb')::int) as max_memory_mb,
    AVG((payload->'summary'->>'events_per_second')::float) as avg_events_per_second,
    MAX((payload->'summary'->>'total_events')::bigint) as total_events,
    MAX((payload->'summary'->>'total_errors')::bigint) as total_errors,
    COUNT(*) as sample_count
FROM raw.events
WHERE source = 'sinex.metrics.collector'
  AND ts_ingest > NOW() - INTERVAL '7 days'  -- Keep last week in view
GROUP BY bucket, host;

-- 5-minute metrics aggregation (regular materialized view)
CREATE MATERIALIZED VIEW IF NOT EXISTS metrics_5min AS
SELECT 
    date_trunc('hour', ts_ingest) + 
      (EXTRACT(minute FROM ts_ingest)::int / 5) * INTERVAL '5 minutes' AS bucket,
    host,
    AVG((payload->'summary'->>'avg_cpu_percent')::float) as avg_cpu_percent,
    MAX((payload->'summary'->>'max_memory_mb')::int) as max_memory_mb,
    AVG((payload->'summary'->>'events_per_second')::float) as avg_events_per_second,
    MAX((payload->'summary'->>'total_events')::bigint) as total_events,
    MAX((payload->'summary'->>'total_errors')::bigint) as total_errors,
    COUNT(*) as sample_count
FROM raw.events
WHERE source = 'sinex.metrics.collector'
  AND ts_ingest > NOW() - INTERVAL '30 days'  -- Keep last month in view
GROUP BY bucket, host;

-- 1-hour metrics aggregation (regular materialized view)
CREATE MATERIALIZED VIEW IF NOT EXISTS metrics_1h AS
SELECT 
    date_trunc('hour', ts_ingest) AS bucket,
    host,
    AVG((payload->'summary'->>'avg_cpu_percent')::float) as avg_cpu_percent,
    MAX((payload->'summary'->>'max_memory_mb')::int) as max_memory_mb,
    AVG((payload->'summary'->>'events_per_second')::float) as avg_events_per_second,
    MAX((payload->'summary'->>'total_events')::bigint) as total_events,
    MAX((payload->'summary'->>'total_errors')::bigint) as total_errors,
    COUNT(*) as sample_count
FROM raw.events
WHERE source = 'sinex.metrics.collector'
  AND ts_ingest > NOW() - INTERVAL '1 year'  -- Keep last year in view
GROUP BY bucket, host;

-- Event count aggregates by source (non-metrics events)
CREATE MATERIALIZED VIEW IF NOT EXISTS event_counts_1min AS
SELECT 
    date_trunc('minute', ts_ingest) AS bucket,
    source,
    event_type,
    COUNT(*) as event_count,
    AVG(EXTRACT(EPOCH FROM COALESCE(ts_ingest - ts_orig, INTERVAL '0'))) as avg_lag_seconds
FROM raw.events
WHERE source NOT LIKE 'sinex.metrics.%'
  AND ts_ingest > NOW() - INTERVAL '7 days'
GROUP BY bucket, source, event_type;

-- 5-minute event aggregates
CREATE MATERIALIZED VIEW IF NOT EXISTS event_counts_5min AS
SELECT 
    date_trunc('hour', ts_ingest) + 
      (EXTRACT(minute FROM ts_ingest)::int / 5) * INTERVAL '5 minutes' AS bucket,
    source,
    event_type,
    COUNT(*) as event_count,
    AVG(EXTRACT(EPOCH FROM COALESCE(ts_ingest - ts_orig, INTERVAL '0'))) as avg_lag_seconds
FROM raw.events
WHERE source NOT LIKE 'sinex.metrics.%'
  AND ts_ingest > NOW() - INTERVAL '30 days'
GROUP BY bucket, source, event_type;

-- Note: These are regular materialized views, not continuous aggregates
-- For automatic refresh, set up a periodic job:
-- CREATE OR REPLACE FUNCTION refresh_metrics_views() RETURNS void AS $$
-- BEGIN
--   REFRESH MATERIALIZED VIEW CONCURRENTLY metrics_1min;
--   REFRESH MATERIALIZED VIEW CONCURRENTLY metrics_5min;
--   REFRESH MATERIALIZED VIEW CONCURRENTLY metrics_1h;
--   REFRESH MATERIALIZED VIEW CONCURRENTLY event_counts_1min;
--   REFRESH MATERIALIZED VIEW CONCURRENTLY event_counts_5min;
-- END;
-- $$ LANGUAGE plpgsql;

-- Create indexes for concurrent refresh
CREATE UNIQUE INDEX IF NOT EXISTS metrics_1min_bucket_host_idx ON metrics_1min (bucket, host);
CREATE UNIQUE INDEX IF NOT EXISTS metrics_5min_bucket_host_idx ON metrics_5min (bucket, host);
CREATE UNIQUE INDEX IF NOT EXISTS metrics_1h_bucket_host_idx ON metrics_1h (bucket, host);
CREATE UNIQUE INDEX IF NOT EXISTS event_counts_1min_bucket_source_type_idx ON event_counts_1min (bucket, source, event_type);
CREATE UNIQUE INDEX IF NOT EXISTS event_counts_5min_bucket_source_type_idx ON event_counts_5min (bucket, source, event_type);

-- Comments for documentation
COMMENT ON MATERIALIZED VIEW metrics_1min IS 'One-minute aggregated metrics for efficient querying';
COMMENT ON MATERIALIZED VIEW metrics_5min IS 'Five-minute aggregated metrics for medium-term analysis';
COMMENT ON MATERIALIZED VIEW metrics_1h IS 'Hourly aggregated metrics for long-term trends';
COMMENT ON MATERIALIZED VIEW event_counts_1min IS 'One-minute event counts by source and type';
COMMENT ON MATERIALIZED VIEW event_counts_5min IS 'Five-minute event counts for trend analysis';
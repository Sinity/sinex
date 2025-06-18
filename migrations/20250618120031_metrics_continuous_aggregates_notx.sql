-- Continuous aggregates disabled for now due to TimescaleDB compatibility issues
-- Will be created manually after deployment if needed

-- To create manually:
-- CREATE MATERIALIZED VIEW metrics_1min WITH (timescaledb.continuous) AS ...

-- For now, we'll use regular views for metrics aggregation
-- These are fast enough for personal-scale data

-- Simple 1-minute metrics view
CREATE OR REPLACE VIEW metrics_1min_view AS
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
  AND ts_ingest > NOW() - INTERVAL '1 day'
GROUP BY bucket, host;

-- Simple event counts view
CREATE OR REPLACE VIEW event_counts_1min_view AS
SELECT 
    date_trunc('minute', ts_ingest) AS bucket,
    source,
    event_type,
    COUNT(*) as event_count,
    AVG(EXTRACT(EPOCH FROM COALESCE(ts_ingest - ts_orig, INTERVAL '0'))) as avg_lag_seconds
FROM raw.events
WHERE ts_ingest > NOW() - INTERVAL '1 day'
  AND source NOT LIKE 'sinex.metrics.%'
GROUP BY bucket, source, event_type;

COMMENT ON VIEW metrics_1min_view IS 'Simple 1-minute metrics aggregation (not a continuous aggregate)';
COMMENT ON VIEW event_counts_1min_view IS 'Simple 1-minute event counts (not a continuous aggregate)';
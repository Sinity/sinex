-- no-transaction
-- Path A: Replace regular materialized views with continuous aggregates
-- Create separate metrics hypertable to work around ULID partitioning limitation

-- Drop existing regular materialized views
DROP MATERIALIZED VIEW IF EXISTS metrics_1min CASCADE;
DROP MATERIALIZED VIEW IF EXISTS metrics_5min CASCADE;
DROP MATERIALIZED VIEW IF EXISTS metrics_1h CASCADE;
DROP MATERIALIZED VIEW IF EXISTS event_counts_1min CASCADE;
DROP MATERIALIZED VIEW IF EXISTS event_counts_5min CASCADE;

-- Create metrics schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS metrics;

-- Create metrics hypertable with standard time partitioning (no custom ULID partitioning)
-- This allows continuous aggregates to work properly
CREATE TABLE IF NOT EXISTS metrics.collector_events (LIKE raw.events INCLUDING DEFAULTS);

-- Convert to hypertable using standard timestamp partitioning
SELECT create_hypertable(
    'metrics.collector_events',
    'ts_ingest',
    if_not_exists => true,
    chunk_time_interval => INTERVAL '1 day'
);

-- Create trigger function to fan out metrics events
CREATE OR REPLACE FUNCTION metrics.fanout() RETURNS trigger 
LANGUAGE plpgsql AS $$
BEGIN
  -- Only copy metrics events to the metrics hypertable
  IF NEW.source = 'sinex.metrics.collector' THEN
    INSERT INTO metrics.collector_events VALUES (NEW.*);
  END IF;
  RETURN NEW;
END$$;

-- Create trigger on raw.events to copy metrics events
DROP TRIGGER IF EXISTS metrics_fanout ON raw.events;
CREATE TRIGGER metrics_fanout 
  AFTER INSERT ON raw.events
  FOR EACH ROW 
  EXECUTE FUNCTION metrics.fanout();

-- Backfill existing metrics events (one-time operation)
INSERT INTO metrics.collector_events 
SELECT * FROM raw.events 
WHERE source = 'sinex.metrics.collector'
ON CONFLICT DO NOTHING;

-- Now create continuous aggregates on the metrics hypertable
-- 1-minute metrics aggregation
CREATE MATERIALIZED VIEW metrics_1min
WITH (timescaledb.continuous) AS
SELECT 
    time_bucket('1 minute', ts_ingest) AS bucket,
    host,
    AVG((payload->'summary'->>'avg_cpu_percent')::float) as avg_cpu_percent,
    MAX((payload->'summary'->>'max_memory_mb')::int) as max_memory_mb,
    AVG((payload->'summary'->>'events_per_second')::float) as avg_events_per_second,
    MAX((payload->'summary'->>'total_events')::bigint) as total_events,
    MAX((payload->'summary'->>'total_errors')::bigint) as total_errors,
    COUNT(*) as sample_count
FROM metrics.collector_events
GROUP BY bucket, host
WITH NO DATA;

-- 5-minute metrics aggregation  
CREATE MATERIALIZED VIEW metrics_5min
WITH (timescaledb.continuous) AS
SELECT 
    time_bucket('5 minutes', ts_ingest) AS bucket,
    host,
    AVG((payload->'summary'->>'avg_cpu_percent')::float) as avg_cpu_percent,
    MAX((payload->'summary'->>'max_memory_mb')::int) as max_memory_mb,
    AVG((payload->'summary'->>'events_per_second')::float) as avg_events_per_second,
    MAX((payload->'summary'->>'total_events')::bigint) as total_events,
    MAX((payload->'summary'->>'total_errors')::bigint) as total_errors,
    COUNT(*) as sample_count
FROM metrics.collector_events
GROUP BY bucket, host
WITH NO DATA;

-- 1-hour metrics aggregation
CREATE MATERIALIZED VIEW metrics_1h
WITH (timescaledb.continuous) AS
SELECT 
    time_bucket('1 hour', ts_ingest) AS bucket,
    host,
    AVG((payload->'summary'->>'avg_cpu_percent')::float) as avg_cpu_percent,
    MAX((payload->'summary'->>'max_memory_mb')::int) as max_memory_mb,
    AVG((payload->'summary'->>'events_per_second')::float) as avg_events_per_second,
    MAX((payload->'summary'->>'total_events')::bigint) as total_events,
    MAX((payload->'summary'->>'total_errors')::bigint) as total_errors,
    COUNT(*) as sample_count
FROM metrics.collector_events
GROUP BY bucket, host
WITH NO DATA;

-- Set up refresh policies for continuous aggregates
-- Refresh every 5 minutes with some lag to ensure data completeness
SELECT add_continuous_aggregate_policy('metrics_1min',
    start_offset => INTERVAL '1 hour',
    end_offset => INTERVAL '1 minute',
    schedule_interval => INTERVAL '5 minutes',
    if_not_exists => true);

SELECT add_continuous_aggregate_policy('metrics_5min',
    start_offset => INTERVAL '6 hours', 
    end_offset => INTERVAL '5 minutes',
    schedule_interval => INTERVAL '15 minutes',
    if_not_exists => true);

SELECT add_continuous_aggregate_policy('metrics_1h',
    start_offset => INTERVAL '2 days',
    end_offset => INTERVAL '1 hour', 
    schedule_interval => INTERVAL '1 hour',
    if_not_exists => true);

-- Recreate regular materialized views for non-metrics event counts
-- (These can't be continuous aggregates since they read from ULID-partitioned table)
CREATE MATERIALIZED VIEW event_counts_1min AS
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

CREATE MATERIALIZED VIEW event_counts_5min AS
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

-- Create indexes for concurrent refresh of regular materialized views
CREATE UNIQUE INDEX event_counts_1min_bucket_source_type_idx 
  ON event_counts_1min (bucket, source, event_type);
CREATE UNIQUE INDEX event_counts_5min_bucket_source_type_idx 
  ON event_counts_5min (bucket, source, event_type);

-- Comments for documentation
COMMENT ON TABLE metrics.collector_events IS 'Dedicated hypertable for metrics events, enables continuous aggregates';
-- Continuous aggregates appear as views, not materialized views in pg_class
COMMENT ON VIEW metrics_1min IS 'Continuous aggregate: one-minute metrics with automatic refresh';
COMMENT ON VIEW metrics_5min IS 'Continuous aggregate: five-minute metrics with automatic refresh';
COMMENT ON VIEW metrics_1h IS 'Continuous aggregate: hourly metrics with automatic refresh';
COMMENT ON MATERIALIZED VIEW event_counts_1min IS 'Regular materialized view: one-minute event counts by source and type';
COMMENT ON MATERIALIZED VIEW event_counts_5min IS 'Regular materialized view: five-minute event counts for trend analysis';
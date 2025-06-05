-- Optimized ULID-only solution with generated timestamp column
-- This provides the best performance while keeping ULID as sole primary key

-- Ensure extensions are available
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Create function for TimescaleDB partitioning
CREATE OR REPLACE FUNCTION ulid_to_timestamptz(ulid_val ULID) 
RETURNS TIMESTAMPTZ AS $$
BEGIN
    RETURN ulid_val::timestamp;
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE;

-- Update the events table to add generated timestamp column
-- This assumes the table already exists from previous migrations
DO $$
BEGIN
    -- Check if ts_computed column already exists
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_schema = 'raw' 
        AND table_name = 'events' 
        AND column_name = 'ts_computed'
    ) THEN
        -- Add the generated column
        ALTER TABLE raw.events 
        ADD COLUMN ts_computed TIMESTAMPTZ 
        GENERATED ALWAYS AS (id::timestamp) STORED;
        
        RAISE NOTICE 'Added ts_computed generated column';
    END IF;
    
    -- Check if already a hypertable
    IF NOT EXISTS (
        SELECT 1 FROM _timescaledb_catalog.hypertable 
        WHERE table_name = 'events' AND schema_name = 'raw'
    ) THEN
        -- Convert to hypertable with ULID partitioning
        PERFORM create_hypertable(
            'raw.events',
            by_range('id', 
                partition_func => 'ulid_to_timestamptz',
                partition_interval => INTERVAL '1 week'  -- Weekly chunks to reduce fragmentation
            ),
            migrate_data => TRUE,
            create_default_indexes => FALSE
        );
        
        RAISE NOTICE 'Created hypertable with ULID partitioning';
    END IF;
END $$;

-- Create optimized indexes
-- These use the generated column for fast time-based queries
CREATE INDEX IF NOT EXISTS idx_raw_events_ts_computed 
    ON raw.events (ts_computed);

CREATE INDEX IF NOT EXISTS idx_raw_events_source_ts 
    ON raw.events (source, ts_computed);

CREATE INDEX IF NOT EXISTS idx_raw_events_host_ts 
    ON raw.events (host, ts_computed);

CREATE INDEX IF NOT EXISTS idx_raw_events_hour 
    ON raw.events (date_trunc('hour', ts_computed));

-- Keep existing indexes that don't depend on timestamp
CREATE INDEX IF NOT EXISTS idx_raw_events_source_type 
    ON raw.events (source, event_type);

CREATE INDEX IF NOT EXISTS idx_raw_events_ts_orig_desc 
    ON raw.events (ts_orig DESC NULLS LAST);

CREATE INDEX IF NOT EXISTS idx_raw_events_payload_schema_id 
    ON raw.events (payload_schema_id) 
    WHERE payload_schema_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_raw_events_payload_gin 
    ON raw.events USING GIN (payload jsonb_path_ops);

-- Set compression policy
SELECT add_compression_policy('raw.events', 
    compress_after => INTERVAL '30 days',
    if_not_exists => TRUE
);

-- Configure compression
ALTER TABLE raw.events SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'source,event_type',
    timescaledb.compress_orderby = 'id DESC'
);

-- Create continuous aggregate for common queries
CREATE MATERIALIZED VIEW IF NOT EXISTS raw.events_hourly
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', ts_computed) AS hour,
    source,
    event_type,
    COUNT(*) as event_count,
    COUNT(DISTINCT host) as unique_hosts,
    MIN(id) as first_event_id,
    MAX(id) as last_event_id
FROM raw.events
GROUP BY hour, source, event_type
WITH NO DATA;

-- Add refresh policy for continuous aggregate
SELECT add_continuous_aggregate_policy('raw.events_hourly',
    start_offset => INTERVAL '3 hours',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '30 minutes',
    if_not_exists => TRUE
);

-- Update table comment
COMMENT ON TABLE raw.events IS 
'Optimized hypertable with ULID-only primary key. Uses generated ts_computed column for fast time queries. Partitioned by ULID ranges.';

COMMENT ON COLUMN raw.events.ts_computed IS 
'Generated timestamp extracted from ULID. Used for fast time-based queries and indexes.';
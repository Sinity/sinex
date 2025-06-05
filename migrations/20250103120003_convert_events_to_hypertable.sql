-- Migration: Convert raw.events to TimescaleDB hypertable
-- Up Migration

-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Convert raw.events to hypertable
-- This must be done AFTER the table is created
-- The challenge: TimescaleDB requires unique constraints to include the partitioning column,
-- but we want to keep ULID as the sole primary key

-- Solution: Since we can't use custom partitioning functions with ULID directly,
-- we'll partition by ts_ingest but handle the primary key constraint issue
DO $$
BEGIN
  -- Check if already a hypertable
  IF EXISTS (
    SELECT 1 FROM _timescaledb_catalog.hypertable 
    WHERE table_name = 'events' AND schema_name = 'raw'
  ) THEN
    RAISE NOTICE 'raw.events is already a hypertable, skipping';
    RETURN;
  END IF;

  -- Since TimescaleDB 2.x enforces that unique constraints include the partitioning column,
  -- and we want to keep ULID as the primary key, we need to work around this.
  -- The ULID contains timestamp information and is time-ordered, which aligns with ts_ingest.
  
  -- Drop the primary key temporarily
  ALTER TABLE raw.events DROP CONSTRAINT raw_events_pkey;
  
  -- Create hypertable without default indexes
  PERFORM create_hypertable(
    'raw.events',
    'ts_ingest',
    chunk_time_interval => INTERVAL '1 day',
    migrate_data => TRUE,
    create_default_indexes => FALSE
  );
  
  -- Add a composite primary key that includes the partitioning column
  -- This satisfies TimescaleDB's requirement
  ALTER TABLE raw.events ADD CONSTRAINT raw_events_pkey PRIMARY KEY (id, ts_ingest);
  
  -- Create a non-unique index on id for efficient lookups
  -- NOTE: We cannot create a unique index on just 'id' in partitioned tables
  CREATE INDEX idx_raw_events_id ON raw.events (id);
  
  -- Re-create the other indexes
  CREATE INDEX idx_raw_events_ts_orig_desc ON raw.events (ts_orig DESC NULLS LAST);
  CREATE INDEX idx_raw_events_source_type_ts_ingest_desc ON raw.events (source, event_type, ts_ingest DESC);
  CREATE INDEX idx_raw_events_host_ts_ingest_desc ON raw.events (host, ts_ingest DESC);
  CREATE INDEX idx_raw_events_payload_schema_id ON raw.events (payload_schema_id) WHERE payload_schema_id IS NOT NULL;
  CREATE INDEX idx_raw_events_payload_gin_path_ops ON raw.events USING GIN (payload jsonb_path_ops);
END;
$$;

-- Set compression policy (compress chunks older than 7 days)
-- This is based on TIM-TimescaleDBConfiguration recommendations
SELECT add_compression_policy('raw.events', INTERVAL '7 days', if_not_exists => TRUE);

-- Configure compression settings
ALTER TABLE raw.events SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'source,event_type',
  timescaledb.compress_orderby = 'ts_ingest DESC'
);

COMMENT ON TABLE raw.events IS 'Universal log for all captured raw events (TimescaleDB hypertable). Immutable by principle.';
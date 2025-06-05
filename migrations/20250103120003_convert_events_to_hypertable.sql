-- Migration: Convert raw.events to TimescaleDB hypertable
-- Up Migration

-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Convert raw.events to hypertable
-- This partitions by ULID using a custom function, keeping ULID as sole primary key
-- The ts_ingest GENERATED column is used for fast queries, not partitioning

-- Create function for ULID to timestamp conversion for partitioning
CREATE OR REPLACE FUNCTION ulid_to_timestamptz(ulid_val ULID) 
RETURNS TIMESTAMPTZ AS $$
BEGIN
    RETURN ulid_val::timestamp;
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION ulid_to_timestamptz(ULID) IS 'Extracts timestamp from ULID for TimescaleDB partitioning';

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

  -- Create hypertable partitioned by ULID using custom time extraction
  PERFORM create_hypertable(
    'raw.events',
    by_range('id', partition_func => 'ulid_to_timestamptz'::regproc)
  );
  
  -- The primary key remains as just 'id' (ULID) - no composite key needed!
  
  -- Create indexes on ts_ingest (GENERATED column) for fast time-based queries
  CREATE INDEX IF NOT EXISTS idx_raw_events_ts_ingest ON raw.events (ts_ingest DESC);
  
  -- Composite indexes for efficient filtering
  CREATE INDEX IF NOT EXISTS idx_raw_events_source_ts ON raw.events (source, ts_ingest DESC);
  CREATE INDEX IF NOT EXISTS idx_raw_events_host_ts ON raw.events (host, ts_ingest DESC);
  CREATE INDEX IF NOT EXISTS idx_raw_events_source_type_ts ON raw.events (source, event_type, ts_ingest DESC);
  
  -- Keep existing non-time indexes if they don't exist
  CREATE INDEX IF NOT EXISTS idx_raw_events_ts_orig_desc ON raw.events (ts_orig DESC NULLS LAST);
  CREATE INDEX IF NOT EXISTS idx_raw_events_payload_schema_id ON raw.events (payload_schema_id) WHERE payload_schema_id IS NOT NULL;
  CREATE INDEX IF NOT EXISTS idx_raw_events_payload_gin_path_ops ON raw.events USING GIN (payload jsonb_path_ops);
END;
$$;

-- Note: Compression configuration moved to a separate migration
-- to avoid issues during initial setup

COMMENT ON TABLE raw.events IS 'TimescaleDB hypertable for event storage. Partitioned by ULID-extracted timestamp while maintaining ULID as sole primary key.';
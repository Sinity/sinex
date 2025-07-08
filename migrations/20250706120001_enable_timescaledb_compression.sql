-- Migration: Enable TimescaleDB compression for raw.events
-- This provides 90%+ storage reduction for event data
-- Up Migration

-- Enable compression on the raw.events hypertable
DO $$
BEGIN
  -- Check if compression is already enabled
  IF EXISTS (
    SELECT 1 FROM _timescaledb_catalog.hypertable 
    WHERE table_name = 'events' 
    AND schema_name = 'raw' 
    AND compression_state != 0
  ) THEN
    RAISE NOTICE 'Compression already enabled for raw.events, skipping';
    RETURN;
  END IF;

  -- Configure compression settings
  -- Segment by source to group similar events together
  -- Order by ts_ingest DESC for time-series access patterns
  ALTER TABLE raw.events SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'source',
    timescaledb.compress_orderby = 'ts_ingest DESC'
  );

  RAISE NOTICE 'Compression enabled for raw.events';
END;
$$;

-- Add compression policy to automatically compress chunks older than 7 days
-- This balances query performance (recent data uncompressed) with storage efficiency
DO $$
BEGIN
  -- Check if compression policy already exists
  IF EXISTS (
    SELECT 1 FROM _timescaledb_config.bgw_job j
    JOIN _timescaledb_catalog.hypertable h ON j.hypertable_id = h.id
    WHERE h.table_name = 'events' 
    AND h.schema_name = 'raw'
    AND j.proc_name = 'policy_compression'
  ) THEN
    RAISE NOTICE 'Compression policy already exists for raw.events, skipping';
    RETURN;
  END IF;

  -- Add policy to compress chunks older than 7 days
  PERFORM add_compression_policy('raw.events', INTERVAL '7 days');

  RAISE NOTICE 'Compression policy added: compress chunks older than 7 days';
END;
$$;

-- No retention policy - keep all event data forever for complete digital memory

-- Create a view to monitor compression status
CREATE OR REPLACE VIEW timescaledb_compression_stats AS
SELECT 
  chunk_name,
  range_start,
  range_end,
  is_compressed,
  pg_size_pretty(pg_total_relation_size(format('%I.%I', chunk_schema, chunk_name))) as chunk_size
FROM timescaledb_information.chunks
WHERE hypertable_name = 'events' AND hypertable_schema = 'raw'
ORDER BY chunk_creation_time DESC;

COMMENT ON VIEW timescaledb_compression_stats IS 'Monitor compression effectiveness for raw.events table';

-- Create a function to manually compress a specific chunk if needed
CREATE OR REPLACE FUNCTION compress_chunk_by_time(
  chunk_start TIMESTAMPTZ,
  chunk_end TIMESTAMPTZ DEFAULT NULL
) RETURNS TEXT AS $$
DECLARE
  chunk_full_name TEXT;
  result TEXT;
BEGIN
  -- Find chunk containing the specified time
  SELECT format('%I.%I', c.chunk_schema, c.chunk_name) INTO chunk_full_name
  FROM timescaledb_information.chunks c
  WHERE c.hypertable_name = 'events' 
    AND c.hypertable_schema = 'raw'
    AND c.chunk_creation_time >= chunk_start
    AND (chunk_end IS NULL OR c.chunk_creation_time <= chunk_end)
  LIMIT 1;
  
  IF chunk_full_name IS NULL THEN
    RETURN 'No chunk found for specified time range';
  END IF;
  
  -- Compress the chunk
  PERFORM compress_chunk(chunk_full_name);
  
  RETURN 'Compressed chunk: ' || chunk_full_name;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION compress_chunk_by_time(TIMESTAMPTZ, TIMESTAMPTZ) IS 'Manually compress a chunk containing events from specified time range';
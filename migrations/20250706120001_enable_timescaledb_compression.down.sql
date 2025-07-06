-- Migration: Disable TimescaleDB compression for raw.events
-- Down Migration

-- No retention policy to remove - data is kept forever

-- Remove compression policy if it exists
DO $$
DECLARE
  job_id INTEGER;
BEGIN
  SELECT j.id INTO job_id
  FROM _timescaledb_config.bgw_job j
  JOIN _timescaledb_catalog.hypertable h ON j.hypertable_id = h.id
  WHERE h.table_name = 'events' 
    AND h.schema_name = 'raw'
    AND j.proc_name = 'policy_compression';
    
  IF job_id IS NOT NULL THEN
    PERFORM remove_compression_policy('raw.events');
    RAISE NOTICE 'Compression policy removed from raw.events';
  END IF;
END;
$$;

-- Decompress all compressed chunks
DO $$
DECLARE
  chunk_record RECORD;
  chunk_full_name TEXT;
BEGIN
  FOR chunk_record IN 
    SELECT chunk_schema, chunk_name
    FROM timescaledb_information.chunks
    WHERE hypertable_name = 'events' 
      AND hypertable_schema = 'raw'
      AND is_compressed = true
  LOOP
    chunk_full_name := format('%I.%I', chunk_record.chunk_schema, chunk_record.chunk_name);
    PERFORM decompress_chunk(chunk_full_name);
    RAISE NOTICE 'Decompressed chunk: %', chunk_full_name;
  END LOOP;
END;
$$;

-- Disable compression on the hypertable
DO $$
BEGIN
  -- Check if compression is enabled
  IF EXISTS (
    SELECT 1 FROM _timescaledb_catalog.hypertable 
    WHERE table_name = 'events' 
    AND schema_name = 'raw' 
    AND compression_state != 0
  ) THEN
    ALTER TABLE raw.events SET (timescaledb.compress = false);
    RAISE NOTICE 'Compression disabled for raw.events';
  ELSE
    RAISE NOTICE 'Compression was not enabled for raw.events';
  END IF;
END;
$$;

-- Drop monitoring view
DROP VIEW IF EXISTS timescaledb_compression_stats;

-- Drop manual compression function
DROP FUNCTION IF EXISTS compress_chunk_by_time(TIMESTAMPTZ, TIMESTAMPTZ);
-- Migration: Remove all retention policies - keep data forever for personal exocortex
-- Up Migration

-- Remove retention policies from storage_metrics if they exist
DO $$
DECLARE
  job_id INTEGER;
BEGIN
  SELECT j.id INTO job_id
  FROM _timescaledb_config.bgw_job j
  JOIN _timescaledb_catalog.hypertable h ON j.hypertable_id = h.id
  WHERE h.table_name = 'storage_metrics' 
    AND h.schema_name = 'sinex_schemas'
    AND j.proc_name = 'policy_retention';
    
  IF job_id IS NOT NULL THEN
    PERFORM remove_retention_policy('sinex_schemas.storage_metrics');
    RAISE NOTICE 'Retention policy removed from sinex_schemas.storage_metrics - keeping all metrics forever';
  ELSE
    RAISE NOTICE 'No retention policy found for sinex_schemas.storage_metrics';
  END IF;
END;
$$;

-- Remove retention policies from sync_errors if they exist
DO $$
DECLARE
  job_id INTEGER;
BEGIN
  SELECT j.id INTO job_id
  FROM _timescaledb_config.bgw_job j
  JOIN _timescaledb_catalog.hypertable h ON j.hypertable_id = h.id
  WHERE h.table_name = 'sync_errors' 
    AND h.schema_name = 'sinex_schemas'
    AND j.proc_name = 'policy_retention';
    
  IF job_id IS NOT NULL THEN
    PERFORM remove_retention_policy('sinex_schemas.sync_errors');
    RAISE NOTICE 'Retention policy removed from sinex_schemas.sync_errors - keeping all error history forever';
  ELSE
    RAISE NOTICE 'No retention policy found for sinex_schemas.sync_errors';
  END IF;
END;
$$;

-- Remove retention policies from raw.events if they exist (just in case)
DO $$
DECLARE
  job_id INTEGER;
BEGIN
  SELECT j.id INTO job_id
  FROM _timescaledb_config.bgw_job j
  JOIN _timescaledb_catalog.hypertable h ON j.hypertable_id = h.id
  WHERE h.table_name = 'events' 
    AND h.schema_name = 'raw'
    AND j.proc_name = 'policy_retention';
    
  IF job_id IS NOT NULL THEN
    PERFORM remove_retention_policy('raw.events');
    RAISE NOTICE 'Retention policy removed from raw.events - keeping all events forever';
  ELSE
    RAISE NOTICE 'No retention policy found for raw.events (expected)';
  END IF;
END;
$$;

COMMENT ON SCHEMA raw IS 'Event storage schema - all data retained forever for complete digital memory';
COMMENT ON SCHEMA sinex_schemas IS 'System metadata schema - all data retained forever for complete operational history';
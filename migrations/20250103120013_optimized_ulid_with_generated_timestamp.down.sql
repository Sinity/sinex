-- Rollback optimized ULID solution

-- Drop continuous aggregate
DROP MATERIALIZED VIEW IF EXISTS raw.events_hourly;

-- Drop indexes that depend on ts_computed
DROP INDEX IF EXISTS raw.idx_raw_events_ts_computed;
DROP INDEX IF EXISTS raw.idx_raw_events_source_ts;
DROP INDEX IF EXISTS raw.idx_raw_events_host_ts;
DROP INDEX IF EXISTS raw.idx_raw_events_hour;

-- Remove the generated column
ALTER TABLE raw.events DROP COLUMN IF EXISTS ts_computed;

-- Note: We don't remove the hypertable conversion or compression settings
-- as those could cause data loss. The table remains functional without
-- the ts_computed column.
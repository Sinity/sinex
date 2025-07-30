-- Down migration for 00000000000005_create_analytics_views.sql

-- Drop functions
DROP FUNCTION IF EXISTS metrics.refresh_all_analytics_views();
DROP FUNCTION IF EXISTS metrics.auto_refresh_analytics_views();

-- Drop indexes
DROP INDEX IF EXISTS idx_event_counts_bucket_source;
DROP INDEX IF EXISTS idx_heartbeats_bucket_process;
DROP INDEX IF EXISTS idx_error_patterns_bucket_source;

-- Drop materialized views
DROP MATERIALIZED VIEW IF EXISTS metrics.error_event_patterns_1h;
DROP MATERIALIZED VIEW IF EXISTS metrics.process_heartbeats_1h;
DROP MATERIALIZED VIEW IF EXISTS metrics.event_counts_by_type_1h;

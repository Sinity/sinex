-- PostgreSQL-native monitoring views for Sinex
-- These views replace the telemetry system by querying state directly

-- Error rates from operations log
CREATE OR REPLACE VIEW metrics.error_rates AS
SELECT 
    operation_type,
    operator,
    COUNT(*) FILTER (WHERE result_status = 'success') as success_count,
    COUNT(*) FILTER (WHERE result_status = 'failure') as failure_count,
    COUNT(*) as total_count,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE result_status = 'failure') / NULLIF(COUNT(*), 0),
        2
    ) as error_rate_percent,
    AVG(duration_ms) FILTER (WHERE duration_ms IS NOT NULL) as avg_duration_ms,
    MAX(created_at) as last_operation
FROM core.operations_log
WHERE created_at > NOW() - INTERVAL '1 hour'
GROUP BY operation_type, operator;

COMMENT ON VIEW metrics.error_rates IS 'Operation error rates over the last hour';

-- Event ingestion rates (last 5 minutes)
CREATE OR REPLACE VIEW metrics.ingestion_rates AS
SELECT 
    source,
    event_type,
    COUNT(*) as event_count,
    COUNT(*) / 300.0 as events_per_sec,
    MIN(ts_ingest) as period_start,
    MAX(ts_ingest) as period_end
FROM core.events
WHERE ts_ingest > NOW() - INTERVAL '5 minutes'
GROUP BY source, event_type
ORDER BY events_per_sec DESC;

COMMENT ON VIEW metrics.ingestion_rates IS 'Event ingestion rates over the last 5 minutes';

-- Database connection stats (using pg_stat_database)
CREATE OR REPLACE VIEW metrics.database_stats AS
SELECT 
    numbackends as active_connections,
    xact_commit as transactions_committed,
    xact_rollback as transactions_rolled_back,
    blks_read as blocks_read,
    blks_hit as blocks_hit,
    ROUND(100.0 * blks_hit / NULLIF(blks_hit + blks_read, 0), 2) as cache_hit_ratio,
    tup_returned as tuples_returned,
    tup_fetched as tuples_fetched,
    tup_inserted as tuples_inserted,
    tup_updated as tuples_updated,
    tup_deleted as tuples_deleted,
    conflicts,
    deadlocks,
    stats_reset
FROM pg_stat_database
WHERE datname = current_database();

COMMENT ON VIEW metrics.database_stats IS 'PostgreSQL database statistics';

-- Query performance (requires pg_stat_statements extension)
CREATE OR REPLACE VIEW metrics.slow_queries AS
SELECT 
    LEFT(query, 100) as query_preview,
    calls,
    ROUND(total_exec_time::numeric, 2) as total_time_ms,
    ROUND(mean_exec_time::numeric, 2) as mean_time_ms,
    ROUND(max_exec_time::numeric, 2) as max_time_ms,
    ROUND(stddev_exec_time::numeric, 2) as stddev_time_ms,
    rows
FROM pg_stat_statements
WHERE query NOT LIKE '%pg_stat_statements%'
ORDER BY mean_exec_time DESC
LIMIT 20;

COMMENT ON VIEW metrics.slow_queries IS 'Top 20 slowest queries by mean execution time';

-- Table sizes and growth
CREATE OR REPLACE VIEW metrics.table_sizes AS
SELECT 
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as total_size,
    pg_size_pretty(pg_relation_size(schemaname||'.'||tablename)) as table_size,
    pg_size_pretty(pg_indexes_size(schemaname||'.'||tablename)) as indexes_size,
    n_live_tup as row_count,
    n_dead_tup as dead_rows,
    ROUND(100.0 * n_dead_tup / NULLIF(n_live_tup + n_dead_tup, 0), 2) as dead_row_percent,
    last_vacuum,
    last_autovacuum,
    last_analyze,
    last_autoanalyze
FROM pg_stat_user_tables
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;

COMMENT ON VIEW metrics.table_sizes IS 'Table sizes and maintenance statistics';

-- Combined system health dashboard query
CREATE OR REPLACE VIEW metrics.system_dashboard AS
WITH ingestion_summary AS (
    SELECT 
        SUM(events_per_sec) as total_ingestion_rate,
        COUNT(DISTINCT source) as active_sources
    FROM metrics.ingestion_rates
),
error_summary AS (
    SELECT 
        AVG(error_rate_percent) as avg_error_rate
    FROM metrics.error_rates
),
db_summary AS (
    SELECT 
        active_connections,
        cache_hit_ratio
    FROM metrics.database_stats
)
SELECT 
    COALESCE(ins.total_ingestion_rate, 0) as total_events_per_sec,
    COALESCE(ins.active_sources, 0) as active_sources,
    COALESCE(es.avg_error_rate, 0) as avg_error_rate,
    ds.active_connections,
    ds.cache_hit_ratio,
    NOW() as snapshot_time
FROM ingestion_summary ins
CROSS JOIN error_summary es
CROSS JOIN db_summary ds;

COMMENT ON VIEW metrics.system_dashboard IS 'Unified system health dashboard metrics';

-- Grant read access to monitoring views
GRANT SELECT ON ALL TABLES IN SCHEMA metrics TO PUBLIC;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA metrics TO PUBLIC;

# PostgreSQL-Native Monitoring for Sinex

## Overview

Sinex has transitioned from a telemetry-based monitoring system to a PostgreSQL-native approach that leverages the database as the single source of truth for all monitoring needs.

## Rationale

The previous telemetry system created ~288,000 synthetic events per day for monitoring purposes, while only ~1,000 real state changes occurred. This represented significant overhead and complexity without providing additional value, since:

1. All state is already in PostgreSQL
2. PostgreSQL/TimescaleDB has excellent built-in monitoring capabilities
3. Synthetic events violate the provenance requirement (Material XOR Synthesis)
4. The monitoring data was redundant with existing database state

## Architecture

### Direct State Tables

The system maintains real state in these tables:
- `core.processor_checkpoints` - Tracks processor progress and activity
- `core.operations_log` - Audit trail of all operations
- `core.processor_manifests` - Service registry and versions

### SQL Monitoring Views

All monitoring is done through SQL views that query state on-demand:

```sql
-- Processing throughput from checkpoint data
metrics.processing_throughput

-- Service health based on checkpoint activity  
metrics.service_health

-- Error rates from operations log
metrics.error_rates

-- Event ingestion rates (5-minute window)
metrics.ingestion_rates

-- Processing lag (unprocessed events)
metrics.processing_lag

-- Database statistics from pg_stat_database
metrics.database_stats

-- Slow queries from pg_stat_statements
metrics.slow_queries

-- Table sizes and maintenance
metrics.table_sizes

-- Unified dashboard view
metrics.system_dashboard
```

### Materialized Views for Performance

Time-series aggregations are pre-computed:
- `metrics.event_counts_by_type_hourly`
- `metrics.process_heartbeats_hourly`
- `metrics.file_activity_hourly`
- `metrics.terminal_commands_daily`

These are refreshed periodically via `metrics.refresh_all_materialized_views()`.

## Usage

### Monitoring Queries

```sql
-- Check system health
SELECT * FROM metrics.system_dashboard;

-- View service status
SELECT * FROM metrics.service_health;

-- Check processing throughput
SELECT * FROM metrics.processing_throughput;

-- View error rates
SELECT * FROM metrics.error_rates;

-- Check for processing lag
SELECT * FROM metrics.processing_lag;
```

### PostgreSQL Native Monitoring

```sql
-- Database connection stats
SELECT * FROM pg_stat_database WHERE datname = current_database();

-- Query performance (requires pg_stat_statements)
SELECT * FROM pg_stat_statements ORDER BY mean_exec_time DESC LIMIT 20;

-- Table bloat and maintenance
SELECT * FROM pg_stat_user_tables;

-- Replication lag (if applicable)
SELECT * FROM pg_stat_replication;
```

### External State Monitoring

For external systems like NATS JetStream, placeholder functions exist:
```sql
-- Would query NATS via foreign data wrapper or HTTP
SELECT * FROM metrics.nats_stream_state();
```

## Integration with Grafana

The monitoring views are designed to be queried directly from Grafana:

1. Add PostgreSQL data source pointing to Sinex database
2. Create panels querying the monitoring views
3. Set appropriate refresh intervals (5-60 seconds)
4. Use TimescaleDB-optimized queries for time-series data

Example Grafana query:
```sql
SELECT 
  $__timeGroupAlias(bucket, 1m),
  source,
  SUM(event_count) as events
FROM metrics.event_counts_by_type_hourly
WHERE $__timeFilter(bucket)
GROUP BY 1, source
ORDER BY 1
```

## Benefits

1. **Simplicity**: No separate telemetry infrastructure
2. **Accuracy**: Real-time queries of actual state
3. **Performance**: PostgreSQL query optimization and caching
4. **Consistency**: Single source of truth
5. **Flexibility**: Easy to add new monitoring views
6. **Cost**: No overhead of synthetic events

## Migration from Telemetry

If you were using the old telemetry system:

1. Replace telemetry event queries with SQL view queries
2. Remove telemetry accumulator initialization
3. Update Grafana dashboards to use PostgreSQL data source
4. Remove Prometheus scraping configuration
5. Use `processor_checkpoints` for service heartbeats

## Future Enhancements

- Foreign Data Wrappers for external system monitoring (NATS, Redis)
- PostgreSQL triggers for real-time alerting
- pg_cron for scheduled maintenance and aggregation
- Custom PostgreSQL functions for complex metrics
- Integration with pg_prometheus for Prometheus compatibility
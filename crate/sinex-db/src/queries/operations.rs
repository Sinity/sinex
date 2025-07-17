//! Operations query registry for centralized system operations
//!
//! This module provides all database queries related to system operations,
//! health checks, and monitoring. All queries automatically handle ULID/UUID
//! conversion and provide consistent error handling.

use crate::query_builder::{QueryBuilder, QueryParam};
use chrono::{DateTime, Utc};
use std::collections::HashMap;

/// Operations query registry with centralized system operations
pub struct OperationQueries;

impl OperationQueries {
    /// Simple health check query
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_one::<(i64,)>(pool)`
    pub fn health_check() -> QueryBuilder {
        // Use a simple table-less select that PostgreSQL supports
        QueryBuilder::select("core.events")
            .columns(&["1 as health"])
            .limit(1)
    }

    /// Get system health metrics
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_one::<HealthMetricsRecord>(pool)`
    pub fn get_health_metrics() -> QueryBuilder {
        QueryBuilder::select("core.events").columns(&[
            "COUNT(*) as \"total_events!\"",
            "COUNT(DISTINCT source) as \"active_sources!\"",
            "COUNT(DISTINCT event_type) as \"event_types!\"",
            "MAX(ts_ingest) as \"last_event_time\"",
            "MIN(ts_ingest) as \"first_event_time\"",
        ])
    }

    /// Get event throughput metrics
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_one::<ThroughputMetricsRecord>(pool)`
    pub fn get_throughput_metrics(since: DateTime<Utc>) -> QueryBuilder {
        QueryBuilder::select("core.events")
            .columns(&[
                "COUNT(*) as \"events_count!\"",
                "COUNT(*) / EXTRACT(EPOCH FROM (NOW() - $1::timestamptz)) as \"events_per_second!\"",
                "COUNT(DISTINCT source) as \"active_sources!\"",
                "AVG(EXTRACT(EPOCH FROM (ts_ingest - COALESCE(ts_orig, ts_ingest)))) as \"avg_ingestion_delay\""
            ])
            .where_op("ts_ingest", ">=", QueryParam::Timestamp(since))
    }

    /// Get source activity metrics
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_all::<SourceActivityRecord>(pool)`
    pub fn get_source_activity(since: DateTime<Utc>) -> QueryBuilder {
        QueryBuilder::select("core.events")
            .columns(&[
                "source as \"source!\"",
                "COUNT(*) as \"event_count!\"",
                "COUNT(DISTINCT event_type) as \"event_types!\"",
                "MAX(ts_ingest) as \"last_event!\"",
                "MIN(ts_ingest) as \"first_event!\"",
            ])
            .where_op("ts_ingest", ">=", QueryParam::Timestamp(since))
            .order_by("event_count", "DESC")
    }

    /// Get event type distribution
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_all::<EventTypeDistributionRecord>(pool)`
    pub fn get_event_type_distribution(since: DateTime<Utc>) -> QueryBuilder {
        QueryBuilder::select("core.events")
            .columns(&[
                "event_type as \"event_type!\"",
                "COUNT(*) as \"count!\"",
                "COUNT(DISTINCT source) as \"sources!\"",
                "MAX(ts_ingest) as \"last_seen!\"",
            ])
            .where_op("ts_ingest", ">=", QueryParam::Timestamp(since))
            .order_by("count", "DESC")
    }

    /// Get error rate metrics
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_one::<ErrorRateRecord>(pool)`
    pub fn get_error_rate(since: DateTime<Utc>) -> QueryBuilder {
        QueryBuilder::select("core.events")
            .columns(&[
                "COUNT(*) as \"total_events!\"",
                "COUNT(*) FILTER (WHERE event_type LIKE '%error%' OR event_type LIKE '%fail%') as \"error_events!\"",
                "ROUND(100.0 * COUNT(*) FILTER (WHERE event_type LIKE '%error%' OR event_type LIKE '%fail%') / COUNT(*), 2) as \"error_rate_percent!\""
            ])
            .where_op("ts_ingest", ">=", QueryParam::Timestamp(since))
    }

    /// Get database connection info
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_one::<DatabaseInfoRecord>(pool)`
    pub fn get_database_info() -> QueryBuilder {
        QueryBuilder::select("pg_stat_database")
            .columns(&[
                "datname as \"database_name!\"",
                "numbackends as \"active_connections!\"",
                "xact_commit as \"transactions_committed!\"",
                "xact_rollback as \"transactions_rolled_back!\"",
                "tup_returned as \"tuples_returned!\"",
                "tup_fetched as \"tuples_fetched!\"",
                "tup_inserted as \"tuples_inserted!\"",
                "tup_updated as \"tuples_updated!\"",
                "tup_deleted as \"tuples_deleted!\"",
            ])
            .where_eq("datname", QueryParam::String("sinex_dev".to_string()))
    }

    /// Get table sizes
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_all::<TableSizeRecord>(pool)`
    pub fn get_table_sizes() -> QueryBuilder {
        QueryBuilder::select("pg_stat_user_tables")
            .columns(&[
                "schemaname as \"schema_name!\"",
                "relname as \"table_name!\"",
                "n_tup_ins as \"rows_inserted!\"",
                "n_tup_upd as \"rows_updated!\"",
                "n_tup_del as \"rows_deleted!\"",
                "n_live_tup as \"live_rows!\"",
                "n_dead_tup as \"dead_rows!\"",
            ])
            .order_by("n_live_tup", "DESC")
    }

    /// Get index usage statistics
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_all::<IndexUsageRecord>(pool)`
    pub fn get_index_usage() -> QueryBuilder {
        QueryBuilder::select("pg_stat_user_indexes")
            .columns(&[
                "schemaname as \"schema_name!\"",
                "relname as \"table_name!\"",
                "indexrelname as \"index_name!\"",
                "idx_scan as \"index_scans!\"",
                "idx_tup_read as \"tuples_read!\"",
                "idx_tup_fetch as \"tuples_fetched!\"",
            ])
            .order_by("idx_scan", "DESC")
    }

    /// Get checkpoint processor health
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_all::<CheckpointHealthRecord>(pool)`
    pub fn get_checkpoint_health() -> QueryBuilder {
        QueryBuilder::select("core.automaton_checkpoints")
            .columns(&[
                "automaton_name as \"processor_name!\"",
                "consumer_group as \"consumer_group!\"",
                "consumer_name as \"consumer_name!\"",
                "processed_count as \"processed_count!\"",
                "last_activity as \"last_activity!\"",
                "EXTRACT(EPOCH FROM (NOW() - last_activity)) as \"seconds_since_activity!\"",
                "checkpoint_version as \"checkpoint_version!\"",
            ])
            .order_by("last_activity", "DESC")
    }

    /// Get slow query analysis
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_all::<SlowQueryRecord>(pool)`
    pub fn get_slow_queries(min_duration_ms: i64) -> QueryBuilder {
        QueryBuilder::select("pg_stat_statements")
            .columns(&[
                "query as \"query!\"",
                "calls as \"calls!\"",
                "total_exec_time as \"total_time!\"",
                "mean_exec_time as \"mean_time!\"",
                "max_exec_time as \"max_time!\"",
                "stddev_exec_time as \"stddev_time!\"",
            ])
            .where_op("mean_exec_time", ">", QueryParam::Integer(min_duration_ms))
            .order_by("mean_exec_time", "DESC")
            .limit(20)
    }

    /// Get replication lag (if applicable)
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_all::<ReplicationLagRecord>(pool)`
    pub fn get_replication_lag() -> QueryBuilder {
        QueryBuilder::select("pg_stat_replication").columns(&[
            "application_name as \"application_name!\"",
            "client_addr as \"client_addr!\"",
            "state as \"state!\"",
            "sync_state as \"sync_state!\"",
            "EXTRACT(EPOCH FROM (NOW() - backend_start)) as \"connection_age!\"",
            "pg_wal_lsn_diff(pg_current_wal_lsn(), sent_lsn) as \"send_lag!\"",
            "pg_wal_lsn_diff(pg_current_wal_lsn(), write_lsn) as \"write_lag!\"",
            "pg_wal_lsn_diff(pg_current_wal_lsn(), flush_lsn) as \"flush_lag!\"",
            "pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn) as \"replay_lag!\"",
        ])
    }

    /// Get lock information
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_all::<LockInfoRecord>(pool)`
    pub fn get_lock_info() -> QueryBuilder {
        QueryBuilder::select("pg_locks")
            .columns(&[
                "locktype as \"lock_type!\"",
                "database as \"database\"",
                "relation as \"relation\"",
                "page as \"page\"",
                "tuple as \"tuple\"",
                "virtualxid as \"virtual_xid\"",
                "transactionid as \"transaction_id\"",
                "classid as \"class_id\"",
                "objid as \"object_id\"",
                "objsubid as \"object_subid\"",
                "virtualtransaction as \"virtual_transaction!\"",
                "pid as \"process_id!\"",
                "mode as \"mode!\"",
                "granted as \"granted!\"",
            ])
            .where_eq("granted", QueryParam::Boolean(false))
    }

    /// Get current activity
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_all::<ActivityRecord>(pool)`
    pub fn get_current_activity() -> QueryBuilder {
        QueryBuilder::select("pg_stat_activity")
            .columns(&[
                "pid as \"process_id!\"",
                "usename as \"username!\"",
                "application_name as \"application_name!\"",
                "client_addr as \"client_addr\"",
                "backend_start as \"backend_start!\"",
                "query_start as \"query_start\"",
                "state as \"state!\"",
                "query as \"query!\"",
            ])
            .where_op("state", "!=", QueryParam::String("idle".to_string()))
    }

    /// Get vacuum and analyze statistics
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_all::<VacuumStatsRecord>(pool)`
    pub fn get_vacuum_stats() -> QueryBuilder {
        QueryBuilder::select("pg_stat_user_tables")
            .columns(&[
                "schemaname as \"schema_name!\"",
                "relname as \"table_name!\"",
                "last_vacuum as \"last_vacuum\"",
                "last_autovacuum as \"last_autovacuum\"",
                "last_analyze as \"last_analyze\"",
                "last_autoanalyze as \"last_autoanalyze\"",
                "vacuum_count as \"vacuum_count!\"",
                "autovacuum_count as \"autovacuum_count!\"",
                "analyze_count as \"analyze_count!\"",
                "autoanalyze_count as \"autoanalyze_count!\"",
            ])
            .order_by("last_autovacuum", "DESC")
    }

    /// Get WAL statistics
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_one::<WalStatsRecord>(pool)`
    pub fn get_wal_stats() -> QueryBuilder {
        QueryBuilder::select("pg_stat_wal").columns(&[
            "wal_records as \"wal_records!\"",
            "wal_fpi as \"wal_fpi!\"",
            "wal_bytes as \"wal_bytes!\"",
            "wal_buffers_full as \"wal_buffers_full!\"",
            "wal_write as \"wal_write!\"",
            "wal_sync as \"wal_sync!\"",
            "wal_write_time as \"wal_write_time!\"",
            "wal_sync_time as \"wal_sync_time!\"",
            "stats_reset as \"stats_reset!\"",
        ])
    }

    /// Get database age information
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_one::<DatabaseAgeRecord>(pool)`
    pub fn get_database_age() -> QueryBuilder {
        QueryBuilder::select("pg_database")
            .columns(&[
                "datname as \"database_name!\"",
                "age(datfrozenxid) as \"age_in_transactions!\"",
                "datfrozenxid as \"frozen_xid!\"",
                "datminmxid as \"min_mxid!\"",
            ])
            .where_eq("datname", QueryParam::String("sinex_dev".to_string()))
    }

    // ===== Metrics Operations =====

    /// Store a single metrics entry
    pub fn store_metric(entry: &sinex_core_types::MetricsEntry) -> QueryBuilder {
        QueryBuilder::insert("sinex.metrics")
            .columns(&["id", "metric_name", "metric_type", "value", "labels", "timestamp"])
            .values(&[
                QueryParam::Uuid(entry.id.to_uuid()),
                QueryParam::String(entry.metric_name.clone()),
                QueryParam::String(entry.metric_type.clone()),
                QueryParam::Float(entry.value),
                QueryParam::Json(serde_json::to_value(&entry.labels).unwrap_or_default()),
                QueryParam::Timestamp(entry.timestamp),
            ])
    }

    /// Store multiple metrics entries in a batch
    pub fn store_metrics_batch(entries: &[sinex_core_types::MetricsEntry]) -> QueryBuilder {
        let mut builder = QueryBuilder::insert("sinex.metrics")
            .columns(&["id", "metric_name", "metric_type", "value", "labels", "timestamp"]);
        
        for entry in entries {
            builder = builder.values(&[
                QueryParam::Uuid(entry.id.to_uuid()),
                QueryParam::String(entry.metric_name.clone()),
                QueryParam::String(entry.metric_type.clone()),
                QueryParam::Float(entry.value),
                QueryParam::Json(serde_json::to_value(&entry.labels).unwrap_or_default()),
                QueryParam::Timestamp(entry.timestamp),
            ]);
        }
        
        builder
    }

    /// Query metrics by name and time range
    pub fn query_metrics(
        metric_name: Option<&str>,
        namespace: Option<&str>,
        subsystem: Option<&str>,
        start_time: Option<DateTime<Utc>>,
        end_time: Option<DateTime<Utc>>,
        limit: Option<i64>,
    ) -> QueryBuilder {
        let mut builder = QueryBuilder::select("sinex.metrics")
            .columns(&["id", "metric_name", "metric_type", "value", "labels", "timestamp"])
            .order_by("timestamp", "DESC");

        if let Some(name) = metric_name {
            builder = builder.where_eq("metric_name", QueryParam::String(name.to_string()));
        }

        if let Some(start) = start_time {
            builder = builder.where_op("timestamp", ">=", QueryParam::Timestamp(start));
        }

        if let Some(end) = end_time {
            builder = builder.where_op("timestamp", "<=", QueryParam::Timestamp(end));
        }

        if let Some(lim) = limit {
            builder = builder.limit(lim as usize);
        } else {
            builder = builder.limit(1000);
        }

        builder
    }

    /// Get aggregated metrics for a metric over time
    pub fn get_metrics_aggregation(
        metric_name: &str,
        namespace: Option<&str>,
        subsystem: Option<&str>,
        start_time: Option<DateTime<Utc>>,
        end_time: Option<DateTime<Utc>>,
    ) -> QueryBuilder {
        let mut builder = QueryBuilder::select("sinex.metrics")
            .columns(&[
                "COUNT(*) as count",
                "SUM(value) as sum",
                "AVG(value) as avg",
                "MIN(value) as min",
                "MAX(value) as max",
            ])
            .where_eq("metric_name", QueryParam::String(metric_name.to_string()));

        if let Some(start) = start_time {
            builder = builder.where_op("timestamp", ">=", QueryParam::Timestamp(start));
        }

        if let Some(end) = end_time {
            builder = builder.where_op("timestamp", "<=", QueryParam::Timestamp(end));
        }

        builder
    }

    /// Clean up old metrics data
    pub fn cleanup_old_metrics(older_than: DateTime<Utc>) -> QueryBuilder {
        QueryBuilder::delete("sinex.metrics")
            .where_op("timestamp", "<", QueryParam::Timestamp(older_than))
    }
}

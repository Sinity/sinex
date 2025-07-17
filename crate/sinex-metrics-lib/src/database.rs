//! Automatic Database Operation Metrics
//!
//! This module provides automatic instrumentation of database operations with detailed metrics.
//! It tracks performance, connection health, and query patterns.

use once_cell::sync::Lazy;
use parking_lot::RwLock;
use prometheus::{Counter, Histogram, IntGauge};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use crate::registry::GlobalMetrics;

/// Database operation metrics collector
#[derive(Debug, Clone)]
pub struct DatabaseMetrics {
    pub operation: String,
    pub queries: Counter,
    pub query_duration: Histogram,
    pub query_errors: Counter,
    pub rows_returned: Histogram,
    pub rows_affected: Histogram,
    pub connection_pool_active: IntGauge,
    pub connection_pool_idle: IntGauge,
    pub transaction_duration: Histogram,
    pub transaction_rollbacks: Counter,
    pub labels: HashMap<String, String>,
}

impl DatabaseMetrics {
    pub fn new(operation: &str, labels: HashMap<String, String>) -> Self {
        let queries = Counter::with_opts(
            prometheus::Opts::new("sinex_db_queries_total", "Total number of database queries")
                .namespace("sinex")
                .subsystem("database")
                .const_labels(labels.clone()),
        )
        .unwrap();

        let query_duration = Histogram::with_opts(
            prometheus::HistogramOpts::new(
                "sinex_db_query_duration_seconds",
                "Database query execution duration in seconds",
            )
            .namespace("sinex")
            .subsystem("database")
            .const_labels(labels.clone())
            .buckets(vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0]),
        )
        .unwrap();

        let query_errors = Counter::with_opts(
            prometheus::Opts::new(
                "sinex_db_query_errors_total",
                "Total number of database query errors",
            )
            .namespace("sinex")
            .subsystem("database")
            .const_labels(labels.clone()),
        )
        .unwrap();

        let rows_returned = Histogram::with_opts(
            prometheus::HistogramOpts::new(
                "sinex_db_rows_returned_total",
                "Number of rows returned by queries",
            )
            .namespace("sinex")
            .subsystem("database")
            .const_labels(labels.clone())
            .buckets(vec![1.0, 10.0, 100.0, 1000.0, 10000.0, 100000.0]),
        )
        .unwrap();

        let rows_affected = Histogram::with_opts(
            prometheus::HistogramOpts::new(
                "sinex_db_rows_affected_total",
                "Number of rows affected by queries",
            )
            .namespace("sinex")
            .subsystem("database")
            .const_labels(labels.clone())
            .buckets(vec![1.0, 10.0, 100.0, 1000.0, 10000.0, 100000.0]),
        )
        .unwrap();

        let connection_pool_active = IntGauge::with_opts(
            prometheus::Opts::new(
                "sinex_db_connection_pool_active",
                "Number of active database connections",
            )
            .namespace("sinex")
            .subsystem("database")
            .const_labels(labels.clone()),
        )
        .unwrap();

        let connection_pool_idle = IntGauge::with_opts(
            prometheus::Opts::new(
                "sinex_db_connection_pool_idle",
                "Number of idle database connections",
            )
            .namespace("sinex")
            .subsystem("database")
            .const_labels(labels.clone()),
        )
        .unwrap();

        let transaction_duration = Histogram::with_opts(
            prometheus::HistogramOpts::new(
                "sinex_db_transaction_duration_seconds",
                "Database transaction duration in seconds",
            )
            .namespace("sinex")
            .subsystem("database")
            .const_labels(labels.clone())
            .buckets(vec![0.01, 0.1, 0.5, 1.0, 5.0, 10.0, 30.0]),
        )
        .unwrap();

        let transaction_rollbacks = Counter::with_opts(
            prometheus::Opts::new(
                "sinex_db_transaction_rollbacks_total",
                "Total number of database transaction rollbacks",
            )
            .namespace("sinex")
            .subsystem("database")
            .const_labels(labels.clone()),
        )
        .unwrap();

        // Register with global metrics
        GlobalMetrics::register_counter(&queries);
        GlobalMetrics::register_histogram(&query_duration);
        GlobalMetrics::register_counter(&query_errors);
        GlobalMetrics::register_histogram(&rows_returned);
        GlobalMetrics::register_histogram(&rows_affected);
        GlobalMetrics::register_gauge(&connection_pool_active);
        GlobalMetrics::register_gauge(&connection_pool_idle);
        GlobalMetrics::register_histogram(&transaction_duration);
        GlobalMetrics::register_counter(&transaction_rollbacks);

        Self {
            operation: operation.to_string(),
            queries,
            query_duration,
            query_errors,
            rows_returned,
            rows_affected,
            connection_pool_active,
            connection_pool_idle,
            transaction_duration,
            transaction_rollbacks,
            labels,
        }
    }

    pub fn record_query_start(&self) {
        self.queries.inc();
    }

    pub fn record_query_complete(&self, duration: std::time::Duration, rows: Option<u64>) {
        self.query_duration.observe(duration.as_secs_f64());
        if let Some(row_count) = rows {
            self.rows_returned.observe(row_count as f64);
        }
    }

    pub fn record_query_error(&self, _error_type: &str) {
        self.query_errors.inc();
    }

    pub fn record_rows_affected(&self, count: u64) {
        self.rows_affected.observe(count as f64);
    }

    pub fn update_connection_pool_stats(&self, active: i64, idle: i64) {
        self.connection_pool_active.set(active);
        self.connection_pool_idle.set(idle);
    }

    pub fn record_transaction_start(&self) {
        // Transaction tracking can be implemented with separate guards if needed
    }

    pub fn record_transaction_complete(&self, duration: std::time::Duration) {
        self.transaction_duration.observe(duration.as_secs_f64());
    }

    pub fn record_transaction_rollback(&self) {
        self.transaction_rollbacks.inc();
    }
}

/// Database query guard that automatically records metrics
pub struct DatabaseQueryGuard {
    metrics: Arc<DatabaseMetrics>,
    start_time: Instant,
}

impl DatabaseQueryGuard {
    pub fn new(metrics: Arc<DatabaseMetrics>) -> Self {
        metrics.record_query_start();
        Self {
            metrics,
            start_time: Instant::now(),
        }
    }

    pub fn record_error(self, error_type: &str) {
        let duration = self.start_time.elapsed();
        self.metrics.query_duration.observe(duration.as_secs_f64());
        self.metrics.record_query_error(error_type);
    }

    pub fn complete_with_rows(self, rows: Option<u64>) {
        let duration = self.start_time.elapsed();
        self.metrics.record_query_complete(duration, rows);
    }
}

impl Drop for DatabaseQueryGuard {
    fn drop(&mut self) {
        let duration = self.start_time.elapsed();
        self.metrics.record_query_complete(duration, None);
    }
}

/// Global database metrics
static DATABASE_METRICS: Lazy<Arc<RwLock<HashMap<String, Arc<DatabaseMetrics>>>>> =
    Lazy::new(|| Arc::new(RwLock::new(HashMap::new())));

/// Get or create database metrics
pub fn get_database_metrics(
    operation: &str,
    labels: HashMap<String, String>,
) -> Arc<DatabaseMetrics> {
    let key = format!("db_{}", operation);

    // Try to get existing metrics
    if let Some(metrics) = DATABASE_METRICS.read().get(&key) {
        return metrics.clone();
    }

    // Create new metrics
    let metrics = Arc::new(DatabaseMetrics::new(operation, labels));
    DATABASE_METRICS.write().insert(key, metrics.clone());

    metrics
}

/// Create a database query guard for automatic metrics
pub fn track_database_query(operation: &str) -> DatabaseQueryGuard {
    let metrics = get_database_metrics(operation, HashMap::new());
    DatabaseQueryGuard::new(metrics)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_database_metrics() {
        let metrics = get_database_metrics("SELECT", HashMap::new());

        let guard = DatabaseQueryGuard::new(metrics.clone());
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        guard.complete_with_rows(Some(5));

        // Verify metrics were recorded
        assert!(metrics.queries.get() > 0.0);
        assert!(metrics.rows_returned.get_sample_count() > 0);
    }

    #[tokio::test]
    async fn test_database_error_tracking() {
        let metrics = get_database_metrics("INSERT", HashMap::new());

        let guard = DatabaseQueryGuard::new(metrics.clone());
        guard.record_error("constraint_violation");

        // Verify error was recorded
        assert!(metrics.query_errors.get() > 0.0);
    }
}

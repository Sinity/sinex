//! Database Storage for Metrics
//!
//! This module provides database persistence for metrics data using the sinex.* namespace.

use chrono::{DateTime, Utc};
use sinex_core_types::{MetricsEntry, MetricsAggregation};
use sinex_error::CoreError;
use sqlx::PgPool;
use sinex_db::queries::MetricsQueries;
use sinex_db::queries::metrics::{MetricRecord, AggregationRecord};

/// Simple error type for metrics operations
pub type MetricsError = CoreError;

// MetricsEntry is now imported from sinex_core_types

/// Database storage for metrics
pub struct MetricsStorage {
    pool: PgPool,
}

impl MetricsStorage {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Initialize the metrics tables
    pub async fn init_schema(&self) -> Result<(), MetricsError> {
        // Create sinex schema if it doesn't exist
        MetricsQueries::create_schema(&self.pool).await?;

        // Create metrics table
        MetricsQueries::create_table(&self.pool).await?;

        // Create indices for efficient queries
        MetricsQueries::create_indices(&self.pool).await?;

        Ok(())
    }

    /// Store a single metrics entry
    pub async fn store_metric(&self, entry: &MetricsEntry) -> Result<(), MetricsError> {
        let labels_json = serde_json::to_value(&entry.labels)
            .map_err(|e| CoreError::Serialization(format!("Failed to serialize labels: {}", e)))?;

        MetricsQueries::insert_metric(
            entry.id,
            entry.metric_name.clone(),
            entry.metric_type.clone(),
            entry.value,
            labels_json,
            entry.timestamp,
            entry.namespace.clone(),
            entry.subsystem.clone(),
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Store multiple metrics entries in a batch
    pub async fn store_metrics_batch(
        &self,
        entries: Vec<MetricsEntry>,
    ) -> Result<(), MetricsError> {
        if entries.is_empty() {
            return Ok(());
        }

        // Use a transaction for batch insert
        let mut tx = self.pool.begin().await
            .map_err(|e| CoreError::Database(format!("Failed to begin transaction: {}", e)))?;

        for entry in &entries {
            let labels_json = serde_json::to_value(&entry.labels)
                .map_err(|e| CoreError::Serialization(format!("Failed to serialize labels: {}", e)))?;

            MetricsQueries::insert_metric(
                entry.id,
                entry.metric_name.clone(),
                entry.metric_type.clone(),
                entry.value,
                labels_json,
                entry.timestamp,
                entry.namespace.clone(),
                entry.subsystem.clone(),
            )
            .execute_tx(&mut tx)
            .await?;
        }

        tx.commit().await
            .map_err(|e| CoreError::Database(format!("Failed to commit metrics batch: {}", e)))?;

        Ok(())
    }

    /// Query metrics by name and time range
    pub async fn query_metrics(
        &self,
        metric_name: Option<&str>,
        namespace: Option<&str>,
        subsystem: Option<&str>,
        start_time: Option<DateTime<Utc>>,
        end_time: Option<DateTime<Utc>>,
        limit: Option<i64>,
    ) -> Result<Vec<MetricsEntry>, MetricsError> {
        use sinex_ulid::Ulid;
        use std::collections::HashMap;

        let records = MetricsQueries::query_metrics(
            metric_name.map(String::from),
            namespace.map(String::from),
            subsystem.map(String::from),
            start_time,
            end_time,
            limit,
        )
        .fetch_all(&self.pool)
        .await?;

        // Convert records to MetricsEntry
        let entries = records
            .into_iter()
            .map(|record: MetricRecord| {
                let labels: HashMap<String, String> = serde_json::from_value(record.labels)
                    .unwrap_or_default();
                
                MetricsEntry {
                    id: Ulid::from_uuid(record.id),
                    metric_name: record.metric_name,
                    metric_type: record.metric_type,
                    value: record.value,
                    labels,
                    timestamp: record.timestamp,
                    namespace: record.namespace,
                    subsystem: record.subsystem,
                }
            })
            .collect();

        Ok(entries)
    }

    /// Get aggregated metrics (sum, avg, min, max) for a metric over time
    pub async fn get_metrics_aggregation(
        &self,
        metric_name: &str,
        namespace: Option<&str>,
        subsystem: Option<&str>,
        start_time: Option<DateTime<Utc>>,
        end_time: Option<DateTime<Utc>>,
    ) -> Result<MetricsAggregation, MetricsError> {

        let record: AggregationRecord = MetricsQueries::get_aggregation(
            metric_name.to_string(),
            namespace.map(String::from),
            subsystem.map(String::from),
            start_time,
            end_time,
        )
        .fetch_one(&self.pool)
        .await?;

        Ok(MetricsAggregation {
            count: record.count as u64,
            sum: record.sum,
            avg: record.avg,
            min: record.min,
            max: record.max,
        })
    }

    /// Clear old metrics data (retention policy)
    pub async fn cleanup_old_metrics(
        &self,
        older_than: DateTime<Utc>,
    ) -> Result<u64, MetricsError> {
        let result = MetricsQueries::delete_older_than(older_than)
            .execute(&self.pool)
            .await?;

        Ok(result.rows_affected())
    }
}

// MetricsAggregation is now imported from sinex_core_types

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_metrics_entry_creation() {
        let entry = MetricsEntry::new(
            "test_counter".to_string(),
            "counter".to_string(),
            42.0,
            HashMap::new(),
            "sinex".to_string(),
            "test".to_string(),
        );

        assert_eq!(entry.metric_name, "test_counter");
        assert_eq!(entry.metric_type, "counter");
        assert_eq!(entry.value, 42.0);
        assert_eq!(entry.namespace, "sinex");
        assert_eq!(entry.subsystem, "test");
    }
}

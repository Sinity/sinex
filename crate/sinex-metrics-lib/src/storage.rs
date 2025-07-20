//! Database Storage for Metrics
//!
//! This module provides database persistence for metrics data using the sinex.* namespace.

use chrono::{DateTime, Utc};
use sinex_core_types::{MetricsEntry, MetricsAggregation};
use sqlx::PgPool;

/// Simple error type for metrics operations
#[derive(Debug, thiserror::Error)]
pub enum MetricsError {
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("Configuration error: {0}")]
    Configuration(String),
}

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
        sqlx::query("CREATE SCHEMA IF NOT EXISTS sinex")
            .execute(&self.pool)
            .await
            .map_err(|e| MetricsError::Database(e))?;

        // Create metrics table
        sqlx::query(r#"
            CREATE TABLE IF NOT EXISTS sinex.metrics (
                id UUID PRIMARY KEY,
                metric_name TEXT NOT NULL,
                metric_type TEXT NOT NULL,
                value DOUBLE PRECISION NOT NULL,
                labels JSONB NOT NULL DEFAULT '{}',
                timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                namespace TEXT NOT NULL DEFAULT 'sinex',
                subsystem TEXT NOT NULL,
                CONSTRAINT valid_metric_type CHECK (metric_type IN ('counter', 'gauge', 'histogram', 'summary'))
            )
        "#)
        .execute(&self.pool)
        .await
        .map_err(|e| MetricsError::Configuration(format!("Failed to create metrics table: {}", e)))?;

        // Create index for efficient queries
        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_sinex_metrics_name_time 
            ON sinex.metrics (metric_name, timestamp DESC)
        "#,
        )
        .execute(&self.pool)
        .await
        .map_err(|e| {
            MetricsError::Configuration(format!("Failed to create metrics index: {}", e))
        })?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_sinex_metrics_namespace_subsystem 
            ON sinex.metrics (namespace, subsystem, timestamp DESC)
        "#,
        )
        .execute(&self.pool)
        .await
        .map_err(|e| {
            MetricsError::Configuration(format!("Failed to create namespace index: {}", e))
        })?;

        Ok(())
    }

    /// Store a single metrics entry
    pub async fn store_metric(&self, entry: &MetricsEntry) -> Result<(), MetricsError> {
        sqlx::query!(
            r#"
            INSERT INTO sinex.metrics (id, metric_name, metric_type, value, labels, timestamp, namespace, subsystem)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            "#,
            entry.id.to_uuid(),
            entry.metric_name,
            entry.metric_type,
            entry.value,
            serde_json::to_value(&entry.labels).unwrap_or_default(),
            entry.timestamp,
            entry.namespace,
            entry.subsystem
        )
        .execute(&self.pool)
        .await
        .map_err(|e| MetricsError::Configuration(format!("Failed to store metric: {}", e)))?;

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
            .map_err(|e| MetricsError::Configuration(format!("Failed to begin transaction: {}", e)))?;

        for entry in &entries {
            sqlx::query!(
                r#"
                INSERT INTO sinex.metrics (id, metric_name, metric_type, value, labels, timestamp, namespace, subsystem)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                "#,
                entry.id.to_uuid(),
                entry.metric_name,
                entry.metric_type,
                entry.value,
                serde_json::to_value(&entry.labels).unwrap_or_default(),
                entry.timestamp,
                entry.namespace,
                entry.subsystem
            )
            .execute(&mut *tx)
            .await
            .map_err(|e| MetricsError::Configuration(format!("Failed to store metric in batch: {}", e)))?;
        }

        tx.commit().await
            .map_err(|e| MetricsError::Configuration(format!("Failed to commit metrics batch: {}", e)))?;

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
        // For now, return empty vec - proper implementation would require dynamic SQL
        // TODO: Implement proper dynamic query building
        Ok(vec![])
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
        // For now, return default aggregation - proper implementation would require complex SQL
        // TODO: Implement proper aggregation query
        Ok(MetricsAggregation {
            count: 0,
            sum: 0.0,
            avg: 0.0,
            min: 0.0,
            max: 0.0,
        })
    }

    /// Clear old metrics data (retention policy)
    pub async fn cleanup_old_metrics(
        &self,
        older_than: DateTime<Utc>,
    ) -> Result<u64, MetricsError> {
        let result = sqlx::query!(
            r#"
            DELETE FROM sinex.metrics
            WHERE timestamp < $1
            "#,
            older_than
        )
        .execute(&self.pool)
        .await
        .map_err(|e| {
            MetricsError::Configuration(format!("Failed to cleanup old metrics: {}", e))
        })?;

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

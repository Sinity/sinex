//! Metrics query registry for centralized metrics operations
//!
//! This module provides all database queries related to metrics storage,
//! retrieval, and management. All queries automatically handle ULID/UUID
//! conversion and provide consistent error handling.

use crate::query_builder::{QueryBuilder, QueryParam};
use crate::query_helpers::{db_error, DbResult};
use chrono::{DateTime, Utc};
use serde_json::Value as JsonValue;
use sinex_ulid::Ulid;
use sqlx::PgPool;

/// Metrics query registry with centralized metrics operations
pub struct MetricsQueries;

impl MetricsQueries {
    /// Create the metrics schema if it doesn't exist
    ///
    /// This uses raw SQL since it's DDL operations
    pub async fn create_schema(pool: &PgPool) -> DbResult<()> {
        sqlx::query("CREATE SCHEMA IF NOT EXISTS sinex")
            .execute(pool)
            .await
            .map_err(|e| db_error(e, "create sinex schema"))?;

        Ok(())
    }

    /// Create the metrics table if it doesn't exist
    ///
    /// This uses raw SQL since it's DDL operations
    pub async fn create_table(pool: &PgPool) -> DbResult<()> {
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
        .execute(pool)
        .await
        .map_err(|e| db_error(e, "create metrics table"))?;

        Ok(())
    }

    /// Create indices for efficient queries
    ///
    /// This uses raw SQL since it's DDL operations
    pub async fn create_indices(pool: &PgPool) -> DbResult<()> {
        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_sinex_metrics_name_time 
            ON sinex.metrics (metric_name, timestamp DESC)
        "#,
        )
        .execute(pool)
        .await
        .map_err(|e| db_error(e, "create metrics name-time index"))?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_sinex_metrics_namespace_subsystem 
            ON sinex.metrics (namespace, subsystem, timestamp DESC)
        "#,
        )
        .execute(pool)
        .await
        .map_err(|e| db_error(e, "create metrics namespace index"))?;

        Ok(())
    }

    /// Insert a new metric entry
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.execute(pool)`
    pub fn insert_metric(
        id: Ulid,
        metric_name: String,
        metric_type: String,
        value: f64,
        labels: JsonValue,
        timestamp: DateTime<Utc>,
        namespace: String,
        subsystem: String,
    ) -> QueryBuilder {
        QueryBuilder::insert("sinex.metrics")
            .columns(&[
                "id",
                "metric_name",
                "metric_type",
                "value",
                "labels",
                "timestamp",
                "namespace",
                "subsystem",
            ])
            .values(&[
                QueryParam::Ulid(id),
                QueryParam::String(metric_name),
                QueryParam::String(metric_type),
                QueryParam::Float(value),
                QueryParam::Json(labels),
                QueryParam::Timestamp(timestamp),
                QueryParam::String(namespace),
                QueryParam::String(subsystem),
            ])
    }

    /// Delete metrics older than a given timestamp
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.execute(pool)`
    pub fn delete_older_than(older_than: DateTime<Utc>) -> QueryBuilder {
        QueryBuilder::delete("sinex.metrics").where_op(
            "timestamp",
            "<",
            QueryParam::Timestamp(older_than),
        )
    }

    /// Query metrics with various filters
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_all::<MetricRecord>(pool)`
    pub fn query_metrics(
        metric_name: Option<String>,
        namespace: Option<String>,
        subsystem: Option<String>,
        start_time: Option<DateTime<Utc>>,
        end_time: Option<DateTime<Utc>>,
        limit: Option<i64>,
    ) -> QueryBuilder {
        let mut builder = QueryBuilder::select("sinex.metrics")
            .columns(&[
                "id::uuid as \"id!\"",
                "metric_name as \"metric_name!\"",
                "metric_type as \"metric_type!\"",
                "value as \"value!\"",
                "labels as \"labels!\"",
                "timestamp as \"timestamp!\"",
                "namespace as \"namespace!\"",
                "subsystem as \"subsystem!\"",
            ])
            .order_by("timestamp", "DESC");

        if let Some(name) = metric_name {
            builder = builder.where_eq("metric_name", QueryParam::String(name));
        }

        if let Some(ns) = namespace {
            builder = builder.where_eq("namespace", QueryParam::String(ns));
        }

        if let Some(sys) = subsystem {
            builder = builder.where_eq("subsystem", QueryParam::String(sys));
        }

        if let Some(start) = start_time {
            builder = builder.where_op("timestamp", ">=", QueryParam::Timestamp(start));
        }

        if let Some(end) = end_time {
            builder = builder.where_op("timestamp", "<=", QueryParam::Timestamp(end));
        }

        if let Some(lim) = limit {
            builder = builder.limit(lim);
        }

        builder
    }

    /// Get aggregated metrics (sum, avg, min, max, count)
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_one::<AggregationRecord>(pool)`
    pub fn get_aggregation(
        metric_name: String,
        namespace: Option<String>,
        subsystem: Option<String>,
        start_time: Option<DateTime<Utc>>,
        end_time: Option<DateTime<Utc>>,
    ) -> QueryBuilder {
        let mut builder = QueryBuilder::select("sinex.metrics")
            .columns(&[
                "COUNT(*) as \"count!\"",
                "SUM(value) as \"sum!\"",
                "AVG(value) as \"avg!\"",
                "MIN(value) as \"min!\"",
                "MAX(value) as \"max!\"",
            ])
            .where_eq("metric_name", QueryParam::String(metric_name));

        if let Some(ns) = namespace {
            builder = builder.where_eq("namespace", QueryParam::String(ns));
        }

        if let Some(sys) = subsystem {
            builder = builder.where_eq("subsystem", QueryParam::String(sys));
        }

        if let Some(start) = start_time {
            builder = builder.where_op("timestamp", ">=", QueryParam::Timestamp(start));
        }

        if let Some(end) = end_time {
            builder = builder.where_op("timestamp", "<=", QueryParam::Timestamp(end));
        }

        builder
    }
}

/// Record type for metrics query results
#[derive(Debug, sqlx::FromRow)]
pub struct MetricRecord {
    pub id: sqlx::types::Uuid,
    pub metric_name: String,
    pub metric_type: String,
    pub value: f64,
    pub labels: JsonValue,
    pub timestamp: DateTime<Utc>,
    pub namespace: String,
    pub subsystem: String,
}

/// Record type for aggregation results
#[derive(Debug, sqlx::FromRow)]
pub struct AggregationRecord {
    pub count: i64,
    pub sum: f64,
    pub avg: f64,
    pub min: f64,
    pub max: f64,
}

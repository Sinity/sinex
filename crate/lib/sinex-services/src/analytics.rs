#![doc = include_str!("../docs/analytics.md")]

//! Analytics service entry points for dashboards and reporting.

use crate::error::{Result as ServiceResult, SinexError};
use serde::Serialize;
use sinex_db::replay::state_machine::{ReplayOperation, ReplayState, ReplayStateMachine};
use sinex_db::repositories::common::{db_error, TimeBucketResult};
use sinex_db::DbPool;
use sinex_primitives::Pagination;
use sinex_primitives::Timestamp;
use sqlx::postgres::types::PgInterval;
use sqlx::{pool::PoolConnection, Postgres, Row};
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::timeout;

fn epoch_start() -> Timestamp {
    Timestamp::UNIX_EPOCH
}

pub struct AnalyticsService {
    pool: DbPool,
}

#[derive(Debug, Clone, Serialize)]
pub struct SourceStatistics {
    pub source: String,
    pub event_count: i64,
    pub event_type_count: i64,
    pub host_count: i64,
    pub first_event: Option<Timestamp>,
    pub last_event: Option<Timestamp>,
    pub avg_ingest_delay: Option<f64>,
}

const ANALYTICS_POOL_ACQUIRE_TIMEOUT: Duration = Duration::from_millis(40);

impl AnalyticsService {
    #[must_use]
    pub fn new(pool: DbPool) -> Self {
        Self { pool }
    }

    async fn acquire_connection(&self) -> ServiceResult<PoolConnection<Postgres>> {
        match timeout(ANALYTICS_POOL_ACQUIRE_TIMEOUT, self.pool.acquire()).await {
            Ok(Ok(conn)) => Ok(conn),
            Ok(Err(e)) => Err(SinexError::database(
                "Failed to acquire analytics database connection",
            )
            .with_source(e.to_string())),
            Err(_) => Err(SinexError::timeout("Analytics database pool exhausted")
                .with_duration(ANALYTICS_POOL_ACQUIRE_TIMEOUT)),
        }
    }

    /// Get event count by source for a time range
    pub async fn get_event_count_by_source(
        &self,
        start_time: Option<Timestamp>,
        end_time: Option<Timestamp>,
    ) -> ServiceResult<HashMap<String, i64>> {
        let mut conn = self.acquire_connection().await?;
        let start = start_time.unwrap_or_else(epoch_start);
        let rows = sqlx::query!(
            r#"
            SELECT
                source,
                COUNT(*) as "event_count!"
            FROM core.events
            WHERE ts_orig >= $1
              AND ($2::timestamptz IS NULL OR ts_orig <= $2)
            GROUP BY source
            ORDER BY COUNT(*) DESC
            LIMIT $3
            "#,
            *start,
            end_time.map(|t| *t),
            Pagination::DEFAULT_LIMIT
        )
        .fetch_all(&mut *conn)
        .await
        .map_err(|e| db_error(e, "get source activity"))?;

        let result = rows
            .into_iter()
            .map(|row| (row.source, row.event_count))
            .collect();

        Ok(result)
    }

    /// Get detailed statistics for each source.
    pub async fn get_source_statistics(
        &self,
        start_time: Option<Timestamp>,
        end_time: Option<Timestamp>,
        limit: i64,
    ) -> ServiceResult<Vec<SourceStatistics>> {
        let limit = limit.clamp(1, Pagination::MAX_LIMIT);
        let mut conn = self.acquire_connection().await?;
        let rows = sqlx::query!(
            r#"
            SELECT
                source,
                COUNT(*) as "event_count!",
                COUNT(DISTINCT event_type) as "event_type_count!",
                COUNT(DISTINCT host) as "host_count!",
                MIN(ts_ingest) as "first_event?",
                MAX(ts_ingest) as "last_event?",
                CAST(AVG(CASE WHEN ts_orig IS NOT NULL THEN EXTRACT(EPOCH FROM (ts_ingest - ts_orig)) ELSE NULL END) AS DOUBLE PRECISION) as "avg_ingest_delay?"
            FROM core.events
            WHERE ($2::timestamptz IS NULL OR ts_orig >= $2)
              AND ($3::timestamptz IS NULL OR ts_orig <= $3)
            GROUP BY source
            ORDER BY COUNT(*) DESC
            LIMIT $1
            "#,
            limit,
            start_time.map(|t| *t),
            end_time.map(|t| *t)
        )
        .fetch_all(&mut *conn)
        .await
        .map_err(|e| db_error(e, "get source statistics"))?;

        let stats = rows
            .into_iter()
            .map(|row| SourceStatistics {
                source: row.source,
                event_count: row.event_count,
                event_type_count: row.event_type_count,
                host_count: row.host_count,
                first_event: row.first_event.map(std::convert::Into::into),
                last_event: row.last_event.map(std::convert::Into::into),
                avg_ingest_delay: row.avg_ingest_delay,
            })
            .collect();

        Ok(stats)
    }

    /// Get event count by event type for a time range
    pub async fn get_event_count_by_type(
        &self,
        start_time: Option<Timestamp>,
        end_time: Option<Timestamp>,
    ) -> ServiceResult<HashMap<String, i64>> {
        let mut conn = self.acquire_connection().await?;
        let result = if let (Some(start), Some(end)) = (start_time, end_time) {
            let rows = sqlx::query!(
                r#"
                SELECT
                    event_type,
                    COUNT(*) as "count!"
                FROM core.events
                WHERE ts_orig >= $1 AND ts_orig < $2
                                    GROUP BY event_type
                                    ORDER BY COUNT(*) DESC
                                    LIMIT $3
                                "#,
                *start,
                *end,
                Pagination::DEFAULT_LIMIT
            )
            .fetch_all(&mut *conn)
            .await
            .map_err(|e| db_error(e, "count by type in range"))?;

            rows.into_iter()
                .map(|row| (row.event_type, row.count))
                .collect()
        } else {
            let rows = sqlx::query!(
                r#"
                SELECT
                    event_type,
                    COUNT(*) as "count!"
                FROM core.events
                GROUP BY event_type
                ORDER BY COUNT(*) DESC
                LIMIT $1
                "#,
                Pagination::DEFAULT_LIMIT
            )
            .fetch_all(&mut *conn)
            .await
            .map_err(|e| db_error(e, "count by type all time"))?;

            rows.into_iter()
                .map(|row| (row.event_type, row.count))
                .collect()
        };

        Ok(result)
    }

    /// Get time series data with configurable intervals
    pub async fn get_events_over_time(
        &self,
        start_time: Timestamp,
        end_time: Timestamp,
        interval_minutes: i32,
    ) -> ServiceResult<Vec<(Timestamp, i64)>> {
        if interval_minutes <= 0 {
            return Err(SinexError::validation("Interval must be positive")
                .with_context("interval_minutes", interval_minutes));
        }

        let expected_buckets =
            (end_time - start_time).whole_minutes() / i64::from(interval_minutes);
        if expected_buckets > Pagination::MAX_LIMIT {
            return Err(SinexError::validation("Time range too large for interval")
                .with_context("expected_buckets", expected_buckets)
                .with_context("max_buckets", Pagination::MAX_LIMIT));
        }

        let mut conn = self.acquire_connection().await?;
        let interval = minutes_to_interval(interval_minutes);

        let rows = sqlx::query_as!(
            TimeBucketResult,
            r#"
            SELECT
                time_bucket($1::interval, COALESCE(ts_orig, ts_ingest)) as "bucket!",
                COUNT(*) as "count!"
            FROM core.events
            WHERE COALESCE(ts_orig, ts_ingest) >= $2 AND COALESCE(ts_orig, ts_ingest) <= $3
            GROUP BY time_bucket($1::interval, COALESCE(ts_orig, ts_ingest))
            ORDER BY time_bucket($1::interval, COALESCE(ts_orig, ts_ingest)) ASC
            LIMIT $4
            "#,
            interval,
            *start_time,
            *end_time,
            Pagination::MAX_LIMIT
        )
        .fetch_all(&mut *conn)
        .await?;
        Ok(rows.into_iter().map(|r| (r.bucket, r.count)).collect())
    }

    /// Get most frequent commands from terminal events
    pub async fn get_top_commands(
        &self,
        start_time: Option<Timestamp>,
        end_time: Option<Timestamp>,
        limit: i32,
    ) -> ServiceResult<Vec<(String, i64)>> {
        let limit = i64::from(limit).clamp(1, Pagination::MAX_LIMIT);
        let mut conn = self.acquire_connection().await?;
        let rows = match (start_time, end_time) {
            (Some(start), Some(end)) => sqlx::query(
                r"
                SELECT
                    payload->>'command' as command,
                    COUNT(*) as count
                FROM core.events
                WHERE event_type IN ('command.executed','terminal.command','command.imported')
                  AND ts_orig >= $1
                  AND ts_orig < $2
                  AND payload->>'command' IS NOT NULL
                GROUP BY payload->>'command'
                ORDER BY count DESC
                LIMIT $3
                ",
            )
            .bind(*start)
            .bind(*end)
            .bind(limit)
            .fetch_all(&mut *conn)
            .await
            .map_err(|e| db_error(e, "top commands"))?,
            _ => sqlx::query(
                r"
                SELECT
                    payload->>'command' as command,
                    COUNT(*) as count
                FROM core.events
                WHERE event_type IN ('command.executed','terminal.command','command.imported')
                  AND payload->>'command' IS NOT NULL
                GROUP BY payload->>'command'
                ORDER BY count DESC
                LIMIT $1
                ",
            )
            .bind(limit)
            .fetch_all(&mut *conn)
            .await
            .map_err(|e| db_error(e, "top commands all time"))?,
        };

        let result = rows
            .into_iter()
            .map(|row| {
                let command = row
                    .try_get::<Option<String>, _>("command")
                    .map_err(|e| {
                        SinexError::database("Failed to extract command column")
                            .with_source(e.to_string())
                    })?
                    .unwrap_or_default();
                let count = row
                    .try_get::<Option<i64>, _>("count")
                    .map_err(|e| {
                        SinexError::database("Failed to extract count column")
                            .with_source(e.to_string())
                    })?
                    .unwrap_or(0);
                Ok((command, count))
            })
            .collect::<ServiceResult<Vec<_>>>()?;

        Ok(result)
    }

    /// Get most active time periods
    pub async fn activity_heatmap(
        &self,
        start_time: Option<Timestamp>,
        end_time: Option<Timestamp>,
        bucket_size_minutes: i32,
        limit: i32,
    ) -> ServiceResult<Vec<(Timestamp, i64)>> {
        let limit = i64::from(limit).clamp(1, Pagination::MAX_LIMIT);
        let mut conn = self.acquire_connection().await?;
        let interval = minutes_to_interval(bucket_size_minutes);

        let rows = sqlx::query_as!(
            TimeBucketResult,
            r#"
            SELECT
                time_bucket($1::interval, COALESCE(ts_orig, ts_ingest)) as "bucket!",
                COUNT(*) as "count!"
            FROM core.events
            WHERE ($3::timestamptz IS NULL OR ts_orig >= $3)
              AND ($4::timestamptz IS NULL OR ts_orig <= $4)
            GROUP BY time_bucket($1::interval, COALESCE(ts_orig, ts_ingest))
            ORDER BY COUNT(*) DESC
            LIMIT $2
            "#,
            interval,
            limit,
            start_time.map(|t| *t),
            end_time.map(|t| *t)
        )
        .fetch_all(&mut *conn)
        .await
        .map_err(|e| db_error(e, "get activity heatmap"))?;

        Ok(rows.into_iter().map(|r| (r.bucket, r.count)).collect())
    }

    /// List replay operations for automation reporting.
    pub async fn list_replay_operations(
        &self,
        state: Option<ReplayState>,
    ) -> ServiceResult<Vec<ReplayOperation>> {
        let mut conn = self.acquire_connection().await?;
        let replay = ReplayStateMachine::new(self.pool.clone());
        replay
            .list_operations_with_executor(&mut *conn, state)
            .await
            .map_err(|e| {
                SinexError::database("Failed to list replay operations").with_source(e.to_string())
            })
    }
}

/// Helper function to create `PgInterval` from minutes
fn minutes_to_interval(minutes: i32) -> PgInterval {
    PgInterval {
        months: 0,
        days: 0,
        microseconds: i64::from(minutes) * 60 * 1_000_000,
    }
}

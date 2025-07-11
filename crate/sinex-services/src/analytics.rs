//! Analytics service for event analysis and insights

use crate::error::ServiceResult;
use sinex_db::DbPool;
use sqlx::postgres::types::PgInterval;
use sqlx::types::chrono::{DateTime, Utc};
use std::collections::HashMap;

pub struct AnalyticsService {
    pool: DbPool,
}

impl AnalyticsService {
    pub fn new(pool: DbPool) -> Self {
        Self { pool }
    }

    /// Get event count by source for a time range
    pub async fn get_event_count_by_source(
        &self,
        start_time: Option<DateTime<Utc>>,
        end_time: Option<DateTime<Utc>>,
    ) -> ServiceResult<HashMap<String, i64>> {
        let mut result = HashMap::new();

        match (start_time, end_time) {
            (Some(start), Some(end)) => {
                let rows = sqlx::query!(
                    r#"
                    SELECT source, COUNT(*) as count
                    FROM raw.events
                    WHERE ts_ingest >= $1 AND ts_ingest <= $2
                    GROUP BY source
                    ORDER BY count DESC
                    "#,
                    start,
                    end
                )
                .fetch_all(&self.pool)
                .await?;

                for row in rows {
                    if let Some(count) = row.count {
                        result.insert(row.source, count);
                    }
                }
            }
            _ => {
                let rows = sqlx::query!(
                    r#"
                    SELECT source, COUNT(*) as count
                    FROM raw.events
                    GROUP BY source
                    ORDER BY count DESC
                    "#
                )
                .fetch_all(&self.pool)
                .await?;

                for row in rows {
                    if let Some(count) = row.count {
                        result.insert(row.source, count);
                    }
                }
            }
        };

        Ok(result)
    }

    /// Get event count by event type for a time range
    pub async fn get_event_count_by_type(
        &self,
        start_time: Option<DateTime<Utc>>,
        end_time: Option<DateTime<Utc>>,
    ) -> ServiceResult<HashMap<String, i64>> {
        let mut result = HashMap::new();

        match (start_time, end_time) {
            (Some(start), Some(end)) => {
                let rows = sqlx::query!(
                    r#"
                    SELECT event_type, COUNT(*) as count
                    FROM raw.events
                    WHERE ts_ingest >= $1 AND ts_ingest <= $2
                    GROUP BY event_type
                    ORDER BY count DESC
                    "#,
                    start,
                    end
                )
                .fetch_all(&self.pool)
                .await?;

                for row in rows {
                    if let Some(count) = row.count {
                        result.insert(row.event_type, count);
                    }
                }
            }
            _ => {
                let rows = sqlx::query!(
                    r#"
                    SELECT event_type, COUNT(*) as count
                    FROM raw.events
                    GROUP BY event_type
                    ORDER BY count DESC
                    "#
                )
                .fetch_all(&self.pool)
                .await?;

                for row in rows {
                    if let Some(count) = row.count {
                        result.insert(row.event_type, count);
                    }
                }
            }
        };

        Ok(result)
    }

    /// Get time series data with configurable intervals
    pub async fn get_events_over_time(
        &self,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
        interval_minutes: i32,
    ) -> ServiceResult<Vec<(DateTime<Utc>, i64)>> {
        let interval = PgInterval {
            months: 0,
            days: 0,
            microseconds: interval_minutes as i64 * 60 * 1_000_000,
        };

        let rows = sqlx::query!(
            r#"
            SELECT 
                time_bucket($1::interval, ts_ingest) as bucket,
                COUNT(*) as count
            FROM raw.events
            WHERE ts_ingest >= $2 AND ts_ingest <= $3
            GROUP BY bucket
            ORDER BY bucket ASC
            "#,
            interval,
            start_time,
            end_time
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .filter_map(|r| match (r.bucket, r.count) {
                (Some(b), Some(c)) => Some((b, c)),
                _ => None,
            })
            .collect())
    }

    /// Get most frequent commands from terminal events
    pub async fn get_top_commands(
        &self,
        start_time: Option<DateTime<Utc>>,
        end_time: Option<DateTime<Utc>>,
        limit: i32,
    ) -> ServiceResult<Vec<(String, i64)>> {
        let mut result = Vec::new();

        match (start_time, end_time) {
            (Some(start), Some(end)) => {
                let rows = sqlx::query!(
                    r#"
                    SELECT 
                        payload->>'command' as command,
                        COUNT(*) as count
                    FROM raw.events
                    WHERE event_type = 'command.executed'
                        AND payload ? 'command'
                        AND ts_ingest >= $1 AND ts_ingest <= $2
                    GROUP BY payload->>'command'
                    ORDER BY count DESC
                    LIMIT $3
                    "#,
                    start,
                    end,
                    limit as i64
                )
                .fetch_all(&self.pool)
                .await?;

                for row in rows {
                    if let (Some(cmd), Some(count)) = (row.command, row.count) {
                        result.push((cmd, count));
                    }
                }
            }
            _ => {
                let rows = sqlx::query!(
                    r#"
                    SELECT 
                        payload->>'command' as command,
                        COUNT(*) as count
                    FROM raw.events
                    WHERE event_type = 'command.executed'
                        AND payload ? 'command'
                    GROUP BY payload->>'command'
                    ORDER BY count DESC
                    LIMIT $1
                    "#,
                    limit as i64
                )
                .fetch_all(&self.pool)
                .await?;

                for row in rows {
                    if let (Some(cmd), Some(count)) = (row.command, row.count) {
                        result.push((cmd, count));
                    }
                }
            }
        };

        Ok(result)
    }

    /// Get most active time periods
    pub async fn activity_heatmap(
        &self,
        bucket_size_minutes: i32,
        limit: i32,
    ) -> ServiceResult<Vec<(DateTime<Utc>, i64)>> {
        let interval = PgInterval {
            months: 0,
            days: 0,
            microseconds: bucket_size_minutes as i64 * 60 * 1_000_000,
        };

        let rows = sqlx::query!(
            r#"
            SELECT 
                time_bucket($1::interval, ts_ingest) as bucket,
                COUNT(*) as count
            FROM raw.events
            GROUP BY bucket
            ORDER BY count DESC
            LIMIT $2
            "#,
            interval,
            limit as i64
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .filter_map(|r| match (r.bucket, r.count) {
                (Some(b), Some(c)) => Some((b, c)),
                _ => None,
            })
            .collect())
    }
}

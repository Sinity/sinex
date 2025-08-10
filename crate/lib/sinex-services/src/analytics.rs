//! Analytics service for event analysis and insights

use crate::error::ServiceResult;
use sinex_core::db::{repositories::DbPoolExt, DbPool};
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
                let rows = self.pool.events().get_source_activity(start).await?;

                for row in rows {
                    // Filter by end time on client side
                    if let Some(last_event) = row.last_event {
                        if last_event <= end {
                            result.insert(row.source, row.event_count);
                        }
                    }
                }
            }
            _ => {
                // For all-time stats, use a timestamp far in the past
                let very_old = DateTime::from_timestamp(0, 0).unwrap();
                let rows = self.pool.events().get_source_activity(very_old).await?;

                for row in rows {
                    result.insert(row.source, row.event_count);
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
                let rows = self
                    .pool
                    .events()
                    .count_by_type_in_range(start, end)
                    .await?;

                for row in rows {
                    result.insert(row.event_type, row.count);
                }
            }
            _ => {
                let rows = self.pool.events().count_by_type_all_time().await?;

                for row in rows {
                    result.insert(row.event_type, row.count);
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

        let rows = self
            .pool
            .events()
            .get_events_over_time(start_time, end_time, interval)
            .await?;

        Ok(rows.into_iter().map(|r| (r.bucket, r.count)).collect())
    }

    /// Get most frequent commands from terminal events
    pub async fn get_top_commands(
        &self,
        start_time: Option<DateTime<Utc>>,
        end_time: Option<DateTime<Utc>>,
        limit: i32,
    ) -> ServiceResult<Vec<(String, i64)>> {
        let commands = match (start_time, end_time) {
            (Some(start), Some(end)) => {
                self.pool
                    .events()
                    .top_commands(start, end, limit as i64)
                    .await?
            }
            _ => {
                self.pool
                    .events()
                    .top_commands_all_time(limit as i64)
                    .await?
            }
        };

        Ok(commands.into_iter().map(|c| (c.command, c.count)).collect())
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

        let rows = self
            .pool
            .events()
            .get_activity_heatmap(interval, limit as i64)
            .await?;

        Ok(rows.into_iter().map(|r| (r.bucket, r.count)).collect())
    }
}

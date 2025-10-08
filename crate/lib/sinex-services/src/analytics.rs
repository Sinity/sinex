//! Analytics service for event analysis and insights

use crate::error::Result as ServiceResult;
use sinex_core::db::{repositories::DbPoolExt, DbPool};
use sqlx::postgres::types::PgInterval;
use sqlx::types::chrono::{DateTime, Utc};
use std::collections::HashMap;

/// Unix epoch start time as a constant
const EPOCH_START: DateTime<Utc> = DateTime::from_timestamp(0, 0).unwrap();

pub struct AnalyticsService {
    pool: DbPool,
}

impl AnalyticsService {
    pub fn new(pool: DbPool) -> Self {
        Self { pool }
    }

    /// Common helper for time range filtering logic
    fn apply_time_range_filter<T, F>(
        data: Vec<T>,
        end_time: Option<DateTime<Utc>>,
        get_timestamp: F,
    ) -> Vec<T>
    where
        F: Fn(&T) -> Option<DateTime<Utc>>,
    {
        if let Some(end) = end_time {
            data.into_iter()
                .filter(|item| get_timestamp(item).map(|ts| ts <= end).unwrap_or(false))
                .collect()
        } else {
            data
        }
    }

    /// Get event count by source for a time range
    pub async fn get_event_count_by_source(
        &self,
        start_time: Option<DateTime<Utc>>,
        end_time: Option<DateTime<Utc>>,
    ) -> ServiceResult<HashMap<String, i64>> {
        let start = start_time.unwrap_or(EPOCH_START);
        let rows = self.pool.events().get_source_activity(start, None).await?;

        // Apply client-side end time filtering
        let filtered_rows = Self::apply_time_range_filter(rows, end_time, |row| row.last_event);

        let result = filtered_rows
            .into_iter()
            .map(|row| (row.source, row.event_count))
            .collect();

        Ok(result)
    }

    /// Get event count by event type for a time range
    pub async fn get_event_count_by_type(
        &self,
        start_time: Option<DateTime<Utc>>,
        end_time: Option<DateTime<Utc>>,
    ) -> ServiceResult<HashMap<String, i64>> {
        let rows = match (start_time, end_time) {
            (Some(start), Some(end)) => {
                self.pool
                    .events()
                    .count_by_type_in_range(start, end)
                    .await?
            }
            _ => self.pool.events().count_by_type_all_time(None).await?,
        };

        let result = rows
            .into_iter()
            .map(|row| (row.event_type, row.count))
            .collect();

        Ok(result)
    }

    /// Get time series data with configurable intervals
    pub async fn get_events_over_time(
        &self,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
        interval_minutes: i32,
    ) -> ServiceResult<Vec<(DateTime<Utc>, i64)>> {
        let interval = minutes_to_interval(interval_minutes);

        let rows = self
            .pool
            .events()
            .get_events_over_time(start_time, end_time, interval, None)
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

        let result = commands.into_iter().map(|c| (c.command, c.count)).collect();

        Ok(result)
    }

    /// Get most active time periods
    pub async fn activity_heatmap(
        &self,
        bucket_size_minutes: i32,
        limit: i32,
    ) -> ServiceResult<Vec<(DateTime<Utc>, i64)>> {
        let interval = minutes_to_interval(bucket_size_minutes);

        let rows = self
            .pool
            .events()
            .get_activity_heatmap(interval, limit as i64)
            .await?;

        Ok(rows.into_iter().map(|r| (r.bucket, r.count)).collect())
    }
}

/// Helper function to create PgInterval from minutes
fn minutes_to_interval(minutes: i32) -> PgInterval {
    PgInterval {
        months: 0,
        days: 0,
        microseconds: minutes as i64 * 60 * 1_000_000,
    }
}

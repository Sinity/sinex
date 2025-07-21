//! Analytics service for event analysis and insights

use crate::error::ServiceResult;
use sinex_db::queries::{OperationQueries, EventQueries};
use sinex_db::queries::events::TimeBucketRecord;
use sinex_db::DbPool;
use sqlx::postgres::types::PgInterval;
use sqlx::types::chrono::{DateTime, Utc};
use sqlx::FromRow;
use std::collections::HashMap;

#[derive(FromRow)]
struct SourceActivityRow {
    source: String,
    event_count: i64,
    #[allow(dead_code)] // Used by database query but not in code
    event_types: i64,
    last_event: DateTime<Utc>,
    #[allow(dead_code)] // Used by database query but not in code
    first_event: DateTime<Utc>,
}

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
                let rows: Vec<SourceActivityRow> = OperationQueries::get_source_activity(start)
                    .fetch_all(&self.pool)
                    .await?;

                for row in rows {
                    // Filter by end time on client side
                    if row.last_event <= end {
                        result.insert(row.source, row.event_count);
                    }
                }
            }
            _ => {
                // For all-time stats, use a timestamp far in the past
                let very_old = DateTime::from_timestamp(0, 0).unwrap();
                let rows: Vec<SourceActivityRow> = OperationQueries::get_source_activity(very_old)
                    .fetch_all(&self.pool)
                    .await?;

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
                #[derive(sqlx::FromRow)]
                struct CountRow {
                    event_type: String,
                    count: i64,
                }
                
                let rows: Vec<CountRow> = EventQueries::count_by_type_in_range(start, end)
                    .fetch_all(&self.pool)
                    .await?;

                for row in rows {
                    result.insert(row.event_type, row.count);
                }
            }
            _ => {
                #[derive(sqlx::FromRow)]
                struct CountRow {
                    event_type: String,
                    count: i64,
                }
                
                let rows: Vec<CountRow> = EventQueries::count_by_type_all_time()
                    .fetch_all(&self.pool)
                    .await?;

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

        let rows = EventQueries::get_events_over_time(&self.pool, start_time, end_time, interval)
            .await?;

        Ok(rows
            .into_iter()
            .map(|r| (r.bucket, r.count))
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

        #[derive(sqlx::FromRow)]
        struct CommandRow {
            command: String,
            count: i64,
        }

        let rows: Vec<CommandRow> = match (start_time, end_time) {
            (Some(start), Some(end)) => {
                EventQueries::top_commands_in_range(start, end, limit as i64)
                    .fetch_all(&self.pool)
                    .await?
            }
            _ => {
                EventQueries::top_commands_all_time(limit as i64)
                    .fetch_all(&self.pool)
                    .await?
            }
        };

        for row in rows {
            result.push((row.command, row.count));
        }

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

        let rows = EventQueries::get_activity_heatmap(&self.pool, interval, limit as i64)
            .await?;

        Ok(rows
            .into_iter()
            .map(|r| (r.bucket, r.count))
            .collect())
    }
}

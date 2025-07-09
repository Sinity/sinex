//! Analytics service for event analysis and insights

use crate::error::{ServiceError, ServiceResult};
use sinex_db::DbPool;
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
    pub async fn event_count_by_source(
        &self,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> ServiceResult<HashMap<String, i64>> {
        let rows = sqlx::query!(
            r#"
            SELECT source, COUNT(*) as count
            FROM raw.events
            WHERE ts_ingest >= $1 AND ts_ingest <= $2
            GROUP BY source
            ORDER BY count DESC
            "#,
            start_time,
            end_time
        )
        .fetch_all(&self.pool)
        .await?;
        
        let mut result = HashMap::new();
        for row in rows {
            if let Some(count) = row.count {
                result.insert(row.source, count);
            }
        }
        
        Ok(result)
    }
    
    /// Get most active time periods
    pub async fn activity_heatmap(
        &self,
        bucket_size_minutes: i32,
        limit: i32,
    ) -> ServiceResult<Vec<(DateTime<Utc>, i64)>> {
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
            format!("{} minutes", bucket_size_minutes),
            limit
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
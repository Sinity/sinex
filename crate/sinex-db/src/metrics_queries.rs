//! Metrics operations following the clean API pattern
//!
//! This module provides metrics-related database operations with proper error handling
//! and clean API design, following the exact same pattern as existing *_correct.rs files.

use crate::DbPoolRef;
use anyhow::Result;
use serde::{Deserialize, Serialize};

/// Queue depth metrics for monitoring
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueDepthMetrics {
    pub target_agent_name: String,
    pub queue_depth: i64,
    pub pending_count: i64,
    pub processing_count: i64,
    pub failed_count: i64,
    pub succeeded_count: i64,
}

/// Calculate queue depth metrics following the exact same pattern as existing correct functions
pub async fn calculate_queue_depth_metrics(pool: DbPoolRef<'_>) -> Result<Vec<QueueDepthMetrics>> {
    let records = sqlx::query!(
        r#"
        SELECT 
            target_agent_name,
            COUNT(*) as queue_depth,
            SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) as pending_count,
            SUM(CASE WHEN status = 'processing' THEN 1 ELSE 0 END) as processing_count,
            SUM(CASE WHEN status = 'failed_retryable' THEN 1 ELSE 0 END) as failed_count,
            SUM(CASE WHEN status = 'succeeded' THEN 1 ELSE 0 END) as succeeded_count
        FROM sinex_schemas.work_queue
        GROUP BY target_agent_name
        ORDER BY queue_depth DESC
        "#
    )
    .fetch_all(pool)
    .await?;
    
    Ok(records
        .into_iter()
        .map(|record| QueueDepthMetrics {
            target_agent_name: record.target_agent_name,
            queue_depth: record.queue_depth.unwrap_or(0),
            pending_count: record.pending_count.unwrap_or(0),
            processing_count: record.processing_count.unwrap_or(0),
            failed_count: record.failed_count.unwrap_or(0),
            succeeded_count: record.succeeded_count.unwrap_or(0),
        })
        .collect())
}
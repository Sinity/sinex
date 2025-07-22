// Worker test utilities for work queue testing
//
// Provides utilities for testing work queue functionality including:
// - Creating work items
// - Claiming and processing work items
// - Testing worker idempotency
//
// NOTE: Most work queue operations use direct SQL due to complex locking requirements
// (FOR UPDATE SKIP LOCKED) that are not abstracted by the centralized query system.

use crate::common::prelude::*;
use serde_json::Value;
use sinex_db::queries::{EventQueries, CheckpointQueries};
use sinex_db::query_builder::{QueryBuilder, QueryParam};

/// A work queue item for testing
#[derive(Debug, Clone)]
pub struct WorkQueueItem {
    pub queue_id: Ulid,
    pub agent_name: String,
    pub worker_name: String,
    pub payload: Value,
    pub status: String,
    pub created_at: DateTime<Utc>,
    pub claimed_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
}

/// Create a work item for testing
/// DEPRECATED: Work queue table removed in satellite architecture - using Redis Streams instead
pub async fn create_work_item(_pool: &DbPool, _agent_name: &str, _event_id: Ulid) -> AnyhowResult<Ulid> {
    // Work queue table deprecated - returning dummy ULID for compatibility
    Ok(Ulid::new())
}

/// Claim work queue items for processing
/// DEPRECATED: Work queue table removed in satellite architecture - using Redis Streams instead
pub async fn claim_work_queue_items(
    _pool: &DbPool,
    _agent_name: &str,
    _worker_name: &str,
    _max_items: usize,
) -> AnyhowResult<Vec<WorkQueueItem>> {
    // Work queue table deprecated - returning empty vec for compatibility
    Ok(Vec::new())
}

/// Complete a work queue item
/// DEPRECATED: Work queue table removed in satellite architecture - using Redis Streams instead
pub async fn complete_work_queue_item(_pool: &DbPool, _queue_id: Ulid) -> TestResult {
    // Work queue table deprecated - no-op for compatibility
    Ok(())
}

/// Get work queue status
/// DEPRECATED: Work queue table removed in satellite architecture - using Redis Streams instead
pub async fn get_work_queue_status(
    _pool: &DbPool,
    _agent_name: &str,
) -> AnyhowResult<(usize, usize, usize)> {
    // Work queue table deprecated - returning zeros for compatibility
    Ok((0, 0, 0))
}

/// Clear all work items for an agent (for test cleanup)
/// DEPRECATED: Work queue table removed in satellite architecture - using Redis Streams instead
pub async fn clear_work_queue(_pool: &DbPool, _agent_name: &str) -> TestResult {
    // Work queue table deprecated - no-op for compatibility
    Ok(())
}

/// Deprecated: work queue table removed in satellite architecture
/// This function is now a no-op for compatibility
pub async fn ensure_work_queue_table(_pool: &DbPool) -> TestResult {
    // Work queue table deprecated - using Redis Streams instead
    Ok(())
}

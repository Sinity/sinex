//! Pipeline coordination and synchronization.

use crate::sandbox::prelude::*;
use crate::sandbox::timing::Timeouts;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Overrides for event metadata during test publishing.
#[derive(Debug, Clone, Default)]
pub struct EventOverrides {
    /// Overide the event timestamp (RFC3339 string).
    pub ts_orig: Option<String>,
    /// Override the event ID.
    pub id: Option<Uuid>,
}

static PIPELINE_SEMAPHORE: std::sync::LazyLock<Arc<Semaphore>> = std::sync::LazyLock::new(|| {
    let permits = std::env::var("SINEX_TEST_PIPELINE_CONCURRENCY")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(8);
    Arc::new(Semaphore::new(permits))
});

/// Acquire a permit for running a pipeline test.
///
/// This limits the number of concurrent pipeline tests to prevent
/// memory exhaustion from multiple ingestd instances.
pub async fn acquire_pipeline_permit(_namespace: &str) -> TestResult<OwnedSemaphorePermit> {
    PIPELINE_SEMAPHORE
        .clone()
        .acquire_owned()
        .await
        .map_err(|e| eyre!("Failed to acquire pipeline permit: {}", e))
}

/// Wait until a published event is persisted in the database.
pub async fn wait_for_event_persisted(
    ctx: &Sandbox,
    event_id: impl Into<EventId>,
) -> TestResult<()> {
    let event_id = event_id.into();
    // Under nextest each test is a separate process, so the in-process semaphore
    // doesn't limit real concurrency (controlled by nextest test-threads = 32).
    // With 32 concurrent ingestd processes + DB slots, events regularly need
    // 15-25s to flow through NATS → ingestd → DB.
    WaitHelpers::wait_for_event_id(&ctx.pool, event_id, Timeouts::STANDARD).await
}

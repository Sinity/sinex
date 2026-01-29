//! Pipeline coordination and synchronization.

use crate::sandbox::prelude::*;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Overrides for event metadata during test publishing.
#[derive(Debug, Clone, Default)]
pub struct EventOverrides {
    /// Overide the event timestamp (RFC3339 string).
    pub ts_orig: Option<String>,
    /// Override the event ID.
    pub id: Option<Ulid>,
}

static PIPELINE_SEMAPHORE: Lazy<Arc<Semaphore>> = Lazy::new(|| {
    let permits = std::env::var("SINEX_TEST_PIPELINE_CONCURRENCY")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(4);
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
    // Pipeline events should persist quickly (within 5 seconds)
    WaitHelpers::wait_for_event_id(&ctx.pool, event_id, 5).await
}

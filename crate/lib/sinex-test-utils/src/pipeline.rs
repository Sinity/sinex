//! Pipeline test utilities - shared NATS handles and concurrency control.
//!
//! This module provides process-wide shared NATS instances and concurrency
//! limiting for pipeline tests. The main test harness is `PipelineScope`.

use crate::nats::{shared_ephemeral_nats, EphemeralNats, SharedNatsProfile};
use crate::timing_utils::WaitHelpers;
use crate::{TestContext, TestResult};
use once_cell::sync::Lazy;
use sinex_core::types::error::SinexError;
use sinex_core::EventId;
use std::cmp;
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tracing::info;

static PIPELINE_CONCURRENCY_LIMIT: Lazy<usize> = Lazy::new(default_pipeline_concurrency_limit);
static PIPELINE_CONCURRENCY_SEMAPHORE: Lazy<Arc<Semaphore>> =
    Lazy::new(|| Arc::new(Semaphore::new(*PIPELINE_CONCURRENCY_LIMIT)));

/// Lazily start (or reuse) a process-wide EphemeralNats server.
pub async fn shared_nats_handle() -> TestResult<Arc<EphemeralNats>> {
    let handle = shared_ephemeral_nats(SharedNatsProfile::Default).await?;
    Ok(handle)
}

/// Lazily start (or reuse) the TLS-enabled shared NATS server profile.
pub async fn shared_secure_nats_handle() -> TestResult<Arc<EphemeralNats>> {
    let handle = shared_ephemeral_nats(SharedNatsProfile::SecureTls).await?;
    Ok(handle)
}

/// Wait for a specific event to be persisted in the database.
pub(crate) async fn wait_for_event_persisted(
    ctx: &TestContext,
    event_ulid: sinex_core::Ulid,
) -> TestResult<()> {
    let event_id: EventId = event_ulid.into();
    let timeout_secs = 5;
    WaitHelpers::wait_for_event_id(&ctx.pool, event_id.clone(), timeout_secs).await?;
    Ok(())
}

fn default_pipeline_concurrency_limit() -> usize {
    let cpu_count = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);
    // Keep default small to avoid starving ingestd/JetStream on busy hosts.
    let heuristic = cmp::max(1, cpu_count / 6);
    heuristic.clamp(1, 6)
}

/// Acquire a permit for running a pipeline test.
///
/// This limits concurrency to avoid overwhelming the system with too many
/// simultaneous ingestd instances.
pub(crate) async fn acquire_pipeline_permit(namespace: &str) -> TestResult<OwnedSemaphorePermit> {
    if PIPELINE_CONCURRENCY_SEMAPHORE.available_permits() == 0 {
        info!(
            target: "pipeline",
            namespace,
            limit = *PIPELINE_CONCURRENCY_LIMIT,
            "waiting for available pipeline slot"
        );
    }

    let permit = PIPELINE_CONCURRENCY_SEMAPHORE
        .clone()
        .acquire_owned()
        .await
        .map_err(|_| SinexError::unknown("pipeline concurrency semaphore closed"))?;

    info!(
        target: "pipeline",
        namespace,
        remaining = PIPELINE_CONCURRENCY_SEMAPHORE.available_permits(),
        limit = *PIPELINE_CONCURRENCY_LIMIT,
        "acquired pipeline slot"
    );

    Ok(permit)
}

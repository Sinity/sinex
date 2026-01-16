use crate::nats::{shared_ephemeral_nats, EphemeralNats, SharedNatsProfile};
use crate::satellite_management_utils::{start_test_ingestd_with_config, TestIngestdConfig};
use crate::timing_utils::WaitHelpers;
use crate::{EventOverrides, TestContext, TestResult, TestSatellitePublisher};
use once_cell::sync::Lazy;
use sinex_core::types::error::SinexError;
use sinex_core::EventId;
use std::cmp;
use std::sync::Arc;
use std::time::Instant;
use tokio::runtime::Handle;
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

/// Harness that spins up ingestd + JetStream and lets tests seed events through the real pipeline.
pub struct PipelineHarness<'ctx> {
    ctx: &'ctx TestContext,
    ingestd: Option<crate::satellite_management_utils::TestIngestdHandle>,
    namespace: String,
    pipeline_permit: Option<OwnedSemaphorePermit>,
}

impl<'ctx> PipelineHarness<'ctx> {
    pub(crate) async fn new(ctx: &'ctx TestContext) -> TestResult<Self> {
        let nats = ctx.nats_handle()?;
        let namespace = ctx.pipeline_namespace().prefix().to_string();
        let pipeline_permit = Some(acquire_pipeline_permit(&namespace).await?);
        let mut config = TestIngestdConfig {
            nats_url: nats.client_url().to_string(),
            database_url: ctx.database_url().to_string(),
            work_dir: None,
            namespace: Some(namespace.clone()),
            ..Default::default()
        };
        config.batch_size = 32;
        config.consumer_fetch_max_messages = 32;
        config.consumer_fetch_timeout_ms = 200;
        let ingestd = start_test_ingestd_with_config(config, Some(ctx)).await?;
        Ok(Self {
            ctx,
            ingestd: Some(ingestd),
            namespace,
            pipeline_permit,
        })
    }

    /// Publish a test event through JetStream and wait until ingestd persists it.
    pub async fn publish_event(
        &self,
        source: &str,
        event_type: &str,
        payload: serde_json::Value,
    ) -> TestResult<EventId> {
        self.publish_event_with_overrides(source, event_type, payload, EventOverrides::default())
            .await
    }

    /// Publish a test event with overrides (ts_orig, id, etc.) and wait until persisted.
    pub async fn publish_event_with_overrides(
        &self,
        source: &str,
        event_type: &str,
        payload: serde_json::Value,
        overrides: EventOverrides,
    ) -> TestResult<EventId> {
        let op_start = Instant::now();
        let publisher = TestSatellitePublisher::with_namespace(
            self.ctx.nats_client(),
            source.to_string(),
            Some(self.namespace.clone()),
        );
        let publish_start = Instant::now();
        let event_id = publisher
            .publish_event_with_overrides(event_type, payload, overrides)
            .await?;
        let publish_ms = publish_start.elapsed().as_millis();
        let wait_start = Instant::now();
        wait_for_event_persisted(self.ctx, event_id).await?;
        let wait_ms = wait_start.elapsed().as_millis();
        let total_ms = op_start.elapsed().as_millis();
        info!(
            target: "pipeline_harness",
            source,
            event_type,
            publish_ms,
            wait_ms,
            total_ms,
            "pipeline harness publish complete"
        );
        println!(
            "[pipeline_harness] {source}::{event_type} publish={}ms wait={}ms total={}ms",
            publish_ms, wait_ms, total_ms
        );
        Ok(event_id.into())
    }

    /// Stop the ingestd instance backing this harness.
    pub async fn shutdown(mut self) -> TestResult<()> {
        if let Some(mut ingestd) = self.ingestd.take() {
            ingestd.stop().await?;
        }
        // Explicitly drop the concurrency permit as soon as the pipeline is shut down.
        self.pipeline_permit.take();
        Ok(())
    }
}

impl Drop for PipelineHarness<'_> {
    fn drop(&mut self) {
        // Drop the permit before tearing down ingestd so the next test can start spinning up.
        self.pipeline_permit.take();

        if let Some(mut ingestd) = self.ingestd.take() {
            if let Ok(handle) = Handle::try_current() {
                handle.spawn(async move {
                    let _ = ingestd.stop().await;
                });
            } else {
                let _ = futures::executor::block_on(ingestd.stop());
            }
        }
    }
}

async fn wait_for_event_persisted(
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

async fn acquire_pipeline_permit(namespace: &str) -> TestResult<OwnedSemaphorePermit> {
    if PIPELINE_CONCURRENCY_SEMAPHORE.available_permits() == 0 {
        info!(
            target: "pipeline_harness",
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
        target: "pipeline_harness",
        namespace,
        remaining = PIPELINE_CONCURRENCY_SEMAPHORE.available_permits(),
        limit = *PIPELINE_CONCURRENCY_LIMIT,
        "acquired pipeline slot"
    );

    Ok(permit)
}
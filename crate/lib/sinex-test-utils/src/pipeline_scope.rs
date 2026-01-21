//! PipelineScope - unified test harness for pipeline integration tests.
//!
//! This module combines the previous PipelineHarness functionality directly,
//! providing a single type for pipeline tests.

use crate::ingestd_test_utils::{start_test_ingestd_with_config, TestIngestdConfig};
use crate::pipeline::{acquire_pipeline_permit, wait_for_event_persisted};
use crate::pipeline_namespace::PipelineNamespace;
use crate::timing_utils::{WaitHelpers, DEFAULT_WAIT_SECS};
use crate::{EventOverrides, TestContext, TestNodePublisher, TestResult};
use chrono::{DateTime, Utc};
use sinex_core::{EventId, EventType};
use std::collections::VecDeque;
use std::time::Instant;
use tokio::runtime::Handle;
use tokio::sync::OwnedSemaphorePermit;
use tracing::info;

/// PipelineScope provides a complete pipeline test harness with ingestd, JetStream,
/// and automatic cleanup.
///
/// This is the primary type for tests that need to exercise the full ingestion pipeline.
pub struct PipelineScope<'ctx> {
    ctx: &'ctx TestContext,
    ingestd: Option<crate::ingestd_test_utils::TestIngestdHandle>,
    namespace: String,
    pipeline_permit: Option<OwnedSemaphorePermit>,
}

impl<'ctx> PipelineScope<'ctx> {
    /// Create a pipeline scope that enforces shared NATS, resets the DB slot,
    /// and starts ingestd.
    pub async fn new(ctx: &'ctx TestContext) -> TestResult<Self> {
        ctx.ensure_shared_nats()?;
        ctx.reset_database_slot().await?;

        let nats = ctx.nats_handle()?;
        let namespace = ctx.pipeline_namespace().prefix().to_string();
        let pipeline_permit = Some(acquire_pipeline_permit(&namespace).await?);

        let mut config = TestIngestdConfig {
            nats: nats.connection_config(),
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

    /// Access the underlying TestContext.
    pub fn ctx(&self) -> &TestContext {
        self.ctx
    }

    /// Access the per-test pipeline namespace.
    pub fn namespace(&self) -> &PipelineNamespace {
        self.ctx.pipeline_namespace()
    }

    /// Build a namespaced JetStream subject.
    pub fn subject(&self, base: &str) -> String {
        self.namespace().subject(base)
    }

    /// Build a namespaced JetStream stream name.
    pub fn stream(&self, base: &str) -> String {
        self.namespace().stream(base)
    }

    /// Build a namespaced JetStream consumer name.
    pub fn consumer_name(&self, base: &str) -> String {
        self.namespace().consumer_name(base)
    }

    /// Create a publisher that always uses the pipeline namespace.
    pub fn publisher(&self, source: impl Into<String>) -> TestNodePublisher {
        TestNodePublisher::with_namespace(
            self.ctx.nats_client(),
            source,
            Some(self.namespace.clone()),
        )
    }

    /// Publish a test event through JetStream and wait until ingestd persists it.
    pub async fn publish(
        &self,
        source: &str,
        event_type: &str,
        payload: serde_json::Value,
    ) -> TestResult<EventId> {
        self.publish_with_overrides(source, event_type, payload, EventOverrides::default())
            .await
    }

    /// Publish a test event with overrides (ts_orig, id, etc.) and wait until persisted.
    pub async fn publish_with_overrides(
        &self,
        source: &str,
        event_type: &str,
        payload: serde_json::Value,
        overrides: EventOverrides,
    ) -> TestResult<EventId> {
        let op_start = Instant::now();
        let publisher = TestNodePublisher::with_namespace(
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
            target: "pipeline_scope",
            source,
            event_type,
            publish_ms,
            wait_ms,
            total_ms,
            "pipeline publish complete"
        );
        Ok(event_id.into())
    }

    /// Publish an event with a concrete timestamp and wait until persisted.
    pub async fn publish_with_timestamp(
        &self,
        source: &str,
        event_type: &str,
        payload: serde_json::Value,
        timestamp: DateTime<Utc>,
    ) -> TestResult<EventId> {
        let overrides = EventOverrides {
            ts_orig: Some(timestamp.to_rfc3339()),
            ..Default::default()
        };
        self.publish_with_overrides(source, event_type, payload, overrides)
            .await
    }

    /// Wait for a specific number of events to be persisted.
    pub async fn wait_for_event_count(&self, expected_count: usize) -> TestResult<usize> {
        WaitHelpers::wait_for_event_count(&self.ctx.pool, expected_count, DEFAULT_WAIT_SECS).await
    }

    /// Wait for events from a specific source to be persisted.
    pub async fn wait_for_source_events(
        &self,
        source: &str,
        expected_count: usize,
    ) -> TestResult<usize> {
        WaitHelpers::wait_for_source_events(
            &self.ctx.pool,
            source,
            expected_count,
            DEFAULT_WAIT_SECS,
        )
        .await
    }

    /// Wait for events of a specific type to be persisted.
    pub async fn wait_for_event_type_events(
        &self,
        event_type: &EventType,
        expected_count: usize,
    ) -> TestResult<usize> {
        WaitHelpers::wait_for_event_type_events(
            &self.ctx.pool,
            event_type,
            expected_count,
            DEFAULT_WAIT_SECS,
        )
        .await
    }

    /// Wait until a specific event id is persisted.
    pub async fn wait_for_event_id(&self, event_id: EventId) -> TestResult<()> {
        WaitHelpers::wait_for_event_id(&self.ctx.pool, event_id, DEFAULT_WAIT_SECS).await
    }

    // ========================================================================
    // Batch Publishing Methods
    // ========================================================================

    /// Publish multiple events and wait for all to be persisted.
    pub async fn publish_batch<F>(
        &self,
        count: usize,
        source: &str,
        event_type: &str,
        payload_fn: F,
    ) -> TestResult<Vec<EventId>>
    where
        F: Fn(usize) -> serde_json::Value,
    {
        let mut ids = Vec::with_capacity(count);
        for i in 0..count {
            let payload = payload_fn(i);
            let id = self.publish(source, event_type, payload).await?;
            ids.push(id);
        }
        self.wait_for_source_events(source, count).await?;
        Ok(ids)
    }

    /// Publish multiple events with a simple incrementing payload.
    pub async fn publish_batch_simple(
        &self,
        count: usize,
        source: &str,
        event_type: &str,
    ) -> TestResult<Vec<EventId>> {
        self.publish_batch(
            count,
            source,
            event_type,
            |i| serde_json::json!({ "index": i }),
        )
        .await
    }

    /// Publish multiple events with timestamps spread over a time range.
    pub async fn publish_batch_with_timestamps<F>(
        &self,
        count: usize,
        source: &str,
        event_type: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        payload_fn: F,
    ) -> TestResult<Vec<EventId>>
    where
        F: Fn(usize) -> serde_json::Value,
    {
        if count == 0 {
            return Ok(vec![]);
        }

        let duration = end.signed_duration_since(start);
        let step = if count > 1 {
            duration / (count as i32 - 1)
        } else {
            chrono::Duration::zero()
        };

        let mut ids = Vec::with_capacity(count);
        for i in 0..count {
            let timestamp = start + step * (i as i32);
            let payload = payload_fn(i);
            let id = self
                .publish_with_timestamp(source, event_type, payload, timestamp)
                .await?;
            ids.push(id);
        }

        self.wait_for_source_events(source, count).await?;
        Ok(ids)
    }

    /// Stop the ingestd instance backing this scope.
    pub async fn shutdown(mut self) -> TestResult<()> {
        if let Some(mut ingestd) = self.ingestd.take() {
            ingestd.stop().await?;
        }
        self.pipeline_permit.take();
        Ok(())
    }

    fn dump_failure_logs(&self) {
        const LOG_TAIL: usize = 200;
        eprintln!(
            "⚠️  PipelineScope failure diagnostics (namespace={})",
            self.namespace().prefix()
        );

        if let Ok(nats) = self.ctx.nats_handle() {
            match nats.log_tail(LOG_TAIL) {
                Some(tail) if !tail.is_empty() => {
                    eprintln!("--- nats log tail ---\n{tail}");
                }
                _ => {
                    eprintln!("--- nats log tail unavailable ---");
                }
            }
        }

        let logs = self.ctx.captured_logs();
        let mut ingestd_lines: VecDeque<String> = VecDeque::with_capacity(LOG_TAIL);
        for line in logs {
            if !line.contains("ingestd") {
                continue;
            }
            if ingestd_lines.len() == LOG_TAIL {
                ingestd_lines.pop_front();
            }
            ingestd_lines.push_back(line);
        }

        if ingestd_lines.is_empty() {
            eprintln!("--- ingestd logs: none captured ---");
        } else {
            eprintln!("--- ingestd log tail ---");
            for line in ingestd_lines {
                eprintln!("{line}");
            }
        }
    }
}

impl Drop for PipelineScope<'_> {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.dump_failure_logs();
        }

        // Release permit before cleanup
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

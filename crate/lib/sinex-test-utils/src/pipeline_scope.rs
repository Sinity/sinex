use crate::pipeline::PipelineHarness;
use crate::pipeline_namespace::PipelineNamespace;
use crate::timing_utils::{WaitHelpers, DEFAULT_WAIT_SECS};
use crate::{EventOverrides, TestContext, TestResult, TestSatellitePublisher};
use chrono::{DateTime, Utc};
use sinex_core::{EventId, EventType};
use std::collections::VecDeque;

/// PipelineScope wraps PipelineHarness with automatic DB cleanup and ergonomics.
pub struct PipelineScope<'ctx> {
    ctx: &'ctx TestContext,
    harness: Option<PipelineHarness<'ctx>>,
}

impl<'ctx> PipelineScope<'ctx> {
    /// Create a pipeline scope that enforces shared NATS and resets the DB slot.
    pub async fn new(ctx: &'ctx TestContext) -> TestResult<Self> {
        ctx.ensure_shared_nats()?;
        ctx.reset_database_slot().await?;
        let harness = PipelineHarness::new(ctx).await?;
        Ok(Self {
            ctx,
            harness: Some(harness),
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
    pub fn publisher(&self, source: impl Into<String>) -> TestSatellitePublisher {
        TestSatellitePublisher::with_namespace(
            self.ctx.nats_client(),
            source,
            Some(self.namespace().prefix().to_string()),
        )
    }

    /// Publish a test event through JetStream and wait until ingestd persists it.
    pub async fn publish(
        &self,
        source: &str,
        event_type: &str,
        payload: serde_json::Value,
    ) -> TestResult<EventId> {
        self.harness
            .as_ref()
            .expect("harness not dropped")
            .publish_event(source, event_type, payload)
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
        self.harness
            .as_ref()
            .expect("harness not dropped")
            .publish_event_with_overrides(source, event_type, payload, overrides)
            .await
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

    /// Stop the ingestd instance backing this scope.
    pub async fn shutdown(mut self) -> TestResult<()> {
        if let Some(harness) = self.harness.take() {
            harness.shutdown().await?;
        }
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
    }
}
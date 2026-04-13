//! `PipelineScope` - unified test harness for pipeline integration tests.
//!
//! This module combines the previous `PipelineHarness` functionality directly,
//! providing a single type for pipeline tests.

use crate::EventOverrides;
use crate::sandbox::Sandbox;
use crate::sandbox::coordination::PipelineNamespace;
use crate::sandbox::events::EventPublisher;
use crate::sandbox::nats::{acquire_pipeline_permit, wait_for_event_persisted};
use crate::sandbox::orchestrator::{TestIngestdConfig, start_test_ingestd_with_config};
use crate::sandbox::prelude::{EventId, TestResult};
use crate::sandbox::timing::{DEFAULT_WAIT_SECS, WaitHelpers};
use sinex_db::DbPoolExt;
use sinex_primitives::Timestamp;
use sinex_primitives::events::{Publishable, SourceMaterial};
use sinex_primitives::{EventType, Id};
use std::collections::VecDeque;
use std::time::Instant;
use tokio::runtime::Handle;
use tokio::sync::OwnedSemaphorePermit;
use tracing::{info, warn};

/// `PipelineScope` provides a complete pipeline test harness with ingestd, `JetStream`,
/// and automatic cleanup.
///
/// This is the primary type for tests that need to exercise the full ingestion pipeline.
pub struct PipelineScope<'ctx> {
    ctx: &'ctx Sandbox,
    ingestd: Option<crate::sandbox::orchestrator::TestIngestdHandle>,
    pipeline_permit: Option<OwnedSemaphorePermit>,
    /// Per-test work directory for ingestd. Held here so the TempDir isn't
    /// dropped (and cleaned up) while ingestd is still running.
    _work_dir: tempfile::TempDir,
}

impl<'ctx> PipelineScope<'ctx> {
    /// Create a pipeline scope that enforces shared NATS, resets the DB slot,
    /// and starts ingestd.
    pub async fn new(ctx: &'ctx Sandbox) -> TestResult<Self> {
        ctx.ensure_shared_nats()?;
        ctx.reset_database_slot().await?;

        let nats = ctx.nats_handle()?;
        let namespace = ctx.pipeline_namespace().prefix().to_string();
        let pipeline_permit = Some(acquire_pipeline_permit(&namespace).await?);

        // Create an isolated work directory per test so WAL files from previous
        // tests don't contaminate this run (avoids 10-20s WAL restoration overhead).
        let work_dir = tempfile::tempdir()?;

        let config = TestIngestdConfig {
            nats: nats.connection_config(),
            database_url: ctx.database_url().to_string(),
            work_dir: Some(work_dir.path().to_path_buf()),
            namespace: Some(namespace.clone()),
            // Fast test settings: small batches, short timeouts
            consumer_fetch_max_messages: 32,
            consumer_fetch_timeout_ms: 50, // 50ms: minimize fetch latency in tests
            database_pool_size: 10, // Needs headroom for JetStream consumer + MaterialAssembler + schema reload
        };

        let ingestd = start_test_ingestd_with_config(config, Some(ctx)).await?;

        Ok(Self {
            ctx,
            ingestd: Some(ingestd),
            pipeline_permit,
            _work_dir: work_dir,
        })
    }

    /// Access the underlying Sandbox.
    #[must_use]
    pub fn ctx(&self) -> &Sandbox {
        self.ctx
    }

    /// Access the per-test pipeline namespace.
    #[must_use]
    pub fn namespace(&self) -> &PipelineNamespace {
        self.ctx.pipeline_namespace()
    }

    /// Build a namespaced `JetStream` subject.
    #[must_use]
    pub fn subject(&self, base: &str) -> String {
        self.namespace().subject(base)
    }

    /// Build a namespaced `JetStream` stream name.
    #[must_use]
    pub fn stream(&self, base: &str) -> String {
        self.namespace().stream(base)
    }

    /// Build a namespaced `JetStream` consumer name.
    #[must_use]
    pub fn consumer_name(&self, base: &str) -> String {
        self.namespace().consumer_name(base)
    }

    /// Publish a test event through `JetStream` and wait until ingestd persists it.
    ///
    /// Accepts any type implementing `Publishable`:
    /// - Typed `EventPayload` implementations (recommended)
    /// - `DynamicPayload` for runtime source/type (escape hatch)
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// // Typed payload (recommended)
    /// scope.publish(FileCreatedPayload { path: sp("/test"), ... }).await?;
    ///
    /// // Dynamic payload (escape hatch)
    /// scope.publish(DynamicPayload::new("source", "type", json!({...}))).await?;
    /// ```
    pub async fn publish<P: Publishable>(&self, payload: P) -> TestResult<EventId> {
        self.publish_with_overrides_internal(
            payload.source(),
            payload.event_type(),
            payload.to_json_value()?,
            EventOverrides::default(),
        )
        .await
    }

    /// Publish a test event with overrides (`ts_orig`, id, etc.) and wait until persisted.
    pub async fn publish_with_overrides<P: Publishable>(
        &self,
        payload: P,
        overrides: EventOverrides,
    ) -> TestResult<EventId> {
        self.publish_with_overrides_internal(
            payload.source(),
            payload.event_type(),
            payload.to_json_value()?,
            overrides,
        )
        .await
    }

    /// Prepare an event for NATS publishing without waiting for DB persistence.
    ///
    /// This is the fast path used by batch methods, which can then publish many
    /// prepared envelopes with a single flush.
    async fn prepare_event_for_nats(
        &self,
        source: sinex_primitives::EventSource,
        event_type: sinex_primitives::EventType,
        payload: serde_json::Value,
        overrides: EventOverrides,
    ) -> TestResult<sinex_primitives::events::Event<serde_json::Value>> {
        let timestamp_override = if let Some(ts) = overrides.ts_orig {
            Some(Timestamp::parse_rfc3339(&ts)?)
        } else {
            None
        };

        // Register a source material for FK constraints before publishing.
        let material_id = Id::<SourceMaterial>::new();
        self.ctx
            .ensure_source_material(material_id, Some(source.as_str()))
            .await?;

        // Construct event manually to handle overrides
        Ok(sinex_primitives::events::Event::<serde_json::Value> {
            id: overrides.id.map(sinex_primitives::Id::from_uuid),
            source,
            event_type,
            payload,
            ts_orig: Some(timestamp_override.unwrap_or_else(Timestamp::now)),
            host: crate::sandbox::local_test_host(),
            node_run_id: None,
            payload_schema_id: None,
            provenance: sinex_primitives::events::Provenance::Material {
                id: material_id,
                anchor_byte: 0,
                offset_start: None,
                offset_end: None,
                offset_kind: sinex_primitives::events::OffsetKind::Byte,
            },
            associated_blob_ids: None,
            temporal_policy: None,
            semantics_version: None,
            scope_key: None,
            equivalence_key: None,
            created_by_operation_id: None,
            node_model: None,
        })
    }

    /// Prepare and publish an event to NATS without waiting for DB persistence.
    ///
    /// This is the fast path used by one-off publish helpers. Returns the event ID.
    async fn prepare_and_publish_to_nats(
        &self,
        source: sinex_primitives::EventSource,
        event_type: sinex_primitives::EventType,
        payload: serde_json::Value,
        overrides: EventOverrides,
    ) -> TestResult<EventId> {
        let event = self
            .prepare_event_for_nats(source, event_type, payload, overrides)
            .await?;
        let event_id: uuid::Uuid = self.ctx.publish_prebuilt_event(&event).await?;
        Ok(event_id.into())
    }

    /// Internal implementation for publish with overrides (publishes + waits for persistence).
    async fn publish_with_overrides_internal(
        &self,
        source: sinex_primitives::EventSource,
        event_type: sinex_primitives::EventType,
        payload: serde_json::Value,
        overrides: EventOverrides,
    ) -> TestResult<EventId> {
        let op_start = Instant::now();

        let publish_start = Instant::now();
        let event_id = self
            .prepare_and_publish_to_nats(source.clone(), event_type.clone(), payload, overrides)
            .await?;
        let publish_ms = publish_start.elapsed().as_millis();

        let wait_start = Instant::now();
        wait_for_event_persisted(self.ctx, event_id).await?;
        let wait_ms = wait_start.elapsed().as_millis();
        let total_ms = op_start.elapsed().as_millis();
        let source_str = source.as_str();
        let event_type_str = event_type.as_str();
        info!(
            target: "pipeline_scope",
            source = source_str,
            event_type = event_type_str,
            publish_ms,
            wait_ms,
            total_ms,
            "pipeline publish complete"
        );
        Ok(event_id)
    }

    /// Publish an event with a concrete timestamp and wait until persisted.
    pub async fn publish_with_timestamp<P: Publishable>(
        &self,
        payload: P,
        timestamp: Timestamp,
    ) -> TestResult<EventId> {
        let overrides = EventOverrides {
            ts_orig: Some(timestamp.format_rfc3339()),
            ..Default::default()
        };
        self.publish_with_overrides(payload, overrides).await
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
    ///
    /// Uses fire-and-forget NATS publishes (no per-event DB wait), then a single
    /// batch wait at the end. Much faster than calling `publish()` in a loop.
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
        let event_source = sinex_primitives::EventSource::new(source)?;
        let expected_total = self.ctx.pool.events().count_by_source(&event_source).await? as usize
            + count;
        let mut events = Vec::with_capacity(count);
        for i in 0..count {
            let payload = payload_fn(i);
            let event = self
                .prepare_event_for_nats(
                    event_source.clone(),
                    sinex_primitives::EventType::new(event_type)?,
                    payload,
                    EventOverrides::default(),
                )
                .await?;
            events.push(event);
        }
        let ids = self
            .ctx
            .publish_prebuilt_events(&events)
            .await?
            .into_iter()
            .map(EventId::from)
            .collect();
        self.wait_for_source_events(source, expected_total).await?;
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
        start: Timestamp,
        end: Timestamp,
        payload_fn: F,
    ) -> TestResult<Vec<EventId>>
    where
        F: Fn(usize) -> serde_json::Value,
    {
        if count == 0 {
            return Ok(vec![]);
        }

        let duration = *end - *start;
        let step = if count > 1 {
            duration / (count as i32 - 1)
        } else {
            time::Duration::seconds(0)
        };

        let event_source = sinex_primitives::EventSource::new(source)?;
        let expected_total = self.ctx.pool.events().count_by_source(&event_source).await? as usize
            + count;
        let mut events = Vec::with_capacity(count);
        for i in 0..count {
            let timestamp = Timestamp::new(*start + step * (i as i32));
            let payload = payload_fn(i);
            let overrides = EventOverrides {
                ts_orig: Some(timestamp.format_rfc3339()),
                ..Default::default()
            };
            let event = self
                .prepare_event_for_nats(
                    event_source.clone(),
                    sinex_primitives::EventType::new(event_type)?,
                    payload,
                    overrides,
                )
                .await?;
            events.push(event);
        }
        let ids = self
            .ctx
            .publish_prebuilt_events(&events)
            .await?
            .into_iter()
            .map(EventId::from)
            .collect();
        self.wait_for_source_events(source, expected_total).await?;
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

        match self.ctx.nats_handle() {
            Ok(nats) => match nats.log_tail(LOG_TAIL) {
                Ok(Some(tail)) if !tail.is_empty() => {
                    eprintln!("--- nats log tail ---\n{tail}");
                }
                Ok(_) => {
                    eprintln!("--- nats log tail unavailable ---");
                }
                Err(error) => {
                    eprintln!("--- nats log tail unreadable: {error:#} ---");
                }
            },
            Err(error) => {
                eprintln!("--- nats log tail unavailable: {error:#} ---");
            }
        }

        // Read the file-based ingestd debug log (written by the orchestrator subprocess).
        // The log file is named after the TEST process PID, not the child process PID.
        let debug_log = crate::sandbox::orchestrator::ingestd_debug_log_path_for_test_process();
        match crate::sandbox::orchestrator::read_ingestd_debug_log(&debug_log) {
            Ok(Some(content)) => {
                let lines: Vec<&str> = content.lines().collect();
                let start = lines.len().saturating_sub(LOG_TAIL);
                eprintln!(
                    "--- ingestd log tail ({} lines, showing last {}) ---",
                    lines.len(),
                    lines.len() - start
                );
                for line in &lines[start..] {
                    eprintln!("{line}");
                }
                return;
            }
            Ok(None) => {
                eprintln!("--- ingestd log tail unavailable: debug log empty ---");
            }
            Err(error) => {
                eprintln!("--- ingestd log tail unavailable: {error:#} ---");
            }
        }

        // Fallback: check in-process captured logs
        let logs = self.ctx.captured_logs();
        let mut ingestd_lines: VecDeque<String> = VecDeque::with_capacity(LOG_TAIL);
        for line in logs {
            if line.contains("ingestd") {
                if ingestd_lines.len() == LOG_TAIL {
                    ingestd_lines.pop_front();
                }
                ingestd_lines.push_back(line);
            }
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
        // Always dump diagnostic logs when dropping without explicit shutdown
        // (panicking OR error return from test)
        if self.ingestd.is_some() {
            self.dump_failure_logs();
        }

        // Release permit before cleanup
        self.pipeline_permit.take();

        if let Some(mut ingestd) = self.ingestd.take() {
            if let Ok(handle) = Handle::try_current() {
                handle.spawn(async move {
                    if let Err(error) = ingestd.stop().await {
                        warn!(error = %error, "Failed to stop pipeline ingestd during drop cleanup");
                    }
                });
            } else {
                if let Err(error) = futures::executor::block_on(ingestd.stop()) {
                    warn!(error = %error, "Failed to stop pipeline ingestd during sync drop cleanup");
                }
            }
        }
    }
}

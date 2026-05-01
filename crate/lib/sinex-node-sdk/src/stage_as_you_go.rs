#![doc = include_str!("../docs/stage_as_you_go.md")]
//! Utilities for staging files during processing.

use crate::acquisition_manager::{AcquisitionManager, SourceMaterialHandle};
use crate::ids::deterministic_material_event_id;
use crate::runtime::stream::{EventEmitter, NodeHandles, NodeRuntimeState};
use crate::{NodeResult, SinexError};

use serde_json::{Map as JsonMap, json};
use sinex_primitives::Id;
use sinex_primitives::JsonValue;
use sinex_primitives::Timestamp;
use sinex_primitives::events::Event;
use std::collections::HashMap;
use std::io::ErrorKind;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncReadExt;
use tokio::sync::mpsc;
use tokio::sync::{Mutex, watch};
use tokio::time::sleep;
use tracing::{debug, info, warn};
use uuid::Uuid;

const MAX_SLICE_BYTES: usize = 512 * 1024;
const CONTENT_PREVIEW_BYTES: usize = 500;
const MATERIAL_FINALIZE_REASON: &str = "stage-as-you-go";
const ORPHAN_CLEANUP_REASON: &str = "stage-as-you-go stale cleanup";
const DEFAULT_RECONCILIATION_INTERVAL: Duration = Duration::from_mins(1);
const DEFAULT_STALE_TTL: Duration = Duration::from_mins(5);

/// Stage-as-You-Go context for managing in-flight source materials
#[derive(Clone)]
pub struct StageAsYouGoContext {
    event_emitter: EventEmitter,
    material_registry: Arc<Mutex<HashMap<Uuid, StageMaterialInfo>>>,
    acquisition_manager: Option<Arc<AcquisitionManager>>,
    acquisition_handles: Arc<Mutex<HashMap<Uuid, SourceMaterialHandle>>>,
    cleanup_config: Option<StageCleanupConfig>,
    reconciliation_task: Option<Arc<ReconciliationTask>>,
}

#[derive(Debug, Clone)]
struct StageMaterialInfo {
    metadata: JsonValue,
    started_at: sinex_primitives::temporal::Timestamp,
    last_activity: sinex_primitives::temporal::Timestamp,
}

#[derive(Clone, Copy)]
struct StageCleanupConfig {
    stale_ttl: Duration,
    interval: Duration,
}

impl StageCleanupConfig {
    fn new(stale_ttl: Duration, interval: Duration) -> Self {
        Self {
            stale_ttl,
            interval,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::StageAsYouGoContext;
    use crate::acquisition_manager::{AcquisitionManager, SOURCE_MATERIAL_END_SUBJECT};
    use crate::ids::deterministic_material_event_id;
    use crate::runtime::stream::EventEmitter;
    use sinex_primitives::environment::environment;
    use sinex_primitives::{DynamicPayload, Id, events::Provenance};
    use std::sync::Arc;
    use tokio::sync::mpsc;
    use tokio::sync::watch;
    use tokio::time::{Duration, timeout};
    use tokio_stream::StreamExt;
    use uuid::Uuid;
    use xtask::sandbox::sinex_test;

    #[sinex_test]
    async fn emit_event_assigns_id_and_anchor() -> TestResult<()> {
        let (tx, mut rx) = mpsc::channel(1);
        let emitter = EventEmitter::new(tx, false);
        let context = StageAsYouGoContext::from_optional_emitter(emitter);

        let event = DynamicPayload::new(
            "stage.test",
            "line.captured",
            serde_json::json!({"line": "hello"}),
        )
        .from_parents([Id::from_uuid(Uuid::now_v7())])?
        .at_time(
            sinex_primitives::Timestamp::from_unix_timestamp_millis(1_710_000_000_123)
                .ok_or_else(|| color_eyre::eyre::eyre!("test timestamp should be valid"))?,
        )
        .build()
        .expect("infallible: test provenance set");
        let material_id = Uuid::now_v7();
        let emitted_id = context
            .emit_event_with_provenance(event, material_id, Some(12), Some(34))
            .await?;

        let emitted = timeout(Duration::from_secs(1), rx.recv())
            .await?
            .ok_or_else(|| color_eyre::eyre::eyre!("event channel closed"))?;

        let stored_id = emitted
            .id
            .ok_or_else(|| color_eyre::eyre::eyre!("event ID should be assigned"))?;
        assert_eq!(*stored_id.as_uuid(), emitted_id);
        assert_eq!(
            *stored_id.as_uuid(),
            deterministic_material_event_id(
                "stage.test",
                "line.captured",
                material_id,
                12,
                Some(12),
                Some(34),
                emitted
                    .ts_orig
                    .ok_or_else(|| color_eyre::eyre::eyre!("event timestamp should be assigned"))?
            )
        );

        match emitted.provenance() {
            Provenance::Material { anchor_byte, .. } => {
                assert_eq!(*anchor_byte, 12);
            }
            other => {
                return Err(color_eyre::eyre::eyre!(
                    "unexpected provenance variant: {:?}",
                    other
                ));
            }
        }

        Ok(())
    }

    #[sinex_test]
    async fn reconciliation_config_is_retained_without_manager() -> TestResult<()> {
        let (tx, _rx) = mpsc::channel(1);
        let emitter = EventEmitter::new(tx, false);
        let context = StageAsYouGoContext::from_optional_emitter(emitter)
            .with_reconciliation(Duration::from_secs(5), Duration::from_secs(1));

        assert!(context.cleanup_config.is_some());
        assert!(context.reconciliation_task.is_none());
        Ok(())
    }

    #[sinex_test]
    async fn signal_reconciliation_shutdown_reports_dropped_receiver() -> TestResult<()> {
        let (tx, rx) = watch::channel(false);
        drop(rx);

        assert!(!super::signal_reconciliation_shutdown(&tx));
        Ok(())
    }

    #[sinex_test]
    async fn signal_reconciliation_shutdown_delivers_to_receiver() -> TestResult<()> {
        let (tx, mut rx) = watch::channel(false);

        assert!(super::signal_reconciliation_shutdown(&tx));
        rx.changed().await?;
        assert!(*rx.borrow());
        Ok(())
    }

    #[sinex_test]
    async fn finalize_source_material_resumes_from_already_staged_bytes(
        ctx: TestContext,
    ) -> TestResult<()> {
        let ctx = ctx.with_nats().shared().await?;
        let work_dir = tempfile::tempdir()?;
        let acquisition = Arc::new(
            AcquisitionManager::with_defaults(ctx.nats_client(), "stage-retry-test")
                .with_work_dir(work_dir.path()),
        );
        let (tx, _rx) = mpsc::channel(1);
        let context = StageAsYouGoContext::from_sender(acquisition.clone(), tx, false);
        let material_id = context
            .register_in_flight("log_file", Some("test://resume"), serde_json::json!({}))
            .await?;
        let end_subject =
            environment().nats_subject_with_namespace(None, SOURCE_MATERIAL_END_SUBJECT);
        let mut end_sub = ctx.nats_client().subscribe(end_subject).await?;

        let mut handle = context
            .acquisition_handles
            .lock()
            .await
            .remove(&material_id)
            .expect("registered material should have an acquisition handle");
        acquisition.append_slice(&mut handle, b"abc").await?;
        context
            .acquisition_handles
            .lock()
            .await
            .insert(material_id, handle);

        context
            .finalize_source_material(material_id, b"abcdef", Some("text/plain"), Some("utf-8"))
            .await?;

        let end = timeout(Duration::from_secs(1), end_sub.next())
            .await?
            .ok_or_else(|| color_eyre::eyre::eyre!("missing material end message"))?;
        let payload: serde_json::Value = serde_json::from_slice(&end.payload)?;
        if payload["material_id"] != material_id.to_string() {
            let end = timeout(Duration::from_secs(1), end_sub.next())
                .await?
                .ok_or_else(|| color_eyre::eyre::eyre!("missing material end message"))?;
            let payload: serde_json::Value = serde_json::from_slice(&end.payload)?;
            assert_eq!(payload["material_id"], material_id.to_string());
            assert_eq!(payload["total_size_bytes"], 6);
            assert_eq!(payload["total_slices"], 2);
            return Ok(());
        }

        assert_eq!(payload["total_size_bytes"], 6);
        assert_eq!(payload["total_slices"], 2);
        Ok(())
    }
}

struct ReconciliationTask {
    shutdown: watch::Sender<bool>,
}

impl ReconciliationTask {
    fn new(shutdown: watch::Sender<bool>) -> Self {
        Self { shutdown }
    }
}

fn signal_reconciliation_shutdown(shutdown: &watch::Sender<bool>) -> bool {
    if shutdown.send(true).is_err() {
        warn!("Stage-as-You-Go reconciliation shutdown receiver dropped before cleanup");
        return false;
    }
    true
}

impl Drop for ReconciliationTask {
    fn drop(&mut self) {
        // Signal graceful shutdown — the task checks this via select! and exits
        // on its next loop iteration. We intentionally do NOT abort() here because
        // that would interrupt in-flight reconciliation (database operations).
        signal_reconciliation_shutdown(&self.shutdown);
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct StageReconciliationSummary {
    pub cancelled: usize,
    pub skipped: usize,
    pub errors: usize,
}

impl StageAsYouGoContext {
    /// Create a Stage-as-You-Go context from node runtime handles
    #[must_use]
    pub fn from_runtime(runtime: &NodeRuntimeState) -> Self {
        Self::from_optional_emitter(runtime.event_emitter().clone())
    }

    /// Attach an acquisition manager so Stage-as-You-Go can publish materials via `JetStream`.
    #[must_use]
    pub fn with_acquisition_manager(mut self, acquisition: Arc<AcquisitionManager>) -> Self {
        self.acquisition_manager = Some(acquisition.clone());
        if self.reconciliation_task.is_none()
            && let Some(config) = self.cleanup_config
        {
            self.start_reconciliation_task(acquisition, config);
        }
        self
    }

    /// Create a Stage-as-You-Go context directly from node handles
    #[must_use]
    pub fn from_handles(handles: &NodeHandles) -> Self {
        Self::from_optional_emitter(handles.emitter().clone())
    }

    /// Convenience helper to build a context from a sender channel (tests/tooling)
    #[must_use]
    pub fn from_sender(
        acquisition: Arc<AcquisitionManager>,
        event_sender: mpsc::Sender<Event<JsonValue>>,
        dry_run: bool,
    ) -> Self {
        Self::from_optional_emitter(EventEmitter::new(event_sender, dry_run))
            .with_acquisition_manager(acquisition)
    }

    /// Enable automatic reconciliation using default thresholds.
    #[must_use]
    pub fn with_default_reconciliation(self) -> Self {
        self.with_reconciliation(DEFAULT_STALE_TTL, DEFAULT_RECONCILIATION_INTERVAL)
    }

    /// Enable automatic reconciliation of stale materials.
    pub fn with_reconciliation(mut self, stale_ttl: Duration, interval: Duration) -> Self {
        let config = StageCleanupConfig::new(stale_ttl, interval);
        self.cleanup_config = Some(config);

        let Some(manager) = self.acquisition_manager.clone() else {
            warn!("Stage-as-You-Go reconciliation skipped: acquisition manager missing");
            return self;
        };

        self.start_reconciliation_task(manager, config);
        self
    }

    /// Reconcile any stale materials using the configured TTL or the default.
    pub async fn reconcile_inflight(&self) -> NodeResult<StageReconciliationSummary> {
        let ttl = self
            .cleanup_config
            .map_or(DEFAULT_STALE_TTL, |cfg| cfg.stale_ttl);
        self.reconcile_inflight_older_than(ttl).await
    }

    /// Reconcile materials older than the provided TTL.
    pub async fn reconcile_inflight_older_than(
        &self,
        stale_ttl: Duration,
    ) -> NodeResult<StageReconciliationSummary> {
        let manager = self.acquisition_manager.as_ref().ok_or_else(|| {
            SinexError::processing(
                "Stage-as-You-Go context requires an acquisition manager".to_string(),
            )
        })?;
        reconcile_shared(
            &self.material_registry,
            &self.acquisition_handles,
            manager,
            stale_ttl,
        )
        .await
    }

    fn from_optional_emitter(event_emitter: EventEmitter) -> Self {
        Self {
            event_emitter,
            material_registry: Arc::new(Mutex::new(HashMap::new())),
            acquisition_manager: None,
            acquisition_handles: Arc::new(Mutex::new(HashMap::new())),
            cleanup_config: None,
            reconciliation_task: None,
        }
    }

    fn start_reconciliation_task(
        &mut self,
        manager: Arc<AcquisitionManager>,
        config: StageCleanupConfig,
    ) {
        let registry = Arc::clone(&self.material_registry);
        let handles = Arc::clone(&self.acquisition_handles);
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    shutdown_result = shutdown_rx.changed() => {
                        if shutdown_result.is_err() {
                            warn!("Stage-as-You-Go reconciliation shutdown channel dropped before explicit shutdown");
                        }
                        if shutdown_result.is_err() || *shutdown_rx.borrow() {
                            break;
                        }
                    }
                    () = sleep(config.interval) => {
                        if let Err(err) = reconcile_shared(
                            &registry,
                            &handles,
                            &manager,
                            config.stale_ttl
                        ).await {
                            warn!(error = %err, "Stage-as-You-Go reconciliation loop failed");
                        }
                    }
                }
            }
        });

        self.reconciliation_task = Some(Arc::new(ReconciliationTask::new(shutdown_tx)));
    }

    /// Register in-flight source material and get its ID immediately
    ///
    /// This is the first step of Stage-as-You-Go: register the source material
    /// with minimal metadata to get an ID that can be attached to events.
    pub async fn register_in_flight(
        &self,
        material_type: &str,
        source_uri: Option<&str>,
        initial_metadata: serde_json::Value,
    ) -> NodeResult<Uuid> {
        let metadata = Self::prepare_initial_metadata(material_type, source_uri, initial_metadata);
        let manager = self
            .acquisition_manager
            .as_ref()
            .ok_or_else(|| {
                SinexError::processing(
                    "Stage-as-You-Go context requires an acquisition manager".to_string(),
                )
            })?
            .clone();

        let identifier = source_uri.unwrap_or(material_type);
        let handle = manager
            .build_material(identifier)
            .with_metadata(metadata.clone())
            .begin()
            .await
            .map_err(|e| SinexError::processing(format!("Failed to begin material: {e}")))?;
        let material_id = handle.material_id;
        let started_at = handle.started_at();
        self.acquisition_handles
            .lock()
            .await
            .insert(material_id, handle);

        info!(
            source_material_id = %material_id,
            material_type = material_type,
            "Opened in-flight source material handle"
        );

        let info = StageMaterialInfo {
            metadata,
            started_at,
            last_activity: sinex_primitives::temporal::Timestamp::now(),
        };

        self.material_registry
            .lock()
            .await
            .insert(material_id, info);

        Ok(material_id)
    }

    /// Get the `started_at` timestamp for an in-flight material.
    ///
    /// This is the wall-clock time recorded when the material capture began.
    /// `register_in_flight()` now publishes BEGIN before it returns, so ingestd
    /// can durably persist the same value as the `staged_at` ledger entry. Use
    /// it as the fallback `ts_orig` for events that lack an intrinsic timestamp
    /// — it is reproducible on replay because it traces to that persisted row.
    pub async fn material_started_at(
        &self,
        material_id: Uuid,
    ) -> Option<sinex_primitives::temporal::Timestamp> {
        if let Some(started_at) = self
            .acquisition_handles
            .lock()
            .await
            .get(&material_id)
            .map(super::acquisition_manager::SourceMaterialHandle::started_at)
        {
            return Some(started_at);
        }

        self.material_registry
            .lock()
            .await
            .get(&material_id)
            .map(|info| info.started_at)
    }

    /// Create and send an event with attached source material reference
    ///
    /// This is the core of Stage-as-You-Go: events are created with immediate
    /// provenance tracking via the `source_material_id` field.
    pub async fn emit_event_with_provenance(
        &self,
        mut event: Event<JsonValue>,
        source_material_id: Uuid,
        offset_start: Option<i64>,
        offset_end: Option<i64>,
    ) -> NodeResult<Uuid> {
        let anchor_byte = offset_start.or(offset_end).unwrap_or(0);
        let ts_orig = if let Some(timestamp) = event.ts_orig {
            timestamp
        } else {
            let timestamp = self
                .material_started_at(source_material_id)
                .await
                .unwrap_or_else(Timestamp::now);
            event.ts_orig = Some(timestamp);
            timestamp
        };

        if event.id.is_none() {
            event.id = Some(Id::from_uuid(deterministic_material_event_id(
                event.source.as_str(),
                event.event_type.as_str(),
                source_material_id,
                anchor_byte,
                offset_start,
                offset_end,
                ts_orig,
            )));
        }

        // Attach source material provenance to the event via the documented
        // constructor; struct-literal construction would bypass the builder
        // typestate guarantees (see issue #559 / #559-tracked ast-grep work).
        event.provenance = sinex_primitives::events::builder::Provenance::from_material(
            source_material_id,
            anchor_byte,
            offset_start,
            offset_end,
        );

        // Add source material reference to payload metadata if not already present
        if let Some(obj) = event.payload.as_object_mut() {
            obj.insert(
                "_source_material_id".to_string(),
                serde_json::json!(source_material_id.to_string()),
            );
        }

        // Send event via event channel
        let event_id: Uuid = *event
            .id
            .as_ref()
            .ok_or_else(|| SinexError::processing("Event must have an ID".to_string()))?
            .as_uuid();

        self.event_emitter.emit(event).await?;

        debug!(
            event_id = %event_id,
            source_material_id = %source_material_id,
            "Emitted event with source material provenance"
        );

        {
            let mut registry = self.material_registry.lock().await;
            if let Some(health) = registry.get_mut(&source_material_id) {
                health.last_activity = sinex_primitives::temporal::Timestamp::now();
            }
        }

        Ok(event_id)
    }

    /// Finalize in-flight source material with actual content details
    ///
    /// This is the final step of Stage-as-You-Go: once the content is fully
    /// processed, update the source material record with complete information.
    pub async fn finalize_source_material(
        &self,
        id: Uuid,
        content: &[u8],
        mime_type: Option<&str>,
        encoding: Option<&str>,
    ) -> NodeResult<()> {
        // Checksum is now computed when creating the blob

        let content_preview = if mime_type.is_some_and(|m| m.starts_with("text/")) {
            Some(String::from_utf8_lossy(&content[..content.len().min(500)]).to_string())
        } else {
            None
        };

        // Get metadata without removing — defer removal until after successful finalization
        // so reconciliation can still find/retry on failure
        let material_info = {
            let registry = self.material_registry.lock().await;
            registry.get(&id).cloned()
        };

        let manager = self
            .acquisition_manager
            .as_ref()
            .ok_or_else(|| {
                SinexError::processing(
                    "Stage-as-You-Go context requires an acquisition manager".to_string(),
                )
            })?
            .clone();

        let mut handle = self
            .acquisition_handles
            .lock()
            .await
            .remove(&id)
            .ok_or_else(|| {
                SinexError::processing(format!("Missing acquisition handle for material {id}"))
            })?;

        let finalize_result = self
            .finalize_via_acquisition(
                manager,
                &mut handle,
                material_info.as_ref(),
                content,
                mime_type,
                encoding,
                content_preview.clone(),
            )
            .await;

        if let Err(error) = finalize_result {
            self.acquisition_handles.lock().await.insert(id, handle);
            return Err(error);
        }

        // Remove from registry only after successful finalization
        {
            let mut registry = self.material_registry.lock().await;
            registry.remove(&id);
        }

        info!(
            material_id = %id,
            bytes = content.len(),
            "Finalized source material via JetStream"
        );

        Ok(())
    }

    /// Finalize in-flight source material with a streaming payload.
    pub async fn finalize_source_material_stream<R>(
        &self,
        id: Uuid,
        reader: &mut R,
        mime_type: Option<&str>,
        encoding: Option<&str>,
    ) -> NodeResult<()>
    where
        R: tokio::io::AsyncRead + Unpin,
    {
        let is_text = mime_type.is_some_and(|m| m.starts_with("text/"));
        let mut preview_bytes: Vec<u8> = Vec::new();

        let material_info = {
            let registry = self.material_registry.lock().await;
            registry.get(&id).cloned()
        };

        let manager = self
            .acquisition_manager
            .as_ref()
            .ok_or_else(|| {
                SinexError::processing(
                    "Stage-as-You-Go context requires an acquisition manager".to_string(),
                )
            })?
            .clone();

        let mut handle = self
            .acquisition_handles
            .lock()
            .await
            .remove(&id)
            .ok_or_else(|| {
                SinexError::processing(format!("Missing acquisition handle for material {id}"))
            })?;
        let resume_offset = resumed_prefix_len(&handle, usize::MAX)?;
        if resume_offset > 0 {
            skip_stream_prefix(reader, resume_offset, is_text.then_some(&mut preview_bytes))
                .await?;
        }
        let mut total_bytes = resume_offset as i64;

        let finalize_result = async {
            let mut buffer = vec![0u8; MAX_SLICE_BYTES];
            loop {
                let read = reader
                    .read(&mut buffer)
                    .await
                    .map_err(|e| SinexError::processing(e.to_string()))?;
                if read == 0 {
                    break;
                }

                if is_text && preview_bytes.len() < CONTENT_PREVIEW_BYTES {
                    let take_len = (CONTENT_PREVIEW_BYTES - preview_bytes.len()).min(read);
                    preview_bytes.extend_from_slice(&buffer[..take_len]);
                }

                manager
                    .append_slice(&mut handle, &buffer[..read])
                    .await
                    .map_err(|e| SinexError::processing(format!("Failed to append slice: {e}")))?;
                total_bytes += read as i64;
            }

            let content_preview = if is_text && !preview_bytes.is_empty() {
                Some(String::from_utf8_lossy(&preview_bytes).to_string())
            } else {
                None
            };
            let metadata = Self::build_finalize_metadata(
                material_info.as_ref(),
                total_bytes,
                content_preview,
                encoding,
            );

            manager
                .finalize_with_metadata(&mut handle, MATERIAL_FINALIZE_REASON, metadata)
                .await
                .map_err(|e| SinexError::processing(format!("Failed to finalize material: {e}")))
        }
        .await;

        if let Err(error) = finalize_result {
            self.acquisition_handles.lock().await.insert(id, handle);
            return Err(error);
        }

        {
            let mut registry = self.material_registry.lock().await;
            registry.remove(&id);
        }

        info!(
            material_id = %id,
            bytes = total_bytes,
            "Finalized source material via JetStream (streaming)"
        );

        Ok(())
    }

    async fn finalize_via_acquisition(
        &self,
        manager: Arc<AcquisitionManager>,
        handle: &mut SourceMaterialHandle,
        info: Option<&StageMaterialInfo>,
        content: &[u8],
        _mime_type: Option<&str>,
        encoding: Option<&str>,
        content_preview: Option<String>,
    ) -> NodeResult<()> {
        let resume_offset = resumed_prefix_len(handle, content.len())?;
        for chunk in content[resume_offset..].chunks(MAX_SLICE_BYTES) {
            manager
                .append_slice(handle, chunk)
                .await
                .map_err(|e| SinexError::processing(format!("Failed to append slice: {e}")))?;
        }

        let metadata =
            Self::build_finalize_metadata(info, content.len() as i64, content_preview, encoding);

        manager
            .finalize_with_metadata(handle, MATERIAL_FINALIZE_REASON, metadata)
            .await
            .map_err(|e| SinexError::processing(format!("Failed to finalize material: {e}")))
    }
}

fn resumed_prefix_len(handle: &SourceMaterialHandle, content_len: usize) -> NodeResult<usize> {
    let resume_offset = usize::try_from(handle.bytes_written()).map_err(|error| {
        SinexError::processing("staged material progress exceeds addressable memory size")
            .with_context("bytes_written", handle.bytes_written().to_string())
            .with_std_error(&error)
    })?;
    if content_len != usize::MAX && resume_offset > content_len {
        return Err(SinexError::processing(
            "staged material progress exceeds supplied content length",
        )
        .with_context("bytes_written", resume_offset.to_string())
        .with_context("content_len", content_len.to_string()));
    }
    Ok(resume_offset)
}

async fn skip_stream_prefix<R>(
    reader: &mut R,
    bytes_to_skip: usize,
    mut preview: Option<&mut Vec<u8>>,
) -> NodeResult<()>
where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut remaining = bytes_to_skip;
    let mut discard = vec![0u8; MAX_SLICE_BYTES];
    while remaining > 0 {
        let window_len = remaining.min(discard.len());
        let read = reader
            .read(&mut discard[..window_len])
            .await
            .map_err(|e| SinexError::processing(e.to_string()))?;
        if read == 0 {
            return Err(SinexError::processing(
                "reader ended before previously staged bytes were replayed",
            )
            .with_context("bytes_to_skip", bytes_to_skip.to_string())
            .with_context("bytes_consumed", (bytes_to_skip - remaining).to_string())
            .with_context("kind", ErrorKind::UnexpectedEof.to_string()));
        }

        if let Some(preview_bytes) = preview.as_deref_mut()
            && preview_bytes.len() < CONTENT_PREVIEW_BYTES
        {
            let take_len = (CONTENT_PREVIEW_BYTES - preview_bytes.len()).min(read);
            preview_bytes.extend_from_slice(&discard[..take_len]);
        }

        remaining -= read;
    }
    Ok(())
}

async fn reconcile_shared(
    registry: &Arc<Mutex<HashMap<Uuid, StageMaterialInfo>>>,
    handles: &Arc<Mutex<HashMap<Uuid, SourceMaterialHandle>>>,
    manager: &Arc<AcquisitionManager>,
    stale_ttl: Duration,
) -> NodeResult<StageReconciliationSummary> {
    let ttl = time::Duration::try_from(stale_ttl).unwrap_or(time::Duration::MAX);
    let now = sinex_primitives::temporal::Timestamp::now();
    let stale_ids = {
        let registry_guard = registry.lock().await;
        registry_guard
            .iter()
            .filter_map(|(id, info)| {
                // Explicitly use nanosecond precision to avoid Duration type ambiguity
                let diff_nanos =
                    now.unix_timestamp_nanos() - info.last_activity.unix_timestamp_nanos();
                let diff = sinex_primitives::temporal::Duration::nanoseconds(diff_nanos as i64);
                if diff >= ttl { Some(*id) } else { None }
            })
            .collect::<Vec<_>>()
    };

    let mut summary = StageReconciliationSummary::default();

    for material_id in stale_ids {
        let info = {
            let mut registry_guard = registry.lock().await;
            registry_guard.remove(&material_id)
        };
        let handle = {
            let mut handles_guard = handles.lock().await;
            handles_guard.remove(&material_id)
        };

        if let Some(mut handle) = handle {
            match manager.cancel(&mut handle, ORPHAN_CLEANUP_REASON).await {
                Ok(()) => {
                    summary.cancelled += 1;
                    info!(%material_id, "Cancelled stale Stage-as-You-Go material");
                }
                Err(err) => {
                    if let Some(info) = info {
                        registry.lock().await.insert(material_id, info);
                    }
                    handles.lock().await.insert(material_id, handle);
                    summary.errors += 1;
                    warn!(
                        error = %err,
                        %material_id,
                        "Failed to cancel stale Stage-as-You-Go material; preserving state for retry"
                    );
                }
            }
        } else {
            if let Some(info) = info {
                registry.lock().await.insert(material_id, info);
            }
            summary.skipped += 1;
            warn!(
                %material_id,
                "Stale Stage-as-You-Go material had no acquisition handle; preserving registry state"
            );
        }
    }

    Ok(summary)
}

impl StageAsYouGoContext {
    #[allow(clippy::expect_used)] // Post-normalization: value guaranteed to be Object
    fn build_finalize_metadata(
        info: Option<&StageMaterialInfo>,
        total_bytes: i64,
        content_preview: Option<String>,
        encoding: Option<&str>,
    ) -> JsonValue {
        let mut base = info.map_or_else(|| json!({}), |i| i.metadata.clone());
        if !base.is_object() {
            base = json!({});
        }
        {
            let map = base.as_object_mut().expect("metadata normalized to object");
            map.insert("total_bytes".to_string(), JsonValue::from(total_bytes));
            if let Some(preview) = content_preview {
                map.insert("content_preview".to_string(), JsonValue::String(preview));
            }
            if let Some(enc) = encoding {
                map.insert("encoding".to_string(), JsonValue::String(enc.to_string()));
            }
        }
        base
    }
}

fn normalize_metadata(value: JsonValue) -> JsonValue {
    match value {
        JsonValue::Object(_) => value,
        JsonValue::Null => json!({}),
        other => {
            let mut map = JsonMap::new();
            map.insert("value".to_string(), other);
            JsonValue::Object(map)
        }
    }
}

impl StageAsYouGoContext {
    #[allow(clippy::expect_used)] // Post-normalization: value guaranteed to be Object
    fn prepare_initial_metadata(
        material_type: &str,
        source_uri: Option<&str>,
        metadata: JsonValue,
    ) -> JsonValue {
        let mut normalized = normalize_metadata(metadata);
        let map = normalized
            .as_object_mut()
            .expect("metadata normalized to object");
        map.entry("material_type".to_string())
            .or_insert_with(|| JsonValue::String(material_type.to_string()));
        if let Some(uri) = source_uri {
            map.entry("source_uri".to_string())
                .or_insert_with(|| JsonValue::String(uri.to_string()));
        }
        normalized
    }
}

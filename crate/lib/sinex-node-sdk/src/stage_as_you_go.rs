#![doc = include_str!("../docs/stage_as_you_go.md")]
//! Utilities for staging files during processing.

use crate::acquisition_manager::{AcquisitionManager, SourceMaterialHandle};
use crate::stream_processor::{EventEmitter, NodeHandles, NodeRuntimeState};
use crate::{NodeResult, SinexError};

use serde_json::{json, Map as JsonMap};
use sinex_primitives::events::{payloads::LogLinePayload, Event};
use sinex_primitives::Id;
use sinex_primitives::JsonValue;
use sinex_primitives::Ulid;
use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;
use tokio::io::AsyncReadExt;
use tokio::sync::mpsc;
use tokio::sync::{watch, Mutex};
use tokio::time::sleep;
use tracing::{debug, info, warn};

const MAX_SLICE_BYTES: usize = 512 * 1024;
const CONTENT_PREVIEW_BYTES: usize = 500;
const MATERIAL_FINALIZE_REASON: &str = "stage-as-you-go";
const ORPHAN_CLEANUP_REASON: &str = "stage-as-you-go stale cleanup";
const DEFAULT_RECONCILIATION_INTERVAL: Duration = Duration::from_secs(60);
const DEFAULT_STALE_TTL: Duration = Duration::from_secs(5 * 60);

/// Stage-as-You-Go context for managing in-flight source materials
#[derive(Clone)]
pub struct StageAsYouGoContext {
    event_emitter: EventEmitter,
    material_registry: Arc<Mutex<HashMap<Ulid, StageMaterialInfo>>>,
    acquisition_manager: Option<Arc<AcquisitionManager>>,
    acquisition_handles: Arc<Mutex<HashMap<Ulid, SourceMaterialHandle>>>,
    cleanup_config: Option<StageCleanupConfig>,
    reconciliation_task: Option<Arc<ReconciliationTask>>,
}

#[derive(Debug, Clone)]
struct StageMaterialInfo {
    metadata: JsonValue,
    _registered_at: sinex_primitives::temporal::OffsetDateTime,
    last_activity: sinex_primitives::temporal::OffsetDateTime,
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
    use crate::stream_processor::EventEmitter;
    use sinex_primitives::{events::Provenance, DynamicPayload, Id};
    use sinex_schema::ulid::Ulid;
    use tokio::sync::mpsc;
    use tokio::time::{timeout, Duration};
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
        .with_provenance(Provenance::from_synthesis_safe(
            Id::from_ulid(Ulid::new()),
            Vec::new(),
        ))
        .build()
        .expect("infallible: test provenance set");
        let material_id = Ulid::new();
        let emitted_id = context
            .emit_event_with_provenance(event, material_id, Some(12), Some(34))
            .await?;

        let emitted = timeout(Duration::from_secs(1), rx.recv())
            .await?
            .ok_or_else(|| color_eyre::eyre::eyre!("event channel closed"))?;

        let stored_id = emitted.id.expect("event ID should be assigned");
        assert_eq!(*stored_id.as_ulid(), emitted_id);

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
    async fn test_register_in_flight_uses_builder() -> TestResult<()> {
        // This test simulates the builder usage pattern via register_in_flight.
        // Since we can't easily mock AcquisitionManager purely without NATS in unit tests,
        // we mainly check that the method signature and types align and compiling works.
        // Real logic integration is covered by the demo and integration tests.

        // However, we can verify that optional metadata is handled correctly
        // by the internal builder logic (which we know we used).

        let initial = serde_json::json!({"foo": "bar"});
        let normalized =
            StageAsYouGoContext::prepare_initial_metadata("test_type", Some("test_uri"), initial);

        // Check builder logic that merges fields
        let obj = normalized.as_object().unwrap();
        assert_eq!(
            obj.get("material_type").unwrap().as_str().unwrap(),
            "test_type"
        );
        assert_eq!(obj.get("source_uri").unwrap().as_str().unwrap(), "test_uri");
        assert_eq!(obj.get("foo").unwrap().as_str().unwrap(), "bar");

        Ok(())
    }
}

struct ReconciliationTask {
    shutdown: watch::Sender<bool>,
    join_handle: StdMutex<Option<tokio::task::JoinHandle<()>>>,
}

impl ReconciliationTask {
    fn new(shutdown: watch::Sender<bool>, handle: tokio::task::JoinHandle<()>) -> Self {
        Self {
            shutdown,
            join_handle: StdMutex::new(Some(handle)),
        }
    }
}

impl Drop for ReconciliationTask {
    fn drop(&mut self) {
        let _ = self.shutdown.send(true);
        if let Some(handle) = self
            .join_handle
            .lock()
            .ok()
            .and_then(|mut guard| guard.take())
        {
            handle.abort();
        }
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct StageReconciliationSummary {
    pub cancelled: usize,
    pub skipped: usize,
    pub errors: usize,
}

impl StageAsYouGoContext {
    /// Create a Stage-as-You-Go context from processor runtime handles
    pub fn from_runtime(runtime: &NodeRuntimeState) -> Self {
        Self::from_optional_emitter(runtime.event_emitter().clone())
    }

    /// Attach an acquisition manager so Stage-as-You-Go can publish materials via JetStream.
    pub fn with_acquisition_manager(mut self, acquisition: Arc<AcquisitionManager>) -> Self {
        self.acquisition_manager = Some(acquisition.clone());
        if self.reconciliation_task.is_none() {
            if let Some(config) = self.cleanup_config {
                self.start_reconciliation_task(acquisition, config);
            }
        }
        self
    }

    /// Create a Stage-as-You-Go context directly from processor handles
    pub fn from_handles(handles: &NodeHandles) -> Self {
        Self::from_optional_emitter(handles.emitter().clone())
    }

    /// Convenience helper to build a context from a sender channel (tests/tooling)
    pub fn from_sender(
        acquisition: Arc<AcquisitionManager>,
        event_sender: mpsc::Sender<Event<JsonValue>>,
        dry_run: bool,
    ) -> Self {
        Self::from_optional_emitter(EventEmitter::new(event_sender, dry_run))
            .with_acquisition_manager(acquisition)
    }

    /// Enable automatic reconciliation using default thresholds.
    pub fn with_default_reconciliation(self) -> Self {
        self.with_reconciliation(DEFAULT_STALE_TTL, DEFAULT_RECONCILIATION_INTERVAL)
    }

    /// Enable automatic reconciliation of stale materials.
    pub fn with_reconciliation(mut self, stale_ttl: Duration, interval: Duration) -> Self {
        let config = StageCleanupConfig::new(stale_ttl, interval);
        self.cleanup_config = Some(config);

        let manager = match self.acquisition_manager.clone() {
            Some(manager) => manager,
            None => {
                warn!("Stage-as-You-Go reconciliation skipped: acquisition manager missing");
                return self;
            }
        };

        self.start_reconciliation_task(manager, config);
        self
    }

    /// Reconcile any stale materials using the configured TTL or the default.
    pub async fn reconcile_inflight(&self) -> NodeResult<StageReconciliationSummary> {
        let ttl = self
            .cleanup_config
            .map(|cfg| cfg.stale_ttl)
            .unwrap_or(DEFAULT_STALE_TTL);
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

        let task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = shutdown_rx.changed() => {
                        if *shutdown_rx.borrow() {
                            break;
                        }
                    }
                    _ = sleep(config.interval) => {
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

        self.reconciliation_task = Some(Arc::new(ReconciliationTask::new(shutdown_tx, task)));
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
    ) -> NodeResult<Ulid> {
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
            .map_err(|e| SinexError::processing(format!("Failed to begin material: {}", e)))?;
        let material_id = handle.material_id;
        self.acquisition_handles
            .lock()
            .await
            .insert(material_id, handle);

        info!(
            blob_id = %material_id,
            material_type = material_type,
            "Registered in-flight source material via JetStream"
        );

        let now = sinex_primitives::temporal::OffsetDateTime::now_utc();
        let info = StageMaterialInfo {
            metadata,
            _registered_at: now,
            last_activity: now,
        };

        self.material_registry
            .lock()
            .await
            .insert(material_id, info);

        Ok(material_id)
    }

    /// Create and send an event with attached source material reference
    ///
    /// This is the core of Stage-as-You-Go: events are created with immediate
    /// provenance tracking via the source_material_id field.
    pub async fn emit_event_with_provenance(
        &self,
        mut event: Event<JsonValue>,
        source_material_id: Ulid,
        offset_start: Option<i64>,
        offset_end: Option<i64>,
    ) -> NodeResult<Ulid> {
        if event.id.is_none() {
            event.id = Some(Id::from_ulid(Ulid::new()));
        }

        // Attach source material provenance to the event
        let anchor_byte = offset_start.or(offset_end).unwrap_or(0);
        event.provenance = sinex_primitives::events::builder::Provenance::Material {
            id: source_material_id.into(),
            anchor_byte,
            offset_start,
            offset_end,
            offset_kind: sinex_primitives::events::builder::OffsetKind::default(),
        };

        // Add source material reference to payload metadata if not already present
        if let Some(obj) = event.payload.as_object_mut() {
            obj.insert(
                "_source_material_id".to_string(),
                serde_json::json!(source_material_id.to_string()),
            );
        }

        // Send event via event channel
        let event_id: Ulid = *event
            .id
            .as_ref()
            .ok_or_else(|| SinexError::processing("Event must have an ID".to_string()))?
            .as_ulid();

        self.event_emitter.emit(event).await?;

        debug!(
            event_id = %event_id,
            source_material_id = %source_material_id,
            "Emitted event with source material provenance"
        );

        {
            let mut registry = self.material_registry.lock().await;
            if let Some(health) = registry.get_mut(&source_material_id) {
                health.last_activity = sinex_primitives::temporal::OffsetDateTime::now_utc();
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
        id: Ulid,
        content: &[u8],
        mime_type: Option<&str>,
        encoding: Option<&str>,
    ) -> NodeResult<()> {
        // Checksum is now computed when creating the blob

        let content_preview = if mime_type.map(|m| m.starts_with("text/")).unwrap_or(false) {
            Some(String::from_utf8_lossy(&content[..content.len().min(500)]).to_string())
        } else {
            None
        };

        let material_info = {
            let mut registry = self.material_registry.lock().await;
            registry.remove(&id)
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

        let handle = self
            .acquisition_handles
            .lock()
            .await
            .remove(&id)
            .ok_or_else(|| {
                SinexError::processing(format!("Missing acquisition handle for material {}", id))
            })?;

        self.finalize_via_acquisition(
            manager,
            handle,
            material_info.as_ref(),
            content,
            mime_type,
            encoding,
            content_preview.clone(),
        )
        .await?;
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
        id: Ulid,
        reader: &mut R,
        mime_type: Option<&str>,
        encoding: Option<&str>,
    ) -> NodeResult<()>
    where
        R: tokio::io::AsyncRead + Unpin,
    {
        let is_text = mime_type.map(|m| m.starts_with("text/")).unwrap_or(false);
        let mut preview_bytes: Vec<u8> = Vec::new();
        let mut total_bytes: i64 = 0;

        let material_info = {
            let mut registry = self.material_registry.lock().await;
            registry.remove(&id)
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
                SinexError::processing(format!("Missing acquisition handle for material {}", id))
            })?;

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
                .map_err(|e| SinexError::processing(format!("Failed to append slice: {}", e)))?;
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
            .finalize_with_metadata(handle, MATERIAL_FINALIZE_REASON, metadata)
            .await
            .map_err(|e| SinexError::processing(format!("Failed to finalize material: {}", e)))?;

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
        mut handle: SourceMaterialHandle,
        info: Option<&StageMaterialInfo>,
        content: &[u8],
        _mime_type: Option<&str>,
        encoding: Option<&str>,
        content_preview: Option<String>,
    ) -> NodeResult<()> {
        for chunk in content.chunks(MAX_SLICE_BYTES) {
            manager
                .append_slice(&mut handle, chunk)
                .await
                .map_err(|e| SinexError::processing(format!("Failed to append slice: {}", e)))?;
        }

        let metadata =
            Self::build_finalize_metadata(info, content.len() as i64, content_preview, encoding);

        manager
            .finalize_with_metadata(handle, MATERIAL_FINALIZE_REASON, metadata)
            .await
            .map_err(|e| SinexError::processing(format!("Failed to finalize material: {}", e)))
    }
}

/// Helper trait for processors that support Stage-as-You-Go
#[async_trait::async_trait]
pub trait StageAsYouGoProcessor: Send + Sync {
    /// Process content with Stage-as-You-Go pattern
    ///
    /// This method should:
    /// 1. Register in-flight source material
    /// 2. Process content and emit events with source_material_id
    /// 3. Finalize source material with complete details
    async fn process_with_staging(
        &mut self,
        content: &[u8],
        source_uri: Option<&str>,
        metadata: serde_json::Value,
    ) -> NodeResult<StageAsYouGoResult>;
}

async fn reconcile_shared(
    registry: &Arc<Mutex<HashMap<Ulid, StageMaterialInfo>>>,
    handles: &Arc<Mutex<HashMap<Ulid, SourceMaterialHandle>>>,
    manager: &Arc<AcquisitionManager>,
    stale_ttl: Duration,
) -> NodeResult<StageReconciliationSummary> {
    let ttl = time::Duration::try_from(stale_ttl).unwrap_or_else(|_| time::Duration::seconds(0));
    let now = sinex_primitives::temporal::OffsetDateTime::now_utc();
    let stale_ids = {
        let registry_guard = registry.lock().await;
        registry_guard
            .iter()
            .filter_map(|(id, info)| {
                // Explicitly use nanosecond precision to avoid Duration type ambiguity
                let diff_nanos =
                    now.unix_timestamp_nanos() - info.last_activity.unix_timestamp_nanos();
                let diff = sinex_primitives::temporal::Duration::nanoseconds(diff_nanos as i64);
                if diff >= ttl {
                    Some(*id)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>()
    };

    let mut summary = StageReconciliationSummary::default();

    for material_id in stale_ids {
        let _ = {
            let mut registry_guard = registry.lock().await;
            registry_guard.remove(&material_id)
        };
        let handle = {
            let mut handles_guard = handles.lock().await;
            handles_guard.remove(&material_id)
        };

        if let Some(handle) = handle {
            match manager.cancel(handle, ORPHAN_CLEANUP_REASON).await {
                Ok(_) => {
                    summary.cancelled += 1;
                    info!(%material_id, "Cancelled stale Stage-as-You-Go material");
                }
                Err(err) => {
                    summary.errors += 1;
                    warn!(
                        error = %err,
                        %material_id,
                        "Failed to cancel stale Stage-as-You-Go material"
                    );
                }
            }
        } else {
            summary.skipped += 1;
            warn!(
                %material_id,
                "Stale Stage-as-You-Go material had no acquisition handle"
            );
        }
    }

    Ok(summary)
}

/// Result of Stage-as-You-Go processing
#[derive(Debug)]
pub struct StageAsYouGoResult {
    /// ID of the source material
    pub source_material_id: Ulid,
    /// IDs of events emitted
    pub event_ids: Vec<String>,
    /// Total bytes processed
    pub bytes_processed: usize,
    /// Processing duration
    pub duration: std::time::Duration,
}

/// Example implementation for a log file processor
///
/// Usage:
/// ```ignore
/// let processor = LogFileStageProcessor::new(context, "nginx");
/// ```
pub struct LogFileStageProcessor {
    context: StageAsYouGoContext,
    log_source: String, // "nginx", "apache", "syslog", etc.
}

impl StageAsYouGoContext {
    fn build_finalize_metadata(
        info: Option<&StageMaterialInfo>,
        total_bytes: i64,
        content_preview: Option<String>,
        encoding: Option<&str>,
    ) -> JsonValue {
        let mut base = info
            .map(|i| i.metadata.clone())
            .unwrap_or_else(|| json!({}));
        if !base.is_object() {
            base = json!({});
        }
        let map = base.as_object_mut().expect("metadata normalized to object");
        map.insert("total_bytes".to_string(), JsonValue::from(total_bytes));
        if let Some(preview) = content_preview {
            map.insert("content_preview".to_string(), JsonValue::String(preview));
        }
        if let Some(enc) = encoding {
            map.insert("encoding".to_string(), JsonValue::String(enc.to_string()));
        }
        JsonValue::Object(map.clone())
    }
}

impl LogFileStageProcessor {
    pub fn new(context: StageAsYouGoContext, log_source: impl Into<String>) -> Self {
        Self {
            context,
            log_source: log_source.into(),
        }
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

#[async_trait::async_trait]
impl StageAsYouGoProcessor for LogFileStageProcessor {
    async fn process_with_staging(
        &mut self,
        content: &[u8],
        source_uri: Option<&str>,
        metadata: serde_json::Value,
    ) -> NodeResult<StageAsYouGoResult> {
        let start_time = std::time::Instant::now();

        // Step 1: Register in-flight source material
        let source_material_id = self
            .context
            .register_in_flight("log_file", source_uri, metadata)
            .await?;

        // Step 2: Process content line by line, emitting events with provenance
        let mut event_ids = Vec::new();
        let content_str = String::from_utf8_lossy(content);
        let lines: Vec<&str> = content_str.lines().collect();

        for (line_num, line) in lines.iter().enumerate() {
            if line.trim().is_empty() {
                continue;
            }

            // Calculate byte offsets for this line
            let offset_start = lines[..line_num]
                .iter()
                .map(|l| l.len() + 1) // +1 for newline
                .sum::<usize>() as i64;
            let offset_end = offset_start + line.len() as i64;

            // Create event for this log line directly with unified Event<T>
            let payload = LogLinePayload {
                line: line.to_string(),
                line_number: (line_num + 1) as u64,
                log_source: self.log_source.clone(),
                log_file: source_uri.unwrap_or("unknown").to_string(),
                offset_start,
                offset_end,
                source_material_id: source_material_id.to_string(),
            };

            // Create typed event and convert to JsonValue for emission
            let typed_event = Event::new(
                payload,
                sinex_primitives::events::builder::Provenance::Synthesis {
                    source_event_ids: sinex_primitives::non_empty::NonEmptyVec::single(
                        sinex_primitives::events::builder::EventId::from_ulid(Ulid::new()),
                    ),
                    operation_id: None,
                },
            );

            // Convert to JsonValue event for emission
            let mut event = typed_event.to_json_event()?;
            event.id = Some(Id::from_ulid(Ulid::new()));
            let now = sinex_primitives::temporal::OffsetDateTime::now_utc();
            if event.ts_orig.is_none() {
                event.ts_orig = Some(now.into());
            }

            // Emit with provenance
            let event_id = self
                .context
                .emit_event_with_provenance(
                    event,
                    source_material_id,
                    Some(offset_start),
                    Some(offset_end),
                )
                .await?;

            event_ids.push(event_id.to_string());
        }

        // Step 3: Finalize source material with complete details
        self.context
            .finalize_source_material(
                source_material_id,
                content,
                Some("text/plain"),
                Some("utf-8"),
            )
            .await?;

        Ok(StageAsYouGoResult {
            source_material_id,
            event_ids,
            bytes_processed: content.len(),
            duration: start_time.elapsed(),
        })
    }
}

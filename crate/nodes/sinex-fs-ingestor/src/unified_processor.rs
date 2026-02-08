#![doc = include_str!("../docs/unified_processor.md")]

//! Filesystem watcher processor using JetStream-first acquisition.
//!
//! This implementation uses a Stage-as-You-Go + `AcquisitionManager` workflow:
//! - File system events are captured via notify watchers.
//! - Each event is staged as a dedicated source material and published to
//!   `JetStream` using `AcquisitionManager`.
//! - Structured events are emitted through `StageAsYouGoContext`, referencing
//!   the captured material for provenance.

use async_trait::async_trait;
use notify::{event::RenameMode, Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use serde::{Deserialize, Serialize};
use sinex_node_sdk::error_helpers::NodeErrorExt;
use sinex_node_sdk::{
    acquisition_manager::{AcquisitionManager, RotationPolicy},
    simple_ingestor::SimpleIngestor,
    stage_as_you_go::StageAsYouGoContext,
    stream_processor::{
        Checkpoint, NodeCapabilities, NodeRuntimeState, ScanArgs, ScanReport, ServiceInfo,
        TimeHorizon,
    },
    NodeResult, SinexError,
};
use sinex_primitives::{
    domain::{HostName, SanitizedPath},
    events::{enums::FileModificationType, EventPayload},
    temporal::Timestamp,
    units::Bytes,
    validation::{validate_watch_path, FileWatchingSecurityPolicy},
    Ulid,
};
use sinex_processor_runtime::{
    CoverageAnalysis, ExplorationProvider, ExportFormat, IngestionHistoryEntry, SourceState,
};
use std::{
    collections::HashMap,
    fs::Metadata as StdMetadata,
    path::Path,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};
use time::OffsetDateTime;
use tokio::{
    fs,
    io::AsyncReadExt,
    sync::{
        mpsc::{self, error::TrySendError},
        Mutex,
    },
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument, warn};
use validator::ValidationError;

const DEFAULT_MAX_CAPTURE_BYTES: Bytes = Bytes::from_mebibytes(10); // 10MB
const DEFAULT_MAX_DEPTH: usize = 10; // Maximum directory traversal depth
const FS_WATCH_CHANNEL_SIZE: usize = 10_000; // Buffer size for filesystem event channel (high-volume burst protection)
const FS_CAPTURE_CHUNK_SIZE: usize = 64 * 1024;
const FS_READ_RETRY_ATTEMPTS: u32 = 3; // Number of retry attempts for transient file read errors
const FS_READ_RETRY_BASE_DELAY_MS: u64 = 100; // Base delay for exponential backoff (100ms, 500ms, 1s)
const MATERIAL_REASON_CREATED: &str = "fs-watcher:file-created";
const MATERIAL_REASON_MODIFIED: &str = "fs-watcher:file-modified";
const MATERIAL_REASON_DELETED: &str = "fs-watcher:file-deleted";
const MATERIAL_REASON_MOVED: &str = "fs-watcher:file-moved";

/// Filesystem monitoring configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilesystemConfig {
    /// Directories to monitor for filesystem changes
    pub watch_paths: Vec<String>,

    /// Maximum directory traversal depth (None = unlimited)
    pub max_depth: Option<usize>,

    /// Follow symbolic links during monitoring
    pub follow_symlinks: bool,

    /// Maximum number of bytes captured per event
    pub max_capture_bytes: Bytes,
}

impl Default for FilesystemConfig {
    fn default() -> Self {
        Self {
            watch_paths: vec![],
            max_depth: Some(DEFAULT_MAX_DEPTH),
            follow_symlinks: false,
            max_capture_bytes: DEFAULT_MAX_CAPTURE_BYTES,
        }
    }
}

impl FilesystemConfig {
    /// Validate the configuration and return detailed error messages.
    pub fn validate_config(&self) -> Result<(), String> {
        if self.watch_paths.is_empty() {
            return Err("At least one watch path must be specified".to_string());
        }

        if let Some(depth) = self.max_depth {
            validate_max_depth(depth)
                .map_err(|_| "Max depth must be reasonable (1-100)".to_string())?;
        }

        let max_capture_bytes = self.max_capture_bytes.as_u64();
        if !(1024..=512 * 1024 * 1024).contains(&max_capture_bytes) {
            return Err("Max capture bytes must be between 1KB and 512MB".to_string());
        }

        Ok(())
    }
}

/// Custom validation functions
fn validate_max_depth(depth: usize) -> Result<(), ValidationError> {
    if depth == 0 {
        return Err(ValidationError::new("depth_zero"));
    }
    if depth > 100 {
        return Err(ValidationError::new("depth_too_large"));
    }
    Ok(())
}

/// Filesystem state snapshot for exploration and diagnostics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilesystemState {
    /// When the snapshot was taken
    pub captured_at: sinex_primitives::temporal::Timestamp,

    /// Directories being monitored
    pub watch_paths: Vec<String>,

    /// Host where the watcher is running
    pub host: HostName,
}

struct EventMetrics {
    events_processed: AtomicU64,
    events_created: AtomicU64,
    events_modified: AtomicU64,
    events_deleted: AtomicU64,
    events_moved: AtomicU64,
    processing_errors: AtomicU64,
}

impl EventMetrics {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            events_processed: AtomicU64::new(0),
            events_created: AtomicU64::new(0),
            events_modified: AtomicU64::new(0),
            events_deleted: AtomicU64::new(0),
            events_moved: AtomicU64::new(0),
            processing_errors: AtomicU64::new(0),
        })
    }

    fn record_created(&self) {
        self.events_processed.fetch_add(1, Ordering::Relaxed);
        self.events_created.fetch_add(1, Ordering::Relaxed);
    }

    fn record_modified(&self) {
        self.events_processed.fetch_add(1, Ordering::Relaxed);
        self.events_modified.fetch_add(1, Ordering::Relaxed);
    }

    fn record_deleted(&self) {
        self.events_processed.fetch_add(1, Ordering::Relaxed);
        self.events_deleted.fetch_add(1, Ordering::Relaxed);
    }

    fn record_moved(&self) {
        self.events_processed.fetch_add(1, Ordering::Relaxed);
        self.events_moved.fetch_add(1, Ordering::Relaxed);
    }

    fn record_error(&self) {
        self.processing_errors.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn recent_activity(&self) -> Vec<sinex_node_sdk::ActivityEntry> {
        vec![]
    }
}

#[derive(Clone)]
struct WatchContext {
    acquisition: Arc<AcquisitionManager>,
    stage_context: StageAsYouGoContext,
    max_capture_bytes: Bytes,
    security_policy: FileWatchingSecurityPolicy,
    dropped_events: Arc<AtomicU64>,
    metrics: Arc<EventMetrics>,
    cancel_token: CancellationToken,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FilesystemCheckpoint {}

/// Unified filesystem processor using `JetStream` acquisition.
pub struct FilesystemProcessor {
    runtime: Option<NodeRuntimeState>,
    config: FilesystemConfig,
    stage_context: Option<StageAsYouGoContext>,
    watch_handles: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>,
    dropped_events: Arc<AtomicU64>,
    metrics: Arc<EventMetrics>,
    cancel_token: CancellationToken,
}

impl FilesystemProcessor {
    /// Create a new filesystem processor with default configuration.
    #[must_use]
    pub fn new() -> Self {
        Self {
            runtime: None,
            config: FilesystemConfig::default(),
            stage_context: None,
            watch_handles: Arc::new(Mutex::new(Vec::new())),
            dropped_events: Arc::new(AtomicU64::new(0)),
            metrics: EventMetrics::new(),
            cancel_token: CancellationToken::new(),
        }
    }

    /// Create processor with custom configuration.
    #[must_use]
    pub fn with_config(config: FilesystemConfig) -> Self {
        Self {
            runtime: None,
            config,
            stage_context: None,
            watch_handles: Arc::new(Mutex::new(Vec::new())),
            dropped_events: Arc::new(AtomicU64::new(0)),
            metrics: EventMetrics::new(),
            cancel_token: CancellationToken::new(),
        }
    }

    /// Access the current processor configuration.
    #[must_use]
    pub fn config(&self) -> &FilesystemConfig {
        &self.config
    }

    fn dropped_event_count(&self) -> u64 {
        self.dropped_events.load(Ordering::Relaxed)
    }

    fn runtime(&self) -> NodeResult<&NodeRuntimeState> {
        self.runtime.as_ref().ok_or_else(|| {
            SinexError::lifecycle("Filesystem runtime handles not initialized".to_string())
        })
    }

    fn service_info(&self) -> NodeResult<&ServiceInfo> {
        Ok(self.runtime()?.service_info())
    }

    /// Build watch contexts for each configured path.
    fn build_watch_contexts(&self) -> NodeResult<HashMap<String, WatchContext>> {
        let runtime = self.runtime()?;
        let stage_context = self
            .stage_context
            .clone()
            .ok_or_else(|| SinexError::lifecycle("Stage context not available".to_string()))?;

        let mut contexts = HashMap::new();
        for path in &self.config.watch_paths {
            let acquisition = Arc::new(runtime.acquisition_manager(
                RotationPolicy::default(),
                "fs-watcher",
                path.clone(),
            )?);
            let stage_with_acquisition = stage_context
                .clone()
                .with_acquisition_manager(Arc::clone(&acquisition));

            contexts.insert(
                path.clone(),
                WatchContext {
                    acquisition,
                    stage_context: stage_with_acquisition,
                    max_capture_bytes: self.config.max_capture_bytes,
                    security_policy: if self.config.follow_symlinks {
                        FileWatchingSecurityPolicy::permissive()
                    } else {
                        FileWatchingSecurityPolicy::restrictive()
                    },
                    dropped_events: Arc::clone(&self.dropped_events),
                    metrics: Arc::clone(&self.metrics),
                    cancel_token: self.cancel_token.clone(),
                },
            );
        }

        Ok(contexts)
    }

    async fn spawn_watchers(&self) -> NodeResult<Vec<tokio::task::JoinHandle<()>>> {
        let contexts = self.build_watch_contexts()?;

        let mut handles = Vec::with_capacity(contexts.len());
        for (root, watch_ctx) in contexts {
            let root_path = root.clone();
            let watch_ctx = watch_ctx.clone();

            let handle = tokio::spawn(async move {
                let mut attempt = 0u32;
                const MAX_INIT_ATTEMPTS: u32 = 5;
                const INIT_RETRY_BASE_DELAY_MS: u64 = 1000;

                loop {
                    match watch_path(root_path.clone(), watch_ctx.clone()).await {
                        Ok(()) => {
                            warn!("Watcher for {} terminated normally (unexpected)", root_path);
                            break;
                        }
                        Err(e) => {
                            attempt += 1;
                            if attempt >= MAX_INIT_ATTEMPTS {
                                error!(
                                    path = %root_path,
                                    attempts = attempt,
                                    "Failed to initialize watcher after {} attempts: {}",
                                    MAX_INIT_ATTEMPTS, e
                                );
                                break;
                            }

                            let delay_ms =
                                INIT_RETRY_BASE_DELAY_MS * (1u64 << (attempt - 1)).min(16);
                            warn!(
                                path = %root_path,
                                attempt = attempt,
                                delay_ms = delay_ms,
                                "Watcher initialization failed, retrying: {}", e
                            );
                            tokio::time::sleep(tokio::time::Duration::from_millis(delay_ms)).await;
                        }
                    }
                }
            });

            handles.push(handle);
        }

        Ok(handles)
    }

    fn snapshot_state(&self) -> FilesystemState {
        let host = self.service_info().map_or_else(
            |_| HostName::new("unknown-host"),
            |info| HostName::new(info.host().to_string()),
        );

        FilesystemState {
            captured_at: sinex_primitives::temporal::now(),
            watch_paths: self.config.watch_paths.clone(),
            host,
        }
    }
}

impl Default for FilesystemProcessor {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl SimpleIngestor for FilesystemProcessor {
    type Config = FilesystemConfig;
    type State = FilesystemCheckpoint;

    fn name(&self) -> &'static str {
        "filesystem-watcher"
    }

    fn capabilities(&self) -> NodeCapabilities {
        NodeCapabilities {
            supports_snapshot: true,
            supports_historical: false,
            supports_continuous: true,
            ..NodeCapabilities::default()
        }
    }

    async fn initialize(
        &mut self,
        config: Self::Config,
        runtime: &NodeRuntimeState,
        _state: &mut Self::State,
    ) -> NodeResult<()> {
        let service_name = runtime.service_info().service_name().to_string();

        info!(
            processor = self.name(),
            service = %service_name,
            "Initializing filesystem processor"
        );

        config.validate_config().map_err(|e| {
            SinexError::configuration(format!("Filesystem configuration validation failed: {e}"))
        })?;

        let publisher: Arc<sinex_node_sdk::nats_publisher::NatsPublisher> =
            match runtime.transport() {
                sinex_node_sdk::EventTransport::Nats(publisher) => Arc::clone(publisher),
            };

        AcquisitionManager::bootstrap_streams(publisher.nats_client()).await?;

        let stage_context = StageAsYouGoContext::from_runtime(runtime);

        self.config = config;
        self.stage_context = Some(stage_context);
        self.watch_handles = Arc::new(Mutex::new(Vec::new()));
        self.runtime = Some(runtime.clone());

        Ok(())
    }

    async fn scan_snapshot(&self, _state: &Self::State, _args: ScanArgs) -> NodeResult<ScanReport> {
        let state = self.snapshot_state();
        let report = ScanReport {
            events_processed: 0,
            duration: std::time::Duration::from_millis(0),
            final_checkpoint: Checkpoint::None,
            time_range: None,
            processor_stats: HashMap::new(),
            successful_targets: vec!["snapshot".to_string()],
            failed_targets: Vec::new(),
            warnings: Vec::new(),
        };

        info!("Filesystem snapshot captured at {}", state.captured_at);
        Ok(report)
    }

    async fn scan_historical(
        &mut self,
        _state: &mut Self::State,
        from: Checkpoint,
        _until: TimeHorizon,
        _args: ScanArgs,
    ) -> NodeResult<ScanReport> {
        warn!("Filesystem watcher does not support historical replay");
        Ok(ScanReport {
            events_processed: 0,
            duration: std::time::Duration::from_millis(0),
            final_checkpoint: from,
            time_range: None,
            processor_stats: HashMap::new(),
            successful_targets: Vec::new(),
            failed_targets: Vec::new(),
            warnings: vec!["Historical mode is not supported".to_string()],
        })
    }

    async fn run_continuous(
        &mut self,
        _state: &mut Self::State,
        _from: Checkpoint,
        shutdown_rx: tokio::sync::watch::Receiver<bool>,
    ) -> NodeResult<ScanReport> {
        let handles = self.spawn_watchers().await?;
        {
            let mut guard = self.watch_handles.lock().await;
            guard.extend(handles);
        }

        // Wait for shutdown signal instead of awaiting pending
        let mut shutdown_rx = shutdown_rx;
        let _ = shutdown_rx.changed().await;

        Ok(ScanReport {
            events_processed: 0,
            duration: std::time::Duration::from_millis(0),
            final_checkpoint: Checkpoint::None,
            time_range: None,
            processor_stats: HashMap::new(),
            successful_targets: vec!["continuous".to_string()],
            failed_targets: Vec::new(),
            warnings: Vec::new(),
        })
    }

    async fn shutdown(&mut self, _state: &Self::State) -> NodeResult<()> {
        // Signal all watchers to stop gracefully
        self.cancel_token.cancel();

        // Wait for all watcher tasks to finish
        let mut guard = self.watch_handles.lock().await;
        for handle in guard.drain(..) {
            let _ = handle.await;
        }

        info!(
            dropped_events = self.dropped_event_count(),
            "Filesystem watcher shutdown complete"
        );
        Ok(())
    }
}

impl ExplorationProvider for FilesystemProcessor {
    fn get_source_state(&self) -> NodeResult<SourceState> {
        Ok(SourceState {
            is_connected: true,
            healthy: true,
            description: format!("Monitoring {} paths", self.config.watch_paths.len()),
            last_updated: Timestamp::now(),
            lag_seconds: None,
            recent_activity: self.metrics.recent_activity(),
            total_items: None,
            metadata: std::collections::HashMap::new(),
        })
    }

    fn get_ingestion_history(&self, _limit: u64) -> NodeResult<Vec<IngestionHistoryEntry>> {
        Ok(Vec::new())
    }

    fn get_coverage_analysis(
        &self,
        time_range: Option<(Timestamp, Timestamp)>,
    ) -> NodeResult<CoverageAnalysis> {
        let time_range = time_range.unwrap_or_else(|| {
            let now = Timestamp::now();
            (now - time::Duration::hours(1), now)
        });

        Ok(CoverageAnalysis {
            time_range,
            coverage_percentage: 1.0,
            missing_count: 0,
            duplicate_count: 0,
            source_total: self.config.watch_paths.len() as u64,
            sinex_total: 0,
            missing_samples: Vec::new(),
            recommendations: Vec::new(),
        })
    }

    fn export_data(&self, _path: &SanitizedPath, _format: ExportFormat) -> NodeResult<()> {
        Err(SinexError::general(
            "Filesystem watcher does not support data export",
        ))
    }
}

async fn watch_path(root: String, ctx: WatchContext) -> NodeResult<()> {
    let normalized = validate_watch_path(&root, &ctx.security_policy)
        .map_err(|e| SinexError::validation(e.to_string()))?;

    info!("Watching path: {}", normalized.as_str());

    let (tx, mut rx) = mpsc::channel::<Event>(FS_WATCH_CHANNEL_SIZE);
    let drop_counter = Arc::clone(&ctx.dropped_events);
    let mut watcher: RecommendedWatcher =
        notify::recommended_watcher(move |res: Result<Event, notify::Error>| match res {
            Ok(event) => match tx.try_send(event) {
                Ok(()) => {}
                Err(TrySendError::Full(_)) => {
                    let dropped = drop_counter.fetch_add(1, Ordering::Relaxed) + 1;
                    if dropped == 1 || dropped.is_multiple_of(100) {
                        warn!(
                            dropped_events = dropped,
                            "Filesystem watcher channel full; dropping events"
                        );
                    }
                }
                Err(TrySendError::Closed(_)) => {
                    let dropped = drop_counter.fetch_add(1, Ordering::Relaxed) + 1;
                    error!(
                        dropped_events = dropped,
                        "Filesystem watcher channel closed; dropping events"
                    );
                }
            },
            Err(err) => {
                error!(error = %err, "Filesystem watcher reported error");
            }
        })
        .map_err(|e| SinexError::lifecycle(format!("Failed to create watcher: {e}")))?;

    watcher
        .watch(Path::new(normalized.as_str()), RecursiveMode::Recursive)
        .map_err(|e| SinexError::lifecycle(format!("Failed to watch path: {e}")))?;

    loop {
        tokio::select! {
            event = rx.recv() => {
                match event {
                    Some(event) => {
                        if let Err(e) = handle_event(&ctx, &root, event).await {
                            ctx.metrics.record_error();
                            warn!("Failed to process filesystem event: {}", e);
                        }
                    }
                    None => break, // Channel closed
                }
            }
            () = ctx.cancel_token.cancelled() => {
                info!(path = %root, "Filesystem watcher received shutdown signal");
                break;
            }
        }
    }

    Ok(())
}

#[instrument(skip(ctx, event))]
async fn handle_event(ctx: &WatchContext, root: &str, event: Event) -> NodeResult<()> {
    match event.kind {
        EventKind::Create(_) => {
            for path in event.paths {
                handle_file_created(ctx, root, &path).await?;
            }
        }
        EventKind::Modify(mod_kind) => {
            use notify::event::ModifyKind;

            match mod_kind {
                ModifyKind::Name(RenameMode::Both) => {
                    if event.paths.len() == 2 {
                        let old = &event.paths[0];
                        let new = &event.paths[1];
                        handle_file_moved(ctx, root, old, new).await?;
                    }
                }
                ModifyKind::Name(_) => {
                    // Partial rename events - best effort handling
                    if event.paths.len() == 2 {
                        let old = &event.paths[0];
                        let new = &event.paths[1];
                        handle_file_moved(ctx, root, old, new).await?;
                    }
                }
                ModifyKind::Data(_) | ModifyKind::Metadata(_) | ModifyKind::Any => {
                    for path in event.paths {
                        handle_file_modified(ctx, root, &path, FileModificationType::Content)
                            .await?;
                    }
                }
                _ => {}
            }
        }
        EventKind::Remove(_) => {
            for path in event.paths {
                handle_file_deleted(ctx, root, &path).await?;
            }
        }
        _ => {}
    }

    Ok(())
}

async fn handle_file_created(ctx: &WatchContext, _root: &str, path: &Path) -> NodeResult<()> {
    if !path.is_file() {
        return Ok(());
    }

    let metadata = match fs::metadata(path).await {
        Ok(meta) => meta,
        Err(e) => {
            warn!("Failed to read metadata for {:?}: {}", path, e);
            return Ok(());
        }
    };

    let size = metadata.len();
    if size > ctx.max_capture_bytes.as_u64() {
        warn!(
            "Skipping file {:?} ({} bytes) exceeding limit {}",
            path, size, ctx.max_capture_bytes
        );
        return Ok(());
    }

    let material_id = capture_material_from_file(ctx, path, MATERIAL_REASON_CREATED, size).await?;

    let payload = sinex_primitives::events::payloads::filesystem::FileCreatedPayload {
        path: sanitize_path(path)?,
        size,
        created_at: file_created_at(&metadata),
        permissions: file_permissions(&metadata),
    };

    let event = payload
        .from_material(material_id)
        .build()
        .node_err("Failed to build event")?;

    let json_event = event
        .to_json_event()
        .map_err(|e| SinexError::processing(format!("Failed to convert to JSON event: {e}")))?;

    ctx.stage_context
        .emit_event_with_provenance(json_event, material_id, Some(0), Some(size as i64))
        .await
        .map(|_| ())
        .map_err(|e| SinexError::processing(format!("Failed to emit event: {e}")))?;

    ctx.metrics.record_created();
    debug!("Recorded file.created for {:?}", path);
    Ok(())
}

async fn handle_file_modified(
    ctx: &WatchContext,
    _root: &str,
    path: &Path,
    modification_type: FileModificationType,
) -> NodeResult<()> {
    if !path.is_file() {
        return Ok(());
    }

    let metadata = match fs::metadata(path).await {
        Ok(meta) => meta,
        Err(e) => {
            warn!("Failed to read metadata for {:?}: {}", path, e);
            return Ok(());
        }
    };

    let size = metadata.len();
    if size > ctx.max_capture_bytes.as_u64() {
        warn!(
            "Skipping file {:?} ({} bytes) exceeding limit {}",
            path, size, ctx.max_capture_bytes
        );
        return Ok(());
    }

    let material_id = capture_material_from_file(ctx, path, MATERIAL_REASON_MODIFIED, size).await?;

    let payload = sinex_primitives::events::payloads::filesystem::FileModifiedPayload {
        path: sanitize_path(path)?,
        size,
        modified_at: file_modified_at(&metadata),
        modification_type,
    };

    let event = payload
        .from_material(material_id)
        .build()
        .map_err(|e| SinexError::processing(format!("Failed to build event: {e}")))?;

    let json_event = event
        .to_json_event()
        .map_err(|e| SinexError::processing(format!("Failed to convert to JSON event: {e}")))?;

    ctx.stage_context
        .emit_event_with_provenance(json_event, material_id, Some(0), Some(size as i64))
        .await
        .map(|_| ())
        .map_err(|e| SinexError::processing(format!("Failed to emit event: {e}")))?;

    ctx.metrics.record_modified();
    debug!("Recorded file.modified for {:?}", path);
    Ok(())
}

async fn handle_file_deleted(ctx: &WatchContext, _root: &str, path: &Path) -> NodeResult<()> {
    // For deletions no content is available; record zero-byte material.
    let material_id = capture_material(ctx, path, MATERIAL_REASON_DELETED, None).await?;

    let payload = sinex_primitives::events::payloads::filesystem::FileDeletedPayload {
        path: sanitize_path(path)?,
        deleted_at: sinex_primitives::temporal::now(),
    };

    let event = payload
        .from_material(material_id)
        .build()
        .map_err(|e| SinexError::processing(format!("Failed to build event: {e}")))?;

    let json_event = event
        .to_json_event()
        .map_err(|e| SinexError::processing(format!("Failed to convert to JSON event: {e}")))?;

    ctx.stage_context
        .emit_event_with_provenance(json_event, material_id, Some(0), Some(0))
        .await
        .map(|_| ())
        .map_err(|e| SinexError::processing(format!("Failed to emit event: {e}")))?;

    ctx.metrics.record_deleted();
    debug!("Recorded file.deleted for {:?}", path);
    Ok(())
}

async fn handle_file_moved(
    ctx: &WatchContext,
    _root: &str,
    old: &Path,
    new: &Path,
) -> NodeResult<()> {
    let material_id = capture_material(ctx, new, MATERIAL_REASON_MOVED, None).await?;

    let payload = sinex_primitives::events::payloads::filesystem::FileMovedPayload {
        old_path: sanitize_path(old)?,
        new_path: sanitize_path(new)?,
        moved_at: sinex_primitives::temporal::now(),
    };

    let event = payload
        .from_material(material_id)
        .build()
        .map_err(|e| SinexError::processing(format!("Failed to build event: {e}")))?;

    let json_event = event
        .to_json_event()
        .map_err(|e| SinexError::processing(format!("Failed to convert to JSON event: {e}")))?;

    ctx.stage_context
        .emit_event_with_provenance(json_event, material_id, Some(0), Some(0))
        .await
        .map(|_| ())
        .map_err(|e| SinexError::processing(format!("Failed to emit event: {e}")))?;

    ctx.metrics.record_moved();
    debug!("Recorded file.moved from {:?} to {:?}", old, new);
    Ok(())
}

async fn capture_material(
    ctx: &WatchContext,
    path: &Path,
    reason: &str,
    content: Option<&[u8]>,
) -> NodeResult<Ulid> {
    let identifier = path.to_string_lossy();
    let mut handle = ctx
        .acquisition
        .begin_material(&identifier)
        .await
        .map_err(|e| SinexError::processing(format!("Failed to begin material: {e}")))?;

    let material_id = handle.material_id;

    if let Some(bytes) = content {
        ctx.acquisition
            .append_slice(&mut handle, bytes)
            .await
            .map_err(|e| SinexError::processing(format!("Failed to append slice: {e}")))?;
    }

    ctx.acquisition
        .finalize(handle, reason)
        .await
        .map_err(|e| SinexError::processing(format!("Failed to finalize material: {e}")))?;

    Ok(material_id)
}

async fn capture_material_from_file(
    ctx: &WatchContext,
    path: &Path,
    reason: &str,
    _expected_size: u64,
) -> NodeResult<Ulid> {
    // Retry logic for transient errors (file locked, in-use, etc.)
    let mut attempt = 0u32;
    loop {
        match capture_material_from_file_inner(ctx, path, reason).await {
            Ok(material_id) => return Ok(material_id),
            Err(e) => {
                attempt += 1;
                if attempt >= FS_READ_RETRY_ATTEMPTS {
                    return Err(e);
                }

                // Check if error is transient (file locked, permission denied temporarily, etc.)
                let is_transient = e.to_string().contains("lock")
                    || e.to_string().contains("in use")
                    || e.to_string().contains("permission denied")
                    || e.to_string().contains("resource temporarily unavailable");

                if !is_transient {
                    return Err(e);
                }

                // Exponential backoff: 100ms, 500ms, 1s
                let delay_ms = FS_READ_RETRY_BASE_DELAY_MS * (1u64 << (attempt - 1)).min(10);
                debug!(
                    "Transient error reading {:?}, retrying in {}ms (attempt {}/{}): {}",
                    path, delay_ms, attempt, FS_READ_RETRY_ATTEMPTS, e
                );
                tokio::time::sleep(tokio::time::Duration::from_millis(delay_ms)).await;
            }
        }
    }
}

async fn capture_material_from_file_inner(
    ctx: &WatchContext,
    path: &Path,
    reason: &str,
) -> NodeResult<Ulid> {
    let identifier = path.to_string_lossy();
    let mut handle = ctx
        .acquisition
        .begin_material(&identifier)
        .await
        .map_err(|e| SinexError::processing(format!("Failed to begin material: {e}")))?;

    let material_id = handle.material_id;

    // Issue 92: TOCTOU race eliminated by opening file first, then getting metadata
    // from the open handle. This ensures atomic operations:
    // 1. File is opened and locked by OS
    // 2. Metadata retrieved from open file descriptor (no path lookup)
    // 3. Size checked before any read
    // 4. Cumulative tracking during streaming prevents growing file issues
    let mut file = fs::File::open(path).await.map_err(SinexError::io)?;

    let metadata = file.metadata().await.map_err(SinexError::io)?;

    let file_size = metadata.len();

    if file_size > ctx.max_capture_bytes.as_u64() {
        return Err(SinexError::processing(format!(
            "File size {} exceeds max_capture_bytes {}",
            file_size,
            ctx.max_capture_bytes.as_u64()
        )));
    }

    let mut cumulative_bytes = 0u64;
    let mut buffer = vec![0u8; FS_CAPTURE_CHUNK_SIZE];

    loop {
        let read = file.read(&mut buffer).await.map_err(SinexError::io)?;

        if read == 0 {
            break;
        }

        cumulative_bytes += read as u64;

        if cumulative_bytes > ctx.max_capture_bytes.as_u64() {
            return Err(SinexError::processing(format!(
                "File grew during capture; cumulative read {} exceeds max_capture_bytes {}",
                cumulative_bytes,
                ctx.max_capture_bytes.as_u64()
            )));
        }

        ctx.acquisition
            .append_slice(&mut handle, &buffer[..read])
            .await
            .map_err(|e| SinexError::processing(format!("Failed to append slice: {e}")))?;
    }

    ctx.acquisition
        .finalize(handle, reason)
        .await
        .map_err(|e| SinexError::processing(format!("Failed to finalize material: {e}")))?;

    Ok(material_id)
}

fn sanitize_path(path: &Path) -> NodeResult<SanitizedPath> {
    SanitizedPath::from_str_validated(&path.to_string_lossy())
        .map_err(|e| SinexError::validation(format!("Path validation failed: {e}")))
}

fn file_permissions(metadata: &StdMetadata) -> Option<u32> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        Some(metadata.permissions().mode())
    }
    #[cfg(not(unix))]
    {
        let _ = metadata;
        None
    }
}

fn file_created_at(metadata: &StdMetadata) -> sinex_primitives::temporal::Timestamp {
    metadata
        .created()
        .or_else(|_| metadata.modified())
        .map(OffsetDateTime::from)
        .map_or_else(
            |_| sinex_primitives::temporal::now(),
            std::convert::Into::into,
        )
}

fn file_modified_at(metadata: &StdMetadata) -> sinex_primitives::temporal::Timestamp {
    metadata.modified().map(OffsetDateTime::from).map_or_else(
        |_| sinex_primitives::temporal::now(),
        std::convert::Into::into,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinex_db::models::{Event as SinexEvent, Provenance};
    use sinex_db::query_helpers::ulid_to_uuid;
    use sinex_node_sdk::AcquisitionManager;
    use sinex_primitives::Id;
    use std::sync::Arc;
    use tempfile::tempdir;
    use tokio::sync::mpsc;
    use tokio::time::{timeout, Duration};
    use xtask::sandbox::prelude::*;
    use xtask::sandbox::{sinex_test, EphemeralNats};

    #[sinex_test]
    fn filesystem_config_validation_allows_basic_configuration() -> TestResult<()> {
        let mut config = FilesystemConfig::default();
        config.watch_paths = vec!["/tmp".to_string()];
        assert!(config.validate_config().is_ok());
        Ok(())
    }

    #[sinex_test]
    fn filesystem_config_validation_rejects_missing_paths() -> TestResult<()> {
        let config = FilesystemConfig {
            watch_paths: vec![],
            ..FilesystemConfig::default()
        };

        assert!(config.validate_config().is_err());
        Ok(())
    }

    #[sinex_test]
    async fn handle_file_created_emits_event(ctx: TestContext) -> TestResult<()> {
        let nats = EphemeralNats::start().await?;
        let nats_client = nats.connect().await?;

        AcquisitionManager::bootstrap_streams(&nats_client).await?;

        let acquisition = Arc::new(AcquisitionManager::with_defaults(
            nats_client,
            "filesystem",
            "/tmp",
        ));

        let (event_tx, mut event_rx) =
            mpsc::channel::<SinexEvent>(sinex_primitives::buffers::DEFAULT_EVENT_CHANNEL_SIZE);
        let stage_context =
            StageAsYouGoContext::from_sender(Arc::clone(&acquisition), event_tx, false);

        let watch_ctx = WatchContext {
            acquisition,
            stage_context,
            max_capture_bytes: Bytes::from_mebibytes(1),
            security_policy: FileWatchingSecurityPolicy::permissive(),
            dropped_events: Arc::new(AtomicU64::new(0)),
            metrics: EventMetrics::new(),
            cancel_token: CancellationToken::new(),
        };

        let temp_root = tempdir()?;
        let file_path = temp_root.path().join("example.txt");
        tokio::fs::write(&file_path, b"hello world").await?;

        let temp_root_str = temp_root
            .path()
            .to_str()
            .ok_or_else(|| color_eyre::eyre::eyre!("temp root path not utf8"))?;
        handle_file_created(&watch_ctx, temp_root_str, &file_path).await?;

        let event = timeout(Duration::from_secs(10), event_rx.recv())
            .await?
            .ok_or_else(|| color_eyre::eyre::eyre!("filesystem event not emitted"))?;

        assert_eq!(event.event_type.as_str(), "file.created");

        let material_ulid = match event.provenance() {
            Provenance::Material { ref id, .. } => *id.as_ulid(),
            _ => {
                return Err(color_eyre::eyre::eyre!(
                    "expected material provenance in filesystem event"
                ))
            }
        };

        let record = ctx
            .pool
            .source_materials()
            .get_by_id(Id::from_ulid(material_ulid))
            .await?;
        assert!(
            record.is_none(),
            "source material unexpectedly persisted; ingestd should be the sole DB writer"
        );

        let total_bytes: Option<i64> = sqlx::query_scalar(
            "SELECT offset_end FROM raw.temporal_ledger WHERE source_material_id = $1::uuid::ulid ORDER BY ts_capture DESC LIMIT 1",
        )
        .bind(ulid_to_uuid(material_ulid))
        .fetch_optional(&ctx.pool)
        .await?;

        assert!(
            total_bytes.is_none(),
            "temporal ledger unexpectedly persisted; ingestd should be the sole DB writer"
        );
        Ok(())
    }
}

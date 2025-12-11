#![doc = include_str!("../docs/unified_processor.md")]

//! Filesystem watcher processor using JetStream-first acquisition.
//!
//! This implementation replaces the legacy sensd pipeline with a direct
//! Stage-as-You-Go + AcquisitionManager workflow:
//! - File system events are captured via notify watchers.
//! - Each event is staged as a dedicated source material and published to
//!   JetStream using `AcquisitionManager`.
//! - Structured events are emitted through `StageAsYouGoContext`, referencing
//!   the captured material for provenance.

use async_trait::async_trait;
use color_eyre::eyre;
use notify::{event::RenameMode, Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use serde::{Deserialize, Serialize};
use sinex_core::{
    types::{
        domain::SanitizedPath,
        validation::{validate_watch_path, FileWatchingSecurityPolicy},
        Id, Ulid,
    },
    Event as CoreEvent, HostName, JsonValue, Provenance,
};
use sinex_processor_runtime::{
    CoverageAnalysis, ExplorationProvider, ExportFormat, IngestionHistoryEntry, SourceState,
};
use sinex_satellite_sdk::{
    acquisition_manager::{AcquisitionManager, RotationPolicy},
    stage_as_you_go::StageAsYouGoContext,
    stream_processor::{
        Checkpoint, ProcessorCapabilities, ProcessorInitContext, ProcessorRuntimeState,
        ProcessorType, ScanArgs, ScanEstimate, ScanReport, ServiceInfo, StatefulStreamProcessor,
        TimeHorizon,
    },
    SatelliteError, SatelliteResult,
};
use std::{collections::HashMap, fs::Metadata as StdMetadata, path::Path, sync::Arc};
use tokio::{
    fs,
    sync::{mpsc, Mutex},
};
use tracing::{debug, error, info, instrument, warn};
use validator::ValidationError;

const DEFAULT_MAX_CAPTURE_BYTES: u64 = 10 * 1024 * 1024; // 10MB
const DEFAULT_MAX_DEPTH: usize = 10; // Maximum directory traversal depth
const FS_WATCH_CHANNEL_SIZE: usize = 256; // Buffer size for filesystem event channel
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
    pub max_capture_bytes: u64,
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

        if !(1024..=512 * 1024 * 1024).contains(&self.max_capture_bytes) {
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
    pub captured_at: chrono::DateTime<chrono::Utc>,

    /// Directories being monitored
    pub watch_paths: Vec<String>,

    /// Host where the watcher is running
    pub host: HostName,
}

#[derive(Clone)]
struct WatchContext {
    acquisition: Arc<AcquisitionManager>,
    stage_context: StageAsYouGoContext,
    max_capture_bytes: u64,
    security_policy: FileWatchingSecurityPolicy,
}

/// Unified filesystem processor using JetStream acquisition.
pub struct FilesystemProcessor {
    runtime: Option<ProcessorRuntimeState>,
    config: FilesystemConfig,
    stage_context: Option<StageAsYouGoContext>,
    watch_handles: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>,
}

impl FilesystemProcessor {
    /// Create a new filesystem processor with default configuration.
    pub fn new() -> Self {
        Self {
            runtime: None,
            config: FilesystemConfig::default(),
            stage_context: None,
            watch_handles: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Create processor with custom configuration.
    pub fn with_config(config: FilesystemConfig) -> Self {
        Self {
            runtime: None,
            config,
            stage_context: None,
            watch_handles: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Access the current processor configuration.
    pub fn config(&self) -> &FilesystemConfig {
        &self.config
    }

    async fn initialise_with_runtime_state(
        &mut self,
        runtime: ProcessorRuntimeState,
        config: FilesystemConfig,
    ) -> SatelliteResult<()> {
        let service_name = runtime.service_info().service_name().to_string();

        info!(
            processor = self.processor_name(),
            service = %service_name,
            "Initializing filesystem processor"
        );

        config.validate_config().map_err(|e| {
            SatelliteError::General(eyre::eyre!(
                "Filesystem configuration validation failed: {}",
                e
            ))
        })?;

        let publisher = match runtime.transport() {
            sinex_satellite_sdk::event_processor::EventTransport::Nats(publisher) => {
                Arc::clone(publisher)
            }
        };

        AcquisitionManager::bootstrap_streams(publisher.nats_client())
            .await
            .map_err(SatelliteError::from)?;

        let stage_context = StageAsYouGoContext::from_runtime(&runtime);

        self.config = config;
        self.stage_context = Some(stage_context);
        self.watch_handles = Arc::new(Mutex::new(Vec::new()));
        self.runtime = Some(runtime);

        Ok(())
    }

    fn runtime(&self) -> SatelliteResult<&ProcessorRuntimeState> {
        self.runtime.as_ref().ok_or_else(|| {
            SatelliteError::General(eyre::eyre!("Filesystem runtime handles not initialised"))
        })
    }

    fn service_info(&self) -> SatelliteResult<&ServiceInfo> {
        Ok(self.runtime()?.service_info())
    }

    /// Build watch contexts for each configured path.
    fn build_watch_contexts(&self) -> SatelliteResult<HashMap<String, WatchContext>> {
        let runtime = self.runtime()?;
        let stage_context = self
            .stage_context
            .clone()
            .ok_or_else(|| SatelliteError::General(eyre::eyre!("Stage context not available")))?;

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
                },
            );
        }

        Ok(contexts)
    }

    async fn spawn_watchers(&self) -> SatelliteResult<Vec<tokio::task::JoinHandle<()>>> {
        let contexts = self.build_watch_contexts()?;

        let mut handles = Vec::with_capacity(contexts.len());
        for (root, watch_ctx) in contexts {
            let root_path = root.clone();
            let watch_ctx = watch_ctx.clone();

            let handle = tokio::spawn(async move {
                if let Err(e) = watch_path(root_path, watch_ctx).await {
                    error!("Watcher terminated with error: {}", e);
                }
            });

            handles.push(handle);
        }

        Ok(handles)
    }

    async fn run_continuous_monitoring(&self) -> SatelliteResult<()> {
        info!("Filesystem watcher running (continuous mode)");
        // Wait forever while watchers stream events.
        futures::future::pending::<()>().await;
        Ok(())
    }

    /// Produce a snapshot of the current processor state.
    fn snapshot_state(&self) -> FilesystemState {
        let host = self
            .service_info()
            .map(|info| HostName::new(info.host().to_string()))
            .unwrap_or_else(|_| HostName::new("unknown-host"));

        FilesystemState {
            captured_at: chrono::Utc::now(),
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
impl StatefulStreamProcessor for FilesystemProcessor {
    type Config = FilesystemConfig;

    #[instrument(skip(self, init), fields(processor = "filesystem", service = %init.service_info().service_name()))]
    async fn initialize(
        &mut self,
        init: ProcessorInitContext<Self::Config>,
    ) -> SatelliteResult<()> {
        let (config, runtime) = init.into_runtime();
        self.initialise_with_runtime_state(runtime, config).await
    }

    async fn scan(
        &mut self,
        from: Checkpoint,
        until: TimeHorizon,
        _args: ScanArgs,
    ) -> SatelliteResult<ScanReport> {
        match until {
            TimeHorizon::Snapshot => {
                let state = self.snapshot_state();
                let report = ScanReport {
                    events_processed: 0,
                    duration: std::time::Duration::from_millis(0),
                    final_checkpoint: from,
                    time_range: None,
                    processor_stats: HashMap::new(),
                    successful_targets: vec!["snapshot".to_string()],
                    failed_targets: Vec::new(),
                    warnings: Vec::new(),
                };

                info!("Filesystem snapshot captured at {}", state.captured_at);
                Ok(report)
            }
            TimeHorizon::Historical { .. } => {
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
            TimeHorizon::Continuous => {
                let handles = self.spawn_watchers().await?;
                {
                    let mut guard = self.watch_handles.lock().await;
                    guard.extend(handles);
                }

                self.run_continuous_monitoring().await?;

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
        }
    }

    fn processor_name(&self) -> &str {
        "filesystem-watcher"
    }

    fn processor_type(&self) -> ProcessorType {
        ProcessorType::Ingestor
    }

    fn capabilities(&self) -> ProcessorCapabilities {
        ProcessorCapabilities {
            supports_snapshot: true,
            supports_historical: false,
            supports_continuous: true,
            ..ProcessorCapabilities::default()
        }
    }

    async fn current_checkpoint(&self) -> SatelliteResult<Checkpoint> {
        Ok(Checkpoint::None)
    }

    async fn estimate_scan_scope(
        &self,
        _from: &Checkpoint,
        _until: &TimeHorizon,
        _args: &ScanArgs,
    ) -> SatelliteResult<ScanEstimate> {
        Ok(ScanEstimate {
            estimated_events: (self.config.watch_paths.len() as u64) * 100,
            estimated_duration: std::time::Duration::from_secs(5),
            estimated_data_size: self.config.max_capture_bytes
                * (self.config.watch_paths.len() as u64),
            estimated_targets: self.config.watch_paths.len() as u64,
            warnings: vec!["Filesystem activity estimation derived from watcher count".to_string()],
            confidence: 0.3,
        })
    }

    async fn shutdown(&mut self) -> SatelliteResult<()> {
        let mut guard = self.watch_handles.lock().await;
        for handle in guard.drain(..) {
            handle.abort();
        }

        info!("Filesystem watcher shutdown complete");
        Ok(())
    }
}

impl ExplorationProvider for FilesystemProcessor {
    fn get_source_state(&self) -> color_eyre::eyre::Result<SourceState> {
        Ok(SourceState {
            description: "Monitors filesystem changes and publishes events".to_string(),
            last_updated: chrono::Utc::now(),
            total_items: Some(self.config.watch_paths.len() as u64),
            metadata: HashMap::from([
                (
                    "max_capture_bytes".to_string(),
                    serde_json::Value::Number(serde_json::Number::from(
                        self.config.max_capture_bytes,
                    )),
                ),
                (
                    "watch_paths".to_string(),
                    serde_json::Value::Array(
                        self.config
                            .watch_paths
                            .iter()
                            .map(|p| serde_json::Value::String(p.clone()))
                            .collect(),
                    ),
                ),
            ]),
            healthy: true,
            recent_activity: Vec::new(),
        })
    }

    fn get_ingestion_history(
        &self,
        _limit: u64,
    ) -> color_eyre::eyre::Result<Vec<IngestionHistoryEntry>> {
        Ok(Vec::new())
    }

    fn get_coverage_analysis(
        &self,
        time_range: Option<(chrono::DateTime<chrono::Utc>, chrono::DateTime<chrono::Utc>)>,
    ) -> color_eyre::eyre::Result<CoverageAnalysis> {
        let time_range = time_range.unwrap_or_else(|| {
            let now = chrono::Utc::now();
            (now - chrono::Duration::hours(1), now)
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

    fn export_data(
        &self,
        _path: &SanitizedPath,
        _format: ExportFormat,
    ) -> color_eyre::eyre::Result<()> {
        Err(eyre::eyre!(
            "Filesystem watcher does not support data export"
        ))
    }
}

async fn watch_path(root: String, ctx: WatchContext) -> SatelliteResult<()> {
    let normalized = validate_watch_path(&root, &ctx.security_policy)
        .map_err(|e| SatelliteError::General(eyre::eyre!(e)))?;

    info!("Watching path: {}", normalized.as_str());

    let (tx, mut rx) = mpsc::channel::<Event>(FS_WATCH_CHANNEL_SIZE);
    let mut watcher: RecommendedWatcher =
        notify::recommended_watcher(move |res: Result<Event, notify::Error>| {
            if let Ok(event) = res {
                let _ = tx.blocking_send(event);
            }
        })
        .map_err(|e| SatelliteError::General(eyre::eyre!("Failed to create watcher: {}", e)))?;

    watcher
        .watch(Path::new(normalized.as_str()), RecursiveMode::Recursive)
        .map_err(|e| SatelliteError::General(eyre::eyre!("Failed to watch path: {}", e)))?;

    while let Some(event) = rx.recv().await {
        if let Err(e) = handle_event(&ctx, &root, event).await {
            warn!("Failed to process filesystem event: {}", e);
        }
    }

    Ok(())
}

#[instrument(skip(ctx, event))]
async fn handle_event(ctx: &WatchContext, root: &str, event: Event) -> SatelliteResult<()> {
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
                        handle_file_modified(ctx, root, &path, "modified").await?;
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

async fn handle_file_created(ctx: &WatchContext, _root: &str, path: &Path) -> SatelliteResult<()> {
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
    if size > ctx.max_capture_bytes {
        warn!(
            "Skipping file {:?} ({} bytes) exceeding limit {} bytes",
            path, size, ctx.max_capture_bytes
        );
        return Ok(());
    }

    let content = match fs::read(path).await {
        Ok(bytes) => bytes,
        Err(e) => {
            warn!("Failed to read file {:?}: {}", path, e);
            return Ok(());
        }
    };

    let material_id = capture_material(ctx, path, MATERIAL_REASON_CREATED, Some(&content)).await?;

    let payload = sinex_core::types::events::payloads::filesystem::FileCreatedPayload {
        path: sanitize_path(path)?,
        size,
        created_at: file_created_at(&metadata),
        permissions: file_permissions(&metadata),
    };

    emit_filesystem_event(
        ctx,
        material_id,
        serde_json::to_value(payload)
            .map_err(|e| SatelliteError::General(eyre::eyre!("Failed to encode payload: {}", e)))?,
        "file.created",
        size as i64,
    )
    .await?;

    debug!("Recorded file.created for {:?}", path);
    Ok(())
}

async fn handle_file_modified(
    ctx: &WatchContext,
    _root: &str,
    path: &Path,
    modification_type: &str,
) -> SatelliteResult<()> {
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
    if size > ctx.max_capture_bytes {
        warn!(
            "Skipping file {:?} ({} bytes) exceeding limit {} bytes",
            path, size, ctx.max_capture_bytes
        );
        return Ok(());
    }

    let content = match fs::read(path).await {
        Ok(bytes) => bytes,
        Err(e) => {
            warn!("Failed to read file {:?}: {}", path, e);
            return Ok(());
        }
    };

    let material_id = capture_material(ctx, path, MATERIAL_REASON_MODIFIED, Some(&content)).await?;

    let payload = sinex_core::types::events::payloads::filesystem::FileModifiedPayload {
        path: sanitize_path(path)?,
        size,
        modified_at: file_modified_at(&metadata),
        modification_type: modification_type.to_string(),
    };

    emit_filesystem_event(
        ctx,
        material_id,
        serde_json::to_value(payload)
            .map_err(|e| SatelliteError::General(eyre::eyre!("Failed to encode payload: {}", e)))?,
        "file.modified",
        size as i64,
    )
    .await?;

    debug!("Recorded file.modified for {:?}", path);
    Ok(())
}

async fn handle_file_deleted(ctx: &WatchContext, _root: &str, path: &Path) -> SatelliteResult<()> {
    // For deletions no content is available; record zero-byte material.
    let material_id = capture_material(ctx, path, MATERIAL_REASON_DELETED, None).await?;

    let payload = sinex_core::types::events::payloads::filesystem::FileDeletedPayload {
        path: sanitize_path(path)?,
        deleted_at: chrono::Utc::now(),
    };

    emit_filesystem_event(
        ctx,
        material_id,
        serde_json::to_value(payload)
            .map_err(|e| SatelliteError::General(eyre::eyre!("Failed to encode payload: {}", e)))?,
        "file.deleted",
        0,
    )
    .await?;

    debug!("Recorded file.deleted for {:?}", path);
    Ok(())
}

async fn handle_file_moved(
    ctx: &WatchContext,
    _root: &str,
    old: &Path,
    new: &Path,
) -> SatelliteResult<()> {
    let material_id = capture_material(ctx, new, MATERIAL_REASON_MOVED, None).await?;

    let payload = sinex_core::types::events::payloads::filesystem::FileMovedPayload {
        old_path: sanitize_path(old)?,
        new_path: sanitize_path(new)?,
        moved_at: chrono::Utc::now(),
    };

    emit_filesystem_event(
        ctx,
        material_id,
        serde_json::to_value(payload)
            .map_err(|e| SatelliteError::General(eyre::eyre!("Failed to encode payload: {}", e)))?,
        "file.moved",
        0,
    )
    .await?;

    debug!("Recorded file.moved from {:?} to {:?}", old, new);
    Ok(())
}

async fn capture_material(
    ctx: &WatchContext,
    path: &Path,
    reason: &str,
    content: Option<&[u8]>,
) -> SatelliteResult<Ulid> {
    let identifier = path.to_string_lossy();
    let mut handle = ctx
        .acquisition
        .begin_material(&identifier)
        .await
        .map_err(|e| SatelliteError::General(eyre::eyre!("Failed to begin material: {}", e)))?;

    let material_id = handle.material_id;

    if let Some(bytes) = content {
        ctx.acquisition
            .append_slice(&mut handle, bytes)
            .await
            .map_err(|e| SatelliteError::General(eyre::eyre!("Failed to append slice: {}", e)))?;
    }

    ctx.acquisition
        .finalize(handle, reason)
        .await
        .map_err(|e| SatelliteError::General(eyre::eyre!("Failed to finalize material: {}", e)))?;

    Ok(material_id)
}

async fn emit_filesystem_event(
    ctx: &WatchContext,
    material_id: Ulid,
    payload: JsonValue,
    event_type: &str,
    total_bytes: i64,
) -> SatelliteResult<()> {
    let provenance = Provenance::Material {
        id: Id::from_ulid(material_id),
        anchor_byte: 0,
        offset_start: Some(0),
        offset_end: Some(total_bytes),
        offset_kind: sinex_core::OffsetKind::Byte,
    };

    let event = CoreEvent::create(
        sinex_core::types::domain::EventSource::from_static("fs-watcher"),
        sinex_core::types::domain::EventType::from(event_type),
        payload,
        provenance,
    );

    let mut event = event;
    event.id = Some(Id::from_ulid(Ulid::new()));

    ctx.stage_context
        .emit_event_with_provenance(event, material_id, Some(0), Some(total_bytes))
        .await
        .map(|_| ())
        .map_err(|e| SatelliteError::General(eyre::eyre!("Failed to emit event: {}", e)))
}

fn sanitize_path(path: &Path) -> SatelliteResult<SanitizedPath> {
    SanitizedPath::from_str_validated(&path.to_string_lossy())
        .map_err(|e| SatelliteError::General(eyre::eyre!("Path validation failed: {}", e)))
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

fn file_created_at(metadata: &StdMetadata) -> chrono::DateTime<chrono::Utc> {
    metadata
        .created()
        .or_else(|_| metadata.modified())
        .map(|ts| ts.into())
        .unwrap_or_else(|_| chrono::Utc::now())
}

fn file_modified_at(metadata: &StdMetadata) -> chrono::DateTime<chrono::Utc> {
    metadata
        .modified()
        .map(|ts| ts.into())
        .unwrap_or_else(|_| chrono::Utc::now())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinex_core::db::models::Provenance;
    use sinex_core::db::query_helpers::ulid_to_uuid;
    use sinex_core::Id;
    use sinex_satellite_sdk::{acquisition_manager::RotationPolicy, AcquisitionManager};
    use sinex_test_utils::prelude::*;
    use sinex_test_utils::{sinex_test, EphemeralNats};
    use std::sync::Arc;
    use tempfile::tempdir;
    use tokio::sync::mpsc;
    use tokio::time::{timeout, Duration};

    #[sinex_test]
    fn filesystem_config_validation_allows_basic_configuration() -> color_eyre::Result<()> {
        let mut config = FilesystemConfig::default();
        config.watch_paths = vec!["/tmp".to_string()];
        assert!(config.validate_config().is_ok());
        Ok(())
    }

    #[sinex_test]
    fn filesystem_config_validation_rejects_missing_paths() -> color_eyre::Result<()> {
        let config = FilesystemConfig {
            watch_paths: vec![],
            ..FilesystemConfig::default()
        };

        assert!(config.validate_config().is_err());
        Ok(())
    }

    #[sinex_test]
    async fn handle_file_created_emits_event(ctx: TestContext) -> color_eyre::Result<()> {
        let _guard = sinex_test_utils::acquire_pool_test_guard().await;
        ctx.ensure_clean().await?;
        sinex_test_utils::db_common::reset_database(&ctx.pool).await?;
        sinex_test_utils::db_common::verify_clean_state(&ctx.pool).await?;
        let nats = EphemeralNats::start().await?;
        let nats_client = nats.connect().await?;

        AcquisitionManager::bootstrap_streams(&nats_client).await?;

        let acquisition = Arc::new(AcquisitionManager::new(
            nats_client,
            RotationPolicy::default(),
            "filesystem".to_string(),
            "/tmp".to_string(),
        ));

        let (event_tx, mut event_rx) = mpsc::unbounded_channel();
        let stage_context =
            StageAsYouGoContext::from_sender(Arc::clone(&acquisition), event_tx, false);

        let watch_ctx = WatchContext {
            acquisition,
            stage_context,
            max_capture_bytes: 1024 * 1024,
            security_policy: FileWatchingSecurityPolicy::permissive(),
        };

        let temp_root = tempdir()?;
        let file_path = temp_root.path().join("example.txt");
        tokio::fs::write(&file_path, b"hello world").await?;

        handle_file_created(&watch_ctx, temp_root.path().to_str().unwrap(), &file_path).await?;

        let event = timeout(Duration::from_secs(10), event_rx.recv())
            .await?
            .expect("filesystem event emitted");

        assert_eq!(event.event_type.as_str(), "file.created");

        let material_ulid = match event.provenance {
            Provenance::Material { ref id, .. } => *id.as_ulid(),
            _ => panic!("expected material provenance"),
        };

        let record = ctx
            .pool
            .source_materials()
            .get_by_id(Id::from_ulid(material_ulid))
            .await?
            .expect("source material persisted");
        assert_eq!(record.status.as_str(), "completed");

        let total_bytes: Option<i64> = sqlx::query_scalar(
            "SELECT offset_end FROM raw.temporal_ledger WHERE source_material_id = $1::uuid::ulid ORDER BY ts_capture DESC LIMIT 1",
        )
        .bind(ulid_to_uuid(material_ulid))
        .fetch_optional(&ctx.pool)
        .await?;

        assert_eq!(total_bytes.unwrap_or_default(), 11);

        sinex_test_utils::db_common::reset_database(&ctx.pool).await?;
        sinex_test_utils::db_common::verify_clean_state(&ctx.pool).await?;
        ctx.force_cleanup().await?;
        Ok(())
    }
}

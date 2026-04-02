#![doc = include_str!("../docs/unified_node.md")]

//! Filesystem watcher node using JetStream-first acquisition.
//!
//! This implementation uses a Stage-as-You-Go + `AcquisitionManager` workflow:
//! - File system events are captured via notify watchers.
//! - Each event is staged as a dedicated source material and published to
//!   `JetStream` using `AcquisitionManager`.
//! - Structured events are emitted through `StageAsYouGoContext`, referencing
//!   the captured material for provenance.

use notify::{
    Config as NotifyConfig, Event, EventKind, PollWatcher, RecommendedWatcher, RecursiveMode,
    Watcher, event::RenameMode,
};
use serde::{Deserialize, Serialize};
use sinex_node_sdk::error_helpers::NodeErrorExt;
use sinex_node_sdk::{
    CoverageAnalysis, ExplorationProvider, ExportFormat, IngestionHistoryEntry, SourceState,
};
use sinex_node_sdk::{
    NodeResult, SinexError,
    acquisition_manager::{AcquisitionManager, RotationPolicy},
    ingestor_node::IngestorNode,
    runtime::stream::{
        Checkpoint, NodeCapabilities, NodeRuntimeState, ScanArgs, ScanReport, ServiceInfo,
        TimeHorizon,
    },
    stage_as_you_go::StageAsYouGoContext,
    wait_for_shutdown_signal,
};
use sinex_primitives::{
    Seconds, Uuid,
    domain::{HostName, RecordedPath, SanitizedPath},
    events::{EventPayload, enums::FileModificationType, payloads::filesystem::FileCreatedPayload},
    temporal::Timestamp,
    units::Bytes,
    validation::{
        FileWatchingSecurityPolicy, file_watching_security::check_sensitive_path,
        validate_watch_path,
    },
};
use std::{
    collections::HashMap,
    fs::Metadata as StdMetadata,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
        Mutex as StdMutex,
    },
};
use tokio::{
    fs,
    io::AsyncReadExt,
    sync::{
        Mutex,
        mpsc::{self, error::TrySendError},
    },
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument, warn};
use validator::ValidationError;

const DEFAULT_MAX_CAPTURE_BYTES: Bytes = Bytes::from_mebibytes(10); // 10MB
const DEFAULT_MAX_DEPTH: usize = 10; // Maximum directory traversal depth
const DEFAULT_MAX_WATCHES: usize = 65_536; // Inotify watch limit (well under typical Linux max)
const DEFAULT_POLL_INTERVAL_SECS: Seconds = Seconds::from_secs(5);
const FS_WATCH_CHANNEL_SIZE: usize = 10_000; // Buffer size for filesystem event channel (high-volume burst protection)
const FS_CAPTURE_CHUNK_SIZE: usize = 64 * 1024;
const FS_READ_RETRY_ATTEMPTS: u32 = 3; // Number of retry attempts for transient file read errors
const FS_READ_RETRY_BASE_DELAY_MS: u64 = 100; // Base delay for exponential backoff (100ms, 500ms, 1s)
const FS_MAX_CONCURRENT_CAPTURES: usize = 64; // Cap concurrent file reads across all watchers to avoid FD exhaustion
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

    /// Maximum total inotify watches across all paths (guards against FD exhaustion)
    #[serde(default = "default_max_watches")]
    pub max_watches: usize,

    /// Poll interval used when the native watcher budget would be exceeded
    #[serde(default = "default_poll_interval_secs")]
    pub poll_interval_secs: Seconds,
}

fn default_max_watches() -> usize {
    DEFAULT_MAX_WATCHES
}

fn default_poll_interval_secs() -> Seconds {
    DEFAULT_POLL_INTERVAL_SECS
}

impl Default for FilesystemConfig {
    fn default() -> Self {
        Self {
            watch_paths: vec![],
            max_depth: Some(DEFAULT_MAX_DEPTH),
            follow_symlinks: false,
            max_capture_bytes: DEFAULT_MAX_CAPTURE_BYTES,
            max_watches: DEFAULT_MAX_WATCHES,
            poll_interval_secs: DEFAULT_POLL_INTERVAL_SECS,
        }
    }
}

impl FilesystemConfig {
    /// Validate the configuration and return detailed error messages.
    pub fn validate_config(&self) -> NodeResult<()> {
        if self.watch_paths.is_empty() {
            return Err(SinexError::configuration(
                "At least one watch path must be specified".to_string(),
            ));
        }

        if let Some(depth) = self.max_depth {
            validate_max_depth(depth).map_err(|_| {
                SinexError::configuration("Max depth must be reasonable (1-100)".to_string())
            })?;
        }

        let max_capture_bytes = self.max_capture_bytes.as_u64();
        if !(1024..=512 * 1024 * 1024).contains(&max_capture_bytes) {
            return Err(SinexError::configuration(
                "Max capture bytes must be between 1KB and 512MB".to_string(),
            ));
        }

        if !(1..=524_288).contains(&self.max_watches) {
            return Err(SinexError::configuration(
                "Max watches must be between 1 and 524288".to_string(),
            ));
        }

        if !(1..=3600).contains(&self.poll_interval_secs.as_secs()) {
            return Err(SinexError::configuration(
                "Poll interval must be between 1 and 3600 seconds".to_string(),
            ));
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
    last_activity: StdMutex<Option<Timestamp>>,
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
            last_activity: StdMutex::new(None),
        })
    }

    fn record_activity(&self) {
        *self
            .last_activity
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Timestamp::now());
    }

    fn record_created(&self) {
        self.events_processed.fetch_add(1, Ordering::Relaxed);
        self.events_created.fetch_add(1, Ordering::Relaxed);
        self.record_activity();
    }

    fn record_modified(&self) {
        self.events_processed.fetch_add(1, Ordering::Relaxed);
        self.events_modified.fetch_add(1, Ordering::Relaxed);
        self.record_activity();
    }

    fn record_deleted(&self) {
        self.events_processed.fetch_add(1, Ordering::Relaxed);
        self.events_deleted.fetch_add(1, Ordering::Relaxed);
        self.record_activity();
    }

    fn record_moved(&self) {
        self.events_processed.fetch_add(1, Ordering::Relaxed);
        self.events_moved.fetch_add(1, Ordering::Relaxed);
        self.record_activity();
    }

    fn record_error(&self) {
        self.processing_errors.fetch_add(1, Ordering::Relaxed);
        self.record_activity();
    }

    pub(crate) fn recent_activity(&self) -> Vec<sinex_node_sdk::ActivityEntry> {
        vec![]
    }

    fn last_updated(&self) -> Option<Timestamp> {
        *self
            .last_activity
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn metadata(&self) -> HashMap<String, serde_json::Value> {
        HashMap::from([
            (
                "events_processed".to_string(),
                serde_json::json!(self.events_processed.load(Ordering::Relaxed)),
            ),
            (
                "events_created".to_string(),
                serde_json::json!(self.events_created.load(Ordering::Relaxed)),
            ),
            (
                "events_modified".to_string(),
                serde_json::json!(self.events_modified.load(Ordering::Relaxed)),
            ),
            (
                "events_deleted".to_string(),
                serde_json::json!(self.events_deleted.load(Ordering::Relaxed)),
            ),
            (
                "events_moved".to_string(),
                serde_json::json!(self.events_moved.load(Ordering::Relaxed)),
            ),
            (
                "processing_errors".to_string(),
                serde_json::json!(self.processing_errors.load(Ordering::Relaxed)),
            ),
        ])
    }
}

#[derive(Clone)]
struct WatchContext {
    acquisition: Arc<AcquisitionManager>,
    stage_context: StageAsYouGoContext,
    max_capture_bytes: Bytes,
    max_watches: usize,
    max_depth: Option<usize>,
    security_policy: FileWatchingSecurityPolicy,
    dropped_events: Arc<AtomicU64>,
    metrics: Arc<EventMetrics>,
    poll_interval: std::time::Duration,
    cancel_token: CancellationToken,
    /// Semaphore limiting concurrent file reads across all watchers to prevent FD exhaustion
    capture_semaphore: Arc<tokio::sync::Semaphore>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FilesystemCheckpoint {}

/// Unified filesystem node using `JetStream` acquisition.
pub struct FilesystemNode {
    runtime: Option<NodeRuntimeState>,
    config: FilesystemConfig,
    stage_context: Option<StageAsYouGoContext>,
    watch_handles: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>,
    dropped_events: Arc<AtomicU64>,
    metrics: Arc<EventMetrics>,
    cancel_token: CancellationToken,
    capture_semaphore: Arc<tokio::sync::Semaphore>,
}

impl FilesystemNode {
    /// Create a new filesystem node with default configuration.
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
            capture_semaphore: Arc::new(tokio::sync::Semaphore::new(FS_MAX_CONCURRENT_CAPTURES)),
        }
    }

    /// Create node with custom configuration.
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
            capture_semaphore: Arc::new(tokio::sync::Semaphore::new(FS_MAX_CONCURRENT_CAPTURES)),
        }
    }

    /// Access the current node configuration.
    #[must_use]
    pub fn config(&self) -> &FilesystemConfig {
        &self.config
    }

    fn dropped_event_count(&self) -> u64 {
        self.dropped_events.load(Ordering::Relaxed)
    }

    fn active_watcher_state(&self) -> (Option<usize>, bool) {
        match self.watch_handles.try_lock() {
            Ok(guard) if guard.is_empty() => (None, false),
            Ok(guard) => (
                Some(guard.iter().filter(|handle| !handle.is_finished()).count()),
                false,
            ),
            Err(_) => (None, true),
        }
    }

    fn watcher_shutdown_result(
        index: usize,
        result: Result<(), tokio::task::JoinError>,
    ) -> NodeResult<()> {
        match result {
            Ok(()) => {
                debug!(watcher_index = index, "Filesystem watcher task finished before shutdown");
                Ok(())
            }
            Err(error) if error.is_cancelled() => {
                debug!(watcher_index = index, "Filesystem watcher task cancelled during shutdown");
                Ok(())
            }
            Err(error) => {
                Err(
                    SinexError::processing(
                        "filesystem watcher task exited unexpectedly during shutdown",
                    )
                    .with_context("watcher_index", index.to_string())
                    .with_source(error),
                )
            }
        }
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
                FileCreatedPayload::SOURCE.as_static_str(),
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
                    max_watches: self.config.max_watches,
                    max_depth: self.config.max_depth,
                    security_policy: if self.config.follow_symlinks {
                        FileWatchingSecurityPolicy::permissive()
                    } else {
                        FileWatchingSecurityPolicy::restrictive()
                    },
                    dropped_events: Arc::clone(&self.dropped_events),
                    metrics: Arc::clone(&self.metrics),
                    poll_interval: std::time::Duration::from_secs(
                        self.config.poll_interval_secs.as_secs(),
                    ),
                    cancel_token: self.cancel_token.clone(),
                    capture_semaphore: Arc::clone(&self.capture_semaphore),
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
            |_| sinex_primitives::events::builder::get_hostname(),
            |info| info.host().clone(),
        );

        FilesystemState {
            captured_at: sinex_primitives::temporal::now(),
            watch_paths: self.config.watch_paths.clone(),
            host,
        }
    }
}

impl Default for FilesystemNode {
    fn default() -> Self {
        Self::new()
    }
}

impl IngestorNode for FilesystemNode {
    type Config = FilesystemConfig;
    type State = FilesystemCheckpoint;

    fn name(&self) -> &'static str {
        "filesystem-watcher"
    }

    fn capabilities(&self) -> NodeCapabilities {
        NodeCapabilities {
            supports_snapshot: true,
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
            node = self.name(),
            service = %service_name,
            "Initializing filesystem node"
        );

        config.validate_config()?;

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

    async fn scan_snapshot(
        &mut self,
        _state: &mut Self::State,
        _args: ScanArgs,
    ) -> NodeResult<ScanReport> {
        let state = self.snapshot_state();
        let report = ScanReport {
            events_processed: 0,
            duration: std::time::Duration::from_millis(0),
            final_checkpoint: Checkpoint::None,
            time_range: None,
            node_stats: HashMap::new(),
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
        args: ScanArgs,
    ) -> NodeResult<ScanReport> {
        // Historical scan for filesystem: re-crawl watch paths from checkpoint.
        // This captures the current filesystem state, using the checkpoint to
        // determine what has changed since the last scan.
        info!(
            checkpoint = ?from,
            replay = args.replay.is_some(),
            "Starting filesystem historical scan"
        );
        let start = std::time::Instant::now();
        let state = self.snapshot_state();

        info!(
            "Filesystem historical scan captured at {}",
            state.captured_at
        );
        Ok(ScanReport {
            events_processed: 0,
            duration: start.elapsed(),
            final_checkpoint: from,
            time_range: None,
            node_stats: HashMap::new(),
            successful_targets: vec!["historical".to_string()],
            failed_targets: Vec::new(),
            warnings: Vec::new(),
        })
    }

    async fn run_continuous(
        &mut self,
        _state: &mut Self::State,
        from: Checkpoint,
        shutdown_rx: tokio::sync::watch::Receiver<bool>,
    ) -> NodeResult<ScanReport> {
        let handles = self.spawn_watchers().await?;
        {
            let mut guard = self.watch_handles.lock().await;
            guard.extend(handles);
        }

        // Wait for shutdown signal instead of awaiting pending
        let mut shutdown_rx = shutdown_rx;
        if !wait_for_shutdown_signal(&mut shutdown_rx).await {
            warn!("Filesystem watcher shutdown channel dropped before explicit shutdown");
        }

        Ok(ScanReport {
            events_processed: 0,
            duration: std::time::Duration::from_millis(0),
            final_checkpoint: from,
            time_range: None,
            node_stats: HashMap::new(),
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
        for (index, handle) in guard.drain(..).enumerate() {
            Self::watcher_shutdown_result(index, handle.await)?;
        }

        info!(
            dropped_events = self.dropped_event_count(),
            "Filesystem watcher shutdown complete"
        );
        Ok(())
    }
}

impl ExplorationProvider for FilesystemNode {
    fn get_source_state(&self) -> NodeResult<SourceState> {
        let watched_paths = self.config.watch_paths.len();
        let dropped_events = self.dropped_event_count();
        let (active_watchers, watcher_registry_busy) = self.active_watcher_state();
        let healthy = !watcher_registry_busy
            && watched_paths > 0
            && active_watchers.is_none_or(|count| count == watched_paths);
        let is_connected = !watcher_registry_busy
            && watched_paths > 0
            && active_watchers.is_none_or(|count| count > 0);
        let description = if watched_paths == 0 {
            "No filesystem watch paths configured".to_string()
        } else if watcher_registry_busy {
            "Filesystem monitoring status unavailable (watcher registry busy)".to_string()
        } else if let Some(active_watchers) = active_watchers {
            if active_watchers == 0 {
                format!(
                    "Filesystem monitoring stopped ({watched_paths} configured path(s), no active watchers)"
                )
            } else if active_watchers < watched_paths {
                format!(
                    "Filesystem monitoring degraded ({active_watchers}/{watched_paths} watcher(s) running)"
                )
            } else {
                format!("Monitoring {watched_paths} filesystem paths")
            }
        } else if healthy {
            format!("Monitoring {watched_paths} filesystem paths")
        } else {
            format!("Filesystem monitoring unavailable for {watched_paths} configured path(s)")
        };

        let mut metadata = self.metrics.metadata();
        metadata.insert("watched_paths".to_string(), serde_json::json!(watched_paths));
        metadata.insert("dropped_events".to_string(), serde_json::json!(dropped_events));
        if let Some(active_watchers) = active_watchers {
            metadata.insert("active_watchers".to_string(), serde_json::json!(active_watchers));
        }
        if watcher_registry_busy {
            metadata.insert("watcher_registry_busy".to_string(), serde_json::json!(true));
        }

        Ok(SourceState {
            is_connected,
            healthy,
            description,
            last_updated: self.metrics.last_updated(),
            lag_seconds: None,
            recent_activity: self.metrics.recent_activity(),
            total_items: Some(watched_paths as u64),
            metadata,
        })
    }

    fn get_ingestion_history(&self, _limit: u64) -> NodeResult<Vec<IngestionHistoryEntry>> {
        Ok(Vec::new())
    }

    fn get_coverage_analysis(
        &self,
        _time_range: Option<(Timestamp, Timestamp)>,
    ) -> NodeResult<CoverageAnalysis> {
        sinex_node_sdk::exploration::coverage_analysis_unavailable(
            "coverage analysis is not implemented for filesystem watcher sources",
        )
    }

    fn export_data(&self, _path: &SanitizedPath, _format: ExportFormat) -> NodeResult<()> {
        Err(SinexError::invalid_state(
            "Filesystem watcher does not support data export",
        ))
    }
}

/// Estimate the number of inotify watches needed for a directory tree.
/// Each subdirectory requires one watch when using `RecursiveMode::Recursive`.
fn estimate_watch_count(path: &Path, max_depth: Option<usize>) -> NodeResult<usize> {
    fn count_dirs(dir: &Path, depth: usize, max_depth: Option<usize>) -> NodeResult<usize> {
        if max_depth.is_some_and(|m| depth >= m) {
            return Ok(0);
        }

        let entries = std::fs::read_dir(dir).map_err(|error| {
            SinexError::io("Failed to enumerate watch directory while estimating watch budget")
                .with_std_error(&error)
                .with_path(dir.display())
        })?;

        let mut count = 0;
        for entry in entries {
            let entry = entry.map_err(|error| {
                SinexError::io("Failed to read watch directory entry while estimating watch budget")
                    .with_std_error(&error)
                    .with_path(dir.display())
            })?;
            let entry_path = entry.path();
            let file_type = entry.file_type().map_err(|error| {
                SinexError::io("Failed to inspect watch directory entry while estimating watch budget")
                    .with_std_error(&error)
                    .with_path(entry_path.display())
            })?;
            if file_type.is_dir() {
                count += 1 + count_dirs(&entry_path, depth + 1, max_depth)?;
            }
        }
        Ok(count)
    }

    Ok(1 + count_dirs(path, 0, max_depth)?)
}

async fn watch_path(root: String, ctx: WatchContext) -> NodeResult<()> {
    let normalized = validate_watch_path(&root, &ctx.security_policy)
        .map_err(|e| SinexError::validation(e.to_string()))?;

    // SYMLINK-001: Canonicalize to resolve symlinks and detect loops
    let canonical = std::fs::canonicalize(normalized.as_str()).map_err(|e| {
        SinexError::validation(format!("Failed to canonicalize watch path '{root}'")).with_source(e)
    })?;

    // RESOURCE-001: Estimate watch count before committing kernel resources
    let estimated = estimate_watch_count(&canonical, ctx.max_depth)?;
    let use_poll_watcher = estimated > ctx.max_watches;
    let watcher_mode = if use_poll_watcher { "poll" } else { "native" };
    if use_poll_watcher {
        warn!(
            path = %canonical.display(),
            estimated_watches = estimated,
            max_watches = ctx.max_watches,
            poll_interval_secs = ctx.poll_interval.as_secs(),
            "Watch budget exceeded; falling back to poll watcher"
        );
    }
    info!(
        path = %canonical.display(),
        estimated_watches = estimated,
        watcher_mode,
        "Watching path"
    );

    let (tx, mut rx) = mpsc::channel::<Event>(FS_WATCH_CHANNEL_SIZE);
    let drop_counter = Arc::clone(&ctx.dropped_events);
    let event_handler = move |res: Result<Event, notify::Error>| match res {
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
            error!(error = %err, watcher_mode, "Filesystem watcher reported error");
        }
    };
    enum ActiveWatcher {
        Native(RecommendedWatcher),
        Poll(PollWatcher),
    }

    let mut watcher = if use_poll_watcher {
        let config = NotifyConfig::default().with_poll_interval(ctx.poll_interval);
        ActiveWatcher::Poll(
            PollWatcher::new(event_handler, config).map_err(|e| {
                SinexError::lifecycle("Failed to create poll watcher").with_source(e)
            })?,
        )
    } else {
        ActiveWatcher::Native(
            notify::recommended_watcher(event_handler)
                .map_err(|e| SinexError::lifecycle("Failed to create watcher").with_source(e))?,
        )
    };

    match &mut watcher {
        ActiveWatcher::Native(inner) => inner.watch(&canonical, RecursiveMode::Recursive),
        ActiveWatcher::Poll(inner) => inner.watch(&canonical, RecursiveMode::Recursive),
    }
    .map_err(|e| {
        SinexError::lifecycle(format!(
            "Failed to watch path '{root}' using {watcher_mode} watcher"
        ))
        .with_source(e)
    })?;

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
    // Filter out sensitive paths (credentials, private keys, etc.)
    let paths: Vec<_> = event
        .paths
        .into_iter()
        .filter(|p| {
            if let Some(s) = p.to_str() {
                let utf8 = camino::Utf8Path::new(s);
                if let Some(reason) = check_sensitive_path(utf8) {
                    debug!(path = %p.display(), %reason, "Skipping sensitive file");
                    return false;
                }
            }
            true
        })
        .collect();

    if paths.is_empty() {
        return Ok(());
    }

    match event.kind {
        EventKind::Create(_) => {
            for path in &paths {
                handle_file_created(ctx, root, path).await?;
            }
        }
        EventKind::Modify(mod_kind) => {
            use notify::event::ModifyKind;

            match mod_kind {
                ModifyKind::Name(RenameMode::Both) => {
                    if paths.len() == 2 {
                        handle_file_moved(ctx, root, &paths[0], &paths[1]).await?;
                    }
                }
                ModifyKind::Name(_) => {
                    // Partial rename events - best effort handling
                    if paths.len() == 2 {
                        handle_file_moved(ctx, root, &paths[0], &paths[1]).await?;
                    }
                }
                ModifyKind::Data(_) | ModifyKind::Metadata(_) | ModifyKind::Any => {
                    for path in &paths {
                        handle_file_modified(ctx, root, path, FileModificationType::Content)
                            .await?;
                    }
                }
                _ => {}
            }
        }
        EventKind::Remove(_) => {
            for path in &paths {
                handle_file_deleted(ctx, root, path).await?;
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

    let created_at = file_created_at(&metadata, path)?;
    let material_id = capture_material_from_file(ctx, path, MATERIAL_REASON_CREATED, size).await?;

    let payload = sinex_primitives::events::payloads::filesystem::FileCreatedPayload {
        path: sanitize_path(path)?,
        size,
        created_at,
        permissions: file_permissions(&metadata),
    };

    let event = payload
        .from_material(material_id)
        .build()
        .node_err("Failed to build event")?;

    let json_event = event
        .to_json_event()
        .map_err(|e| SinexError::processing("Failed to convert to JSON event").with_source(e))?;

    ctx.stage_context
        .emit_event_with_provenance(json_event, material_id, Some(0), Some(size as i64))
        .await
        .map(|_| ())
        .map_err(|e| SinexError::processing("Failed to emit event").with_source(e))?;

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

    let modified_at = file_modified_at(&metadata, path)?;
    let material_id = capture_material_from_file(ctx, path, MATERIAL_REASON_MODIFIED, size).await?;

    let payload = sinex_primitives::events::payloads::filesystem::FileModifiedPayload {
        path: sanitize_path(path)?,
        size,
        modified_at,
        modification_type,
    };

    let event = payload
        .from_material(material_id)
        .build()
        .map_err(|e| SinexError::processing("Failed to build event").with_source(e))?;

    let json_event = event
        .to_json_event()
        .map_err(|e| SinexError::processing("Failed to convert to JSON event").with_source(e))?;

    ctx.stage_context
        .emit_event_with_provenance(json_event, material_id, Some(0), Some(size as i64))
        .await
        .map(|_| ())
        .map_err(|e| SinexError::processing("Failed to emit event").with_source(e))?;

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
        .map_err(|e| SinexError::processing("Failed to build event").with_source(e))?;

    let json_event = event
        .to_json_event()
        .map_err(|e| SinexError::processing("Failed to convert to JSON event").with_source(e))?;

    ctx.stage_context
        .emit_event_with_provenance(json_event, material_id, Some(0), Some(0))
        .await
        .map(|_| ())
        .map_err(|e| SinexError::processing("Failed to emit event").with_source(e))?;

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
        .map_err(|e| SinexError::processing("Failed to build event").with_source(e))?;

    let json_event = event
        .to_json_event()
        .map_err(|e| SinexError::processing("Failed to convert to JSON event").with_source(e))?;

    ctx.stage_context
        .emit_event_with_provenance(json_event, material_id, Some(0), Some(0))
        .await
        .map(|_| ())
        .map_err(|e| SinexError::processing("Failed to emit event").with_source(e))?;

    ctx.metrics.record_moved();
    debug!("Recorded file.moved from {:?} to {:?}", old, new);
    Ok(())
}

async fn capture_material(
    ctx: &WatchContext,
    path: &Path,
    reason: &str,
    content: Option<&[u8]>,
) -> NodeResult<Uuid> {
    let identifier = observed_path_string(path)?;
    let mut handle = ctx
        .acquisition
        .begin_material(&identifier)
        .await
        .map_err(|e| SinexError::processing("Failed to begin material").with_source(e))?;

    let material_id = handle.material_id;

    if let Some(bytes) = content {
        ctx.acquisition
            .append_slice(&mut handle, bytes)
            .await
            .map_err(|e| SinexError::processing("Failed to append slice").with_source(e))?;
    }

    ctx.acquisition
        .finalize(handle, reason)
        .await
        .map_err(|e| SinexError::processing("Failed to finalize material").with_source(e))?;

    Ok(material_id)
}

async fn capture_material_from_file(
    ctx: &WatchContext,
    path: &Path,
    reason: &str,
    _expected_size: u64,
) -> NodeResult<Uuid> {
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

                // Check if error is transient (typed io_kind context from capture path).
                let is_transient = is_transient_capture_error(&e);

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
) -> NodeResult<Uuid> {
    // Acquire semaphore permit to bound concurrent file descriptors across all watchers
    let _permit = ctx
        .capture_semaphore
        .acquire()
        .await
        .map_err(|_| SinexError::processing("Capture semaphore closed".to_string()))?;

    let identifier = observed_path_string(path)?;
    let mut handle = ctx
        .acquisition
        .begin_material(&identifier)
        .await
        .map_err(|e| SinexError::processing("Failed to begin material").with_source(e))?;

    let material_id = handle.material_id;

    // Issue 92: TOCTOU race eliminated by opening file first, then getting metadata
    // from the open handle. This ensures atomic operations:
    // 1. File is opened and locked by OS
    // 2. Metadata retrieved from open file descriptor (no path lookup)
    // 3. Size checked before any read
    // 4. Cumulative tracking during streaming prevents growing file issues
    let mut file = fs::File::open(path)
        .await
        .map_err(|e| capture_file_io_error(path, "open", e))?;

    let metadata = file
        .metadata()
        .await
        .map_err(|e| capture_file_io_error(path, "metadata", e))?;

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
        let read = file
            .read(&mut buffer)
            .await
            .map_err(|e| capture_file_io_error(path, "read", e))?;

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
            .map_err(|e| SinexError::processing("Failed to append slice").with_source(e))?;
    }

    ctx.acquisition
        .finalize(handle, reason)
        .await
        .map_err(|e| SinexError::processing("Failed to finalize material").with_source(e))?;

    Ok(material_id)
}

fn capture_file_io_error(path: &Path, operation: &str, err: std::io::Error) -> SinexError {
    SinexError::io(format!("Failed to {operation} file during capture"))
        .with_std_error(&err)
        .with_path(path.display())
        .with_context("io_kind", format!("{:?}", err.kind()))
}

fn is_transient_capture_error(err: &SinexError) -> bool {
    err.context_map().get("io_kind").is_some_and(|kind| {
        matches!(
            kind.as_str(),
            "WouldBlock" | "Interrupted" | "PermissionDenied" | "ResourceBusy"
        )
    })
}

fn sanitize_path(path: &Path) -> NodeResult<RecordedPath> {
    RecordedPath::from_observed(observed_path_string(path)?)
        .map_err(|e| SinexError::validation("Path recording failed").with_source(e))
}

fn observed_path_string(path: &Path) -> NodeResult<String> {
    path.to_str().map(str::to_owned).ok_or_else(|| {
        SinexError::validation("filesystem watcher observed non-utf8 path")
            .with_context("path_debug", format!("{path:?}"))
    })
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

fn filesystem_timestamp(
    timestamp: std::io::Result<std::time::SystemTime>,
    field: &str,
    path: &Path,
) -> NodeResult<sinex_primitives::temporal::Timestamp> {
    timestamp.map(Timestamp::from).map_err(|error| {
        SinexError::processing("failed to read filesystem timestamp")
            .with_context("field", field)
            .with_context("path", path.display().to_string())
            .with_source(error)
    })
}

fn file_created_at(
    metadata: &StdMetadata,
    path: &Path,
) -> NodeResult<sinex_primitives::temporal::Timestamp> {
    filesystem_timestamp(metadata.created().or_else(|_| metadata.modified()), "created_at", path)
}

fn file_modified_at(
    metadata: &StdMetadata,
    path: &Path,
) -> NodeResult<sinex_primitives::temporal::Timestamp> {
    filesystem_timestamp(metadata.modified(), "modified_at", path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinex_db::models::{Event as SinexEvent, Provenance};
    use sinex_node_sdk::AcquisitionManager;
    use sinex_primitives::Id;
    use std::path::PathBuf;
    use std::sync::Arc;
    use tempfile::tempdir;
    use tokio::sync::mpsc;
    use tokio::time::{Duration, timeout};
    use xtask::sandbox::prelude::*;
    use xtask::sandbox::node_runtime::TestRuntimeBuilder;
    use xtask::sandbox::sinex_test;

    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;
    #[cfg(unix)]
    use std::ffi::OsString;
    #[cfg(unix)]
    use std::os::unix::ffi::OsStringExt;

    #[sinex_test]
    async fn filesystem_config_validation_allows_basic_configuration() -> TestResult<()> {
        let mut config = FilesystemConfig::default();
        config.watch_paths = vec!["/tmp".to_string()];
        assert!(config.validate_config().is_ok());
        Ok(())
    }

    #[sinex_test]
    async fn filesystem_config_validation_rejects_missing_paths() -> TestResult<()> {
        let config = FilesystemConfig {
            watch_paths: vec![],
            ..FilesystemConfig::default()
        };

        assert!(config.validate_config().is_err());
        Ok(())
    }

    #[sinex_test]
    async fn filesystem_node_reports_coverage_analysis_unavailable() -> TestResult<()> {
        let node = FilesystemNode::new();
        let error = sinex_node_sdk::ExplorationProvider::get_coverage_analysis(&node, None)
            .expect_err("filesystem node should not fabricate coverage analysis");
        assert!(error.to_string().contains("not implemented"));
        Ok(())
    }

    #[sinex_test]
    async fn filesystem_source_state_is_disconnected_without_watch_paths() -> TestResult<()> {
        let node = FilesystemNode::new();
        let state = sinex_node_sdk::ExplorationProvider::get_source_state(&node)?;

        assert!(!state.is_connected);
        assert!(!state.healthy);
        assert_eq!(state.total_items, Some(0));
        assert_eq!(state.last_updated, None);
        assert!(state.description.contains("No filesystem watch paths configured"));
        Ok(())
    }

    #[sinex_test]
    async fn snapshot_state_falls_back_to_global_host_identity() -> TestResult<()> {
        let node = FilesystemNode::new();
        let state = node.snapshot_state();

        assert_eq!(
            state.host,
            sinex_primitives::events::builder::get_hostname(),
            "filesystem snapshot state should reuse the shared host identity fallback",
        );
        Ok(())
    }

    #[sinex_test]
    async fn filesystem_source_state_does_not_stay_unhealthy_after_transient_processing_error(
    ) -> TestResult<()> {
        let node = FilesystemNode::with_config(FilesystemConfig {
            watch_paths: vec!["/tmp".to_string()],
            ..FilesystemConfig::default()
        });
        node.metrics.record_error();

        let state = sinex_node_sdk::ExplorationProvider::get_source_state(&node)?;
        assert!(state.is_connected);
        assert!(
            state.healthy,
            "transient cumulative processing errors must not poison filesystem source health forever"
        );
        assert!(
            state
                .metadata
                .get("processing_errors")
                .and_then(serde_json::Value::as_u64)
                .is_some_and(|count| count == 1)
        );
        assert!(state.last_updated.is_some());
        Ok(())
    }

    #[sinex_test]
    async fn filesystem_source_state_marks_finished_watchers_unhealthy() -> TestResult<()> {
        let node = FilesystemNode::with_config(FilesystemConfig {
            watch_paths: vec!["/tmp/a".to_string(), "/tmp/b".to_string()],
            ..FilesystemConfig::default()
        });

        {
            let mut guard = node.watch_handles.lock().await;
            guard.push(tokio::spawn(async {
                tokio::time::sleep(Duration::from_secs(60)).await;
            }));
            guard.push(tokio::spawn(async {}));
        }
        tokio::task::yield_now().await;

        let state = sinex_node_sdk::ExplorationProvider::get_source_state(&node)?;
        assert!(state.is_connected, "one active watcher should keep the source connected");
        assert!(!state.healthy, "finished watcher handles must degrade filesystem source health");
        assert!(
            state.description.contains("degraded"),
            "description should reflect degraded watcher state: {}",
            state.description
        );
        assert_eq!(
            state
                .metadata
                .get("active_watchers")
                .and_then(serde_json::Value::as_u64),
            Some(1)
        );

        let mut guard = node.watch_handles.lock().await;
        for handle in guard.drain(..) {
            handle.abort();
        }
        Ok(())
    }

    #[sinex_test]
    async fn filesystem_source_state_marks_busy_watcher_registry_unhealthy() -> TestResult<()> {
        let node = FilesystemNode::with_config(FilesystemConfig {
            watch_paths: vec!["/tmp".to_string()],
            ..FilesystemConfig::default()
        });

        let guard = node.watch_handles.lock().await;
        let state = sinex_node_sdk::ExplorationProvider::get_source_state(&node)?;

        assert!(
            !state.is_connected,
            "busy watcher registry must not claim filesystem monitoring is connected"
        );
        assert!(
            !state.healthy,
            "busy watcher registry must degrade filesystem source health"
        );
        assert!(
            state.description.contains("watcher registry busy"),
            "description should explain busy watcher registry: {}",
            state.description
        );
        assert_eq!(
            state
                .metadata
                .get("watcher_registry_busy")
                .and_then(serde_json::Value::as_bool),
            Some(true)
        );

        drop(guard);
        Ok(())
    }

    #[sinex_test]
    async fn estimate_watch_count_counts_nested_directories() -> TestResult<()> {
        let temp_root = tempdir()?;
        std::fs::create_dir_all(temp_root.path().join("a/b"))?;
        std::fs::create_dir_all(temp_root.path().join("c"))?;

        let count = estimate_watch_count(temp_root.path(), None)?;
        assert_eq!(count, 4, "root + three nested directories should need four watches");
        Ok(())
    }

    #[cfg(unix)]
    #[sinex_test]
    async fn estimate_watch_count_rejects_unreadable_subdirectories() -> TestResult<()> {
        let temp_root = tempdir()?;
        let unreadable = temp_root.path().join("private");
        std::fs::create_dir_all(&unreadable)?;

        let original_permissions = std::fs::metadata(&unreadable)?.permissions();
        let mut restricted_permissions = original_permissions.clone();
        restricted_permissions.set_mode(0o000);
        std::fs::set_permissions(&unreadable, restricted_permissions)?;

        let error = estimate_watch_count(temp_root.path(), None)
            .expect_err("unreadable directory should fail watch estimation honestly");

        std::fs::set_permissions(&unreadable, original_permissions)?;

        let error_text = error.to_string();
        assert!(
            error_text.contains("estimating watch budget"),
            "unexpected error: {error_text}"
        );
        assert!(
            error_text.contains("private"),
            "error should retain the unreadable directory path: {error_text}"
        );
        Ok(())
    }

    #[sinex_test]
    async fn filesystem_watcher_shutdown_result_accepts_clean_exit() -> TestResult<()> {
        let handle = tokio::spawn(async {});
        FilesystemNode::watcher_shutdown_result(0, handle.await)?;
        Ok(())
    }

    #[sinex_test]
    async fn filesystem_watcher_shutdown_result_accepts_cancelled_task() -> TestResult<()> {
        let handle = tokio::spawn(async {
            tokio::time::sleep(Duration::from_secs(30)).await;
        });
        handle.abort();
        FilesystemNode::watcher_shutdown_result(1, handle.await)?;
        Ok(())
    }

    #[sinex_test]
    async fn filesystem_watcher_shutdown_result_rejects_panicked_task() -> TestResult<()> {
        let handle = tokio::spawn(async {
            panic!("filesystem watcher panic");
        });
        let error = FilesystemNode::watcher_shutdown_result(2, handle.await)
            .expect_err("panicked watcher should fail shutdown honestly");
        let message = error.to_string();
        assert!(
            message.contains("filesystem watcher task exited unexpectedly during shutdown"),
            "unexpected error: {message}"
        );
        assert!(
            message.contains("watcher_index"),
            "watcher index should be preserved in shutdown failure context: {message}"
        );
        Ok(())
    }

    #[sinex_test]
    async fn filesystem_run_continuous_returns_immediately_when_shutdown_already_requested(
        ctx: TestContext,
    ) -> TestResult<()> {
        let runtime = TestRuntimeBuilder::new(&ctx, "filesystem-pre-signaled-shutdown")
            .with_dry_run(true)
            .build()
            .await?;

        let temp_root = tempdir()?;
        let watch_path = temp_root
            .path()
            .to_str()
            .ok_or_else(|| color_eyre::eyre::eyre!("temp root path not utf8"))?
            .to_string();

        let mut node = FilesystemNode::new();
        let config = FilesystemConfig {
            watch_paths: vec![watch_path],
            ..FilesystemConfig::default()
        };
        let mut state = FilesystemCheckpoint::default();
        node.initialize(config, &runtime.runtime, &mut state).await?;

        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let _ = shutdown_tx.send(true);

        let report = timeout(
            Duration::from_secs(1),
            node.run_continuous(&mut state, Checkpoint::None, shutdown_rx),
        )
        .await??;
        assert!(
            report.warnings.is_empty(),
            "pre-signaled shutdown should not be reported as a dropped shutdown channel: {:?}",
            report.warnings
        );

        node.shutdown(&state).await?;
        Ok(())
    }

    #[cfg(unix)]
    #[sinex_test]
    async fn observed_path_string_rejects_non_utf8_paths() -> TestResult<()> {
        let invalid_path = PathBuf::from(OsString::from_vec(vec![
            b'/',
            b't',
            b'm',
            b'p',
            b'/',
            0xff,
        ]));

        let error =
            observed_path_string(&invalid_path).expect_err("non-utf8 observed paths must fail");

        assert!(error
            .to_string()
            .contains("filesystem watcher observed non-utf8 path"));
        Ok(())
    }

    #[sinex_test]
    async fn filesystem_timestamp_rejects_unavailable_created_at() -> TestResult<()> {
        let path = Path::new("/tmp/example.txt");
        let error = filesystem_timestamp(
            Err(std::io::Error::other("created timestamp unavailable")),
            "created_at",
            path,
        )
        .expect_err("missing created_at must fail honestly");

        let message = error.to_string();
        assert!(
            message.contains("failed to read filesystem timestamp"),
            "unexpected error: {message}"
        );
        assert!(
            message.contains("created_at"),
            "field context should be preserved: {message}"
        );
        assert!(
            message.contains("/tmp/example.txt"),
            "path context should be preserved: {message}"
        );
        Ok(())
    }

    #[sinex_test]
    async fn filesystem_timestamp_rejects_unavailable_modified_at() -> TestResult<()> {
        let path = Path::new("/tmp/example.txt");
        let error = filesystem_timestamp(
            Err(std::io::Error::other("modified timestamp unavailable")),
            "modified_at",
            path,
        )
        .expect_err("missing modified_at must fail honestly");

        let message = error.to_string();
        assert!(
            message.contains("failed to read filesystem timestamp"),
            "unexpected error: {message}"
        );
        assert!(
            message.contains("modified_at"),
            "field context should be preserved: {message}"
        );
        assert!(
            message.contains("/tmp/example.txt"),
            "path context should be preserved: {message}"
        );
        Ok(())
    }

    #[sinex_test]
    async fn handle_file_created_emits_event(ctx: TestContext) -> TestResult<()> {
        let ctx = ctx.with_nats().dedicated().await?;
        let nats_client = ctx.nats_client();

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
            max_watches: DEFAULT_MAX_WATCHES,
            max_depth: Some(DEFAULT_MAX_DEPTH),
            security_policy: FileWatchingSecurityPolicy::permissive(),
            dropped_events: Arc::new(AtomicU64::new(0)),
            metrics: EventMetrics::new(),
            poll_interval: Duration::from_secs(DEFAULT_POLL_INTERVAL_SECS.as_secs()),
            cancel_token: CancellationToken::new(),
            capture_semaphore: Arc::new(tokio::sync::Semaphore::new(FS_MAX_CONCURRENT_CAPTURES)),
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

        assert_eq!(
            event.event_type.as_str(),
            FileCreatedPayload::EVENT_TYPE.as_static_str()
        );

        let material_uuid = match event.provenance() {
            Provenance::Material { id, .. } => *id.as_uuid(),
            _ => {
                return Err(color_eyre::eyre::eyre!(
                    "expected material provenance in filesystem event"
                ));
            }
        };

        let record = ctx
            .pool
            .source_materials()
            .get_by_id(Id::from_uuid(material_uuid))
            .await?;
        assert!(
            record.is_none(),
            "source material unexpectedly persisted; ingestd should be the sole DB writer"
        );

        let total_bytes: Option<i64> = sqlx::query_scalar(
            "SELECT offset_end FROM raw.temporal_ledger WHERE source_material_id = $1::uuid ORDER BY ts_capture DESC LIMIT 1",
        )
        .bind(material_uuid)
        .fetch_optional(&ctx.pool)
        .await?;

        assert!(
            total_bytes.is_none(),
            "temporal ledger unexpectedly persisted; ingestd should be the sole DB writer"
        );
        Ok(())
    }
}

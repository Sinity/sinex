#![doc = include_str!("../docs/unified_node.md")]

//! Filesystem watcher node using JetStream-first acquisition.
//!
//! This implementation uses a Stage-as-You-Go + `AcquisitionManager` workflow:
//! - File system events are captured via notify watchers.
//! - Non-empty file content is staged as dedicated source material and published
//!   to `JetStream` using `AcquisitionManager`.
//! - Metadata-only and empty-file observations are recorded in a bounded
//!   append-only observation stream to avoid one zero-byte material per event.
//! - Structured events are emitted through `StageAsYouGoContext`, referencing
//!   the captured material for provenance.

use notify::{Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher, event::RenameMode};
use serde::{Deserialize, Serialize};
use sinex_node_sdk::error_helpers::NodeErrorExt;
use sinex_node_sdk::{
    BufferedRecordMaterializer, NodeResult, SinexError,
    acquisition_manager::{
        AcquisitionManager, BufferedAppendStreamWriterConfig, RotationPolicy, SourceRecordAnchor,
    },
    ingestor_node::IngestorNode,
    runtime::stream::{
        Checkpoint, ContinuousStart, MaterialReplayContext, NodeCapabilities, NodeRuntimeState,
        ResolvedReplayMaterial, ScanArgs, ScanReport, ServiceInfo, TimeHorizon,
    },
    stage_as_you_go::StageAsYouGoContext,
    wait_for_shutdown_signal,
};
use sinex_node_sdk::{
    CoverageAnalysis, ExplorationProvider, ExportFormat, IngestionHistoryEntry, SourceState,
};
use sinex_primitives::{
    Seconds, Uuid,
    domain::{HostName, RecordedPath, SanitizedPath},
    events::{
        EventPayload,
        enums::FileModificationType,
        payloads::filesystem::{
            FileCreatedPayload, FileDeletedPayload, FileModifiedPayload, FileMovedPayload,
        },
    },
    privacy::{self, ProcessingContext},
    temporal::Timestamp,
    units::Bytes,
    validation::{
        FileWatchingSecurityPolicy, file_watching_security::check_sensitive_path,
        validate_watch_path,
    },
};
use std::{
    collections::{HashMap, HashSet},
    fs::Metadata as StdMetadata,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicU64, Ordering},
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
const DEFAULT_MAX_WATCHES: usize = 524_288; // Align with the documented/recommended Linux inotify limit
const DEFAULT_POLL_INTERVAL_SECS: Seconds = Seconds::from_secs(5);
const FS_WATCH_CHANNEL_SIZE: usize = 10_000; // Buffer size for filesystem event channel (high-volume burst protection)
const FS_CAPTURE_CHUNK_SIZE: usize = 64 * 1024;
const FS_READ_RETRY_ATTEMPTS: u32 = 3; // Number of retry attempts for transient file read errors
const FS_READ_RETRY_BASE_DELAY_MS: u64 = 100; // Base delay for exponential backoff (100ms, 500ms, 1s)
const FS_MAX_CONCURRENT_CAPTURES: usize = 64; // Cap concurrent file reads across all watchers to avoid FD exhaustion
const FS_OVERSIZED_LOG_BUCKET_BYTES: u64 = 1024 * 1024; // Re-log oversized files only after 1 MiB growth
const FS_OBSERVATION_BATCH_MAX_RECORDS: usize = 64;
const FS_OBSERVATION_BATCH_MAX_BYTES: usize = 128 * 1024;
const FS_OBSERVATION_BATCH_COALESCE_WINDOW: std::time::Duration =
    std::time::Duration::from_millis(20);
const FS_OBSERVATION_WRITER_CHANNEL_CAPACITY: usize = 256;
const DEFAULT_IGNORED_DIRECTORY_NAMES: &[&str] = &[".git", ".direnv", "node_modules", "target"];
const INOTIFY_MAX_USER_WATCHES_PATH: &str = "/proc/sys/fs/inotify/max_user_watches";
const MATERIAL_REASON_CREATED: &str = "fs-watcher:file-created";
const MATERIAL_REASON_MODIFIED: &str = "fs-watcher:file-modified";
const MATERIAL_REASON_DELETED: &str = "fs-watcher:file-deleted";
const MATERIAL_REASON_MOVED: &str = "fs-watcher:file-moved";

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct WatchTreeSurvey {
    accessible_watch_count: usize,
    filtered_watch_count: usize,
    unreadable_directories: usize,
    ignored_directories: usize,
    filtered_targets: Vec<PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum WatchStrategy {
    NativeRecursive,
    NativeFiltered { filtered_targets: Vec<PathBuf> },
}

impl WatchStrategy {
    fn mode_name(&self) -> &'static str {
        match self {
            Self::NativeRecursive => "native-recursive",
            Self::NativeFiltered { .. } => "native-filtered",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct WatchBudget {
    configured_max_watches: usize,
    effective_max_watches: usize,
    kernel_max_watches: Option<usize>,
}

impl WatchBudget {
    fn detect(configured_max_watches: usize) -> Self {
        let kernel_max_watches = read_kernel_inotify_watch_limit();
        let effective_max_watches = kernel_max_watches.map_or(configured_max_watches, |limit| {
            limit.min(configured_max_watches)
        });

        Self {
            configured_max_watches,
            effective_max_watches,
            kernel_max_watches,
        }
    }
}

enum ActiveWatcher {
    NativeRecursive {
        _watcher: RecommendedWatcher,
    },
    NativeFiltered {
        watcher: RecommendedWatcher,
        watched_targets: HashSet<PathBuf>,
    },
}

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

    /// Poll interval retained for backwards config compatibility; poll fallback is no longer automatic
    #[serde(default = "default_poll_interval_secs")]
    pub poll_interval_secs: Seconds,

    /// Directory names that should be excluded from recursive watch planning and historical scans
    #[serde(default = "default_ignored_directory_names")]
    pub ignored_directory_names: Vec<String>,
}

fn default_max_watches() -> usize {
    DEFAULT_MAX_WATCHES
}

fn default_poll_interval_secs() -> Seconds {
    DEFAULT_POLL_INTERVAL_SECS
}

fn default_ignored_directory_names() -> Vec<String> {
    DEFAULT_IGNORED_DIRECTORY_NAMES
        .iter()
        .map(|name| (*name).to_string())
        .collect()
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
            ignored_directory_names: default_ignored_directory_names(),
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

        if self
            .ignored_directory_names
            .iter()
            .any(|name| name.is_empty() || name.contains(std::path::MAIN_SEPARATOR))
        {
            return Err(SinexError::configuration(
                "Ignored directory names must be non-empty path component names".to_string(),
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
    observation_materializer: BufferedRecordMaterializer,
    observation_source_identifier: Arc<str>,
    stage_context: StageAsYouGoContext,
    max_capture_bytes: Bytes,
    max_watches: usize,
    max_depth: Option<usize>,
    follow_symlinks: bool,
    security_policy: FileWatchingSecurityPolicy,
    dropped_events: Arc<AtomicU64>,
    metrics: Arc<EventMetrics>,
    ignored_directory_names: Arc<HashSet<String>>,
    oversized_skip_log_buckets: Arc<StdMutex<HashMap<PathBuf, u64>>>,
    cancel_token: CancellationToken,
    /// Semaphore limiting concurrent file reads across all watchers to prevent FD exhaustion
    capture_semaphore: Arc<tokio::sync::Semaphore>,
}

#[derive(Debug, Serialize)]
struct FilesystemObservationRecord {
    source_identifier: String,
    reason: String,
    event_type: String,
    path: String,
    old_path: Option<String>,
    new_path: Option<String>,
    size: Option<u64>,
    observed_at: Timestamp,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FilesystemCheckpoint {}

/// Unified filesystem node using `JetStream` acquisition.
pub struct FilesystemNode {
    runtime: Option<NodeRuntimeState>,
    config: FilesystemConfig,
    stage_context: Option<StageAsYouGoContext>,
    watch_handles: Arc<Mutex<Vec<tokio::task::JoinHandle<NodeResult<()>>>>>,
    dropped_events: Arc<AtomicU64>,
    metrics: Arc<EventMetrics>,
    cancel_token: CancellationToken,
    capture_semaphore: Arc<tokio::sync::Semaphore>,
    oversized_skip_log_buckets: Arc<StdMutex<HashMap<PathBuf, u64>>>,
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
            oversized_skip_log_buckets: Arc::new(StdMutex::new(HashMap::new())),
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
            oversized_skip_log_buckets: Arc::new(StdMutex::new(HashMap::new())),
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
        result: Result<NodeResult<()>, tokio::task::JoinError>,
    ) -> NodeResult<()> {
        match result {
            Ok(Ok(())) => {
                debug!(
                    watcher_index = index,
                    "Filesystem watcher task finished before shutdown"
                );
                Ok(())
            }
            Ok(Err(error)) => Err(error.with_context("watcher_index", index.to_string())),
            Err(error) if error.is_cancelled() => {
                debug!(
                    watcher_index = index,
                    "Filesystem watcher task cancelled during shutdown"
                );
                Ok(())
            }
            Err(error) => Err(SinexError::processing(
                "filesystem watcher task exited unexpectedly during shutdown",
            )
            .with_context("watcher_index", index.to_string())
            .with_source(error)),
        }
    }

    async fn join_finished_watchers(&self) -> NodeResult<()> {
        let finished = {
            let mut guard = self.watch_handles.lock().await;
            let mut pending = Vec::with_capacity(guard.len());
            let mut finished = Vec::new();

            for handle in guard.drain(..) {
                if handle.is_finished() {
                    finished.push(handle);
                } else {
                    pending.push(handle);
                }
            }

            *guard = pending;
            finished
        };

        for (index, handle) in finished.into_iter().enumerate() {
            Self::watcher_shutdown_result(index, handle.await)?;
        }

        Ok(())
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
            )?);
            let stage_with_acquisition = stage_context
                .clone()
                .with_acquisition_manager(Arc::clone(&acquisition));
            let observation_source_identifier =
                Arc::<str>::from(format!("filesystem.observations:{path}"));
            let observation_materializer = BufferedRecordMaterializer::buffered(
                Arc::clone(&acquisition),
                observation_source_identifier.to_string(),
                BufferedAppendStreamWriterConfig {
                    channel_capacity: FS_OBSERVATION_WRITER_CHANNEL_CAPACITY,
                    batch_max_records: FS_OBSERVATION_BATCH_MAX_RECORDS,
                    batch_max_bytes: FS_OBSERVATION_BATCH_MAX_BYTES,
                    batch_coalesce_window: FS_OBSERVATION_BATCH_COALESCE_WINDOW,
                },
            );

            contexts.insert(
                path.clone(),
                WatchContext {
                    acquisition,
                    observation_materializer,
                    observation_source_identifier,
                    stage_context: stage_with_acquisition,
                    max_capture_bytes: self.config.max_capture_bytes,
                    max_watches: self.config.max_watches,
                    max_depth: self.config.max_depth,
                    follow_symlinks: self.config.follow_symlinks,
                    security_policy: if self.config.follow_symlinks {
                        FileWatchingSecurityPolicy::permissive()
                    } else {
                        FileWatchingSecurityPolicy::restrictive()
                    },
                    dropped_events: Arc::clone(&self.dropped_events),
                    metrics: Arc::clone(&self.metrics),
                    ignored_directory_names: Arc::new(
                        self.config
                            .ignored_directory_names
                            .iter()
                            .cloned()
                            .collect(),
                    ),
                    oversized_skip_log_buckets: Arc::clone(&self.oversized_skip_log_buckets),
                    cancel_token: self.cancel_token.clone(),
                    capture_semaphore: Arc::clone(&self.capture_semaphore),
                },
            );
        }

        Ok(contexts)
    }

    fn spawn_watchers(&self) -> NodeResult<Vec<tokio::task::JoinHandle<NodeResult<()>>>> {
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
                            debug!("Watcher for {} terminated normally", root_path);
                            return Ok(());
                        }
                        Err(error) => {
                            attempt += 1;
                            if attempt >= MAX_INIT_ATTEMPTS {
                                error!(
                                    path = %root_path,
                                    attempts = attempt,
                                    "Failed to initialize watcher after {} attempts: {}",
                                    MAX_INIT_ATTEMPTS, error
                                );
                                return Err(error.with_context("watch_root", root_path.clone()));
                            }

                            let delay_ms =
                                INIT_RETRY_BASE_DELAY_MS * (1u64 << (attempt - 1)).min(16);
                            warn!(
                                path = %root_path,
                                attempt = attempt,
                                delay_ms = delay_ms,
                                "Watcher initialization failed, retrying: {}",
                                error
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

fn checkpoint_timestamp(checkpoint: &Checkpoint) -> Option<Timestamp> {
    match checkpoint {
        Checkpoint::Timestamp { timestamp, .. } => Some(*timestamp),
        _ => None,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HistoricalEmissionKind {
    Created,
    Modified,
}

impl HistoricalEmissionKind {
    fn from_scan_args(args: &ScanArgs) -> Option<Self> {
        let Some(event_types) = args
            .replay
            .as_ref()
            .and_then(|replay| replay.replay_scope.event_types.as_ref())
        else {
            return Some(Self::Created);
        };

        if event_types
            .iter()
            .any(|event_type| event_type == FileCreatedPayload::EVENT_TYPE.as_static_str())
        {
            return Some(Self::Created);
        }

        if event_types
            .iter()
            .any(|event_type| event_type == FileModifiedPayload::EVENT_TYPE.as_static_str())
        {
            return Some(Self::Modified);
        }

        None
    }

    async fn emit(self, ctx: &WatchContext, root: &str, path: &Path) -> NodeResult<()> {
        match self {
            Self::Created => handle_file_created(ctx, root, path).await,
            Self::Modified => {
                handle_file_modified(ctx, root, path, FileModificationType::Content).await
            }
        }
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
        info!(
            checkpoint = ?from,
            replay = args.replay.is_some(),
            "Starting filesystem historical scan"
        );
        let start = std::time::Instant::now();
        let contexts = self.build_watch_contexts()?;
        let Some(emission_kind) = HistoricalEmissionKind::from_scan_args(&args) else {
            return Ok(ScanReport {
                events_processed: 0,
                duration: start.elapsed(),
                final_checkpoint: Checkpoint::timestamp(Timestamp::now(), None),
                time_range: checkpoint_timestamp(&from).map(|started_at| (started_at, Timestamp::now())),
                node_stats: HashMap::new(),
                successful_targets: Vec::new(),
                failed_targets: Vec::new(),
                warnings: vec![
                    "filesystem historical replay only re-emits current file state for file.created or file.modified scopes".to_string(),
                ],
            });
        };

        let targets = historical_scan_targets(&self.config, &args.replay);
        let mut events_processed = 0u64;
        let mut successful_targets = Vec::new();
        let mut failed_targets = Vec::new();
        let mut warnings = Vec::new();

        for target in targets {
            let target_path = PathBuf::from(&target);
            let Some((root, ctx)) = watch_context_for_target(&contexts, &target_path) else {
                failed_targets.push((
                    target.clone(),
                    "filesystem historical scan could not map replay target to a configured watch root"
                        .to_string(),
                ));
                continue;
            };

            let files = collect_historical_files(
                &target_path,
                Path::new(root),
                0,
                ctx.max_depth,
                ctx.follow_symlinks,
                &ctx.ignored_directory_names,
                &mut warnings,
            )?;

            if files.is_empty() {
                warnings.push(format!(
                    "filesystem historical scan found no replayable files under {}",
                    target_path.display()
                ));
                continue;
            }

            let mut processed_for_target = 0u64;
            for file in files {
                if args.max_events > 0 && events_processed >= args.max_events {
                    warnings.push(format!(
                        "filesystem historical scan reached max_events={} before finishing {}",
                        args.max_events, target
                    ));
                    break;
                }

                emission_kind.emit(ctx, root, &file).await?;
                processed_for_target = processed_for_target.saturating_add(1);
                events_processed = events_processed.saturating_add(1);
            }

            if processed_for_target > 0 {
                successful_targets.push(target);
            } else {
                failed_targets.push((
                    target,
                    "filesystem historical scan emitted no events for target".to_string(),
                ));
            }
        }

        info!(
            events_processed,
            successful_targets = successful_targets.len(),
            failed_targets = failed_targets.len(),
            "Filesystem historical scan finished"
        );
        Ok(ScanReport {
            events_processed,
            duration: start.elapsed(),
            final_checkpoint: Checkpoint::timestamp(Timestamp::now(), None),
            time_range: checkpoint_timestamp(&from)
                .map(|started_at| (started_at, Timestamp::now())),
            node_stats: HashMap::new(),
            successful_targets,
            failed_targets,
            warnings,
        })
    }

    async fn run_continuous(
        &mut self,
        _state: &mut Self::State,
        start: ContinuousStart,
        shutdown_rx: tokio::sync::watch::Receiver<bool>,
    ) -> NodeResult<ScanReport> {
        let handles = self.spawn_watchers()?;
        {
            let mut guard = self.watch_handles.lock().await;
            guard.extend(handles);
        }

        let mut shutdown_rx = shutdown_rx;
        let mut watcher_health_check = tokio::time::interval(std::time::Duration::from_secs(1));

        loop {
            tokio::select! {
                _ = watcher_health_check.tick() => {
                    self.join_finished_watchers().await?;
                }
                signaled = wait_for_shutdown_signal(&mut shutdown_rx) => {
                    if !signaled {
                        warn!("Filesystem watcher shutdown channel dropped before explicit shutdown");
                    }
                    break;
                }
            }
        }

        Ok(ScanReport {
            events_processed: 0,
            duration: std::time::Duration::from_millis(0),
            final_checkpoint: start.checkpoint().clone(),
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
        metadata.insert(
            "watched_paths".to_string(),
            serde_json::json!(watched_paths),
        );
        metadata.insert(
            "dropped_events".to_string(),
            serde_json::json!(dropped_events),
        );
        if let Some(active_watchers) = active_watchers {
            metadata.insert(
                "active_watchers".to_string(),
                serde_json::json!(active_watchers),
            );
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

fn path_component_is_ignored(path: &Path, ignored_directory_names: &HashSet<String>) -> bool {
    path.file_name()
        .and_then(std::ffi::OsStr::to_str)
        .is_some_and(|name| ignored_directory_names.contains(name))
}

fn path_contains_ignored_descendant(
    root: &Path,
    path: &Path,
    ignored_directory_names: &HashSet<String>,
) -> bool {
    path.strip_prefix(root).ok().is_some_and(|relative| {
        relative.components().any(|component| {
            matches!(
                component,
                std::path::Component::Normal(name)
                    if name
                        .to_str()
                        .is_some_and(|name| ignored_directory_names.contains(name))
            )
        })
    })
}

fn relative_depth(root: &Path, path: &Path) -> Option<usize> {
    path.strip_prefix(root).ok().map(|relative| {
        relative
            .components()
            .filter(|component| matches!(component, std::path::Component::Normal(_)))
            .count()
    })
}

fn metadata_is_directory(
    metadata: &StdMetadata,
    follow_symlinks: bool,
    path: &Path,
) -> NodeResult<bool> {
    if metadata.is_dir() {
        return Ok(true);
    }

    if follow_symlinks && metadata.file_type().is_symlink() {
        return std::fs::metadata(path)
            .map(|resolved| resolved.is_dir())
            .map_err(|error| {
                SinexError::io("Failed to follow filesystem watch symlink")
                    .with_std_error(&error)
                    .with_path(path.display())
            });
    }

    Ok(false)
}

/// Survey a watch target once at startup so the node can choose a bounded native strategy.
fn survey_watch_tree(
    path: &Path,
    depth: usize,
    max_depth: Option<usize>,
    follow_symlinks: bool,
    ignored_directory_names: &HashSet<String>,
) -> NodeResult<WatchTreeSurvey> {
    fn is_permission_denied(error: &std::io::Error) -> bool {
        error.kind() == std::io::ErrorKind::PermissionDenied
    }

    fn inspect_path(
        path: &Path,
        depth: usize,
        max_depth: Option<usize>,
        follow_symlinks: bool,
        ignored_directory_names: &HashSet<String>,
        visited: &mut HashSet<(u64, u64)>,
    ) -> NodeResult<WatchTreeSurvey> {
        use std::os::unix::fs::MetadataExt;

        let metadata = match std::fs::symlink_metadata(path) {
            Ok(metadata) => metadata,
            Err(error) if depth > 0 && is_permission_denied(&error) => {
                warn!(
                    path = %path.display(),
                    "Skipping unreadable directory while surveying watch strategy"
                );
                return Ok(WatchTreeSurvey {
                    accessible_watch_count: 1,
                    unreadable_directories: 1,
                    ..WatchTreeSurvey::default()
                });
            }
            Err(error) => {
                return Err(SinexError::io(
                    "Failed to inspect watch target while surveying watch strategy",
                )
                .with_std_error(&error)
                .with_path(path.display()));
            }
        };

        if !metadata_is_directory(&metadata, follow_symlinks, path)? {
            return Ok(WatchTreeSurvey {
                accessible_watch_count: 1,
                filtered_watch_count: 1,
                filtered_targets: vec![path.to_path_buf()],
                ..WatchTreeSurvey::default()
            });
        }

        // Resolve the inode to detect symlink cycles. For symlinks pointing at
        // directories, follow to the real inode so both the link and the target
        // share the same (dev, ino) key.
        let resolved_meta = if metadata.file_type().is_symlink() {
            std::fs::metadata(path).unwrap_or(metadata)
        } else {
            metadata
        };
        let inode_key = (resolved_meta.dev(), resolved_meta.ino());
        if !visited.insert(inode_key) {
            warn!(
                path = %path.display(),
                "Symlink cycle detected while surveying watch strategy; skipping"
            );
            return Ok(WatchTreeSurvey::default());
        }

        let mut survey = WatchTreeSurvey {
            accessible_watch_count: 1,
            filtered_watch_count: 1,
            filtered_targets: vec![path.to_path_buf()],
            ..WatchTreeSurvey::default()
        };

        if max_depth.is_some_and(|m| depth >= m) {
            return Ok(survey);
        }

        let entries = match std::fs::read_dir(path) {
            Ok(entries) => entries,
            Err(error) if depth > 0 && is_permission_denied(&error) => {
                warn!(
                    path = %path.display(),
                    "Skipping unreadable directory while surveying watch strategy"
                );
                return Ok(WatchTreeSurvey {
                    accessible_watch_count: 1,
                    unreadable_directories: 1,
                    ..WatchTreeSurvey::default()
                });
            }
            Err(error) => {
                return Err(SinexError::io(
                    "Failed to enumerate watch directory while surveying watch strategy",
                )
                .with_std_error(&error)
                .with_path(path.display()));
            }
        };

        for entry in entries {
            let entry = match entry {
                Ok(entry) => entry,
                Err(error) if depth > 0 && is_permission_denied(&error) => {
                    warn!(
                        path = %path.display(),
                        "Skipping unreadable directory entry while surveying watch strategy"
                    );
                    continue;
                }
                Err(error) => {
                    return Err(SinexError::io(
                        "Failed to read watch directory entry while surveying watch strategy",
                    )
                    .with_std_error(&error)
                    .with_path(path.display()));
                }
            };
            let entry_path = entry.path();
            let metadata = match std::fs::symlink_metadata(&entry_path) {
                Ok(metadata) => metadata,
                Err(error) if depth > 0 && is_permission_denied(&error) => {
                    warn!(
                        path = %entry_path.display(),
                        "Skipping unreadable watch directory entry while surveying watch strategy"
                    );
                    continue;
                }
                Err(error) => {
                    return Err(SinexError::io(
                        "Failed to inspect watch directory entry while surveying watch strategy",
                    )
                    .with_std_error(&error)
                    .with_path(entry_path.display()));
                }
            };

            if metadata_is_directory(&metadata, follow_symlinks, &entry_path)? {
                if path_component_is_ignored(&entry_path, ignored_directory_names) {
                    survey.accessible_watch_count += 1;
                    survey.ignored_directories += 1;
                    continue;
                }
                let child_survey = inspect_path(
                    &entry_path,
                    depth + 1,
                    max_depth,
                    follow_symlinks,
                    ignored_directory_names,
                    visited,
                )?;
                survey.accessible_watch_count += child_survey.accessible_watch_count;
                survey.filtered_watch_count += child_survey.filtered_watch_count;
                survey.unreadable_directories += child_survey.unreadable_directories;
                survey.ignored_directories += child_survey.ignored_directories;
                survey
                    .filtered_targets
                    .extend(child_survey.filtered_targets);
            }
        }

        Ok(survey)
    }

    let mut visited: HashSet<(u64, u64)> = HashSet::new();
    inspect_path(
        path,
        depth,
        max_depth,
        follow_symlinks,
        ignored_directory_names,
        &mut visited,
    )
}

fn choose_watch_strategy(
    survey: &WatchTreeSurvey,
    budget: WatchBudget,
) -> NodeResult<WatchStrategy> {
    let needs_filtered_plan = survey.accessible_watch_count > budget.effective_max_watches
        || survey.unreadable_directories > 0
        || survey.ignored_directories > 0;

    if !needs_filtered_plan {
        return Ok(WatchStrategy::NativeRecursive);
    }

    if survey.filtered_watch_count <= budget.effective_max_watches {
        return Ok(WatchStrategy::NativeFiltered {
            filtered_targets: survey.filtered_targets.clone(),
        });
    }

    let mut error = SinexError::configuration(
        "Filesystem watch budget exceeded even after applying filtered native watch planning",
    )
    .with_context(
        "configured_max_watches",
        budget.configured_max_watches.to_string(),
    )
    .with_context(
        "effective_max_watches",
        budget.effective_max_watches.to_string(),
    )
    .with_context(
        "accessible_watch_count",
        survey.accessible_watch_count.to_string(),
    )
    .with_context(
        "filtered_watch_count",
        survey.filtered_watch_count.to_string(),
    )
    .with_context(
        "unreadable_directories",
        survey.unreadable_directories.to_string(),
    )
    .with_context(
        "ignored_directories",
        survey.ignored_directories.to_string(),
    );

    if let Some(kernel_max_watches) = budget.kernel_max_watches {
        error = error.with_context("kernel_max_user_watches", kernel_max_watches.to_string());
    }

    Err(error)
}

fn notify_error_is_skippable_filtered_target(error: &notify::Error) -> bool {
    match &error.kind {
        notify::ErrorKind::PathNotFound => true,
        notify::ErrorKind::Io(error) => matches!(
            error.kind(),
            std::io::ErrorKind::NotFound | std::io::ErrorKind::PermissionDenied
        ),
        _ => false,
    }
}

fn add_filtered_watch_targets(
    watcher: &mut RecommendedWatcher,
    watched_targets: &mut HashSet<PathBuf>,
    targets: impl IntoIterator<Item = PathBuf>,
) -> NodeResult<()> {
    for target in targets {
        if watched_targets.insert(target.clone()) {
            if let Err(error) = watcher.watch(&target, RecursiveMode::NonRecursive) {
                if notify_error_is_skippable_filtered_target(&error) {
                    watched_targets.remove(&target);
                    warn!(
                        path = %target.display(),
                        error = %error,
                        "Skipping filtered native watcher target that became unavailable after survey"
                    );
                    continue;
                }

                return Err(SinexError::lifecycle(
                    "Failed to register filtered native watcher target",
                )
                .with_source(error)
                .with_path(target.display()));
            }
        }
    }

    Ok(())
}

fn remove_filtered_watch_targets(
    watcher: &mut RecommendedWatcher,
    watched_targets: &mut HashSet<PathBuf>,
    path: &Path,
) -> NodeResult<()> {
    let to_remove: Vec<_> = watched_targets
        .iter()
        .filter(|watched| watched.starts_with(path))
        .cloned()
        .collect();

    for target in to_remove {
        watcher.unwatch(&target).map_err(|error| {
            SinexError::lifecycle("Failed to remove filtered native watcher target")
                .with_source(error)
                .with_path(target.display())
        })?;
        watched_targets.remove(&target);
    }

    Ok(())
}

fn add_filtered_subtree_watch(
    watcher: &mut RecommendedWatcher,
    watched_targets: &mut HashSet<PathBuf>,
    root: &Path,
    path: &Path,
    ctx: &WatchContext,
) -> NodeResult<()> {
    if path_contains_ignored_descendant(root, path, &ctx.ignored_directory_names) {
        return Ok(());
    }

    let Some(depth) = relative_depth(root, path) else {
        return Ok(());
    };

    if ctx.max_depth.is_some_and(|limit| depth > limit) {
        return Ok(());
    }

    let survey = survey_watch_tree(
        path,
        depth,
        ctx.max_depth,
        ctx.follow_symlinks,
        &ctx.ignored_directory_names,
    )?;
    add_filtered_watch_targets(watcher, watched_targets, survey.filtered_targets)
}

fn reconcile_filtered_watch_targets(
    watcher: &mut RecommendedWatcher,
    watched_targets: &mut HashSet<PathBuf>,
    root: &Path,
    event: &Event,
    ctx: &WatchContext,
) -> NodeResult<()> {
    match &event.kind {
        EventKind::Create(_) => {
            for path in &event.paths {
                if let Ok(metadata) = std::fs::symlink_metadata(path)
                    && metadata_is_directory(&metadata, ctx.follow_symlinks, path)?
                {
                    add_filtered_subtree_watch(watcher, watched_targets, root, path, ctx)?;
                }
            }
        }
        EventKind::Modify(notify::event::ModifyKind::Name(_)) => {
            if event.paths.len() == 2 {
                remove_filtered_watch_targets(watcher, watched_targets, &event.paths[0])?;
                if let Ok(metadata) = std::fs::symlink_metadata(&event.paths[1])
                    && metadata_is_directory(&metadata, ctx.follow_symlinks, &event.paths[1])?
                {
                    add_filtered_subtree_watch(
                        watcher,
                        watched_targets,
                        root,
                        &event.paths[1],
                        ctx,
                    )?;
                }
            }
        }
        EventKind::Remove(_) => {
            for path in &event.paths {
                remove_filtered_watch_targets(watcher, watched_targets, path)?;
            }
        }
        _ => {}
    }

    Ok(())
}

async fn watch_path(root: String, ctx: WatchContext) -> NodeResult<()> {
    let (canonical, canonical_root, survey, watch_strategy, budget) =
        prepare_watch_root(&root, &ctx)?;
    let watcher_mode = watch_strategy.mode_name();
    log_watch_strategy(&canonical, &survey, budget, watcher_mode);

    let (tx, mut rx) = mpsc::channel::<Event>(FS_WATCH_CHANNEL_SIZE);
    let drop_counter = Arc::clone(&ctx.dropped_events);
    let error_counter = Arc::new(AtomicU64::new(0));
    let event_handler = {
        let tx = tx.clone();
        let drop_counter = Arc::clone(&drop_counter);
        let error_counter = Arc::clone(&error_counter);
        move |result: Result<Event, notify::Error>| {
            handle_watcher_callback(result, &tx, &drop_counter, &error_counter, watcher_mode);
        }
    };
    let mut watcher = build_active_watcher(
        &canonical,
        &root,
        watcher_mode,
        watch_strategy,
        event_handler,
    )?;

    loop {
        tokio::select! {
            event = rx.recv() => {
                match event {
                    Some(event) => {
                        if let ActiveWatcher::NativeFiltered { watcher, watched_targets } = &mut watcher {
                            reconcile_filtered_watch_targets(
                                watcher,
                                watched_targets,
                                &canonical,
                                &event,
                                &ctx,
                            )?;
                        }
                        if let Err(e) = handle_event(&ctx, &canonical_root, event).await {
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

        // Keep the watcher live across the select! await points. Dropping the watcher
        // tears down the kernel watch registrations even if the task itself remains alive.
        match &watcher {
            ActiveWatcher::NativeRecursive { .. } | ActiveWatcher::NativeFiltered { .. } => {}
        }
    }

    ctx.observation_materializer
        .finalize("filesystem watcher shutdown")
        .await?;
    Ok(())
}

#[instrument(skip(ctx, event))]
async fn handle_event(ctx: &WatchContext, root: &str, event: Event) -> NodeResult<()> {
    let root_path = Path::new(root);

    // Filter out sensitive paths (credentials, private keys, etc.)
    let paths: Vec<_> = event
        .paths
        .into_iter()
        .filter(|p| {
            if path_contains_ignored_descendant(root_path, p, &ctx.ignored_directory_names) {
                debug!(path = %p.display(), "Skipping ignored filesystem path");
                return false;
            }
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
        warn_oversized_skip(ctx, path, size);
        return Ok(());
    }
    clear_oversized_skip_tracking(ctx, path);

    let material_anchor = if size == 0 {
        capture_observation_record(
            ctx,
            MATERIAL_REASON_CREATED,
            FileCreatedPayload::EVENT_TYPE.as_static_str(),
            path,
            None,
            None,
            Some(size),
        )
        .await?
    } else {
        let material_id =
            capture_material_from_file(ctx, path, MATERIAL_REASON_CREATED, size).await?;
        SourceRecordAnchor {
            material_id,
            offset_start: 0,
            offset_end: size as i64,
        }
    };
    let created_at = file_created_at(&metadata, path)?;

    let payload = sinex_primitives::events::payloads::filesystem::FileCreatedPayload {
        path: sanitize_path(path)?,
        size,
        created_at,
        permissions: file_permissions(&metadata),
    };

    let event = payload
        .from_material(material_anchor.material_id)
        .build()
        .node_err("Failed to build event")?;

    let json_event = event
        .to_json_event()
        .map_err(|e| SinexError::processing("Failed to convert to JSON event").with_source(e))?;

    ctx.stage_context
        .emit_event_with_provenance(
            json_event,
            material_anchor.material_id,
            Some(material_anchor.offset_start),
            Some(material_anchor.offset_end),
        )
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
        warn_oversized_skip(ctx, path, size);
        return Ok(());
    }
    clear_oversized_skip_tracking(ctx, path);

    let material_anchor = if size == 0 {
        capture_observation_record(
            ctx,
            MATERIAL_REASON_MODIFIED,
            FileModifiedPayload::EVENT_TYPE.as_static_str(),
            path,
            None,
            None,
            Some(size),
        )
        .await?
    } else {
        let material_id =
            capture_material_from_file(ctx, path, MATERIAL_REASON_MODIFIED, size).await?;
        SourceRecordAnchor {
            material_id,
            offset_start: 0,
            offset_end: size as i64,
        }
    };
    let modified_at = file_modified_at(&metadata, path)?;

    let payload = sinex_primitives::events::payloads::filesystem::FileModifiedPayload {
        path: sanitize_path(path)?,
        size,
        modified_at,
        modification_type,
    };

    let event = payload
        .from_material(material_anchor.material_id)
        .build()
        .map_err(|e| SinexError::processing("Failed to build event").with_source(e))?;

    let json_event = event
        .to_json_event()
        .map_err(|e| SinexError::processing("Failed to convert to JSON event").with_source(e))?;

    ctx.stage_context
        .emit_event_with_provenance(
            json_event,
            material_anchor.material_id,
            Some(material_anchor.offset_start),
            Some(material_anchor.offset_end),
        )
        .await
        .map(|_| ())
        .map_err(|e| SinexError::processing("Failed to emit event").with_source(e))?;

    ctx.metrics.record_modified();
    debug!("Recorded file.modified for {:?}", path);
    Ok(())
}

async fn handle_file_deleted(ctx: &WatchContext, _root: &str, path: &Path) -> NodeResult<()> {
    clear_oversized_skip_tracking(ctx, path);
    // For deletions no file bytes are available. Record the observation in the
    // filesystem metadata stream instead of creating a dedicated zero-byte material.
    let material_anchor = capture_observation_record(
        ctx,
        MATERIAL_REASON_DELETED,
        FileDeletedPayload::EVENT_TYPE.as_static_str(),
        path,
        None,
        None,
        None,
    )
    .await?;

    let payload = sinex_primitives::events::payloads::filesystem::FileDeletedPayload {
        path: sanitize_path(path)?,
        deleted_at: sinex_primitives::temporal::now(),
    };

    let event = payload
        .from_material(material_anchor.material_id)
        .build()
        .map_err(|e| SinexError::processing("Failed to build event").with_source(e))?;

    let json_event = event
        .to_json_event()
        .map_err(|e| SinexError::processing("Failed to convert to JSON event").with_source(e))?;

    ctx.stage_context
        .emit_event_with_provenance(
            json_event,
            material_anchor.material_id,
            Some(material_anchor.offset_start),
            Some(material_anchor.offset_end),
        )
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
    clear_oversized_skip_tracking(ctx, old);
    clear_oversized_skip_tracking(ctx, new);
    let material_anchor = capture_observation_record(
        ctx,
        MATERIAL_REASON_MOVED,
        FileMovedPayload::EVENT_TYPE.as_static_str(),
        new,
        Some(old),
        Some(new),
        None,
    )
    .await?;

    let payload = sinex_primitives::events::payloads::filesystem::FileMovedPayload {
        old_path: sanitize_path(old)?,
        new_path: sanitize_path(new)?,
        moved_at: sinex_primitives::temporal::now(),
    };

    let event = payload
        .from_material(material_anchor.material_id)
        .build()
        .map_err(|e| SinexError::processing("Failed to build event").with_source(e))?;

    let json_event = event
        .to_json_event()
        .map_err(|e| SinexError::processing("Failed to convert to JSON event").with_source(e))?;

    ctx.stage_context
        .emit_event_with_provenance(
            json_event,
            material_anchor.material_id,
            Some(material_anchor.offset_start),
            Some(material_anchor.offset_end),
        )
        .await
        .map(|_| ())
        .map_err(|e| SinexError::processing("Failed to emit event").with_source(e))?;

    ctx.metrics.record_moved();
    debug!("Recorded file.moved from {:?} to {:?}", old, new);
    Ok(())
}

async fn capture_observation_record(
    ctx: &WatchContext,
    reason: &str,
    event_type: &str,
    path: &Path,
    old_path: Option<&Path>,
    new_path: Option<&Path>,
    size: Option<u64>,
) -> NodeResult<SourceRecordAnchor> {
    let record = FilesystemObservationRecord {
        source_identifier: ctx.observation_source_identifier.to_string(),
        reason: reason.to_string(),
        event_type: event_type.to_string(),
        path: observed_path_string(path)?,
        old_path: old_path.map(observed_path_string).transpose()?,
        new_path: new_path.map(observed_path_string).transpose()?,
        size,
        observed_at: sinex_primitives::temporal::now(),
    };
    ctx.observation_materializer
        .append_json_line(&record)
        .await
        .map_err(|error| {
            SinexError::processing("Failed to append filesystem observation record")
                .with_source(error)
        })
}

fn warn_oversized_skip(ctx: &WatchContext, path: &Path, size: u64) {
    if should_log_oversized_skip(&ctx.oversized_skip_log_buckets, path, size) {
        warn!(
            "Skipping file {:?} ({} bytes) exceeding limit {}",
            path, size, ctx.max_capture_bytes
        );
    }
}

fn should_log_oversized_skip(
    buckets: &StdMutex<HashMap<PathBuf, u64>>,
    path: &Path,
    size: u64,
) -> bool {
    let bucket = size / FS_OVERSIZED_LOG_BUCKET_BYTES;
    let mut guard = buckets
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    match guard.get(path) {
        Some(previous) if *previous == bucket => false,
        _ => {
            guard.insert(path.to_path_buf(), bucket);
            true
        }
    }
}

fn clear_oversized_skip_tracking(ctx: &WatchContext, path: &Path) {
    let mut guard = ctx
        .oversized_skip_log_buckets
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    guard.remove(path);
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
        .map_err(|e| capture_file_io_error(path, "open", &e))?;

    let metadata = file
        .metadata()
        .await
        .map_err(|e| capture_file_io_error(path, "metadata", &e))?;

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
            .map_err(|e| capture_file_io_error(path, "read", &e))?;

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

fn capture_file_io_error(path: &Path, operation: &str, err: &std::io::Error) -> SinexError {
    SinexError::io(format!("Failed to {operation} file during capture"))
        .with_std_error(err)
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

/// Convert an observed `Path` into a `RecordedPath`, applying the privacy
/// engine's metadata-context rules first.
///
/// The `user_home_path` rule (defined in
/// `crate/lib/sinex-primitives/src/privacy/catalog.rs`) collapses
/// `/home/USER/foo/bar` to `<HOME>/foo/bar` for any context, but until this
/// path runs through `privacy::process` no rule fires. Applying the engine
/// here means downstream events, derived analytics, and any export carry
/// home-relative paths instead of literal user-home prefixes — without
/// losing the user-meaningful suffix.
///
/// See issue #555.
fn sanitize_path(path: &Path) -> NodeResult<RecordedPath> {
    let observed = observed_path_string(path)?;
    let redacted = redact_metadata(&observed)?;
    RecordedPath::from_observed(redacted)
        .map_err(|e| SinexError::validation("Path recording failed").with_source(e))
}

fn observed_path_string(path: &Path) -> NodeResult<String> {
    path.to_str().map(str::to_owned).ok_or_else(|| {
        SinexError::validation("filesystem watcher observed non-utf8 path")
            .with_context("path_debug", path.display().to_string())
    })
}

/// Run a value through the privacy engine using the metadata context.
///
/// Returns the redacted text. Privacy-engine initialization failure is
/// surfaced as a `SinexError::configuration` rather than swallowed; the fs
/// ingestor cannot honestly emit if redaction is broken.
fn redact_metadata(value: &str) -> NodeResult<String> {
    Ok(privacy::process(value, ProcessingContext::Metadata)
        .map_err(|error| {
            SinexError::configuration("failed to initialize privacy engine")
                .with_context("component", "fs_path_redaction")
                .with_std_error(error)
        })?
        .text
        .into_owned())
}

#[allow(
    clippy::unnecessary_wraps,
    reason = "Returns None under #[cfg(not(unix))]; Option shape is forced by the cross-platform split"
)]
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
    filesystem_timestamp(
        metadata.created().or_else(|_| metadata.modified()),
        "created_at",
        path,
    )
}

fn log_watch_strategy(
    path: &Path,
    survey: &WatchTreeSurvey,
    budget: WatchBudget,
    watcher_mode: &str,
) {
    if let Some(kernel_max_watches) = budget.kernel_max_watches
        && kernel_max_watches < budget.configured_max_watches
    {
        warn!(
            path = %path.display(),
            configured_max_watches = budget.configured_max_watches,
            kernel_max_user_watches = kernel_max_watches,
            effective_max_watches = budget.effective_max_watches,
            "Filesystem watch budget exceeds the current kernel inotify limit; the effective limit is clamped by the host"
        );
    }
    if survey.accessible_watch_count > budget.effective_max_watches {
        warn!(
            path = %path.display(),
            accessible_watch_count = survey.accessible_watch_count,
            filtered_watch_count = survey.filtered_watch_count,
            effective_max_watches = budget.effective_max_watches,
            "Watch budget exceeded for native recursive mode; switching to filtered native watch plan"
        );
    }
    if survey.unreadable_directories > 0 {
        warn!(
            path = %path.display(),
            unreadable_directories = survey.unreadable_directories,
            "Unreadable descendants detected; switching to filtered native watch plan"
        );
    }
    if survey.ignored_directories > 0 {
        info!(
            path = %path.display(),
            ignored_directories = survey.ignored_directories,
            "Ignored descendants removed from filesystem watch plan"
        );
    }
    info!(
        path = %path.display(),
        accessible_watch_count = survey.accessible_watch_count,
        filtered_watch_count = survey.filtered_watch_count,
        configured_max_watches = budget.configured_max_watches,
        effective_max_watches = budget.effective_max_watches,
        watcher_mode,
        "Watching path"
    );
}

fn read_kernel_inotify_watch_limit() -> Option<usize> {
    std::fs::read_to_string(INOTIFY_MAX_USER_WATCHES_PATH)
        .ok()?
        .trim()
        .parse::<usize>()
        .ok()
}

fn handle_watcher_callback(
    result: Result<Event, notify::Error>,
    tx: &mpsc::Sender<Event>,
    drop_counter: &AtomicU64,
    error_counter: &AtomicU64,
    watcher_mode: &'static str,
) {
    match result {
        Ok(event) => match tx.try_send(event) {
            Ok(()) => {}
            Err(TrySendError::Full(_) | TrySendError::Closed(_)) => {
                let dropped = drop_counter.fetch_add(1, Ordering::Relaxed) + 1;
                if dropped == 1 || dropped.is_multiple_of(100) {
                    warn!(
                        dropped_events = dropped,
                        "Filesystem watcher channel unavailable; dropping events"
                    );
                }
            }
        },
        Err(error) => {
            let error_count = error_counter.fetch_add(1, Ordering::Relaxed) + 1;
            if error_count == 1 || error_count.is_multiple_of(100) {
                error!(
                    watcher_errors = error_count,
                    error = %error,
                    watcher_mode,
                    "Filesystem watcher reported error"
                );
            }
        }
    }
}

fn build_active_watcher<F>(
    canonical: &Path,
    root: &str,
    watcher_mode: &'static str,
    strategy: WatchStrategy,
    event_handler: F,
) -> NodeResult<ActiveWatcher>
where
    F: FnMut(Result<Event, notify::Error>) + Send + 'static,
{
    match strategy {
        WatchStrategy::NativeRecursive => {
            let mut watcher = notify::recommended_watcher(event_handler).map_err(|error| {
                SinexError::lifecycle("Failed to create watcher").with_source(error)
            })?;
            watcher
                .watch(canonical, RecursiveMode::Recursive)
                .map_err(|error| {
                    SinexError::lifecycle(format!(
                        "Failed to watch path '{root}' using {watcher_mode} watcher"
                    ))
                    .with_source(error)
                })?;
            Ok(ActiveWatcher::NativeRecursive { _watcher: watcher })
        }
        WatchStrategy::NativeFiltered { filtered_targets } => {
            let mut watcher = notify::recommended_watcher(event_handler).map_err(|error| {
                SinexError::lifecycle("Failed to create watcher").with_source(error)
            })?;
            let mut watched_targets = HashSet::new();
            add_filtered_watch_targets(&mut watcher, &mut watched_targets, filtered_targets)?;
            Ok(ActiveWatcher::NativeFiltered {
                watcher,
                watched_targets,
            })
        }
    }
}

fn prepare_watch_root(
    root: &str,
    ctx: &WatchContext,
) -> NodeResult<(PathBuf, String, WatchTreeSurvey, WatchStrategy, WatchBudget)> {
    let normalized = validate_watch_path(root, &ctx.security_policy)
        .map_err(|error| SinexError::validation(error.to_string()))?;
    let canonical = std::fs::canonicalize(normalized.as_str()).map_err(|error| {
        SinexError::validation(format!("Failed to canonicalize watch path '{root}'"))
            .with_source(error)
    })?;
    let canonical_root = canonical.to_str().map(str::to_owned).ok_or_else(|| {
        SinexError::validation("filesystem watcher root resolved to non-utf8 path")
            .with_context("path_debug", canonical.display().to_string())
    })?;
    let survey = survey_watch_tree(
        &canonical,
        0,
        ctx.max_depth,
        ctx.follow_symlinks,
        &ctx.ignored_directory_names,
    )?;
    let budget = WatchBudget::detect(ctx.max_watches);
    let strategy = choose_watch_strategy(&survey, budget)?;
    Ok((canonical, canonical_root, survey, strategy, budget))
}

fn file_modified_at(
    metadata: &StdMetadata,
    path: &Path,
) -> NodeResult<sinex_primitives::temporal::Timestamp> {
    filesystem_timestamp(metadata.modified(), "modified_at", path)
}

fn replay_material_identifier(material: &ResolvedReplayMaterial) -> &str {
    material
        .material_metadata
        .get("logical_source_identifier")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_else(|| {
            material
                .source_identifier
                .split("#material=")
                .next()
                .unwrap_or(material.source_identifier.as_str())
        })
}

fn historical_scan_targets(
    config: &FilesystemConfig,
    replay: &Option<MaterialReplayContext>,
) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut targets = Vec::new();

    if let Some(replay) = replay {
        for material in &replay.materials {
            let identifier = replay_material_identifier(material);
            if seen.insert(identifier.to_string()) {
                targets.push(identifier.to_string());
            }
        }
    }

    if targets.is_empty() {
        for path in &config.watch_paths {
            if seen.insert(path.clone()) {
                targets.push(path.clone());
            }
        }
    }

    targets
}

fn watch_context_for_target<'a>(
    contexts: &'a HashMap<String, WatchContext>,
    target: &Path,
) -> Option<(&'a str, &'a WatchContext)> {
    contexts
        .iter()
        .filter(|(root, _)| {
            target.starts_with(Path::new(root.as_str())) || Path::new(root).starts_with(target)
        })
        .max_by_key(|(root, _)| root.len())
        .map(|(root, ctx)| (root.as_str(), ctx))
        .or_else(|| {
            contexts
                .iter()
                .next()
                .map(|(root, ctx)| (root.as_str(), ctx))
        })
}

fn collect_historical_files(
    path: &Path,
    root: &Path,
    depth: usize,
    max_depth: Option<usize>,
    follow_symlinks: bool,
    ignored_directory_names: &HashSet<String>,
    warnings: &mut Vec<String>,
) -> NodeResult<Vec<PathBuf>> {
    if path_contains_ignored_descendant(root, path, ignored_directory_names) {
        warnings.push(format!(
            "skipping ignored filesystem historical target {}",
            path.display()
        ));
        return Ok(Vec::new());
    }

    if let Some(path_str) = path.to_str() {
        let utf8 = camino::Utf8Path::new(path_str);
        if let Some(reason) = check_sensitive_path(utf8) {
            warnings.push(format!(
                "skipping sensitive filesystem historical target {}: {}",
                path.display(),
                reason
            ));
            return Ok(Vec::new());
        }
    }

    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            warnings.push(format!(
                "filesystem historical target no longer exists: {}",
                path.display()
            ));
            return Ok(Vec::new());
        }
        Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => {
            warnings.push(format!(
                "filesystem historical target is unreadable: {}",
                path.display()
            ));
            return Ok(Vec::new());
        }
        Err(error) => {
            return Err(
                SinexError::io("Failed to inspect filesystem historical target")
                    .with_std_error(&error)
                    .with_path(path.display()),
            );
        }
    };

    let mut is_file = metadata.is_file();
    let mut is_dir = metadata.is_dir();

    if metadata.file_type().is_symlink() {
        if !follow_symlinks {
            warnings.push(format!(
                "skipping symlink during filesystem historical scan: {}",
                path.display()
            ));
            return Ok(Vec::new());
        }

        let resolved = std::fs::metadata(path).map_err(|error| {
            SinexError::io("Failed to follow filesystem historical symlink")
                .with_std_error(&error)
                .with_path(path.display())
        })?;
        is_file = resolved.is_file();
        is_dir = resolved.is_dir();
    }

    if is_file {
        return Ok(vec![path.to_path_buf()]);
    }

    if !is_dir {
        return Ok(Vec::new());
    }

    if max_depth.is_some_and(|limit| depth >= limit) {
        return Ok(Vec::new());
    }

    let entries = match std::fs::read_dir(path) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => {
            warnings.push(format!(
                "skipping unreadable directory during filesystem historical scan: {}",
                path.display()
            ));
            return Ok(Vec::new());
        }
        Err(error) => {
            return Err(
                SinexError::io("Failed to enumerate filesystem historical target")
                    .with_std_error(&error)
                    .with_path(path.display()),
            );
        }
    };

    let mut files = Vec::new();
    for entry in entries {
        let entry = match entry {
            Ok(entry) => entry,
            Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => {
                warnings.push(format!(
                    "skipping unreadable directory entry during filesystem historical scan: {}",
                    path.display()
                ));
                continue;
            }
            Err(error) => {
                return Err(SinexError::io(
                    "Failed to inspect filesystem historical directory entry",
                )
                .with_std_error(&error)
                .with_path(path.display()));
            }
        };

        files.extend(collect_historical_files(
            &entry.path(),
            root,
            depth + 1,
            max_depth,
            follow_symlinks,
            ignored_directory_names,
            warnings,
        )?);
    }

    Ok(files)
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
    use xtask::sandbox::node_runtime::TestRuntimeBuilder;
    use xtask::sandbox::prelude::*;
    use xtask::sandbox::sinex_test;

    #[cfg(unix)]
    use std::ffi::OsString;
    #[cfg(unix)]
    use std::os::unix::ffi::OsStringExt;
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;

    #[sinex_test]
    async fn filesystem_config_validation_allows_basic_configuration() -> TestResult<()> {
        let mut config = FilesystemConfig::default();
        config.watch_paths = vec!["/tmp".to_string()];
        assert!(config.validate_config().is_ok());
        Ok(())
    }

    #[sinex_test]
    async fn redact_metadata_collapses_user_home_prefix() -> TestResult<()> {
        // Emulate the fs ingestor running under a real user shell — the
        // privacy engine's `user_home_path` rule reads $HOME / $USER at
        // first call and caches a regex. Ensure we set a value the rule
        // can match on.
        unsafe {
            std::env::set_var("HOME", "/home/sinity-test-fs-redact");
        }

        let observed = "/home/sinity-test-fs-redact/projects/sinex/Cargo.toml";
        let redacted = redact_metadata(observed)?;

        // Outside the home prefix → unchanged. Inside → replaced with
        // `<HOME>/...`. The exact replacement label is owned by the
        // catalog, so assert on the substitution shape rather than the
        // literal expanded suffix.
        assert!(
            !redacted.contains("/home/sinity-test-fs-redact/"),
            "redacted output should not contain the literal home prefix, got {redacted:?}"
        );
        Ok(())
    }

    #[sinex_test]
    async fn redact_metadata_passes_non_home_paths_through() -> TestResult<()> {
        let observed = "/etc/hosts";
        let redacted = redact_metadata(observed)?;
        assert_eq!(
            redacted, observed,
            "system paths should not be touched by user_home_path rule"
        );
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
        assert!(
            state
                .description
                .contains("No filesystem watch paths configured")
        );
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
    async fn filesystem_source_state_does_not_stay_unhealthy_after_transient_processing_error()
    -> TestResult<()> {
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
                tokio::time::sleep(Duration::from_mins(1)).await;
                Ok(())
            }));
            guard.push(tokio::spawn(async { Ok(()) }));
        }
        tokio::task::yield_now().await;

        let state = sinex_node_sdk::ExplorationProvider::get_source_state(&node)?;
        assert!(
            state.is_connected,
            "one active watcher should keep the source connected"
        );
        assert!(
            !state.healthy,
            "finished watcher handles must degrade filesystem source health"
        );
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
    async fn survey_watch_tree_counts_nested_directories() -> TestResult<()> {
        let temp_root = tempdir()?;
        std::fs::create_dir_all(temp_root.path().join("a/b"))?;
        std::fs::create_dir_all(temp_root.path().join("c"))?;

        let survey = survey_watch_tree(temp_root.path(), 0, None, false, &HashSet::new())?;
        assert_eq!(
            survey.accessible_watch_count, 4,
            "root + three nested directories should need four watches"
        );
        assert_eq!(survey.filtered_watch_count, 4);
        Ok(())
    }

    #[cfg(unix)]
    #[sinex_test]
    async fn survey_watch_tree_skips_unreadable_subdirectories() -> TestResult<()> {
        let temp_root = tempdir()?;
        let unreadable = temp_root.path().join("private");
        let nested = unreadable.join("nested");
        std::fs::create_dir_all(&nested)?;
        std::fs::create_dir_all(&unreadable)?;

        let original_permissions = std::fs::metadata(&unreadable)?.permissions();
        let mut restricted_permissions = original_permissions.clone();
        restricted_permissions.set_mode(0o000);
        std::fs::set_permissions(&unreadable, restricted_permissions)?;

        let survey = survey_watch_tree(temp_root.path(), 0, None, false, &HashSet::new())?;

        std::fs::set_permissions(&unreadable, original_permissions)?;

        assert!(
            survey.accessible_watch_count >= 2,
            "root and unreadable directory should still count toward watch budget: {}",
            survey.accessible_watch_count
        );
        assert_eq!(
            survey.accessible_watch_count, 2,
            "nested descendants under an unreadable subtree should be skipped conservatively"
        );
        assert_eq!(survey.filtered_watch_count, 1);
        assert_eq!(survey.unreadable_directories, 1);
        Ok(())
    }

    #[sinex_test]
    async fn survey_watch_tree_skips_ignored_directories() -> TestResult<()> {
        let temp_root = tempdir()?;
        std::fs::create_dir_all(temp_root.path().join(".direnv/profile/bin"))?;
        std::fs::create_dir_all(temp_root.path().join("notes/daily"))?;

        let ignored = HashSet::from([".direnv".to_string()]);
        let survey = survey_watch_tree(temp_root.path(), 0, None, false, &ignored)?;

        assert_eq!(survey.accessible_watch_count, 4);
        assert_eq!(survey.filtered_watch_count, 3);
        assert_eq!(survey.ignored_directories, 1);
        Ok(())
    }

    #[sinex_test]
    async fn choose_watch_strategy_uses_effective_watch_budget() -> TestResult<()> {
        let survey = WatchTreeSurvey {
            accessible_watch_count: 6,
            filtered_watch_count: 4,
            filtered_targets: vec![PathBuf::from("/tmp"), PathBuf::from("/tmp/notes")],
            ..WatchTreeSurvey::default()
        };
        let budget = WatchBudget {
            configured_max_watches: 8,
            effective_max_watches: 4,
            kernel_max_watches: Some(4),
        };

        let strategy = choose_watch_strategy(&survey, budget)?;
        assert!(matches!(strategy, WatchStrategy::NativeFiltered { .. }));
        Ok(())
    }

    #[sinex_test]
    async fn choose_watch_strategy_reports_kernel_limit_when_filtered_plan_still_too_large()
    -> TestResult<()> {
        let survey = WatchTreeSurvey {
            accessible_watch_count: 8,
            filtered_watch_count: 5,
            ..WatchTreeSurvey::default()
        };
        let budget = WatchBudget {
            configured_max_watches: 8,
            effective_max_watches: 4,
            kernel_max_watches: Some(4),
        };

        let error = choose_watch_strategy(&survey, budget)
            .expect_err("oversized filtered plan should fail honestly");
        let message = error.to_string();
        assert!(message.contains("kernel_max_user_watches"));
        assert!(message.contains("effective_max_watches"));
        Ok(())
    }

    #[sinex_test]
    async fn filtered_watch_target_registration_errors_are_classified() -> TestResult<()> {
        let permission_denied = notify::Error::io(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "permission denied",
        ));
        let io_not_found =
            notify::Error::io(std::io::Error::new(std::io::ErrorKind::NotFound, "missing"));
        let path_not_found = notify::Error::path_not_found();
        let watch_limit = notify::Error::new(notify::ErrorKind::MaxFilesWatch);

        assert!(notify_error_is_skippable_filtered_target(
            &permission_denied
        ));
        assert!(notify_error_is_skippable_filtered_target(&io_not_found));
        assert!(notify_error_is_skippable_filtered_target(&path_not_found));
        assert!(
            !notify_error_is_skippable_filtered_target(&watch_limit),
            "watch exhaustion must remain fatal"
        );
        Ok(())
    }

    fn test_watch_context(
        acquisition: Arc<AcquisitionManager>,
        stage_context: StageAsYouGoContext,
        cancel_token: CancellationToken,
    ) -> WatchContext {
        let observation_source_identifier = Arc::<str>::from("filesystem.observations:test");
        let observation_materializer = BufferedRecordMaterializer::buffered(
            Arc::clone(&acquisition),
            observation_source_identifier.to_string(),
            BufferedAppendStreamWriterConfig {
                channel_capacity: FS_OBSERVATION_WRITER_CHANNEL_CAPACITY,
                batch_max_records: FS_OBSERVATION_BATCH_MAX_RECORDS,
                batch_max_bytes: FS_OBSERVATION_BATCH_MAX_BYTES,
                batch_coalesce_window: std::time::Duration::from_millis(1),
            },
        );
        WatchContext {
            acquisition,
            observation_materializer,
            observation_source_identifier,
            stage_context,
            max_capture_bytes: Bytes::from_mebibytes(1),
            max_watches: DEFAULT_MAX_WATCHES,
            max_depth: Some(DEFAULT_MAX_DEPTH),
            follow_symlinks: true,
            security_policy: FileWatchingSecurityPolicy::permissive(),
            dropped_events: Arc::new(AtomicU64::new(0)),
            metrics: EventMetrics::new(),
            ignored_directory_names: Arc::new(
                [
                    ".git".to_string(),
                    ".direnv".to_string(),
                    "node_modules".to_string(),
                    "target".to_string(),
                ]
                .into_iter()
                .collect(),
            ),
            cancel_token,
            capture_semaphore: Arc::new(tokio::sync::Semaphore::new(FS_MAX_CONCURRENT_CAPTURES)),
            oversized_skip_log_buckets: Arc::new(StdMutex::new(HashMap::new())),
        }
    }

    #[test]
    fn oversized_skip_logging_is_bucketed_per_path() {
        let buckets = StdMutex::new(HashMap::new());
        let path = Path::new("/tmp/session.cast");

        assert!(should_log_oversized_skip(&buckets, path, 11 * 1024 * 1024,));
        assert!(!should_log_oversized_skip(
            &buckets,
            path,
            11 * 1024 * 1024 + 512,
        ));
        assert!(should_log_oversized_skip(&buckets, path, 12 * 1024 * 1024,));
    }

    #[cfg(unix)]
    #[sinex_test(timeout = 30)]
    async fn watch_path_skips_unreadable_descendant_with_filtered_native_plan(
        ctx: TestContext,
    ) -> TestResult<()> {
        use std::os::unix::fs::PermissionsExt;

        let ctx = ctx.with_nats().dedicated().await?;
        let nats_client = ctx.nats_client();

        AcquisitionManager::bootstrap_streams(&nats_client).await?;

        let acquisition = Arc::new(AcquisitionManager::with_defaults(nats_client, "filesystem"));
        let (event_tx, mut event_rx) =
            mpsc::channel::<SinexEvent>(sinex_primitives::buffers::DEFAULT_EVENT_CHANNEL_SIZE);
        let cancel_token = CancellationToken::new();
        let stage_context =
            StageAsYouGoContext::from_sender(Arc::clone(&acquisition), event_tx, false);

        let temp_root = tempdir()?;
        let unreadable = temp_root.path().join("private");
        std::fs::create_dir_all(unreadable.join("nested"))?;
        let original_permissions = std::fs::metadata(&unreadable)?.permissions();
        let mut restricted_permissions = original_permissions.clone();
        restricted_permissions.set_mode(0o000);
        std::fs::set_permissions(&unreadable, restricted_permissions)?;

        let watch_ctx = test_watch_context(acquisition, stage_context, cancel_token.clone());

        let watch_path_root = temp_root
            .path()
            .to_str()
            .ok_or_else(|| color_eyre::eyre::eyre!("temp root path not utf8"))?
            .to_string();

        let watcher_task = tokio::spawn(watch_path(watch_path_root, watch_ctx));

        tokio::time::sleep(Duration::from_millis(350)).await;

        let created_path = temp_root.path().join("readable-created.txt");
        tokio::fs::write(&created_path, b"watch me").await?;

        let event = timeout(Duration::from_secs(15), event_rx.recv())
            .await?
            .ok_or_else(|| {
                color_eyre::eyre::eyre!("filesystem filtered native watcher emitted no event")
            })?;

        assert_eq!(
            event.event_type.as_str(),
            FileCreatedPayload::EVENT_TYPE.as_static_str()
        );

        let event_path = event
            .payload
            .get("path")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| color_eyre::eyre::eyre!("filesystem event missing path payload"))?;
        assert!(
            event_path.ends_with("readable-created.txt"),
            "unexpected filesystem event path after filtered native watch planning: {event_path}"
        );

        cancel_token.cancel();
        watcher_task.await??;
        std::fs::set_permissions(&unreadable, original_permissions)?;
        Ok(())
    }

    #[sinex_test(timeout = 30)]
    async fn watch_path_ignores_configured_heavy_descendants(ctx: TestContext) -> TestResult<()> {
        let ctx = ctx.with_nats().dedicated().await?;
        let nats_client = ctx.nats_client();

        AcquisitionManager::bootstrap_streams(&nats_client).await?;

        let acquisition = Arc::new(AcquisitionManager::with_defaults(nats_client, "filesystem"));
        let (event_tx, mut event_rx) =
            mpsc::channel::<SinexEvent>(sinex_primitives::buffers::DEFAULT_EVENT_CHANNEL_SIZE);
        let cancel_token = CancellationToken::new();
        let stage_context =
            StageAsYouGoContext::from_sender(Arc::clone(&acquisition), event_tx, false);

        let temp_root = tempdir()?;
        std::fs::create_dir_all(temp_root.path().join(".direnv/profile/bin"))?;
        std::fs::create_dir_all(temp_root.path().join("notes"))?;

        let mut watch_ctx = test_watch_context(acquisition, stage_context, cancel_token.clone());
        watch_ctx.max_watches = 2;

        let watch_path_root = temp_root
            .path()
            .to_str()
            .ok_or_else(|| color_eyre::eyre::eyre!("temp root path not utf8"))?
            .to_string();

        let watcher_task = tokio::spawn(watch_path(watch_path_root, watch_ctx));
        tokio::time::sleep(Duration::from_millis(350)).await;

        tokio::fs::write(temp_root.path().join("notes/kept.txt"), b"keep").await?;
        let kept_event = timeout(Duration::from_secs(10), event_rx.recv())
            .await?
            .ok_or_else(|| {
                color_eyre::eyre::eyre!("filtered native watcher emitted no kept event")
            })?;
        let kept_path = kept_event
            .payload
            .get("path")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| color_eyre::eyre::eyre!("filesystem event missing kept path payload"))?;
        assert!(kept_path.ends_with("notes/kept.txt"));

        tokio::fs::write(
            temp_root.path().join(".direnv/profile/bin/ignored.txt"),
            b"ignore",
        )
        .await?;
        let deadline = tokio::time::Instant::now() + Duration::from_millis(800);
        while tokio::time::Instant::now() < deadline {
            let Ok(Some(event)) = timeout(Duration::from_millis(100), event_rx.recv()).await else {
                continue;
            };
            let path = event
                .payload
                .get("path")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| color_eyre::eyre::eyre!("filesystem event missing path payload"))?;
            assert!(
                !path.ends_with(".direnv/profile/bin/ignored.txt"),
                "ignored heavy descendants should not emit filesystem events"
            );
        }

        cancel_token.cancel();
        watcher_task.await??;
        Ok(())
    }

    #[sinex_test]
    async fn filesystem_watcher_shutdown_result_accepts_clean_exit() -> TestResult<()> {
        let handle = tokio::spawn(async { Ok(()) });
        FilesystemNode::watcher_shutdown_result(0, handle.await)?;
        Ok(())
    }

    #[sinex_test]
    async fn filesystem_watcher_shutdown_result_accepts_cancelled_task() -> TestResult<()> {
        let handle = tokio::spawn(async {
            tokio::time::sleep(Duration::from_secs(30)).await;
            Ok(())
        });
        handle.abort();
        FilesystemNode::watcher_shutdown_result(1, handle.await)?;
        Ok(())
    }

    #[sinex_test]
    async fn filesystem_watcher_shutdown_result_rejects_panicked_task() -> TestResult<()> {
        let handle = tokio::spawn(async {
            panic!("filesystem watcher panic");
            #[allow(unreachable_code)]
            Ok(())
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
    async fn filesystem_watcher_shutdown_result_rejects_task_error() -> TestResult<()> {
        let handle = tokio::spawn(async {
            Err(SinexError::lifecycle(
                "watcher failed before shutdown".to_string(),
            ))
        });
        let error = FilesystemNode::watcher_shutdown_result(3, handle.await)
            .expect_err("watcher task errors must surface honestly");
        assert!(error.to_string().contains("watcher failed before shutdown"));
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
        node.initialize(config, &runtime.runtime, &mut state)
            .await?;

        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let _ = shutdown_tx.send(true);

        let report = timeout(
            Duration::from_secs(1),
            node.run_continuous(
                &mut state,
                ContinuousStart::from_checkpoint(Checkpoint::None),
                shutdown_rx,
            ),
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
        let invalid_path =
            PathBuf::from(OsString::from_vec(vec![b'/', b't', b'm', b'p', b'/', 0xff]));

        let error =
            observed_path_string(&invalid_path).expect_err("non-utf8 observed paths must fail");

        assert!(
            error
                .to_string()
                .contains("filesystem watcher observed non-utf8 path")
        );
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

        let acquisition = Arc::new(AcquisitionManager::with_defaults(nats_client, "filesystem"));

        let (event_tx, mut event_rx) =
            mpsc::channel::<SinexEvent>(sinex_primitives::buffers::DEFAULT_EVENT_CHANNEL_SIZE);
        let stage_context =
            StageAsYouGoContext::from_sender(Arc::clone(&acquisition), event_tx, false);

        let watch_ctx = test_watch_context(acquisition, stage_context, CancellationToken::new());

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

    #[sinex_test]
    async fn zero_byte_file_events_use_observation_stream_material(
        ctx: TestContext,
    ) -> TestResult<()> {
        ctx.set_proof_metadata(ProofMetadata {
            runner_id: Some(
                "source-material.filesystem-zero-byte-observation-stream.v1".to_string(),
            ),
            subject_refs: vec!["https://github.com/Sinity/sinex/issues/315".to_string()],
            claim_ids: vec![
                "metadata-only-filesystem-events-use-observation-stream".to_string(),
                "zero-byte-files-do-not-create-per-path-zero-byte-materials".to_string(),
                "observation-stream-anchors-remain-contiguous".to_string(),
            ],
            status: Some("asserted_by_test".to_string()),
            reproducer: Some(
                "xtask test -p sinex-fs-ingestor -E 'test(zero_byte_file_events_use_observation_stream_material)'"
                    .to_string(),
            ),
            environment: serde_json::json!({
                "plane": "isolated-dev",
                "stack": ["fs-ingestor", "node-sdk", "nats"],
            }),
        });
        let ctx = ctx.with_nats().dedicated().await?;
        let nats_client = ctx.nats_client();

        AcquisitionManager::bootstrap_streams(&nats_client).await?;

        let acquisition = Arc::new(AcquisitionManager::with_defaults(nats_client, "filesystem"));

        let (event_tx, mut event_rx) =
            mpsc::channel::<SinexEvent>(sinex_primitives::buffers::DEFAULT_EVENT_CHANNEL_SIZE);
        let stage_context =
            StageAsYouGoContext::from_sender(Arc::clone(&acquisition), event_tx, false);

        let watch_ctx = test_watch_context(acquisition, stage_context, CancellationToken::new());

        let temp_root = tempdir()?;
        let first_path = temp_root.path().join("first.lock");
        let second_path = temp_root.path().join("second.lock");
        tokio::fs::write(&first_path, b"").await?;
        tokio::fs::write(&second_path, b"").await?;

        let temp_root_str = temp_root
            .path()
            .to_str()
            .ok_or_else(|| color_eyre::eyre::eyre!("temp root path not utf8"))?;
        handle_file_created(&watch_ctx, temp_root_str, &first_path).await?;
        handle_file_created(&watch_ctx, temp_root_str, &second_path).await?;

        let first = timeout(Duration::from_secs(10), event_rx.recv())
            .await?
            .ok_or_else(|| color_eyre::eyre::eyre!("first zero-byte event not emitted"))?;
        let second = timeout(Duration::from_secs(10), event_rx.recv())
            .await?
            .ok_or_else(|| color_eyre::eyre::eyre!("second zero-byte event not emitted"))?;

        let Provenance::Material {
            id: first_material,
            offset_start: Some(first_start),
            offset_end: Some(first_end),
            ..
        } = first.provenance
        else {
            return Err(color_eyre::eyre::eyre!(
                "first event should use material provenance with offsets"
            ));
        };
        let Provenance::Material {
            id: second_material,
            offset_start: Some(second_start),
            offset_end: Some(second_end),
            ..
        } = second.provenance
        else {
            return Err(color_eyre::eyre::eyre!(
                "second event should use material provenance with offsets"
            ));
        };

        assert_eq!(
            first_material, second_material,
            "zero-byte filesystem observations should share the bounded metadata stream"
        );
        assert_eq!(first_end, second_start);
        assert!(first_start < first_end);
        assert!(second_start < second_end);
        Ok(())
    }

    #[sinex_test(timeout = 30)]
    async fn watch_path_keeps_watcher_alive_and_emits_created_event(
        ctx: TestContext,
    ) -> TestResult<()> {
        let ctx = ctx.with_nats().dedicated().await?;
        let nats_client = ctx.nats_client();

        AcquisitionManager::bootstrap_streams(&nats_client).await?;

        let acquisition = Arc::new(AcquisitionManager::with_defaults(nats_client, "filesystem"));
        let (event_tx, mut event_rx) =
            mpsc::channel::<SinexEvent>(sinex_primitives::buffers::DEFAULT_EVENT_CHANNEL_SIZE);
        let cancel_token = CancellationToken::new();
        let stage_context =
            StageAsYouGoContext::from_sender(Arc::clone(&acquisition), event_tx, false);

        let watch_ctx = test_watch_context(acquisition, stage_context, cancel_token.clone());

        let temp_root = tempdir()?;
        let watch_path_root = temp_root
            .path()
            .to_str()
            .ok_or_else(|| color_eyre::eyre::eyre!("temp root path not utf8"))?
            .to_string();

        let watcher_task = tokio::spawn(watch_path(watch_path_root.clone(), watch_ctx));

        // Give notify enough time to arm the recursive watcher before mutating the tree.
        tokio::time::sleep(Duration::from_millis(250)).await;

        let created_path = temp_root.path().join("continuous-created.txt");
        tokio::fs::write(&created_path, b"watch me").await?;

        let event = timeout(Duration::from_secs(10), event_rx.recv())
            .await?
            .ok_or_else(|| {
                color_eyre::eyre::eyre!("filesystem continuous watcher emitted no event")
            })?;

        assert_eq!(
            event.event_type.as_str(),
            FileCreatedPayload::EVENT_TYPE.as_static_str()
        );

        let event_path = event
            .payload
            .get("path")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| color_eyre::eyre::eyre!("filesystem event missing path payload"))?;
        assert!(
            event_path.ends_with("continuous-created.txt"),
            "unexpected filesystem event path: {event_path}"
        );

        cancel_token.cancel();
        watcher_task.await??;
        Ok(())
    }

    #[sinex_test]
    async fn filesystem_historical_replay_emits_events_for_resolved_material_paths(
        ctx: TestContext,
    ) -> TestResult<()> {
        let mut runtime = TestRuntimeBuilder::new(&ctx, "filesystem-historical-replay")
            .build()
            .await?;

        let temp_root = tempdir()?;
        let root_str = temp_root
            .path()
            .to_str()
            .ok_or_else(|| color_eyre::eyre::eyre!("temp root path not utf8"))?
            .to_string();
        let file_a = temp_root.path().join("replay-a.txt");
        let file_b = temp_root.path().join("nested/replay-b.txt");
        std::fs::create_dir_all(
            file_b
                .parent()
                .ok_or_else(|| color_eyre::eyre::eyre!("nested replay file missing parent"))?,
        )?;
        tokio::fs::write(&file_a, b"replay-a").await?;
        tokio::fs::write(&file_b, b"replay-b").await?;

        let mut node = FilesystemNode::new();
        let mut state = FilesystemCheckpoint::default();
        node.initialize(
            FilesystemConfig {
                watch_paths: vec![root_str.clone()],
                ..FilesystemConfig::default()
            },
            &runtime.runtime,
            &mut state,
        )
        .await?;

        let report = node
            .scan_historical(
                &mut state,
                Checkpoint::None,
                TimeHorizon::Historical {
                    end_time: Timestamp::now(),
                },
                ScanArgs {
                    replay: Some(MaterialReplayContext {
                        operation_id: Uuid::now_v7(),
                        materials: vec![
                            ResolvedReplayMaterial {
                                source_material_id: Uuid::now_v7(),
                                material_kind: "annex".to_string(),
                                source_identifier: format!(
                                    "{}#material={}",
                                    file_a.display(),
                                    Uuid::now_v7()
                                ),
                                material_metadata: serde_json::json!({
                                    "logical_source_identifier": file_a.display().to_string()
                                }),
                                material_start_time: None,
                                material_end_time: None,
                            },
                            ResolvedReplayMaterial {
                                source_material_id: Uuid::now_v7(),
                                material_kind: "annex".to_string(),
                                source_identifier: format!(
                                    "{}#material={}",
                                    file_b.display(),
                                    Uuid::now_v7()
                                ),
                                material_metadata: serde_json::json!({
                                    "logical_source_identifier": file_b.display().to_string()
                                }),
                                material_start_time: None,
                                material_end_time: None,
                            },
                        ],
                        replay_scope: sinex_node_sdk::runtime::stream::ReplayScopeFilters::default(
                        ),
                    }),
                    ..ScanArgs::default()
                },
            )
            .await?;

        assert_eq!(report.events_processed, 2);

        let mut emitted_paths = Vec::new();
        for _ in 0..2 {
            let event = timeout(Duration::from_secs(10), runtime.event_rx.recv())
                .await?
                .ok_or_else(|| color_eyre::eyre::eyre!("filesystem replay emitted no event"))?;
            emitted_paths.push(
                event
                    .payload
                    .get("path")
                    .and_then(serde_json::Value::as_str)
                    .ok_or_else(|| {
                        color_eyre::eyre::eyre!("filesystem replay event missing path payload")
                    })?
                    .to_string(),
            );
        }

        assert!(
            emitted_paths
                .iter()
                .any(|path| path.ends_with("replay-a.txt"))
        );
        assert!(
            emitted_paths
                .iter()
                .any(|path| path.ends_with("replay-b.txt"))
        );

        node.shutdown(&state).await?;
        Ok(())
    }
}

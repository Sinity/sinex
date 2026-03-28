#![doc = include_str!("../docs/overview.md")]

//! Terminal node that tails configured history files and emits structured
//! command events. Each discovered command is captured as a source material via
//! `AcquisitionManager` and published to `JetStream`, while the structured event
//! is emitted through the shared Stage-as-You-Go channel.

use camino::Utf8PathBuf;
use serde::{Deserialize, Serialize};
use serde_json::{self, json};
use sinex_node_sdk::{
    ActivityEntry, CoverageAnalysis, ExplorationProvider, ExportFormat, IngestionHistoryEntry,
    SourceState, SqliteHistoryRowOutcome, import_sqlite_history_lenient, stage_material,
};
use sinex_node_sdk::{
    NodeResult, SinexError,
    acquisition_manager::{AcquisitionManager, RotationPolicy},
    ingestor_node::IngestorNode,
    runtime::stream::{
        Checkpoint, NodeRuntimeState, ScanArgs, ScanReport, ServiceInfo, TimeHorizon,
    },
    stage_as_you_go::StageAsYouGoContext,
};
use sinex_primitives::{
    Bytes, Seconds,
    domain::{RecordedPath, SanitizedPath},
    events::{
        EventPayload,
        payloads::shell::{AtuinCommandExecutedPayload, HistoryCommandImportedPayload},
    },
    temporal::Timestamp,
    validate_path,
};
use std::{
    collections::{HashMap, VecDeque},
    path::PathBuf,
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{
    fs,
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    sync::{Mutex, watch},
};
use tracing::{debug, info, warn};
use uuid::Uuid;
use validator::ValidationError;

const MATERIAL_REASON_HISTORY: &str = "terminal-history";

// Default configuration values
const DEFAULT_POLLING_INTERVAL: Seconds = Seconds::from_secs(5);
const DEFAULT_MAX_CAPTURE_BYTES: Bytes = Bytes::from_bytes(32 * 1024); // 32 KiB
const ENV_POLLING_INTERVAL: &str = "SINEX_TERMINAL_POLLING_INTERVAL_SECS";
const TERMINAL_ACTIVITY_CAPACITY: usize = 32;

/// Configuration for a shell history source.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistorySourceConfig {
    pub path: Utf8PathBuf,

    /// Shell type (bash, zsh, fish, etc.)
    pub shell: String,
}

use sinex_primitives::privacy::{self, ProcessingContext};

fn validate_history_path(path: &Utf8PathBuf) -> Result<(), ValidationError> {
    validate_path(path.as_str())
        .map(|_| ())
        .map_err(|_| ValidationError::new("invalid_history_path"))
}

/// Terminal node configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TerminalConfig {
    /// Shell history sources to monitor.
    pub history_sources: Vec<HistorySourceConfig>,

    /// Polling interval for checking file changes (seconds)
    pub polling_interval_secs: Seconds,

    /// Maximum capture size per command (bytes)
    pub max_capture_bytes: Bytes,
}

impl Default for TerminalConfig {
    fn default() -> Self {
        let home =
            crate::shell_detection::utf8_home_dir("building default terminal history sources");
        let default_sources = default_history_sources(home.as_ref());

        // Allow polling interval override via environment variable
        let polling_interval_secs = default_polling_interval();

        Self {
            history_sources: default_sources,
            polling_interval_secs,
            max_capture_bytes: DEFAULT_MAX_CAPTURE_BYTES,
        }
    }
}

fn default_history_sources(home: Option<&Utf8PathBuf>) -> Vec<HistorySourceConfig> {
    let Some(home) = home else {
        return Vec::new();
    };

    vec![
        HistorySourceConfig {
            path: home.join(".bash_history"),
            shell: "bash".to_string(),
        },
        HistorySourceConfig {
            path: home.join(".zsh_history"),
            shell: "zsh".to_string(),
        },
        HistorySourceConfig {
            path: home.join(".local/share/atuin/history.db"),
            shell: "atuin".to_string(),
        },
    ]
}

impl TerminalConfig {
    pub fn validate_config(&self) -> NodeResult<()> {
        if self.history_sources.is_empty() {
            return Err(SinexError::configuration(
                "At least one history source must be configured".to_string(),
            ));
        }

        for source in &self.history_sources {
            validate_history_path(&source.path)
                .map_err(|_| SinexError::configuration("Invalid history file path".to_string()))?;
            if source.shell.trim().is_empty() {
                return Err(SinexError::configuration(
                    "Shell type cannot be empty".to_string(),
                ));
            }
        }

        let polling_secs = self.polling_interval_secs.as_secs();
        if !(1..=3600).contains(&polling_secs) {
            return Err(SinexError::configuration(
                "Polling interval must be between 1 and 3600 seconds".to_string(),
            ));
        }

        let max_bytes = self.max_capture_bytes.as_u64();
        if !(64..=1024 * 1024).contains(&max_bytes) {
            return Err(SinexError::configuration(
                "Max capture bytes must be between 64B and 1MB".to_string(),
            ));
        }

        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct TerminalState {
    pub captured_at: Timestamp,
    pub monitored_sources: Vec<Utf8PathBuf>,
    pub host: String,
}

/// Maximum number of command hashes to retain for deduplication.
/// Covers ~10K most recent commands, which is sufficient to handle history rotation/truncation.
const DEDUP_HASH_CAPACITY: usize = 10_000;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct HistoryState {
    offset_bytes: u64,
    line_number: u64,
    #[serde(default)]
    pending_timestamp: Option<Timestamp>,
    /// Inode of the file when last processed (Unix only, used to detect rotation vs truncation)
    #[cfg(unix)]
    inode: Option<u64>,
    /// For `SQLite`-backed history sources: last processed ROWID
    sqlite_row_id: Option<i64>,
    /// Rolling window of command content hashes for deduplication across file rotation/truncation.
    /// When a history file is rotated (new inode), old commands may reappear; this set prevents
    /// duplicate events from being emitted.
    #[serde(default)]
    recent_hashes: VecDeque<u64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct TerminalHistoryCheckpoint {
    #[serde(default)]
    sources: HashMap<String, HistoryState>,
}

#[derive(Debug, Clone)]
struct HistoryScanOutcome {
    processed: usize,
    state: HistoryState,
    warnings: Vec<String>,
    failure: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum HistorySourceMode {
    Text,
    FishSqlite,
    AtuinSqlite,
    ConfiguredError(String),
}

fn default_polling_interval() -> Seconds {
    match std::env::var(ENV_POLLING_INTERVAL) {
        Ok(raw) => match raw.parse::<u64>() {
            Ok(seconds) => Seconds::from_secs(seconds),
            Err(error) => {
                warn!(
                    env = ENV_POLLING_INTERVAL,
                    value = %raw,
                    %error,
                    "Invalid terminal polling interval override; using default"
                );
                DEFAULT_POLLING_INTERVAL
            }
        },
        Err(std::env::VarError::NotPresent) => DEFAULT_POLLING_INTERVAL,
        Err(std::env::VarError::NotUnicode(_)) => {
            warn!(
                env = ENV_POLLING_INTERVAL,
                "Terminal polling interval override is not valid UTF-8; using default"
            );
            DEFAULT_POLLING_INTERVAL
        }
    }
}

fn classify_history_source(source: &HistorySourceConfig) -> HistorySourceMode {
    match source.shell.to_lowercase().as_str() {
        "fish" => match crate::fish_history::ensure_fish_sqlite_history(&source.path) {
            Ok(()) => HistorySourceMode::FishSqlite,
            Err(error) => HistorySourceMode::ConfiguredError(format!(
                "configured Fish history source {} is unusable: {error}",
                source.path
            )),
        },
        "atuin" => match crate::atuin_history::ensure_atuin_sqlite_history(&source.path) {
            Ok(()) => HistorySourceMode::AtuinSqlite,
            Err(error) => HistorySourceMode::ConfiguredError(format!(
                "configured Atuin history source {} is unusable: {error}",
                source.path
            )),
        },
        "elvish" => HistorySourceMode::ConfiguredError(format!(
            "configured Elvish history source {} uses Elvish's native database format, which is not supported",
            source.path
        )),
        _ => HistorySourceMode::Text,
    }
}

#[derive(Clone)]
struct HistoryWatcherContext {
    acquisition: Arc<AcquisitionManager>,
    stage_context: StageAsYouGoContext,
    metrics: Arc<TerminalMetrics>,
    shell: String,
    path: Utf8PathBuf,
    max_capture_bytes: Bytes,
    polling_interval: Duration,
    state_path: Option<PathBuf>,
    shutdown_rx: watch::Receiver<bool>,
    processed_commands: Option<Arc<Mutex<Vec<String>>>>,
    source_mode: HistorySourceMode,
    initial_state_override: Option<HistoryState>,
}

#[derive(Debug, Clone, Default, Serialize)]
struct ShellMetrics {
    commands_processed: u64,
    polls_completed: u64,
    processing_errors: u64,
    skipped_binary: u64,
    skipped_duplicate: u64,
    skipped_too_large: u64,
    last_poll_duration_ms: u64,
    last_history_size_bytes: u64,
    last_command_size_bytes: u64,
    last_command_line_number: Option<u64>,
}

struct TerminalMetrics {
    commands_processed: AtomicU64,
    polls_completed: AtomicU64,
    processing_errors: AtomicU64,
    skipped_binary: AtomicU64,
    skipped_duplicate: AtomicU64,
    skipped_too_large: AtomicU64,
    bytes_captured: AtomicU64,
    poll_duration_ms_total: AtomicU64,
    shells: StdMutex<HashMap<String, ShellMetrics>>,
    recent_activity: StdMutex<VecDeque<ActivityEntry>>,
}

impl TerminalMetrics {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            commands_processed: AtomicU64::new(0),
            polls_completed: AtomicU64::new(0),
            processing_errors: AtomicU64::new(0),
            skipped_binary: AtomicU64::new(0),
            skipped_duplicate: AtomicU64::new(0),
            skipped_too_large: AtomicU64::new(0),
            bytes_captured: AtomicU64::new(0),
            poll_duration_ms_total: AtomicU64::new(0),
            shells: StdMutex::new(HashMap::new()),
            recent_activity: StdMutex::new(VecDeque::with_capacity(TERMINAL_ACTIVITY_CAPACITY)),
        })
    }

    fn record_command(&self, shell: &str, path: &Utf8PathBuf, bytes: usize, line_number: u64) {
        self.commands_processed.fetch_add(1, Ordering::Relaxed);
        self.bytes_captured
            .fetch_add(bytes as u64, Ordering::Relaxed);

        {
            let mut shells = self
                .shells
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let entry = shells.entry(shell.to_string()).or_default();
            entry.commands_processed = entry.commands_processed.saturating_add(1);
            entry.last_command_size_bytes = bytes as u64;
            entry.last_command_line_number = Some(line_number);
        }

        self.push_activity(
            format!("Imported {shell} history command from {path}"),
            json!({
                "shell": shell,
                "path": path,
                "bytes": bytes,
                "line_number": line_number,
            }),
        );
    }

    fn record_poll(
        &self,
        shell: &str,
        path: &Utf8PathBuf,
        duration: Duration,
        file_size: u64,
        processed: usize,
    ) {
        let duration_ms = duration.as_millis().min(u128::from(u64::MAX)) as u64;
        self.polls_completed.fetch_add(1, Ordering::Relaxed);
        self.poll_duration_ms_total
            .fetch_add(duration_ms, Ordering::Relaxed);

        {
            let mut shells = self
                .shells
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let entry = shells.entry(shell.to_string()).or_default();
            entry.polls_completed = entry.polls_completed.saturating_add(1);
            entry.last_poll_duration_ms = duration_ms;
            entry.last_history_size_bytes = file_size;
        }

        self.push_activity(
            format!("Polled {shell} history source {path}"),
            json!({
                "shell": shell,
                "path": path,
                "duration_ms": duration_ms,
                "file_size_bytes": file_size,
                "commands_processed": processed,
            }),
        );
    }

    fn record_skip(
        &self,
        shell: &str,
        path: &Utf8PathBuf,
        reason: &'static str,
        line_number: u64,
        bytes: Option<usize>,
    ) {
        {
            let mut shells = self
                .shells
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let entry = shells.entry(shell.to_string()).or_default();
            match reason {
                "binary" => {
                    self.skipped_binary.fetch_add(1, Ordering::Relaxed);
                    entry.skipped_binary = entry.skipped_binary.saturating_add(1);
                }
                "duplicate" => {
                    self.skipped_duplicate.fetch_add(1, Ordering::Relaxed);
                    entry.skipped_duplicate = entry.skipped_duplicate.saturating_add(1);
                }
                "too_large" => {
                    self.skipped_too_large.fetch_add(1, Ordering::Relaxed);
                    entry.skipped_too_large = entry.skipped_too_large.saturating_add(1);
                }
                _ => {}
            }
        }

        self.push_activity(
            format!("Skipped {shell} history command from {path}"),
            json!({
                "shell": shell,
                "path": path,
                "reason": reason,
                "line_number": line_number,
                "bytes": bytes,
            }),
        );
    }

    fn record_error(&self, shell: &str, path: &Utf8PathBuf, stage: &'static str, error: &str) {
        self.processing_errors.fetch_add(1, Ordering::Relaxed);
        {
            let mut shells = self
                .shells
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let entry = shells.entry(shell.to_string()).or_default();
            entry.processing_errors = entry.processing_errors.saturating_add(1);
        }

        self.push_activity(
            format!("Terminal watcher error while {stage} for {path}"),
            json!({
                "shell": shell,
                "path": path,
                "stage": stage,
                "error": error,
            }),
        );
    }

    fn metadata(&self) -> HashMap<String, serde_json::Value> {
        let mut metadata = HashMap::new();
        metadata.insert(
            "commands_processed".to_string(),
            json!(self.commands_processed.load(Ordering::Relaxed)),
        );
        metadata.insert(
            "polls_completed".to_string(),
            json!(self.polls_completed.load(Ordering::Relaxed)),
        );
        metadata.insert(
            "processing_errors".to_string(),
            json!(self.processing_errors.load(Ordering::Relaxed)),
        );
        metadata.insert(
            "skipped_binary".to_string(),
            json!(self.skipped_binary.load(Ordering::Relaxed)),
        );
        metadata.insert(
            "skipped_duplicate".to_string(),
            json!(self.skipped_duplicate.load(Ordering::Relaxed)),
        );
        metadata.insert(
            "skipped_too_large".to_string(),
            json!(self.skipped_too_large.load(Ordering::Relaxed)),
        );
        metadata.insert(
            "bytes_captured".to_string(),
            json!(self.bytes_captured.load(Ordering::Relaxed)),
        );
        metadata.insert(
            "poll_duration_ms_total".to_string(),
            json!(self.poll_duration_ms_total.load(Ordering::Relaxed)),
        );
        metadata.insert(
            "shells".to_string(),
            json!(
                self.shells
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .clone()
            ),
        );
        metadata
    }

    fn recent_activity(&self) -> Vec<ActivityEntry> {
        self.recent_activity
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .cloned()
            .collect()
    }

    fn push_activity(&self, description: String, data: serde_json::Value) {
        let mut activity = self
            .recent_activity
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if activity.len() >= TERMINAL_ACTIVITY_CAPACITY {
            activity.pop_front();
        }
        activity.push_back(ActivityEntry {
            timestamp: Timestamp::now(),
            description,
            data: Some(data),
        });
    }
}

impl HistoryWatcherContext {
    fn requires_sqlite_row_id(&self) -> bool {
        matches!(
            self.source_mode,
            HistorySourceMode::FishSqlite | HistorySourceMode::AtuinSqlite
        )
    }

    fn empty_state(&self) -> HistoryState {
        if self.requires_sqlite_row_id() {
            Self::sqlite_history_state(0, VecDeque::new())
        } else {
            HistoryState::default()
        }
    }

    fn require_sqlite_row_id(&self, state: &HistoryState) -> NodeResult<i64> {
        state.sqlite_row_id.ok_or_else(|| {
            SinexError::processing(
                "history watcher state missing sqlite_row_id for SQLite-backed source",
            )
            .with_context("shell", self.shell.clone())
            .with_context("path", self.path.to_string())
        })
    }

    async fn remove_temp_state_file(&self, temp_path: &std::path::Path) {
        if let Err(error) = fs::remove_file(temp_path).await {
            warn!(
                path = %self.path,
                temp_path = %temp_path.display(),
                error = %error,
                "Failed to remove temporary terminal history state file"
            );
        }
    }

    fn validate_state(&self, state: HistoryState) -> NodeResult<HistoryState> {
        if self.requires_sqlite_row_id() && state.sqlite_row_id.is_none() {
            return Err(SinexError::processing(
                "history watcher state missing sqlite_row_id for SQLite-backed source",
            )
            .with_context("shell", self.shell.clone())
            .with_context("path", self.path.to_string()));
        }
        if let Some(sqlite_row_id) = state.sqlite_row_id
            && sqlite_row_id < 0
        {
            return Err(SinexError::processing(
                "history watcher state has invalid negative sqlite_row_id",
            )
            .with_context("shell", self.shell.clone())
            .with_context("path", self.path.to_string())
            .with_context("sqlite_row_id", sqlite_row_id.to_string()));
        }
        Ok(state)
    }

    fn checkpoint_key(&self) -> String {
        format!("{}:{}", self.shell, self.path)
    }

    fn record_poll(&self, started_at: Instant, file_size: u64, processed: usize) {
        self.metrics.record_poll(
            &self.shell,
            &self.path,
            started_at.elapsed(),
            file_size,
            processed,
        );
    }

    fn record_error(&self, stage: &'static str, error: &str) {
        self.metrics
            .record_error(&self.shell, &self.path, stage, error);
    }

    async fn monitor(self) -> NodeResult<()> {
        match &self.source_mode {
            HistorySourceMode::Text => self.monitor_text_history().await,
            HistorySourceMode::FishSqlite => self.monitor_fish_sqlite().await,
            HistorySourceMode::AtuinSqlite => self.monitor_atuin_sqlite().await,
            HistorySourceMode::ConfiguredError(error) => {
                self.record_error("configure_history_source", error);
                warn!(shell = %self.shell, path = %self.path, %error, "Terminal source disabled");
                Ok(())
            }
        }
    }

    fn strict_warning(&self, detail: impl Into<String>) -> String {
        format!("{}: {}", self.checkpoint_key(), detail.into())
    }

    fn success_outcome(
        &self,
        processed: usize,
        state: HistoryState,
        warnings: Vec<String>,
    ) -> HistoryScanOutcome {
        HistoryScanOutcome {
            processed,
            state,
            warnings,
            failure: None,
        }
    }

    fn failed_outcome(
        &self,
        stage: &'static str,
        error: impl std::fmt::Display,
        state: HistoryState,
    ) -> HistoryScanOutcome {
        let error = error.to_string();
        self.record_error(stage, &error);
        HistoryScanOutcome {
            processed: 0,
            state,
            warnings: Vec::new(),
            failure: Some(error),
        }
    }

    async fn monitor_text_history(self) -> NodeResult<()> {
        let state = self.resolve_state(self.initial_state_override.clone()).await?;
        let mut offset_bytes = state.offset_bytes;
        let mut line_number = state.line_number;
        let mut pending_timestamp = state.pending_timestamp;
        #[cfg(unix)]
        let mut last_inode = state.inode;
        let mut recent_hashes = state.recent_hashes;
        let mut shutdown_rx = self.shutdown_rx.clone();
        debug!(
            path = %self.path,
            offset = offset_bytes,
            line_number,
            dedup_hashes = recent_hashes.len(),
            "Restored terminal watcher state"
        );
        if self.initial_state_override.is_some() {
            self.persist_state(
                offset_bytes,
                line_number,
                pending_timestamp,
                &recent_hashes,
            )
            .await?;
        }

        loop {
            if *shutdown_rx.borrow() {
                info!(path = %self.path, "History watcher shutdown requested");
                break;
            }

            #[cfg(unix)]
            {
                self.poll_history_once(
                    &mut offset_bytes,
                    &mut line_number,
                    &mut pending_timestamp,
                    &mut last_inode,
                    &mut recent_hashes,
                    true,
                )
                .await?;
            }
            #[cfg(not(unix))]
            {
                self.poll_history_once(
                    &mut offset_bytes,
                    &mut line_number,
                    &mut pending_timestamp,
                    &mut recent_hashes,
                    true,
                )
                .await?;
            }

            tokio::select! {
                () = tokio::time::sleep(self.polling_interval) => {},
                shutdown_result = shutdown_rx.changed() => {
                    if shutdown_result.is_err() || *shutdown_rx.borrow() {
                        info!(path = %self.path, "History watcher shutdown requested");
                        break;
                    }
                }
            }
        }

        Ok(())
    }

    async fn monitor_fish_sqlite(self) -> NodeResult<()> {
        let (mut sqlite_row_id, mut recent_hashes) = match self
            .resolve_state(self.initial_state_override.clone())
            .await
        {
            Ok(state) => match self.require_sqlite_row_id(&state) {
                Ok(sqlite_row_id) => (sqlite_row_id, state.recent_hashes),
                Err(error) => return Err(error),
            },
            Err(error) => return Err(error),
        };
        let mut shutdown_rx = self.shutdown_rx.clone();
        debug!(
            path = %self.path,
            sqlite_row_id,
            dedup_hashes = recent_hashes.len(),
            "Restored Fish history watcher state"
        );
        if self.initial_state_override.is_some() {
            self.persist_sqlite_state(sqlite_row_id, &recent_hashes).await?;
        }

        loop {
            if *shutdown_rx.borrow() {
                info!(path = %self.path, "Fish history watcher shutdown requested");
                break;
            }

            self.poll_fish_history_once(&mut sqlite_row_id, &mut recent_hashes, true)
                .await?;

            tokio::select! {
                () = tokio::time::sleep(self.polling_interval) => {},
                shutdown_result = shutdown_rx.changed() => {
                    if shutdown_result.is_err() || *shutdown_rx.borrow() {
                        info!(path = %self.path, "Fish history watcher shutdown requested");
                        break;
                    }
                }
            }
        }

        Ok(())
    }

    async fn monitor_atuin_sqlite(self) -> NodeResult<()> {
        let (mut sqlite_row_id, mut recent_hashes) = match self
            .resolve_state(self.initial_state_override.clone())
            .await
        {
            Ok(state) => match self.require_sqlite_row_id(&state) {
                Ok(sqlite_row_id) => (sqlite_row_id, state.recent_hashes),
                Err(error) => return Err(error),
            },
            Err(error) => return Err(error),
        };
        let mut shutdown_rx = self.shutdown_rx.clone();
        debug!(
            path = %self.path,
            sqlite_row_id,
            dedup_hashes = recent_hashes.len(),
            "Restored Atuin history watcher state"
        );
        if self.initial_state_override.is_some() {
            self.persist_sqlite_state(sqlite_row_id, &recent_hashes).await?;
        }

        loop {
            if *shutdown_rx.borrow() {
                info!(path = %self.path, "Atuin history watcher shutdown requested");
                break;
            }

            self.poll_atuin_history_once(&mut sqlite_row_id, &mut recent_hashes, true)
                .await?;

            tokio::select! {
                () = tokio::time::sleep(self.polling_interval) => {},
                shutdown_result = shutdown_rx.changed() => {
                    if shutdown_result.is_err() || *shutdown_rx.borrow() {
                        info!(path = %self.path, "Atuin history watcher shutdown requested");
                        break;
                    }
                }
            }
        }

        Ok(())
    }

    async fn load_state(&self) -> NodeResult<Option<HistoryState>> {
        let Some(path) = self.state_path.as_ref() else {
            return Ok(None);
        };
        match fs::read(path).await {
            Ok(bytes) => match serde_json::from_slice::<HistoryState>(&bytes) {
                Ok(state) => Ok(Some(state)),
                Err(error) => Err(
                    SinexError::io("failed to decode history watcher state")
                        .with_context("path", path.display().to_string())
                        .with_std_error(&error),
                ),
            },
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(
                SinexError::io("failed to load history watcher state")
                    .with_context("path", path.display().to_string())
                    .with_std_error(&error),
            ),
        }
    }

    async fn resolve_state(&self, state_override: Option<HistoryState>) -> NodeResult<HistoryState> {
        match state_override {
            Some(state) => self.validate_state(state),
            None => match self.load_state().await? {
                Some(state) => self.validate_state(state),
                None => Ok(self.empty_state()),
            },
        }
    }

    async fn history_file_size(&self) -> NodeResult<u64> {
        fs::metadata(&self.path).await.map(|metadata| metadata.len()).map_err(|error| {
            SinexError::io("failed to stat terminal history source")
                .with_context("shell", self.shell.clone())
                .with_context("path", self.path.to_string())
                .with_std_error(&error)
        })
    }

    async fn persist_state(
        &self,
        offset_bytes: u64,
        line_number: u64,
        pending_timestamp: Option<Timestamp>,
        recent_hashes: &VecDeque<u64>,
    ) -> NodeResult<()> {
        self.persist_state_full(
            offset_bytes,
            line_number,
            pending_timestamp,
            None,
            recent_hashes,
        )
        .await
    }

    async fn persist_sqlite_state(
        &self,
        sqlite_row_id: i64,
        recent_hashes: &VecDeque<u64>,
    ) -> NodeResult<()> {
        self.persist_state_full(0, 0, None, Some(sqlite_row_id), recent_hashes)
            .await
    }

    fn history_state_temp_path(path: &std::path::Path, suffix: Uuid) -> std::path::PathBuf {
        let mut file_name = path
            .file_name()
            .map_or_else(|| std::ffi::OsString::from("history_state"), std::ffi::OsStr::to_os_string);
        file_name.push(".");
        file_name.push(suffix.to_string());
        file_name.push(".tmp");

        path.parent()
            .unwrap_or_else(|| std::path::Path::new("."))
            .join(file_name)
    }

    fn sqlite_history_state(sqlite_row_id: i64, recent_hashes: VecDeque<u64>) -> HistoryState {
        HistoryState {
            sqlite_row_id: Some(sqlite_row_id),
            recent_hashes,
            ..HistoryState::default()
        }
    }

    async fn scan_history_once_from_state(
        &self,
        state_override: Option<HistoryState>,
        historical_end_time: Option<Timestamp>,
    ) -> HistoryScanOutcome {
        match &self.source_mode {
            HistorySourceMode::FishSqlite => {
                let state = match self.resolve_state(state_override).await {
                    Ok(state) => state,
                    Err(error) => {
                        return self.failed_outcome(
                            "load_history_state",
                            format!("failed to restore Fish history watcher state: {error}"),
                            HistoryState::default(),
                        );
                    }
                };
                let mut sqlite_row_id = match self.require_sqlite_row_id(&state) {
                    Ok(sqlite_row_id) => sqlite_row_id,
                    Err(error) => {
                        return self.failed_outcome(
                            "load_history_state",
                            format!("failed to restore Fish history watcher state: {error}"),
                            self.empty_state(),
                        );
                    }
                };
                let mut recent_hashes = state.recent_hashes;
                let poll_started_at = Instant::now();
                let file_size = match self.history_file_size().await {
                    Ok(size) => size,
                    Err(error) => {
                        self.record_poll(poll_started_at, 0, 0);
                        return self.failed_outcome(
                            "stat_history_file",
                            error,
                            Self::sqlite_history_state(sqlite_row_id, recent_hashes),
                        );
                    }
                };
                match import_sqlite_history_lenient(
                    sqlite_row_id,
                    historical_end_time,
                    |from_row_id, end_time| {
                        crate::fish_history::read_fish_history(&self.path, from_row_id, end_time)
                    },
                    |entry| entry.row_id,
                    |entry| {
                        let row_id = entry.row_id;
                        let prepared = match sqlite_row_id_to_line_number(self, row_id) {
                            Ok(line_number) => prepare_command_for_capture(
                                self,
                                &entry.command,
                                line_number,
                                Some(&mut recent_hashes),
                            )
                            .map_err(|error| {
                                let message =
                                    format!("failed to process Fish row {row_id}: {error}");
                                self.record_error("process_fish_entry", &message);
                                self.strict_warning(message)
                            }),
                            Err(error) => {
                                let message =
                                    format!("failed to process Fish row {row_id}: {error}");
                                self.record_error("process_fish_entry", &message);
                                Err(self.strict_warning(message))
                            }
                        };
                        async move {
                            let Some(final_command) = prepared? else {
                                return Ok(SqliteHistoryRowOutcome::Skipped);
                            };

                            emit_prepared_fish_entry(self, &entry, final_command)
                                .await
                                .map(|()| SqliteHistoryRowOutcome::Processed)
                                .map_err(|error| {
                                    let message =
                                        format!("failed to process Fish row {row_id}: {error}");
                                    self.record_error("process_fish_entry", &message);
                                    self.strict_warning(message)
                                })
                        }
                    },
                )
                .await
                {
                    Ok(report) => {
                        sqlite_row_id = sqlite_row_id.max(report.last_row_id);
                        self.record_poll(poll_started_at, file_size, report.processed_rows);
                        self.success_outcome(
                            report.processed_rows,
                            Self::sqlite_history_state(sqlite_row_id, recent_hashes),
                            report.warnings,
                        )
                    }
                    Err(error) => {
                        self.record_poll(poll_started_at, file_size, 0);
                        self.failed_outcome(
                            "read_fish_history",
                            format!("failed to read Fish history from {}: {error}", self.path),
                            Self::sqlite_history_state(sqlite_row_id, recent_hashes),
                        )
                    }
                }
            }
            HistorySourceMode::AtuinSqlite => {
                let state = match self.resolve_state(state_override).await {
                    Ok(state) => state,
                    Err(error) => {
                        return self.failed_outcome(
                            "load_history_state",
                            format!("failed to restore Atuin history watcher state: {error}"),
                            HistoryState::default(),
                        );
                    }
                };
                let mut sqlite_row_id = match self.require_sqlite_row_id(&state) {
                    Ok(sqlite_row_id) => sqlite_row_id,
                    Err(error) => {
                        return self.failed_outcome(
                            "load_history_state",
                            format!("failed to restore Atuin history watcher state: {error}"),
                            self.empty_state(),
                        );
                    }
                };
                let recent_hashes = state.recent_hashes;
                let poll_started_at = Instant::now();
                let file_size = match self.history_file_size().await {
                    Ok(size) => size,
                    Err(error) => {
                        self.record_poll(poll_started_at, 0, 0);
                        return self.failed_outcome(
                            "stat_history_file",
                            error,
                            Self::sqlite_history_state(sqlite_row_id, recent_hashes),
                        );
                    }
                };
                match import_sqlite_history_lenient(
                    sqlite_row_id,
                    historical_end_time,
                    |from_row_id, end_time| {
                        crate::atuin_history::read_atuin_history(&self.path, from_row_id, end_time)
                    },
                    |entry| entry.row_id,
                    |entry| {
                        let row_id = entry.row_id;
                        let prepared = match sqlite_row_id_to_line_number(self, row_id) {
                            Ok(line_number) => {
                                prepare_command_for_capture(self, &entry.command, line_number, None)
                                    .map_err(|error| {
                                        let message =
                                            format!("failed to process Atuin row {row_id}: {error}");
                                        self.record_error("process_atuin_entry", &message);
                                        self.strict_warning(message)
                                    })
                            }
                            Err(error) => {
                                let message =
                                    format!("failed to process Atuin row {row_id}: {error}");
                                self.record_error("process_atuin_entry", &message);
                                Err(self.strict_warning(message))
                            }
                        };
                        async move {
                            let Some(final_command) = prepared? else {
                                return Ok(SqliteHistoryRowOutcome::Skipped);
                            };

                            emit_prepared_atuin_entry(self, &entry, final_command)
                                .await
                                .map(|()| SqliteHistoryRowOutcome::Processed)
                                .map_err(|error| {
                                    let message =
                                        format!("failed to process Atuin row {row_id}: {error}");
                                    self.record_error("process_atuin_entry", &message);
                                    self.strict_warning(message)
                                })
                        }
                    },
                )
                .await
                {
                    Ok(report) => {
                        sqlite_row_id = sqlite_row_id.max(report.last_row_id);
                        self.record_poll(poll_started_at, file_size, report.processed_rows);
                        self.success_outcome(
                            report.processed_rows,
                            Self::sqlite_history_state(sqlite_row_id, recent_hashes),
                            report.warnings,
                        )
                    }
                    Err(error) => {
                        self.record_poll(poll_started_at, file_size, 0);
                        self.failed_outcome(
                            "read_atuin_history",
                            format!("failed to read Atuin history from {}: {error}", self.path),
                            Self::sqlite_history_state(sqlite_row_id, recent_hashes),
                        )
                    }
                }
            }
            HistorySourceMode::Text => {
                let state = match self.resolve_state(state_override).await {
                    Ok(state) => state,
                    Err(error) => {
                        return self.failed_outcome(
                            "load_history_state",
                            format!("failed to restore terminal history watcher state: {error}"),
                            HistoryState::default(),
                        );
                    }
                };
                let mut offset_bytes = state.offset_bytes;
                let mut line_number = state.line_number;
                let mut pending_timestamp = state.pending_timestamp;
                let mut recent_hashes = state.recent_hashes;
                #[cfg(unix)]
                let mut last_inode = state.inode;

                let poll_started_at = Instant::now();
                let mut file_size = 0u64;
                let mut processed = 0usize;
                let mut warnings = Vec::new();

                match fs::metadata(&self.path).await {
                    Ok(metadata) => {
                        file_size = metadata.len();
                        #[cfg(unix)]
                        {
                            use std::os::unix::fs::MetadataExt;

                            let current_inode = metadata.ino();
                            let inode_changed =
                                last_inode.is_some_and(|prev| prev != current_inode);
                            last_inode = Some(current_inode);

                            if file_size < offset_bytes {
                                if inode_changed {
                                    warnings.push(self.strict_warning(format!(
                                        "history file rotated at {}; restarting from the beginning",
                                        self.path
                                    )));
                                    offset_bytes = 0;
                                    line_number = 0;
                                    pending_timestamp = None;
                                } else {
                                    warnings.push(self.strict_warning(format!(
                                        "history file truncated at {}; advancing checkpoint to the new end",
                                        self.path
                                    )));
                                    offset_bytes = file_size;
                                    pending_timestamp = None;
                                }
                            }
                        }

                        #[cfg(not(unix))]
                        {
                            if file_size < offset_bytes {
                                warnings.push(self.strict_warning(format!(
                                    "history file truncated at {}; restarting from the beginning",
                                    self.path
                                )));
                                offset_bytes = 0;
                                line_number = 0;
                                pending_timestamp = None;
                            }
                        }

                        if file_size == offset_bytes {
                            self.record_poll(poll_started_at, file_size, processed);
                            return self.success_outcome(
                                processed,
                                HistoryState {
                                    offset_bytes,
                                    line_number,
                                    pending_timestamp,
                                    #[cfg(unix)]
                                    inode: last_inode,
                                    sqlite_row_id: None,
                                    recent_hashes,
                                },
                                warnings,
                            );
                        }

                        match self.read_new_segment(offset_bytes).await {
                            Ok(new_segment) => {
                                if !new_segment.is_empty() {
                                    let mut consumed_bytes = 0u64;
                                    for line in new_segment.split_inclusive('\n') {
                                        if !line.ends_with('\n') && new_segment.ends_with(line) {
                                            break;
                                        }
                                        let trimmed = line.trim_end_matches('\n');
                                        consumed_bytes += line.len() as u64;
                                        if trimmed.is_empty() {
                                            continue;
                                        }
                                        match process_text_history_line(
                                            self,
                                            trimmed,
                                            &mut line_number,
                                            &mut pending_timestamp,
                                            &mut recent_hashes,
                                        )
                                        .await
                                        {
                                            Ok(true) => {
                                                processed += 1;
                                            }
                                            Ok(false) => {}
                                            Err(error) => {
                                                let message = format!(
                                                    "failed to process history entry near line {line_number}: {error}"
                                                );
                                                self.record_error("process_command", &message);
                                                warnings.push(self.strict_warning(message));
                                            }
                                        }
                                    }
                                    if consumed_bytes > 0 {
                                        offset_bytes = offset_bytes.saturating_add(consumed_bytes);
                                    }
                                }
                                self.record_poll(poll_started_at, file_size, processed);
                                self.success_outcome(
                                    processed,
                                    HistoryState {
                                        offset_bytes,
                                        line_number,
                                        pending_timestamp,
                                        #[cfg(unix)]
                                        inode: last_inode,
                                        sqlite_row_id: None,
                                        recent_hashes,
                                    },
                                    warnings,
                                )
                            }
                            Err(error) => {
                                self.record_poll(poll_started_at, file_size, processed);
                                self.failed_outcome(
                                    "read_history_segment",
                                    format!(
                                        "failed to read terminal history from {}: {error}",
                                        self.path
                                    ),
                                    HistoryState {
                                        offset_bytes,
                                        line_number,
                                        pending_timestamp,
                                        #[cfg(unix)]
                                        inode: last_inode,
                                        sqlite_row_id: None,
                                        recent_hashes,
                                    },
                                )
                            }
                        }
                    }
                    Err(error) => {
                        self.record_poll(poll_started_at, file_size, processed);
                        self.failed_outcome(
                            "stat_history_file",
                            format!("failed to stat terminal history {}: {error}", self.path),
                            HistoryState {
                                offset_bytes,
                                line_number,
                                pending_timestamp,
                                #[cfg(unix)]
                                inode: last_inode,
                                sqlite_row_id: None,
                                recent_hashes,
                            },
                        )
                    }
                }
            }
            HistorySourceMode::ConfiguredError(error) => {
                let state = match self.resolve_state(state_override).await {
                    Ok(state) => state,
                    Err(load_error) => {
                        return self.failed_outcome(
                            "load_history_state",
                            format!(
                                "failed to restore terminal state for misconfigured source: {load_error}"
                            ),
                            HistoryState::default(),
                        );
                    }
                };
                self.failed_outcome("configure_history_source", error.clone(), state)
            }
        }
    }

    async fn persist_state_full(
        &self,
        offset_bytes: u64,
        line_number: u64,
        pending_timestamp: Option<Timestamp>,
        sqlite_row_id: Option<i64>,
        recent_hashes: &VecDeque<u64>,
    ) -> NodeResult<()> {
        let Some(path) = &self.state_path else {
            return Ok(());
        };

        // Get current inode for tracking file rotation vs truncation
        #[cfg(unix)]
        let current_inode = {
            use std::os::unix::fs::MetadataExt;
            match std::fs::metadata(self.path.as_std_path()) {
                Ok(metadata) => Some(metadata.ino()),
                Err(error) => {
                    warn!(
                        path = %self.path,
                        error = %error,
                        "Failed to read history file metadata while persisting watcher state"
                    );
                    None
                }
            }
        };

        let state = HistoryState {
            offset_bytes,
            line_number,
            pending_timestamp,
            #[cfg(unix)]
            inode: current_inode,
            sqlite_row_id,
            recent_hashes: recent_hashes.clone(),
        };

        match serde_json::to_vec_pretty(&state) {
            Ok(serialized) => {
                if let Some(parent) = path.parent()
                    && let Err(e) = fs::create_dir_all(parent).await
                {
                    return Err(
                        SinexError::io("failed to create terminal history state directory")
                            .with_context("path", path.display().to_string())
                            .with_context("parent", parent.display().to_string())
                            .with_std_error(&e),
                    );
                }

                let temp_path = Self::history_state_temp_path(path, Uuid::now_v7());

                match fs::OpenOptions::new()
                    .create_new(true)
                    .write(true)
                    .open(&temp_path)
                    .await
                {
                    Ok(mut file) => {
                        if let Err(e) = file.write_all(&serialized).await {
                            self.remove_temp_state_file(&temp_path).await;
                            return Err(
                                SinexError::io("failed to write terminal history state file")
                                    .with_context("path", path.display().to_string())
                                    .with_context("temp_path", temp_path.display().to_string())
                                    .with_std_error(&e),
                            );
                        }
                        if let Err(e) = file.sync_all().await {
                            self.remove_temp_state_file(&temp_path).await;
                            return Err(
                                SinexError::io("failed to fsync terminal history state file")
                                    .with_context("path", path.display().to_string())
                                    .with_context("temp_path", temp_path.display().to_string())
                                    .with_std_error(&e),
                            );
                        }
                        if let Err(e) = fs::rename(&temp_path, path).await {
                            self.remove_temp_state_file(&temp_path).await;
                            return Err(
                                SinexError::io("failed to replace terminal history state file")
                                    .with_context("path", path.display().to_string())
                                    .with_context("temp_path", temp_path.display().to_string())
                                    .with_std_error(&e),
                            );
                        } else {
                            // Fsync the parent directory to ensure the rename is durable.
                            // Without this, the renamed file might not be visible after a crash.
                            if let Some(parent) = path.parent()
                                && let Ok(dir) = std::fs::File::open(parent)
                                && let Err(e) = dir.sync_all()
                            {
                                return Err(
                                    SinexError::io(
                                        "failed to fsync terminal history state directory",
                                    )
                                    .with_context("path", path.display().to_string())
                                    .with_context("parent", parent.display().to_string())
                                    .with_std_error(&e),
                                );
                            }
                        }
                    }
                    Err(e) => {
                        return Err(
                            SinexError::io("failed to create terminal history temp state file")
                                .with_context("path", path.display().to_string())
                                .with_context("temp_path", temp_path.display().to_string())
                                .with_std_error(&e),
                        );
                    }
                }
            }
            Err(e) => {
                return Err(
                    SinexError::serialization("failed to serialize terminal history watcher state")
                        .with_context("path", path.display().to_string())
                        .with_std_error(&e),
                );
            }
        }

        Ok(())
    }

    async fn read_new_segment(&self, offset: u64) -> std::io::Result<String> {
        use std::io::SeekFrom;

        let mut file = tokio::fs::File::open(&self.path).await?;
        file.seek(SeekFrom::Start(offset)).await?;

        // Pre-allocate to reduce repeated growth; capped by max_capture_bytes.
        let prealloc = self.max_capture_bytes.as_usize().min(32 * 1024);
        let mut buffer = Vec::with_capacity(prealloc);
        file.read_to_end(&mut buffer).await?;

        Ok(String::from_utf8_lossy(&buffer).to_string())
    }

    /// Poll history file for new content (Unix version with inode tracking)
    ///
    /// On Unix, tracks file inode to distinguish between:
    /// - File rotation (new inode): Reset to offset 0 and re-process from start
    /// - File truncation (same inode): Adjust offset without re-processing
    #[cfg(unix)]
    async fn poll_history_once(
        &self,
        offset_bytes: &mut u64,
        line_number: &mut u64,
        pending_timestamp: &mut Option<Timestamp>,
        last_inode: &mut Option<u64>,
        recent_hashes: &mut VecDeque<u64>,
        persist_state: bool,
    ) -> NodeResult<usize> {
        use std::os::unix::fs::MetadataExt;

        let poll_started_at = Instant::now();
        let mut processed = 0usize;
        let mut file_size = 0u64;
        match fs::metadata(&self.path).await {
            Ok(metadata) => {
                file_size = metadata.len();
                let current_inode = metadata.ino();

                // Update inode tracking
                let inode_changed = last_inode.is_some_and(|prev| prev != current_inode);
                *last_inode = Some(current_inode);

                if file_size < *offset_bytes {
                    if inode_changed {
                        // File rotation: new file with new inode, reset and re-process
                        debug!(
                            path = %self.path,
                            previous_offset = *offset_bytes,
                            new_size = file_size,
                            old_inode = ?last_inode,
                            new_inode = current_inode,
                            "History file rotated (new inode); resetting to read new file"
                        );
                        *offset_bytes = 0;
                        *line_number = 0;
                        *pending_timestamp = None;
                    } else {
                        // Same inode but smaller: truncation, adjust offset without re-processing
                        debug!(
                            path = %self.path,
                            previous_offset = *offset_bytes,
                            new_size = file_size,
                            inode = current_inode,
                            "History file truncated (same inode); adjusting offset"
                        );
                        *offset_bytes = file_size;
                        *pending_timestamp = None;
                        // Keep line_number as-is; we don't know exactly where we are
                    }
                    if persist_state {
                        self.persist_state(
                            *offset_bytes,
                            *line_number,
                            *pending_timestamp,
                            recent_hashes,
                        )
                        .await?;
                    }
                    self.record_poll(poll_started_at, file_size, processed);
                    return Ok(processed);
                }

                if file_size == *offset_bytes {
                    self.record_poll(poll_started_at, file_size, processed);
                    return Ok(processed);
                }

                match self.read_new_segment(*offset_bytes).await {
                    Ok(new_segment) => {
                        if new_segment.is_empty() {
                            return Ok(processed);
                        }

                        let mut consumed_bytes: u64 = 0;

                        for line in new_segment.split_inclusive('\n') {
                            if !line.ends_with('\n') && new_segment.ends_with(line) {
                                break;
                            }

                            let trimmed = line.trim_end_matches('\n');
                            consumed_bytes += line.len() as u64;

                            if trimmed.is_empty() {
                                continue;
                            }

                            match process_text_history_line(
                                self,
                                trimmed,
                                line_number,
                                pending_timestamp,
                                recent_hashes,
                            )
                            .await
                            {
                                Ok(true) => {
                                    processed += 1;
                                }
                                Ok(false) => {}
                                Err(e) => {
                                    self.record_error("process_command", &e.to_string());
                                    warn!(
                                        "Failed to process history entry from {}: {}",
                                        self.path, e
                                    );
                                }
                            }
                        }

                        if consumed_bytes > 0 {
                            *offset_bytes = offset_bytes.saturating_add(consumed_bytes);
                            if persist_state {
                                self.persist_state(
                                    *offset_bytes,
                                    *line_number,
                                    *pending_timestamp,
                                    recent_hashes,
                                )
                                .await?;
                            }
                        }
                    }
                    Err(e) => {
                        self.record_error("read_history_segment", &e.to_string());
                        warn!("History watcher unable to read {}: {}", self.path, e);
                    }
                }
            }
            Err(e) => {
                self.record_error("stat_history_file", &e.to_string());
                warn!("History watcher unable to stat {}: {}", self.path, e);
            }
        }

        self.record_poll(poll_started_at, file_size, processed);
        Ok(processed)
    }

    /// Poll history file for new content (non-Unix version without inode tracking)
    #[cfg(not(unix))]
    async fn poll_history_once(
        &self,
        offset_bytes: &mut u64,
        line_number: &mut u64,
        pending_timestamp: &mut Option<Timestamp>,
        recent_hashes: &mut VecDeque<u64>,
        persist_state: bool,
    ) -> NodeResult<usize> {
        let poll_started_at = Instant::now();
        let mut processed = 0usize;
        let mut file_size = 0u64;
        match fs::metadata(&self.path).await {
            Ok(metadata) => {
                file_size = metadata.len();

                if file_size < *offset_bytes {
                    debug!(
                        path = %self.path,
                        previous_offset = *offset_bytes,
                        new_size = file_size,
                        "History file truncated; resetting offsets"
                    );
                    *offset_bytes = 0;
                    *line_number = 0;
                    *pending_timestamp = None;
                    if persist_state {
                        self.persist_state(
                            *offset_bytes,
                            *line_number,
                            *pending_timestamp,
                            recent_hashes,
                        )
                        .await?;
                    }
                    self.record_poll(poll_started_at, file_size, processed);
                    return Ok(processed);
                }

                if file_size == *offset_bytes {
                    self.record_poll(poll_started_at, file_size, processed);
                    return Ok(processed);
                }

                match self.read_new_segment(*offset_bytes).await {
                    Ok(new_segment) => {
                        if new_segment.is_empty() {
                            return Ok(processed);
                        }

                        let mut consumed_bytes: u64 = 0;

                        for line in new_segment.split_inclusive('\n') {
                            if !line.ends_with('\n') && new_segment.ends_with(line) {
                                break;
                            }

                            let trimmed = line.trim_end_matches('\n');
                            consumed_bytes += line.len() as u64;

                            if trimmed.is_empty() {
                                continue;
                            }

                            match process_text_history_line(
                                self,
                                trimmed,
                                line_number,
                                pending_timestamp,
                                recent_hashes,
                            )
                            .await
                            {
                                Ok(true) => {
                                    processed += 1;
                                }
                                Ok(false) => {}
                                Err(e) => {
                                    self.record_error("process_command", &e.to_string());
                                    warn!(
                                        "Failed to process history entry from {}: {}",
                                        self.path, e
                                    );
                                }
                            };
                        }

                        if consumed_bytes > 0 {
                            *offset_bytes = offset_bytes.saturating_add(consumed_bytes);
                            if persist_state {
                                self.persist_state(
                                    *offset_bytes,
                                    *line_number,
                                    *pending_timestamp,
                                    recent_hashes,
                                )
                                .await?;
                            }
                        }
                    }
                    Err(e) => {
                        self.record_error("read_history_segment", &e.to_string());
                        warn!("History watcher unable to read {}: {}", self.path, e);
                    }
                }
            }
            Err(e) => {
                self.record_error("stat_history_file", &e.to_string());
                warn!("History watcher unable to stat {}: {}", self.path, e);
            }
        }

        self.record_poll(poll_started_at, file_size, processed);
        Ok(processed)
    }

    async fn poll_fish_history_once(
        &self,
        sqlite_row_id: &mut i64,
        recent_hashes: &mut VecDeque<u64>,
        persist_state: bool,
    ) -> NodeResult<usize> {
        use crate::fish_history;

        let poll_started_at = Instant::now();
        let file_size = match self.history_file_size().await {
            Ok(size) => size,
            Err(error) => {
                self.record_error("stat_history_file", &error.to_string());
                warn!("Fish history watcher unable to stat {}: {}", self.path, error);
                self.record_poll(poll_started_at, 0, 0);
                return Ok(0);
            }
        };

        let processed = match import_sqlite_history_lenient(
            *sqlite_row_id,
            None,
            |from_row_id, end_time| fish_history::read_fish_history(&self.path, from_row_id, end_time),
            |entry| entry.row_id,
            |entry| {
                let prepared = match sqlite_row_id_to_line_number(self, entry.row_id) {
                    Ok(line_number) => prepare_command_for_capture(
                        self,
                        &entry.command,
                        line_number,
                        Some(recent_hashes),
                    ),
                    Err(error) => {
                        self.record_error("process_fish_entry", &error.to_string());
                        warn!(
                            "Failed to process Fish history entry from {}: {}",
                            self.path, error
                        );
                        Ok(None)
                    }
                };
                async move {
                    let Some(final_command) = prepared
                        .map_err(|error| {
                            self.record_error("process_fish_entry", &error.to_string());
                            warn!(
                                "Failed to process Fish history entry from {}: {}",
                                self.path, error
                            );
                        })?
                    else {
                        return Ok(SqliteHistoryRowOutcome::Skipped);
                    };

                    emit_prepared_fish_entry(self, &entry, final_command)
                        .await
                        .map(|()| SqliteHistoryRowOutcome::Processed)
                        .map_err(|error| {
                            self.record_error("process_fish_entry", &error.to_string());
                            warn!(
                                "Failed to process Fish history entry from {}: {}",
                                self.path, error
                            );
                        })
                }
            },
        )
        .await
        {
            Ok(report) => {
                if report.last_row_id > *sqlite_row_id {
                    *sqlite_row_id = report.last_row_id;
                    if persist_state {
                        self.persist_sqlite_state(*sqlite_row_id, recent_hashes)
                            .await?;
                    }
                }
                report.processed_rows
            }
            Err(error) => {
                self.record_error("read_fish_history", &error.to_string());
                warn!("Fish history watcher unable to read {}: {}", self.path, error);
                0
            }
        };

        self.record_poll(poll_started_at, file_size, processed);
        Ok(processed)
    }

    async fn poll_atuin_history_once(
        &self,
        sqlite_row_id: &mut i64,
        recent_hashes: &mut VecDeque<u64>,
        persist_state: bool,
    ) -> NodeResult<usize> {
        use crate::atuin_history;

        let poll_started_at = Instant::now();
        let file_size = match self.history_file_size().await {
            Ok(size) => size,
            Err(error) => {
                self.record_error("stat_history_file", &error.to_string());
                warn!("Atuin history watcher unable to stat {}: {}", self.path, error);
                self.record_poll(poll_started_at, 0, 0);
                return Ok(0);
            }
        };

        let processed = match import_sqlite_history_lenient(
            *sqlite_row_id,
            None,
            |from_row_id, end_time| {
                atuin_history::read_atuin_history(&self.path, from_row_id, end_time)
            },
            |entry| entry.row_id,
            |entry| {
                let prepared = match sqlite_row_id_to_line_number(self, entry.row_id) {
                    Ok(line_number) => {
                        prepare_command_for_capture(self, &entry.command, line_number, None)
                    }
                    Err(error) => {
                        self.record_error("process_atuin_entry", &error.to_string());
                        warn!(
                            "Failed to process Atuin history entry from {}: {}",
                            self.path, error
                        );
                        Ok(None)
                    }
                };
                async move {
                    let Some(final_command) = prepared
                        .map_err(|error| {
                            self.record_error("process_atuin_entry", &error.to_string());
                            warn!(
                                "Failed to process Atuin history entry from {}: {}",
                                self.path, error
                            );
                        })?
                    else {
                        return Ok(SqliteHistoryRowOutcome::Skipped);
                    };

                    emit_prepared_atuin_entry(self, &entry, final_command)
                        .await
                        .map(|()| SqliteHistoryRowOutcome::Processed)
                        .map_err(|error| {
                            self.record_error("process_atuin_entry", &error.to_string());
                            warn!(
                                "Failed to process Atuin history entry from {}: {}",
                                self.path, error
                            );
                        })
                }
            },
        )
        .await
        {
            Ok(report) => {
                if report.last_row_id > *sqlite_row_id {
                    *sqlite_row_id = report.last_row_id;
                    if persist_state {
                        self.persist_sqlite_state(*sqlite_row_id, recent_hashes)
                            .await?;
                    }
                }
                report.processed_rows
            }
            Err(error) => {
                self.record_error("read_atuin_history", &error.to_string());
                warn!("Atuin history watcher unable to read {}: {}", self.path, error);
                0
            }
        };

        self.record_poll(poll_started_at, file_size, processed);
        Ok(processed)
    }
}

fn prepare_command_for_capture(
    ctx: &HistoryWatcherContext,
    command: &str,
    line_number: u64,
    recent_hashes: Option<&mut VecDeque<u64>>,
) -> NodeResult<Option<String>> {
    if command.contains('\0') {
        ctx.metrics.record_skip(
            &ctx.shell,
            &ctx.path,
            "binary",
            line_number,
            Some(command.len()),
        );
        warn!(
            path = %ctx.path,
            line_number,
            "Skipping command with null bytes (binary data)"
        );
        return Ok(None);
    }

    let has_binary = command
        .chars()
        .any(|c| c.is_control() && c != '\t' && c != '\n' && c != '\r');
    if has_binary {
        ctx.metrics.record_skip(
            &ctx.shell,
            &ctx.path,
            "binary",
            line_number,
            Some(command.len()),
        );
        warn!(
            path = %ctx.path,
            line_number,
            "Skipping command with binary/control characters"
        );
        return Ok(None);
    }

    if let Some(recent_hashes) = recent_hashes {
        use std::hash::{Hash, Hasher};

        let command_hash = {
            let mut hasher = std::hash::DefaultHasher::new();
            command.hash(&mut hasher);
            hasher.finish()
        };

        if recent_hashes.contains(&command_hash) {
            ctx.metrics.record_skip(
                &ctx.shell,
                &ctx.path,
                "duplicate",
                line_number,
                Some(command.len()),
            );
            debug!(
                path = %ctx.path,
                line_number,
                "Skipping duplicate command (hash match)"
            );
            return Ok(None);
        }

        if recent_hashes.len() >= DEDUP_HASH_CAPACITY {
            recent_hashes.pop_front();
        }
        recent_hashes.push_back(command_hash);
    }

    let processed = privacy::engine().process(command, ProcessingContext::Command);
    if processed.any_matched() {
        tracing::info!(
            rules = ?processed.matched_rules,
            path = %ctx.path,
            "Privacy rules matched in command"
        );
    }
    let final_command = processed.text.into_owned();

    if final_command.len() as u64 > ctx.max_capture_bytes.as_u64() {
        ctx.metrics.record_skip(
            &ctx.shell,
            &ctx.path,
            "too_large",
            line_number,
            Some(final_command.len()),
        );
        warn!(
            "Skipping command exceeding capture limit ({} bytes > {} limit)",
            final_command.len(),
            ctx.max_capture_bytes.as_u64()
        );
        return Ok(None);
    }

    Ok(Some(final_command))
}

async fn record_processed_command_for_test(ctx: &HistoryWatcherContext, command: &str) {
    if let Some(commands) = &ctx.processed_commands {
        commands.lock().await.push(command.to_string());
    }
}

async fn stage_history_material(
    ctx: &HistoryWatcherContext,
    material_bytes: &[u8],
    error_context: &str,
) -> NodeResult<Uuid> {
    stage_material(
        ctx.acquisition.as_ref(),
        ctx.path.as_str(),
        material_bytes,
        MATERIAL_REASON_HISTORY,
        None,
    )
    .await
    .map_err(|error| SinexError::service(error_context).with_source(error))
}

fn build_material_json_event<P: EventPayload>(
    payload: P,
    material_id: Uuid,
    material_len: usize,
    build_error_context: &str,
    encode_error_context: &str,
) -> NodeResult<sinex_primitives::events::Event<serde_json::Value>> {
    payload
        .from_material(material_id)
        .with_offset_start(0)
        .map_err(|error| SinexError::service(build_error_context).with_source(error))?
        .with_offset_end(material_len as i64)
        .map_err(|error| SinexError::service(build_error_context).with_source(error))?
        .build()
        .map_err(|error| SinexError::service(build_error_context).with_source(error))?
        .to_json_event()
        .map_err(|error| SinexError::serialization(encode_error_context).with_source(error))
}

async fn emit_history_event(
    ctx: &HistoryWatcherContext,
    event: sinex_primitives::events::Event<serde_json::Value>,
    material_id: Uuid,
    material_len: usize,
    emit_error_context: &str,
    line_number: u64,
) -> NodeResult<()> {
    ctx.stage_context
        .emit_event_with_provenance(event, material_id, Some(0), Some(material_len as i64))
        .await
        .map(|_| ())
        .map_err(|error| SinexError::messaging(emit_error_context).with_source(error))?;

    ctx.metrics
        .record_command(&ctx.shell, &ctx.path, material_len, line_number);

    Ok(())
}

async fn process_command(
    ctx: &HistoryWatcherContext,
    command: &str,
    timestamp: Option<Timestamp>,
    line_number: u64,
    recent_hashes: &mut VecDeque<u64>,
) -> NodeResult<()> {
    let Some(final_command) =
        prepare_command_for_capture(ctx, command, line_number, Some(recent_hashes))?
    else {
        return Ok(());
    };
    let material_bytes = final_command.as_bytes().to_vec();

    record_processed_command_for_test(ctx, &final_command).await;

    let material_id = stage_history_material(
        ctx,
        &material_bytes,
        "Failed to stage terminal history material",
    )
    .await?;

    let payload = HistoryCommandImportedPayload {
        command: final_command,
        timestamp,
        shell_type: ctx.shell.clone(),
        source_file: ctx.path.to_string(),
        line_number: Some(payload_line_number(ctx, line_number)?),
    };

    let event = build_material_json_event(
        payload,
        material_id,
        material_bytes.len(),
        "Failed to build terminal history event",
        "Failed to convert terminal history event to JSON",
    )?;

    emit_history_event(
        ctx,
        event,
        material_id,
        material_bytes.len(),
        "Failed to emit terminal event",
        line_number,
    )
    .await
}

fn sqlite_row_id_to_line_number(ctx: &HistoryWatcherContext, row_id: i64) -> NodeResult<u64> {
    u64::try_from(row_id).map_err(|error| {
        SinexError::processing("history entry has invalid negative sqlite row id")
            .with_context("shell", ctx.shell.clone())
            .with_context("path", ctx.path.to_string())
            .with_context("row_id", row_id.to_string())
            .with_std_error(&error)
    })
}

fn payload_line_number(ctx: &HistoryWatcherContext, line_number: u64) -> NodeResult<u32> {
    u32::try_from(line_number).map_err(|error| {
        SinexError::processing("history line number exceeds payload range")
            .with_context("shell", ctx.shell.clone())
            .with_context("path", ctx.path.to_string())
            .with_context("line_number", line_number.to_string())
            .with_std_error(&error)
    })
}

enum TextHistoryLine<'a> {
    TimestampMarker(Timestamp),
    Command {
        command: &'a str,
        timestamp: Option<Timestamp>,
    },
    MalformedMetadata {
        kind: &'static str,
        raw_line: &'a str,
    },
}

fn parse_text_history_line<'a>(shell: &str, line: &'a str) -> TextHistoryLine<'a> {
    if shell == "bash"
        && let Some(raw) = line.strip_prefix('#')
        && raw.chars().all(|ch| ch.is_ascii_digit())
    {
        return match raw.parse::<i64>().ok().and_then(Timestamp::from_unix_timestamp) {
            Some(timestamp) => TextHistoryLine::TimestampMarker(timestamp),
            None => TextHistoryLine::MalformedMetadata {
                kind: "bash timestamp marker",
                raw_line: line,
            },
        };
    }

    if shell == "zsh"
        && let Some(history) = line.strip_prefix(": ")
        && let Some((timestamp, remainder)) = history.split_once(':')
        && let Some((_, command)) = remainder.split_once(';')
    {
        return match timestamp.parse::<i64>().ok().and_then(Timestamp::from_unix_timestamp) {
            Some(timestamp) => TextHistoryLine::Command {
                command,
                timestamp: Some(timestamp),
            },
            None => TextHistoryLine::MalformedMetadata {
                kind: "zsh extended history timestamp",
                raw_line: line,
            },
        };
    }

    TextHistoryLine::Command {
        command: line,
        timestamp: None,
    }
}

async fn process_text_history_line(
    ctx: &HistoryWatcherContext,
    line: &str,
    line_number: &mut u64,
    pending_timestamp: &mut Option<Timestamp>,
    recent_hashes: &mut VecDeque<u64>,
) -> NodeResult<bool> {
    match parse_text_history_line(&ctx.shell, line) {
        TextHistoryLine::TimestampMarker(timestamp) => {
            *pending_timestamp = Some(timestamp);
            Ok(false)
        }
        TextHistoryLine::Command { command, timestamp } => {
            *line_number += 1;
            process_command(
                ctx,
                command,
                timestamp.or_else(|| pending_timestamp.take()),
                *line_number,
                recent_hashes,
            )
            .await?;
            Ok(true)
        }
        TextHistoryLine::MalformedMetadata { kind, raw_line } => Err(
            SinexError::processing("malformed terminal history metadata line")
                .with_context("shell", ctx.shell.clone())
                .with_context("path", ctx.path.to_string())
                .with_context("metadata_kind", kind.to_string())
                .with_context("line_preview", raw_line.chars().take(120).collect::<String>()),
        ),
    }
}

async fn emit_prepared_fish_entry(
    ctx: &HistoryWatcherContext,
    entry: &crate::fish_history::FishHistoryEntry,
    final_command: String,
) -> NodeResult<()> {
    let line_number = sqlite_row_id_to_line_number(ctx, entry.row_id)?;
    let material_bytes = final_command.as_bytes().to_vec();
    let timestamp = match entry.when {
        Some(raw_timestamp) => {
            let Some(timestamp) = Timestamp::from_unix_timestamp(raw_timestamp) else {
                warn!(
                    row_id = entry.row_id,
                    timestamp = raw_timestamp,
                    "Rejecting Fish row with invalid timestamp"
                );
                return Err(
                    SinexError::validation(format!("Fish row {} has invalid timestamp", entry.row_id))
                        .with_context("timestamp", raw_timestamp.to_string()),
                );
            };
            Some(timestamp)
        }
        None => None,
    };

    record_processed_command_for_test(ctx, &final_command).await;

    let material_id = stage_history_material(
        ctx,
        &material_bytes,
        "Failed to stage Fish history material",
    )
    .await?;

    let payload = HistoryCommandImportedPayload {
        command: final_command,
        timestamp,
        shell_type: ctx.shell.clone(),
        source_file: ctx.path.to_string(),
        line_number: Some(payload_line_number(ctx, line_number)?),
    };

    let event = build_material_json_event(
        payload,
        material_id,
        material_bytes.len(),
        "Failed to build Fish history event",
        "Failed to convert Fish event to JSON",
    )?;

    emit_history_event(
        ctx,
        event,
        material_id,
        material_bytes.len(),
        "Failed to emit Fish history event",
        line_number,
    )
    .await
}

#[cfg(test)]
async fn process_atuin_entry(
    ctx: &HistoryWatcherContext,
    entry: &crate::atuin_history::AtuinHistoryEntry,
    _recent_hashes: &mut VecDeque<u64>,
) -> NodeResult<()> {
    let line_number = sqlite_row_id_to_line_number(ctx, entry.row_id)?;
    let Some(final_command) = prepare_command_for_capture(ctx, &entry.command, line_number, None)?
    else {
        return Ok(());
    };
    emit_prepared_atuin_entry(ctx, entry, final_command).await
}

async fn emit_prepared_atuin_entry(
    ctx: &HistoryWatcherContext,
    entry: &crate::atuin_history::AtuinHistoryEntry,
    final_command: String,
) -> NodeResult<()> {
    let line_number = sqlite_row_id_to_line_number(ctx, entry.row_id)?;
    let payload = match AtuinCommandExecutedPayload::from_raw_history(
        final_command.clone(),
        RecordedPath::from(entry.cwd.clone()),
        entry.exit_code,
        entry.duration_ns,
        entry.history_id.clone(),
        entry.session_id.clone(),
        entry.timestamp_ns,
        entry.hostname.clone(),
    ) {
        Ok(payload) => payload,
        Err(error) => {
            warn!(
                row_id = entry.row_id,
                error = %error,
                "Rejecting Atuin row with invalid timestamp or duration"
            );
            return Err(SinexError::validation(format!(
                "Atuin row {} has invalid timestamp or duration",
                entry.row_id
            ))
            .with_source(error));
        }
    };
    let material_bytes = final_command.as_bytes().to_vec();

    record_processed_command_for_test(ctx, &final_command).await;

    let material_id = stage_history_material(
        ctx,
        &material_bytes,
        "Failed to stage Atuin history material",
    )
    .await?;

    let event = build_material_json_event(
        payload,
        material_id,
        material_bytes.len(),
        "Failed to build Atuin event",
        "Failed to convert Atuin event to JSON",
    )?;

    emit_history_event(
        ctx,
        event,
        material_id,
        material_bytes.len(),
        "Failed to emit Atuin event",
        line_number,
    )
    .await
}

/// Terminal node that monitors history files.
pub struct TerminalNode {
    config: TerminalConfig,
    stage_context: Option<StageAsYouGoContext>,
    watch_handles: Arc<Mutex<Vec<tokio::task::JoinHandle<NodeResult<()>>>>>,
    state_dir: Option<PathBuf>,
    metrics: Arc<TerminalMetrics>,
    runtime: Option<NodeRuntimeState>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TerminalCheckpoint {}

impl TerminalNode {
    fn validate_checkpoint_state(key: &str, state: HistoryState) -> NodeResult<HistoryState> {
        if let Some(sqlite_row_id) = state.sqlite_row_id
            && sqlite_row_id < 0
        {
            return Err(SinexError::checkpoint(
                "terminal history checkpoint has invalid negative sqlite_row_id",
            )
            .with_context("source", key.to_string())
            .with_context("sqlite_row_id", sqlite_row_id.to_string()));
        }
        Ok(state)
    }

    fn validate_checkpoint_state_for_source_mode(
        key: &str,
        state: HistoryState,
        source_mode: &HistorySourceMode,
    ) -> NodeResult<HistoryState> {
        if matches!(
            source_mode,
            HistorySourceMode::AtuinSqlite | HistorySourceMode::FishSqlite
        ) && state.sqlite_row_id.is_none()
        {
            return Err(SinexError::checkpoint(
                "terminal history checkpoint missing sqlite_row_id for SQLite-backed source",
            )
            .with_context("source", key.to_string()));
        }

        Self::validate_checkpoint_state(key, state)
    }

    #[must_use]
    pub fn new() -> Self {
        Self {
            config: TerminalConfig::default(),
            stage_context: None,
            watch_handles: Arc::new(Mutex::new(Vec::new())),
            state_dir: None,
            metrics: TerminalMetrics::new(),
            runtime: None,
        }
    }

    #[must_use]
    pub fn with_config(config: TerminalConfig) -> Self {
        Self {
            config,
            stage_context: None,
            watch_handles: Arc::new(Mutex::new(Vec::new())),
            state_dir: None,
            metrics: TerminalMetrics::new(),
            runtime: None,
        }
    }

    fn collapse_shutdown_errors(mut errors: Vec<SinexError>) -> NodeResult<()> {
        if errors.is_empty() {
            return Ok(());
        }

        let mut error = errors.remove(0);
        for (index, extra) in errors.into_iter().enumerate() {
            error = error.with_context(
                format!("additional_shutdown_error_{}", index + 1),
                extra.to_string(),
            );
        }
        Err(error)
    }

    #[must_use]
    pub fn config(&self) -> &TerminalConfig {
        &self.config
    }

    fn runtime(&self) -> NodeResult<&NodeRuntimeState> {
        self.runtime.as_ref().ok_or_else(|| {
            SinexError::invalid_state(
                "Terminal node runtime not initialized prior to scan".to_string(),
            )
        })
    }

    #[allow(dead_code)] // Used by runtime initialization
    fn service_info(&self) -> NodeResult<&ServiceInfo> {
        Ok(self.runtime()?.service_info())
    }

    #[allow(dead_code)] // Used by runtime initialization
    async fn initialise_from_runtime(
        &mut self,
        config: TerminalConfig,
        runtime: NodeRuntimeState,
    ) -> NodeResult<()> {
        let service_info = runtime.service_info();
        info!(
            node = self.name(),
            service = %service_info.service_name(),
            "Initialising terminal node"
        );

        config.validate_config()?;

        let publisher = match runtime.transport() {
            sinex_node_sdk::EventTransport::Nats(publisher) => publisher.clone(),
        };

        AcquisitionManager::bootstrap_streams(publisher.nats_client()).await?;

        let mut state_dir = service_info.work_dir().clone();
        state_dir.push("terminal-history");

        if let Err(e) = fs::create_dir_all(&state_dir).await {
            return Err(SinexError::io(format!(
                "Failed to create terminal state directory {}: {}",
                state_dir.display(),
                e
            )));
        }

        self.state_dir = Some(state_dir);
        self.stage_context = Some(StageAsYouGoContext::from_runtime(&runtime));
        self.runtime = Some(runtime);
        self.config = config;
        self.metrics = TerminalMetrics::new();
        self.watch_handles = Arc::new(Mutex::new(Vec::new()));
        // shutdown_tx removed

        Ok(())
    }

    fn build_history_contexts(
        &self,
        shutdown_rx: watch::Receiver<bool>,
    ) -> NodeResult<Vec<HistoryWatcherContext>> {
        let runtime = self.runtime()?;

        let stage = self.stage_context.clone().ok_or_else(|| {
            SinexError::invalid_state("Stage context not initialized".to_string())
        })?;

        let state_dir = self.state_dir.clone();
        let mut contexts = Vec::new();
        for source in &self.config.history_sources {
            let acquisition = Arc::new(runtime.acquisition_manager(
                RotationPolicy::default(),
                "terminal-history",
                source.path.to_string(),
            )?);

            let state_path = state_dir.as_ref().map(|dir| {
                let hash = blake3::hash(source.path.as_str().as_bytes())
                    .to_hex()
                    .to_string();
                dir.join(format!("{hash}.json"))
            });

            let stage_context = stage
                .clone()
                .with_acquisition_manager(Arc::clone(&acquisition));

            let source_mode = classify_history_source(source);

            contexts.push(HistoryWatcherContext {
                acquisition,
                stage_context,
                metrics: Arc::clone(&self.metrics),
                shell: source.shell.clone(),
                path: source.path.clone(),
                max_capture_bytes: self.config.max_capture_bytes,
                polling_interval: Duration::from_secs(self.config.polling_interval_secs.as_secs()),
                state_path,
                shutdown_rx: shutdown_rx.clone(),
                processed_commands: None,
                source_mode,
                initial_state_override: None,
            });
        }

        Ok(contexts)
    }

    fn incoming_checkpoint_state_for_source(
        checkpoint: &Checkpoint,
        key: &str,
    ) -> NodeResult<IncomingHistoryCheckpointState> {
        let position = match checkpoint {
            Checkpoint::None => return Ok(IncomingHistoryCheckpointState::MissingCheckpoint),
            Checkpoint::External { position, .. } => position,
            _ => {
                return Err(
                    SinexError::checkpoint("terminal history requires an external per-source checkpoint")
                        .with_context("checkpoint", checkpoint.description()),
                );
            }
        };

        let checkpoint: TerminalHistoryCheckpoint = serde_json::from_value(position.clone())
            .map_err(|error| {
                SinexError::serialization("failed to parse terminal history checkpoint state")
                    .with_std_error(&error)
            })?;

        checkpoint
            .sources
            .get(key)
            .cloned()
            .map(|state| Self::validate_checkpoint_state(key, state))
            .transpose()
            .map(|state| match state {
                Some(state) => IncomingHistoryCheckpointState::State(state),
                None => IncomingHistoryCheckpointState::MissingSource,
            })
    }

    fn incoming_checkpoint_state_for_source_mode(
        checkpoint: &Checkpoint,
        key: &str,
        source_mode: &HistorySourceMode,
    ) -> NodeResult<IncomingHistoryCheckpointState> {
        match Self::incoming_checkpoint_state_for_source(checkpoint, key)? {
            IncomingHistoryCheckpointState::State(state) => Ok(IncomingHistoryCheckpointState::State(
                Self::validate_checkpoint_state_for_source_mode(key, state, source_mode)?,
            )),
            IncomingHistoryCheckpointState::MissingCheckpoint => {
                Ok(IncomingHistoryCheckpointState::MissingCheckpoint)
            }
            IncomingHistoryCheckpointState::MissingSource => Ok(IncomingHistoryCheckpointState::MissingSource),
        }
    }

    #[cfg(test)]
    fn checkpoint_state_for_source(
        checkpoint: &Checkpoint,
        key: &str,
    ) -> NodeResult<Option<HistoryState>> {
        match Self::incoming_checkpoint_state_for_source(checkpoint, key)? {
            IncomingHistoryCheckpointState::MissingCheckpoint
            | IncomingHistoryCheckpointState::MissingSource => Ok(None),
            IncomingHistoryCheckpointState::State(state) => Ok(Some(state)),
        }
    }

    #[cfg(test)]
    fn checkpoint_state_for_source_mode(
        checkpoint: &Checkpoint,
        key: &str,
        source_mode: &HistorySourceMode,
    ) -> NodeResult<Option<HistoryState>> {
        match Self::incoming_checkpoint_state_for_source_mode(checkpoint, key, source_mode)? {
            IncomingHistoryCheckpointState::MissingCheckpoint
            | IncomingHistoryCheckpointState::MissingSource => Ok(None),
            IncomingHistoryCheckpointState::State(state) => Ok(Some(state)),
        }
    }

    fn checkpoint_from_states(states: HashMap<String, HistoryState>) -> NodeResult<Checkpoint> {
        let validated_states = states
            .into_iter()
            .map(|(key, state)| Self::validate_checkpoint_state(&key, state).map(|state| (key, state)))
            .collect::<NodeResult<HashMap<_, _>>>()?;
        let position = serde_json::to_value(TerminalHistoryCheckpoint {
            sources: validated_states,
        })
            .map_err(|error| {
                SinexError::serialization("failed to encode terminal history checkpoint state")
                    .with_std_error(&error)
            })?;
        Ok(Checkpoint::external(
            position,
            "terminal history source progress",
        ))
    }

    fn checkpoint_timestamp(checkpoint: &Checkpoint) -> Option<Timestamp> {
        match checkpoint {
            Checkpoint::Timestamp { timestamp, .. } => Some(*timestamp),
            _ => None,
        }
    }

    async fn preserve_checkpoint_state_after_failure(
        from: &Checkpoint,
        context: &HistoryWatcherContext,
        warnings: &mut Vec<String>,
    ) -> NodeResult<HistoryState> {
        let checkpoint_key = context.checkpoint_key();
        match Self::incoming_checkpoint_state_for_source_mode(
            from,
            &checkpoint_key,
            &context.source_mode,
        ) {
            Ok(IncomingHistoryCheckpointState::State(state)) => Ok(state),
            Ok(IncomingHistoryCheckpointState::MissingSource) => Ok(context.empty_state()),
            Ok(IncomingHistoryCheckpointState::MissingCheckpoint) => context
                .load_state()
                .await?
                .map(|state| context.validate_state(state))
                .transpose()?
                .map_or_else(|| Ok(context.empty_state()), Ok),
            Err(error) => {
                warnings.push(context.strict_warning(format!(
                    "incoming checkpoint state is unusable for continuous monitoring: {error}"
                )));
                Err(
                    SinexError::processing(
                        "failed to restore incoming terminal checkpoint state for continuous monitoring",
                    )
                    .with_context("source", checkpoint_key)
                    .with_source(error),
                )
            }
        }
    }
}

impl Default for TerminalNode {
    fn default() -> Self {
        Self::new()
    }
}

enum IncomingHistoryCheckpointState {
    MissingCheckpoint,
    MissingSource,
    State(HistoryState),
}

impl IngestorNode for TerminalNode {
    type Config = TerminalConfig;
    type State = TerminalCheckpoint;

    fn name(&self) -> &'static str {
        "terminal-watcher"
    }

    async fn initialize(
        &mut self,
        config: Self::Config,
        runtime: &NodeRuntimeState,
        _state: &mut Self::State,
    ) -> NodeResult<()> {
        let service_info = runtime.service_info();
        config.validate_config().map_err(|e| {
            SinexError::configuration("Terminal configuration validation failed").with_source(e)
        })?;

        let publisher = match runtime.transport() {
            sinex_node_sdk::EventTransport::Nats(publisher) => publisher.clone(),
        };

        AcquisitionManager::bootstrap_streams(publisher.nats_client()).await?;

        let mut state_dir = service_info.work_dir().clone();
        state_dir.push("terminal-history");

        if let Err(e) = fs::create_dir_all(&state_dir).await {
            return Err(SinexError::io(format!(
                "Failed to create terminal state directory {}: {}",
                state_dir.display(),
                e
            )));
        }

        self.state_dir = Some(state_dir);
        self.stage_context = Some(StageAsYouGoContext::from_runtime(runtime));
        self.config = config;
        self.metrics = TerminalMetrics::new();
        self.runtime = Some(runtime.clone());

        Ok(())
    }

    async fn scan_snapshot(
        &mut self,
        _state: &mut Self::State,
        _args: ScanArgs,
    ) -> NodeResult<ScanReport> {
        let started_at = Timestamp::now();
        let start_time = Instant::now();
        let monitored: Vec<Utf8PathBuf> = self
            .config
            .history_sources
            .iter()
            .map(|src| src.path.clone())
            .collect();
        let finished_at = Timestamp::now();

        debug!(monitored = monitored.len(), "Terminal snapshot captured");

        Ok(ScanReport {
            events_processed: 0,
            duration: start_time.elapsed(),
            final_checkpoint: Checkpoint::timestamp(finished_at, None),
            time_range: Some((started_at, finished_at)),
            node_stats: HashMap::new(),
            successful_targets: vec!["snapshot".to_string()],
            failed_targets: Vec::new(),
            warnings: Vec::new(),
        })
    }

    async fn scan_historical(
        &mut self,
        _state: &mut Self::State,
        from: Checkpoint,
        until: TimeHorizon,
        _args: ScanArgs,
    ) -> NodeResult<ScanReport> {
        let started_at = Instant::now();
        let (_, shutdown_rx) = watch::channel(false);
        let contexts = self.build_history_contexts(shutdown_rx)?;
        let mut events_processed = 0u64;
        let mut checkpoint_states = HashMap::new();
        let mut successful_targets = Vec::new();
        let mut failed_targets = Vec::new();
        let mut warnings = Vec::new();

        for ctx in contexts {
            let checkpoint_key = ctx.checkpoint_key();
            let state_override = match Self::incoming_checkpoint_state_for_source_mode(
                &from,
                &checkpoint_key,
                &ctx.source_mode,
            ) {
                Ok(IncomingHistoryCheckpointState::State(state)) => Some(state),
                Ok(IncomingHistoryCheckpointState::MissingCheckpoint) => {
                    if matches!(ctx.source_mode, HistorySourceMode::ConfiguredError(_)) {
                        None
                    } else {
                        Some(ctx.empty_state())
                    }
                }
                Ok(IncomingHistoryCheckpointState::MissingSource) => Some(ctx.empty_state()),
                Err(error) => {
                    warnings.push(ctx.strict_warning(format!(
                        "incoming checkpoint state is unusable for historical replay: {error}"
                    )));
                    warn!(
                        source = %checkpoint_key,
                        error = %error,
                        "Historical terminal scan refused unusable incoming checkpoint state"
                    );
                    failed_targets.push((
                        checkpoint_key.clone(),
                        format!("failed to restore incoming terminal checkpoint state: {error}"),
                    ));
                    let preserved_state = ctx.load_state().await.map_err(|load_error| {
                        SinexError::processing(
                            "failed to preserve local terminal state after checkpoint restore failure",
                        )
                        .with_context("source", checkpoint_key.clone())
                        .with_source(load_error)
                    })?;
                    checkpoint_states.insert(
                        checkpoint_key,
                        preserved_state
                            .map(|state| ctx.validate_state(state))
                            .transpose()?
                            .unwrap_or_else(|| ctx.empty_state()),
                    );
                    continue;
                }
            };
            let outcome = ctx
                .scan_history_once_from_state(state_override, until.end_time())
                .await;
            events_processed = events_processed.saturating_add(outcome.processed as u64);
            warnings.extend(outcome.warnings);
            if let Some(error) = outcome.failure {
                failed_targets.push((checkpoint_key.clone(), error));
            } else {
                successful_targets.push(checkpoint_key.clone());
            }
            checkpoint_states.insert(checkpoint_key, ctx.validate_state(outcome.state)?);
        }

        Ok(ScanReport {
            events_processed,
            duration: started_at.elapsed(),
            final_checkpoint: Self::checkpoint_from_states(checkpoint_states)?,
            time_range: Self::checkpoint_timestamp(&from)
                .zip(until.end_time())
                .map(|(started_at, finished_at)| (started_at, finished_at)),
            node_stats: HashMap::new(),
            successful_targets,
            failed_targets,
            warnings,
        })
    }

    async fn run_continuous(
        &mut self,
        _state: &mut Self::State,
        from: Checkpoint,
        shutdown_rx: watch::Receiver<bool>,
    ) -> NodeResult<ScanReport> {
        let started_at = Timestamp::now();
        let start_time = Instant::now();
        let contexts = self.build_history_contexts(shutdown_rx.clone())?;
        let mut successful_targets = Vec::new();
        let mut failed_targets = Vec::new();
        let mut warnings = Vec::new();
        let mut checkpoint_states = HashMap::new();
        let mut monitored_contexts = Vec::new();

        let mut guard = self.watch_handles.lock().await;
        for mut watch_ctx in contexts {
            let checkpoint_key = watch_ctx.checkpoint_key();
            if let HistorySourceMode::ConfiguredError(error) = &watch_ctx.source_mode {
                failed_targets.push((checkpoint_key.clone(), error.clone()));
                warnings.push(watch_ctx.strict_warning(
                    "configured source will not be monitored until its SQLite database is repaired",
                ));
                checkpoint_states.insert(
                    checkpoint_key,
                    Self::preserve_checkpoint_state_after_failure(&from, &watch_ctx, &mut warnings)
                        .await?,
                );
            } else {
                let state_override = match Self::incoming_checkpoint_state_for_source_mode(
                    &from,
                    &checkpoint_key,
                    &watch_ctx.source_mode,
                ) {
                    Ok(IncomingHistoryCheckpointState::State(state)) => Some(state),
                    Ok(IncomingHistoryCheckpointState::MissingCheckpoint) => None,
                    Ok(IncomingHistoryCheckpointState::MissingSource) => {
                        Some(watch_ctx.empty_state())
                    }
                    Err(error) => {
                        warnings.push(watch_ctx.strict_warning(format!(
                            "incoming checkpoint state is unusable for continuous monitoring: {error}"
                        )));
                        failed_targets.push((
                            checkpoint_key.clone(),
                            format!("failed to restore incoming terminal checkpoint state: {error}"),
                        ));
                        checkpoint_states.insert(
                            checkpoint_key,
                            watch_ctx
                                .load_state()
                                .await?
                                .map(|state| watch_ctx.validate_state(state))
                                .transpose()?
                                .unwrap_or_else(|| watch_ctx.empty_state()),
                        );
                        continue;
                    }
                };
                watch_ctx.initial_state_override = state_override;
                monitored_contexts.push(watch_ctx.clone());
                let handle = tokio::spawn(watch_ctx.clone().monitor());
                guard.push(handle);
            }
        }

        if monitored_contexts.is_empty() && !failed_targets.is_empty() {
            return Err(SinexError::configuration(
                "terminal continuous monitoring has no usable history sources".to_string(),
            )
            .with_context(
                "failed_targets",
                format!("{failed_targets:?}"),
            ));
        }

        info!(
            watches = guard.len(),
            "Terminal watcher monitoring history sources"
        );

        let mut shutdown_rx = shutdown_rx;
        if shutdown_rx.changed().await.is_err() {
            let warning =
                "terminal continuous monitoring shutdown channel dropped before explicit shutdown";
            warn!("{warning}");
            warnings.push(warning.to_string());
        }

        let handles: Vec<_> = guard.drain(..).collect();
        drop(guard);
        for (watch_ctx, handle) in monitored_contexts.iter().zip(handles) {
            let checkpoint_key = watch_ctx.checkpoint_key();
            let fallback_state = match Self::incoming_checkpoint_state_for_source_mode(
                &from,
                &checkpoint_key,
                &watch_ctx.source_mode,
            )? {
                IncomingHistoryCheckpointState::State(state) => state,
                IncomingHistoryCheckpointState::MissingSource => watch_ctx.empty_state(),
                IncomingHistoryCheckpointState::MissingCheckpoint => watch_ctx
                    .load_state()
                    .await?
                    .map(|state| watch_ctx.validate_state(state))
                    .transpose()?
                    .unwrap_or_else(|| watch_ctx.empty_state()),
            };
            match handle.await {
                Ok(Ok(())) => {
                    successful_targets.push(checkpoint_key.clone());
                }
                Ok(Err(error)) => {
                    failed_targets.push((
                        checkpoint_key.clone(),
                        format!("terminal watcher failed during continuous monitoring: {error}"),
                    ));
                }
                Err(error) => {
                    failed_targets.push((
                        checkpoint_key.clone(),
                        format!(
                            "terminal watcher task ended with join error during shutdown: {error}"
                        ),
                    ));
                }
            }
            let final_state = match watch_ctx.load_state().await {
                Ok(Some(state)) => watch_ctx.validate_state(state).map_err(|error| {
                    SinexError::processing(
                        "failed to restore terminal watcher state after continuous monitoring",
                    )
                    .with_context("source", checkpoint_key.clone())
                    .with_source(error)
                })?,
                Ok(None) => fallback_state,
                Err(error) => {
                    return Err(SinexError::processing(
                        "failed to reload terminal watcher state after continuous monitoring",
                    )
                    .with_context("source", checkpoint_key)
                    .with_source(error));
                }
            };
            checkpoint_states.insert(watch_ctx.checkpoint_key(), final_state);
        }
        let finished_at = Timestamp::now();

        Ok(ScanReport {
            events_processed: 0,
            duration: start_time.elapsed(),
            final_checkpoint: Self::checkpoint_from_states(checkpoint_states)?,
            time_range: Some((started_at, finished_at)),
            node_stats: HashMap::new(),
            successful_targets,
            failed_targets,
            warnings,
        })
    }

    async fn shutdown(&mut self, _state: &Self::State) -> NodeResult<()> {
        let mut guard = self.watch_handles.lock().await;
        let handles: Vec<_> = guard.drain(..).collect();
        drop(guard);

        let mut shutdown_errors = Vec::new();
        for (watcher_index, handle) in handles.into_iter().enumerate() {
            match handle.await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    shutdown_errors.push(SinexError::processing(
                        "terminal watcher failed before shutdown completed",
                    )
                    .with_context("watcher_index", watcher_index.to_string())
                    .with_source(error));
                }
                Err(error) => {
                    shutdown_errors.push(SinexError::processing(
                        "terminal watcher task ended with join error during shutdown",
                    )
                    .with_context("watcher_index", watcher_index.to_string())
                    .with_std_error(&error));
                }
            }
        }
        Self::collapse_shutdown_errors(shutdown_errors)?;
        info!("Terminal watcher shutdown complete");
        Ok(())
    }
}

impl ExplorationProvider for TerminalNode {
    fn get_source_state(&self) -> NodeResult<SourceState> {
        let mut usable_sources = 0usize;
        let mut configured_failures = Vec::new();
        for source in &self.config.history_sources {
            match classify_history_source(source) {
                HistorySourceMode::ConfiguredError(error) => {
                    configured_failures.push(json!({
                        "shell": source.shell,
                        "path": source.path,
                        "error": error,
                    }));
                }
                _ => {
                    usable_sources = usable_sources.saturating_add(1);
                }
            }
        }

        let processing_errors = self.metrics.processing_errors.load(Ordering::Relaxed);
        let healthy = usable_sources > 0 && configured_failures.is_empty() && processing_errors == 0;
        let description = if self.config.history_sources.is_empty() {
            "No terminal history sources configured".to_string()
        } else if configured_failures.is_empty() {
            format!(
                "Monitoring {} terminal history sources",
                self.config.history_sources.len()
            )
        } else if usable_sources > 0 {
            format!(
                "Monitoring {usable_sources} usable terminal history sources ({} misconfigured)",
                configured_failures.len()
            )
        } else {
            format!(
                "No usable terminal history sources configured ({} misconfigured)",
                configured_failures.len()
            )
        };

        let mut metadata = self.metrics.metadata();
        metadata.insert("usable_sources".to_string(), json!(usable_sources));
        metadata.insert(
            "misconfigured_sources".to_string(),
            json!(configured_failures),
        );

        Ok(SourceState {
            is_connected: usable_sources > 0,
            healthy,
            description,
            last_updated: Timestamp::now(),
            lag_seconds: None,
            recent_activity: self.metrics.recent_activity(),
            total_items: Some(self.config.history_sources.len() as u64),
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
            "coverage analysis is not implemented for terminal history sources",
        )
    }

    fn export_data(&self, _path: &SanitizedPath, _format: ExportFormat) -> NodeResult<()> {
        Err(SinexError::invalid_state(
            "Terminal watcher does not support data export",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinex_node_sdk::{AcquisitionManager, acquisition_manager::RotationPolicy};
    use sinex_primitives::Id;
    use sinex_primitives::events::Provenance;
    use std::sync::Arc;

    use tokio::{
        io::AsyncWriteExt,
        time::{Duration, timeout},
    };
    use xtask::sandbox::sinex_test;
    use xtask::sandbox::timing::Timeouts;
    use xtask::sandbox::{
        TestIngestdConfig, TestRuntime, TestRuntimeBuilder, prelude::*,
        start_test_ingestd_with_config,
    };

    #[cfg(unix)]
    #[sinex_test]
    async fn history_state_temp_path_preserves_non_utf8_filenames() -> TestResult<()> {
        use std::os::unix::ffi::{OsStrExt, OsStringExt};

        let path = std::path::PathBuf::from("/tmp").join(std::ffi::OsString::from_vec(vec![
            b'h', b'i', b's', b't', 0xff, b'.', b's', b't', b'a', b't', b'e',
        ]));
        let suffix = Uuid::from_u128(0x1234);
        let temp_path = HistoryWatcherContext::history_state_temp_path(&path, suffix);
        let file_name = temp_path.file_name().expect("temp path file name");
        let expected_prefix = [
            b'h', b'i', b's', b't', 0xff, b'.', b's', b't', b'a', b't', b'e', b'.',
        ];

        assert!(
            file_name.as_bytes().starts_with(&expected_prefix),
            "unexpected temp file prefix: {:?}",
            file_name
        );
        assert!(
            file_name.as_bytes().ends_with(b".tmp"),
            "unexpected temp file suffix: {:?}",
            file_name
        );
        Ok(())
    }

    #[sinex_test]
    async fn terminal_config_validation_allows_valid_configuration() -> TestResult<()> {
        let config = TerminalConfig {
            history_sources: vec![HistorySourceConfig {
                path: Utf8PathBuf::from("/tmp/.bash_history"),
                shell: "bash".to_string(),
            }],
            polling_interval_secs: Seconds::from_secs(30),
            max_capture_bytes: Bytes::from_bytes(1024),
        };

        assert!(config.validate_config().is_ok());
        Ok(())
    }

    #[sinex_test]
    async fn terminal_config_validation_rejects_empty_sources() -> TestResult<()> {
        let config = TerminalConfig {
            history_sources: vec![],
            polling_interval_secs: Seconds::from_secs(30),
            max_capture_bytes: Bytes::from_bytes(1024),
        };

        assert!(config.validate_config().is_err());
        Ok(())
    }

    #[sinex_test]
    async fn terminal_config_default_ignores_invalid_polling_interval_override() -> TestResult<()> {
        let previous = std::env::var(ENV_POLLING_INTERVAL).ok();
        unsafe { std::env::set_var(ENV_POLLING_INTERVAL, "not-a-number") };

        let config = TerminalConfig::default();

        match previous {
            Some(value) => unsafe { std::env::set_var(ENV_POLLING_INTERVAL, value) },
            None => unsafe { std::env::remove_var(ENV_POLLING_INTERVAL) },
        }

        assert_eq!(config.polling_interval_secs, DEFAULT_POLLING_INTERVAL);
        Ok(())
    }

    #[sinex_test]
    async fn default_history_sources_do_not_fabricate_tmp_paths_without_home() -> TestResult<()> {
        let sources = default_history_sources(None);
        assert!(sources.is_empty(), "missing home should not fabricate fallback paths");
        Ok(())
    }

    #[sinex_test]
    async fn default_history_sources_follow_home_directory() -> TestResult<()> {
        let home = Utf8PathBuf::from("/home/tester");
        let sources = default_history_sources(Some(&home));
        assert_eq!(sources.len(), 3);
        assert_eq!(sources[0].path, home.join(".bash_history"));
        assert_eq!(sources[1].path, home.join(".zsh_history"));
        assert_eq!(sources[2].path, home.join(".local/share/atuin/history.db"));
        Ok(())
    }

    #[sinex_test]
    async fn parse_text_history_line_preserves_shell_timestamps() -> TestResult<()> {
        match parse_text_history_line("bash", "#1710877544") {
            TextHistoryLine::TimestampMarker(timestamp) => {
                assert_eq!(
                    timestamp,
                    Timestamp::from_unix_timestamp(1_710_877_544).expect("valid timestamp")
                );
            }
            TextHistoryLine::Command { .. } => {
                return Err(color_eyre::eyre::eyre!(
                    "bash marker parsed as command"
                ));
            }
            TextHistoryLine::MalformedMetadata { .. } => {
                return Err(color_eyre::eyre::eyre!(
                    "bash marker parsed as malformed metadata"
                ));
            }
        }

        match parse_text_history_line("zsh", ": 1710877544:0;echo hello") {
            TextHistoryLine::Command { command, timestamp } => {
                assert_eq!(command, "echo hello");
                assert_eq!(
                    timestamp,
                    Timestamp::from_unix_timestamp(1_710_877_544)
                );
            }
            TextHistoryLine::TimestampMarker(_) => {
                return Err(color_eyre::eyre::eyre!(
                    "zsh extended history parsed as marker"
                ));
            }
            TextHistoryLine::MalformedMetadata { .. } => {
                return Err(color_eyre::eyre::eyre!(
                    "zsh extended history parsed as malformed metadata"
                ));
            }
        }

        match parse_text_history_line("bash", "echo plain") {
            TextHistoryLine::Command { command, timestamp } => {
                assert_eq!(command, "echo plain");
                assert!(timestamp.is_none());
            }
            TextHistoryLine::TimestampMarker(_) => {
                return Err(color_eyre::eyre::eyre!(
                    "plain history line parsed as marker"
                ));
            }
            TextHistoryLine::MalformedMetadata { .. } => {
                return Err(color_eyre::eyre::eyre!(
                    "plain history line parsed as malformed metadata"
                ));
            }
        }

        Ok(())
    }

    #[sinex_test]
    async fn parse_text_history_line_rejects_malformed_shell_metadata() -> TestResult<()> {
        match parse_text_history_line("bash", "#999999999999999999999999") {
            TextHistoryLine::MalformedMetadata { kind, raw_line } => {
                assert_eq!(kind, "bash timestamp marker");
                assert_eq!(raw_line, "#999999999999999999999999");
            }
            other => {
                return Err(color_eyre::eyre::eyre!(
                    "expected malformed bash metadata, got {:?}",
                    std::mem::discriminant(&other)
                ));
            }
        }

        match parse_text_history_line("zsh", ": nope:0;echo hello") {
            TextHistoryLine::MalformedMetadata { kind, raw_line } => {
                assert_eq!(kind, "zsh extended history timestamp");
                assert_eq!(raw_line, ": nope:0;echo hello");
            }
            other => {
                return Err(color_eyre::eyre::eyre!(
                    "expected malformed zsh metadata, got {:?}",
                    std::mem::discriminant(&other)
                ));
            }
        }

        Ok(())
    }

    #[sinex_test]
    async fn process_command_emits_event(ctx: TestContext) -> TestResult<()> {
        let ctx = ctx.with_nats().dedicated().await?;
        let TestRuntime {
            runtime,
            mut event_rx,
            nats,
        } = TestRuntimeBuilder::new(&ctx, "terminal-ingestor-test")
            .with_dry_run(false)
            .build()
            .await?;

        let work_dir = tempfile::tempdir()?;
        let ingest_config = TestIngestdConfig {
            nats: nats.connection_config(),
            database_url: ctx.database_url().to_string(),
            work_dir: Some(work_dir.path().to_path_buf()),
            ..Default::default()
        };
        let mut ingest_handle = start_test_ingestd_with_config(ingest_config, Some(&ctx)).await?;

        let publisher = match runtime.transport() {
            sinex_node_sdk::EventTransport::Nats(publisher) => publisher.clone(),
        };
        AcquisitionManager::bootstrap_streams(publisher.nats_client()).await?;

        // Wait for MaterialAssembler consumers before publishing
        let env = sinex_primitives::environment::environment();
        let js_check = nats.jetstream_with_client(publisher.nats_client().clone());
        for stream in [
            env.nats_stream_name("SOURCE_MATERIAL_BEGIN"),
            env.nats_stream_name("SOURCE_MATERIAL_SLICES"),
            env.nats_stream_name("SOURCE_MATERIAL_END"),
        ] {
            nats.wait_for_consumer_on_stream(&js_check, &stream, Duration::from_mins(1))
                .await?;
        }

        let acquisition = Arc::new(runtime.acquisition_manager(
            RotationPolicy::default(),
            "terminal-history",
            "/home/test/.bash_history",
        )?);

        let stage_context = StageAsYouGoContext::from_runtime(&runtime)
            .with_acquisition_manager(Arc::clone(&acquisition));

        let watcher_ctx = HistoryWatcherContext {
            acquisition,
            stage_context,
            metrics: TerminalMetrics::new(),
            shell: "bash".to_string(),
            path: Utf8PathBuf::from("/home/test/.bash_history"),
            max_capture_bytes: Bytes::from_bytes(1024),
            polling_interval: Duration::from_secs(1),
            state_path: None,
            shutdown_rx: tokio::sync::watch::channel(false).1,
            #[cfg(test)]
            processed_commands: None,
            source_mode: HistorySourceMode::Text,
            initial_state_override: None,
        };

        let command = "echo 'hello world'";
        let mut recent_hashes = VecDeque::new();
        let timestamp = Timestamp::from_unix_timestamp(1_710_877_544).expect("valid timestamp");
        process_command(
            &watcher_ctx,
            command,
            Some(timestamp),
            42,
            &mut recent_hashes,
        )
        .await?;
        assert_eq!(
            watcher_ctx
                .metrics
                .commands_processed
                .load(Ordering::Relaxed),
            1
        );

        let event = timeout(Duration::from_secs(5), event_rx.recv())
            .await?
            .ok_or_else(|| color_eyre::eyre::eyre!("terminal event not emitted"))?;

        assert_eq!(event.event_type.as_str(), "command.imported");
        assert_eq!(
            event.payload.get("timestamp"),
            Some(&serde_json::json!("2024-03-19T19:45:44Z"))
        );

        let material_uuid = match event.provenance() {
            Provenance::Material { id, .. } => *id.as_uuid(),
            _ => {
                return Err(color_eyre::eyre::eyre!(
                    "expected material provenance in terminal event"
                ));
            }
        };
        let expected_bytes = command.len() as i64;
        xtask::sandbox::timing::WaitHelpers::wait_for_condition(
            || {
                let pool = ctx.pool.clone();
                async move {
                    let expected = expected_bytes;
                    if let Some(material) = pool
                        .source_materials()
                        .get_by_id(Id::from_uuid(material_uuid))
                        .await
                        .map_err(|e| color_eyre::eyre::eyre!("{e}"))?
                    {
                        if material.status.as_str() != "completed" {
                            return Ok::<bool, color_eyre::eyre::Report>(false);
                        }
                    } else {
                        return Ok::<bool, color_eyre::eyre::Report>(false);
                    }

                    let ledger_bytes: Option<i64> = sqlx::query_scalar(
                        "SELECT MAX(offset_end) FROM raw.temporal_ledger WHERE source_material_id = $1::uuid AND source_type = 'realtime_capture'",
                    )
                    .bind(material_uuid)
                    .fetch_one(&pool)
                    .await
                    .map_err(|e| color_eyre::eyre::eyre!("database error: {e}"))?;
                    Ok::<bool, color_eyre::eyre::Report>(
                        ledger_bytes.unwrap_or_default() == expected
                    )
                }
            },
            Timeouts::STANDARD,
        )
        .await?;

        let record = ctx
            .pool
            .source_materials()
            .get_by_id(Id::from_uuid(material_uuid))
            .await?
            .ok_or_else(|| color_eyre::eyre::eyre!("source material not persisted"))?;
        assert_eq!(record.status.as_str(), "completed");

        let total_bytes: Option<i64> = sqlx::query_scalar(
            "SELECT MAX(offset_end) FROM raw.temporal_ledger WHERE source_material_id = $1::uuid AND source_type = 'realtime_capture'",
        )
        .bind(material_uuid)
        .fetch_one(&ctx.pool)
        .await?;

        assert_eq!(total_bytes.unwrap_or_default(), expected_bytes);

        let payload_command = event
            .payload
            .get("command")
            .and_then(|v| v.as_str())
            .ok_or_else(|| color_eyre::eyre::eyre!("payload command missing"))?;
        assert_eq!(payload_command, command);

        ingest_handle.stop().await?;
        Ok(())
    }

    #[sinex_test]
    async fn terminal_history_checkpoint_roundtrips_per_source_progress() -> TestResult<()> {
        let source_key = "atuin:/home/test/.local/share/atuin/history.db".to_string();
        let mut states = HashMap::new();
        states.insert(
            source_key.clone(),
            HistoryState {
                sqlite_row_id: Some(42),
                recent_hashes: VecDeque::from([1_u64, 2_u64]),
                ..HistoryState::default()
            },
        );

        let checkpoint = TerminalNode::checkpoint_from_states(states)?;
        let restored = TerminalNode::checkpoint_state_for_source(&checkpoint, &source_key)?
            .ok_or_else(|| color_eyre::eyre::eyre!("checkpoint state missing"))?;

        assert_eq!(restored.sqlite_row_id, Some(42));
        assert_eq!(restored.recent_hashes, VecDeque::from([1_u64, 2_u64]));
        assert!(
            TerminalNode::checkpoint_state_for_source(&checkpoint, "bash:/tmp/missing")?.is_none()
        );

        Ok(())
    }

    #[sinex_test]
    async fn terminal_history_checkpoint_rejects_incompatible_variants() -> TestResult<()> {
        let error = TerminalNode::checkpoint_state_for_source(
            &Checkpoint::timestamp(Timestamp::now(), None),
            "atuin:/tmp/history.db",
        )
        .expect_err("timestamp checkpoints should not be accepted for terminal per-source state");

        assert!(
            error
                .to_string()
                .contains("terminal history requires an external per-source checkpoint")
        );
        Ok(())
    }

    #[sinex_test]
    async fn terminal_history_checkpoint_rejects_negative_sqlite_row_id() -> TestResult<()> {
        let error = TerminalNode::checkpoint_from_states(HashMap::from([(
            "atuin:/tmp/history.db".to_string(),
            HistoryState {
                sqlite_row_id: Some(-1),
                ..HistoryState::default()
            },
        )]))
        .expect_err("negative sqlite row ids must not be serialized into checkpoints");

        assert!(error.to_string().contains("invalid negative sqlite_row_id"));
        Ok(())
    }

    #[sinex_test]
    async fn terminal_history_checkpoint_restore_rejects_negative_sqlite_row_id(
    ) -> TestResult<()> {
        let checkpoint = Checkpoint::external(
            serde_json::json!({
                "sources": {
                    "atuin:/tmp/history.db": {
                        "offset_bytes": 0,
                        "line_number": 0,
                        "pending_timestamp": null,
                        "sqlite_row_id": -1,
                        "recent_hashes": [],
                    }
                }
            }),
            "terminal history source progress",
        );

        let error = TerminalNode::checkpoint_state_for_source(&checkpoint, "atuin:/tmp/history.db")
            .expect_err("negative sqlite row ids must not be accepted from incoming checkpoints");

        assert!(error.to_string().contains("invalid negative sqlite_row_id"));
        Ok(())
    }

    #[sinex_test]
    async fn terminal_history_checkpoint_restore_rejects_missing_sqlite_row_id(
    ) -> TestResult<()> {
        let checkpoint = Checkpoint::external(
            serde_json::json!({
                "sources": {
                    "atuin:/tmp/history.db": {
                        "offset_bytes": 0,
                        "line_number": 0,
                        "pending_timestamp": null,
                        "recent_hashes": [],
                    }
                }
            }),
            "terminal history source progress",
        );

        let error = TerminalNode::checkpoint_state_for_source_mode(
            &checkpoint,
            "atuin:/tmp/history.db",
            &HistorySourceMode::AtuinSqlite,
        )
            .expect_err("sqlite-backed checkpoints must carry a row id");

        assert!(error.to_string().contains("missing sqlite_row_id"));
        Ok(())
    }

    #[sinex_test]
    async fn terminal_history_checkpoint_restore_allows_text_fish_source_without_sqlite_row_id(
    ) -> TestResult<()> {
        let temp_dir = tempfile::tempdir()?;
        let history_path = temp_dir.path().join("fish_history");
        std::fs::write(&history_path, "- cmd: echo hello\n  when: 1234567890\n")?;
        let history_path = Utf8PathBuf::from_path_buf(history_path).map_err(|path| {
            color_eyre::eyre::eyre!(
                "invalid Fish temp path should be utf-8: {}",
                path.display()
            )
        })?;
        let source_key = format!("fish:{history_path}");
        let checkpoint = Checkpoint::external(
            serde_json::json!({
                "sources": {
                    source_key.clone(): {
                        "offset_bytes": 42,
                        "line_number": 3,
                        "pending_timestamp": null,
                        "recent_hashes": [],
                    }
                }
            }),
            "terminal history source progress",
        );

        let state = TerminalNode::checkpoint_state_for_source(&checkpoint, &source_key)?
            .expect("fish checkpoint state should be present");
        assert_eq!(state.offset_bytes, 42);
        assert_eq!(state.line_number, 3);
        assert!(state.sqlite_row_id.is_none());
        Ok(())
    }

    #[sinex_test]
    async fn terminal_history_checkpoint_restore_rejects_missing_sqlite_row_id_for_sqlite_fish_source(
    ) -> TestResult<()> {
        let temp_dir = tempfile::tempdir()?;
        let history_path = temp_dir.path().join("fish_history.db");
        let conn = rusqlite::Connection::open(&history_path)?;
        conn.execute(
            "CREATE TABLE history (
                command TEXT NOT NULL,
                \"when\" INTEGER
            )",
            [],
        )?;
        let history_path = Utf8PathBuf::from_path_buf(history_path).map_err(|path| {
            color_eyre::eyre::eyre!(
                "invalid Fish temp path should be utf-8: {}",
                path.display()
            )
        })?;
        let source_key = format!("fish:{history_path}");
        let checkpoint = Checkpoint::external(
            serde_json::json!({
                "sources": {
                    source_key.clone(): {
                        "offset_bytes": 0,
                        "line_number": 0,
                        "pending_timestamp": null,
                        "recent_hashes": [],
                    }
                }
            }),
            "terminal history source progress",
        );

        let error = TerminalNode::checkpoint_state_for_source_mode(
            &checkpoint,
            &source_key,
            &HistorySourceMode::FishSqlite,
        )
            .expect_err("SQLite-backed Fish checkpoints must carry a row id");

        assert!(error.to_string().contains("missing sqlite_row_id"));
        Ok(())
    }

    #[sinex_test]
    async fn process_atuin_entry_emits_shell_atuin_event(ctx: TestContext) -> TestResult<()> {
        let ctx = ctx.with_nats().dedicated().await?;
        let TestRuntime {
            runtime,
            mut event_rx,
            nats,
        } = TestRuntimeBuilder::new(&ctx, "terminal-atuin-test")
            .with_dry_run(false)
            .build()
            .await?;

        let work_dir = tempfile::tempdir()?;
        let ingest_config = TestIngestdConfig {
            nats: nats.connection_config(),
            database_url: ctx.database_url().to_string(),
            work_dir: Some(work_dir.path().to_path_buf()),
            ..Default::default()
        };
        let mut ingest_handle = start_test_ingestd_with_config(ingest_config, Some(&ctx)).await?;

        let publisher = match runtime.transport() {
            sinex_node_sdk::EventTransport::Nats(publisher) => publisher.clone(),
        };
        AcquisitionManager::bootstrap_streams(publisher.nats_client()).await?;

        let env = sinex_primitives::environment::environment();
        let js_check = nats.jetstream_with_client(publisher.nats_client().clone());
        for stream in [
            env.nats_stream_name("SOURCE_MATERIAL_BEGIN"),
            env.nats_stream_name("SOURCE_MATERIAL_SLICES"),
            env.nats_stream_name("SOURCE_MATERIAL_END"),
        ] {
            nats.wait_for_consumer_on_stream(&js_check, &stream, Duration::from_mins(1))
                .await?;
        }

        let acquisition = Arc::new(runtime.acquisition_manager(
            RotationPolicy::default(),
            "terminal-history",
            "/home/test/.local/share/atuin/history.db",
        )?);
        let stage_context = StageAsYouGoContext::from_runtime(&runtime)
            .with_acquisition_manager(Arc::clone(&acquisition));

        let watcher_ctx = HistoryWatcherContext {
            acquisition,
            stage_context,
            metrics: TerminalMetrics::new(),
            shell: "atuin".to_string(),
            path: Utf8PathBuf::from("/home/test/.local/share/atuin/history.db"),
            max_capture_bytes: Bytes::from_bytes(1024),
            polling_interval: Duration::from_secs(1),
            state_path: None,
            shutdown_rx: tokio::sync::watch::channel(false).1,
            #[cfg(test)]
            processed_commands: None,
            source_mode: HistorySourceMode::AtuinSqlite,
            initial_state_override: None,
        };

        let entry = crate::atuin_history::AtuinHistoryEntry {
            row_id: 42,
            history_id: "h1".to_string(),
            timestamp_ns: 1_700_000_000_000_000_000,
            duration_ns: 50_000_000,
            exit_code: 0,
            command: "echo 'hello from atuin'".to_string(),
            cwd: "/realm/project/sinex".to_string(),
            session_id: "session-1".to_string(),
            hostname: "test-host".to_string(),
        };
        let mut recent_hashes = VecDeque::new();
        process_atuin_entry(&watcher_ctx, &entry, &mut recent_hashes).await?;

        let event = timeout(Duration::from_secs(5), event_rx.recv())
            .await?
            .ok_or_else(|| color_eyre::eyre::eyre!("Atuin event not emitted"))?;

        assert_eq!(event.source.as_str(), "shell.atuin");
        assert_eq!(event.event_type.as_str(), "command.executed");
        assert_eq!(
            event
                .payload
                .get("command_string")
                .and_then(|value| value.as_str()),
            Some("echo 'hello from atuin'")
        );
        match event.provenance() {
            Provenance::Material { id, .. } => {
                // Material ID should be a valid UUIDv7 (each observation is a fresh material)
                assert!(!id.as_uuid().is_nil(), "material ID should not be nil");
            }
            _ => {
                return Err(color_eyre::eyre::eyre!(
                    "expected material provenance in Atuin event"
                ));
            }
        };

        ingest_handle.stop().await?;
        Ok(())
    }

    #[sinex_test]
    async fn terminal_watcher_tails_incrementally(ctx: TestContext) -> TestResult<()> {
        let ctx = ctx.with_nats().dedicated().await?;
        let TestRuntime { runtime, nats, .. } =
            TestRuntimeBuilder::new(&ctx, "terminal-watcher-incremental")
                .with_dry_run(false)
                .build()
                .await?;

        let ingest_config = TestIngestdConfig {
            nats: nats.connection_config(),
            database_url: ctx.database_url().to_string(),
            work_dir: None,
            ..Default::default()
        };
        let mut ingest_handle = start_test_ingestd_with_config(ingest_config, Some(&ctx)).await?;

        let publisher = match runtime.transport() {
            sinex_node_sdk::EventTransport::Nats(publisher) => publisher.clone(),
        };
        AcquisitionManager::bootstrap_streams(publisher.nats_client()).await?;

        let acquisition = Arc::new(runtime.acquisition_manager(
            RotationPolicy::default(),
            "terminal-history",
            "/tmp/history",
        )?);
        let stage_context = StageAsYouGoContext::from_runtime(&runtime)
            .with_acquisition_manager(Arc::clone(&acquisition));

        let temp_dir = tempfile::tempdir()?;
        let history_path = temp_dir.path().join("history.txt");
        tokio::fs::write(&history_path, "echo first\n").await?;
        let state_path = temp_dir.path().join("history_state.json");

        let history_utf8 = Utf8PathBuf::from_path_buf(history_path.clone())
            .map_err(|path| color_eyre::eyre::eyre!("history path not utf8: {}", path.display()))?;

        let mut watcher_ctx = HistoryWatcherContext {
            acquisition,
            stage_context,
            metrics: TerminalMetrics::new(),
            shell: "bash".to_string(),
            path: history_utf8,
            max_capture_bytes: Bytes::from_bytes(2048),
            polling_interval: Duration::from_millis(50),
            state_path: Some(state_path),
            shutdown_rx: tokio::sync::watch::channel(false).1,
            #[cfg(test)]
            processed_commands: None,
            source_mode: HistorySourceMode::Text,
            initial_state_override: None,
        };

        #[cfg(test)]
        let processed_commands = Arc::new(Mutex::new(Vec::new()));
        #[cfg(test)]
        {
            watcher_ctx.processed_commands = Some(processed_commands.clone());
        }

        let mut offset_bytes = 0u64;
        let mut line_number = 0u64;
        let mut pending_timestamp = None;
        let mut recent_hashes: VecDeque<u64> = VecDeque::new();
        #[cfg(unix)]
        let mut last_inode: Option<u64> = None;

        #[cfg(unix)]
        watcher_ctx
            .poll_history_once(
                &mut offset_bytes,
                &mut line_number,
                &mut pending_timestamp,
                &mut last_inode,
                &mut recent_hashes,
                true,
            )
            .await?;
        #[cfg(not(unix))]
        watcher_ctx
            .poll_history_once(
                &mut offset_bytes,
                &mut line_number,
                &mut pending_timestamp,
                &mut recent_hashes,
                true,
            )
            .await?;

        let mut history_file: tokio::fs::File = tokio::fs::OpenOptions::new()
            .append(true)
            .open(&history_path)
            .await?;
        history_file.write_all(b"echo second\n").await?;
        history_file.write_all(b"echo third\n").await?;
        history_file.flush().await?;

        #[cfg(unix)]
        watcher_ctx
            .poll_history_once(
                &mut offset_bytes,
                &mut line_number,
                &mut pending_timestamp,
                &mut last_inode,
                &mut recent_hashes,
                true,
            )
            .await?;
        #[cfg(not(unix))]
        watcher_ctx
            .poll_history_once(
                &mut offset_bytes,
                &mut line_number,
                &mut pending_timestamp,
                &mut recent_hashes,
                true,
            )
            .await?;

        #[cfg(test)]
        {
            let commands = processed_commands.lock().await.clone();
            assert_eq!(
                commands,
                vec!["echo first", "echo second", "echo third"],
                "history watcher should append only new commands in order"
            );
        }

        ingest_handle.stop().await?;
        Ok(())
    }

    #[sinex_test]
    async fn process_atuin_entry_rejects_invalid_duration(ctx: TestContext) -> TestResult<()> {
        let ctx = ctx.with_nats().dedicated().await?;
        let TestRuntime {
            runtime,
            mut event_rx,
            nats,
        } = TestRuntimeBuilder::new(&ctx, "terminal-atuin-invalid-duration")
            .with_dry_run(false)
            .build()
            .await?;

        let work_dir = tempfile::tempdir()?;
        let ingest_config = TestIngestdConfig {
            nats: nats.connection_config(),
            database_url: ctx.database_url().to_string(),
            work_dir: Some(work_dir.path().to_path_buf()),
            ..Default::default()
        };
        let mut ingest_handle = start_test_ingestd_with_config(ingest_config, Some(&ctx)).await?;

        let publisher = match runtime.transport() {
            sinex_node_sdk::EventTransport::Nats(publisher) => publisher.clone(),
        };
        AcquisitionManager::bootstrap_streams(publisher.nats_client()).await?;

        let env = sinex_primitives::environment::environment();
        let js_check = nats.jetstream_with_client(publisher.nats_client().clone());
        for stream in [
            env.nats_stream_name("SOURCE_MATERIAL_BEGIN"),
            env.nats_stream_name("SOURCE_MATERIAL_SLICES"),
            env.nats_stream_name("SOURCE_MATERIAL_END"),
        ] {
            nats.wait_for_consumer_on_stream(&js_check, &stream, Duration::from_mins(1))
                .await?;
        }

        let acquisition = Arc::new(runtime.acquisition_manager(
            RotationPolicy::default(),
            "terminal-history",
            "/home/test/.local/share/atuin/history.db",
        )?);
        let stage_context = StageAsYouGoContext::from_runtime(&runtime)
            .with_acquisition_manager(Arc::clone(&acquisition));

        let watcher_ctx = HistoryWatcherContext {
            acquisition,
            stage_context,
            metrics: TerminalMetrics::new(),
            shell: "atuin".to_string(),
            path: Utf8PathBuf::from("/home/test/.local/share/atuin/history.db"),
            max_capture_bytes: Bytes::from_bytes(1024),
            polling_interval: Duration::from_secs(1),
            state_path: None,
            shutdown_rx: tokio::sync::watch::channel(false).1,
            #[cfg(test)]
            processed_commands: None,
            source_mode: HistorySourceMode::AtuinSqlite,
            initial_state_override: None,
        };

        let entry = crate::atuin_history::AtuinHistoryEntry {
            row_id: 42,
            history_id: "h1".to_string(),
            timestamp_ns: 1_700_000_000_000_000_000,
            duration_ns: -1,
            exit_code: 0,
            command: "echo 'hello from atuin'".to_string(),
            cwd: "/realm/project/sinex".to_string(),
            session_id: "session-1".to_string(),
            hostname: "test-host".to_string(),
        };
        let mut recent_hashes = VecDeque::new();
        let error = process_atuin_entry(&watcher_ctx, &entry, &mut recent_hashes)
            .await
            .expect_err("invalid Atuin row should fail loudly");
        assert!(
            error.to_string().contains("invalid timestamp or duration"),
            "unexpected error: {error}"
        );

        let next = timeout(Duration::from_millis(200), event_rx.recv()).await;
        assert!(next.is_err(), "invalid Atuin row should not emit an event");

        ingest_handle.stop().await?;
        Ok(())
    }

    #[sinex_test]
    async fn emit_prepared_fish_entry_rejects_invalid_timestamp(ctx: TestContext) -> TestResult<()> {
        let ctx = ctx.with_nats().dedicated().await?;
        let TestRuntime {
            runtime,
            mut event_rx,
            nats,
        } = TestRuntimeBuilder::new(&ctx, "terminal-fish-invalid-timestamp")
            .with_dry_run(false)
            .build()
            .await?;

        let work_dir = tempfile::tempdir()?;
        let ingest_config = TestIngestdConfig {
            nats: nats.connection_config(),
            database_url: ctx.database_url().to_string(),
            work_dir: Some(work_dir.path().to_path_buf()),
            ..Default::default()
        };
        let mut ingest_handle = start_test_ingestd_with_config(ingest_config, Some(&ctx)).await?;

        let publisher = match runtime.transport() {
            sinex_node_sdk::EventTransport::Nats(publisher) => publisher.clone(),
        };
        AcquisitionManager::bootstrap_streams(publisher.nats_client()).await?;

        let env = sinex_primitives::environment::environment();
        let js_check = nats.jetstream_with_client(publisher.nats_client().clone());
        for stream in [
            env.nats_stream_name("SOURCE_MATERIAL_BEGIN"),
            env.nats_stream_name("SOURCE_MATERIAL_SLICES"),
            env.nats_stream_name("SOURCE_MATERIAL_END"),
        ] {
            nats.wait_for_consumer_on_stream(&js_check, &stream, Duration::from_mins(1))
                .await?;
        }

        let acquisition = Arc::new(runtime.acquisition_manager(
            RotationPolicy::default(),
            "terminal-history",
            "/home/test/.local/share/fish/fish_history.db",
        )?);
        let stage_context = StageAsYouGoContext::from_runtime(&runtime)
            .with_acquisition_manager(Arc::clone(&acquisition));

        let watcher_ctx = HistoryWatcherContext {
            acquisition,
            stage_context,
            metrics: TerminalMetrics::new(),
            shell: "fish".to_string(),
            path: Utf8PathBuf::from("/home/test/.local/share/fish/fish_history.db"),
            max_capture_bytes: Bytes::from_bytes(1024),
            polling_interval: Duration::from_secs(1),
            state_path: None,
            shutdown_rx: tokio::sync::watch::channel(false).1,
            #[cfg(test)]
            processed_commands: None,
            source_mode: HistorySourceMode::FishSqlite,
            initial_state_override: None,
        };

        let entry = crate::fish_history::FishHistoryEntry {
            row_id: 42,
            command: "echo 'hello from fish'".to_string(),
            when: Some(i64::MAX),
        };

        let error = emit_prepared_fish_entry(&watcher_ctx, &entry, entry.command.clone())
            .await
            .expect_err("invalid Fish row should fail loudly");
        assert!(
            error.to_string().contains("invalid timestamp"),
            "unexpected error: {error}"
        );

        let next = timeout(Duration::from_millis(200), event_rx.recv()).await;
        assert!(next.is_err(), "invalid Fish row should not emit an event");

        ingest_handle.stop().await?;
        Ok(())
    }

    #[sinex_test]
    async fn process_atuin_entry_rejects_negative_row_id(ctx: TestContext) -> TestResult<()> {
        let ctx = ctx.with_nats().dedicated().await?;
        let TestRuntime { runtime, .. } =
            TestRuntimeBuilder::new(&ctx, "terminal-atuin-negative-row-id")
                .with_dry_run(false)
                .build()
                .await?;

        let acquisition = Arc::new(runtime.acquisition_manager(
            RotationPolicy::default(),
            "terminal-history",
            "/home/test/.local/share/atuin/history.db",
        )?);
        let stage_context = StageAsYouGoContext::from_runtime(&runtime)
            .with_acquisition_manager(Arc::clone(&acquisition));

        let watcher_ctx = HistoryWatcherContext {
            acquisition,
            stage_context,
            metrics: TerminalMetrics::new(),
            shell: "atuin".to_string(),
            path: Utf8PathBuf::from("/home/test/.local/share/atuin/history.db"),
            max_capture_bytes: Bytes::from_bytes(1024),
            polling_interval: Duration::from_secs(1),
            state_path: None,
            shutdown_rx: tokio::sync::watch::channel(false).1,
            #[cfg(test)]
            processed_commands: None,
            source_mode: HistorySourceMode::AtuinSqlite,
            initial_state_override: None,
        };

        let entry = crate::atuin_history::AtuinHistoryEntry {
            row_id: -1,
            history_id: "h1".to_string(),
            timestamp_ns: 1_700_000_000_000_000_000,
            duration_ns: 50_000_000,
            exit_code: 0,
            command: "echo 'hello from atuin'".to_string(),
            cwd: "/realm/project/sinex".to_string(),
            session_id: "session-1".to_string(),
            hostname: "test-host".to_string(),
        };
        let mut recent_hashes = VecDeque::new();
        let error = process_atuin_entry(&watcher_ctx, &entry, &mut recent_hashes)
            .await
            .expect_err("negative Atuin row ids must fail honestly");
        assert!(error.to_string().contains("invalid negative sqlite row id"));
        Ok(())
    }

    #[sinex_test]
    async fn scan_historical_reports_invalid_atuin_database_per_target(
        ctx: TestContext,
    ) -> TestResult<()> {
        let TestRuntime { runtime, .. } =
            TestRuntimeBuilder::new(&ctx, "terminal-historical-invalid-atuin")
                .with_dry_run(true)
                .build()
                .await?;

        let temp_dir = tempfile::tempdir()?;
        let invalid_db = temp_dir.path().join("history.db");
        tokio::fs::write(&invalid_db, "not a sqlite database").await?;
        let invalid_db = Utf8PathBuf::from_path_buf(invalid_db).map_err(|path| {
            color_eyre::eyre::eyre!(
                "invalid Atuin temp path should be utf-8: {}",
                path.display()
            )
        })?;

        let config = TerminalConfig {
            history_sources: vec![HistorySourceConfig {
                path: invalid_db.clone(),
                shell: "atuin".to_string(),
            }],
            polling_interval_secs: Seconds::from_secs(5),
            max_capture_bytes: Bytes::from_bytes(1024),
        };

        let mut node = TerminalNode::new();
        let mut state = TerminalCheckpoint::default();
        node.initialize(config, &runtime, &mut state).await?;

        let report = node
            .scan_historical(
                &mut state,
                Checkpoint::None,
                TimeHorizon::Historical {
                    end_time: Timestamp::now(),
                },
                ScanArgs::default(),
            )
            .await?;

        assert_eq!(report.events_processed, 0);
        assert!(report.successful_targets.is_empty());
        assert_eq!(report.failed_targets.len(), 1);
        assert_eq!(report.failed_targets[0].0, format!("atuin:{invalid_db}"));
        assert!(
            report.failed_targets[0]
                .1
                .contains("configured Atuin history source"),
            "unexpected failure: {:?}",
            report.failed_targets
        );

        Ok(())
    }

    #[sinex_test]
    async fn scan_historical_preserves_local_state_for_configured_error_sources(
        ctx: TestContext,
    ) -> TestResult<()> {
        let TestRuntime { runtime, .. } =
            TestRuntimeBuilder::new(&ctx, "terminal-historical-preserve-local-state")
                .with_dry_run(true)
                .build()
                .await?;

        let temp_dir = tempfile::tempdir()?;
        let invalid_db = temp_dir.path().join("history.db");
        tokio::fs::write(&invalid_db, "not a sqlite database").await?;
        let invalid_db = Utf8PathBuf::from_path_buf(invalid_db).map_err(|path| {
            color_eyre::eyre::eyre!(
                "invalid Atuin temp path should be utf-8: {}",
                path.display()
            )
        })?;

        let config = TerminalConfig {
            history_sources: vec![HistorySourceConfig {
                path: invalid_db.clone(),
                shell: "atuin".to_string(),
            }],
            polling_interval_secs: Seconds::from_secs(5),
            max_capture_bytes: Bytes::from_bytes(1024),
        };

        let mut node = TerminalNode::new();
        let mut state = TerminalCheckpoint::default();
        node.initialize(config, &runtime, &mut state).await?;

        let (_, shutdown_rx) = tokio::sync::watch::channel(false);
        let state_path = node
            .build_history_contexts(shutdown_rx)?
            .into_iter()
            .next()
            .and_then(|ctx| ctx.state_path)
            .ok_or_else(|| color_eyre::eyre::eyre!("missing terminal watcher state path"))?;

        let expected_state = HistoryState {
            sqlite_row_id: Some(41),
            recent_hashes: VecDeque::from([11, 17]),
            ..HistoryState::default()
        };
        tokio::fs::write(&state_path, serde_json::to_vec(&expected_state)?).await?;

        let report = node
            .scan_historical(
                &mut state,
                Checkpoint::None,
                TimeHorizon::Historical {
                    end_time: Timestamp::now(),
                },
                ScanArgs::default(),
            )
            .await?;

        let restored = TerminalNode::checkpoint_state_for_source(
            &report.final_checkpoint,
            &format!("atuin:{invalid_db}"),
        )?
        .ok_or_else(|| color_eyre::eyre::eyre!("missing preserved checkpoint state"))?;
        assert_eq!(restored.sqlite_row_id, expected_state.sqlite_row_id);
        assert_eq!(restored.recent_hashes, expected_state.recent_hashes);
        Ok(())
    }

    #[sinex_test]
    async fn scan_historical_ignores_stale_local_state_when_checkpoint_missing(
        ctx: TestContext,
    ) -> TestResult<()> {
        let TestRuntime { runtime, .. } =
            TestRuntimeBuilder::new(&ctx, "terminal-historical-ignore-local-state")
                .with_dry_run(true)
                .build()
                .await?;

        let temp_dir = tempfile::tempdir()?;
        let history_path = temp_dir.path().join("atuin.db");
        let conn = rusqlite::Connection::open(&history_path)?;
        conn.execute(
            "CREATE TABLE history (
                id TEXT PRIMARY KEY,
                timestamp INTEGER NOT NULL,
                command TEXT NOT NULL,
                cwd TEXT,
                exit INTEGER,
                duration INTEGER,
                hostname TEXT,
                session TEXT,
                deleted_at INTEGER
            )",
            [],
        )?;
        conn.execute(
            "INSERT INTO history (id, timestamp, command, cwd, exit, duration, hostname, session, deleted_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, NULL)",
            rusqlite::params![
                "hist-1",
                1_700_100_000_i64,
                "echo replay me",
                "/realm/project/sinex",
                0_i64,
                1_i64,
                "test-host",
                "session-1",
            ],
        )?;
        let history_path = Utf8PathBuf::from_path_buf(history_path).map_err(|path| {
            color_eyre::eyre::eyre!("invalid Atuin temp path should be utf-8: {}", path.display())
        })?;

        let config = TerminalConfig {
            history_sources: vec![HistorySourceConfig {
                path: history_path.clone(),
                shell: "atuin".to_string(),
            }],
            polling_interval_secs: Seconds::from_secs(5),
            max_capture_bytes: Bytes::from_bytes(1024),
        };

        let mut node = TerminalNode::new();
        let mut state = TerminalCheckpoint::default();
        node.initialize(config, &runtime, &mut state).await?;

        let (_, shutdown_rx) = tokio::sync::watch::channel(false);
        let state_path = node
            .build_history_contexts(shutdown_rx)?
            .into_iter()
            .next()
            .and_then(|ctx| ctx.state_path)
            .ok_or_else(|| color_eyre::eyre::eyre!("missing terminal watcher state path"))?;
        tokio::fs::write(
            &state_path,
            serde_json::to_vec(&HistoryState {
                sqlite_row_id: Some(41),
                recent_hashes: VecDeque::from([7, 11]),
                ..HistoryState::default()
            })?,
        )
        .await?;

        let report = node
            .scan_historical(
                &mut state,
                Checkpoint::None,
                TimeHorizon::Historical {
                    end_time: Timestamp::now(),
                },
                ScanArgs::default(),
            )
            .await?;

        assert_eq!(report.events_processed, 1);
        let restored = TerminalNode::checkpoint_state_for_source(
            &report.final_checkpoint,
            &format!("atuin:{history_path}"),
        )?
        .ok_or_else(|| color_eyre::eyre::eyre!("missing replay checkpoint state"))?;
        assert_eq!(restored.sqlite_row_id, Some(1));
        Ok(())
    }

    #[sinex_test]
    async fn scan_historical_reports_invalid_incoming_checkpoint_per_target(
        ctx: TestContext,
    ) -> TestResult<()> {
        let TestRuntime { runtime, .. } =
            TestRuntimeBuilder::new(&ctx, "terminal-historical-invalid-checkpoint")
                .with_dry_run(true)
                .build()
                .await?;

        let temp_dir = tempfile::tempdir()?;
        let history_path = temp_dir.path().join("atuin.db");
        let conn = rusqlite::Connection::open(&history_path)?;
        conn.execute(
            "CREATE TABLE history (
                id TEXT PRIMARY KEY,
                timestamp INTEGER NOT NULL,
                command TEXT NOT NULL,
                cwd TEXT,
                exit INTEGER,
                duration INTEGER,
                hostname TEXT,
                session TEXT,
                deleted_at INTEGER
            )",
            [],
        )?;
        let history_path = Utf8PathBuf::from_path_buf(history_path).map_err(|path| {
            color_eyre::eyre::eyre!("invalid Atuin temp path should be utf-8: {}", path.display())
        })?;

        let config = TerminalConfig {
            history_sources: vec![HistorySourceConfig {
                path: history_path.clone(),
                shell: "atuin".to_string(),
            }],
            polling_interval_secs: Seconds::from_secs(5),
            max_capture_bytes: Bytes::from_bytes(1024),
        };

        let mut node = TerminalNode::new();
        let mut state = TerminalCheckpoint::default();
        node.initialize(config, &runtime, &mut state).await?;

        let report = node
            .scan_historical(
                &mut state,
                Checkpoint::timestamp(Timestamp::now(), None),
                TimeHorizon::Historical {
                    end_time: Timestamp::now(),
                },
                ScanArgs::default(),
            )
            .await?;

        let checkpoint_key = format!("atuin:{history_path}");
        assert_eq!(report.events_processed, 0);
        assert!(report.successful_targets.is_empty());
        assert_eq!(report.failed_targets.len(), 1);
        assert_eq!(report.failed_targets[0].0, checkpoint_key);
        assert!(
            report.failed_targets[0]
                .1
                .contains("failed to restore incoming terminal checkpoint state"),
            "unexpected failure: {:?}",
            report.failed_targets
        );
        assert!(
            report
                .warnings
                .iter()
                .any(|warning| warning.contains("incoming checkpoint state is unusable for historical replay")),
            "expected checkpoint warning, got {:?}",
            report.warnings
        );
        assert!(
            TerminalNode::checkpoint_state_for_source(
                &report.final_checkpoint,
                &format!("atuin:{history_path}")
            )?
            .is_some(),
            "failed target should preserve its local/default state in the returned checkpoint"
        );

        Ok(())
    }

    #[sinex_test]
    async fn scan_historical_reports_unsupported_fish_history_per_target(
        ctx: TestContext,
    ) -> TestResult<()> {
        let TestRuntime { runtime, .. } =
            TestRuntimeBuilder::new(&ctx, "terminal-historical-invalid-fish")
                .with_dry_run(true)
                .build()
                .await?;

        let temp_dir = tempfile::tempdir()?;
        let invalid_history = temp_dir.path().join("fish_history");
        tokio::fs::write(&invalid_history, "- cmd: echo hello\n  when: 1234567890\n").await?;
        let invalid_history = Utf8PathBuf::from_path_buf(invalid_history).map_err(|path| {
            color_eyre::eyre::eyre!(
                "invalid Fish temp path should be utf-8: {}",
                path.display()
            )
        })?;

        let config = TerminalConfig {
            history_sources: vec![HistorySourceConfig {
                path: invalid_history.clone(),
                shell: "fish".to_string(),
            }],
            polling_interval_secs: Seconds::from_secs(5),
            max_capture_bytes: Bytes::from_bytes(1024),
        };

        let mut node = TerminalNode::new();
        let mut state = TerminalCheckpoint::default();
        node.initialize(config, &runtime, &mut state).await?;

        let report = node
            .scan_historical(
                &mut state,
                Checkpoint::None,
                TimeHorizon::Historical {
                    end_time: Timestamp::now(),
                },
                ScanArgs::default(),
            )
            .await?;

        assert_eq!(report.events_processed, 0);
        assert!(report.successful_targets.is_empty());
        assert_eq!(report.failed_targets.len(), 1);
        assert_eq!(report.failed_targets[0].0, format!("fish:{invalid_history}"));
        assert!(
            report.failed_targets[0]
                .1
                .contains("configured Fish history source"),
            "unexpected failure: {:?}",
            report.failed_targets
        );

        Ok(())
    }

    #[sinex_test]
    async fn run_continuous_rejects_all_invalid_terminal_sources(
        ctx: TestContext,
    ) -> TestResult<()> {
        let TestRuntime { runtime, .. } =
            TestRuntimeBuilder::new(&ctx, "terminal-continuous-invalid-atuin")
                .with_dry_run(true)
                .build()
                .await?;

        let temp_dir = tempfile::tempdir()?;
        let invalid_db = temp_dir.path().join("history.db");
        tokio::fs::write(&invalid_db, "not a sqlite database").await?;
        let invalid_db = Utf8PathBuf::from_path_buf(invalid_db).map_err(|path| {
            color_eyre::eyre::eyre!(
                "invalid Atuin temp path should be utf-8: {}",
                path.display()
            )
        })?;

        let config = TerminalConfig {
            history_sources: vec![HistorySourceConfig {
                path: invalid_db,
                shell: "atuin".to_string(),
            }],
            polling_interval_secs: Seconds::from_secs(5),
            max_capture_bytes: Bytes::from_bytes(1024),
        };

        let mut node = TerminalNode::new();
        let mut state = TerminalCheckpoint::default();
        node.initialize(config, &runtime, &mut state).await?;

        let (_, shutdown_rx) = tokio::sync::watch::channel(false);
        let error = node
            .run_continuous(&mut state, Checkpoint::None, shutdown_rx)
            .await
            .expect_err("continuous mode should fail when no valid sources remain");
        assert!(
            error.to_string().contains("no usable history sources"),
            "unexpected error: {error}"
        );
        assert!(
            error.to_string().contains("atuin:"),
            "failed target context should remain visible: {error}"
        );

        Ok(())
    }

    #[sinex_test]
    async fn run_continuous_rejects_unsupported_fish_history(
        ctx: TestContext,
    ) -> TestResult<()> {
        let TestRuntime { runtime, .. } =
            TestRuntimeBuilder::new(&ctx, "terminal-continuous-invalid-fish")
                .with_dry_run(true)
                .build()
                .await?;

        let temp_dir = tempfile::tempdir()?;
        let invalid_history = temp_dir.path().join("fish_history");
        tokio::fs::write(&invalid_history, "- cmd: echo hello\n  when: 1234567890\n").await?;
        let invalid_history = Utf8PathBuf::from_path_buf(invalid_history).map_err(|path| {
            color_eyre::eyre::eyre!(
                "invalid Fish temp path should be utf-8: {}",
                path.display()
            )
        })?;

        let config = TerminalConfig {
            history_sources: vec![HistorySourceConfig {
                path: invalid_history,
                shell: "fish".to_string(),
            }],
            polling_interval_secs: Seconds::from_secs(5),
            max_capture_bytes: Bytes::from_bytes(1024),
        };

        let mut node = TerminalNode::new();
        let mut state = TerminalCheckpoint::default();
        node.initialize(config, &runtime, &mut state).await?;

        let (_, shutdown_rx) = tokio::sync::watch::channel(false);
        let error = node
            .run_continuous(&mut state, Checkpoint::None, shutdown_rx)
            .await
            .expect_err("continuous mode should fail when Fish history is unsupported");
        assert!(
            error.to_string().contains("no usable history sources"),
            "unexpected error: {error}"
        );
        assert!(
            error.to_string().contains("fish:"),
            "failed target context should remain visible: {error}"
        );
        assert!(
            error
                .to_string()
                .contains("configured Fish history source"),
            "continuous mode should preserve the real Fish SQLite validation error: {error}"
        );

        Ok(())
    }

    #[sinex_test]
    async fn scan_historical_reports_unsupported_elvish_history_per_target(
        ctx: TestContext,
    ) -> TestResult<()> {
        let TestRuntime { runtime, .. } =
            TestRuntimeBuilder::new(&ctx, "terminal-historical-invalid-elvish")
                .with_dry_run(true)
                .build()
                .await?;

        let temp_dir = tempfile::tempdir()?;
        let invalid_history = temp_dir.path().join("elvish.db");
        tokio::fs::write(&invalid_history, "sqlite-like-or-binary-does-not-matter").await?;
        let invalid_history = Utf8PathBuf::from_path_buf(invalid_history).map_err(|path| {
            color_eyre::eyre::eyre!(
                "invalid Elvish temp path should be utf-8: {}",
                path.display()
            )
        })?;

        let config = TerminalConfig {
            history_sources: vec![HistorySourceConfig {
                path: invalid_history.clone(),
                shell: "elvish".to_string(),
            }],
            polling_interval_secs: Seconds::from_secs(5),
            max_capture_bytes: Bytes::from_bytes(1024),
        };

        let mut node = TerminalNode::new();
        let mut state = TerminalCheckpoint::default();
        node.initialize(config, &runtime, &mut state).await?;

        let report = node
            .scan_historical(
                &mut state,
                Checkpoint::None,
                TimeHorizon::Historical {
                    end_time: Timestamp::now(),
                },
                ScanArgs::default(),
            )
            .await?;

        assert_eq!(report.events_processed, 0);
        assert!(report.successful_targets.is_empty());
        assert_eq!(report.failed_targets.len(), 1);
        assert_eq!(report.failed_targets[0].0, format!("elvish:{invalid_history}"));
        assert!(
            report.failed_targets[0]
                .1
                .contains("native database format, which is not supported"),
            "unexpected failure: {:?}",
            report.failed_targets
        );

        Ok(())
    }

    #[sinex_test]
    async fn run_continuous_rejects_unsupported_elvish_history(
        ctx: TestContext,
    ) -> TestResult<()> {
        let TestRuntime { runtime, .. } =
            TestRuntimeBuilder::new(&ctx, "terminal-continuous-invalid-elvish")
                .with_dry_run(true)
                .build()
                .await?;

        let temp_dir = tempfile::tempdir()?;
        let invalid_history = temp_dir.path().join("elvish.db");
        tokio::fs::write(&invalid_history, "sqlite-like-or-binary-does-not-matter").await?;
        let invalid_history = Utf8PathBuf::from_path_buf(invalid_history).map_err(|path| {
            color_eyre::eyre::eyre!(
                "invalid Elvish temp path should be utf-8: {}",
                path.display()
            )
        })?;

        let config = TerminalConfig {
            history_sources: vec![HistorySourceConfig {
                path: invalid_history,
                shell: "elvish".to_string(),
            }],
            polling_interval_secs: Seconds::from_secs(5),
            max_capture_bytes: Bytes::from_bytes(1024),
        };

        let mut node = TerminalNode::new();
        let mut state = TerminalCheckpoint::default();
        node.initialize(config, &runtime, &mut state).await?;

        let (_, shutdown_rx) = tokio::sync::watch::channel(false);
        let error = node
            .run_continuous(&mut state, Checkpoint::None, shutdown_rx)
            .await
            .expect_err("continuous mode should fail when Elvish history is unsupported");
        assert!(
            error.to_string().contains("no usable history sources"),
            "unexpected error: {error}"
        );

        Ok(())
    }

    #[sinex_test]
    async fn run_continuous_preserves_incoming_checkpoint(ctx: TestContext) -> TestResult<()> {
        let TestRuntime { runtime, .. } =
            TestRuntimeBuilder::new(&ctx, "terminal-continuous-preserve-checkpoint")
                .with_dry_run(true)
                .build()
                .await?;

        let temp_dir = tempfile::tempdir()?;
        let history_path = temp_dir.path().join("atuin.db");
        let conn = rusqlite::Connection::open(&history_path)?;
        conn.execute(
            "CREATE TABLE history (
                id TEXT PRIMARY KEY,
                timestamp INTEGER NOT NULL,
                command TEXT NOT NULL,
                cwd TEXT,
                exit INTEGER,
                duration INTEGER,
                hostname TEXT,
                session TEXT,
                deleted_at INTEGER
            )",
            [],
        )?;
        let history_path = Utf8PathBuf::from_path_buf(history_path).map_err(|path| {
            color_eyre::eyre::eyre!("invalid Atuin temp path should be utf-8: {}", path.display())
        })?;

        let config = TerminalConfig {
            history_sources: vec![HistorySourceConfig {
                path: history_path.clone(),
                shell: "atuin".to_string(),
            }],
            polling_interval_secs: Seconds::from_secs(5),
            max_capture_bytes: Bytes::from_bytes(1024),
        };

        let mut node = TerminalNode::new();
        let mut state = TerminalCheckpoint::default();
        node.initialize(config, &runtime, &mut state).await?;

        let incoming = TerminalNode::checkpoint_from_states(HashMap::from([(
            format!("atuin:{history_path}"),
            HistoryState {
                sqlite_row_id: Some(42),
                ..HistoryState::default()
            },
        )]))?;

        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let node_task = tokio::spawn(async move {
            let mut node = node;
            let mut state = state;
            node.run_continuous(&mut state, incoming.clone(), shutdown_rx)
                .await
                .map(|report| (report, incoming))
        });

        tokio::task::yield_now().await;
        let _ = shutdown_tx.send(true);
        let (report, incoming) = node_task.await??;

        let checkpoint_key = format!("atuin:{history_path}");
        let report_state =
            TerminalNode::checkpoint_state_for_source(&report.final_checkpoint, &checkpoint_key)?
                .ok_or_else(|| color_eyre::eyre::eyre!("missing final checkpoint state"))?;
        let incoming_state = TerminalNode::checkpoint_state_for_source(&incoming, &checkpoint_key)?
            .ok_or_else(|| color_eyre::eyre::eyre!("missing incoming checkpoint state"))?;
        assert_eq!(report_state.sqlite_row_id, incoming_state.sqlite_row_id);
        Ok(())
    }

    #[sinex_test]
    async fn run_continuous_overrides_stale_local_checkpoint(ctx: TestContext) -> TestResult<()> {
        let TestRuntime { runtime, .. } =
            TestRuntimeBuilder::new(&ctx, "terminal-continuous-override-checkpoint")
                .with_dry_run(true)
                .build()
                .await?;

        let temp_dir = tempfile::tempdir()?;
        let history_path = temp_dir.path().join("atuin.db");
        let conn = rusqlite::Connection::open(&history_path)?;
        conn.execute(
            "CREATE TABLE history (
                id TEXT PRIMARY KEY,
                timestamp INTEGER NOT NULL,
                command TEXT NOT NULL,
                cwd TEXT,
                exit INTEGER,
                duration INTEGER,
                hostname TEXT,
                session TEXT,
                deleted_at INTEGER
            )",
            [],
        )?;
        let history_path = Utf8PathBuf::from_path_buf(history_path).map_err(|path| {
            color_eyre::eyre::eyre!("invalid Atuin temp path should be utf-8: {}", path.display())
        })?;

        let config = TerminalConfig {
            history_sources: vec![HistorySourceConfig {
                path: history_path.clone(),
                shell: "atuin".to_string(),
            }],
            polling_interval_secs: Seconds::from_secs(5),
            max_capture_bytes: Bytes::from_bytes(1024),
        };

        let mut node = TerminalNode::new();
        let mut state = TerminalCheckpoint::default();
        node.initialize(config, &runtime, &mut state).await?;

        let checkpoint_key = format!("atuin:{history_path}");
        let state_path = node
            .build_history_contexts(tokio::sync::watch::channel(false).1)?
            .into_iter()
            .next()
            .and_then(|ctx| ctx.state_path)
            .ok_or_else(|| color_eyre::eyre::eyre!("watcher should expose a state path"))?;
        tokio::fs::write(
            &state_path,
            serde_json::to_vec_pretty(&HistoryState {
                sqlite_row_id: Some(7),
                ..HistoryState::default()
            })?,
        )
        .await?;

        let incoming = TerminalNode::checkpoint_from_states(HashMap::from([(
            checkpoint_key.clone(),
            HistoryState {
                sqlite_row_id: Some(42),
                ..HistoryState::default()
            },
        )]))?;

        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let task = tokio::spawn(async move {
            let mut node = node;
            let mut state = state;
            node.run_continuous(&mut state, incoming, shutdown_rx).await
        });

        tokio::task::yield_now().await;
        let _ = shutdown_tx.send(true);

        let report = task.await??;
        let final_state =
            TerminalNode::checkpoint_state_for_source(&report.final_checkpoint, &checkpoint_key)?
                .ok_or_else(|| color_eyre::eyre::eyre!("missing final checkpoint state"))?;
        assert_eq!(final_state.sqlite_row_id, Some(42));
        Ok(())
    }

    #[sinex_test]
    async fn run_continuous_preserves_local_state_when_checkpoint_missing(
        ctx: TestContext,
    ) -> TestResult<()> {
        let TestRuntime { runtime, .. } =
            TestRuntimeBuilder::new(&ctx, "terminal-continuous-preserve-local-state")
                .with_dry_run(true)
                .build()
                .await?;

        let temp_dir = tempfile::tempdir()?;
        let history_path = temp_dir.path().join("atuin.db");
        let conn = rusqlite::Connection::open(&history_path)?;
        conn.execute(
            "CREATE TABLE history (
                id TEXT PRIMARY KEY,
                timestamp INTEGER NOT NULL,
                command TEXT NOT NULL,
                cwd TEXT,
                exit INTEGER,
                duration INTEGER,
                hostname TEXT,
                session TEXT,
                deleted_at INTEGER
            )",
            [],
        )?;
        let history_path = Utf8PathBuf::from_path_buf(history_path).map_err(|path| {
            color_eyre::eyre::eyre!("invalid Atuin temp path should be utf-8: {}", path.display())
        })?;

        let config = TerminalConfig {
            history_sources: vec![HistorySourceConfig {
                path: history_path.clone(),
                shell: "atuin".to_string(),
            }],
            polling_interval_secs: Seconds::from_secs(5),
            max_capture_bytes: Bytes::from_bytes(1024),
        };

        let mut node = TerminalNode::new();
        let mut state = TerminalCheckpoint::default();
        node.initialize(config, &runtime, &mut state).await?;

        let checkpoint_key = format!("atuin:{history_path}");
        let state_path = node
            .build_history_contexts(tokio::sync::watch::channel(false).1)?
            .into_iter()
            .next()
            .and_then(|ctx| ctx.state_path)
            .ok_or_else(|| color_eyre::eyre::eyre!("watcher should expose a state path"))?;
        tokio::fs::write(
            &state_path,
            serde_json::to_vec_pretty(&HistoryState {
                sqlite_row_id: Some(7),
                recent_hashes: VecDeque::from([13, 17]),
                ..HistoryState::default()
            })?,
        )
        .await?;

        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let task = tokio::spawn(async move {
            let mut node = node;
            let mut state = state;
            node.run_continuous(&mut state, Checkpoint::None, shutdown_rx)
                .await
        });

        tokio::task::yield_now().await;
        let _ = shutdown_tx.send(true);

        let report = task.await??;
        let final_state =
            TerminalNode::checkpoint_state_for_source(&report.final_checkpoint, &checkpoint_key)?
                .ok_or_else(|| color_eyre::eyre::eyre!("missing final checkpoint state"))?;
        assert_eq!(final_state.sqlite_row_id, Some(7));
        assert_eq!(final_state.recent_hashes, VecDeque::from([13, 17]));
        Ok(())
    }

    #[sinex_test]
    async fn run_continuous_rejects_invalid_incoming_checkpoint(ctx: TestContext) -> TestResult<()> {
        let TestRuntime { runtime, .. } =
            TestRuntimeBuilder::new(&ctx, "terminal-continuous-invalid-checkpoint")
                .with_dry_run(true)
                .build()
                .await?;

        let temp_dir = tempfile::tempdir()?;
        let history_path = temp_dir.path().join("atuin.db");
        let conn = rusqlite::Connection::open(&history_path)?;
        conn.execute(
            "CREATE TABLE history (
                id TEXT PRIMARY KEY,
                timestamp INTEGER NOT NULL,
                command TEXT NOT NULL,
                cwd TEXT,
                exit INTEGER,
                duration INTEGER,
                hostname TEXT,
                session TEXT,
                deleted_at INTEGER
            )",
            [],
        )?;
        let history_path = Utf8PathBuf::from_path_buf(history_path).map_err(|path| {
            color_eyre::eyre::eyre!("invalid Atuin temp path should be utf-8: {}", path.display())
        })?;

        let config = TerminalConfig {
            history_sources: vec![HistorySourceConfig {
                path: history_path.clone(),
                shell: "atuin".to_string(),
            }],
            polling_interval_secs: Seconds::from_secs(5),
            max_capture_bytes: Bytes::from_bytes(1024),
        };

        let mut node = TerminalNode::new();
        let mut state = TerminalCheckpoint::default();
        node.initialize(config, &runtime, &mut state).await?;

        let checkpoint_key = format!("atuin:{history_path}");
        let invalid = Checkpoint::external(
            serde_json::json!({
                "sources": {
                    checkpoint_key.clone(): {
                        "sqlite_row_id": -1
                    }
                }
            }),
            "terminal history source progress",
        );

        let (_, shutdown_rx) = tokio::sync::watch::channel(false);
        let error = node
            .run_continuous(&mut state, invalid, shutdown_rx)
            .await
            .expect_err("continuous mode should reject unusable incoming checkpoints");
        assert!(
            error.to_string().contains("no usable history sources"),
            "unexpected error: {error}"
        );
        assert!(
            error.to_string()
                .contains("failed to restore incoming terminal checkpoint state"),
            "unexpected error: {error}"
        );
        Ok(())
    }

    #[sinex_test]
    async fn run_continuous_warns_when_shutdown_sender_drops(ctx: TestContext) -> TestResult<()> {
        let TestRuntime { runtime, .. } =
            TestRuntimeBuilder::new(&ctx, "terminal-continuous-shutdown-drop")
                .with_dry_run(true)
                .build()
                .await?;

        let temp_dir = tempfile::tempdir()?;
        let history_path = temp_dir.path().join("atuin.db");
        let conn = rusqlite::Connection::open(&history_path)?;
        conn.execute(
            "CREATE TABLE history (
                id TEXT PRIMARY KEY,
                timestamp INTEGER NOT NULL,
                command TEXT NOT NULL,
                cwd TEXT,
                exit INTEGER,
                duration INTEGER,
                hostname TEXT,
                session TEXT,
                deleted_at INTEGER
            )",
            [],
        )?;
        let history_path = Utf8PathBuf::from_path_buf(history_path).map_err(|path| {
            color_eyre::eyre::eyre!("invalid Atuin temp path should be utf-8: {}", path.display())
        })?;

        let config = TerminalConfig {
            history_sources: vec![HistorySourceConfig {
                path: history_path,
                shell: "atuin".to_string(),
            }],
            polling_interval_secs: Seconds::from_secs(5),
            max_capture_bytes: Bytes::from_bytes(1024),
        };

        let mut node = TerminalNode::new();
        let mut state = TerminalCheckpoint::default();
        node.initialize(config, &runtime, &mut state).await?;

        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let task = tokio::spawn(async move {
            let mut node = node;
            let mut state = state;
            node.run_continuous(&mut state, Checkpoint::None, shutdown_rx)
                .await
        });

        tokio::task::yield_now().await;
        drop(shutdown_tx);

        let report = task.await??;
        assert!(
            report.warnings.iter().any(|warning| warning.contains("shutdown channel dropped")),
            "expected shutdown channel drop warning, got: {:?}",
            report.warnings
        );
        Ok(())
    }

    #[sinex_test]
    async fn run_continuous_reports_elapsed_time_window(ctx: TestContext) -> TestResult<()> {
        let TestRuntime { runtime, .. } =
            TestRuntimeBuilder::new(&ctx, "terminal-continuous-time-range")
                .with_dry_run(true)
                .build()
                .await?;

        let temp_dir = tempfile::tempdir()?;
        let history_path = temp_dir.path().join("atuin.db");
        let conn = rusqlite::Connection::open(&history_path)?;
        conn.execute(
            "CREATE TABLE history (
                id TEXT PRIMARY KEY,
                timestamp INTEGER NOT NULL,
                command TEXT NOT NULL,
                cwd TEXT,
                exit INTEGER,
                duration INTEGER,
                hostname TEXT,
                session TEXT,
                deleted_at INTEGER
            )",
            [],
        )?;
        let history_path = Utf8PathBuf::from_path_buf(history_path).map_err(|path| {
            color_eyre::eyre::eyre!("invalid Atuin temp path should be utf-8: {}", path.display())
        })?;

        let config = TerminalConfig {
            history_sources: vec![HistorySourceConfig {
                path: history_path,
                shell: "atuin".to_string(),
            }],
            polling_interval_secs: Seconds::from_secs(5),
            max_capture_bytes: Bytes::from_bytes(1024),
        };

        let mut node = TerminalNode::new();
        let mut state = TerminalCheckpoint::default();
        node.initialize(config, &runtime, &mut state).await?;

        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let task = tokio::spawn(async move {
            let mut node = node;
            let mut state = state;
            node.run_continuous(&mut state, Checkpoint::None, shutdown_rx)
                .await
        });

        tokio::task::yield_now().await;
        let _ = shutdown_tx.send(true);

        let report = task.await??;
        let (window_start, window_end) = report
            .time_range
            .expect("continuous monitoring should report an elapsed time window");
        assert!(window_end >= window_start);
        Ok(())
    }

    #[sinex_test]
    async fn run_continuous_reports_persisted_final_checkpoint(ctx: TestContext) -> TestResult<()> {
        let TestRuntime { runtime, .. } =
            TestRuntimeBuilder::new(&ctx, "terminal-continuous-final-checkpoint")
                .with_dry_run(true)
                .build()
                .await?;

        let temp_dir = tempfile::tempdir()?;
        let history_path = temp_dir.path().join("atuin.db");
        let conn = rusqlite::Connection::open(&history_path)?;
        conn.execute(
            "CREATE TABLE history (
                id TEXT PRIMARY KEY,
                timestamp INTEGER NOT NULL,
                command TEXT NOT NULL,
                cwd TEXT,
                exit INTEGER,
                duration INTEGER,
                hostname TEXT,
                session TEXT,
                deleted_at INTEGER
            )",
            [],
        )?;
        let history_path = Utf8PathBuf::from_path_buf(history_path).map_err(|path| {
            color_eyre::eyre::eyre!("invalid Atuin temp path should be utf-8: {}", path.display())
        })?;

        let config = TerminalConfig {
            history_sources: vec![HistorySourceConfig {
                path: history_path.clone(),
                shell: "atuin".to_string(),
            }],
            polling_interval_secs: Seconds::from_secs(5),
            max_capture_bytes: Bytes::from_bytes(1024),
        };

        let mut node = TerminalNode::new();
        let mut state = TerminalCheckpoint::default();
        node.initialize(config, &runtime, &mut state).await?;

        let state_path = node
            .build_history_contexts(tokio::sync::watch::channel(false).1)?
            .into_iter()
            .next()
            .and_then(|ctx| ctx.state_path)
            .ok_or_else(|| color_eyre::eyre::eyre!("terminal state path missing"))?;
        let expected_state = HistoryState {
            sqlite_row_id: Some(99),
            ..HistoryState::default()
        };
        tokio::fs::write(&state_path, serde_json::to_vec(&expected_state)?).await?;

        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let task = tokio::spawn(async move {
            let mut node = node;
            let mut state = state;
            node.run_continuous(&mut state, Checkpoint::None, shutdown_rx)
                .await
        });

        tokio::task::yield_now().await;
        let _ = shutdown_tx.send(true);

        let report = task.await??;
        assert_eq!(
            report.final_checkpoint,
            TerminalNode::checkpoint_from_states(HashMap::from([(
                format!("atuin:{history_path}"),
                expected_state,
            )]))?
        );
        Ok(())
    }

    #[sinex_test]
    async fn scan_historical_reports_requested_time_window(ctx: TestContext) -> TestResult<()> {
        let TestRuntime { runtime, .. } =
            TestRuntimeBuilder::new(&ctx, "terminal-historical-time-range")
                .with_dry_run(true)
                .build()
                .await?;

        let temp_dir = tempfile::tempdir()?;
        let history_path = temp_dir.path().join("atuin.db");
        let conn = rusqlite::Connection::open(&history_path)?;
        conn.execute(
            "CREATE TABLE history (
                id TEXT PRIMARY KEY,
                timestamp INTEGER NOT NULL,
                command TEXT NOT NULL,
                cwd TEXT,
                exit INTEGER,
                duration INTEGER,
                hostname TEXT,
                session TEXT,
                deleted_at INTEGER
            )",
            [],
        )?;
        let history_path = Utf8PathBuf::from_path_buf(history_path).map_err(|path| {
            color_eyre::eyre::eyre!("invalid Atuin temp path should be utf-8: {}", path.display())
        })?;

        let config = TerminalConfig {
            history_sources: vec![HistorySourceConfig {
                path: history_path,
                shell: "atuin".to_string(),
            }],
            polling_interval_secs: Seconds::from_secs(5),
            max_capture_bytes: Bytes::from_bytes(1024),
        };

        let mut node = TerminalNode::new();
        let mut state = TerminalCheckpoint::default();
        node.initialize(config, &runtime, &mut state).await?;

        let start =
            Timestamp::from_unix_timestamp(1_700_100_000).expect("timestamp should be valid");
        let end =
            Timestamp::from_unix_timestamp(1_700_100_600).expect("timestamp should be valid");

        let report = node
            .scan_historical(
                &mut TerminalCheckpoint::default(),
                Checkpoint::timestamp(start, None),
                TimeHorizon::Historical { end_time: end },
                ScanArgs::default(),
            )
            .await?;

        assert_eq!(report.time_range, Some((start, end)));
        Ok(())
    }

    #[sinex_test]
    async fn get_source_state_marks_misconfigured_sources_unhealthy() -> TestResult<()> {
        let invalid_db = Utf8PathBuf::from("/tmp/definitely-missing-atuin.db");
        let node = TerminalNode::with_config(TerminalConfig {
            history_sources: vec![
                HistorySourceConfig {
                    path: Utf8PathBuf::from("/tmp/.bash_history"),
                    shell: "bash".to_string(),
                },
                HistorySourceConfig {
                    path: invalid_db.clone(),
                    shell: "atuin".to_string(),
                },
            ],
            polling_interval_secs: Seconds::from_secs(5),
            max_capture_bytes: Bytes::from_bytes(1024),
        });

        let state = ExplorationProvider::get_source_state(&node)?;
        assert!(state.is_connected, "bash source should keep node connected");
        assert!(!state.healthy, "misconfigured sources must degrade health");
        assert!(
            state.description.contains("misconfigured"),
            "description should reflect bad source state: {}",
            state.description
        );
        assert_eq!(state.total_items, Some(2));

        let usable_sources = state
            .metadata
            .get("usable_sources")
            .and_then(serde_json::Value::as_u64)
            .ok_or_else(|| color_eyre::eyre::eyre!("usable_sources missing"))?;
        assert_eq!(usable_sources, 1);

        let misconfigured = state
            .metadata
            .get("misconfigured_sources")
            .and_then(serde_json::Value::as_array)
            .ok_or_else(|| color_eyre::eyre::eyre!("misconfigured_sources missing"))?;
        assert_eq!(misconfigured.len(), 1);
        assert!(
            misconfigured[0]["error"]
                .as_str()
                .is_some_and(|error: &str| error.contains("configured Atuin history source")),
            "unexpected misconfigured source payload: {misconfigured:?}"
        );

        Ok(())
    }

    #[sinex_test]
    async fn get_source_state_marks_empty_configuration_unhealthy() -> TestResult<()> {
        let node = TerminalNode::with_config(TerminalConfig {
            history_sources: vec![],
            polling_interval_secs: Seconds::from_secs(5),
            max_capture_bytes: Bytes::from_bytes(1024),
        });

        let state = ExplorationProvider::get_source_state(&node)?;
        assert!(!state.is_connected, "empty configuration must not appear connected");
        assert!(!state.healthy, "empty configuration must not appear healthy");
        assert!(
            state.description.contains("No terminal history sources configured"),
            "description should make the missing configuration explicit: {}",
            state.description
        );
        assert_eq!(state.total_items, Some(0));
        Ok(())
    }

    #[sinex_test]
    async fn terminal_node_reports_coverage_analysis_unavailable() -> TestResult<()> {
        let node = TerminalNode::new();
        let error = sinex_node_sdk::ExplorationProvider::get_coverage_analysis(&node, None)
            .expect_err("terminal node should not fabricate coverage analysis");
        assert!(error.to_string().contains("not implemented"));
        Ok(())
    }

    // ─── PTY-boundary filtering tests ────────────────────────────────────────────
    //
    // These tests are inline (not in tests/) because HistoryWatcherContext is
    // private — extracting to tests/ would require exposing internal structs.
    // Each test simulates writing what a real terminal session would write to
    // $HISTFILE and asserts the canonical captured commands.

    struct WatcherFixture {
        ctx: HistoryWatcherContext,
        commands: Arc<Mutex<Vec<String>>>,
        history_path: std::path::PathBuf,
        _temp_dir: tempfile::TempDir,
        _ingest_handle: xtask::sandbox::TestIngestdHandle,
    }

    async fn make_watcher(
        test_ctx: &TestContext,
        test_name: &str,
        max_capture_bytes: u64,
    ) -> TestResult<WatcherFixture> {
        let TestRuntime { runtime, nats, .. } = TestRuntimeBuilder::new(test_ctx, test_name)
            .with_dry_run(false)
            .build()
            .await?;

        let ingest_config = TestIngestdConfig {
            nats: nats.connection_config(),
            database_url: test_ctx.database_url().to_string(),
            work_dir: None,
            ..Default::default()
        };
        let ingest_handle = start_test_ingestd_with_config(ingest_config, Some(test_ctx)).await?;

        let publisher = match runtime.transport() {
            sinex_node_sdk::EventTransport::Nats(publisher) => publisher.clone(),
        };
        AcquisitionManager::bootstrap_streams(publisher.nats_client()).await?;

        let acquisition = Arc::new(runtime.acquisition_manager(
            RotationPolicy::default(),
            "terminal-history",
            "/tmp/test-history",
        )?);
        let stage_context = StageAsYouGoContext::from_runtime(&runtime)
            .with_acquisition_manager(Arc::clone(&acquisition));

        let temp_dir = tempfile::tempdir()?;
        let history_path = temp_dir.path().join("history.txt");
        let state_path = temp_dir.path().join("history_state.json");
        let history_utf8 = Utf8PathBuf::from_path_buf(history_path.clone())
            .map_err(|p| color_eyre::eyre::eyre!("path not utf8: {}", p.display()))?;

        let mut ctx = HistoryWatcherContext {
            acquisition,
            stage_context,
            metrics: TerminalMetrics::new(),
            shell: "bash".to_string(),
            path: history_utf8,
            max_capture_bytes: Bytes::from_bytes(max_capture_bytes),
            polling_interval: Duration::from_millis(50),
            state_path: Some(state_path),
            shutdown_rx: tokio::sync::watch::channel(false).1,
            #[cfg(test)]
            processed_commands: None,
            source_mode: HistorySourceMode::Text,
            initial_state_override: None,
        };

        let commands = Arc::new(Mutex::new(Vec::new()));
        #[cfg(test)]
        {
            ctx.processed_commands = Some(commands.clone());
        }

        Ok(WatcherFixture {
            ctx,
            commands,
            history_path,
            _temp_dir: temp_dir,
            _ingest_handle: ingest_handle,
        })
    }

    #[sinex_test]
    async fn load_state_surfaces_corrupt_state_files(ctx: TestContext) -> TestResult<()> {
        let ctx = ctx.with_nats().dedicated().await?;
        let fix = make_watcher(&ctx, "corrupt-state-load", 4096).await?;
        let state_path = fix
            .ctx
            .state_path
            .clone()
            .ok_or_else(|| color_eyre::eyre::eyre!("watcher should have a state path"))?;
        tokio::fs::write(&state_path, "{ definitely not valid json").await?;

        let error = fix
            .ctx
            .load_state()
            .await
            .expect_err("corrupt state file should surface");
        let message = format!("{error:#}");
        assert!(message.contains("failed to decode history watcher state"));
        assert!(message.contains(state_path.display().to_string().as_str()));
        Ok(())
    }

    #[sinex_test]
    async fn scan_history_once_from_state_fails_on_corrupt_local_state(
        ctx: TestContext,
    ) -> TestResult<()> {
        let ctx = ctx.with_nats().dedicated().await?;
        let fix = make_watcher(&ctx, "corrupt-state-scan", 4096).await?;
        tokio::fs::write(&fix.history_path, "echo hello\n").await?;
        let state_path = fix
            .ctx
            .state_path
            .clone()
            .ok_or_else(|| color_eyre::eyre::eyre!("watcher should have a state path"))?;
        tokio::fs::write(&state_path, "{ definitely not valid json").await?;

        let outcome = fix.ctx.scan_history_once_from_state(None, None).await;
        assert_eq!(outcome.processed, 0);
        let failure = outcome
            .failure
            .ok_or_else(|| color_eyre::eyre::eyre!("corrupt state should fail the scan"))?;
        assert!(failure.contains("failed to restore terminal history watcher state"));
        assert!(failure.contains("failed to decode history watcher state"));
        assert!(fix.commands.lock().await.is_empty());
        Ok(())
    }

    #[sinex_test]
    async fn scan_history_once_from_state_fails_on_sqlite_state_missing_row_id(
        ctx: TestContext,
    ) -> TestResult<()> {
        let ctx = ctx.with_nats().dedicated().await?;
        let mut fix = make_watcher(&ctx, "sqlite-state-missing-row-id", 4096).await?;
        fix.ctx.source_mode = HistorySourceMode::AtuinSqlite;
        fix.ctx.shell = "atuin".to_string();
        tokio::fs::write(&fix.history_path, "ignored\n").await?;
        let state_path = fix
            .ctx
            .state_path
            .clone()
            .ok_or_else(|| color_eyre::eyre::eyre!("watcher should have a state path"))?;
        tokio::fs::write(
            &state_path,
            serde_json::json!({
                "offset_bytes": 0,
                "line_number": 0,
                "pending_timestamp": null,
                "recent_hashes": [17, 23]
            })
            .to_string(),
        )
        .await?;

        let outcome = fix.ctx.scan_history_once_from_state(None, None).await;
        assert_eq!(outcome.processed, 0);
        let failure = outcome
            .failure
            .ok_or_else(|| color_eyre::eyre::eyre!("invalid sqlite state should fail the scan"))?;
        assert!(failure.contains("missing sqlite_row_id"));
        assert!(fix.commands.lock().await.is_empty());
        Ok(())
    }

    #[sinex_test]
    async fn scan_history_once_from_state_does_not_advance_past_warned_sqlite_row(
        ctx: TestContext,
    ) -> TestResult<()> {
        let ctx = ctx.with_nats().dedicated().await?;
        let mut fix = make_watcher(&ctx, "sqlite-warning-checkpoint", 4096).await?;
        fix.ctx.source_mode = HistorySourceMode::AtuinSqlite;
        fix.ctx.shell = "atuin".to_string();

        let history_path = fix.history_path.with_extension("sqlite");
        let conn = rusqlite::Connection::open(&history_path)?;
        conn.execute(
            "CREATE TABLE history (
                id TEXT PRIMARY KEY,
                timestamp INTEGER NOT NULL,
                command TEXT NOT NULL,
                cwd TEXT,
                exit INTEGER,
                duration INTEGER,
                hostname TEXT,
                session TEXT,
                deleted_at INTEGER
            )",
            [],
        )?;
        conn.execute(
            "INSERT INTO history (id, timestamp, command, cwd, exit, duration, hostname, session, deleted_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, NULL)",
            rusqlite::params![
                "bad-row",
                1_700_000_000_000_000_000_i64,
                "echo broken",
                "/tmp",
                0_i64,
                -1_i64,
                "test-host",
                "session-1",
            ],
        )?;
        conn.execute(
            "INSERT INTO history (id, timestamp, command, cwd, exit, duration, hostname, session, deleted_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, NULL)",
            rusqlite::params![
                "good-row",
                1_700_000_000_000_000_100_i64,
                "echo should-not-run-yet",
                "/tmp",
                0_i64,
                1_i64,
                "test-host",
                "session-1",
            ],
        )?;

        fix.ctx.path = Utf8PathBuf::from_path_buf(history_path.clone()).map_err(|path| {
            color_eyre::eyre::eyre!("invalid Atuin temp path should be utf-8: {}", path.display())
        })?;

        let outcome = fix
            .ctx
            .scan_history_once_from_state(Some(fix.ctx.empty_state()), None)
            .await;
        assert_eq!(outcome.processed, 0);
        assert!(
            outcome
                .warnings
                .iter()
                .any(|warning| warning.contains("failed to process Atuin row 1")),
            "expected row warning, got {:?}",
            outcome.warnings
        );
        assert_eq!(outcome.state.sqlite_row_id, Some(0));
        assert!(
            fix.commands.lock().await.is_empty(),
            "rows after the warned row must not be emitted before the checkpoint advances"
        );
        fix._ingest_handle.stop().await?;
        Ok(())
    }

    /// Invariant: commands containing null bytes (\0) are rejected — they indicate
    /// binary data or corrupted history entries, not shell commands.
    #[sinex_test]
    async fn history_rejects_null_byte_commands(ctx: TestContext) -> TestResult<()> {
        let ctx = ctx.with_nats().dedicated().await?;
        let mut fix = make_watcher(&ctx, "null-byte-filter", 4096).await?;
        tokio::fs::write(&fix.history_path, "echo hello\necho\x00null\ngit status\n").await?;

        let mut offset = 0u64;
        let mut line_number = 0u64;
        let mut pending_timestamp = None;
        let mut last_inode: Option<u64> = None;
        let mut hashes: VecDeque<u64> = VecDeque::new();
        #[cfg(unix)]
        fix.ctx
            .poll_history_once(
                &mut offset,
                &mut line_number,
                &mut pending_timestamp,
                &mut last_inode,
                &mut hashes,
                true,
            )
            .await?;

        let commands = fix.commands.lock().await.clone();
        assert!(
            !commands.iter().any(|c| c.contains('\0')),
            "commands with null bytes must be rejected, got: {commands:?}"
        );
        assert!(
            commands.contains(&"echo hello".to_string()),
            "clean commands before null-byte line must still be captured, got: {commands:?}"
        );
        assert!(
            commands.contains(&"git status".to_string()),
            "clean commands after null-byte line must still be captured, got: {commands:?}"
        );
        fix._ingest_handle.stop().await?;
        Ok(())
    }

    /// Invariant: commands containing ANSI escape sequences or other non-printable
    /// control characters are rejected — they indicate readline corruption or terminal
    /// escape sequences that were erroneously written to the history file.
    #[sinex_test]
    async fn history_rejects_ansi_escape_commands(ctx: TestContext) -> TestResult<()> {
        let ctx = ctx.with_nats().dedicated().await?;
        let mut fix = make_watcher(&ctx, "ansi-escape-filter", 4096).await?;
        // \x1b = ESC (start of ANSI escape sequence like \x1b[A = cursor up)
        tokio::fs::write(
            &fix.history_path,
            "echo clean\necho\x1b[Acorrupted\nls -la\n",
        )
        .await?;

        let mut offset = 0u64;
        let mut line_number = 0u64;
        let mut pending_timestamp = None;
        let mut last_inode: Option<u64> = None;
        let mut hashes: VecDeque<u64> = VecDeque::new();
        #[cfg(unix)]
        fix.ctx
            .poll_history_once(
                &mut offset,
                &mut line_number,
                &mut pending_timestamp,
                &mut last_inode,
                &mut hashes,
                true,
            )
            .await?;

        let commands = fix.commands.lock().await.clone();
        assert!(
            !commands.iter().any(|c| c.contains('\x1b')),
            "commands with ANSI escapes must be rejected, got: {commands:?}"
        );
        assert!(
            commands.contains(&"echo clean".to_string()),
            "clean commands must be captured, got: {commands:?}"
        );
        fix._ingest_handle.stop().await?;
        Ok(())
    }

    /// Invariant: the same command appearing twice in the history file produces
    /// exactly one captured event — the dedup window prevents duplicate ingestion.
    #[sinex_test]
    async fn history_deduplicates_repeated_commands(ctx: TestContext) -> TestResult<()> {
        let ctx = ctx.with_nats().dedicated().await?;
        let mut fix = make_watcher(&ctx, "dedup-filter", 4096).await?;
        tokio::fs::write(&fix.history_path, "git status\ngit diff\ngit status\n").await?;

        let mut offset = 0u64;
        let mut line_number = 0u64;
        let mut pending_timestamp = None;
        let mut last_inode: Option<u64> = None;
        let mut hashes: VecDeque<u64> = VecDeque::new();
        #[cfg(unix)]
        fix.ctx
            .poll_history_once(
                &mut offset,
                &mut line_number,
                &mut pending_timestamp,
                &mut last_inode,
                &mut hashes,
                true,
            )
            .await?;

        let commands = fix.commands.lock().await.clone();
        assert_eq!(
            commands
                .iter()
                .filter(|c| c.as_str() == "git status")
                .count(),
            1,
            "repeated 'git status' must be deduplicated to exactly 1 capture, got: {commands:?}"
        );
        fix._ingest_handle.stop().await?;
        Ok(())
    }

    /// Invariant: a partial line at the end of the history file (no trailing newline)
    /// is not captured on the first poll — it's held until the line is complete.
    /// This prevents capturing half-written commands that are still being typed.
    #[sinex_test]
    async fn history_withholds_incomplete_trailing_line(ctx: TestContext) -> TestResult<()> {
        let ctx = ctx.with_nats().dedicated().await?;
        let mut fix = make_watcher(&ctx, "incomplete-line", 4096).await?;
        // "echo complete" has a newline; "echo incomplete" does not
        tokio::fs::write(&fix.history_path, "echo complete\necho incomplete").await?;

        let mut offset = 0u64;
        let mut line_number = 0u64;
        let mut pending_timestamp = None;
        let mut last_inode: Option<u64> = None;
        let mut hashes: VecDeque<u64> = VecDeque::new();
        #[cfg(unix)]
        fix.ctx
            .poll_history_once(
                &mut offset,
                &mut line_number,
                &mut pending_timestamp,
                &mut last_inode,
                &mut hashes,
                true,
            )
            .await?;

        let after_first_poll = fix.commands.lock().await.clone();
        assert!(
            after_first_poll.contains(&"echo complete".to_string()),
            "complete line must be captured on first poll, got: {after_first_poll:?}"
        );
        assert!(
            !after_first_poll.contains(&"echo incomplete".to_string()),
            "incomplete line (no trailing newline) must NOT be captured on first poll, got: {after_first_poll:?}"
        );

        // Now append the terminating newline — next poll must capture the previously held line
        let mut f = tokio::fs::OpenOptions::new()
            .append(true)
            .open(&fix.history_path)
            .await?;
        tokio::io::AsyncWriteExt::write_all(&mut f, b"\n").await?;
        f.flush().await?;
        drop(f);

        #[cfg(unix)]
        fix.ctx
            .poll_history_once(
                &mut offset,
                &mut line_number,
                &mut pending_timestamp,
                &mut last_inode,
                &mut hashes,
                true,
            )
            .await?;

        let after_second_poll = fix.commands.lock().await.clone();
        assert!(
            after_second_poll.contains(&"echo incomplete".to_string()),
            "line completed by newline must be captured on subsequent poll, got: {after_second_poll:?}"
        );
        fix._ingest_handle.stop().await?;
        Ok(())
    }

    /// Invariant: a command whose byte length exceeds `max_capture_bytes` is dropped
    /// entirely — the ingestor does not truncate silently, it skips and logs.
    #[sinex_test]
    async fn history_rejects_oversized_commands(ctx: TestContext) -> TestResult<()> {
        let ctx = ctx.with_nats().dedicated().await?;
        let max_bytes = 64u64;
        let mut fix = make_watcher(&ctx, "oversized-cmd", max_bytes).await?;

        let oversized = "A".repeat(max_bytes as usize + 1);
        let content = format!("echo small\n{oversized}\ngit log\n");
        tokio::fs::write(&fix.history_path, &content).await?;

        let mut offset = 0u64;
        let mut line_number = 0u64;
        let mut pending_timestamp = None;
        let mut last_inode: Option<u64> = None;
        let mut hashes: VecDeque<u64> = VecDeque::new();
        #[cfg(unix)]
        fix.ctx
            .poll_history_once(
                &mut offset,
                &mut line_number,
                &mut pending_timestamp,
                &mut last_inode,
                &mut hashes,
                true,
            )
            .await?;

        let commands = fix.commands.lock().await.clone();
        assert!(
            !commands.iter().any(|c| c.len() > max_bytes as usize),
            "oversized command must be dropped entirely (not truncated), got: {commands:?}"
        );
        assert!(
            commands.contains(&"echo small".to_string()),
            "commands within size limit must still be captured, got: {commands:?}"
        );
        fix._ingest_handle.stop().await?;
        Ok(())
    }

    #[sinex_test]
    async fn shutdown_waits_for_watcher_handles() -> TestResult<()> {
        let mut node = TerminalNode::default();
        let (done_tx, done_rx) = tokio::sync::oneshot::channel();

        {
            let mut guard = node.watch_handles.lock().await;
            guard.push(tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(25)).await;
                let _ = done_tx.send(());
                Ok::<(), SinexError>(())
            }));
        }

        node.shutdown(&TerminalCheckpoint::default()).await?;
        done_rx.await?;
        Ok(())
    }

    #[sinex_test]
    async fn shutdown_surfaces_watcher_failures() -> TestResult<()> {
        let mut node = TerminalNode::default();

        {
            let mut guard = node.watch_handles.lock().await;
            guard.push(tokio::spawn(async {
                Err::<(), _>(SinexError::processing(
                    "terminal watcher exploded before shutdown",
                ))
            }));
        }

        let error = node
            .shutdown(&TerminalCheckpoint::default())
            .await
            .expect_err("shutdown should surface watcher failures");
        assert!(
            error
                .to_string()
                .contains("terminal watcher exploded before shutdown"),
            "unexpected error: {error}"
        );
        Ok(())
    }

    #[sinex_test]
    async fn shutdown_waits_for_remaining_handles_after_failure() -> TestResult<()> {
        let mut node = TerminalNode::default();
        let start = Instant::now();

        {
            let mut guard = node.watch_handles.lock().await;
            guard.push(tokio::spawn(async {
                Err::<(), _>(SinexError::processing("first terminal watcher failed"))
            }));
            guard.push(tokio::spawn(async {
                tokio::time::sleep(Duration::from_millis(25)).await;
                Ok::<(), SinexError>(())
            }));
        }

        let error = node
            .shutdown(&TerminalCheckpoint::default())
            .await
            .expect_err("shutdown should wait for all watcher handles before returning");
        assert!(
            start.elapsed() >= Duration::from_millis(25),
            "shutdown returned before awaiting the later watcher handle",
        );
        assert!(
            error.to_string().contains("first terminal watcher failed"),
            "unexpected error: {error}",
        );
        Ok(())
    }
}

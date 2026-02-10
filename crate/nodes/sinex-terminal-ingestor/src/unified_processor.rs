#![doc = include_str!("../docs/overview.md")]

//! Terminal processor that tails configured history files and emits structured
//! command events. Each discovered command is captured as a source material via
//! `AcquisitionManager` and published to `JetStream`, while the structured event
//! is emitted through the shared Stage-as-You-Go channel.

use async_trait::async_trait;
use camino::Utf8PathBuf;
use serde::{Deserialize, Serialize};
use serde_json;
use sinex_node_sdk::{
    acquisition_manager::{AcquisitionManager, RotationPolicy},
    simple_ingestor::SimpleIngestor,
    stage_as_you_go::StageAsYouGoContext,
    stream_processor::{
        Checkpoint, NodeRuntimeState, ScanArgs, ScanReport, ServiceInfo, TimeHorizon,
    },
    NodeResult, SinexError,
};
use sinex_primitives::Ulid;
use sinex_primitives::{
    domain::SanitizedPath, events::EventPayload, temporal::Timestamp, validate_path, Bytes, Seconds,
};
use sinex_processor_runtime::{
    CoverageAnalysis, ExplorationProvider, ExportFormat, IngestionHistoryEntry, SourceState,
};
use std::{collections::HashMap, path::PathBuf, sync::Arc, time::Duration};
use tokio::{
    fs,
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    sync::{watch, Mutex},
};
use tracing::{debug, info, warn};
use validator::ValidationError;

const MATERIAL_REASON_HISTORY: &str = "terminal-history";

// Default configuration values
const DEFAULT_POLLING_INTERVAL: Seconds = Seconds::from_secs(5);
const DEFAULT_MAX_CAPTURE_BYTES: Bytes = Bytes::from_bytes(32 * 1024); // 32 KiB
const ENV_POLLING_INTERVAL: &str = "SINEX_TERMINAL_POLLING_INTERVAL_SECS";

// TODO(metrics): Add terminal event metrics for command rates, shell types, and polling performance.
// Useful metrics include:
// - commands_processed_total (counter, labeled by shell_type)
// - polling_duration_seconds (histogram, labeled by shell_type, source_path)
// - history_file_size_bytes (gauge, labeled by source_path)
// - command_size_bytes (histogram)
// - processing_errors_total (counter, labeled by error_type)

/// Configuration for a shell history source.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistorySourceConfig {
    pub path: Utf8PathBuf,

    /// Shell type (bash, zsh, fish, etc.)
    pub shell: String,
}

use crate::secret_redaction::SecretRedactor;

fn validate_history_path(path: &Utf8PathBuf) -> Result<(), ValidationError> {
    validate_path(path.as_str())
        .map(|_| ())
        .map_err(|_| ValidationError::new("invalid_history_path"))
}

/// Terminal processor configuration.
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
        let home = dirs::home_dir()
            .and_then(|p| Utf8PathBuf::from_path_buf(p).ok())
            .unwrap_or_else(|| Utf8PathBuf::from("/tmp"));

        let default_sources = vec![
            HistorySourceConfig {
                path: home.join(".bash_history"),
                shell: "bash".to_string(),
            },
            HistorySourceConfig {
                path: home.join(".zsh_history"),
                shell: "zsh".to_string(),
            },
        ];

        // Allow polling interval override via environment variable
        let polling_interval_secs = std::env::var(ENV_POLLING_INTERVAL)
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .map_or(DEFAULT_POLLING_INTERVAL, Seconds::from_secs);

        Self {
            history_sources: default_sources,
            polling_interval_secs,
            max_capture_bytes: DEFAULT_MAX_CAPTURE_BYTES,
        }
    }
}

impl TerminalConfig {
    pub fn validate_config(&self) -> Result<(), String> {
        if self.history_sources.is_empty() {
            return Err("At least one history source must be configured".to_string());
        }

        for source in &self.history_sources {
            validate_history_path(&source.path)
                .map_err(|_| "Invalid history file path".to_string())?;
            if source.shell.trim().is_empty() {
                return Err("Shell type cannot be empty".to_string());
            }
        }

        let polling_secs = self.polling_interval_secs.as_secs();
        if !(1..=3600).contains(&polling_secs) {
            return Err("Polling interval must be between 1 and 3600 seconds".to_string());
        }

        let max_bytes = self.max_capture_bytes.as_u64();
        if !(64..=1024 * 1024).contains(&max_bytes) {
            return Err("Max capture bytes must be between 64B and 1MB".to_string());
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

#[derive(Debug, Default, Serialize, Deserialize)]
struct HistoryState {
    offset_bytes: u64,
    line_number: u64,
    /// Inode of the file when last processed (Unix only, used to detect rotation vs truncation)
    #[cfg(unix)]
    inode: Option<u64>,
    /// For Fish `SQLite` history: last processed ROWID
    fish_row_id: Option<i64>,
    /// Rolling window of command content hashes for deduplication across file rotation/truncation.
    /// When a history file is rotated (new inode), old commands may reappear; this set prevents
    /// duplicate events from being emitted.
    #[serde(default)]
    recent_hashes: Vec<u64>,
}

#[derive(Clone)]
struct HistoryWatcherContext {
    acquisition: Arc<AcquisitionManager>,
    stage_context: StageAsYouGoContext,
    shell: String,
    path: Utf8PathBuf,
    max_capture_bytes: Bytes,
    polling_interval: Duration,
    state_path: Option<PathBuf>,
    shutdown_rx: watch::Receiver<bool>,
    processed_commands: Option<Arc<Mutex<Vec<String>>>>,
    /// True if this is a Fish `SQLite` history database
    is_fish_sqlite: bool,
}

impl HistoryWatcherContext {
    async fn monitor(self) {
        if self.is_fish_sqlite {
            self.monitor_fish_sqlite().await;
        } else {
            self.monitor_text_history().await;
        }
    }

    async fn monitor_text_history(self) {
        let mut offset_bytes: u64 = 0;
        let mut line_number: u64 = 0;
        #[cfg(unix)]
        let mut last_inode: Option<u64> = None;
        let mut recent_hashes: Vec<u64> = Vec::new();
        let mut shutdown_rx = self.shutdown_rx.clone();

        if let Some(state) = self.load_state().await {
            offset_bytes = state.offset_bytes;
            line_number = state.line_number;
            recent_hashes = state.recent_hashes;
            #[cfg(unix)]
            {
                last_inode = state.inode;
            }
            debug!(
                path = %self.path,
                offset = offset_bytes,
                line_number,
                dedup_hashes = recent_hashes.len(),
                "Restored terminal watcher state"
            );
        }

        loop {
            if *shutdown_rx.borrow() {
                info!(path = %self.path, "History watcher shutdown requested");
                break;
            }

            #[cfg(unix)]
            {
                let _ = self
                    .poll_history_once(
                        &mut offset_bytes,
                        &mut line_number,
                        &mut last_inode,
                        &mut recent_hashes,
                    )
                    .await;
            }
            #[cfg(not(unix))]
            {
                let _ = self
                    .poll_history_once(&mut offset_bytes, &mut line_number, &mut recent_hashes)
                    .await;
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
    }

    async fn monitor_fish_sqlite(self) {
        let mut fish_row_id: i64 = 0;
        let mut recent_hashes: Vec<u64> = Vec::new();
        let mut shutdown_rx = self.shutdown_rx.clone();

        if let Some(state) = self.load_state().await {
            fish_row_id = state.fish_row_id.unwrap_or(0);
            recent_hashes = state.recent_hashes;
            debug!(
                path = %self.path,
                fish_row_id,
                dedup_hashes = recent_hashes.len(),
                "Restored Fish history watcher state"
            );
        }

        loop {
            if *shutdown_rx.borrow() {
                info!(path = %self.path, "Fish history watcher shutdown requested");
                break;
            }

            let _ = self
                .poll_fish_history_once(&mut fish_row_id, &mut recent_hashes)
                .await;

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
    }

    async fn load_state(&self) -> Option<HistoryState> {
        let path = self.state_path.as_ref()?;
        match fs::read(path).await {
            Ok(bytes) => match serde_json::from_slice::<HistoryState>(&bytes) {
                Ok(state) => Some(state),
                Err(e) => {
                    warn!("Failed to decode history watcher state {:?}: {}", path, e);
                    None
                }
            },
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => None,
            Err(err) => {
                warn!("Failed to load history watcher state {:?}: {}", path, err);
                None
            }
        }
    }

    async fn persist_state(&self, offset_bytes: u64, line_number: u64, recent_hashes: &[u64]) {
        self.persist_state_full(offset_bytes, line_number, None, recent_hashes)
            .await;
    }

    async fn persist_fish_state(&self, fish_row_id: i64, recent_hashes: &[u64]) {
        self.persist_state_full(0, 0, Some(fish_row_id), recent_hashes)
            .await;
    }

    async fn persist_state_full(
        &self,
        offset_bytes: u64,
        line_number: u64,
        fish_row_id: Option<i64>,
        recent_hashes: &[u64],
    ) {
        let Some(path) = &self.state_path else {
            return;
        };

        // Get current inode for tracking file rotation vs truncation
        #[cfg(unix)]
        let current_inode = {
            use std::os::unix::fs::MetadataExt;
            std::fs::metadata(self.path.as_std_path())
                .ok()
                .map(|m| m.ino())
        };

        let state = HistoryState {
            offset_bytes,
            line_number,
            #[cfg(unix)]
            inode: current_inode,
            fish_row_id,
            recent_hashes: recent_hashes.to_vec(),
        };

        match serde_json::to_vec_pretty(&state) {
            Ok(serialized) => {
                if let Some(parent) = path.parent() {
                    if let Err(e) = fs::create_dir_all(parent).await {
                        warn!(
                            "Failed to create history watcher state dir {:?}: {}",
                            parent, e
                        );
                        return;
                    }
                }

                let file_name = path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .unwrap_or("history_state");
                let temp_path = path
                    .parent()
                    .unwrap_or_else(|| std::path::Path::new("."))
                    .join(format!("{}.{}.tmp", file_name, Ulid::new()));

                match fs::OpenOptions::new()
                    .create_new(true)
                    .write(true)
                    .open(&temp_path)
                    .await
                {
                    Ok(mut file) => {
                        if let Err(e) = file.write_all(&serialized).await {
                            warn!(
                                "Failed to persist history watcher state {:?}: {}",
                                temp_path, e
                            );
                            let _ = fs::remove_file(&temp_path).await;
                            return;
                        }
                        if let Err(e) = file.sync_all().await {
                            warn!(
                                "Failed to fsync history watcher state {:?}: {}",
                                temp_path, e
                            );
                            let _ = fs::remove_file(&temp_path).await;
                            return;
                        }
                        if let Err(e) = fs::rename(&temp_path, path).await {
                            warn!("Failed to replace history watcher state {:?}: {}", path, e);
                            let _ = fs::remove_file(&temp_path).await;
                        } else {
                            // Fsync the parent directory to ensure the rename is durable.
                            // Without this, the renamed file might not be visible after a crash.
                            if let Some(parent) = path.parent() {
                                if let Ok(dir) = std::fs::File::open(parent) {
                                    if let Err(e) = dir.sync_all() {
                                        warn!(
                                            "Failed to fsync parent directory {:?}: {}",
                                            parent, e
                                        );
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        warn!(
                            "Failed to create history watcher temp state {:?}: {}",
                            temp_path, e
                        );
                    }
                }
            }
            Err(e) => warn!("Failed to serialize history watcher state: {}", e),
        }
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
        last_inode: &mut Option<u64>,
        recent_hashes: &mut Vec<u64>,
    ) -> usize {
        use std::os::unix::fs::MetadataExt;

        let mut processed = 0usize;
        match fs::metadata(&self.path).await {
            Ok(metadata) => {
                let file_size = metadata.len();
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
                        // Keep line_number as-is; we don't know exactly where we are
                    }
                    self.persist_state(*offset_bytes, *line_number, recent_hashes)
                        .await;
                    return processed;
                }

                if file_size == *offset_bytes {
                    return processed;
                }

                match self.read_new_segment(*offset_bytes).await {
                    Ok(new_segment) => {
                        if new_segment.is_empty() {
                            return processed;
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

                            *line_number += 1;

                            match process_command(self, trimmed, *line_number, recent_hashes).await
                            {
                                Ok(()) => {
                                    processed += 1;
                                }
                                Err(e) => {
                                    warn!(
                                        "Failed to process history entry from {}: {}",
                                        self.path, e
                                    );
                                }
                            };
                        }

                        if consumed_bytes > 0 {
                            *offset_bytes = offset_bytes.saturating_add(consumed_bytes);
                            self.persist_state(*offset_bytes, *line_number, recent_hashes)
                                .await;
                        }
                    }
                    Err(e) => warn!("History watcher unable to read {}: {}", self.path, e),
                }
            }
            Err(e) => {
                warn!("History watcher unable to stat {}: {}", self.path, e);
            }
        }

        processed
    }

    /// Poll history file for new content (non-Unix version without inode tracking)
    #[cfg(not(unix))]
    async fn poll_history_once(
        &self,
        offset_bytes: &mut u64,
        line_number: &mut u64,
        recent_hashes: &mut Vec<u64>,
    ) -> usize {
        let mut processed = 0usize;
        match fs::metadata(&self.path).await {
            Ok(metadata) => {
                let file_size = metadata.len();

                if file_size < *offset_bytes {
                    debug!(
                        path = %self.path,
                        previous_offset = *offset_bytes,
                        new_size = file_size,
                        "History file truncated; resetting offsets"
                    );
                    *offset_bytes = 0;
                    *line_number = 0;
                    self.persist_state(*offset_bytes, *line_number, recent_hashes)
                        .await;
                    return processed;
                }

                if file_size == *offset_bytes {
                    return processed;
                }

                match self.read_new_segment(*offset_bytes).await {
                    Ok(new_segment) => {
                        if new_segment.is_empty() {
                            return processed;
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

                            *line_number += 1;

                            match process_command(self, trimmed, *line_number, recent_hashes).await
                            {
                                Ok(()) => {
                                    processed += 1;
                                }
                                Err(e) => {
                                    warn!(
                                        "Failed to process history entry from {}: {}",
                                        self.path, e
                                    );
                                }
                            };
                        }

                        if consumed_bytes > 0 {
                            *offset_bytes = offset_bytes.saturating_add(consumed_bytes);
                            self.persist_state(*offset_bytes, *line_number, recent_hashes)
                                .await;
                        }
                    }
                    Err(e) => warn!("History watcher unable to read {}: {}", self.path, e),
                }
            }
            Err(e) => {
                warn!("History watcher unable to stat {}: {}", self.path, e);
            }
        }

        processed
    }

    async fn scan_history_once(&self) -> usize {
        if self.is_fish_sqlite {
            let mut fish_row_id = 0i64;
            let mut recent_hashes: Vec<u64> = Vec::new();

            if let Some(state) = self.load_state().await {
                fish_row_id = state.fish_row_id.unwrap_or(0);
                recent_hashes = state.recent_hashes;
            }

            self.poll_fish_history_once(&mut fish_row_id, &mut recent_hashes)
                .await
        } else {
            let mut offset_bytes = 0u64;
            let mut line_number = 0u64;
            let mut recent_hashes: Vec<u64> = Vec::new();
            #[cfg(unix)]
            let mut last_inode: Option<u64> = None;

            if let Some(state) = self.load_state().await {
                offset_bytes = state.offset_bytes;
                line_number = state.line_number;
                recent_hashes = state.recent_hashes;
                #[cfg(unix)]
                {
                    last_inode = state.inode;
                }
            }

            #[cfg(unix)]
            {
                self.poll_history_once(
                    &mut offset_bytes,
                    &mut line_number,
                    &mut last_inode,
                    &mut recent_hashes,
                )
                .await
            }
            #[cfg(not(unix))]
            {
                self.poll_history_once(&mut offset_bytes, &mut line_number, &mut recent_hashes)
                    .await
            }
        }
    }

    async fn poll_fish_history_once(
        &self,
        fish_row_id: &mut i64,
        recent_hashes: &mut Vec<u64>,
    ) -> usize {
        use crate::fish_history;

        let mut processed = 0usize;

        match fish_history::read_fish_history(&self.path, *fish_row_id) {
            Ok((entries, last_row_id)) => {
                for entry in entries {
                    if entry.command.trim().is_empty() {
                        continue;
                    }

                    match process_command(self, &entry.command, last_row_id as u64, recent_hashes)
                        .await
                    {
                        Ok(()) => {
                            processed += 1;
                        }
                        Err(e) => {
                            warn!(
                                "Failed to process Fish history entry from {}: {}",
                                self.path, e
                            );
                        }
                    }
                }

                if last_row_id > *fish_row_id {
                    *fish_row_id = last_row_id;
                    self.persist_fish_state(*fish_row_id, recent_hashes).await;
                }
            }
            Err(e) => {
                warn!("Fish history watcher unable to read {}: {}", self.path, e);
            }
        }

        processed
    }
}

async fn process_command(
    ctx: &HistoryWatcherContext,
    command: &str,
    line_number: u64,
    recent_hashes: &mut Vec<u64>,
) -> NodeResult<()> {
    // Validate command is valid UTF-8 and reject binary data
    if command.contains('\0') {
        warn!(
            path = %ctx.path,
            line_number,
            "Skipping command with null bytes (binary data)"
        );
        return Ok(());
    }

    // Check for non-printable control characters that indicate binary data
    let has_binary = command
        .chars()
        .any(|c| c.is_control() && c != '\t' && c != '\n' && c != '\r');
    if has_binary {
        warn!(
            path = %ctx.path,
            line_number,
            "Skipping command with binary/control characters"
        );
        return Ok(());
    }

    // Deduplication: hash command text and check against recent history.
    // This prevents duplicate events when history files are rotated or truncated.
    use std::hash::{Hash, Hasher};
    let command_hash = {
        let mut hasher = std::hash::DefaultHasher::new();
        command.hash(&mut hasher);
        hasher.finish()
    };
    if recent_hashes.contains(&command_hash) {
        debug!(
            path = %ctx.path,
            line_number,
            "Skipping duplicate command (hash match)"
        );
        return Ok(());
    }
    // Add hash to dedup set after we've decided to emit.
    // Bounded to prevent unbounded memory growth.
    if recent_hashes.len() >= DEDUP_HASH_CAPACITY {
        recent_hashes.remove(0);
    }
    recent_hashes.push(command_hash);

    // Redact sensitive information
    let (redacted_command, redaction_stats) = SecretRedactor::redact_with_stats(command);
    if redaction_stats.any_redacted() {
        tracing::info!(
            patterns = ?redaction_stats.matched_patterns,
            path = %ctx.path,
            "Redacted secrets from command"
        );
    }
    let final_command = redacted_command.as_ref();
    let bytes = final_command.as_bytes();

    if bytes.len() as u64 > ctx.max_capture_bytes.as_u64() {
        warn!(
            "Skipping command exceeding capture limit ({} bytes > {} limit)",
            bytes.len(),
            ctx.max_capture_bytes.as_u64()
        );
        return Ok(());
    }

    if let Some(commands) = &ctx.processed_commands {
        commands.lock().await.push(final_command.to_string());
    }

    let mut handle = ctx
        .acquisition
        .begin_material(ctx.path.as_str())
        .await
        .map_err(|e| SinexError::general(format!("Failed to begin material: {e}")))?;
    let material_id = handle.material_id;

    ctx.acquisition
        .append_slice(&mut handle, bytes)
        .await
        .map_err(|e| SinexError::general(format!("Failed to append slice: {e}")))?;

    ctx.acquisition
        .finalize(handle, MATERIAL_REASON_HISTORY)
        .await
        .map_err(|e| SinexError::general(format!("Failed to finalize material: {e}")))?;

    let payload = sinex_primitives::events::payloads::shell::HistoryCommandImportedPayload {
        command: final_command.to_string(),
        timestamp: Some(Timestamp::now()),
        shell_type: ctx.shell.clone(),
        source_file: ctx.path.to_string(),
        line_number: Some(line_number as u32),
    };

    let event = payload
        .from_material(material_id)
        .with_offset_start(0)
        .map_err(|e| SinexError::general(format!("Failed to set offset start: {e}")))?
        .with_offset_end(bytes.len() as i64)
        .map_err(|e| SinexError::general(format!("Failed to set offset end: {e}")))?
        .build()
        .map_err(|e| SinexError::general(format!("Failed to build event: {e}")))?
        .to_json_event()
        .map_err(|e| SinexError::general(format!("Failed to convert event to JSON: {e}")))?;

    ctx.stage_context
        .emit_event_with_provenance(event, material_id, Some(0), Some(bytes.len() as i64))
        .await
        .map(|_| ())
        .map_err(|e| SinexError::general(format!("Failed to emit terminal event: {e}")))?;

    Ok(())
}

/// Terminal processor that monitors history files.
pub struct TerminalProcessor {
    config: TerminalConfig,
    stage_context: Option<StageAsYouGoContext>,
    watch_handles: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>,
    state_dir: Option<PathBuf>,
    runtime: Option<NodeRuntimeState>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TerminalCheckpoint {}

impl TerminalProcessor {
    #[must_use]
    pub fn new() -> Self {
        Self {
            config: TerminalConfig::default(),
            stage_context: None,
            watch_handles: Arc::new(Mutex::new(Vec::new())),
            state_dir: None,
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
            runtime: None,
        }
    }

    #[must_use]
    pub fn config(&self) -> &TerminalConfig {
        &self.config
    }

    fn runtime(&self) -> NodeResult<&NodeRuntimeState> {
        self.runtime.as_ref().ok_or_else(|| {
            SinexError::general(
                "Terminal processor runtime not initialized prior to scan".to_string(),
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
            processor = self.name(),
            service = %service_info.service_name(),
            "Initialising terminal processor"
        );

        config.validate_config().map_err(|e| {
            SinexError::general(format!("Terminal configuration validation failed: {e}"))
        })?;

        let publisher = match runtime.transport() {
            sinex_node_sdk::EventTransport::Nats(publisher) => publisher.clone(),
        };

        AcquisitionManager::bootstrap_streams(publisher.nats_client()).await?;

        let mut state_dir = service_info.work_dir().clone();
        state_dir.push("terminal-history");

        if let Err(e) = fs::create_dir_all(&state_dir).await {
            return Err(SinexError::general(format!(
                "Failed to create terminal state directory {}: {}",
                state_dir.display(),
                e
            )));
        }

        self.state_dir = Some(state_dir);
        self.stage_context = Some(StageAsYouGoContext::from_runtime(&runtime));
        self.runtime = Some(runtime);
        self.config = config;
        self.watch_handles = Arc::new(Mutex::new(Vec::new()));
        // shutdown_tx removed

        Ok(())
    }

    fn build_history_contexts(
        &self,
        shutdown_rx: watch::Receiver<bool>,
    ) -> NodeResult<Vec<HistoryWatcherContext>> {
        let runtime = self.runtime()?;

        let stage = self
            .stage_context
            .clone()
            .ok_or_else(|| SinexError::general("Stage context not initialized".to_string()))?;

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

            // Detect if this is a Fish SQLite history database
            let is_fish_sqlite = source.shell.to_lowercase() == "fish"
                && crate::fish_history::is_fish_sqlite_history(&source.path);

            if source.shell.to_lowercase() == "fish" && !is_fish_sqlite {
                debug!(
                    path = %source.path,
                    "Fish history file is not SQLite format; will attempt text parsing"
                );
            }

            contexts.push(HistoryWatcherContext {
                acquisition,
                stage_context,
                shell: source.shell.clone(),
                path: source.path.clone(),
                max_capture_bytes: self.config.max_capture_bytes,
                polling_interval: Duration::from_secs(self.config.polling_interval_secs.as_secs()),
                state_path,
                shutdown_rx: shutdown_rx.clone(),
                processed_commands: None,
                is_fish_sqlite,
            });
        }

        Ok(contexts)
    }
}

impl Default for TerminalProcessor {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl SimpleIngestor for TerminalProcessor {
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
            SinexError::general(format!("Terminal configuration validation failed: {e}"))
        })?;

        let publisher = match runtime.transport() {
            sinex_node_sdk::EventTransport::Nats(publisher) => publisher.clone(),
        };

        AcquisitionManager::bootstrap_streams(publisher.nats_client()).await?;

        let mut state_dir = service_info.work_dir().clone();
        state_dir.push("terminal-history");

        if let Err(e) = fs::create_dir_all(&state_dir).await {
            return Err(SinexError::general(format!(
                "Failed to create terminal state directory {}: {}",
                state_dir.display(),
                e
            )));
        }

        self.state_dir = Some(state_dir);
        self.stage_context = Some(StageAsYouGoContext::from_runtime(runtime));
        self.config = config;
        self.runtime = Some(runtime.clone());

        Ok(())
    }

    async fn scan_snapshot(
        &mut self,
        _state: &mut Self::State,
        _args: ScanArgs,
    ) -> NodeResult<ScanReport> {
        let monitored: Vec<Utf8PathBuf> = self
            .config
            .history_sources
            .iter()
            .map(|src| src.path.clone())
            .collect();

        debug!(monitored = monitored.len(), "Terminal snapshot captured");

        Ok(ScanReport {
            events_processed: 0,
            duration: std::time::Duration::from_millis(0),
            final_checkpoint: Checkpoint::None,
            time_range: None,
            processor_stats: HashMap::new(),
            successful_targets: vec!["snapshot".to_string()],
            failed_targets: Vec::new(),
            warnings: Vec::new(),
        })
    }

    async fn scan_historical(
        &mut self,
        _state: &mut Self::State,
        from: Checkpoint,
        _until: TimeHorizon,
        _args: ScanArgs,
    ) -> NodeResult<ScanReport> {
        let (_, shutdown_rx) = watch::channel(false);
        let contexts = self.build_history_contexts(shutdown_rx)?;
        let mut events_processed = 0u64;

        for ctx in contexts {
            events_processed =
                events_processed.saturating_add(ctx.scan_history_once().await as u64);
        }

        Ok(ScanReport {
            events_processed,
            duration: std::time::Duration::from_millis(0),
            final_checkpoint: from,
            time_range: None,
            processor_stats: HashMap::new(),
            successful_targets: vec!["historical".to_string()],
            failed_targets: Vec::new(),
            warnings: Vec::new(),
        })
    }

    async fn run_continuous(
        &mut self,
        _state: &mut Self::State,
        _from: Checkpoint,
        shutdown_rx: watch::Receiver<bool>,
    ) -> NodeResult<ScanReport> {
        let contexts = self.build_history_contexts(shutdown_rx.clone())?;

        let mut guard = self.watch_handles.lock().await;
        for watch_ctx in contexts {
            let handle = tokio::spawn(watch_ctx.clone().monitor());
            guard.push(handle);
        }

        info!(
            watches = guard.len(),
            "Terminal watcher monitoring history sources"
        );

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
        let mut guard = self.watch_handles.lock().await;
        for handle in guard.drain(..) {
            handle.abort();
        }
        info!("Terminal watcher shutdown complete");
        Ok(())
    }
}

impl ExplorationProvider for TerminalProcessor {
    fn get_source_state(&self) -> NodeResult<SourceState> {
        Ok(SourceState {
            is_connected: true,
            healthy: true,
            description: format!(
                "Monitoring {} history sources",
                self.config.history_sources.len()
            ),
            last_updated: Timestamp::now(),
            lag_seconds: None,
            recent_activity: vec![],
            total_items: Some(self.config.history_sources.len() as u64),
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
            let one_hour_ago = Timestamp::now() - time::Duration::hours(1);
            (one_hour_ago, now)
        });

        Ok(CoverageAnalysis {
            time_range,
            coverage_percentage: 1.0,
            missing_count: 0,
            duplicate_count: 0,
            source_total: self.config.history_sources.len() as u64,
            sinex_total: 0,
            missing_samples: Vec::new(),
            recommendations: vec![
                "Ensure history files are readable by the terminal ingestor".to_string()
            ],
        })
    }

    fn export_data(&self, _path: &SanitizedPath, _format: ExportFormat) -> NodeResult<()> {
        Err(SinexError::general(
            "Terminal watcher does not support data export",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinex_node_sdk::{acquisition_manager::RotationPolicy, AcquisitionManager};
    use sinex_primitives::events::Provenance;
    use sinex_primitives::Id;
    use sinex_schema::primitives::ulid_to_uuid;
    use std::sync::Arc;
    use tokio::{
        io::AsyncWriteExt,
        time::{timeout, Duration},
    };
    use xtask::sandbox::sinex_test;
    use xtask::sandbox::{
        prelude::*, start_test_ingestd_with_config, TestIngestdConfig, TestRuntime,
        TestRuntimeBuilder,
    };

    #[sinex_test]
    fn terminal_config_validation_allows_valid_configuration() -> TestResult<()> {
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
    fn terminal_config_validation_rejects_empty_sources() -> TestResult<()> {
        let config = TerminalConfig {
            history_sources: vec![],
            polling_interval_secs: Seconds::from_secs(30),
            max_capture_bytes: Bytes::from_bytes(1024),
        };

        assert!(config.validate_config().is_err());
        Ok(())
    }

    #[sinex_test]
    async fn process_command_emits_event(ctx: TestContext) -> TestResult<()> {
        let TestRuntime {
            runtime,
            mut event_rx,
            nats,
        } = TestRuntimeBuilder::new(&ctx, "terminal-ingestor-test")
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
            "/home/test/.bash_history",
        )?);

        let stage_context = StageAsYouGoContext::from_runtime(&runtime)
            .with_acquisition_manager(Arc::clone(&acquisition));

        let watcher_ctx = HistoryWatcherContext {
            acquisition,
            stage_context,
            shell: "bash".to_string(),
            path: Utf8PathBuf::from("/home/test/.bash_history"),
            max_capture_bytes: Bytes::from_bytes(1024),
            polling_interval: Duration::from_secs(1),
            state_path: None,
            shutdown_rx: tokio::sync::watch::channel(false).1,
            #[cfg(test)]
            processed_commands: None,
            is_fish_sqlite: false,
        };

        let command = "echo 'hello world'";
        let mut recent_hashes = Vec::new();
        process_command(&watcher_ctx, command, 42, &mut recent_hashes).await?;

        let event = timeout(Duration::from_secs(5), event_rx.recv())
            .await?
            .ok_or_else(|| color_eyre::eyre::eyre!("terminal event not emitted"))?;

        assert_eq!(event.event_type.as_str(), "command.imported");

        let material_ulid = match event.provenance() {
            Provenance::Material { ref id, .. } => *id.as_ulid(),
            _ => {
                return Err(color_eyre::eyre::eyre!(
                    "expected material provenance in terminal event"
                ))
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
                        .get_by_id(Id::from_ulid(material_ulid))
                        .await
                        .map_err(|e| anyhow::anyhow!("{e}"))?
                    {
                        if material.status.as_str() != "completed" {
                            return Ok::<bool, anyhow::Error>(false);
                        }
                    } else {
                        return Ok::<bool, anyhow::Error>(false);
                    }

                    let ledger_bytes: Option<i64> = sqlx::query_scalar(
                        "SELECT offset_end FROM raw.temporal_ledger WHERE source_material_id = $1::uuid::ulid ORDER BY ts_capture DESC LIMIT 1",
                    )
                    .bind(ulid_to_uuid(material_ulid))
                    .fetch_optional(&pool)
                    .await
                    .map_err(|e| anyhow::anyhow!("database error: {e}"))?;
                    Ok::<bool, anyhow::Error>(
                        ledger_bytes.unwrap_or_default() == expected
                    )
                }
            },
            20,
        )
        .await?;

        let record = ctx
            .pool
            .source_materials()
            .get_by_id(Id::from_ulid(material_ulid))
            .await?
            .ok_or_else(|| color_eyre::eyre::eyre!("source material not persisted"))?;
        assert_eq!(record.status.as_str(), "completed");

        let total_bytes: Option<i64> = sqlx::query_scalar(
            "SELECT offset_end FROM raw.temporal_ledger WHERE source_material_id = $1::uuid::ulid ORDER BY ts_capture DESC LIMIT 1",
        )
        .bind(ulid_to_uuid(material_ulid))
        .fetch_optional(&ctx.pool)
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
    async fn terminal_watcher_tails_incrementally(ctx: TestContext) -> TestResult<()> {
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
            shell: "bash".to_string(),
            path: history_utf8,
            max_capture_bytes: Bytes::from_bytes(2048),
            polling_interval: Duration::from_millis(50),
            state_path: Some(state_path),
            shutdown_rx: tokio::sync::watch::channel(false).1,
            #[cfg(test)]
            processed_commands: None,
            is_fish_sqlite: false,
        };

        #[cfg(test)]
        let processed_commands = Arc::new(Mutex::new(Vec::new()));
        #[cfg(test)]
        {
            watcher_ctx.processed_commands = Some(processed_commands.clone());
        }

        let mut offset_bytes = 0u64;
        let mut line_number = 0u64;
        let mut recent_hashes: Vec<u64> = Vec::new();
        #[cfg(unix)]
        let mut last_inode: Option<u64> = None;

        #[cfg(unix)]
        let _ = watcher_ctx
            .poll_history_once(
                &mut offset_bytes,
                &mut line_number,
                &mut last_inode,
                &mut recent_hashes,
            )
            .await;
        #[cfg(not(unix))]
        let _ = watcher_ctx
            .poll_history_once(&mut offset_bytes, &mut line_number, &mut recent_hashes)
            .await;

        let mut history_file: tokio::fs::File = tokio::fs::OpenOptions::new()
            .append(true)
            .open(&history_path)
            .await?;
        history_file.write_all(b"echo second\n").await?;
        history_file.write_all(b"echo third\n").await?;
        history_file.flush().await?;

        #[cfg(unix)]
        let _ = watcher_ctx
            .poll_history_once(
                &mut offset_bytes,
                &mut line_number,
                &mut last_inode,
                &mut recent_hashes,
            )
            .await;
        #[cfg(not(unix))]
        let _ = watcher_ctx
            .poll_history_once(&mut offset_bytes, &mut line_number, &mut recent_hashes)
            .await;

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
}

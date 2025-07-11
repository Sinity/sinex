//! Shell History File Monitoring
//!
//! This module monitors shell history files for new commands and imports them as events.
//! It supports various shell history formats including bash, zsh, and other POSIX shells.

use async_trait::async_trait;
use notify::{RecommendedWatcher, RecursiveMode, Watcher};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::SystemTime;
use tokio::fs;
use tokio::sync::mpsc;
use tokio::time::{interval, Duration};
use tracing::{debug, error, info, warn};

use sinex_core::{
    sources, ChannelSenderExt, CoreError, EventFactory, EventSender, EventSource, EventSourceBase,
    EventSourceContext, Result,
};

use crate::{ShellCommandInfo, ShellConfig};

// ============================================================================
// Event Payloads
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ShellHistoryCommandPayload {
    pub command_line: String,
    pub shell_type: Option<String>,
    pub history_file: String,
    pub line_number: usize,
    pub timestamp: Option<chrono::DateTime<chrono::Utc>>,
    pub shell_command_info: ShellCommandInfo,
}

// ============================================================================
// Event Types
// ============================================================================

pub struct ShellHistoryCommand;

// ============================================================================
// Shell History Monitor
// ============================================================================

pub struct ShellHistoryMonitor {
    config: ShellConfig,
    event_factory: EventFactory,
    file_states: HashMap<PathBuf, FileState>,
}

#[derive(Debug, Clone)]
struct FileState {
    last_modified: SystemTime,
    last_line_count: usize,
    shell_type: Option<String>,
}

impl EventSourceBase for ShellHistoryMonitor {}

#[async_trait]
impl EventSource for ShellHistoryMonitor {
    type Config = ShellConfig;

    const SOURCE_NAME: &'static str = sources::SHELL_HISTORY;

    async fn initialize(ctx: EventSourceContext) -> Result<Self> {
        let config = <Self as EventSourceBase>::parse_config::<Self::Config>(&ctx).await?;

        info!(
            history_paths = ?config.history_paths,
            "Initializing shell history monitor"
        );

        let mut file_states = HashMap::new();

        // Initialize file states for existing history files
        for history_path in &config.history_paths {
            let expanded_path = shellexpand::tilde(history_path);
            let path = PathBuf::from(expanded_path.as_ref());

            if path.exists() {
                match fs::metadata(&path).await {
                    Ok(metadata) => {
                        let shell_type = detect_shell_type(&path);
                        let line_count = count_lines(&path).await.unwrap_or(0);

                        file_states.insert(
                            path.clone(),
                            FileState {
                                last_modified: metadata
                                    .modified()
                                    .unwrap_or(SystemTime::UNIX_EPOCH),
                                last_line_count: line_count,
                                shell_type,
                            },
                        );

                        info!(
                            "Monitoring shell history file: {} ({} lines)",
                            path.display(),
                            line_count
                        );
                    }
                    Err(e) => {
                        warn!("Cannot access history file {}: {}", path.display(), e);
                    }
                }
            } else {
                debug!("History file does not exist: {}", path.display());
            }
        }

        Ok(Self {
            config,
            event_factory: EventFactory::new(Self::SOURCE_NAME),
            file_states,
        })
    }

    async fn stream_events(&mut self, tx: EventSender) -> Result<()> {
        let (notify_tx, mut notify_rx) = mpsc::channel(100);

        // Set up file watcher for all history files
        let mut watcher: RecommendedWatcher = Watcher::new(
            move |res: notify::Result<notify::Event>| {
                if let Ok(event) = res {
                    let _ = notify_tx.blocking_send(event);
                }
            },
            notify::Config::default(),
        )
        .map_err(|e| CoreError::Configuration(format!("Failed to create file watcher: {}", e)))?;

        // Watch all history files
        for path in self.file_states.keys() {
            if let Err(e) = watcher.watch(path, RecursiveMode::NonRecursive) {
                warn!("Failed to watch history file {}: {}", path.display(), e);
            } else {
                info!("Started watching history file: {}", path.display());
            }
        }

        // Set up periodic polling as backup
        let mut poll_interval = interval(Duration::from_secs(30));

        loop {
            tokio::select! {
                // File change notification
                Some(event) = notify_rx.recv() => {
                    for path in &event.paths {
                        if self.file_states.contains_key(path) {
                            if let Err(e) = self.process_history_file_changes(path, &tx).await {
                                error!("Error processing history file {}: {}", path.display(), e);
                            }
                        }
                    }
                }

                // Periodic poll
                _ = poll_interval.tick() => {
                    for path in self.file_states.keys().cloned().collect::<Vec<_>>() {
                        if let Err(e) = self.process_history_file_changes(&path, &tx).await {
                            error!("Error during periodic poll of {}: {}", path.display(), e);
                        }
                    }
                }
            }
        }
    }
}

impl ShellHistoryMonitor {
    async fn process_history_file_changes(
        &mut self,
        path: &PathBuf,
        tx: &EventSender,
    ) -> Result<()> {
        // Check if file still exists
        if !path.exists() {
            debug!("History file no longer exists: {}", path.display());
            return Ok(());
        }

        let metadata = fs::metadata(path).await.map_err(|e| {
            CoreError::Io(format!(
                "Failed to get metadata for {}: {}",
                path.display(),
                e
            ))
        })?;

        let current_modified = metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH);
        let current_line_count = count_lines(path).await.unwrap_or(0);

        let (last_modified, last_line_count, shell_type) = {
            let state = self.file_states.get(path).unwrap();
            (
                state.last_modified,
                state.last_line_count,
                state.shell_type.clone(),
            )
        };

        // Check if file has been modified or has new lines
        if current_modified > last_modified || current_line_count > last_line_count {
            debug!(
                "History file changed: {} (lines: {} -> {})",
                path.display(),
                last_line_count,
                current_line_count
            );

            // Read new lines
            let new_lines = if current_line_count > last_line_count {
                read_lines_from_offset(path, last_line_count).await?
            } else {
                // File might have been truncated and rewritten
                read_lines_from_offset(path, 0).await?
            };

            // Process new commands
            let mut processed_count = 0;
            for (line_num, command_line) in new_lines.into_iter().enumerate() {
                let absolute_line_num = last_line_count + line_num + 1;

                if let Err(e) = self
                    .process_history_line(&command_line, path, absolute_line_num, &shell_type, tx)
                    .await
                {
                    error!("Error processing history line: {}", e);
                } else {
                    processed_count += 1;
                }
            }

            if processed_count > 0 {
                info!(
                    "Processed {} new commands from {}",
                    processed_count,
                    path.display()
                );
            }

            // Update state
            let state = self.file_states.get_mut(path).unwrap();
            state.last_modified = current_modified;
            state.last_line_count = current_line_count;
        }

        Ok(())
    }

    async fn process_history_line(
        &self,
        command_line: &str,
        history_file: &Path,
        line_number: usize,
        shell_type: &Option<String>,
        tx: &EventSender,
    ) -> Result<()> {
        let trimmed = command_line.trim();

        // Skip empty lines and comments
        if trimmed.is_empty() || trimmed.starts_with('#') {
            return Ok(());
        }

        // Parse timestamp for zsh extended history
        let (actual_command, timestamp) = if let Some(shell) = shell_type.as_ref() {
            if shell == "zsh" && trimmed.starts_with(": ") {
                parse_zsh_extended_history(trimmed)
            } else {
                (trimmed.to_string(), None)
            }
        } else {
            (trimmed.to_string(), None)
        };

        // Skip commands that are too short
        if actual_command.len() < self.config.min_command_length {
            return Ok(());
        }

        // Check if command should be ignored
        if let Ok((command, _)) = ShellCommandInfo::parse_command_line(&actual_command) {
            if self
                .config
                .ignore_commands
                .iter()
                .any(|ignored| command.starts_with(ignored))
            {
                return Ok(());
            }
        }

        // Create shell command info
        let (command, args) = ShellCommandInfo::parse_command_line(&actual_command)
            .unwrap_or_else(|_| (actual_command.clone(), Vec::new()));

        let shell_command_info = ShellCommandInfo {
            command: command.clone(),
            args,
            working_directory: None, // Not available in history files
            shell_type: shell_type.clone(),
            session_id: None,
            pid: None,
            exit_code: None,
            execution_time_ms: None,
            start_time: timestamp.unwrap_or_else(chrono::Utc::now),
            end_time: None,
        };

        let payload = ShellHistoryCommandPayload {
            command_line: actual_command,
            shell_type: shell_type.clone(),
            history_file: history_file.to_string_lossy().to_string(),
            line_number,
            timestamp,
            shell_command_info,
        };

        let event = self
            .event_factory
            .create_event("command.imported", serde_json::to_value(payload)?);

        tx.send_or_log(event, "shell_history_command").await?;

        Ok(())
    }
}

// ============================================================================
// Helper Functions
// ============================================================================

fn detect_shell_type(path: &Path) -> Option<String> {
    if let Some(file_name) = path.file_name().and_then(|n| n.to_str()) {
        if file_name.contains("bash") {
            Some("bash".to_string())
        } else if file_name.contains("zsh") {
            Some("zsh".to_string())
        } else if file_name.contains("fish") {
            Some("fish".to_string())
        } else {
            None
        }
    } else {
        None
    }
}

async fn count_lines(path: &PathBuf) -> Result<usize> {
    let content = fs::read_to_string(path)
        .await
        .map_err(|e| CoreError::Io(format!("Failed to read file: {}", e)))?;

    Ok(content.lines().count())
}

async fn read_lines_from_offset(path: &PathBuf, offset: usize) -> Result<Vec<String>> {
    let content = fs::read_to_string(path)
        .await
        .map_err(|e| CoreError::Io(format!("Failed to read file: {}", e)))?;

    let lines: Vec<String> = content
        .lines()
        .skip(offset)
        .map(|s| s.to_string())
        .collect();

    Ok(lines)
}

fn parse_zsh_extended_history(line: &str) -> (String, Option<chrono::DateTime<chrono::Utc>>) {
    // Zsh extended history format: : timestamp:elapsed;command
    if let Some(rest) = line.strip_prefix(": ") {
        if let Some(semicolon_pos) = rest.find(';') {
            let timestamp_part = &rest[..semicolon_pos];
            let command_part = &rest[semicolon_pos + 1..];

            // Parse timestamp (format: "timestamp:elapsed")
            if let Some(colon_pos) = timestamp_part.find(':') {
                let timestamp_str = &timestamp_part[..colon_pos];
                if let Ok(timestamp) = timestamp_str.parse::<i64>() {
                    let dt = chrono::DateTime::from_timestamp(timestamp, 0)
                        .unwrap_or_else(chrono::Utc::now);
                    return (command_part.to_string(), Some(dt));
                }
            }

            return (command_part.to_string(), None);
        }
    }

    (line.to_string(), None)
}

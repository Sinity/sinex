//! Shell history file watcher
//!
//! Watches shell history files (.bash_history, .zsh_history, fish_history) for new commands

use notify::{Config, Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use serde_json::json;
use sinex_events::{EventFactory, RawEvent};
use sinex_satellite_sdk::SatelliteResult;
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

/// Shell history file watcher
pub struct HistoryWatcher {
    files: Vec<PathBuf>,
    file_positions: HashMap<PathBuf, u64>,
    watcher: Option<RecommendedWatcher>,
}

impl HistoryWatcher {
    /// Create new history watcher
    pub async fn new(files: Vec<PathBuf>) -> SatelliteResult<Self> {
        let mut watcher = Self {
            files,
            file_positions: HashMap::new(),
            watcher: None,
        };

        // Initialize file positions to end of files (to catch only new entries)
        watcher.initialize_positions()?;

        Ok(watcher)
    }

    /// Initialize file positions to current end of files
    fn initialize_positions(&mut self) -> SatelliteResult<()> {
        for file_path in &self.files {
            if file_path.exists() {
                match fs::metadata(file_path) {
                    Ok(metadata) => {
                        self.file_positions
                            .insert(file_path.clone(), metadata.len());
                        info!(
                            "Tracking history file: {} (starting at byte {})",
                            file_path.display(),
                            metadata.len()
                        );
                    }
                    Err(e) => {
                        warn!("Failed to get metadata for {}: {}", file_path.display(), e);
                    }
                }
            } else {
                info!(
                    "History file does not exist (will watch if created): {}",
                    file_path.display()
                );
                self.file_positions.insert(file_path.clone(), 0);
            }
        }
        Ok(())
    }

    /// Read new lines from a file since last position
    fn read_new_lines(&mut self, file_path: &PathBuf) -> SatelliteResult<Vec<String>> {
        let current_pos = self.file_positions.get(file_path).copied().unwrap_or(0);

        let content = match fs::read_to_string(file_path) {
            Ok(content) => content,
            Err(e) => {
                warn!("Failed to read {}: {}", file_path.display(), e);
                return Ok(vec![]);
            }
        };

        let content_bytes = content.as_bytes();
        if content_bytes.len() <= current_pos as usize {
            // File hasn't grown
            return Ok(vec![]);
        }

        // Read new content
        let new_content = &content_bytes[current_pos as usize..];
        let new_text = String::from_utf8_lossy(new_content);

        // Update position
        self.file_positions
            .insert(file_path.clone(), content_bytes.len() as u64);

        // Parse lines based on shell type
        let lines = self.parse_history_content(&new_text, file_path);

        if !lines.is_empty() {
            debug!(
                "Found {} new history entries in {}",
                lines.len(),
                file_path.display()
            );
        }

        Ok(lines)
    }

    /// Parse history content based on shell type
    fn parse_history_content(&self, content: &str, file_path: &PathBuf) -> Vec<String> {
        let filename = file_path.file_name().and_then(|n| n.to_str()).unwrap_or("");

        let mut commands = Vec::new();

        if filename.contains("fish") {
            // Fish history format: "- cmd: command\n  when: timestamp"
            let mut current_command = None;
            for line in content.lines() {
                let line = line.trim();
                if line.starts_with("- cmd: ") {
                    current_command = Some(line[7..].to_string());
                } else if line.starts_with("when: ") && current_command.is_some() {
                    if let Some(cmd) = current_command.take() {
                        if !cmd.trim().is_empty() {
                            commands.push(cmd);
                        }
                    }
                }
            }
        } else if filename.contains("zsh") {
            // Zsh extended history format: ": timestamp:duration;command"
            for line in content.lines() {
                let line = line.trim();
                if line.starts_with(": ") {
                    if let Some(semicolon_pos) = line.find(';') {
                        let command = line[semicolon_pos + 1..].trim();
                        if !command.is_empty() {
                            commands.push(command.to_string());
                        }
                    }
                } else if !line.is_empty() {
                    // Simple command line
                    commands.push(line.to_string());
                }
            }
        } else {
            // Bash history format: simple line-by-line commands
            for line in content.lines() {
                let line = line.trim();
                if !line.is_empty() && !line.starts_with('#') {
                    commands.push(line.to_string());
                }
            }
        }

        commands
    }

    /// Convert command to RawEvent
    fn command_to_event(&self, command: String, source_file: &PathBuf) -> RawEvent {
        let (shell_type, source_name) = if source_file.to_string_lossy().contains("fish") {
            ("fish", sinex_core_types::sources::SHELL_FISH_HISTORY)
        } else if source_file.to_string_lossy().contains("zsh") {
            ("zsh", sinex_core_types::sources::SHELL_ZSH_HISTFILE)
        } else {
            ("bash", sinex_core_types::sources::SHELL_BASH_HISTFILE)
        };

        let payload = json!({
            "command_string": command,
            "shell_type": shell_type,
            "source_file": source_file.to_string_lossy(),
            "timestamp": chrono::Utc::now().to_rfc3339()
        });

        let factory = EventFactory::new(source_name);
        factory.create_event("command.hist", payload)
    }

    /// Start streaming events
    pub async fn start_streaming(
        &mut self,
        tx: mpsc::UnboundedSender<RawEvent>,
    ) -> SatelliteResult<()> {
        info!(
            "Starting shell history event streaming for {} files",
            self.files.len()
        );

        // Set up file watcher
        let (notify_tx, mut notify_rx) = mpsc::unbounded_channel::<Event>();

        let mut watcher = RecommendedWatcher::new(
            move |result: Result<Event, notify::Error>| match result {
                Ok(event) => {
                    if let Err(e) = notify_tx.send(event) {
                        error!("Failed to send notify event: {}", e);
                    }
                }
                Err(e) => {
                    error!("File watch error: {}", e);
                }
            },
            Config::default(),
        )
        .map_err(|e| {
            sinex_satellite_sdk::SatelliteError::Processing(format!(
                "Failed to create file watcher: {}",
                e
            ))
        })?;

        // Watch all history file directories
        let mut watched_dirs = std::collections::HashSet::new();
        for file_path in &self.files {
            if let Some(parent) = file_path.parent() {
                if parent.exists() && watched_dirs.insert(parent.to_path_buf()) {
                    watcher
                        .watch(parent, RecursiveMode::NonRecursive)
                        .map_err(|e| {
                            sinex_satellite_sdk::SatelliteError::Processing(format!(
                                "Failed to watch directory {}: {}",
                                parent.display(),
                                e
                            ))
                        })?;
                    info!("Watching directory: {}", parent.display());
                }
            }
        }

        self.watcher = Some(watcher);

        // Read any existing new content first
        for file_path in self.files.clone() {
            match self.read_new_lines(&file_path) {
                Ok(commands) => {
                    for command in commands {
                        let event = self.command_to_event(command, &file_path);
                        if tx.send(event).is_err() {
                            warn!("Event channel closed, stopping history watcher");
                            return Ok(());
                        }
                    }
                }
                Err(e) => {
                    warn!(
                        "Failed to read initial content from {}: {}",
                        file_path.display(),
                        e
                    );
                }
            }
        }

        // Process file change events
        while let Some(event) = notify_rx.recv().await {
            // Check if the event is for one of our watched files
            for path in event.paths {
                if self.files.contains(&path) {
                    match event.kind {
                        EventKind::Modify(_) | EventKind::Create(_) => {
                            // File was modified or created, read new content
                            match self.read_new_lines(&path) {
                                Ok(commands) => {
                                    for command in commands {
                                        let event = self.command_to_event(command, &path);
                                        if tx.send(event).is_err() {
                                            warn!("Event channel closed, stopping history watcher");
                                            return Ok(());
                                        }
                                    }
                                }
                                Err(e) => {
                                    warn!(
                                        "Failed to read new content from {}: {}",
                                        path.display(),
                                        e
                                    );
                                }
                            }
                        }
                        _ => {
                            // Ignore other event types
                        }
                    }
                }
            }
        }

        info!("Shell history event streaming stopped");
        Ok(())
    }
}

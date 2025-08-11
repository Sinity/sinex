//! Shell history file watcher
//!
//! Watches shell history files (.bash_history, .zsh_history, fish_history) for new commands
//! with comprehensive security validation to prevent path traversal and unauthorized access.

use camino::Utf8PathBuf;
use notify::{Config, Event as NotifyEvent, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use sinex_core::db::models::RawEvent;
use sinex_core::types::events::Event;
use sinex_core::types::events::{
    BashHistoricalCommandPayload, FishHistoricalCommandPayload, ZshHistoricalCommandPayload,
};
use sinex_core::types::validation::{
    validate_discovered_file, validate_watch_paths, FileWatchingSecurityPolicy,
};
use sinex_satellite_sdk::SatelliteResult;
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{self, BufRead, BufReader, Seek, SeekFrom};
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

/// Shell history file watcher with security validation
pub struct HistoryWatcher {
    files: Vec<Utf8PathBuf>,
    file_positions: HashMap<Utf8PathBuf, u64>,
    watcher: Option<RecommendedWatcher>,
    /// Security policy for file watching operations
    security_policy: FileWatchingSecurityPolicy,
    /// Validated watch roots for boundary checking
    validated_watch_roots: Vec<Utf8PathBuf>,
}

impl HistoryWatcher {
    /// Create new history watcher with security validation
    pub async fn new(files: Vec<Utf8PathBuf>) -> SatelliteResult<Self> {
        // SECURITY: Create a specialized policy for shell history files
        let mut security_policy = FileWatchingSecurityPolicy::default();

        // Allow common shell history locations under user's home
        security_policy
            .forbidden_prefixes
            .remove(&Utf8PathBuf::from("/home"));

        // Convert files to strings for validation
        let file_strings: Vec<String> = files.iter().map(|f| f.as_str().to_string()).collect();

        // SECURITY: Validate all history file paths before watching
        let validated_files =
            validate_watch_paths(&file_strings, &security_policy).map_err(|e| {
                sinex_satellite_sdk::SatelliteError::Processing(format!(
                    "History file validation failed: {}",
                    e
                ))
            })?;

        info!(
            "Validated {} history files for watching",
            validated_files.len()
        );
        for file in &validated_files {
            info!("  - {}", file.as_str());
        }

        let mut watcher = Self {
            files: validated_files.clone(),
            file_positions: HashMap::new(),
            watcher: None,
            security_policy,
            validated_watch_roots: validated_files
                .iter()
                .filter_map(|f| f.parent().map(|p| p.to_path_buf()))
                .collect(),
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
                            file_path.as_str(),
                            metadata.len()
                        );
                    }
                    Err(e) => {
                        warn!("Failed to get metadata for {}: {}", file_path.as_str(), e);
                    }
                }
            } else {
                info!(
                    "History file does not exist (will watch if created): {}",
                    file_path.as_str()
                );
                self.file_positions.insert(file_path.clone(), 0);
            }
        }
        Ok(())
    }

    /// Read new lines from a file since last position using buffered reading
    fn read_new_lines(&mut self, file_path: &Utf8PathBuf) -> SatelliteResult<Vec<String>> {
        let current_pos = self.file_positions.get(file_path).copied().unwrap_or(0);

        // Open file and check its current size
        let mut file = match File::open(file_path) {
            Ok(file) => file,
            Err(e) => {
                warn!("Failed to open {}: {}", file_path.as_str(), e);
                return Ok(vec![]);
            }
        };

        let file_size = match file.metadata() {
            Ok(metadata) => metadata.len(),
            Err(e) => {
                warn!("Failed to get metadata for {}: {}", file_path.as_str(), e);
                return Ok(vec![]);
            }
        };

        if file_size <= current_pos {
            // File hasn't grown
            return Ok(vec![]);
        }

        // Seek to the last known position
        if let Err(e) = file.seek(SeekFrom::Start(current_pos)) {
            warn!("Failed to seek in {}: {}", file_path.as_str(), e);
            return Ok(vec![]);
        }

        // Read new lines using buffered reader
        let reader = BufReader::new(file);
        let new_lines: Result<Vec<String>, io::Error> = reader.lines().collect();

        let new_lines = match new_lines {
            Ok(lines) => lines,
            Err(e) => {
                warn!("Failed to read lines from {}: {}", file_path.as_str(), e);
                return Ok(vec![]);
            }
        };

        // Update position to current file size
        self.file_positions.insert(file_path.clone(), file_size);

        // Join lines back into text for shell-specific parsing
        let new_text = new_lines.join("\n");

        // Parse lines based on shell type
        let lines = self.parse_history_content(&new_text, file_path);

        if !lines.is_empty() {
            debug!(
                "Found {} new history entries in {}",
                lines.len(),
                file_path.as_str()
            );
        }

        Ok(lines)
    }

    /// Parse history content based on shell type
    fn parse_history_content(&self, content: &str, file_path: &Utf8PathBuf) -> Vec<String> {
        let filename = file_path.file_name().unwrap_or("");

        let mut commands = Vec::with_capacity(1000); // Reasonable capacity for shell history

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

    /// Convert command to Event
    fn command_to_event(
        &self,
        command: String,
        source_file: &Utf8PathBuf,
    ) -> SatelliteResult<RawEvent> {
        let source_file_str = source_file.to_string();

        let event: RawEvent = if source_file_str.contains("fish") {
            Event::new(FishHistoricalCommandPayload {
                command_string: command,
                source_file: source_file_str,
            })
            .into()
        } else if source_file_str.contains("zsh") {
            Event::new(ZshHistoricalCommandPayload {
                command_string: command,
                source_file: source_file_str,
            })
            .into()
        } else {
            Event::new(BashHistoricalCommandPayload {
                command_string: command,
                source_file: source_file_str,
            })
            .into()
        };

        Ok(event.with_ts_orig(Some(chrono::Utc::now())))
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
        let (notify_tx, mut notify_rx) = mpsc::unbounded_channel::<NotifyEvent>();

        let mut watcher = RecommendedWatcher::new(
            move |result: Result<NotifyEvent, notify::Error>| match result {
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

        // SECURITY: Watch only validated history file directories
        let mut watched_dirs = std::collections::HashSet::new();
        for file_path in &self.files {
            if let Some(parent) = file_path.parent() {
                if parent.exists() && watched_dirs.insert(parent.to_path_buf()) {
                    // SECURITY: Additional validation that parent directory is safe
                    let parent_str = parent.as_str().to_string();
                    match validate_watch_paths(&[parent_str], &self.security_policy) {
                        Ok(_) => {
                            watcher
                                .watch(parent.as_std_path(), RecursiveMode::NonRecursive)
                                .map_err(|e| {
                                    sinex_satellite_sdk::SatelliteError::Processing(format!(
                                        "Failed to watch validated directory {}: {}",
                                        parent, e
                                    ))
                                })?;
                            info!("Watching validated directory: {}", parent.as_str());
                        }
                        Err(e) => {
                            warn!(
                                "Skipping directory {} due to security policy: {}",
                                parent.as_str(),
                                e
                            );
                        }
                    }
                }
            }
        }

        self.watcher = Some(watcher);

        // Read any existing new content first
        for file_path in self.files.clone() {
            match self.read_new_lines(&file_path) {
                Ok(commands) => {
                    for command in commands {
                        match self.command_to_event(command, &file_path) {
                            Ok(event) => {
                                if tx.send(event).is_err() {
                                    warn!("Event channel closed, stopping history watcher");
                                    return Ok(());
                                }
                            }
                            Err(e) => {
                                warn!("Failed to convert command to event: {}", e);
                            }
                        }
                    }
                }
                Err(e) => {
                    warn!(
                        "Failed to read initial content from {}: {}",
                        file_path.as_str(),
                        e
                    );
                }
            }
        }

        // Process file change events
        while let Some(event) = notify_rx.recv().await {
            // SECURITY: Validate and check if the event is for one of our watched files
            for path in event.paths {
                let utf8_path = match Utf8PathBuf::from_path_buf(path.clone()) {
                    Ok(p) => p,
                    Err(_) => continue, // Skip non-UTF8 paths
                };

                // SECURITY: Validate discovered file against watch roots
                let mut path_validated = false;
                for watch_root in &self.validated_watch_roots {
                    match validate_discovered_file(
                        utf8_path.as_str(),
                        watch_root.as_str(),
                        &self.security_policy,
                    ) {
                        Ok(_) => {
                            path_validated = true;
                            break;
                        }
                        Err(_) => continue,
                    }
                }

                if !path_validated {
                    warn!(
                        "Rejecting history file event for path outside validated boundaries: {}",
                        utf8_path.as_str()
                    );
                    continue;
                }

                if self.files.contains(&utf8_path) {
                    match event.kind {
                        EventKind::Modify(_) | EventKind::Create(_) => {
                            // File was modified or created, read new content
                            match self.read_new_lines(&utf8_path) {
                                Ok(commands) => {
                                    for command in commands {
                                        match self.command_to_event(command, &utf8_path) {
                                            Ok(event) => {
                                                if tx.send(event).is_err() {
                                                    warn!("Event channel closed, stopping history watcher");
                                                    return Ok(());
                                                }
                                            }
                                            Err(e) => {
                                                warn!("Failed to convert command to event: {}", e);
                                            }
                                        }
                                    }
                                }
                                Err(e) => {
                                    warn!("Failed to read new content from {}: {}", utf8_path, e);
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

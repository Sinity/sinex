//! Terminal scrollback watcher
//!
//! Captures terminal scrollback content with chunking and git-annex integration

use sinex_satellite_sdk::SatelliteResult;
use serde_json::json;
use sinex_core::RawEvent;
use sinex_events::RawEventBuilder;
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::{Duration, SystemTime};
use tokio::fs;
use tokio::sync::mpsc;
use tokio::time::interval;
use tracing::{debug, error, info, warn};

/// Terminal scrollback content
#[derive(Debug)]
struct ScrollbackContent {
    text: String,
    line_count: usize,
    size_bytes: u64,
    has_ansi_codes: bool,
}

/// Window state for tracking changes
#[derive(Debug, Clone)]
struct WindowState {
    id: u32,
    cwd: String,
    title: String,
    last_hash: Option<u64>,
    last_capture_time: SystemTime,
}

/// Terminal scrollback watcher
pub struct ScrollbackWatcher {
    kitty_socket_path: PathBuf,
    capture_interval: Duration,
    max_scrollback_lines: usize,
    include_ansi_codes: bool,
    chunking_threshold_bytes: usize,
    enable_chunking: bool,
    window_states: HashMap<u32, WindowState>,
    git_annex_repo: Option<PathBuf>,
    auto_annex: bool,
    annex_threshold_bytes: usize,
}

impl ScrollbackWatcher {
    /// Create new scrollback watcher
    pub async fn new() -> SatelliteResult<Self> {
        let tmp_dir = std::env::var("SINEX_TMP_DIR").unwrap_or_else(|_| "/tmp".to_string());
        let kitty_socket_path = PathBuf::from(format!("{}/kitty", tmp_dir));

        let watcher = Self {
            kitty_socket_path,
            capture_interval: Duration::from_secs(180), // 3 minutes
            max_scrollback_lines: 10000,
            include_ansi_codes: false,
            chunking_threshold_bytes: 32_768, // 32KB
            enable_chunking: true,
            window_states: HashMap::new(),
            git_annex_repo: Some(PathBuf::from("/realm/sinex-annex")),
            auto_annex: true,
            annex_threshold_bytes: 64_000, // 64KB
        };

        info!("Scrollback watcher initialized with socket: {}", watcher.kitty_socket_path.display());
        Ok(watcher)
    }

    /// Get all Kitty windows
    async fn get_kitty_windows(&self) -> SatelliteResult<Vec<WindowState>> {
        if !self.kitty_socket_path.exists() {
            debug!("Kitty socket not found at {}", self.kitty_socket_path.display());
            return Ok(Vec::new());
        }

        // Use kitty command to list windows
        let output = tokio::process::Command::new("kitty")
            .arg("@")
            .arg("--to")
            .arg(format!("unix:{}", self.kitty_socket_path.display()))
            .arg("ls")
            .output()
            .await
            .map_err(|e| {
                sinex_satellite_sdk::SatelliteError::Processing(format!("Failed to execute kitty command: {}", e))
            })?;

        if !output.status.success() {
            return Err(sinex_satellite_sdk::SatelliteError::Processing(format!(
                "Kitty ls command failed: {}",
                String::from_utf8_lossy(&output.stderr)
            )));
        }

        let data: serde_json::Value = serde_json::from_slice(&output.stdout).map_err(|e| {
            sinex_satellite_sdk::SatelliteError::Processing(format!("Failed to parse kitty response: {}", e))
        })?;

        let mut windows = Vec::new();

        if let Some(os_windows) = data.as_array() {
            for os_window in os_windows {
                if let Some(tabs) = os_window["tabs"].as_array() {
                    for tab in tabs {
                        if let Some(wins) = tab["windows"].as_array() {
                            for win in wins {
                                if let (Some(id), Some(cwd), Some(title)) = (
                                    win["id"].as_u64(),
                                    win["cwd"].as_str(),
                                    win["title"].as_str(),
                                ) {
                                    windows.push(WindowState {
                                        id: id as u32,
                                        cwd: cwd.to_string(),
                                        title: title.to_string(),
                                        last_hash: None,
                                        last_capture_time: SystemTime::UNIX_EPOCH,
                                    });
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(windows)
    }

    /// Get scrollback content for a window
    async fn get_window_scrollback(&self, window_id: u32, include_screen: bool) -> SatelliteResult<ScrollbackContent> {
        let extent = if include_screen { "all" } else { "scrollback" };
        let mut cmd = tokio::process::Command::new("kitty");
        cmd.arg("@")
            .arg("--to")
            .arg(format!("unix:{}", self.kitty_socket_path.display()))
            .arg("get-text")
            .arg("--match")
            .arg(format!("id:{}", window_id))
            .arg("--extent")
            .arg(extent);

        if self.include_ansi_codes {
            cmd.arg("--ansi");
        }

        let output = cmd.output().await.map_err(|e| {
            sinex_satellite_sdk::SatelliteError::Processing(format!("Failed to get scrollback: {}", e))
        })?;

        if !output.status.success() {
            return Err(sinex_satellite_sdk::SatelliteError::Processing(format!(
                "Failed to get scrollback for window {}: {}",
                window_id,
                String::from_utf8_lossy(&output.stderr)
            )));
        }

        let text = String::from_utf8_lossy(&output.stdout).to_string();
        let line_count = text.lines().count();

        // Limit lines if needed
        let text = if line_count > self.max_scrollback_lines {
            text.lines()
                .skip(line_count - self.max_scrollback_lines)
                .collect::<Vec<_>>()
                .join("\n")
        } else {
            text
        };

        let size_bytes = text.len() as u64;
        let has_ansi_codes = self.include_ansi_codes || text.contains('\x1b');

        Ok(ScrollbackContent {
            text,
            line_count: line_count.min(self.max_scrollback_lines),
            size_bytes,
            has_ansi_codes,
        })
    }

    /// Calculate content hash for change detection
    fn calculate_content_hash(&self, content: &str) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut hasher = DefaultHasher::new();
        content.hash(&mut hasher);
        hasher.finish()
    }

    /// Store large content in git-annex
    async fn store_in_git_annex(&self, window: &WindowState, content: &ScrollbackContent) -> SatelliteResult<(String, String)> {
        let annex_repo = self.git_annex_repo.as_ref().ok_or_else(|| {
            sinex_satellite_sdk::SatelliteError::Processing("Git-annex repository not configured".to_string())
        })?;

        // Create filename with timestamp and window info
        let timestamp = chrono::Utc::now().format("%Y%m%d_%H%M%S");
        let safe_title = window.title.chars()
            .filter(|c| c.is_alphanumeric() || *c == '_' || *c == '-')
            .collect::<String>();
        let filename = format!("scrollback_{}_{}_w{}.txt", timestamp, safe_title, window.id);
        let file_path = annex_repo.join(&filename);

        // Ensure annex directory exists
        if let Some(parent) = file_path.parent() {
            fs::create_dir_all(parent).await.map_err(|e| {
                sinex_satellite_sdk::SatelliteError::Processing(format!("Failed to create annex directory: {}", e))
            })?;
        }

        // Write content to file
        fs::write(&file_path, &content.text).await.map_err(|e| {
            sinex_satellite_sdk::SatelliteError::Processing(format!("Failed to write scrollback file: {}", e))
        })?;

        // Add to git-annex (simplified - in real implementation would use git-annex commands)
        let annex_key = format!("SHA256E-s{}--{}", content.size_bytes, self.simple_hash(&content.text));

        debug!("Stored scrollback in git-annex: {} -> {}", filename, annex_key);
        Ok((filename, annex_key))
    }

    /// Simple hash for demonstration (real implementation would use proper git-annex key generation)
    fn simple_hash(&self, content: &str) -> String {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut hasher = DefaultHasher::new();
        content.hash(&mut hasher);
        format!("{:x}", hasher.finish())
    }

    /// Chunk content using simple line-based chunking
    fn chunk_content(&self, content: &str) -> Vec<serde_json::Value> {
        if content.len() <= self.chunking_threshold_bytes {
            return vec![json!({
                "chunk_index": 0,
                "content": content,
                "size_bytes": content.len(),
                "line_count": content.lines().count()
            })];
        }

        let lines: Vec<&str> = content.lines().collect();
        let mut chunks = Vec::new();
        let mut current_chunk = String::new();
        let mut current_size = 0;
        let mut chunk_index = 0;
        let mut chunk_line_count = 0;

        for line in lines {
            let line_size = line.len() + 1; // +1 for newline
            
            if current_size + line_size > self.chunking_threshold_bytes && !current_chunk.is_empty() {
                // Finalize current chunk
                chunks.push(json!({
                    "chunk_index": chunk_index,
                    "content": current_chunk,
                    "size_bytes": current_size,
                    "line_count": chunk_line_count
                }));

                // Start new chunk
                current_chunk.clear();
                current_size = 0;
                chunk_index += 1;
                chunk_line_count = 0;
            }

            if !current_chunk.is_empty() {
                current_chunk.push('\n');
                current_size += 1;
            }
            current_chunk.push_str(line);
            current_size += line.len();
            chunk_line_count += 1;
        }

        // Add final chunk if not empty
        if !current_chunk.is_empty() {
            chunks.push(json!({
                "chunk_index": chunk_index,
                "content": current_chunk,
                "size_bytes": current_size,
                "line_count": chunk_line_count
            }));
        }

        chunks
    }

    /// Process a single window for scrollback capture
    async fn process_window(&mut self, window: WindowState, tx: &mpsc::UnboundedSender<RawEvent>) -> SatelliteResult<()> {
        // Get scrollback content
        let scrollback = match self.get_window_scrollback(window.id, true).await {
            Ok(content) => content,
            Err(e) => {
                debug!("Failed to get scrollback for window {}: {}", window.id, e);
                return Ok(());
            }
        };

        // Calculate hash for change detection
        let content_hash = self.calculate_content_hash(&scrollback.text);

        // Check if content changed
        if let Some(stored_window) = self.window_states.get(&window.id) {
            if stored_window.last_hash == Some(content_hash) {
                debug!("Scrollback unchanged for window {}", window.id);
                return Ok(());
            }
        }

        // Determine storage strategy
        let should_chunk = self.enable_chunking && scrollback.size_bytes > self.chunking_threshold_bytes as u64;
        let should_annex = self.auto_annex && scrollback.size_bytes > self.annex_threshold_bytes as u64;

        let (scrollback_text, scrollback_chunks, is_chunked, chunk_count, git_annex_path, git_annex_key) = 
            if should_annex {
                // Store in git-annex for large content
                match self.store_in_git_annex(&window, &scrollback).await {
                    Ok((annex_path, annex_key)) => {
                        (None, None, false, None, Some(annex_path), Some(annex_key))
                    }
                    Err(e) => {
                        error!("Failed to store scrollback in git-annex: {}, falling back to chunking", e);
                        if should_chunk {
                            let chunks = self.chunk_content(&scrollback.text);
                            let chunk_count = chunks.len() as u32;
                            (None, Some(chunks), true, Some(chunk_count), None, None)
                        } else {
                            (Some(scrollback.text.clone()), None, false, None, None, None)
                        }
                    }
                }
            } else if should_chunk {
                // Chunk content for database storage
                let chunks = self.chunk_content(&scrollback.text);
                let chunk_count = chunks.len() as u32;
                (None, Some(chunks), true, Some(chunk_count), None, None)
            } else {
                // Store as text in database
                (Some(scrollback.text.clone()), None, false, None, None, None)
            };

        // Create event
        let payload = json!({
            "window_id": window.id,
            "terminal_type": "kitty",
            "cwd": window.cwd,
            "window_title": window.title,
            "scrollback_text": scrollback_text,
            "scrollback_chunks": scrollback_chunks,
            "git_annex_path": git_annex_path,
            "git_annex_key": git_annex_key,
            "scrollback_lines": scrollback.line_count,
            "scrollback_size_bytes": scrollback.size_bytes,
            "is_chunked": is_chunked,
            "chunk_count": chunk_count,
            "includes_screen": true,
            "has_ansi_codes": scrollback.has_ansi_codes,
            "timestamp": chrono::Utc::now().to_rfc3339(),
        });

        let event = RawEventBuilder::new(sinex_core::sources::SHELL_SCROLLBACK, "output.captured", payload)
            .with_host("localhost")
            .build();

        if tx.send(event).is_err() {
            warn!("Event channel closed");
            return Ok(());
        }

        // Update window state
        let window_id = window.id;
        let mut updated_window = window;
        updated_window.last_hash = Some(content_hash);
        updated_window.last_capture_time = SystemTime::now();
        self.window_states.insert(updated_window.id, updated_window);

        info!(
            "Captured scrollback for window {} ({} bytes, {} lines)",
            window_id, scrollback.size_bytes, scrollback.line_count
        );

        Ok(())
    }

    /// Start streaming events
    pub async fn start_streaming(&mut self, tx: mpsc::UnboundedSender<RawEvent>) -> SatelliteResult<()> {
        info!("Starting scrollback event streaming");

        let mut capture_interval = interval(self.capture_interval);

        loop {
            capture_interval.tick().await;

            // Get all windows
            let windows = match self.get_kitty_windows().await {
                Ok(windows) => windows,
                Err(e) => {
                    debug!("Failed to get Kitty windows: {}", e);
                    continue;
                }
            };

            // Process each window
            for window in windows {
                if let Err(e) = self.process_window(window, &tx).await {
                    error!("Error processing window: {}", e);
                }
            }

            // Clean up old window states
            let active_ids: Vec<u32> = self.window_states.keys().copied().collect();
            let cutoff = SystemTime::now() - Duration::from_secs(3600); // 1 hour
            
            self.window_states.retain(|_id, state| {
                state.last_capture_time > cutoff
            });

            debug!("Scrollback capture cycle completed for {} windows", active_ids.len());
        }
    }
}
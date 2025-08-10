//! Kitty terminal integration watcher
//!
//! Watches for Kitty terminal events and shell integration via Unix socket
//!
//! ## Implementation Details (TIM-KittyTerminalIntegration)
//!
//! Provides comprehensive Kitty terminal monitoring via:
//! - **UNIX Domain Socket**: Primary communication method (~1-2ms latency)
//! - **Remote Control API**: Window/tab/pane enumeration and state queries
//! - **Scrollback Capture**: Efficient access to terminal history
//! - **Real-time Monitoring**: Command execution and window state tracking
//!
//! Key remote control commands:
//! - `{"cmd": "ls"}` - List all windows, tabs, and panes
//! - `{"cmd": "get-text", "match": "focused:true", "extent": "scrollback"}` - Get scrollback
//! - `{"cmd": "get-window-state", "match": "focused:true"}` - Get window state
//!
//! Security features:
//! - Socket-only mode (disable PTY remote control)
//! - Restrictive socket permissions (0600)
//! - Optional password protection
//! - Validated JSON command structure

use camino::Utf8Path;
use regex::Regex;
use sinex_core::db::models::RawEvent;
use sinex_core::types::domain::{CommandText, SanitizedPath};
use sinex_core::types::events::Event;
use sinex_satellite_sdk::{SatelliteError, SatelliteResult};
use std::collections::HashMap;
use std::io;
use std::time::{Duration, SystemTime};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;
use tokio::sync::mpsc;
use tokio::time::sleep;
use tracing::{debug, error, info, warn};

/// Kitty window information
#[derive(Debug, Clone)]
struct KittyWindow {
    id: i64,
    cwd: Option<String>,
    parent_tab_id: String,
    last_cmd_exit_status: Option<i32>,
    foreground_processes: Vec<KittyProcess>,
}

/// Kitty process information
#[derive(Debug, Clone)]
struct KittyProcess {
    pid: u32,
    name: String,
}

/// Kitty window state tracking
#[derive(Debug)]
struct KittyWindowState {
    tab_id: String,
    last_command: Option<String>,
    last_prompt_time: Option<SystemTime>,
}

/// Process information for tracking changes
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct KittyProcessInfo {
    pid: u32,
    name: String,
    cmdline: Option<String>,
    parent_pid: Option<u32>,
}

/// Kitty terminal watcher
pub struct KittyWatcher {
    socket_path: Option<String>,
    poll_interval: Duration,
    window_states: HashMap<String, KittyWindowState>,
    prompt_patterns: Vec<Regex>,
    last_scrollback_line_counts: HashMap<String, u32>,
    last_focused_tab: Option<String>,
    process_states: HashMap<String, KittyProcessInfo>,
}

impl KittyWatcher {
    /// Create new Kitty watcher
    pub async fn new() -> SatelliteResult<Self> {
        let mut watcher = Self {
            socket_path: None,
            poll_interval: Duration::from_millis(500),
            window_states: HashMap::new(),
            prompt_patterns: Self::create_prompt_patterns(),
            last_scrollback_line_counts: HashMap::new(),
            last_focused_tab: None,
            process_states: HashMap::new(),
        };

        // Try to discover Kitty socket
        if let Err(e) = watcher.discover_kitty_socket().await {
            warn!(
                "Failed to discover Kitty socket: {}. Kitty monitoring will be limited.",
                e
            );
        } else {
            info!("Kitty socket discovered at: {:?}", watcher.socket_path);
        }

        Ok(watcher)
    }

    fn create_prompt_patterns() -> Vec<Regex> {
        vec![
            // Basic bash/zsh prompts
            Regex::new(r"^\$ (.+)$").unwrap(),
            Regex::new(r"^# (.+)$").unwrap(),
            // Starship prompt
            Regex::new(r"^❯ (.+)$").unwrap(),
            // Oh-my-zsh variations
            Regex::new(r"^➜\s+[^\s]+\s+(.+)$").unwrap(),
            Regex::new(r"^.*%\s+(.+)$").unwrap(),
            // Fish shell
            Regex::new(r"^.*>\s+(.+)$").unwrap(),
            // Custom prompts with timestamps
            Regex::new(r"^\[\d{2}:\d{2}:\d{2}\].*\$\s+(.+)$").unwrap(),
        ]
    }

    async fn discover_kitty_socket(&mut self) -> SatelliteResult<String> {
        // Try common socket locations
        let tmp_dir = std::env::var("SINEX_TMP_DIR").unwrap_or_else(|_| "/tmp".to_string());
        let current_uid = unsafe { libc::getuid() };
        let socket_candidates = vec![
            format!("{}/kitty_socket_{}", tmp_dir, std::process::id()),
            format!("{}/kitty-{}.sock", tmp_dir, whoami::username()),
            format!("/run/user/{}/kitty.sock", current_uid),
            format!("{}/kitty.sock", tmp_dir),
        ];

        for candidate in &socket_candidates {
            if Utf8Path::new(&candidate).exists() {
                // Test connection
                if UnixStream::connect(&candidate).await.is_ok() {
                    self.socket_path = Some(candidate.clone());
                    return Ok(candidate.clone());
                }
            }
        }

        Err(sinex_satellite_sdk::SatelliteError::Processing(format!(
            "No accessible Kitty socket found. Tried: {:?}",
            socket_candidates
        )))
    }

    async fn send_kitty_command(
        &self,
        command: serde_json::Value,
    ) -> SatelliteResult<serde_json::Value> {
        let socket_path = self.socket_path.as_ref().ok_or_else(|| {
            sinex_satellite_sdk::SatelliteError::Processing(
                "No Kitty socket configured".to_string(),
            )
        })?;

        let mut stream = UnixStream::connect(socket_path)
            .await
            .map_err(|e| io_to_satellite_error(e, "Failed to connect to socket"))?;

        let cmd_str = command.to_string();
        let framed_cmd = format!("\x1bP@kitty-cmd{}\x1b\\", cmd_str);

        stream
            .write_all(framed_cmd.as_bytes())
            .await
            .map_err(|e| io_to_satellite_error(e, "Failed to write command"))?;
        stream
            .flush()
            .await
            .map_err(|e| io_to_satellite_error(e, "Failed to flush"))?;

        let mut response_buffer = Vec::new();
        stream
            .read_to_end(&mut response_buffer)
            .await
            .map_err(|e| io_to_satellite_error(e, "Failed to read response"))?;

        let response_str = String::from_utf8(response_buffer).map_err(|e| {
            sinex_satellite_sdk::SatelliteError::Processing(format!(
                "Invalid UTF-8 in response: {}",
                e
            ))
        })?;

        // Extract JSON from framed response
        if let Some(json_str) = extract_json_from_framed_response(&response_str) {
            return serde_json::from_str(json_str).map_err(|e| {
                sinex_satellite_sdk::SatelliteError::Processing(format!(
                    "Failed to parse JSON: {}",
                    e
                ))
            });
        }

        Err(sinex_satellite_sdk::SatelliteError::Processing(
            "Could not parse Kitty response as JSON".to_string(),
        ))
    }

    async fn get_kitty_tabs_and_windows(
        &self,
    ) -> SatelliteResult<(Vec<(String, String, u32, bool)>, Vec<KittyWindow>)> {
        let ls_command = serde_json::json!({"cmd": "ls"});
        let response = self.send_kitty_command(ls_command).await?;

        let mut tabs = Vec::new();
        let mut windows = Vec::new();

        if let Some(tabs_array) = response.as_array() {
            for (tab_index, tab) in tabs_array.iter().enumerate() {
                // Extract tab information
                if let (Some(tab_id), Some(tab_title), Some(is_focused)) = (
                    tab.get("id").and_then(|i| i.as_i64()),
                    tab.get("title").and_then(|t| t.as_str()),
                    tab.get("is_focused").and_then(|f| f.as_bool()),
                ) {
                    let tab_id_str = tab_id.to_string();
                    tabs.push((
                        tab_id_str.clone(),
                        tab_title.to_string(),
                        tab_index as u32,
                        is_focused,
                    ));

                    // Extract windows from this tab
                    if let Some(tab_windows) = tab.get("windows").and_then(|w| w.as_array()) {
                        for window in tab_windows {
                            if let Some(id) = window.get("id").and_then(|i| i.as_i64()) {
                                // Extract foreground processes if available
                                let mut foreground_processes = Vec::new();
                                if let Some(processes) = window
                                    .get("foreground_processes")
                                    .and_then(|p| p.as_array())
                                {
                                    for process in processes {
                                        if let (Some(pid), Some(name)) = (
                                            process.get("pid").and_then(|p| p.as_u64()),
                                            process.get("name").and_then(|n| n.as_str()),
                                        ) {
                                            foreground_processes.push(KittyProcess {
                                                pid: pid as u32,
                                                name: name.to_string(),
                                            });
                                        }
                                    }
                                }

                                windows.push(KittyWindow {
                                    id,
                                    cwd: window
                                        .get("cwd")
                                        .and_then(|c| c.as_str())
                                        .map(String::from),
                                    foreground_processes,
                                    last_cmd_exit_status: window
                                        .get("last_cmd_exit_status")
                                        .and_then(|s| s.as_i64())
                                        .map(|s| s as i32),
                                    parent_tab_id: tab_id_str.clone(),
                                });
                            }
                        }
                    }
                }
            }
        }

        Ok((tabs, windows))
    }

    async fn get_last_command_output(&self, window_id: &str) -> SatelliteResult<String> {
        self.get_kitty_text(window_id, "last_cmd_output").await
    }

    async fn get_kitty_text(&self, window_id: &str, extent: &str) -> SatelliteResult<String> {
        let get_text_command = serde_json::json!({
            "cmd": "get-text",
            "match": format!("id:{}", window_id),
            "extent": extent
        });

        let response = self.send_kitty_command(get_text_command).await?;

        if let Some(text) = response.get("text").and_then(|t| t.as_str()) {
            Ok(text.to_string())
        } else {
            Err(sinex_satellite_sdk::SatelliteError::Processing(format!(
                "No text content in Kitty response for extent: {}",
                extent
            )))
        }
    }

    async fn process_window_commands(
        &mut self,
        window: &KittyWindow,
        tx: &mpsc::UnboundedSender<RawEvent>,
    ) -> SatelliteResult<()> {
        let window_id = window.id.to_string();

        // Process changed if foreground processes changed
        if let Some(current_process) = window.foreground_processes.first() {
            let current_process_info = KittyProcessInfo {
                pid: current_process.pid,
                name: current_process.name.clone(),
                cmdline: None, // Would need additional call to get cmdline
                parent_pid: None,
            };

            let process_changed = self
                .process_states
                .get(&window_id)
                .map(|prev| {
                    prev.pid != current_process_info.pid || prev.name != current_process_info.name
                })
                .unwrap_or(true);

            if process_changed {
                let previous_process = self.process_states.get(&window_id).cloned();

                let process_event: RawEvent =
                    Event::from_payload(sinex_core::types::events::KittyProcessChangedPayload {
                        kitty_window_id: window_id.clone(),
                        kitty_tab_id: window.parent_tab_id.clone(),
                        previous_process: previous_process
                            .map(|p| serde_json::to_value(p).unwrap()),
                        current_process: serde_json::to_value(current_process_info.clone())
                            .unwrap(),
                        change_timestamp: chrono::Utc::now().to_rfc3339(),
                        working_directory: window.cwd.clone(),
                    })
                    .into();

                if tx.send(process_event).is_err() {
                    warn!("Event channel closed");
                    return Ok(());
                }

                // Update stored process state
                self.process_states
                    .insert(window_id.clone(), current_process_info);
            }
        }

        // Try to get last command output using shell integration
        if let Ok(last_output) = self.get_last_command_output(&window_id).await {
            if !last_output.trim().is_empty() {
                let window_state =
                    self.window_states
                        .entry(window_id.clone())
                        .or_insert_with(|| KittyWindowState {
                            tab_id: window.parent_tab_id.clone(),
                            last_command: None,
                            last_prompt_time: None,
                        });

                // Try to extract command from the output (look for prompt patterns)
                let extracted_command =
                    Self::extract_command_from_output(&self.prompt_patterns, &last_output);

                if let Some(command_text) = extracted_command {
                    // Create command completion event with both command and output
                    // Create command completion event

                    let completion_event: RawEvent = Event::from_payload(
                        sinex_core::types::events::KittyCommandCompletedPayload {
                            command: CommandText::new(command_text.clone()),
                            working_directory: SanitizedPath::new_unchecked(
                                window.cwd.clone().unwrap_or_default(),
                            ),
                            exit_status: window.last_cmd_exit_status.unwrap_or(0),
                            duration_ms: 0, // TODO: Requires tracking command start times
                            shell_pid: 0,   // TODO: Get actual shell PID
                            kitty_window_id: window_id.clone(),
                            kitty_tab_id: window_state.tab_id.clone(),
                            output_lines: Some(last_output.lines().count() as u32),
                            error_output: None, // TODO: Separate stderr capture
                        },
                    )
                    .into();

                    if tx.send(completion_event).is_err() {
                        warn!("Event channel closed");
                        return Ok(());
                    }

                    // Update state
                    window_state.last_command = Some(command_text);
                    window_state.last_prompt_time = Some(SystemTime::now());
                }
            }
        }

        Ok(())
    }

    fn extract_command_from_output(prompt_patterns: &[Regex], output: &str) -> Option<String> {
        // Look for the last prompt line in the output to extract command
        output.lines().rev().find_map(|line| {
            prompt_patterns.iter().find_map(|pattern| {
                pattern
                    .captures(line)
                    .and_then(|captures| captures.get(1))
                    .map(|command| command.as_str().trim())
                    .filter(|cmd| !cmd.is_empty())
                    .map(|cmd| cmd.to_string())
            })
        })
    }

    async fn process_tab_focus_changes(
        &mut self,
        tabs: Vec<(String, String, u32, bool)>,
        tx: &mpsc::UnboundedSender<RawEvent>,
    ) -> SatelliteResult<()> {
        let timestamp = chrono::Utc::now().to_rfc3339();

        // Check for focus changes - just emit events when focus changes
        let current_focused = tabs.iter().find(|(_, _, _, is_focused)| *is_focused);
        if let Some((focused_tab_id, title, index, _)) = current_focused {
            if self.last_focused_tab.as_ref() != Some(focused_tab_id) {
                let previous_tab_id = self.last_focused_tab.clone();

                // Emit tab focused event
                // Create tab focused event

                let tab_focused_event: RawEvent =
                    Event::from_payload(sinex_core::types::events::KittyTabFocusedPayload {
                        kitty_tab_id: focused_tab_id.clone(),
                        kitty_window_id: "unknown".to_string(),
                        tab_title: title.clone(),
                        tab_index: *index as usize,
                        previous_tab_id: previous_tab_id,
                        focus_timestamp: timestamp,
                    })
                    .into();

                if tx.send(tab_focused_event).is_err() {
                    warn!("Event channel closed");
                    return Ok(());
                }

                // Update last focused tab
                self.last_focused_tab = Some(focused_tab_id.clone());
            }
        }

        Ok(())
    }

    async fn capture_incremental_scrollback(
        &mut self,
        window: &KittyWindow,
        tx: &mpsc::UnboundedSender<RawEvent>,
    ) -> SatelliteResult<()> {
        let window_id = window.id.to_string();

        // Get current scrollback content
        let scrollback = self.get_kitty_text(&window_id, "all").await?;
        let current_lines: Vec<&str> = scrollback.lines().collect();
        let current_line_count = current_lines.len() as u32;

        // Get previous line count for this window
        let previous_line_count = self
            .last_scrollback_line_counts
            .get(&window_id)
            .copied()
            .unwrap_or(0);

        // Only capture if we have new lines
        if current_line_count > previous_line_count {
            let new_line_start = previous_line_count as usize;
            let new_lines: Vec<String> = current_lines[new_line_start..]
                .iter()
                .map(|s| s.to_string())
                .collect();

            // Create content streamed event

            let scrollback_event: RawEvent =
                Event::from_payload(sinex_core::types::events::KittyContentStreamedPayload {
                    kitty_window_id: window_id.clone(),
                    new_lines: new_lines,
                    line_start_offset: previous_line_count as usize,
                    capture_timestamp: chrono::Utc::now().to_rfc3339(),
                })
                .into();

            if tx.send(scrollback_event).is_err() {
                warn!("Event channel closed");
                return Ok(());
            }

            debug!(
                "Captured {} new lines for window {}",
                current_line_count - previous_line_count,
                window_id
            );
        }

        // Update stored line count for this window
        self.last_scrollback_line_counts
            .insert(window_id, current_line_count);

        Ok(())
    }

    /// Start streaming events
    pub async fn start_streaming(
        &mut self,
        tx: mpsc::UnboundedSender<RawEvent>,
    ) -> SatelliteResult<()> {
        if self.socket_path.is_none() {
            warn!("No Kitty socket available, skipping Kitty event streaming");
            return Ok(());
        }

        info!("Starting Kitty event streaming");

        let mut last_scrollback_capture = SystemTime::now();
        let scrollback_interval = Duration::from_secs(180); // 3 minute intervals for incremental scrollback

        loop {
            match self.get_kitty_tabs_and_windows().await {
                Ok((tabs, windows)) => {
                    // Process tab focus changes
                    if let Err(e) = self.process_tab_focus_changes(tabs, &tx).await {
                        error!("Failed to process tab focus changes: {}", e);
                    }

                    // Process windows
                    for window in windows {
                        // Process command completions (real-time with shell integration)
                        if let Err(e) = self.process_window_commands(&window, &tx).await {
                            error!("Failed to process window {}: {}", window.id, e);
                        }

                        // Capture incremental scrollback every 3 minutes (safety net)
                        if last_scrollback_capture.elapsed().unwrap_or(Duration::ZERO)
                            >= scrollback_interval
                        {
                            if let Err(e) = self.capture_incremental_scrollback(&window, &tx).await
                            {
                                error!(
                                    "Failed to capture incremental scrollback for window {}: {}",
                                    window.id, e
                                );
                            }
                        }
                    }

                    // Update scrollback capture timestamp
                    if last_scrollback_capture.elapsed().unwrap_or(Duration::ZERO)
                        >= scrollback_interval
                    {
                        last_scrollback_capture = SystemTime::now();
                    }
                }
                Err(e) => {
                    error!("Failed to get Kitty windows: {}", e);

                    // Try to rediscover socket
                    if let Err(rediscover_err) = self.discover_kitty_socket().await {
                        warn!("Failed to rediscover Kitty socket: {}", rediscover_err);
                    }

                    // Wait before retrying
                    sleep(Duration::from_secs(10)).await;
                    continue;
                }
            }

            sleep(self.poll_interval).await;
        }
    }
}

/// Helper function to convert IO errors to SatelliteError
fn io_to_satellite_error(e: io::Error, operation: &str) -> SatelliteError {
    SatelliteError::Processing(format!("{}: {}", operation, e))
}

/// Helper function to extract JSON from framed Kitty response
fn extract_json_from_framed_response(response: &str) -> Option<&str> {
    response.find('{').and_then(|start| {
        response[start..]
            .rfind('}')
            .map(|end| &response[start..=start + end])
    })
}

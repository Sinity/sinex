//! Terminal recording watcher (Asciinema)
//!
//! Monitors for asciinema recording files and tracks session lifecycle
//!
//! ## Asciinema Format Details
//!
//! ### File Format (v2)
//! ```json
//! {"version": 2, "width": 80, "height": 24, "timestamp": 1234567890, ...}
//! [0.248, "o", "user@host:~$ "]
//! [0.500, "o", "l"]
//! [0.501, "o", "s"]
//! [0.502, "o", "\r\n"]
//! ```
//!
//! ### Auto-Recording Setup
//! Add to shell profile (.bashrc/.zshrc):
//! ```bash
//! if [[ $- == *i* ]] && [[ -z "$ASCIINEMA_REC" ]]; then
//!     export ASCIINEMA_REC=1
//!     asciinema rec --quiet --stdin --command "$SHELL" \
//!         "/tmp/asciinema/$(date +%Y%m%d_%H%M%S)_$$.cast"
//! fi
//! ```
//!
//! ### Event Generation
//! - `terminal.session.started`: Recording begins
//! - `terminal.session.ended`: Recording completes
//! - Blob storage for .cast files
//! - Metadata includes dimensions, duration, command

use serde_json::json;
use sinex_events::{EventFactory, RawEvent};
use sinex_satellite_sdk::SatelliteResult;
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::{Duration, SystemTime};
use tokio::fs;
use tokio::sync::mpsc;
use tokio::time::interval;
use tracing::{error, info, warn};

/// Recording session information
#[derive(Debug)]
struct RecordingSession {
    id: String,
    _file_path: PathBuf,
    start_time: chrono::DateTime<chrono::Utc>,
    last_size: u64,
    header: Option<AsciinemaHeader>,
}

/// Asciinema header structure
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct AsciinemaHeader {
    version: u32,
    width: u32,
    height: u32,
    timestamp: Option<f64>,
    duration: Option<f64>,
    command: Option<String>,
    title: Option<String>,
    env: Option<HashMap<String, String>>,
}

/// Terminal recording watcher
pub struct RecordingWatcher {
    recordings_dir: PathBuf,
    file_pattern: String,
    polling_interval: Duration,
    auto_start_recording: bool,
    record_command: String,
    active_sessions: HashMap<PathBuf, RecordingSession>,
    processed_files: Vec<PathBuf>,
}

impl RecordingWatcher {
    /// Create new recording watcher
    pub async fn new(recordings_dir: PathBuf) -> SatelliteResult<Self> {
        // Ensure recordings directory exists
        if !recordings_dir.exists() {
            fs::create_dir_all(&recordings_dir).await.map_err(|e| {
                sinex_satellite_sdk::SatelliteError::Processing(format!(
                    "Failed to create recordings directory {}: {}",
                    recordings_dir.display(),
                    e
                ))
            })?;
        }

        let watcher = Self {
            recordings_dir,
            file_pattern: "*.cast".to_string(),
            polling_interval: Duration::from_secs(5),
            auto_start_recording: false,
            record_command: "asciinema rec --quiet --overwrite".to_string(),
            active_sessions: HashMap::new(),
            processed_files: Vec::new(),
        };

        info!(
            "Recording watcher initialized for directory: {}",
            watcher.recordings_dir.display()
        );
        Ok(watcher)
    }

    /// Setup auto-recording integration for shells
    async fn setup_auto_recording(&self) -> SatelliteResult<()> {
        if !self.auto_start_recording {
            return Ok(());
        }

        info!("Setting up automatic asciinema recording for shell sessions");

        let home = std::env::var("HOME").map_err(|e| {
            sinex_satellite_sdk::SatelliteError::Processing(format!(
                "HOME environment variable not set: {}",
                e
            ))
        })?;

        let home_path = PathBuf::from(&home);

        // Create asciinema recordings directory if it doesn't exist
        fs::create_dir_all(&self.recordings_dir)
            .await
            .map_err(|e| {
                sinex_satellite_sdk::SatelliteError::Processing(format!(
                    "Failed to create recordings directory: {}",
                    e
                ))
            })?;

        // Setup shell integration for each supported shell
        self.setup_bash_integration(&home_path).await?;
        self.setup_zsh_integration(&home_path).await?;
        self.setup_fish_integration(&home_path).await?;

        info!("Auto-recording setup completed - new shell sessions will be automatically recorded");
        Ok(())
    }

    async fn setup_bash_integration(&self, home_path: &PathBuf) -> SatelliteResult<()> {
        let bashrc_path = home_path.join(".bashrc");
        self.add_shell_integration(&bashrc_path, "bash").await
    }

    async fn setup_zsh_integration(&self, home_path: &PathBuf) -> SatelliteResult<()> {
        let zshrc_path = home_path.join(".zshrc");
        self.add_shell_integration(&zshrc_path, "zsh").await
    }

    async fn setup_fish_integration(&self, home_path: &PathBuf) -> SatelliteResult<()> {
        let fish_config_dir = home_path.join(".config/fish");
        fs::create_dir_all(&fish_config_dir).await.map_err(|e| {
            sinex_satellite_sdk::SatelliteError::Processing(format!(
                "Failed to create fish config directory: {}",
                e
            ))
        })?;

        let fish_config_path = fish_config_dir.join("config.fish");
        self.add_shell_integration(&fish_config_path, "fish").await
    }

    async fn add_shell_integration(
        &self,
        shell_config_path: &PathBuf,
        shell_name: &str,
    ) -> SatelliteResult<()> {
        // Check if shell config exists
        if !shell_config_path.exists() {
            // Create minimal shell config if it doesn't exist
            fs::write(shell_config_path, "").await.map_err(|e| {
                sinex_satellite_sdk::SatelliteError::Processing(format!(
                    "Failed to create shell config {}: {}",
                    shell_config_path.display(),
                    e
                ))
            })?;
        }

        // Read current config
        let mut config_content = fs::read_to_string(shell_config_path).await.map_err(|e| {
            sinex_satellite_sdk::SatelliteError::Processing(format!(
                "Failed to read shell config {}: {}",
                shell_config_path.display(),
                e
            ))
        })?;

        // Check if sinex integration is already present
        if config_content.contains("# SINEX AUTO-RECORDING") {
            info!("Sinex auto-recording already configured for {}", shell_name);
            return Ok(());
        }

        // Generate shell integration code
        let integration_code = self.generate_shell_integration_code(shell_name)?;

        // Add integration code to shell config
        config_content.push_str("\n\n");
        config_content.push_str(&integration_code);

        // Write updated config
        fs::write(shell_config_path, config_content)
            .await
            .map_err(|e| {
                sinex_satellite_sdk::SatelliteError::Processing(format!(
                    "Failed to write shell config {}: {}",
                    shell_config_path.display(),
                    e
                ))
            })?;

        info!(
            "Added sinex auto-recording integration to {}",
            shell_config_path.display()
        );
        Ok(())
    }

    fn generate_shell_integration_code(&self, shell_name: &str) -> SatelliteResult<String> {
        let recordings_dir = self.recordings_dir.to_string_lossy();
        let record_command = &self.record_command;

        let integration_code = match shell_name {
            "bash" | "zsh" => format!(
                r#"# SINEX AUTO-RECORDING - Automatically record terminal sessions
# Generated by sinex-terminal-satellite RecordingWatcher

# Helper function to generate ULID-like ID (timestamp + randomness)
_sinex_generate_session_ulid() {{
    # Generate timestamp in milliseconds since epoch (ULID compatible)
    local timestamp_ms=$(date +%s%3N 2>/dev/null || echo "$(date +%s)000")
    # Generate random suffix (10 characters, ULID uses base32)
    local random_suffix=$(tr -dc 'A-Z0-9' < /dev/urandom | head -c 10 || echo "$(printf '%010d' $RANDOM$RANDOM)")
    echo "${{timestamp_ms}}_${{random_suffix}}"
}}

# Only auto-record for interactive TTY sessions
if [[ $- == *i* ]] && [[ -t 0 ]] && [[ -z "$SINEX_RECORDING_ACTIVE" ]]; then
    # Generate unique session ULID
    export SINEX_TERMINAL_SESSION_ULID=$(_sinex_generate_session_ulid)
    export SINEX_RECORDING_ACTIVE=1
    
    # Create recording filename
    SINEX_RECORDING_FILE="{recordings_dir}/${{SINEX_TERMINAL_SESSION_ULID}}.cast"
    
    # Ensure recordings directory exists
    mkdir -p "{recordings_dir}"
    
    # Start asciinema recording
    echo "🎬 Sinex: Auto-recording session $SINEX_TERMINAL_SESSION_ULID"
    exec {record_command} --env SINEX_TERMINAL_SESSION_ULID "$SINEX_RECORDING_FILE" "$SHELL"
fi
"#,
                recordings_dir = recordings_dir,
                record_command = record_command
            ),
            "fish" => format!(
                r#"# SINEX AUTO-RECORDING - Automatically record terminal sessions
# Generated by sinex-terminal-satellite RecordingWatcher

# Helper function to generate ULID-like ID
function _sinex_generate_session_ulid
    # Generate timestamp in milliseconds since epoch
    set timestamp_ms (date +%s%3N 2>/dev/null; or echo (date +%s)"000")
    # Generate random suffix
    set random_suffix (tr -dc 'A-Z0-9' < /dev/urandom | head -c 10 2>/dev/null; or printf '%010d' (random)(random))
    echo $timestamp_ms"_"$random_suffix
end

# Only auto-record for interactive TTY sessions
if status is-interactive; and isatty stdin; and not set -q SINEX_RECORDING_ACTIVE
    # Generate unique session ULID
    set -gx SINEX_TERMINAL_SESSION_ULID (_sinex_generate_session_ulid)
    set -gx SINEX_RECORDING_ACTIVE 1
    
    # Create recording filename
    set SINEX_RECORDING_FILE "{recordings_dir}/$SINEX_TERMINAL_SESSION_ULID.cast"
    
    # Ensure recordings directory exists
    mkdir -p "{recordings_dir}"
    
    # Start asciinema recording
    echo "🎬 Sinex: Auto-recording session $SINEX_TERMINAL_SESSION_ULID"
    exec {record_command} --env SINEX_TERMINAL_SESSION_ULID "$SINEX_RECORDING_FILE" (which fish)
end
"#,
                recordings_dir = recordings_dir,
                record_command = record_command
            ),
            _ => {
                return Err(sinex_satellite_sdk::SatelliteError::Processing(format!(
                    "Unsupported shell: {}",
                    shell_name
                )))
            }
        };

        Ok(integration_code)
    }

    async fn scan_recordings(
        &mut self,
        tx: &mpsc::UnboundedSender<RawEvent>,
    ) -> SatelliteResult<()> {
        let pattern = self.recordings_dir.join(&self.file_pattern);
        let pattern_str = pattern.to_string_lossy();

        // Find all recording files
        let paths = glob::glob(&pattern_str).map_err(|e| {
            sinex_satellite_sdk::SatelliteError::Processing(format!(
                "Invalid glob pattern {}: {}",
                pattern_str, e
            ))
        })?;

        for entry in paths {
            match entry {
                Ok(path) => {
                    if let Err(e) = self.process_recording_file(&path, tx).await {
                        error!("Error processing recording {:?}: {}", path, e);
                    }
                }
                Err(e) => warn!("Error reading recording path: {}", e),
            }
        }

        // Clean up completed sessions
        self.cleanup_completed_sessions().await?;

        Ok(())
    }

    async fn process_recording_file(
        &mut self,
        path: &PathBuf,
        tx: &mpsc::UnboundedSender<RawEvent>,
    ) -> SatelliteResult<()> {
        let metadata = fs::metadata(path).await.map_err(|e| {
            sinex_satellite_sdk::SatelliteError::Processing(format!(
                "Failed to get metadata for {}: {}",
                path.display(),
                e
            ))
        })?;

        let file_size = metadata.len();

        // Check if this is a new file
        let is_new = if !self.active_sessions.contains_key(path) {
            // Check if we've already processed this completed file
            if self.processed_files.contains(path) {
                return Ok(());
            }

            // New active recording
            let session_id = sinex_ulid::Ulid::new().to_string();
            self.active_sessions.insert(
                path.clone(),
                RecordingSession {
                    id: session_id.clone(),
                    _file_path: path.clone(),
                    start_time: chrono::Utc::now(),
                    last_size: 0,
                    header: None,
                },
            );
            true
        } else {
            false
        };

        if is_new {
            // Read header and emit session started event
            if let Ok(header) = self.read_asciinema_header(path).await {
                let session_id = {
                    if let Some(session) = self.active_sessions.get_mut(path) {
                        session.header = Some(header.clone());
                        session.id.clone()
                    } else {
                        return Ok(());
                    }
                };

                let payload = json!({
                    "session_id": session_id,
                    "command": header.command.unwrap_or_else(|| "unknown".to_string()),
                    "title": header.title,
                    "env": header.env.unwrap_or_default(),
                    "timestamp": if let Some(ts) = header.timestamp {
                        chrono::DateTime::from_timestamp(ts as i64, 0)
                            .unwrap_or_else(chrono::Utc::now)
                            .to_rfc3339()
                    } else {
                        chrono::Utc::now().to_rfc3339()
                    },
                    "recording_file": path.clone(),
                });

                let factory = EventFactory::new(sinex_events::sources::SHELL_ASCIINEMA);
                let event = factory.create_event("session.started", payload);

                if tx.send(event).is_err() {
                    warn!("Event channel closed");
                    return Ok(());
                }

                info!("Recording session started: {}", session_id);
            }
        } else {
            // Check if file size hasn't changed (recording might be complete)
            let (should_check_complete, session_info) = {
                if let Some(session) = self.active_sessions.get_mut(path) {
                    if session.last_size == file_size && file_size > 0 {
                        (true, Some((session.id.clone(), session.start_time)))
                    } else {
                        session.last_size = file_size;
                        (false, None)
                    }
                } else {
                    (false, None)
                }
            };

            if should_check_complete {
                if let Some((session_id, start_time)) = session_info {
                    // Check if the file has a valid ending
                    if self.is_recording_complete(path).await? {
                        // Emit session ended event
                        let duration = chrono::Utc::now().signed_duration_since(start_time);

                        let payload = json!({
                            "session_id": session_id.clone(),
                            "exit_code": 0, // Would need to parse from recording
                            "duration_secs": duration.num_milliseconds() as f64 / 1000.0,
                            "recording_file": path.clone(),
                            "file_size_bytes": file_size,
                            "git_annex_path": null, // Would be set if using git-annex
                            "git_annex_key": null,
                        });

                        let factory = EventFactory::new(sinex_events::sources::SHELL_ASCIINEMA);
                        let event = factory.create_event("session.ended", payload);

                        if tx.send(event).is_err() {
                            warn!("Event channel closed");
                            return Ok(());
                        }

                        info!("Recording session ended: {}", session_id);

                        // Mark as processed
                        self.processed_files.push(path.clone());

                        // Remove from active sessions
                        self.active_sessions.remove(path);
                    }
                }
            }
        }

        Ok(())
    }

    async fn read_asciinema_header(&self, path: &PathBuf) -> SatelliteResult<AsciinemaHeader> {
        use tokio::io::{AsyncBufReadExt, BufReader};

        let file = fs::File::open(path).await.map_err(|e| {
            sinex_satellite_sdk::SatelliteError::Processing(format!(
                "Failed to open file {}: {}",
                path.display(),
                e
            ))
        })?;

        let reader = BufReader::new(file);
        let mut lines = reader.lines();

        // First line should be the header
        if let Ok(Some(line)) = lines.next_line().await {
            serde_json::from_str(&line).map_err(|e| {
                sinex_satellite_sdk::SatelliteError::Processing(format!(
                    "Failed to parse header: {}",
                    e
                ))
            })
        } else {
            Err(sinex_satellite_sdk::SatelliteError::Processing(
                "No header found".to_string(),
            ))
        }
    }

    async fn is_recording_complete(&self, path: &PathBuf) -> SatelliteResult<bool> {
        // Simple heuristic: check if file hasn't been modified for a while
        // A more sophisticated approach would parse the file to check for proper ending
        let metadata = fs::metadata(path).await.map_err(|e| {
            sinex_satellite_sdk::SatelliteError::Processing(format!(
                "Failed to get metadata: {}",
                e
            ))
        })?;

        if let Ok(modified) = metadata.modified() {
            let elapsed = SystemTime::now()
                .duration_since(modified)
                .unwrap_or(Duration::from_secs(0));

            // If file hasn't been modified for 10 seconds, consider it complete
            Ok(elapsed > Duration::from_secs(10))
        } else {
            Ok(false)
        }
    }

    async fn cleanup_completed_sessions(&mut self) -> SatelliteResult<()> {
        // Clean up old processed files from memory
        if self.processed_files.len() > 1000 {
            let len = self.processed_files.len();
            self.processed_files.drain(0..len.saturating_sub(1000));
        }

        Ok(())
    }

    /// Start streaming events
    pub async fn start_streaming(
        &mut self,
        tx: mpsc::UnboundedSender<RawEvent>,
    ) -> SatelliteResult<()> {
        info!("Starting recording event streaming");

        if self.auto_start_recording {
            // Start recording in new terminals automatically
            self.setup_auto_recording().await?;
        }

        // Monitor for recording files
        let mut poll_interval = interval(self.polling_interval);

        loop {
            poll_interval.tick().await;

            // Scan for new/updated recording files
            if let Err(e) = self.scan_recordings(&tx).await {
                error!("Error scanning recordings: {}", e);
            }
        }
    }
}

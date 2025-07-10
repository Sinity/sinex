use async_trait::async_trait;
use chrono::Utc;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time;
use tracing::{error, info, warn};

use sinex_core::{
    sources, ChannelSenderExt, EventSender, EventSource, EventSourceBase, EventSourceContext, EventType, JsonValue,
    Result, Timestamp, EventFactory, ErrorContext, CoreError, RawEvent,
};

// ============================================================================
// Event Payloads
// ============================================================================

/// Asciinema recording session started
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct AsciinemaSessionStartedPayload {
    pub session_id: String,
    pub command: String,
    pub title: Option<String>,
    pub env: HashMap<String, String>,
    pub timestamp: Timestamp,
    pub recording_file: PathBuf,
}

/// Asciinema recording session ended
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct AsciinemaSessionEndedPayload {
    pub session_id: String,
    pub exit_code: i32,
    pub duration_secs: f64,
    pub recording_file: PathBuf,
    pub file_size_bytes: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub git_annex_path: Option<PathBuf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub git_annex_key: Option<String>,
}

// ============================================================================
// Event Types
// ============================================================================

pub struct AsciinemaSessionStarted;
impl EventType for AsciinemaSessionStarted {
    type Payload = AsciinemaSessionStartedPayload;
    type SourceImpl = AsciinemaRecorder;
    const EVENT_NAME: &'static str = "recording.started";
}

pub struct AsciinemaSessionEnded;
impl EventType for AsciinemaSessionEnded {
    type Payload = AsciinemaSessionEndedPayload;
    type SourceImpl = AsciinemaRecorder;
    const EVENT_NAME: &'static str = "recording.ended";
}

// ============================================================================
// Event Source
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AsciinemaConfig {
    /// Directory to monitor for asciinema recordings
    pub recordings_dir: PathBuf,
    /// Pattern for recording files (e.g., "*.cast")
    pub file_pattern: String,
    /// How often to check for new recordings (seconds)
    pub polling_interval_secs: u64,
    /// Whether to start asciinema recording automatically
    pub auto_start_recording: bool,
    /// Command to run for auto-recording
    pub record_command: String,
    /// Git-annex repository path for storing recordings
    #[serde(default)]
    pub git_annex_repo: Option<PathBuf>,
    /// Automatically add recordings to git-annex
    #[serde(default)]
    pub auto_annex: bool,
}

impl Default for AsciinemaConfig {
    fn default() -> Self {
        let home = std::env::var("HOME").unwrap_or_else(|_| "/home/user".to_string());
        Self {
            recordings_dir: PathBuf::from(&home).join(".local/share/asciinema/asciicast"),
            file_pattern: "*.cast".to_string(),
            polling_interval_secs: 5,
            auto_start_recording: false,
            record_command: "asciinema rec --quiet --overwrite".to_string(),
            git_annex_repo: Some(PathBuf::from("/realm/sinex-annex")),
            auto_annex: true,
        }
    }
}

struct RecordingSession {
    id: String,
    #[allow(dead_code)]
    file_path: PathBuf,
    start_time: Timestamp,
    last_size: u64,
    header: Option<AsciinemaHeader>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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

pub struct AsciinemaRecorder {
    config: AsciinemaConfig,
    active_sessions: Arc<Mutex<HashMap<PathBuf, RecordingSession>>>,
    processed_files: Arc<Mutex<Vec<PathBuf>>>,
    blob_manager: Option<sinex_annex::BlobManager>,
    event_factory: EventFactory,
}

// Implement EventSourceBase to get common functionality
impl EventSourceBase for AsciinemaRecorder {}

#[async_trait]
impl EventSource for AsciinemaRecorder {
    type Config = AsciinemaConfig;

    const SOURCE_NAME: &'static str = sources::SHELL_RECORDING;

    async fn initialize(ctx: EventSourceContext) -> Result<Self> {
        let config = <Self as EventSourceBase>::parse_config::<Self::Config>(&ctx).await?;

        info!("Initializing asciinema recorder");

        // Ensure recordings directory exists
        if !config.recordings_dir.exists() {
            tokio::fs::create_dir_all(&config.recordings_dir)
                .await
                .map_err(|e| 
                    ErrorContext::new(CoreError::Io(format!("Failed to create recordings directory: {}", e)))
                        .with_operation("initialize_asciinema_recorder")
                        .with_context("recordings_dir", config.recordings_dir.display().to_string())
                        .build())?;
        }

        // Initialize BlobManager if configured with both annex path and database
        let annex_repo_path = ctx
            .annex_repo_path
            .clone()
            .or(config.git_annex_repo.as_ref().map(|p| p.to_string_lossy().to_string()));
            
        let blob_manager = match (annex_repo_path.as_ref(), &ctx.db_pool) {
            (Some(repo_path), Some(db_pool)) => {
                let path = std::path::PathBuf::from(repo_path);

                // Initialize git-annex repository if it doesn't exist
                if !path.join(".git").exists() {
                    use sinex_annex::GitAnnex;
                    GitAnnex::init(&path, Some("sinex-asciinema-annex"))
                        .await
                        .map_err(|e| ErrorContext::new(CoreError::Configuration(format!("Failed to initialize git-annex: {}", e)))
                            .with_operation("initialize_asciinema_recorder")
                            .with_context("repo_path", path.display().to_string())
                            .with_context("repo_name", "sinex-asciinema-annex")
                            .build())?;
                }

                let annex_config = sinex_annex::AnnexConfig {
                    repo_path: path.clone(),
                    num_copies: Some(2),
                    large_files: None,
                };

                match sinex_annex::BlobManager::new(annex_config, db_pool.clone()) {
                    Ok(manager) => Some(manager),
                    Err(e) => {
                        error!("Failed to create BlobManager: {}. Asciinema recordings will not be stored.", e);
                        None
                    }
                }
            }
            _ => {
                if annex_repo_path.is_some() && ctx.db_pool.is_none() {
                    info!("Git-annex path configured but no database connection available. Asciinema recordings will not be stored.");
                }
                None
            }
        };

        let recorder = Self {
            config,
            active_sessions: Arc::new(Mutex::new(HashMap::new())),
            processed_files: Arc::new(Mutex::new(Vec::new())),
            blob_manager,
            event_factory: EventFactory::new(Self::SOURCE_NAME),
        };

        // Set up auto-recording if enabled (one-time setup)
        if recorder.config.auto_start_recording {
            recorder.setup_auto_recording().await?;
        }

        Ok(recorder)
    }

    async fn stream_events(&mut self, tx: EventSender) -> Result<()> {
        info!("Starting asciinema recording monitor");

        if self.config.auto_start_recording {
            // Start recording in new terminals automatically
            self.setup_auto_recording().await?;
        }

        // Monitor for recording files
        let mut interval = time::interval(Duration::from_secs(self.config.polling_interval_secs));

        loop {
            interval.tick().await;

            // Scan for new/updated recording files
            if let Err(e) = self.scan_recordings(&tx).await {
                error!("Error scanning recordings: {}", e);
            }
        }
    }
}

impl AsciinemaRecorder {
    async fn setup_auto_recording(&self) -> Result<()> {
        if !self.config.auto_start_recording {
            return Ok(());
        }

        info!("Setting up automatic asciinema recording for shell sessions");

        let home = std::env::var("HOME").map_err(|e| {
            sinex_core::CoreError::configuration("HOME environment variable not set")
                .with_source(e)
                .build()
        })?;

        let home_path = PathBuf::from(&home);

        // Create asciinema recordings directory if it doesn't exist
        tokio::fs::create_dir_all(&self.config.recordings_dir).await.map_err(|e| {
            sinex_core::CoreError::io_error(&self.config.recordings_dir)
                .with_operation("create_recordings_dir")
                .with_source(e)
                .build()
        })?;

        // Setup shell integration for each supported shell
        self.setup_bash_integration(&home_path).await?;
        self.setup_zsh_integration(&home_path).await?;
        self.setup_fish_integration(&home_path).await?;

        info!("Auto-recording setup completed - new shell sessions will be automatically recorded");
        Ok(())
    }

    async fn setup_bash_integration(&self, home_path: &Path) -> Result<()> {
        let bashrc_path = home_path.join(".bashrc");
        self.add_shell_integration(&bashrc_path, "bash").await
    }

    async fn setup_zsh_integration(&self, home_path: &Path) -> Result<()> {
        let zshrc_path = home_path.join(".zshrc");
        self.add_shell_integration(&zshrc_path, "zsh").await
    }

    async fn setup_fish_integration(&self, home_path: &Path) -> Result<()> {
        let fish_config_dir = home_path.join(".config/fish");
        tokio::fs::create_dir_all(&fish_config_dir).await.map_err(|e| {
            sinex_core::CoreError::io_error(&fish_config_dir)
                .with_operation("create_fish_config_dir")
                .with_source(e)
                .build()
        })?;

        let fish_config_path = fish_config_dir.join("config.fish");
        self.add_shell_integration(&fish_config_path, "fish").await
    }

    async fn add_shell_integration(&self, shell_config_path: &PathBuf, shell_name: &str) -> Result<()> {
        // Check if shell config exists
        if !shell_config_path.exists() {
            // Create minimal shell config if it doesn't exist
            tokio::fs::write(shell_config_path, "").await.map_err(|e| {
                sinex_core::CoreError::io_error(shell_config_path)
                    .with_operation("create_shell_config")
                    .with_source(e)
                    .build()
            })?;
        }

        // Read current config
        let mut config_content = tokio::fs::read_to_string(shell_config_path).await.map_err(|e| {
            sinex_core::CoreError::io_error(shell_config_path)
                .with_operation("read_shell_config")
                .with_source(e)
                .build()
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
        tokio::fs::write(shell_config_path, config_content).await.map_err(|e| {
            sinex_core::CoreError::io_error(shell_config_path)
                .with_operation("write_shell_config")
                .with_source(e)
                .build()
        })?;

        info!("Added sinex auto-recording integration to {}", shell_config_path.display());
        Ok(())
    }

    fn generate_shell_integration_code(&self, shell_name: &str) -> Result<String> {
        let recordings_dir = self.config.recordings_dir.to_string_lossy();
        let record_command = &self.config.record_command;

        let integration_code = match shell_name {
            "bash" | "zsh" => format!(
                r#"# SINEX AUTO-RECORDING - Automatically record terminal sessions
# Generated by sinex-events-terminal AsciinemaRecorder

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
# Generated by sinex-events-terminal AsciinemaRecorder

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
            _ => return Err(sinex_core::CoreError::configuration(format!("Unsupported shell: {}", shell_name)).build())
        };

        Ok(integration_code)
    }

    async fn scan_recordings(&self, tx: &EventSender) -> Result<()> {
        let pattern = self.config.recordings_dir.join(&self.config.file_pattern);
        let pattern_str = pattern.to_string_lossy();

        // Find all recording files
        let paths = glob::glob(&pattern_str).map_err(|e| {
            sinex_core::CoreError::configuration("Invalid glob pattern")
                .with_context("pattern", pattern_str.clone())
                .with_source(e)
                .build()
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

    async fn process_recording_file(&self, path: &PathBuf, tx: &EventSender) -> Result<()> {
        let metadata = tokio::fs::metadata(path).await.map_err(|e| {
            sinex_core::CoreError::io_error(path)
                .with_operation("get_metadata")
                .with_source(e)
                .build()
        })?;

        let file_size = metadata.len();

        // Check if this is a new file
        let is_new = {
            let mut sessions = self.active_sessions.lock().unwrap();
            if !sessions.contains_key(path) {
                // Check if we've already processed this completed file
                let processed = self.processed_files.lock().unwrap();
                if processed.contains(path) {
                    return Ok(());
                }

                // New active recording
                let session_id = sinex_ulid::Ulid::new().to_string();
                sessions.insert(
                    path.clone(),
                    RecordingSession {
                        id: session_id.clone(),
                        file_path: path.clone(),
                        start_time: Utc::now(),
                        last_size: 0,
                        header: None,
                    },
                );
                true
            } else {
                false
            }
        };

        if is_new {
            // Read header and emit session started event
            if let Ok(header) = self.read_asciinema_header(path).await {
                let session_id = {
                    let mut sessions = self.active_sessions.lock().unwrap();
                    if let Some(session) = sessions.get_mut(path) {
                        session.header = Some(header.clone());
                        session.id.clone()
                    } else {
                        return Ok(());
                    }
                };

                let payload = AsciinemaSessionStartedPayload {
                    session_id,
                    command: header.command.unwrap_or_else(|| "unknown".to_string()),
                    title: header.title,
                    env: header.env.unwrap_or_default(),
                    timestamp: if let Some(ts) = header.timestamp {
                        sinex_core::timestamp_to_datetime(ts as i64)
                    } else {
                        Utc::now()
                    },
                    recording_file: path.clone(),
                };

                let event = create_event(
                    AsciinemaSessionStarted::EVENT_NAME,
                    serde_json::to_value(payload)?,
                );
                tx.send_or_log(event, "asciinema_session_started").await?;
            }
        } else {
            // Check if file size hasn't changed (recording might be complete)
            let (should_check_complete, session_info) = {
                let mut sessions = self.active_sessions.lock().unwrap();
                if let Some(session) = sessions.get_mut(path) {
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
                        let duration = Utc::now().signed_duration_since(start_time);

                        let mut payload = AsciinemaSessionEndedPayload {
                            session_id: session_id.clone(),
                            exit_code: 0, // Would need to parse from recording
                            duration_secs: duration.num_milliseconds() as f64 / 1000.0,
                            recording_file: path.clone(),
                            file_size_bytes: file_size,
                            git_annex_path: None,
                            git_annex_key: None,
                        };

                        // Add to git-annex if configured and BlobManager available
                        if self.config.auto_annex {
                            if let Some(ref blob_manager) = self.blob_manager {
                                match self.add_to_git_annex(blob_manager, path, &session_id).await {
                                    Ok((filename, annex_key)) => {
                                        payload.git_annex_path = Some(std::path::PathBuf::from(filename));
                                        payload.git_annex_key = Some(annex_key);
                                    }
                                    Err(e) => {
                                        error!("Failed to add recording to git-annex: {}", e);
                                    }
                                }
                            }
                        }

                        let event = create_event(
                            AsciinemaSessionEnded::EVENT_NAME,
                            serde_json::to_value(payload)?,
                        );
                        tx.send_or_log(event, "asciinema_session_ended").await?;

                        // Mark as processed
                        let mut processed = self.processed_files.lock().unwrap();
                        processed.push(path.clone());

                        // Remove from active sessions
                        self.active_sessions.lock().unwrap().remove(path);
                    }
                }
            }
        }

        Ok(())
    }

    async fn read_asciinema_header(&self, path: &PathBuf) -> Result<AsciinemaHeader> {
        use tokio::io::{AsyncBufReadExt, BufReader};

        let file = tokio::fs::File::open(path).await.map_err(|e| {
            sinex_core::CoreError::io_error(path)
                .with_operation("open_file")
                .with_source(e)
                .build()
        })?;

        let reader = BufReader::new(file);
        let mut lines = reader.lines();

        // First line should be the header
        if let Ok(Some(line)) = lines.next_line().await {
            sinex_core::parse_json_file(&line, path, "read_asciinema_header")
        } else {
            Err(ErrorContext::new(CoreError::Validation("No header found".to_string()))
                .with_operation("parse_asciinema_header")
                .with_context("file_path", path.display().to_string())
                .build())
        }
    }

    async fn is_recording_complete(&self, path: &PathBuf) -> Result<bool> {
        // Simple heuristic: check if file hasn't been modified for a while
        // A more sophisticated approach would parse the file to check for proper ending
        let metadata = tokio::fs::metadata(path).await?;

        if let Ok(modified) = metadata.modified() {
            let elapsed = std::time::SystemTime::now()
                .duration_since(modified)
                .unwrap_or(Duration::from_secs(0));

            // If file hasn't been modified for 10 seconds, consider it complete
            Ok(elapsed > Duration::from_secs(10))
        } else {
            Ok(false)
        }
    }

    async fn cleanup_completed_sessions(&self) -> Result<()> {
        // Clean up old processed files from memory
        let mut processed = self.processed_files.lock().unwrap();

        // Keep only last 1000 processed files
        if processed.len() > 1000 {
            let len = processed.len();
            processed.drain(0..len.saturating_sub(1000));
        }

        Ok(())
    }

    async fn add_to_git_annex(
        &self,
        blob_manager: &sinex_annex::BlobManager,
        recording_path: &Path,
        session_id: &str,
    ) -> Result<(String, String)> {
        // Generate filename with timestamp and session ID
        let timestamp = Utc::now().format("%Y%m%d_%H%M%S");
        let filename = format!("recording_{}_{}.cast", timestamp, session_id);

        // Use BlobManager to ingest the recording file
        let metadata = blob_manager
            .ingest_file(recording_path, Some(&filename))
            .await
            .map_err(|e| {
                sinex_core::CoreError::processing_failed()
                    .with_operation("ingest_recording")
                    .with_context("file", recording_path.display())
                    .with_context("filename", filename.clone())
                    .with_source(e)
                    .build()
            })?;

        info!(
            "Successfully ingested recording via BlobManager: {} -> {} (blob_id: {}, key: {}):",
            recording_path.display(),
            filename,
            metadata.blob_id,
            metadata.annex_key
        );

        Ok((filename, metadata.annex_key))
    }
}

fn create_event(event_type: &str, payload: JsonValue) -> RawEvent {
    let event_factory = EventFactory::new(AsciinemaRecorder::SOURCE_NAME);
    event_factory.create_event(event_type, payload)
}

use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::Duration;
use async_trait::async_trait;
use tokio::sync::mpsc;
use tokio::time;
use tracing::{error, info, warn};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use sinex_core::{EventType, EventSource, EventSourceContext, Result};
use sinex_db::models::RawEvent;

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
    const EVENT_NAME: &'static str = "terminal.asciinema.session_started";
}

pub struct AsciinemaSessionEnded;
impl EventType for AsciinemaSessionEnded {
    type Payload = AsciinemaSessionEndedPayload;
    type SourceImpl = AsciinemaRecorder;
    const EVENT_NAME: &'static str = "terminal.asciinema.session_ended";
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
}

#[async_trait]
impl EventSource for AsciinemaRecorder {
    type Config = AsciinemaConfig;
    
    const SOURCE_NAME: &'static str = "ingestor.asciinema_recorder";
    
    async fn initialize(ctx: EventSourceContext) -> Result<Self> {
        let config: Self::Config = serde_json::from_value(ctx.config)
            .map_err(|e| sinex_core::CoreError::Configuration(format!("Failed to parse config: {}", e)))?;
        
        info!("Initializing asciinema recorder");
        
        // Ensure recordings directory exists
        if !config.recordings_dir.exists() {
            tokio::fs::create_dir_all(&config.recordings_dir).await
                .map_err(|e| sinex_core::CoreError::Other(
                    format!("Failed to create recordings directory: {}", e)
                ))?;
        }
        
        Ok(Self {
            config,
            active_sessions: Arc::new(Mutex::new(HashMap::new())),
            processed_files: Arc::new(Mutex::new(Vec::new())),
        })
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
        // This would set up shell integration to automatically start recording
        // For example, by modifying .bashrc/.zshrc to run asciinema rec on new sessions
        warn!("Auto-recording setup not yet implemented - requires shell configuration");
        Ok(())
    }
    
    async fn scan_recordings(&self, tx: &EventSender) -> Result<()> {
        
        let pattern = self.config.recordings_dir.join(&self.config.file_pattern);
        let pattern_str = pattern.to_string_lossy();
        
        // Find all recording files
        let paths = glob::glob(&pattern_str)
            .map_err(|e| sinex_core::CoreError::Other(format!("Invalid glob pattern: {}", e)))?;
        
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
        let metadata = tokio::fs::metadata(path).await
            .map_err(|e| sinex_core::CoreError::Other(format!("Failed to get metadata: {}", e)))?;
        
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
                sessions.insert(path.clone(), RecordingSession {
                    id: session_id.clone(),
                    file_path: path.clone(),
                    start_time: Utc::now(),
                    last_size: 0,
                    header: None,
                });
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
                        DateTime::from_timestamp(ts as i64, 0).unwrap_or_else(Utc::now)
                    } else {
                        Utc::now()
                    },
                    recording_file: path.clone(),
                };
                
                let event = create_event(
                    AsciinemaSessionStarted::EVENT_NAME,
                    serde_json::to_value(payload)?
                );
                tx.send(event).await.map_err(|_| sinex_core::CoreError::Other(
                    "Channel closed".to_string()
                ))?;
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
                        
                        // Add to git-annex if configured
                        if self.config.auto_annex {
                            if let Some(ref annex_repo) = self.config.git_annex_repo {
                                match self.add_to_git_annex(annex_repo, path, &session_id).await {
                                    Ok((annex_path, annex_key)) => {
                                        payload.git_annex_path = Some(annex_path);
                                        payload.git_annex_key = annex_key;
                                    }
                                    Err(e) => {
                                        error!("Failed to add recording to git-annex: {}", e);
                                    }
                                }
                            }
                        }
                        
                        let event = create_event(
                            AsciinemaSessionEnded::EVENT_NAME,
                            serde_json::to_value(payload)?
                        );
                        tx.send(event).await.map_err(|_| sinex_core::CoreError::Other(
                    "Channel closed".to_string()
                ))?;
                        
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
        
        let file = tokio::fs::File::open(path).await
            .map_err(|e| sinex_core::CoreError::Other(format!("Failed to open file: {}", e)))?;
        
        let reader = BufReader::new(file);
        let mut lines = reader.lines();
        
        // First line should be the header
        if let Ok(Some(line)) = lines.next_line().await {
            serde_json::from_str(&line)
                .map_err(|e| sinex_core::CoreError::Other(format!("Failed to parse header: {}", e)))
        } else {
            Err(sinex_core::CoreError::Other("No header found".to_string()))
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
    
    async fn add_to_git_annex(&self, annex_repo: &PathBuf, recording_path: &PathBuf, session_id: &str) -> Result<(PathBuf, Option<String>)> {
        use tokio::process::Command;
        
        // Create subdirectory for asciinema recordings
        let asciinema_dir = annex_repo.join("asciinema");
        tokio::fs::create_dir_all(&asciinema_dir).await
            .map_err(|e| sinex_core::CoreError::Other(format!("Failed to create asciinema dir: {}", e)))?;
        
        // Generate filename with timestamp and session ID
        let timestamp = Utc::now().format("%Y%m%d_%H%M%S");
        let filename = format!("recording_{}_{}.cast", timestamp, session_id);
        let dest_path = asciinema_dir.join(&filename);
        
        // Copy file to git-annex repository
        tokio::fs::copy(recording_path, &dest_path).await
            .map_err(|e| sinex_core::CoreError::Other(format!("Failed to copy recording: {}", e)))?;
        
        // Add to git-annex
        let output = Command::new("git")
            .arg("annex")
            .arg("add")
            .arg(&filename)
            .current_dir(&asciinema_dir)
            .output()
            .await
            .map_err(|e| sinex_core::CoreError::Other(format!("Failed to run git-annex add: {}", e)))?;
        
        if !output.status.success() {
            return Err(sinex_core::CoreError::Other(
                format!("git-annex add failed: {}", String::from_utf8_lossy(&output.stderr))
            ));
        }
        
        // Commit the addition
        let output = Command::new("git")
            .arg("commit")
            .arg("-m")
            .arg(format!("Add asciinema recording {}", session_id))
            .current_dir(&asciinema_dir)
            .output()
            .await
            .map_err(|e| sinex_core::CoreError::Other(format!("Failed to run git commit: {}", e)))?;
        
        if !output.status.success() {
            warn!("git commit failed (might be nothing to commit): {}", String::from_utf8_lossy(&output.stderr));
        }
        
        // Get the git-annex key for the file
        let output = Command::new("git")
            .arg("annex")
            .arg("find")
            .arg("--format=${key}")
            .arg(&filename)
            .current_dir(&asciinema_dir)
            .output()
            .await
            .map_err(|e| sinex_core::CoreError::Other(format!("Failed to get git-annex key: {}", e)))?;
        
        let annex_key = if output.status.success() {
            let key = String::from_utf8_lossy(&output.stdout).trim().to_string();
            if key.is_empty() { None } else { Some(key) }
        } else {
            None
        };
        
        info!("Added asciinema recording to git-annex: {} (key: {:?})", filename, annex_key);
        Ok((dest_path, annex_key))
    }
}

fn create_event(event_type: &str, payload: serde_json::Value) -> RawEvent {
    RawEvent {
        id: sinex_ulid::Ulid::new(),
        source: AsciinemaRecorder::SOURCE_NAME.to_string(),
        event_type: event_type.to_string(),
        ts_ingest: Utc::now(),
        ts_orig: Some(Utc::now()),
        host: gethostname::gethostname().to_string_lossy().to_string(),
        ingestor_version: Some(env!("CARGO_PKG_VERSION").to_string()),
        payload_schema_id: None,
        payload,
    }
}
use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::Duration;
use async_trait::async_trait;
use tokio::sync::mpsc;
use tokio::time;
use tracing::{debug, error, info, warn};

use sinex_core::{EventType, EventSource, Result};
use sinex_db::models::RawEvent;

// ============================================================================
// Event Payloads
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct CommandExecutedAtuinPayload {
    pub command_string: String,
    pub cwd: String,
    pub exit_code: i32,
    pub duration_ns: i64,
    pub atuin_history_id: String,
    pub atuin_session_id: String,
    pub ts_start_orig: DateTime<Utc>,
    pub ts_end_orig: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub terminal_session_ulid: Option<String>,
}

// ============================================================================
// Event Types
// ============================================================================

pub struct CommandExecutedAtuin;
impl EventType for CommandExecutedAtuin {
    type Payload = CommandExecutedAtuinPayload;
    type SourceImpl = AtuinDbReader;
    const EVENT_NAME: &'static str = "shell.command.executed_atuin";
}

// ============================================================================
// Event Source
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AtuinConfig {
    pub db_path: PathBuf,
    pub polling_interval_secs: u64,
    pub batch_size: usize,
    #[serde(default)]
    pub use_file_watch: bool,
}

impl Default for AtuinConfig {
    fn default() -> Self {
        let home = std::env::var("HOME").unwrap_or_else(|_| "/home/user".to_string());
        Self {
            db_path: PathBuf::from(home).join(".local/share/atuin/history.db"),
            polling_interval_secs: 3,
            batch_size: 100,
            use_file_watch: true,
        }
    }
}

pub struct AtuinDbReader {
    config: AtuinConfig,
    last_processed_id: Option<String>,
    hostname: String,
}

#[async_trait]
impl EventSource for AtuinDbReader {
    type Config = AtuinConfig;
    
    const SOURCE_NAME: &'static str = "ingestor.atuin_db_reader";
    
    async fn initialize(config: Self::Config) -> Result<Self> {
        info!(
            db_path = ?config.db_path,
            "Initializing Atuin database reader"
        );
        
        // Verify database exists
        if !config.db_path.exists() {
            return Err(sinex_core::CoreError::Other(
                format!("Atuin database not found at: {:?}", config.db_path)
            ));
        }
        
        // Get current hostname for filtering
        let hostname = gethostname::gethostname()
            .to_string_lossy()
            .to_string();
        
        info!("Will filter Atuin history to host: {}", hostname);
        
        Ok(Self { 
            config,
            last_processed_id: None,
            hostname,
        })
    }
    
    async fn stream_events(&mut self, tx: mpsc::Sender<RawEvent>) -> Result<()> {
        info!(
            db_path = ?self.config.db_path,
            polling_interval = self.config.polling_interval_secs,
            use_file_watch = self.config.use_file_watch,
            "Starting Atuin history event source"
        );
        
        if self.config.use_file_watch {
            // File watching implementation would go here
            // For now, just use polling with a more responsive interval
            warn!("File watching not yet implemented, using polling mode");
            self.poll_mode(tx).await?;
        } else {
            self.poll_mode(tx).await?;
        }
        
        Ok(())
    }
}

impl AtuinDbReader {
    async fn poll_mode(&mut self, tx: mpsc::Sender<RawEvent>) -> Result<()> {
        let mut interval = time::interval(Duration::from_secs(self.config.polling_interval_secs));
        
        loop {
            interval.tick().await;
            
            if let Err(e) = self.poll_atuin_history(&tx).await {
                error!("Error polling Atuin history: {}", e);
            }
        }
    }
    
    async fn poll_atuin_history(&mut self, tx: &mpsc::Sender<RawEvent>) -> Result<()> {
        let db_path = self.config.db_path.clone();
        let last_id = self.last_processed_id.clone();
        let batch_size = self.config.batch_size;
        let hostname = self.hostname.clone();
        
        // Use spawn_blocking to run database operations
        let entries = tokio::task::spawn_blocking(move || -> Result<Vec<AtuinHistoryEntry>> {
            debug!("Opening Atuin database at {:?}", db_path);
            
            let conn = rusqlite::Connection::open_with_flags(
                &db_path,
                rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY
            ).map_err(|e| sinex_core::CoreError::Other(
                format!("Failed to open Atuin database: {}", e)
            ))?;
            
            // Log the total number of entries if this is the first run
            if last_id.is_none() {
                let count: i64 = conn.query_row(
                    "SELECT COUNT(*) FROM history",
                    [],
                    |row| row.get(0)
                ).unwrap_or(0);
                
                info!("Atuin database contains {} history entries", count);
            }
            
            // Query new entries
            let query = if last_id.is_some() {
                "SELECT
                    id,
                    timestamp,
                    duration,
                    exit as exit_code,
                    command,
                    cwd,
                    session,
                    hostname
                FROM history
                WHERE id > ?1 AND hostname = ?3
                ORDER BY id ASC
                LIMIT ?2"
            } else {
                "SELECT
                    id,
                    timestamp,
                    duration,
                    exit as exit_code,
                    command,
                    cwd,
                    session,
                    hostname
                FROM history
                WHERE hostname = ?2
                ORDER BY id ASC
                LIMIT ?1"
            };
            
            let mut stmt = conn.prepare(query).map_err(|e| sinex_core::CoreError::Other(
                format!("Failed to prepare query: {}", e)
            ))?;
            
            let result: Vec<AtuinHistoryEntry> = if let Some(ref last_id) = last_id {
                stmt.query_map(
                    rusqlite::params![last_id, batch_size, hostname],
                    |row| {
                        Ok(AtuinHistoryEntry {
                            id: row.get(0)?,
                            timestamp_ns: row.get(1)?,
                            duration_ns: row.get(2)?,
                            exit_code: row.get(3)?,
                            command: row.get(4)?,
                            cwd: row.get(5)?,
                            session: row.get(6)?,
                            hostname: row.get(7)?,
                        })
                    }
                ).map_err(|e| sinex_core::CoreError::Other(
                    format!("Failed to query history: {}", e)
                ))?
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(|e| sinex_core::CoreError::Other(
                    format!("Failed to read history entry: {}", e)
                ))?
            } else {
                stmt.query_map(
                    rusqlite::params![batch_size, hostname],
                    |row| {
                        Ok(AtuinHistoryEntry {
                            id: row.get(0)?,
                            timestamp_ns: row.get(1)?,
                            duration_ns: row.get(2)?,
                            exit_code: row.get(3)?,
                            command: row.get(4)?,
                            cwd: row.get(5)?,
                            session: row.get(6)?,
                            hostname: row.get(7)?,
                        })
                    }
                ).map_err(|e| sinex_core::CoreError::Other(
                    format!("Failed to query history: {}", e)
                ))?
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(|e| sinex_core::CoreError::Other(
                    format!("Failed to read history entry: {}", e)
                ))?
            };
            
            Ok(result)
        }).await.map_err(|e| sinex_core::CoreError::Other(
            format!("Failed to execute database query: {}", e)
        ))??;
        
        let mut max_id = self.last_processed_id.clone();
        let mut count = 0;
        
        for entry in entries {
            if max_id.is_none() || max_id.as_ref().unwrap() < &entry.id {
                max_id = Some(entry.id.clone());
            }
            
            // Convert to event
            let event = self.convert_to_event(entry)?;
            
            // Send event
            tx.send(event).await.map_err(|_| sinex_core::CoreError::Other(
                "Channel closed".to_string()
            ))?;
            
            count += 1;
        }
        
        if count > 0 {
            info!("Processed {} new Atuin history entries", count);
            self.last_processed_id = max_id;
        } else {
            debug!("No new Atuin history entries");
        }
        
        Ok(())
    }
    
    fn convert_to_event(&self, entry: AtuinHistoryEntry) -> Result<RawEvent> {
        // Convert nanosecond timestamp to DateTime
        // Atuin stores timestamps in nanoseconds since Unix epoch
        let ts_end = DateTime::from_timestamp(
            entry.timestamp_ns / 1_000_000_000,
            (entry.timestamp_ns % 1_000_000_000) as u32
        ).unwrap_or_else(Utc::now);
        
        // Debug log to check timestamp conversion
        debug!(
            "Atuin timestamp: {} ns -> {}", 
            entry.timestamp_ns, 
            ts_end.format("%Y-%m-%d %H:%M:%S UTC")
        );
        
        // Calculate start time from duration
        let duration_secs = entry.duration_ns as f64 / 1_000_000_000.0;
        let ts_start = ts_end - chrono::Duration::milliseconds((duration_secs * 1000.0) as i64);
        
        let payload = CommandExecutedAtuinPayload {
            command_string: entry.command,
            cwd: entry.cwd,
            exit_code: entry.exit_code,
            duration_ns: entry.duration_ns,
            atuin_history_id: entry.id,
            atuin_session_id: entry.session,
            ts_start_orig: ts_start,
            ts_end_orig: ts_end,
            terminal_session_ulid: None, // Could be enhanced to extract from env
        };
        
        Ok(RawEvent {
            id: sinex_ulid::Ulid::new(),
            source: Self::SOURCE_NAME.to_string(),
            event_type: CommandExecutedAtuin::EVENT_NAME.to_string(),
            ts_ingest: Utc::now(),
            ts_orig: Some(ts_end),
            host: entry.hostname,
            ingestor_version: Some(env!("CARGO_PKG_VERSION").to_string()),
            payload_schema_id: None,
            payload: serde_json::to_value(payload)?,
        })
    }
}

#[derive(Debug)]
struct AtuinHistoryEntry {
    id: String,  // Atuin uses TEXT for id
    timestamp_ns: i64,
    duration_ns: i64,
    exit_code: i32,
    command: String,
    cwd: String,
    session: String,
    hostname: String,
}
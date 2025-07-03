use async_trait::async_trait;
use chrono::{DateTime, Utc};
use notify::event::{DataChange, ModifyKind};
use notify::{EventKind, RecursiveMode, Watcher};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::{self, Instant};
use tracing::{debug, error, info, warn};

use sinex_core::RawEvent;
use sinex_core::{
    ChannelSenderExt, DbPoolRef, EventSender, EventSource, EventSourceBase, EventSourceContext,
    EventType, Result, Timestamp,
};
use sinex_db::DbPool;

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
    pub timestamp: i64, // Unix timestamp in nanoseconds
    pub ts_start_orig: Timestamp,
    pub ts_end_orig: Timestamp,
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
    last_processed_timestamp: Option<i64>,
    db_pool: Option<DbPool>,
}

// Implement EventSourceBase to get common functionality
impl EventSourceBase for AtuinDbReader {}

#[async_trait]
impl EventSource for AtuinDbReader {
    type Config = AtuinConfig;

    const SOURCE_NAME: &'static str = "ingestor.atuin_db_reader";

    async fn initialize(ctx: EventSourceContext) -> Result<Self> {
        let config = <Self as EventSourceBase>::parse_config::<Self::Config>(&ctx).await?;

        info!(
            db_path = ?config.db_path,
            "Initializing Atuin database reader"
        );

        // Verify database exists
        if !config.db_path.exists() {
            return Err(sinex_core::CoreError::Other(format!(
                "Atuin database not found at: {:?}",
                config.db_path
            )));
        }

        Ok(Self {
            config,
            last_processed_timestamp: None,
            db_pool: ctx.db_pool,
        })
    }

    async fn stream_events(&mut self, tx: EventSender) -> Result<()> {
        // Try to get last processed timestamp and count from database
        if let Some(ref pool) = self.db_pool {
            match self.get_startup_info_from_pool(pool).await {
                Ok((last_timestamp, our_count)) => {
                    if let Some(ref ts) = last_timestamp {
                        info!("Resuming from last processed Atuin timestamp: {}", ts);
                        self.last_processed_timestamp = last_timestamp;
                    } else {
                        info!("No previous Atuin history found, starting from beginning");
                    }

                    // Compare counts
                    let atuin_count = self.get_atuin_total_count().await?;
                    info!(
                        "Event count comparison - Atuin DB: {}, Our DB: {}, Difference: {}",
                        atuin_count,
                        our_count,
                        atuin_count.saturating_sub(our_count as i64)
                    );
                }
                Err(e) => {
                    warn!(
                        "Failed to query startup info: {}, starting from beginning",
                        e
                    );
                }
            }
        } else {
            warn!("No database connection available, cannot resume from last position");
        }
        info!(
            db_path = ?self.config.db_path,
            polling_interval = self.config.polling_interval_secs,
            use_file_watch = self.config.use_file_watch,
            "Starting Atuin history event source"
        );

        if self.config.use_file_watch {
            self.watch_mode(tx).await?;
        } else {
            self.poll_mode(tx).await?;
        }

        Ok(())
    }
}

impl AtuinDbReader {
    async fn get_atuin_total_count(&self) -> Result<i64> {
        let db_path = self.config.db_path.clone();

        tokio::task::spawn_blocking(move || -> Result<i64> {
            let conn = rusqlite::Connection::open_with_flags(
                &db_path,
                rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
            )
            .map_err(|e| {
                sinex_core::CoreError::Other(format!("Failed to open Atuin database: {}", e))
            })?;

            let count: i64 = conn
                .query_row("SELECT COUNT(*) FROM history", [], |row| row.get(0))
                .unwrap_or(0);

            Ok(count)
        })
        .await
        .map_err(|e| sinex_core::CoreError::Other(format!("Failed to get Atuin count: {}", e)))?
    }

    async fn get_startup_info_from_pool(
        &self,
        pool: DbPoolRef<'_>,
    ) -> Result<(Option<i64>, usize)> {
        use sqlx::Row;

        // Get both last timestamp and count in one query
        let query = r#"
            WITH stats AS (
                SELECT 
                    MAX((payload->>'timestamp')::bigint) as last_timestamp,
                    COUNT(*) as total_count
                FROM raw.events
                WHERE event_type = 'shell.command.executed_atuin'
            )
            SELECT last_timestamp, total_count FROM stats
        "#;

        let result = sqlx::query(query)
            .fetch_optional(pool)
            .await
            .map_err(|e| sinex_core::CoreError::Database(e.to_string()))?;

        match result {
            Some(row) => {
                let last_timestamp: Option<i64> = row.try_get("last_timestamp").ok();
                let count: i64 = row.try_get("total_count").unwrap_or(0);
                Ok((last_timestamp, count as usize))
            }
            None => Ok((None, 0)),
        }
    }

    async fn watch_mode(&mut self, tx: EventSender) -> Result<()> {
        let (notify_tx, mut notify_rx) = mpsc::channel(100);
        let db_path = self.config.db_path.clone();

        // Set up file watcher
        let mut watcher = notify::recommended_watcher(move |res: notify::Result<notify::Event>| {
            if let Ok(event) = res {
                if matches!(
                    event.kind,
                    EventKind::Modify(ModifyKind::Data(DataChange::Any))
                ) {
                    let _ = notify_tx.blocking_send(());
                }
            }
        })
        .map_err(|e| {
            sinex_core::CoreError::Other(format!("Failed to create file watcher: {}", e))
        })?;

        watcher
            .watch(&self.config.db_path, RecursiveMode::NonRecursive)
            .map_err(|e| {
                sinex_core::CoreError::Other(format!("Failed to watch Atuin database: {}", e))
            })?;

        info!("Started file watching on Atuin database: {:?}", db_path);

        // Track last poll time to reset interval on each event
        let poll_interval = Duration::from_secs(self.config.polling_interval_secs);
        let mut last_poll = Instant::now();

        loop {
            tokio::select! {
                // File change detected
                Some(_) = notify_rx.recv() => {
                    debug!("Atuin database file changed, polling for new entries");
                    if let Err(e) = self.poll_atuin_history(&tx).await {
                        error!("Error polling Atuin history after file change: {}", e);
                    }
                    // Reset poll timer on activity
                    last_poll = Instant::now();
                }
                // Periodic poll as fallback (only if no activity)
                _ = time::sleep_until(last_poll + poll_interval) => {
                    debug!("No activity for {} seconds, running periodic poll", self.config.polling_interval_secs);
                    if let Err(e) = self.poll_atuin_history(&tx).await {
                        error!("Error during periodic Atuin poll: {}", e);
                    }
                    last_poll = Instant::now();
                }
            }
        }
    }

    async fn poll_mode(&mut self, tx: EventSender) -> Result<()> {
        let mut interval = time::interval(Duration::from_secs(self.config.polling_interval_secs));

        loop {
            interval.tick().await;

            if let Err(e) = self.poll_atuin_history(&tx).await {
                error!("Error polling Atuin history: {}", e);
            }
        }
    }

    async fn poll_atuin_history(&mut self, tx: &EventSender) -> Result<()> {
        let db_path = self.config.db_path.clone();
        let last_timestamp = self.last_processed_timestamp;
        let batch_size = self.config.batch_size;

        // Use spawn_blocking to run database operations
        let entries = tokio::task::spawn_blocking(move || -> Result<Vec<AtuinHistoryEntry>> {
            debug!("Opening Atuin database at {:?}", db_path);

            let conn = rusqlite::Connection::open_with_flags(
                &db_path,
                rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
            )
            .map_err(|e| {
                sinex_core::CoreError::Other(format!("Failed to open Atuin database: {}", e))
            })?;

            // Log the total number of entries if this is the first run
            if last_timestamp.is_none() {
                let count: i64 = conn
                    .query_row("SELECT COUNT(*) FROM history", [], |row| row.get(0))
                    .unwrap_or(0);

                info!("Atuin database contains {} history entries", count);
            }

            // Query new entries using timestamp
            let query = if last_timestamp.is_some() {
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
                WHERE timestamp > ?1
                ORDER BY timestamp ASC
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
                ORDER BY timestamp ASC
                LIMIT ?1"
            };

            let mut stmt = conn.prepare(query).map_err(|e| {
                sinex_core::CoreError::Other(format!("Failed to prepare query: {}", e))
            })?;

            let result: Vec<AtuinHistoryEntry> = if let Some(ref last_ts) = last_timestamp {
                stmt.query_map(rusqlite::params![last_ts, batch_size], |row| {
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
                })
                .map_err(|e| {
                    sinex_core::CoreError::Other(format!("Failed to query history: {}", e))
                })?
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(|e| {
                    sinex_core::CoreError::Other(format!("Failed to read history entry: {}", e))
                })?
            } else {
                stmt.query_map(rusqlite::params![batch_size], |row| {
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
                })
                .map_err(|e| {
                    sinex_core::CoreError::Other(format!("Failed to query history: {}", e))
                })?
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(|e| {
                    sinex_core::CoreError::Other(format!("Failed to read history entry: {}", e))
                })?
            };

            Ok(result)
        })
        .await
        .map_err(|e| {
            sinex_core::CoreError::Other(format!("Failed to execute database query: {}", e))
        })??;

        let mut max_timestamp = self.last_processed_timestamp;
        let mut count = 0;

        for entry in entries {
            if max_timestamp.is_none() || max_timestamp.as_ref().unwrap() < &entry.timestamp_ns {
                max_timestamp = Some(entry.timestamp_ns);
            }

            // Convert to event
            let event = self.convert_to_event(entry)?;

            // Send event
            tx.send_or_log(event, "atuin_history_entry").await?;

            count += 1;
        }

        if count > 0 {
            info!("Processed {} new Atuin history entries", count);
            self.last_processed_timestamp = max_timestamp;
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
            (entry.timestamp_ns % 1_000_000_000) as u32,
        )
        .unwrap_or_else(Utc::now);

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
            timestamp: entry.timestamp_ns,
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
    id: String, // Atuin uses TEXT for id
    timestamp_ns: i64,
    duration_ns: i64,
    exit_code: i32,
    command: String,
    cwd: String,
    session: String,
    hostname: String,
}

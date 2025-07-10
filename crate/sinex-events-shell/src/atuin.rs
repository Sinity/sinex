//! Atuin History Integration
//!
//! This module provides event sourcing from Atuin shell history database.
//! Atuin (https://atuin.sh) is a shell history replacement that stores command
//! history in a SQLite database with rich metadata.

use async_trait::async_trait;
use notify::event::{DataChange, ModifyKind};
use notify::{EventKind, RecursiveMode, Watcher};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::{self, Instant};
use tracing::{debug, error, info, warn};

use sinex_core::{
    sources, ChannelSenderExt, DbPoolRef, EventSender, EventSource, EventSourceBase,
    EventSourceContext, Result, Timestamp, EventFactory, CoreError, RawEvent,
    SqliteConnection, SqliteStatementExt,
};
use sinex_db::DbPool;

use crate::ShellCommandInfo;

// ============================================================================
// Event Payloads
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct AtuinCommandExecutedPayload {
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
    /// Parsed command information
    pub shell_command_info: ShellCommandInfo,
}

// ============================================================================
// Event Types
// ============================================================================

pub struct AtuinCommandExecuted;

// ============================================================================
// Configuration
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct AtuinConfig {
    pub db_path: PathBuf,
    pub polling_interval_secs: u64,
    pub batch_size: usize,
    #[serde(default)]
    pub use_file_watch: bool,
    /// Minimum command length to import
    pub min_command_length: usize,
    /// Commands to exclude from import
    pub excluded_commands: Vec<String>,
}

impl Default for AtuinConfig {
    fn default() -> Self {
        let home = std::env::var("HOME").unwrap_or_else(|_| "/home/user".to_string());
        Self {
            db_path: PathBuf::from(home).join(".local/share/atuin/history.db"),
            polling_interval_secs: 3,
            batch_size: 100,
            use_file_watch: true,
            min_command_length: 2,
            excluded_commands: vec![
                "ls".to_string(),
                "cd".to_string(),
                "pwd".to_string(),
                "clear".to_string(),
            ],
        }
    }
}

// ============================================================================
// Atuin History Importer
// ============================================================================

pub struct AtuinHistoryImporter {
    config: AtuinConfig,
    last_processed_timestamp: Option<i64>,
    db_pool: Option<DbPool>,
    event_factory: EventFactory,
}

// Implement EventSourceBase to get common functionality
impl EventSourceBase for AtuinHistoryImporter {}

#[async_trait]
impl EventSource for AtuinHistoryImporter {
    type Config = AtuinConfig;

    const SOURCE_NAME: &'static str = sources::SHELL_ATUIN;

    async fn initialize(ctx: EventSourceContext) -> Result<Self> {
        let config = <Self as EventSourceBase>::parse_config::<Self::Config>(&ctx).await?;

        info!(
            db_path = ?config.db_path,
            "Initializing Atuin history importer"
        );

        // Verify database exists
        if !config.db_path.exists() {
            return Err(CoreError::Configuration(format!(
                "Atuin database not found at {:?}",
                config.db_path
            )));
        }

        Ok(Self {
            config,
            last_processed_timestamp: None,
            db_pool: ctx.db_pool,
            event_factory: EventFactory::new(Self::SOURCE_NAME),
        })
    }

    async fn stream_events(&mut self, tx: EventSender) -> Result<()> {
        // Try to get last processed timestamp from database
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
                    if let Ok(atuin_count) = self.get_atuin_total_count().await {
                        info!(
                            "Event count comparison - Atuin DB: {}, Our DB: {}, Difference: {}",
                            atuin_count,
                            our_count,
                            atuin_count.saturating_sub(our_count as i64)
                        );
                    }
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

impl AtuinHistoryImporter {
    async fn get_atuin_total_count(&self) -> Result<i64> {
        let db_path = self.config.db_path.clone();

        tokio::task::spawn_blocking(move || -> Result<i64> {
            let conn = SqliteConnection::open_readonly(&db_path, "get_atuin_total_count")?;

            let count: i64 = conn
                .query_row("SELECT COUNT(*) FROM history", [], |row| row.get(0))
                .unwrap_or(0);

            Ok(count)
        })
        .await
        .map_err(|e| CoreError::Io(format!("Failed to get Atuin count: {}", e)))?
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
                WHERE event_type = 'command.executed' AND source = 'shell.atuin'
            )
            SELECT last_timestamp, total_count FROM stats
        "#;

        let result = sqlx::query(query)
            .fetch_optional(pool)
            .await
            .map_err(|e| CoreError::Database(format!("Failed to query startup info: {}", e)))?;

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
        .map_err(|e| CoreError::Configuration(format!("Failed to create file watcher: {}", e)))?;

        watcher
            .watch(&self.config.db_path, RecursiveMode::NonRecursive)
            .map_err(|e| CoreError::Configuration(format!("Failed to watch Atuin database: {}", e)))?;

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

            let conn = SqliteConnection::open_readonly(&db_path, "poll_atuin_history")?;

            // Log the total number of entries if this is the first run
            if last_timestamp.is_none() {
                let count: i64 = conn
                    .query_row("SELECT COUNT(*) FROM history", [], |row| row.get(0))
                    .unwrap_or(0);

                info!("Atuin database contains {} history entries", count);
            }

            // Query new entries using timestamp
            let query = if last_timestamp.is_some() {
                "SELECT id, timestamp, duration, exit as exit_code, command, cwd, session, hostname
                FROM history
                WHERE timestamp > ?1
                ORDER BY timestamp ASC
                LIMIT ?2"
            } else {
                "SELECT id, timestamp, duration, exit as exit_code, command, cwd, session, hostname
                FROM history
                ORDER BY timestamp ASC
                LIMIT ?1"
            };

            let mut stmt = conn.prepare_with_context(query, "poll_atuin_history")?;

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
                .map_err(|e| CoreError::Database(format!("Query failed: {}", e)))?
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(|e| CoreError::Database(format!("Row collection failed: {}", e)))?
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
                .map_err(|e| CoreError::Database(format!("Query failed: {}", e)))?
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(|e| CoreError::Database(format!("Row collection failed: {}", e)))?
            };

            Ok(result)
        })
        .await
        .map_err(|e| CoreError::Io(format!("Failed to execute database query: {}", e)))??;

        let mut max_timestamp = self.last_processed_timestamp;
        let mut count = 0;

        for entry in entries {
            // Skip commands that are too short or excluded
            if entry.command.len() < self.config.min_command_length {
                continue;
            }

            if let Ok((command, _)) = ShellCommandInfo::parse_command_line(&entry.command) {
                if self.config.excluded_commands.contains(&command) {
                    continue;
                }
            }

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
        let ts_end = sinex_core::timestamp_nanos_to_datetime(entry.timestamp_ns);

        // Calculate start time from duration
        let duration_secs = entry.duration_ns as f64 / 1_000_000_000.0;
        let ts_start = ts_end - chrono::Duration::milliseconds((duration_secs * 1000.0) as i64);

        // Parse command information
        let (command, args) = ShellCommandInfo::parse_command_line(&entry.command)
            .unwrap_or_else(|_| (entry.command.clone(), Vec::new()));

        let shell_command_info = ShellCommandInfo {
            command: command.clone(),
            args,
            working_directory: Some(entry.cwd.clone()),
            shell_type: None, // Could be enhanced to detect shell type
            session_id: Some(entry.session.clone()),
            pid: None, // Not available in Atuin
            exit_code: Some(entry.exit_code),
            execution_time_ms: Some((entry.duration_ns / 1_000_000) as u64),
            start_time: ts_start,
            end_time: Some(ts_end),
        };

        let payload = AtuinCommandExecutedPayload {
            command_string: entry.command,
            cwd: entry.cwd,
            exit_code: entry.exit_code,
            duration_ns: entry.duration_ns,
            atuin_history_id: entry.id,
            atuin_session_id: entry.session,
            timestamp: entry.timestamp_ns,
            ts_start_orig: ts_start,
            ts_end_orig: ts_end,
            terminal_session_ulid: None,
            shell_command_info,
        };

        let event = self.event_factory.create_event(
            "command.executed",
            serde_json::to_value(payload)?,
        );
        
        Ok(event)
    }
}

#[derive(Debug)]
struct AtuinHistoryEntry {
    id: String,
    timestamp_ns: i64,
    duration_ns: i64,
    exit_code: i32,
    command: String,
    cwd: String,
    session: String,
    #[allow(dead_code)]
    hostname: String,
}
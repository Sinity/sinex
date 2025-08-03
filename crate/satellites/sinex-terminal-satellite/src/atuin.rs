//! Atuin shell history watcher
//!
//! Watches the Atuin SQLite database for new command entries with sophisticated
//! file watching and batch processing capabilities

use camino::Utf8PathBuf;
use chrono::DateTime;
use notify::event::{DataChange, ModifyKind};
use notify::{EventKind, RecursiveMode, Watcher};
use rusqlite::{Connection, Row};
use sinex_db::models::Event;
use sinex_satellite_sdk::SatelliteResult;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::{self, Instant};
use tracing::{debug, error, info, warn}; // event_types already imported above

/// Configuration for Atuin watcher
#[derive(Debug, Clone)]
pub struct AtuinConfig {
    pub db_path: Utf8PathBuf,
    pub polling_interval_secs: u64,
    pub batch_size: usize,
    pub use_file_watch: bool,
}

impl Default for AtuinConfig {
    fn default() -> Self {
        let home = std::env::var("HOME").unwrap_or_else(|_| "/home/user".to_string());
        Self {
            db_path: Utf8PathBuf::from(home).join(".local/share/atuin/history.db"),
            polling_interval_secs: 3,
            batch_size: 100,
            use_file_watch: true,
        }
    }
}

/// Atuin database watcher with file watching and batch processing
pub struct AtuinWatcher {
    config: AtuinConfig,
    last_timestamp: Option<i64>,
}

impl AtuinWatcher {
    /// Create new Atuin watcher with default config
    pub async fn new(db_path: Utf8PathBuf) -> SatelliteResult<Self> {
        let mut config = AtuinConfig::default();
        config.db_path = db_path;
        Self::with_config(config).await
    }

    /// Create new Atuin watcher with custom config
    pub async fn with_config(config: AtuinConfig) -> SatelliteResult<Self> {
        // Test database connection
        Self::test_connection(&config.db_path)?;

        Ok(Self {
            config,
            last_timestamp: None,
        })
    }

    /// Test database connection and verify schema
    fn test_connection(db_path: &Utf8PathBuf) -> SatelliteResult<()> {
        if !db_path.exists() {
            return Err(sinex_satellite_sdk::SatelliteError::Processing(format!(
                "Atuin database not found: {}",
                db_path.as_str()
            )));
        }

        let conn = Connection::open(db_path).map_err(|e| {
            sinex_satellite_sdk::SatelliteError::Processing(format!(
                "Failed to open Atuin DB: {}",
                e
            ))
        })?;

        // Test query to verify schema
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM history", [], |row| row.get(0))
            .map_err(|e| {
                sinex_satellite_sdk::SatelliteError::Processing(format!(
                    "Failed to query Atuin DB: {}",
                    e
                ))
            })?;

        info!("Connected to Atuin database with {} total entries", count);
        Ok(())
    }

    /// Get total count from Atuin database
    pub async fn get_atuin_total_count(&self) -> SatelliteResult<i64> {
        let db_path = self.config.db_path.clone();

        tokio::task::spawn_blocking(move || -> SatelliteResult<i64> {
            let conn = Connection::open(&db_path).map_err(|e| {
                sinex_satellite_sdk::SatelliteError::Processing(format!(
                    "Failed to open Atuin DB: {}",
                    e
                ))
            })?;

            let count: i64 = conn
                .query_row("SELECT COUNT(*) FROM history", [], |row| row.get(0))
                .unwrap_or(0);

            Ok(count)
        })
        .await
        .map_err(|e| {
            sinex_satellite_sdk::SatelliteError::Processing(format!(
                "Failed to get Atuin count: {}",
                e
            ))
        })?
    }

    /// Get the latest timestamp from database to resume from
    async fn get_last_timestamp(&self) -> SatelliteResult<Option<i64>> {
        let db_path = self.config.db_path.clone();

        tokio::task::spawn_blocking(move || -> SatelliteResult<Option<i64>> {
            let conn = Connection::open(&db_path).map_err(|e| {
                sinex_satellite_sdk::SatelliteError::Processing(format!(
                    "Failed to open Atuin DB: {}",
                    e
                ))
            })?;

            let result: Result<i64, rusqlite::Error> =
                conn.query_row("SELECT MAX(timestamp) FROM history", [], |row| row.get(0));

            match result {
                Ok(timestamp) => Ok(Some(timestamp)),
                Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
                Err(e) => Err(sinex_satellite_sdk::SatelliteError::Processing(format!(
                    "Failed to get last timestamp: {}",
                    e
                ))),
            }
        })
        .await
        .map_err(|e| {
            sinex_satellite_sdk::SatelliteError::Processing(format!("Spawn blocking failed: {}", e))
        })?
    }

    /// Query new commands since last timestamp with proper async handling
    async fn query_new_commands(&mut self) -> SatelliteResult<Vec<Event>> {
        let db_path = self.config.db_path.clone();
        let last_timestamp = self.last_timestamp;
        let batch_size = self.config.batch_size;

        let (entries, max_timestamp) = tokio::task::spawn_blocking(
            move || -> SatelliteResult<(Vec<AtuinHistoryEntry>, Option<i64>)> {
                let conn = Connection::open(&db_path).map_err(|e| {
                    sinex_satellite_sdk::SatelliteError::Processing(format!(
                        "Failed to open Atuin DB: {}",
                        e
                    ))
                })?;

                let query = if last_timestamp.is_some() {
                    "SELECT id, timestamp, duration, exit, command, cwd, session, hostname 
                 FROM history 
                 WHERE timestamp > ?1 
                 ORDER BY timestamp ASC 
                 LIMIT ?2"
                } else {
                    "SELECT id, timestamp, duration, exit, command, cwd, session, hostname 
                 FROM history 
                 ORDER BY timestamp ASC 
                 LIMIT ?1"
                };

                let mut stmt = conn.prepare(query).map_err(|e| {
                    sinex_satellite_sdk::SatelliteError::Processing(format!(
                        "Failed to prepare query: {}",
                        e
                    ))
                })?;

                let row_mapper = |row: &Row| -> Result<AtuinHistoryEntry, rusqlite::Error> {
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
                };

                let entries: Vec<AtuinHistoryEntry> = if let Some(last_ts) = last_timestamp {
                    stmt.query_map([last_ts, batch_size as i64], row_mapper)
                        .map_err(|e| {
                            sinex_satellite_sdk::SatelliteError::Processing(format!(
                                "Query with timestamp failed: {}",
                                e
                            ))
                        })?
                        .collect::<Result<Vec<_>, _>>()
                        .map_err(|e| {
                            sinex_satellite_sdk::SatelliteError::Processing(format!(
                                "Row mapping failed: {}",
                                e
                            ))
                        })?
                } else {
                    stmt.query_map([batch_size as i64], row_mapper)
                        .map_err(|e| {
                            sinex_satellite_sdk::SatelliteError::Processing(format!(
                                "Query without timestamp failed: {}",
                                e
                            ))
                        })?
                        .collect::<Result<Vec<_>, _>>()
                        .map_err(|e| {
                            sinex_satellite_sdk::SatelliteError::Processing(format!(
                                "Row mapping failed: {}",
                                e
                            ))
                        })?
                };

                let max_timestamp = entries.iter().map(|e| e.timestamp_ns).max();
                Ok((entries, max_timestamp))
            },
        )
        .await
        .map_err(|e| {
            sinex_satellite_sdk::SatelliteError::Processing(format!("Spawn blocking failed: {}", e))
        })??;

        let mut events = Vec::new();
        for entry in entries {
            match self.create_event_from_entry(&entry) {
                Ok(event) => events.push(event),
                Err(e) => warn!("Failed to create event from entry {}: {}", entry.id, e),
            }
        }

        // Update last processed timestamp
        if let Some(new_ts) = max_timestamp {
            if self.last_timestamp.is_none_or(|last| new_ts > last) {
                self.last_timestamp = Some(new_ts);
                debug!("Updated last timestamp to: {}", new_ts);
            }
        }

        if !events.is_empty() {
            info!("Found {} new Atuin commands", events.len());
        }

        Ok(events)
    }

    /// Create Event from Atuin history entry
    fn create_event_from_entry(
        &self,
        entry: &AtuinHistoryEntry,
    ) -> Result<Event, sinex_satellite_sdk::SatelliteError> {
        // Convert nanosecond timestamp to UTC datetime for proper timestamps
        let ts_end = DateTime::from_timestamp_nanos(entry.timestamp_ns);

        // Calculate start time from duration
        let duration_secs = entry.duration_ns as f64 / 1_000_000_000.0;
        let ts_start = ts_end - chrono::Duration::milliseconds((duration_secs * 1000.0) as i64);

        let event = Event::from_payload(sinex_types::events::AtuinCommandExecutedPayload {
            command_string: entry.command.clone(),
            cwd: entry.cwd.clone(),
            exit_code: entry.exit_code,
            duration_ns: entry.duration_ns,
            atuin_history_id: entry.id.clone(),
            atuin_session_id: entry.session.clone(),
            timestamp: entry.timestamp_ns,
            ts_start_orig: ts_start,
            ts_end_orig: ts_end,
            hostname: entry.hostname.clone(),
            terminal_session_ulid: None, // Could be enhanced later
        });

        Ok(event)
    }

    /// Start streaming events with file watching or polling mode
    pub async fn start_streaming(
        &mut self,
        tx: mpsc::UnboundedSender<Event>,
    ) -> SatelliteResult<()> {
        info!(
            db_path = ?self.config.db_path,
            polling_interval = self.config.polling_interval_secs,
            use_file_watch = self.config.use_file_watch,
            "Starting Atuin history event streaming"
        );

        // Get initial timestamp if not set
        if self.last_timestamp.is_none() {
            self.last_timestamp = self.get_last_timestamp().await?;
            if let Some(ts) = self.last_timestamp {
                info!("Starting from Atuin timestamp: {}", ts);
            }
        }

        // Log count information
        match self.get_atuin_total_count().await {
            Ok(count) => info!("Atuin database contains {} total entries", count),
            Err(e) => warn!("Failed to get Atuin total count: {}", e),
        }

        if self.config.use_file_watch {
            self.watch_mode(tx).await
        } else {
            self.poll_mode(tx).await
        }
    }

    /// File watching mode with event-driven polling
    async fn watch_mode(&mut self, tx: mpsc::UnboundedSender<Event>) -> SatelliteResult<()> {
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
            sinex_satellite_sdk::SatelliteError::Processing(format!(
                "Failed to create file watcher: {}",
                e
            ))
        })?;

        watcher
            .watch(
                self.config.db_path.as_std_path(),
                RecursiveMode::NonRecursive,
            )
            .map_err(|e| {
                sinex_satellite_sdk::SatelliteError::Processing(format!(
                    "Failed to watch Atuin database: {}",
                    e
                ))
            })?;

        info!("Started file watching on Atuin database: {:?}", db_path);

        // Track last poll time to reset interval on each event
        let poll_interval = Duration::from_secs(self.config.polling_interval_secs);
        let mut last_poll = std::time::Instant::now();

        loop {
            tokio::select! {
                // File change detected
                Some(_) = notify_rx.recv() => {
                    debug!("Atuin database file changed, polling for new entries");
                    if let Err(e) = self.poll_atuin_history(&tx).await {
                        error!("Error polling Atuin history after file change: {}", e);
                    }
                    // Reset poll timer on activity
                    last_poll = std::time::Instant::now();
                }
                // Periodic poll as fallback (only if no activity)
                _ = time::sleep_until(Instant::from_std(last_poll + poll_interval)) => {
                    debug!("No activity for {} seconds, running periodic poll", self.config.polling_interval_secs);
                    if let Err(e) = self.poll_atuin_history(&tx).await {
                        error!("Error during periodic Atuin poll: {}", e);
                    }
                    last_poll = std::time::Instant::now();
                }
            }
        }
    }

    /// Simple polling mode without file watching
    async fn poll_mode(&mut self, tx: mpsc::UnboundedSender<Event>) -> SatelliteResult<()> {
        let mut interval = time::interval(Duration::from_secs(self.config.polling_interval_secs));

        loop {
            interval.tick().await;
            if let Err(e) = self.poll_atuin_history(&tx).await {
                error!("Error polling Atuin history: {}", e);
            }
        }
    }

    /// Poll Atuin history and send events
    async fn poll_atuin_history(
        &mut self,
        tx: &mpsc::UnboundedSender<Event>,
    ) -> SatelliteResult<()> {
        match self.query_new_commands().await {
            Ok(events) => {
                let count = events.len();
                for event in events {
                    if tx.send(event).is_err() {
                        warn!("Event channel closed, stopping Atuin watcher");
                        return Ok(());
                    }
                }
                if count > 0 {
                    debug!("Processed {} new Atuin history entries", count);
                }
            }
            Err(e) => {
                error!("Failed to query Atuin commands: {}", e);
            }
        }
        Ok(())
    }
}

/// Atuin history entry structure
#[derive(Debug, Clone)]
struct AtuinHistoryEntry {
    id: String,
    timestamp_ns: i64,
    duration_ns: i64,
    exit_code: i32,
    command: String,
    cwd: String,
    session: String,
    hostname: String,
}

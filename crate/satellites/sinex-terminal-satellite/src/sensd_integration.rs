//! Integration with sensd for terminal data acquisition
//!
//! This module refactors the terminal satellite to use sensd's MaterialSliceStream
//! instead of directly creating events from terminal sources.

use chrono::{DateTime, Utc};
use color_eyre::eyre::{eyre, Result};
use futures::pin_mut;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sinex_core::{
    db::models::{event::OffsetKind, Provenance, RawEvent, SourceMaterial},
    types::{
        domain::{EventSource, EventType},
        Id, Ulid,
    },
};
use sinex_schema::ulid_conversions::ulid_to_uuid;
use sqlx::PgPool;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tracing::{debug, error, info};

/// Configuration for sensd integration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SensdIntegrationConfig {
    /// Database URL for connecting to sensd tables
    pub database_url: String,

    /// gRPC endpoint for MaterialSliceStream service
    pub sensd_grpc_endpoint: String,

    /// Batch size for processing material slices
    pub batch_size: usize,

    /// Processing interval in milliseconds
    pub processing_interval_ms: u64,
}

impl Default for SensdIntegrationConfig {
    fn default() -> Self {
        Self {
            database_url: String::from("postgresql:///sinex_dev?host=/run/postgresql"),
            sensd_grpc_endpoint: String::from("http://localhost:50051"),
            batch_size: 100,
            processing_interval_ms: 1000,
        }
    }
}

/// Material slice from sensd
#[derive(Debug, Clone)]
pub struct MaterialSlice {
    pub material_id: Ulid,
    pub offset_start: i64,
    pub offset_end: i64,
    pub ts_capture_start: DateTime<Utc>,
    pub ts_capture_end: DateTime<Utc>,
    pub data: Vec<u8>,
    pub metadata: serde_json::Value,
}

/// Terminal processor that uses sensd for data acquisition
pub struct SensdTerminalProcessor {
    config: SensdIntegrationConfig,
    db_pool: PgPool,
    event_sender: mpsc::Sender<RawEvent>,
}

impl SensdTerminalProcessor {
    /// Create new processor
    pub async fn new(
        config: SensdIntegrationConfig,
        event_sender: mpsc::Sender<RawEvent>,
    ) -> Result<Self> {
        let db_pool = PgPool::connect(&config.database_url).await?;

        Ok(Self {
            config,
            db_pool,
            event_sender,
        })
    }

    /// Submit a job to sensd for Atuin database monitoring
    pub async fn submit_atuin_job(&self, db_path: &str) -> Result<Ulid> {
        let job_id = Ulid::new();

        sqlx::query!(
            r#"
            INSERT INTO raw.sensor_jobs (
                job_id, sensor_type, target_uri, source_identifier,
                acquisition_mode, parameters, status, created_at
            )
            VALUES ($1::ulid, 'append_stream', $2, $3, $4, $5, 'pending', NOW())
            "#,
            job_id as Ulid,
            db_path,                              // target_uri
            format!("atuin-history:{}", db_path), // source_identifier
            json!({ "mode": "continuous" }),      // acquisition_mode
            json!({
                "format": "sqlite",
                "table": "history",
                "poll_interval_secs": 5,
                "batch_size": 100,
            }), // parameters
        )
        .execute(&self.db_pool)
        .await?;

        info!(
            "Submitted Atuin monitoring job {} for path: {}",
            job_id, db_path
        );
        Ok(job_id)
    }

    /// Submit a job to sensd for shell history file monitoring
    pub async fn submit_history_file_job(&self, file_path: &str) -> Result<Ulid> {
        let job_id = Ulid::new();

        sqlx::query!(
            r#"
            INSERT INTO raw.sensor_jobs (
                job_id, sensor_type, target_uri, source_identifier,
                acquisition_mode, parameters, status, created_at
            )
            VALUES ($1::ulid, 'append_stream', $2, $3, $4, $5, 'pending', NOW())
            "#,
            job_id as Ulid,
            file_path,                             // target_uri
            format!("history-file:{}", file_path), // source_identifier
            json!({ "mode": "continuous" }),       // acquisition_mode
            json!({
                "format": "text_lines",
                "poll_interval_secs": 3,
                "batch_size": 50,
            }), // parameters
        )
        .execute(&self.db_pool)
        .await?;

        info!(
            "Submitted history file monitoring job {} for: {}",
            job_id, file_path
        );
        Ok(job_id)
    }

    /// Submit a job to sensd for terminal recording monitoring
    pub async fn submit_recording_job(&self, recordings_dir: &str) -> Result<Ulid> {
        let job_id = Ulid::new();

        sqlx::query!(
            r#"
            INSERT INTO raw.sensor_jobs (
                job_id, sensor_type, target_uri, source_identifier,
                acquisition_mode, parameters, status, created_at
            )
            VALUES ($1::ulid, 'tree_watch', $2, $3, $4, $5, 'pending', NOW())
            "#,
            job_id as Ulid,
            recordings_dir,                           // target_uri
            format!("recordings:{}", recordings_dir), // source_identifier
            json!({ "mode": "continuous" }),          // acquisition_mode
            json!({
                "patterns": ["*.cast"],
                "recursive": false,
                "events": ["CREATE", "MODIFY", "DELETE"],
                "poll_interval_secs": 5,
            }), // parameters
        )
        .execute(&self.db_pool)
        .await?;

        info!(
            "Submitted recording monitoring job {} for: {}",
            job_id, recordings_dir
        );
        Ok(job_id)
    }

    /// Submit a job to sensd for Kitty socket monitoring
    pub async fn submit_kitty_job(&self, socket_path: &str) -> Result<Ulid> {
        let job_id = Ulid::new();

        sqlx::query!(
            r#"
            INSERT INTO raw.sensor_jobs (
                job_id, sensor_type, target_uri, source_identifier,
                acquisition_mode, parameters, status, created_at
            )
            VALUES ($1::ulid, 'append_stream', $2, $3, $4, $5, 'pending', NOW())
            "#,
            job_id as Ulid,
            socket_path,                             // target_uri
            format!("kitty-socket:{}", socket_path), // source_identifier
            json!({ "mode": "continuous" }),         // acquisition_mode
            json!({
                "format": "kitty_remote_control",
                "commands": ["ls", "get-text"],
                "poll_interval_secs": 1,
            }), // parameters
        )
        .execute(&self.db_pool)
        .await?;

        info!(
            "Submitted Kitty monitoring job {} for: {}",
            job_id, socket_path
        );
        Ok(job_id)
    }

    /// Process material slices from sensd
    pub async fn process_material(&self, material_id: Ulid) -> Result<()> {
        info!("Processing terminal material: {}", material_id);

        // Create stream for material slices
        let stream = self.create_material_stream(material_id).await?;
        pin_mut!(stream);

        let mut total_events = 0;

        while let Some(slice_result) = stream.next().await {
            match slice_result {
                Ok(slice) => {
                    // Convert slice to terminal events
                    let events = self.slice_to_events(slice).await?;

                    for event in events {
                        if let Err(e) = self.event_sender.send(event).await {
                            error!("Failed to send event: {}", e);
                        } else {
                            total_events += 1;
                        }
                    }
                }
                Err(e) => {
                    error!("Error processing slice: {}", e);
                }
            }
        }

        info!(
            "Completed processing material {}, generated {} events",
            material_id, total_events
        );

        Ok(())
    }

    /// Create a stream of material slices
    async fn create_material_stream(
        &self,
        material_id: Ulid,
    ) -> Result<impl tokio_stream::Stream<Item = Result<MaterialSlice>> + '_> {
        // Query temporal ledger for slices
        let stream = async_stream::stream! {
            let mut offset = 0i64;

            loop {
                let slices = sqlx::query!(
                    r#"
                    SELECT 
                        tl.material_id as "material_id: Ulid",
                        tl.offset_start,
                        tl.offset_end,
                        tl.ts_capture,
                        tl.note,
                        sm.optional_blob_id as "optional_blob_id?: Ulid",
                        sm.data as "inline_data?: Vec<u8>"
                    FROM raw.temporal_ledger tl
                    LEFT JOIN raw.source_material_registry sm 
                        ON sm.source_material_id::uuid = tl.material_id::uuid
                    WHERE tl.material_id = $1::ulid
                    AND tl.offset_start >= $2
                    ORDER BY tl.offset_start
                    LIMIT $3
                    "#,
                    material_id as Ulid,
                    offset,
                    self.config.batch_size as i64,
                )
                .fetch_all(&self.db_pool)
                .await;

                match slices {
                    Ok(records) => {
                        if records.is_empty() {
                            break;
                        }

                        for record in records {
                            // Load data from storage
                            let data = if let Some(inline_data) = record.inline_data {
                                inline_data
                            } else if let Some(blob_id) = record.optional_blob_id {
                                match self.load_blob_data(blob_id).await {
                                    Ok(blob_data) => {
                                        let start = record.offset_start as usize;
                                        let end = record.offset_end as usize;
                                        if end <= blob_data.len() {
                                            blob_data[start..end].to_vec()
                                        } else {
                                            error!(
                                                "Blob data size {} is smaller than slice end offset {}",
                                                blob_data.len(), end
                                            );
                                            vec![]
                                        }
                                    }
                                    Err(e) => {
                                        error!("Failed to load blob {}: {}", blob_id, e);
                                        vec![]
                                    }
                                }
                            } else {
                                debug!("No data source found for material {}", material_id);
                                vec![]
                            };

                            let slice = MaterialSlice {
                                material_id: record.material_id,
                                offset_start: record.offset_start,
                                offset_end: record.offset_end,
                                ts_capture_start: record.ts_capture,
                                ts_capture_end: record.ts_capture,
                                data,
                                metadata: serde_json::from_str(&record.note.unwrap_or("{}".to_string())).unwrap_or_default(),
                            };

                            offset = record.offset_end;
                            yield Ok(slice);
                        }
                    }
                    Err(e) => {
                        yield Err(eyre!("Failed to fetch slices: {}", e));
                        break;
                    }
                }
            }
        };

        Ok(stream)
    }

    /// Load blob data from storage
    async fn load_blob_data(&self, blob_id: Ulid) -> Result<Vec<u8>> {
        let blob = sqlx::query!(
            r#"
            SELECT 
                annex_backend,
                size_bytes,
                checksum_sha256,
                storage_backend
            FROM core.blobs
            WHERE id = $1::uuid
            "#,
            ulid_to_uuid(blob_id),
        )
        .fetch_optional(&self.db_pool)
        .await?
        .ok_or_else(|| eyre!("Blob {} not found", blob_id))?;

        match blob.storage_backend.as_str() {
            "git-annex" => {
                let annex_path = std::path::Path::new(".git/annex/objects")
                    .join(&blob.annex_backend[0..2])
                    .join(&blob.annex_backend[2..4])
                    .join(&blob.annex_backend);

                if annex_path.exists() {
                    tokio::fs::read(&annex_path)
                        .await
                        .map_err(|e| eyre!("Failed to read annex file: {}", e))
                } else {
                    Err(eyre!("Annex file not found at {:?}", annex_path))
                }
            }
            "filesystem" => {
                let path = std::path::Path::new(&blob.annex_backend);
                tokio::fs::read(path)
                    .await
                    .map_err(|e| eyre!("Failed to read file: {}", e))
            }
            backend => Err(eyre!("Unknown storage backend: {}", backend)),
        }
    }

    /// Convert a material slice to terminal events
    async fn slice_to_events(&self, slice: MaterialSlice) -> Result<Vec<RawEvent>> {
        let mut events = Vec::new();

        // Determine source type from metadata
        let source_identifier = slice
            .metadata
            .get("source_identifier")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");

        if source_identifier.starts_with("atuin-history:") {
            events.extend(self.process_atuin_slice(&slice).await?);
        } else if source_identifier.starts_with("history-file:") {
            events.extend(self.process_history_file_slice(&slice).await?);
        } else if source_identifier.starts_with("recordings:") {
            events.extend(self.process_recording_slice(&slice).await?);
        } else if source_identifier.starts_with("kitty-socket:") {
            events.extend(self.process_kitty_slice(&slice).await?);
        } else {
            debug!("Unknown source type: {}", source_identifier);
        }

        Ok(events)
    }

    /// Process Atuin database material slice
    async fn process_atuin_slice(&self, slice: &MaterialSlice) -> Result<Vec<RawEvent>> {
        let mut events = Vec::new();

        // Parse SQLite data from slice (simplified - real implementation would parse SQLite binary format)
        let data_str = String::from_utf8_lossy(&slice.data);

        // For now, assume we get structured data in JSON format from sensd
        if let Ok(entries) = serde_json::from_str::<serde_json::Value>(&data_str) {
            if let Some(entries_array) = entries.as_array() {
                for entry in entries_array {
                    let mut event = RawEvent::from_material(
                        EventSource::from("terminal"),
                        EventType::from("terminal.atuin_command_executed"),
                        json!({
                            "command": entry.get("command").unwrap_or(&json!("")),
                            "cwd": entry.get("cwd").unwrap_or(&json!("")),
                            "exit_code": entry.get("exit_code").unwrap_or(&json!(0)),
                            "duration_ns": entry.get("duration_ns").unwrap_or(&json!(0)),
                            "hostname": entry.get("hostname").unwrap_or(&json!("")),
                            "timestamp_ns": entry.get("timestamp_ns").unwrap_or(&json!(0)),
                        }),
                        slice.material_id,
                        slice.offset_start,
                    );
                    event.ts_orig = Some(slice.ts_capture_start);
                    // Update provenance with full offset information
                    event.provenance = Provenance::Material {
                        id: Id::from(slice.material_id),
                        anchor_byte: slice.offset_start,
                        offset_kind: OffsetKind::Byte,
                        offset_start: Some(slice.offset_start),
                        offset_end: Some(slice.offset_end),
                    };

                    events.push(event);
                }
            }
        }

        Ok(events)
    }

    /// Process shell history file material slice
    async fn process_history_file_slice(&self, slice: &MaterialSlice) -> Result<Vec<RawEvent>> {
        let mut events = Vec::new();

        let data_str = String::from_utf8_lossy(&slice.data);
        let file_path = slice
            .metadata
            .get("target_uri")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");

        // Parse shell commands from the slice data
        for (line_num, line) in data_str.lines().enumerate() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }

            let event_type = if file_path.contains("fish") {
                "terminal.fish_historical_command"
            } else if file_path.contains("zsh") {
                "terminal.zsh_historical_command"
            } else {
                "terminal.bash_historical_command"
            };

            let mut event = RawEvent::from_material(
                EventSource::from("terminal"),
                EventType::from(event_type),
                json!({
                    "command_string": line,
                    "source_file": file_path,
                    "line_number": line_num,
                }),
                slice.material_id,
                slice.offset_start + line.as_ptr() as i64 - slice.data.as_ptr() as i64,
            );
            event.ts_orig = Some(slice.ts_capture_start);
            // Update provenance with full offset information
            event.provenance = Provenance::Material {
                id: Id::from(slice.material_id),
                anchor_byte: slice.offset_start + line.as_ptr() as i64 - slice.data.as_ptr() as i64,
                offset_kind: OffsetKind::Byte,
                offset_start: Some(slice.offset_start),
                offset_end: Some(slice.offset_end),
            };

            events.push(event);
        }

        Ok(events)
    }

    /// Process recording material slice
    async fn process_recording_slice(&self, slice: &MaterialSlice) -> Result<Vec<RawEvent>> {
        let mut events = Vec::new();

        let event_type = slice
            .metadata
            .get("event_type")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");

        let file_path = slice
            .metadata
            .get("file_path")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");

        match event_type {
            "CREATE" => {
                let mut event = RawEvent::from_material(
                    EventSource::from("terminal"),
                    EventType::from("terminal.recording_started"),
                    json!({
                        "recording_file": file_path,
                        "session_id": ulid::Ulid::new().to_string(),
                        "terminal_type": "asciinema",
                    }),
                    slice.material_id,
                    slice.offset_start,
                );
                event.ts_orig = Some(slice.ts_capture_start);
                // Update provenance with full offset information
                event.provenance = Provenance::Material {
                    id: Id::from(slice.material_id),
                    anchor_byte: slice.offset_start,
                    offset_kind: OffsetKind::Byte,
                    offset_start: Some(slice.offset_start),
                    offset_end: Some(slice.offset_end),
                };

                events.push(event);
            }
            "MODIFY" => {
                // Recording file was updated - could indicate session progress
            }
            "DELETE" => {
                let mut event = RawEvent::from_material(
                    EventSource::from("terminal"),
                    EventType::from("terminal.recording_ended"),
                    json!({
                        "recording_file": file_path,
                        "terminal_type": "asciinema",
                    }),
                    slice.material_id,
                    slice.offset_start,
                );
                event.ts_orig = Some(slice.ts_capture_start);
                // Update provenance with full offset information
                event.provenance = Provenance::Material {
                    id: Id::from(slice.material_id),
                    anchor_byte: slice.offset_start,
                    offset_kind: OffsetKind::Byte,
                    offset_start: Some(slice.offset_start),
                    offset_end: Some(slice.offset_end),
                };

                events.push(event);
            }
            _ => {}
        }

        Ok(events)
    }

    /// Process Kitty socket material slice
    async fn process_kitty_slice(&self, slice: &MaterialSlice) -> Result<Vec<RawEvent>> {
        let mut events = Vec::new();

        let data_str = String::from_utf8_lossy(&slice.data);

        // Parse Kitty remote control response data
        if let Ok(kitty_data) = serde_json::from_str::<serde_json::Value>(&data_str) {
            // Process based on command type stored in metadata
            let command_type = slice
                .metadata
                .get("command")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown");

            match command_type {
                "ls" => {
                    // Process window/tab listings
                    if let Some(windows) = kitty_data.as_array() {
                        for window in windows {
                            let mut event = RawEvent::from_material(
                                EventSource::from("terminal"),
                                EventType::from("terminal.kitty_window_state"),
                                window.clone(),
                                slice.material_id,
                                slice.offset_start,
                            );
                            event.ts_orig = Some(slice.ts_capture_start);
                            // Update provenance with full offset information
                            event.provenance = Provenance::Material {
                                id: Id::from(slice.material_id),
                                anchor_byte: slice.offset_start,
                                offset_kind: OffsetKind::Byte,
                                offset_start: Some(slice.offset_start),
                                offset_end: Some(slice.offset_end),
                            };

                            events.push(event);
                        }
                    }
                }
                "get-text" => {
                    // Process scrollback content
                    let mut event = RawEvent::from_material(
                        EventSource::from("terminal"),
                        EventType::from("terminal.kitty_content_captured"),
                        json!({
                            "scrollback_content": kitty_data.get("text").unwrap_or(&json!("")),
                            "window_id": slice.metadata.get("window_id").unwrap_or(&json!("")),
                        }),
                        slice.material_id,
                        slice.offset_start,
                    );
                    event.ts_orig = Some(slice.ts_capture_start);
                    // Update provenance with full offset information
                    event.provenance = Provenance::Material {
                        id: Id::from(slice.material_id),
                        anchor_byte: slice.offset_start,
                        offset_kind: OffsetKind::Byte,
                        offset_start: Some(slice.offset_start),
                        offset_end: Some(slice.offset_end),
                    };

                    events.push(event);
                }
                _ => {}
            }
        }

        Ok(events)
    }

    /// Monitor active jobs and process their materials
    pub async fn monitor_jobs(&self) -> Result<()> {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(
            self.config.processing_interval_ms,
        ));

        loop {
            interval.tick().await;

            // Query for completed terminal jobs that haven't been processed
            let completed_jobs = sqlx::query!(
                r#"
                SELECT 
                    job_id as "job_id: Ulid",
                    material_id as "material_id: Ulid"
                FROM raw.sensor_jobs
                WHERE status = 'completed'
                AND material_id IS NOT NULL
                AND source_identifier ~ '^(atuin-history:|history-file:|recordings:|kitty-socket:)'
                AND NOT EXISTS (
                    SELECT 1 FROM core.events 
                    WHERE source_material_id = sensor_jobs.material_id
                )
                ORDER BY completed_at DESC
                LIMIT 10
                "#,
            )
            .fetch_all(&self.db_pool)
            .await?;

            for job in completed_jobs {
                if let Some(material_id) = job.material_id {
                    info!(
                        "Processing terminal material {} from job {}",
                        material_id, job.job_id
                    );

                    if let Err(e) = self.process_material(material_id).await {
                        error!("Failed to process material {}: {}", material_id, e);
                    }
                }
            }
        }
    }
}

/// Run terminal processor with sensd integration
pub async fn run_terminal_with_sensd(config: SensdIntegrationConfig) -> Result<()> {
    info!("Starting terminal processor with sensd integration");

    // Create event channel
    let (event_sender, mut event_receiver) = mpsc::channel(1000);

    // Create processor
    let processor = Arc::new(SensdTerminalProcessor::new(config, event_sender).await?);

    // Submit initial monitoring jobs for common terminal sources
    let home_dir = dirs::home_dir().unwrap_or_else(|| std::path::PathBuf::from("/tmp"));

    // Atuin database
    let atuin_path = home_dir.join(".local/share/atuin/history.db");
    if atuin_path.exists() {
        processor
            .submit_atuin_job(&atuin_path.to_string_lossy())
            .await?;
    }

    // Shell history files
    let history_files = vec![
        home_dir.join(".bash_history"),
        home_dir.join(".zsh_history"),
        home_dir.join(".local/share/fish/fish_history"),
    ];

    for file in &history_files {
        if file.exists() {
            processor
                .submit_history_file_job(&file.to_string_lossy())
                .await?;
        }
    }

    // Recording directory
    let recordings_dir = home_dir.join(".local/share/sinex/recordings");
    processor
        .submit_recording_job(&recordings_dir.to_string_lossy())
        .await?;

    // Start job monitoring task
    let monitor_processor = processor.clone();
    let monitor_task = tokio::spawn(async move {
        if let Err(e) = monitor_processor.monitor_jobs().await {
            error!("Job monitoring error: {}", e);
        }
    });

    // Process events (would send to ingestd in real implementation)
    let event_task = tokio::spawn(async move {
        while let Some(event) = event_receiver.recv().await {
            debug!("Received terminal event: {:?}", event.event_type);
            // Here we would send to ingestd via gRPC
        }
    });

    // Wait for tasks
    tokio::try_join!(monitor_task, event_task)?;

    Ok(())
}

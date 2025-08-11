//! Integration with sensd for filesystem data acquisition
//!
//! This module refactors fs-watcher to use sensd's MaterialSliceStream
//! instead of directly monitoring filesystems.

use chrono::{DateTime, Utc};
use color_eyre::eyre::{eyre, Result};
use futures::pin_mut;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sinex_core::{
    db::models::{Provenance, RawEvent, SourceMaterial},
    types::{
        domain::{EventSource, EventType},
        Id, Ulid,
    },
};
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

/// Filesystem processor that uses sensd for data acquisition
pub struct SensdFilesystemProcessor {
    config: SensdIntegrationConfig,
    db_pool: PgPool,
    event_sender: mpsc::Sender<RawEvent>,
}

impl SensdFilesystemProcessor {
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

    /// Submit a job to sensd for filesystem monitoring
    pub async fn submit_monitoring_job(&self, path: &str) -> Result<Ulid> {
        let job_id = Ulid::new();

        // Insert job into sensor_jobs table
        sqlx::query!(
            r#"
            INSERT INTO raw.sensor_jobs (
                job_id, sensor_type, target_path, 
                config, status, created_at
            )
            VALUES ($1::ulid, 'tree_watch', $2, $3, 'pending', NOW())
            "#,
            job_id as Ulid,
            path,
            json!({
                "recursive": true,
                "follow_symlinks": false,
                "max_depth": 10,
            }),
        )
        .execute(&self.db_pool)
        .await?;

        info!("Submitted tree_watch job {} for path: {}", job_id, path);

        Ok(job_id)
    }

    /// Process material slices from sensd
    pub async fn process_material(&self, material_id: Ulid) -> Result<()> {
        info!("Processing material: {}", material_id);

        // Create stream for material slices
        let stream = self.create_material_stream(material_id).await?;
        pin_mut!(stream);

        let mut total_events = 0;

        while let Some(slice_result) = stream.next().await {
            match slice_result {
                Ok(slice) => {
                    // Convert slice to filesystem events
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
                        tl.ts_capture_start,
                        tl.ts_capture_end,
                        tl.capture_metadata,
                        sm.optional_blob_id as "blob_id?: Ulid",
                        sm.data as "inline_data?: Vec<u8>"
                    FROM raw.temporal_ledger tl
                    LEFT JOIN raw.source_material_registry sm 
                        ON sm.source_material_id = tl.material_id
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
                                // Data is stored inline in the source_material_registry
                                inline_data
                            } else if let Some(blob_id) = record.blob_id {
                                // Load from blob storage
                                match self.load_blob_data(blob_id).await {
                                    Ok(blob_data) => {
                                        // Extract slice from blob based on offsets
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
                                ts_capture_start: record.ts_capture_start,
                                ts_capture_end: record.ts_capture_end,
                                data,
                                metadata: record.capture_metadata,
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
        // First query the blob metadata
        let blob = sqlx::query!(
            r#"
            SELECT 
                annex_key,
                size_bytes,
                checksum_sha256,
                storage_backend
            FROM core.blobs
            WHERE id = $1::ulid
            "#,
            blob_id as Ulid,
        )
        .fetch_optional(&self.db_pool)
        .await?
        .ok_or_else(|| eyre!("Blob {} not found", blob_id))?;

        match blob.storage_backend.as_str() {
            "git-annex" => {
                // Load from git-annex storage
                let annex_path = std::path::Path::new(".git/annex/objects")
                    .join(&blob.annex_key[0..2])
                    .join(&blob.annex_key[2..4])
                    .join(&blob.annex_key);
                
                if annex_path.exists() {
                    tokio::fs::read(&annex_path).await
                        .map_err(|e| eyre!("Failed to read annex file: {}", e))
                } else {
                    Err(eyre!("Annex file not found at {:?}", annex_path))
                }
            }
            "filesystem" => {
                // Load from filesystem path stored in annex_key
                let path = std::path::Path::new(&blob.annex_key);
                tokio::fs::read(path).await
                    .map_err(|e| eyre!("Failed to read file: {}", e))
            }
            "s3" => {
                // S3 support would go here
                Err(eyre!("S3 storage backend not yet implemented"))
            }
            backend => {
                Err(eyre!("Unknown storage backend: {}", backend))
            }
        }
    }

    /// Convert a material slice to filesystem events
    async fn slice_to_events(&self, slice: MaterialSlice) -> Result<Vec<RawEvent>> {
        let mut events = Vec::new();

        // Extract metadata from slice
        let path = slice
            .metadata
            .get("path")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");

        let event_kind = slice
            .metadata
            .get("event_kind")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");

        let file_size = slice
            .metadata
            .get("size")
            .and_then(|v| v.as_i64())
            .unwrap_or(0);

        // Determine event type from metadata
        let event_type = match event_kind {
            kind if kind.contains("Create") => "filesystem.created",
            kind if kind.contains("Modify") => "filesystem.modified",
            kind if kind.contains("Remove") => "filesystem.deleted",
            kind if kind.contains("Rename") => "filesystem.renamed",
            _ => "filesystem.unknown",
        };

        // Create raw event with provenance
        let raw_event = RawEvent::builder()
            .event_type(EventType::from(event_type))
            .source(EventSource::from("filesystem"))
            .payload(json!({
                "path": path,
                "size": file_size,
                "event_kind": event_kind,
                "material_id": slice.material_id.to_string(),
                "offset_start": slice.offset_start,
                "offset_end": slice.offset_end,
            }))
            .ts_orig(Some(slice.ts_capture_start))
            .provenance(Provenance::Material {
                id: Id::from(slice.material_id),
                offset_start: Some(slice.offset_start),
                offset_end: Some(slice.offset_end),
            })
            .anchor_byte(slice.offset_start)
            .build();

        events.push(raw_event);

        Ok(events)
    }

    /// Monitor active jobs and process their materials
    pub async fn monitor_jobs(&self) -> Result<()> {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(
            self.config.processing_interval_ms,
        ));

        loop {
            interval.tick().await;

            // Query for completed jobs that haven't been processed
            let completed_jobs = sqlx::query!(
                r#"
                SELECT 
                    job_id as "job_id: Ulid",
                    material_id as "material_id: Ulid"
                FROM raw.sensor_jobs
                WHERE status = 'completed'
                AND material_id IS NOT NULL
                AND NOT EXISTS (
                    SELECT 1 FROM core.events 
                    WHERE material_id = sensor_jobs.material_id
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
                        "Processing material {} from job {}",
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

/// Run filesystem processor with sensd integration
pub async fn run_with_sensd(config: SensdIntegrationConfig) -> Result<()> {
    info!("Starting filesystem processor with sensd integration");

    // Create event channel
    let (event_sender, mut event_receiver) = mpsc::channel(1000);

    // Create processor
    let processor = Arc::new(SensdFilesystemProcessor::new(config, event_sender).await?);

    // Submit initial monitoring jobs for configured paths
    let watch_paths = vec![
        "/home/user/documents",
        "/home/user/downloads",
        "/tmp/sinex-test",
    ];

    for path in &watch_paths {
        processor.submit_monitoring_job(path).await?;
    }

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
            debug!("Received event: {:?}", event.event_type);
            // Here we would send to ingestd via gRPC
        }
    });

    // Wait for tasks
    tokio::try_join!(monitor_task, event_task)?;

    Ok(())
}

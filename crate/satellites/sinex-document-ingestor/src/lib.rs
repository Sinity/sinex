//! Document Ingestor that consumes MaterialSliceStream from sensd
//!
//! This ingestor demonstrates the proper pattern for consuming source materials
//! from sensd and creating events with proper provenance traceability.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use color_eyre::eyre::{eyre, Result};
use futures::pin_mut;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sinex_core::{
    types::{events::DocumentIngestedPayload, Event, Ulid},
    RawEvent,
};
use sinex_satellite_sdk::{
    cli::{
        CoverageAnalysis, ExplorationProvider, ExportFormat, IngestionHistoryEntry, SourceState,
    },
    stream_processor::{
        Checkpoint, ProcessorCapabilities, ProcessorType, ScanArgs, ScanReport,
        StatefulStreamProcessor, StreamProcessorContext, TimeHorizon,
    },
    SatelliteError, SatelliteResult,
};
use sinex_schema::ulid_conversions::ulid_to_uuid;
use sqlx::PgPool;
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tracing::{debug, error, info, warn};

/// Configuration for Document Ingestor
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DocumentIngestorConfig {
    /// Database URL for connecting to sensd tables
    pub database_url: String,
    /// Batch size for processing material slices
    pub batch_size: usize,
    /// Processing interval in milliseconds
    pub processing_interval_ms: u64,
    /// Supported document MIME types
    pub supported_mime_types: Vec<String>,
    /// Maximum document size in bytes to process
    pub max_document_size: u64,
}

impl Default for DocumentIngestorConfig {
    fn default() -> Self {
        Self {
            database_url: String::from("postgresql:///sinex_dev?host=/run/postgresql"),
            batch_size: 50,
            processing_interval_ms: 2000,
            supported_mime_types: vec![
                "text/plain".to_string(),
                "text/markdown".to_string(),
                "application/pdf".to_string(),
                "application/json".to_string(),
                "text/html".to_string(),
                "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
                    .to_string(),
            ],
            max_document_size: 100 * 1024 * 1024, // 100MB
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

/// Document processor that consumes MaterialSliceStream from sensd
pub struct DocumentProcessor {
    context: Option<StreamProcessorContext>,
    config: DocumentIngestorConfig,
    db_pool: Option<PgPool>,
    event_sender: Option<mpsc::Sender<RawEvent>>,
}

impl DocumentProcessor {
    pub fn new() -> Self {
        Self {
            context: None,
            config: DocumentIngestorConfig::default(),
            db_pool: None,
            event_sender: None,
        }
    }

    /// Submit a job to sensd for document processing
    pub async fn submit_document_job(&self, file_path: &str) -> Result<Ulid> {
        let db_pool = self
            .db_pool
            .as_ref()
            .ok_or_else(|| eyre!("Database pool not initialized"))?;

        let job_id = Ulid::new();

        // Insert job into sensor_jobs table for sensd to process
        sqlx::query!(
            r#"
            INSERT INTO raw.sensor_jobs (
                job_id, sensor_type, target_uri, source_identifier,
                acquisition_mode, parameters, status, created_at
            )
            VALUES ($1::ulid, 'document_capture', $2, $3, $4, $5, 'pending', NOW())
            "#,
            job_id as Ulid,
            format!("file://{}", file_path),   // target_uri
            format!("document:{}", file_path), // source_identifier
            json!({ "mode": "snapshot" }),     // acquisition_mode
            json!({
                "document_type": "file",
                "max_size": self.config.max_document_size,
                "supported_types": self.config.supported_mime_types,
            }), // parameters
        )
        .execute(db_pool)
        .await?;

        info!(
            "Submitted document capture job {} for: {}",
            job_id, file_path
        );

        Ok(job_id)
    }

    /// Process material slices from sensd for a given material
    pub async fn process_material(&self, material_id: Ulid) -> Result<u64> {
        let db_pool = self
            .db_pool
            .as_ref()
            .ok_or_else(|| eyre!("Database pool not initialized"))?;

        let event_sender = self
            .event_sender
            .as_ref()
            .ok_or_else(|| eyre!("Event sender not initialized"))?;

        info!("Processing material: {}", material_id);

        // Create stream for material slices
        let stream = self.create_material_stream(material_id, db_pool).await?;
        pin_mut!(stream);

        let mut events_generated = 0u64;
        let mut document_data = Vec::new();
        let mut document_metadata: Option<serde_json::Value> = None;

        // Collect all slices to reconstruct the complete document
        while let Some(slice_result) = stream.next().await {
            match slice_result {
                Ok(slice) => {
                    // Append slice data to reconstruct full document
                    document_data.extend_from_slice(&slice.data);

                    // Capture metadata from first slice
                    if document_metadata.is_none() {
                        document_metadata = Some(slice.metadata.clone());
                    }

                    debug!(
                        "Processed slice: material_id={}, offset={}..{}, data_len={}",
                        slice.material_id,
                        slice.offset_start,
                        slice.offset_end,
                        slice.data.len()
                    );
                }
                Err(e) => {
                    error!("Error processing slice: {}", e);
                    return Err(e);
                }
            }
        }

        // Once we have the complete document, process it
        if !document_data.is_empty() {
            let events = self
                .process_complete_document(
                    material_id,
                    document_data,
                    document_metadata.unwrap_or_default(),
                )
                .await?;

            // Send events
            for event in events {
                if let Err(e) = event_sender.send(event).await {
                    error!("Failed to send event: {}", e);
                } else {
                    events_generated += 1;
                }
            }
        }

        info!(
            "Completed processing material {}, generated {} events",
            material_id, events_generated
        );

        Ok(events_generated)
    }

    /// Create a stream of material slices from sensd temporal ledger
    async fn create_material_stream(
        &self,
        material_id: Ulid,
        db_pool: &PgPool,
    ) -> Result<impl tokio_stream::Stream<Item = Result<MaterialSlice>> + '_> {
        let batch_size = self.config.batch_size;

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
                        ON sm.source_material_id = tl.material_id
                    WHERE tl.material_id = $1::ulid
                    AND tl.offset_start >= $2
                    ORDER BY tl.offset_start
                    LIMIT $3
                    "#,
                    material_id as Ulid,
                    offset,
                    batch_size as i64,
                )
                .fetch_all(db_pool)
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
                                let start = record.offset_start as usize;
                                let end = record.offset_end as usize;
                                if start < inline_data.len() && end <= inline_data.len() && start <= end {
                                    inline_data[start..end].to_vec()
                                } else {
                                    inline_data // Return full data if slice bounds are invalid
                                }
                            } else if let Some(blob_id) = record.optional_blob_id {
                                // Load from blob storage
                                match self.load_blob_data(blob_id, db_pool).await {
                                    Ok(blob_data) => {
                                        // Extract slice from blob based on offsets
                                        let start = record.offset_start as usize;
                                        let end = record.offset_end as usize;
                                        if end <= blob_data.len() && start <= end {
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
                                ts_capture_end: record.ts_capture, // Same timestamp
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

    /// Load blob data from storage backend
    async fn load_blob_data(&self, blob_id: Ulid, db_pool: &PgPool) -> Result<Vec<u8>> {
        // First query the blob metadata
        let blob = sqlx::query!(
            r#"
            SELECT 
                annex_key,
                size_bytes,
                checksum_sha256,
                storage_backend
            FROM core.blobs
            WHERE id = $1::uuid
            "#,
            ulid_to_uuid(blob_id),
        )
        .fetch_optional(db_pool)
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
                    tokio::fs::read(&annex_path)
                        .await
                        .map_err(|e| eyre!("Failed to read annex file: {}", e))
                } else {
                    Err(eyre!("Annex file not found at {:?}", annex_path))
                }
            }
            "filesystem" => {
                // Load from filesystem path stored in annex_key
                let path = std::path::Path::new(&blob.annex_key);
                tokio::fs::read(path)
                    .await
                    .map_err(|e| eyre!("Failed to read file: {}", e))
            }
            "s3" => {
                // S3 support would go here
                Err(eyre!("S3 storage backend not yet implemented"))
            }
            backend => Err(eyre!("Unknown storage backend: {}", backend)),
        }
    }

    /// Process a complete document and generate events
    async fn process_complete_document(
        &self,
        material_id: Ulid,
        document_data: Vec<u8>,
        metadata: serde_json::Value,
    ) -> Result<Vec<RawEvent>> {
        let mut events = Vec::new();

        // Extract document information from metadata
        let file_path = metadata
            .get("path")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");

        let mime_type = metadata
            .get("mime_type")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        // Detect encoding for text documents
        let encoding = if let Some(ref mime) = mime_type {
            if mime.starts_with("text/") {
                match std::str::from_utf8(&document_data) {
                    Ok(_) => Some("utf-8".to_string()),
                    Err(_) => Some("binary".to_string()),
                }
            } else {
                None
            }
        } else {
            None
        };

        // Check if this document type is supported
        if let Some(ref mime) = mime_type {
            if !self.config.supported_mime_types.contains(mime) {
                warn!(
                    "Unsupported document type: {} for file: {}",
                    mime, file_path
                );
                // We still process it but with a warning
            }
        }

        // Check document size limit
        if document_data.len() as u64 > self.config.max_document_size {
            warn!(
                "Document exceeds size limit: {} bytes > {} bytes for file: {}",
                document_data.len(),
                self.config.max_document_size,
                file_path
            );
            return Ok(events); // Skip processing oversized documents
        }

        // Create document.ingested event with proper provenance
        let event: RawEvent = Event::new(DocumentIngestedPayload {
            file_path: file_path.to_string(),
            source_material_id: material_id.to_string(),
            size_bytes: document_data.len() as u64,
            mime_type,
            encoding,
        })
        .into();

        events.push(event);

        info!(
            "Generated document.ingested event for: {} ({} bytes, material_id: {})",
            file_path,
            document_data.len(),
            material_id
        );

        Ok(events)
    }

    /// Monitor active jobs and process their materials
    pub async fn monitor_jobs(&self) -> Result<()> {
        let db_pool = self
            .db_pool
            .as_ref()
            .ok_or_else(|| eyre!("Database pool not initialized"))?;

        let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(
            self.config.processing_interval_ms,
        ));

        loop {
            interval.tick().await;

            // Query for completed document capture jobs that haven't been processed
            let completed_jobs = sqlx::query!(
                r#"
                SELECT 
                    job_id as "job_id: Ulid",
                    material_id as "material_id: Ulid"
                FROM raw.sensor_jobs
                WHERE sensor_type = 'document_capture'
                AND status = 'completed'
                AND material_id IS NOT NULL
                AND NOT EXISTS (
                    SELECT 1 FROM core.events e
                    WHERE e.payload->>'source_material_id' = sensor_jobs.material_id::text
                    AND e.event_type = 'document.ingested'
                )
                ORDER BY completed_at DESC
                LIMIT 10
                "#,
            )
            .fetch_all(db_pool)
            .await?;

            for job in completed_jobs {
                if let Some(material_id) = job.material_id {
                    info!(
                        "Processing material {} from document capture job {}",
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

#[async_trait]
impl StatefulStreamProcessor for DocumentProcessor {
    type Config = DocumentIngestorConfig;

    async fn initialize(
        &mut self,
        ctx: StreamProcessorContext,
        config: Self::Config,
    ) -> SatelliteResult<()> {
        info!("Initializing document ingestor that consumes MaterialSliceStream from sensd");

        // Connect to database for sensd integration
        let db_pool = PgPool::connect(&config.database_url)
            .await
            .map_err(|e| SatelliteError::General(eyre!("Failed to connect to database: {}", e)))?;

        // Create event channel
        let (event_sender, mut event_receiver) = mpsc::channel(1000);

        // Store configuration and resources
        self.config = config;
        self.db_pool = Some(db_pool);
        self.event_sender = Some(event_sender);
        self.context = Some(ctx.clone());

        // Start background task to forward events to ingestd
        let ingest_client = ctx.ingest_client.clone();
        tokio::spawn(async move {
            while let Some(event) = event_receiver.recv().await {
                if let Err(e) = ingest_client.emit_event(event).await {
                    error!("Failed to emit event to ingestd: {}", e);
                }
            }
        });

        info!("Document ingestor initialized with sensd integration");
        Ok(())
    }

    async fn scan(
        &mut self,
        _from: Checkpoint,
        until: TimeHorizon,
        args: ScanArgs,
    ) -> SatelliteResult<ScanReport> {
        let start_time = Utc::now();
        let mut jobs_submitted = 0;

        match until {
            TimeHorizon::Snapshot => {
                // Submit jobs to sensd for document processing
                info!("Starting document snapshot scan - submitting jobs to sensd");

                for target in &args.targets {
                    // Submit job to sensd
                    match self.submit_document_job(target).await {
                        Ok(job_id) => {
                            info!("Submitted job {} for document: {}", job_id, target);
                            jobs_submitted += 1;
                        }
                        Err(e) => {
                            error!("Failed to submit job for {}: {}", target, e);
                        }
                    }
                }

                // Start monitoring jobs in background
                let self_clone = self.clone();
                tokio::spawn(async move {
                    if let Err(e) = self_clone.monitor_jobs().await {
                        error!("Job monitoring error: {}", e);
                    }
                });

                Ok(ScanReport {
                    events_processed: jobs_submitted as u64,
                    duration: Duration::from_millis(
                        (Utc::now() - start_time).num_milliseconds() as u64
                    ),
                    final_checkpoint: Checkpoint::timestamp(Utc::now(), None),
                    time_range: Some((start_time, Utc::now())),
                    processor_stats: HashMap::new(),
                    successful_targets: args.targets.clone(),
                    failed_targets: Vec::new(),
                    warnings: Vec::new(),
                })
            }
            TimeHorizon::Historical { .. } => {
                // Document ingestor doesn't support historical mode
                Ok(ScanReport {
                    events_processed: 0,
                    duration: Duration::from_millis(1),
                    final_checkpoint: Checkpoint::None,
                    time_range: None,
                    processor_stats: HashMap::new(),
                    successful_targets: Vec::new(),
                    failed_targets: Vec::new(),
                    warnings: vec!["Document ingestor does not support historical mode".to_string()],
                })
            }
            TimeHorizon::Continuous => {
                // Document ingestor doesn't support continuous mode
                // Documents should be monitored via filesystem watchers
                Ok(ScanReport {
                    events_processed: 0,
                    duration: Duration::from_millis(1),
                    final_checkpoint: Checkpoint::None,
                    time_range: None,
                    processor_stats: HashMap::new(),
                    successful_targets: Vec::new(),
                    failed_targets: Vec::new(),
                    warnings: vec![
                        "Document ingestor does not support continuous mode - use filesystem watchers"
                            .to_string(),
                    ],
                })
            }
        }
    }

    fn processor_name(&self) -> &str {
        "document-ingestor"
    }

    fn processor_type(&self) -> ProcessorType {
        ProcessorType::Ingestor
    }

    async fn current_checkpoint(&self) -> SatelliteResult<Checkpoint> {
        Ok(Checkpoint::None)
    }

    fn capabilities(&self) -> ProcessorCapabilities {
        ProcessorCapabilities {
            supports_continuous: false,
            supports_historical: false,
            supports_snapshot: true,
            supports_interactive: false,
            max_scan_size: Some(1000), // Limit for batch job submission
            supports_concurrent: false,
        }
    }
}

impl ExplorationProvider for DocumentProcessor {
    fn get_source_state(&self) -> color_eyre::eyre::Result<SourceState> {
        Ok(SourceState {
            description: "Document ingestor consuming MaterialSliceStream from sensd".to_string(),
            last_updated: Utc::now(),
            total_items: Some(0), // Could track processed documents
            metadata: HashMap::new(),
            healthy: true,
            recent_activity: Vec::new(),
        })
    }

    fn get_ingestion_history(
        &self,
        _limit: u64,
    ) -> color_eyre::eyre::Result<Vec<IngestionHistoryEntry>> {
        // Could query sensor_jobs table for history
        Ok(Vec::new())
    }

    fn get_coverage_analysis(
        &self,
        _time_range: Option<(chrono::DateTime<Utc>, chrono::DateTime<Utc>)>,
    ) -> color_eyre::eyre::Result<CoverageAnalysis> {
        Ok(CoverageAnalysis {
            coverage_percentage: 100.0,
            missing_count: 0,
            missing_samples: Vec::new(),
            duplicate_count: 0,
            sinex_total: 0,
            source_total: 0,
            time_range: (Utc::now() - chrono::Duration::days(30), Utc::now()),
            recommendations: Vec::new(),
        })
    }

    fn export_data(
        &self,
        _output_path: &sinex_core::SanitizedPath,
        _format: ExportFormat,
    ) -> color_eyre::eyre::Result<()> {
        Err(color_eyre::eyre::eyre!(
            "Document ingestor does not support data export"
        ))
    }
}

impl Clone for DocumentProcessor {
    fn clone(&self) -> Self {
        Self {
            context: self.context.clone(),
            config: self.config.clone(),
            db_pool: self.db_pool.clone(),
            event_sender: self.event_sender.clone(),
        }
    }
}

impl Default for DocumentProcessor {
    fn default() -> Self {
        Self::new()
    }
}

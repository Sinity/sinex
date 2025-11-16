#![doc = include_str!("../doc/README.md")]
#![doc = include_str!("../../../../docs/architecture/Core_Architecture.md")]
#![doc = include_str!("../../../lib/sinex-satellite-sdk/doc/overview.md")]

//! Document ingestor that ingests local files directly instead of relying on the
//! removed sensd pipeline.

use async_trait::async_trait;
use camino::{Utf8Path, Utf8PathBuf};
use chrono::{DateTime, Utc};
use color_eyre::eyre::{eyre, Result};
use mime_guess::MimeGuess;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sinex_core::{db::models::Event, types::ulid::Ulid, JsonValue};
use sinex_processor_runtime::{
    CoverageAnalysis, ExplorationProvider, ExportFormat, IngestionHistoryEntry, SourceState,
};
use sinex_satellite_sdk::{
    annex::{AnnexConfig, BlobManager},
    stream_processor::{
        Checkpoint, ProcessorCapabilities, ProcessorInitContext, ProcessorRuntimeState,
        ProcessorType, ScanArgs, ScanEstimate, ScanReport, StatefulStreamProcessor, TimeHorizon,
    },
    SatelliteError, SatelliteResult,
};
use std::{collections::HashMap, sync::Arc, time::Instant};
use tokio::{fs, io::AsyncReadExt, sync::mpsc};
use tracing::{error, info, warn};

const ENCODING_SNIFF_BYTES: usize = 4096;

/// Configuration for the document ingestor.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DocumentIngestorConfig {
    /// Supported document MIME types. When empty, all types are accepted.
    pub supported_mime_types: Vec<String>,
    /// Maximum document size (bytes) allowed for ingestion.
    pub max_document_size: u64,
}

impl Default for DocumentIngestorConfig {
    fn default() -> Self {
        Self {
            supported_mime_types: vec![
                "text/plain".to_string(),
                "text/markdown".to_string(),
                "application/pdf".to_string(),
                "application/json".to_string(),
                "text/html".to_string(),
                "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
                    .to_string(),
            ],
            max_document_size: 25 * 1024 * 1024, // 25 MB default for direct ingestion
        }
    }
}

/// Simplified document processor that ingests local files.
pub struct DocumentProcessor {
    runtime: Option<ProcessorRuntimeState>,
    config: DocumentIngestorConfig,
    event_sender: Option<mpsc::UnboundedSender<Event<JsonValue>>>,
    blob_manager: Option<Arc<BlobManager>>,
}

impl DocumentProcessor {
    pub fn new() -> Self {
        Self {
            runtime: None,
            config: DocumentIngestorConfig::default(),
            event_sender: None,
            blob_manager: None,
        }
    }

    async fn initialise_with_runtime_state(
        &mut self,
        runtime: ProcessorRuntimeState,
        config: DocumentIngestorConfig,
    ) -> SatelliteResult<()> {
        let annex_repo = match std::env::var("SINEX_ANNEX_PATH") {
            Ok(path) => Utf8PathBuf::from(path),
            Err(_) => {
                let default_path =
                    sinex_core::environment::environment().work_directory("/tmp/sinex/annex");
                Utf8PathBuf::from_path_buf(default_path)
                    .unwrap_or_else(|_| Utf8PathBuf::from("/tmp/sinex/annex"))
            }
        };

        fs::create_dir_all(annex_repo.as_std_path())
            .await
            .map_err(|e| {
                SatelliteError::General(eyre!("Failed to create annex directory: {}", e))
            })?;

        let (blob_event_tx, mut blob_event_rx) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            while let Some(event) = blob_event_rx.recv().await {
                tracing::debug!(?event, "Blob manager emitted event");
            }
        });

        let annex_config = AnnexConfig {
            repo_path: annex_repo,
            num_copies: None,
            large_files: None,
        };

        let blob_manager = Arc::new(
            BlobManager::new(annex_config, runtime.db_pool().clone(), Some(blob_event_tx))
                .map_err(|e| {
                    SatelliteError::General(eyre!("Failed to create blob manager: {}", e))
                })?,
        );

        let event_sender = runtime.event_sender();

        self.runtime = Some(runtime);
        self.config = config;
        self.event_sender = Some(event_sender);
        self.blob_manager = Some(blob_manager);

        Ok(())
    }

    async fn ingest_target(&self, target: &str) -> Result<Vec<Event<JsonValue>>> {
        let path_buf = std::path::PathBuf::from(target);
        let utf8_path = Utf8PathBuf::from_path_buf(path_buf.clone())
            .map_err(|_| eyre!("Document path must be valid UTF-8: {}", target))?;

        let metadata = fs::metadata(&utf8_path).await?;
        if !metadata.is_file() {
            return Err(eyre!("Document path is not a file: {}", target));
        }

        let file_size = metadata.len();
        if file_size > self.config.max_document_size {
            warn!(
                size = file_size,
                limit = self.config.max_document_size,
                path = %utf8_path,
                "Skipping document larger than configured limit"
            );
            return Ok(Vec::new());
        }

        let guess = MimeGuess::from_path(&utf8_path);
        let mime = guess
            .first_raw()
            .unwrap_or("application/octet-stream")
            .to_string();

        if !self.config.supported_mime_types.is_empty()
            && !self.config.supported_mime_types.iter().any(|m| m == &mime)
        {
            warn!(mime = %mime, path = %utf8_path, "Unsupported MIME type");
        }

        let mut metadata_json = json!({
            "path": utf8_path.as_str(),
            "mime_type": mime,
        });

        if let Some(manager) = &self.blob_manager {
            match manager.ingest_file(&utf8_path, utf8_path.file_name()).await {
                Ok(blob) => {
                    metadata_json["blob_id"] = json!(blob.id.to_string());
                    metadata_json["annex_key"] = json!(blob.annex_key());
                }
                Err(err) => {
                    warn!(error = %err, path = %utf8_path, "Failed to ingest document into annex");
                }
            }
        }

        let material_id = Ulid::new();
        let encoding = self
            .detect_encoding(&utf8_path, &mime)
            .await
            .unwrap_or_else(|err| {
                warn!(error = %err, path = %utf8_path, "Failed to detect encoding; defaulting to binary");
                Some("binary".to_string())
            });
        self.process_complete_document(material_id, file_size, encoding, metadata_json)
            .await
    }

    fn event_sender(&self) -> Result<&mpsc::UnboundedSender<Event<JsonValue>>> {
        self.event_sender
            .as_ref()
            .ok_or_else(|| eyre!("Event sender not initialized"))
    }

    async fn emit_events(&self, events: Vec<Event<JsonValue>>) -> Result<u64> {
        let sender = self.event_sender()?;
        let mut emitted = 0;
        for event in events {
            sender
                .send(event)
                .map_err(|e| eyre!("Failed to emit document event: {}", e))?;
            emitted += 1;
        }
        Ok(emitted)
    }

    async fn process_complete_document(
        &self,
        material_id: Ulid,
        size_bytes: u64,
        encoding: Option<String>,
        metadata: serde_json::Value,
    ) -> Result<Vec<Event<JsonValue>>> {
        let mut events = Vec::new();

        let file_path = metadata
            .get("path")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");

        let mime_type = metadata
            .get("mime_type")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        let event = Event::<JsonValue>::dynamic(
            sinex_core::types::domain::EventSource::from("document_ingestor"),
            sinex_core::types::domain::EventType::from("document.ingested"),
            serde_json::json!({
                "file_path": file_path,
                "source_material_id": material_id.to_string(),
                "size_bytes": size_bytes,
                "mime_type": mime_type,
                "encoding": encoding,
                "metadata": metadata,
            }),
        )
        .with_provenance(sinex_core::db::models::event::Provenance::from_material(
            sinex_core::types::Id::from(material_id),
            0,
            Some(0),
            Some(size_bytes as i64),
        ))
        .build();

        events.push(event);
        Ok(events)
    }

    async fn detect_encoding(
        &self,
        path: &Utf8Path,
        mime_type: &str,
    ) -> std::result::Result<Option<String>, std::io::Error> {
        if !mime_type.starts_with("text/") {
            return Ok(None);
        }

        let mut file = fs::File::open(path).await?;
        let file_size = file.metadata().await?.len() as usize;
        let mut buf = vec![0u8; ENCODING_SNIFF_BYTES.min(file_size)];
        let mut total = 0usize;

        while total < buf.len() {
            let read = file.read(&mut buf[total..]).await?;
            if read == 0 {
                break;
            }
            total += read;
        }
        buf.truncate(total);

        if buf.is_empty() || std::str::from_utf8(&buf).is_ok() {
            Ok(Some("utf-8".to_string()))
        } else {
            Ok(Some("binary".to_string()))
        }
    }
}

#[async_trait]
impl StatefulStreamProcessor for DocumentProcessor {
    type Config = DocumentIngestorConfig;

    async fn initialize(
        &mut self,
        init: ProcessorInitContext<Self::Config>,
    ) -> SatelliteResult<()> {
        let (config, runtime) = init.into_runtime();
        self.initialise_with_runtime_state(runtime, config).await
    }

    async fn scan(
        &mut self,
        _from: Checkpoint,
        until: TimeHorizon,
        args: ScanArgs,
    ) -> SatelliteResult<ScanReport> {
        let start = Instant::now();
        let mut events_processed = 0u64;
        let mut successful_targets = Vec::new();
        let mut failed_targets = Vec::new();

        match until {
            TimeHorizon::Snapshot | TimeHorizon::Historical { .. } => {
                if args.dry_run {
                    info!(targets = args.targets.len(), "Dry-run document ingestion");
                } else {
                    for target in &args.targets {
                        match self.ingest_target(target).await {
                            Ok(events) => {
                                if !events.is_empty() {
                                    events_processed += self.emit_events(events).await?;
                                }
                                successful_targets.push(target.clone());
                            }
                            Err(err) => {
                                error!(path = %target, error = %err, "Failed to ingest document");
                                failed_targets.push((target.clone(), err.to_string()));
                            }
                        }
                    }
                }
            }
            TimeHorizon::Continuous => {
                return Err(SatelliteError::Processing(
                    "Continuous document ingestion is no longer supported".into(),
                ));
            }
        }

        Ok(ScanReport {
            events_processed,
            duration: start.elapsed(),
            final_checkpoint: Checkpoint::timestamp(Utc::now(), None),
            time_range: None,
            processor_stats: HashMap::new(),
            successful_targets,
            failed_targets,
            warnings: Vec::new(),
        })
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
            supports_historical: true,
            supports_snapshot: true,
            supports_interactive: false,
            max_scan_size: Some(1024),
            supports_concurrent: false,
            manages_own_continuous_loop: false,
        }
    }

    async fn estimate_scan_scope(
        &self,
        _from: &Checkpoint,
        until: &TimeHorizon,
        args: &ScanArgs,
    ) -> SatelliteResult<ScanEstimate> {
        let estimated_events = args.targets.len() as u64;
        let (duration_factor, confidence) = match until {
            TimeHorizon::Snapshot => (1.0, 0.8),
            TimeHorizon::Historical { .. } => (1.0, 0.6),
            TimeHorizon::Continuous => (0.0, 0.0),
        };

        Ok(ScanEstimate {
            estimated_events: (estimated_events as f64 * duration_factor) as u64,
            estimated_duration: std::time::Duration::from_millis(estimated_events * 250),
            estimated_data_size: estimated_events * 64 * 1024,
            estimated_targets: args.targets.len() as u64,
            warnings: Vec::new(),
            confidence,
        })
    }
}

impl ExplorationProvider for DocumentProcessor {
    fn get_source_state(&self) -> color_eyre::eyre::Result<SourceState> {
        Ok(SourceState {
            description: "Direct document ingestion".to_string(),
            last_updated: Utc::now(),
            total_items: None,
            metadata: HashMap::new(),
            healthy: true,
            recent_activity: Vec::new(),
        })
    }

    fn get_ingestion_history(
        &self,
        _limit: u64,
    ) -> color_eyre::eyre::Result<Vec<IngestionHistoryEntry>> {
        Ok(Vec::new())
    }

    fn get_coverage_analysis(
        &self,
        _time_range: Option<(DateTime<Utc>, DateTime<Utc>)>,
    ) -> color_eyre::eyre::Result<CoverageAnalysis> {
        Ok(CoverageAnalysis {
            coverage_percentage: 100.0,
            missing_count: 0,
            missing_samples: Vec::new(),
            duplicate_count: 0,
            sinex_total: 0,
            source_total: 0,
            time_range: (Utc::now() - chrono::Duration::days(7), Utc::now()),
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
            runtime: self.runtime.clone(),
            config: self.config.clone(),
            event_sender: self.event_sender.clone(),
            blob_manager: self.blob_manager.clone(),
        }
    }
}

impl Default for DocumentProcessor {
    fn default() -> Self {
        Self::new()
    }
}

#![doc = include_str!("../docs/README.md")]
#![doc = include_str!("../../../../docs/current/architecture/Core_Architecture.md")]
#![doc = include_str!("../../../lib/sinex-node-sdk/docs/overview.md")]

//! Document ingestor that captures materials directly into JetStream via the
//! AcquisitionManager (Stage-as-You-Go).

use async_trait::async_trait;
use camino::{Utf8Path, Utf8PathBuf};
use chrono::{DateTime, Utc};
use mime_guess::MimeGuess;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sinex_core::validation::validate_path_within_root;
use sinex_core::{
    types::{
        domain::{EventSource, EventType, SanitizedPath},
        ulid::Ulid,
    },
    Event as CoreEvent, Id, OffsetKind, Provenance,
};
use sinex_node_sdk::{
    acquisition_manager::{AcquisitionManager, RotationPolicy},
    event_processor::EventTransport,
    stage_as_you_go::StageAsYouGoContext,
    stream_processor::{
        Checkpoint, Node, ProcessorCapabilities, ProcessorInitContext, ProcessorRuntimeState,
        ProcessorType, ScanArgs, ScanEstimate, ScanReport, TimeHorizon,
    },
    NodeError, NodeResult,
};
use sinex_processor_runtime::{
    CoverageAnalysis, ExplorationProvider, ExportFormat, IngestionHistoryEntry, SourceState,
};
use std::{collections::HashMap, sync::Arc, time::Instant};
use tokio::{fs, io::AsyncReadExt};
use tracing::{error, info, warn};

const ENCODING_SNIFF_BYTES: usize = 4096;
const MATERIAL_REASON_INGEST: &str = "document-ingestor:ingest";
const MAX_CHUNK_BYTES: usize = 256 * 1024;

/// Configuration for the document ingestor.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DocumentIngestorConfig {
    /// Supported document MIME types. When empty, all types are accepted.
    pub supported_mime_types: Vec<String>,
    /// Maximum document size (bytes) allowed for ingestion.
    pub max_document_size: u64,
    /// Allowed root directories for ingestion targets.
    pub allowed_roots: Vec<String>,
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
            allowed_roots: Vec::new(),
        }
    }
}

impl DocumentIngestorConfig {
    pub fn validate(&self) -> Result<(), String> {
        if !(1024..=512 * 1024 * 1024).contains(&self.max_document_size) {
            return Err("Max document size must be between 1KB and 512MB".to_string());
        }

        if self
            .supported_mime_types
            .iter()
            .any(|m| m.trim().is_empty())
        {
            return Err("Supported MIME types cannot contain empty entries".to_string());
        }

        if self.allowed_roots.is_empty() {
            return Err("Allowed roots must be configured for document ingestion".to_string());
        }

        for root in &self.allowed_roots {
            if root.trim().is_empty() {
                return Err("Allowed roots cannot contain empty entries".to_string());
            }
            sinex_core::validation::validate_path(root)
                .map_err(|e| format!("Invalid allowed root '{root}': {e}"))?;
        }

        Ok(())
    }
}

/// Simplified document processor that ingests local files.
pub struct DocumentProcessor {
    runtime: Option<ProcessorRuntimeState>,
    config: DocumentIngestorConfig,
    stage_context: Option<StageAsYouGoContext>,
    acquisition: Option<Arc<AcquisitionManager>>,
}

impl DocumentProcessor {
    pub fn new() -> Self {
        Self {
            runtime: None,
            config: DocumentIngestorConfig::default(),
            stage_context: None,
            acquisition: None,
        }
    }

    fn is_allowed_path(&self, target: &str) -> NodeResult<bool> {
        for root in &self.config.allowed_roots {
            if validate_path_within_root(target, root).is_ok() {
                return Ok(true);
            }
        }
        Ok(false)
    }

    async fn initialise_with_runtime_state(
        &mut self,
        runtime: ProcessorRuntimeState,
        config: DocumentIngestorConfig,
    ) -> NodeResult<()> {
        config.validate().map_err(NodeError::Configuration)?;

        let publisher = match runtime.transport() {
            EventTransport::Nats(publisher) => Arc::clone(publisher),
        };

        AcquisitionManager::bootstrap_streams(publisher.nats_client())
            .await
            .map_err(NodeError::from)?;

        let acquisition = Arc::new(runtime.acquisition_manager(
            RotationPolicy::default(),
            "document",
            "document-ingestor",
        )?);
        let stage_context = StageAsYouGoContext::from_runtime(&runtime)
            .with_acquisition_manager(Arc::clone(&acquisition));

        self.runtime = Some(runtime);
        self.config = config;
        self.stage_context = Some(stage_context);
        self.acquisition = Some(acquisition);

        Ok(())
    }

    async fn ingest_target(&self, target: &str) -> NodeResult<Option<Ulid>> {
        let stage_context = self
            .stage_context
            .as_ref()
            .ok_or_else(|| NodeError::Lifecycle("Stage context not initialized".into()))?;
        let acquisition = self
            .acquisition
            .as_ref()
            .ok_or_else(|| NodeError::Lifecycle("Acquisition manager not initialized".into()))?;

        let path_buf = std::path::PathBuf::from(target);
        let utf8_path = Utf8PathBuf::from_path_buf(path_buf.clone()).map_err(|_| {
            NodeError::Processing(format!("Document path must be valid UTF-8: {target}"))
        })?;
        let sanitized_path = SanitizedPath::from_str_validated(utf8_path.as_str())
            .map_err(|e| NodeError::Processing(format!("Invalid document path: {e}")))?;

        if !self.is_allowed_path(utf8_path.as_str())? {
            return Err(NodeError::Processing(format!(
                "Document path is outside allowed roots: {target}"
            )));
        }

        let metadata = fs::metadata(&utf8_path).await?;
        if !metadata.is_file() {
            return Err(NodeError::Processing(format!(
                "Document path is not a file: {target}"
            )));
        }

        let file_size = metadata.len();
        if file_size > self.config.max_document_size {
            warn!(
                size = file_size,
                limit = self.config.max_document_size,
                path = %utf8_path,
                "Skipping document larger than configured limit"
            );
            return Ok(None);
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

        let encoding = self
            .detect_encoding(&utf8_path, &mime)
            .await
            .unwrap_or_else(|err| {
                warn!(error = %err, path = %utf8_path, "Failed to detect encoding; defaulting to binary");
                Some("binary".to_string())
            });

        let mut metadata_json = json!({
            "path": utf8_path.as_str(),
            "sanitized_path": sanitized_path.as_str(),
            "mime_type": mime,
            "size_bytes": file_size,
            "encoding": encoding.clone(),
        });

        let mut handle = acquisition
            .begin_material_with_metadata(utf8_path.as_str(), metadata_json.clone())
            .await
            .map_err(NodeError::from)?;
        let mut file = fs::File::open(&utf8_path).await?;
        let mut total_bytes: i64 = 0;
        let mut buf = vec![0u8; MAX_CHUNK_BYTES];

        loop {
            let read = file.read(&mut buf).await?;
            if read == 0 {
                break;
            }

            acquisition
                .append_slice(&mut handle, &buf[..read])
                .await
                .map_err(NodeError::from)?;
            total_bytes += read as i64;
        }

        let material_id = handle.material_id;

        acquisition
            .finalize(handle, MATERIAL_REASON_INGEST)
            .await
            .map_err(NodeError::from)?;

        if let Some(obj) = metadata_json.as_object_mut() {
            obj.insert(
                "source_material_id".to_string(),
                serde_json::json!(material_id.to_string()),
            );
        }

        let payload = serde_json::json!({
            "file_path": sanitized_path.as_str(),
            "source_material_id": material_id.to_string(),
            "size_bytes": file_size,
            "mime_type": mime.clone(),
            "encoding": encoding,
            "metadata": metadata_json,
        });

        let provenance = Provenance::Material {
            id: Id::from_ulid(material_id),
            anchor_byte: 0,
            offset_start: Some(0),
            offset_end: Some(total_bytes),
            offset_kind: OffsetKind::Byte,
        };

        let mut event = CoreEvent::create(
            EventSource::from_static("document_ingestor"),
            EventType::from("document.ingested"),
            payload,
            provenance,
        );
        event.id = Some(Id::from_ulid(Ulid::new()));

        stage_context
            .emit_event_with_provenance(event, material_id, Some(0), Some(total_bytes))
            .await?;

        Ok(Some(material_id))
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
impl Node for DocumentProcessor {
    type Config = DocumentIngestorConfig;

    async fn initialize(&mut self, init: ProcessorInitContext<Self::Config>) -> NodeResult<()> {
        let (config, runtime) = init.into_runtime();
        self.initialise_with_runtime_state(runtime, config).await
    }

    async fn scan(
        &mut self,
        _from: Checkpoint,
        until: TimeHorizon,
        args: ScanArgs,
    ) -> NodeResult<ScanReport> {
        let start = Instant::now();
        let mut events_processed = 0u64;
        let mut successful_targets = Vec::new();
        let mut failed_targets = Vec::new();
        let mut warnings = Vec::new();

        match until {
            TimeHorizon::Snapshot | TimeHorizon::Historical { .. } => {
                if args.dry_run {
                    info!(targets = args.targets.len(), "Dry-run document ingestion");
                } else {
                    for target in &args.targets {
                        match self.ingest_target(target).await {
                            Ok(Some(_doc)) => {
                                events_processed += 1;
                                successful_targets.push(target.clone());
                            }
                            Ok(None) => {
                                warnings
                                    .push(format!("Skipped target {target} (no events emitted)"));
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
                return Err(NodeError::Processing(
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
            warnings,
        })
    }

    fn processor_name(&self) -> &str {
        "document-ingestor"
    }

    fn processor_type(&self) -> ProcessorType {
        ProcessorType::Ingestor
    }

    async fn current_checkpoint(&self) -> NodeResult<Checkpoint> {
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
    ) -> NodeResult<ScanEstimate> {
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
            stage_context: self.stage_context.clone(),
            acquisition: self.acquisition.clone(),
        }
    }
}

impl Default for DocumentProcessor {
    fn default() -> Self {
        Self::new()
    }
}

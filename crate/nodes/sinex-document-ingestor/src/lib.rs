#![doc = include_str!("../docs/README.md")]
#![doc = include_str!("../../../lib/sinex-node-sdk/docs/overview.md")]

//! Document ingestor that captures materials directly into `JetStream` via the
//! `AcquisitionManager` (Stage-as-You-Go).

use camino::{Utf8Path, Utf8PathBuf};
use mime_guess::MimeGuess;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sinex_node_sdk::{
    CoverageAnalysis, ExplorationProvider, ExportFormat, IngestionHistoryEntry, SourceState,
};
use sinex_node_sdk::{
    EventTransport, NodeResult, SinexError,
    acquisition_manager::{AcquisitionManager, RotationPolicy},
    ingestor_node::IngestorNode,
    runtime::stream::{
        Checkpoint, NodeCapabilities, NodeRuntimeState, ScanArgs, ScanReport, TimeHorizon,
    },
    stage_as_you_go::StageAsYouGoContext,
};
use sinex_primitives::temporal::Timestamp;
use sinex_primitives::validation::validate_path_within_root;
use sinex_primitives::{
    Uuid,
    domain::SanitizedPath,
    events::{EventPayload, payloads::document::DocumentIngestedPayload},
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
    pub fn validate(&self) -> NodeResult<()> {
        if !(1024..=512 * 1024 * 1024).contains(&self.max_document_size) {
            return Err(SinexError::configuration(
                "Max document size must be between 1KB and 512MB".to_string(),
            ));
        }

        if self
            .supported_mime_types
            .iter()
            .any(|m| m.trim().is_empty())
        {
            return Err(SinexError::configuration(
                "Supported MIME types cannot contain empty entries".to_string(),
            ));
        }

        if self.allowed_roots.is_empty() {
            return Err(SinexError::configuration(
                "Allowed roots must be configured for document ingestion".to_string(),
            ));
        }

        for root in &self.allowed_roots {
            if root.trim().is_empty() {
                return Err(SinexError::configuration(
                    "Allowed roots cannot contain empty entries".to_string(),
                ));
            }
            sinex_primitives::validation::validate_path(root).map_err(|e| {
                SinexError::configuration(format!("Invalid allowed root '{root}': {e}"))
            })?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DocumentCheckpoint {}

/// Simplified document node that ingests local files.
pub struct DocumentNode {
    runtime: Option<NodeRuntimeState>,
    config: DocumentIngestorConfig,
    stage_context: Option<StageAsYouGoContext>,
    acquisition: Option<Arc<AcquisitionManager>>,
}

impl DocumentNode {
    #[must_use]
    pub fn new() -> Self {
        Self {
            runtime: None,
            config: DocumentIngestorConfig::default(),
            stage_context: None,
            acquisition: None,
        }
    }

    fn dry_run_report(target_count: usize) -> ScanReport {
        ScanReport {
            events_processed: 0,
            duration: std::time::Duration::from_millis(0),
            final_checkpoint: Checkpoint::None,
            time_range: None,
            node_stats: HashMap::new(),
            successful_targets: Vec::new(),
            failed_targets: Vec::new(),
            warnings: vec![format!(
                "Dry-run mode enabled; skipped {target_count} document target(s)"
            )],
        }
    }

    fn completed_report(
        started_at: Timestamp,
        finished_at: Timestamp,
        duration: std::time::Duration,
        events_processed: u64,
        successful_targets: Vec<String>,
        failed_targets: Vec<(String, String)>,
        warnings: Vec<String>,
    ) -> ScanReport {
        ScanReport {
            events_processed,
            duration,
            final_checkpoint: Checkpoint::timestamp(finished_at, None),
            time_range: Some((started_at, finished_at)),
            node_stats: HashMap::new(),
            successful_targets,
            failed_targets,
            warnings,
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

    async fn ingest_targets(&self, targets: &[String]) -> NodeResult<ScanReport> {
        let started_at = Timestamp::now();
        let start = Instant::now();
        let mut events_processed = 0u64;
        let mut successful_targets = Vec::new();
        let mut failed_targets = Vec::new();
        let mut warnings = Vec::new();

        for target in targets {
            match self.ingest_target(target).await {
                Ok(Some(_doc)) => {
                    events_processed += 1;
                    successful_targets.push(target.clone());
                }
                Ok(None) => {
                    warnings.push(format!("Skipped target {target} (no events emitted)"));
                }
                Err(err) => {
                    error!(path = %target, error = %err, "Failed to ingest document");
                    failed_targets.push((target.clone(), err.to_string()));
                }
            }
        }

        let finished_at = Timestamp::now();
        Ok(Self::completed_report(
            started_at,
            finished_at,
            start.elapsed(),
            events_processed,
            successful_targets,
            failed_targets,
            warnings,
        ))
    }

    async fn ingest_target(&self, target: &str) -> NodeResult<Option<Uuid>> {
        let stage_context = self
            .stage_context
            .as_ref()
            .ok_or_else(|| SinexError::lifecycle("Stage context not initialized"))?;
        let acquisition = self
            .acquisition
            .as_ref()
            .ok_or_else(|| SinexError::lifecycle("Acquisition manager not initialized"))?;

        let path_buf = std::path::PathBuf::from(target);
        let utf8_path = Utf8PathBuf::from_path_buf(path_buf.clone()).map_err(|_| {
            SinexError::processing(format!("Document path must be valid UTF-8: {target}"))
        })?;
        let sanitized_path = SanitizedPath::from_str_validated(utf8_path.as_str())
            .map_err(|e| SinexError::processing("Invalid document path").with_source(e))?;

        if !self.is_allowed_path(utf8_path.as_str())? {
            return Err(SinexError::processing(format!(
                "Document path is outside allowed roots: {target}"
            )));
        }

        let metadata = fs::metadata(&utf8_path).await?;
        if !metadata.is_file() {
            return Err(SinexError::processing(format!(
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
            return Err(SinexError::processing(format!(
                "Unsupported MIME type '{mime}' for document path {target}"
            )));
        }

        let encoding = self
            .detect_encoding(&utf8_path, &mime)
            .await
            .unwrap_or_else(|err| {
                warn!(error = %err, path = %utf8_path, "Failed to detect encoding; defaulting to binary");
                Some("binary".to_string())
            });

        let metadata_json = json!({
            "path": utf8_path.as_str(),
            "sanitized_path": sanitized_path.as_str(),
            "mime_type": mime,
            "size_bytes": file_size,
            "encoding": encoding.clone(),
        });

        let mut handle = acquisition
            .begin_material_with_metadata(utf8_path.as_str(), metadata_json.clone())
            .await?;
        let mut file = fs::File::open(&utf8_path).await?;
        let mut total_bytes: i64 = 0;
        let mut buf = vec![0u8; MAX_CHUNK_BYTES];

        loop {
            let read = file.read(&mut buf).await?;
            if read == 0 {
                break;
            }

            acquisition.append_slice(&mut handle, &buf[..read]).await?;
            total_bytes += read as i64;
        }

        let material_id = handle.material_id;

        acquisition.finalize(handle, MATERIAL_REASON_INGEST).await?;

        let payload = DocumentIngestedPayload {
            file_path: sanitized_path.as_str().to_string(),
            source_material_id: material_id.to_string(),
            size_bytes: file_size,
            mime_type: Some(mime.clone()),
            encoding,
        };

        let event = payload
            .from_material(material_id)
            .with_offset_start(0)?
            .with_offset_end(total_bytes)?
            .build()
            .map_err(|e| SinexError::processing("Failed to build event").with_source(e))?;

        let json_event = event
            .to_json_event()
            .map_err(|e| SinexError::processing("Failed to serialize event").with_source(e))?;

        stage_context
            .emit_event_with_provenance(json_event, material_id, Some(0), Some(total_bytes))
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

impl IngestorNode for DocumentNode {
    type Config = DocumentIngestorConfig;
    type State = DocumentCheckpoint;

    fn name(&self) -> &'static str {
        "document-ingestor"
    }

    fn capabilities(&self) -> NodeCapabilities {
        NodeCapabilities {
            supports_continuous: false,
            supports_historical: true,
            supports_snapshot: true,
            supports_interactive: false,
            max_scan_size: Some(1024),
            supports_concurrent: false,
            manages_own_continuous_loop: false,
        }
    }

    async fn initialize(
        &mut self,
        config: Self::Config,
        runtime: &NodeRuntimeState,
        _state: &mut Self::State,
    ) -> NodeResult<()> {
        config.validate()?;

        let publisher = match runtime.transport() {
            EventTransport::Nats(publisher) => Arc::clone(publisher),
        };

        AcquisitionManager::bootstrap_streams(publisher.nats_client()).await?;

        let acquisition = Arc::new(runtime.acquisition_manager(
            RotationPolicy::default(),
            "document",
            "document-ingestor",
        )?);
        let stage_context = StageAsYouGoContext::from_runtime(runtime)
            .with_acquisition_manager(Arc::clone(&acquisition));

        self.runtime = Some(runtime.clone());
        self.config = config;
        self.stage_context = Some(stage_context);
        self.acquisition = Some(acquisition);

        Ok(())
    }

    async fn scan_snapshot(
        &mut self,
        _state: &mut Self::State,
        args: ScanArgs,
    ) -> NodeResult<ScanReport> {
        if args.dry_run {
            info!(targets = args.targets.len(), "Dry-run document ingestion");
            Ok(Self::dry_run_report(args.targets.len()))
        } else {
            self.ingest_targets(&args.targets).await
        }
    }

    async fn scan_historical(
        &mut self,
        _state: &mut Self::State,
        _from: Checkpoint,
        _until: TimeHorizon,
        args: ScanArgs,
    ) -> NodeResult<ScanReport> {
        // Historical scan for files is effectively the same as snapshot for specific targets.
        if args.dry_run {
            info!(targets = args.targets.len(), "Dry-run historical document ingestion");
            Ok(Self::dry_run_report(args.targets.len()))
        } else {
            self.ingest_targets(&args.targets).await
        }
    }

    async fn run_continuous(
        &mut self,
        _state: &mut Self::State,
        _from: Checkpoint,
        _shutdown_rx: tokio::sync::watch::Receiver<bool>,
    ) -> NodeResult<ScanReport> {
        Err(SinexError::processing(
            "Continuous document ingestion is no longer supported",
        ))
    }

    async fn shutdown(&mut self, _state: &Self::State) -> NodeResult<()> {
        Ok(())
    }
}

impl ExplorationProvider for DocumentNode {
    fn get_source_state(&self) -> NodeResult<SourceState> {
        Ok(SourceState {
            is_connected: true,
            healthy: true,
            description: "Document Ingestor".to_string(),
            last_updated: Timestamp::now(),
            lag_seconds: None,
            recent_activity: Vec::new(),
            total_items: None,
            metadata: std::collections::HashMap::new(),
        })
    }

    fn get_ingestion_history(&self, _limit: u64) -> NodeResult<Vec<IngestionHistoryEntry>> {
        Ok(Vec::new())
    }

    fn get_coverage_analysis(
        &self,
        _time_range: Option<(Timestamp, Timestamp)>,
    ) -> NodeResult<CoverageAnalysis> {
        sinex_node_sdk::exploration::coverage_analysis_unavailable(
            "coverage analysis is not implemented for document ingestor sources",
        )
    }

    fn export_data(&self, _path: &SanitizedPath, _format: ExportFormat) -> NodeResult<()> {
        Err(SinexError::invalid_state(
            "Document ingestor does not support data export",
        ))
    }
}

impl Clone for DocumentNode {
    fn clone(&self) -> Self {
        Self {
            runtime: self.runtime.clone(),
            config: self.config.clone(),
            stage_context: self.stage_context.clone(),
            acquisition: self.acquisition.clone(),
        }
    }
}

impl Default for DocumentNode {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::DocumentNode;
    use sinex_node_sdk::runtime::stream::Checkpoint;
    use sinex_primitives::temporal::Timestamp;
    use xtask::sandbox::sinex_test;

    #[sinex_test]
    async fn test_completed_report_uses_elapsed_window() -> ::xtask::sandbox::TestResult<()> {
        let started_at =
            Timestamp::from_unix_timestamp(1_700_000_000).expect("timestamp should be valid");
        let finished_at =
            Timestamp::from_unix_timestamp(1_700_000_123).expect("timestamp should be valid");
        let report = DocumentNode::completed_report(
            started_at,
            finished_at,
            std::time::Duration::from_secs(2),
            3,
            vec!["/tmp/doc.txt".to_string()],
            Vec::new(),
            Vec::new(),
        );

        assert_eq!(
            report.final_checkpoint,
            Checkpoint::timestamp(finished_at, None)
        );
        assert_eq!(report.time_range, Some((started_at, finished_at)));
        Ok(())
    }
}

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
        Checkpoint, ContinuousStart, NodeCapabilities, NodeRuntimeState, ScanArgs, ScanReport,
        TimeHorizon,
    },
    stage_as_you_go::StageAsYouGoContext,
    stage_material_from_file,
};
use sinex_primitives::temporal::Timestamp;
use sinex_primitives::validation::validate_path_within_root;
use sinex_primitives::{
    Uuid,
    domain::SanitizedPath,
    events::{EventPayload, payloads::document::DocumentIngestedPayload},
    privacy::{self, ProcessingContext},
};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::Arc,
    time::{Instant, SystemTime},
};
use tokio::fs;
use tokio::io::AsyncReadExt;
use tracing::{error, info, warn};

const ENCODING_SNIFF_BYTES: usize = 4096;
const MATERIAL_REASON_INGEST: &str = "document-ingestor:ingest";

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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct DocumentFingerprint {
    size_bytes: u64,
    modified_unix_ms: Option<u64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DocumentCheckpoint {
    #[serde(default)]
    scanned_documents: HashMap<String, DocumentFingerprint>,
}

#[derive(Debug, Clone)]
struct ResolvedDocumentTarget {
    path: Utf8PathBuf,
    strict: bool,
}

#[derive(Debug, Clone)]
struct ResolvedDocumentTargets {
    targets: Vec<ResolvedDocumentTarget>,
    full_root_scan: bool,
}

#[derive(Debug)]
struct DocumentInput {
    path: Utf8PathBuf,
    sanitized_path: SanitizedPath,
    file_size: u64,
    mime: String,
    encoding: Option<String>,
    fingerprint: DocumentFingerprint,
}

#[derive(Debug)]
enum DocumentSkipReason {
    Unchanged,
    UnsupportedMime,
    TooLarge,
}

#[derive(Debug)]
struct DocumentSkip {
    reason: DocumentSkipReason,
    fingerprint: DocumentFingerprint,
}

#[derive(Debug)]
enum DocumentInspection {
    Emit(DocumentInput),
    Skip(DocumentSkip),
}

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

    fn is_allowed_path(&self, target: &str) -> bool {
        self.config
            .allowed_roots
            .iter()
            .any(|root| validate_path_within_root(target, root).is_ok())
    }

    fn default_scan_roots(&self) -> Vec<String> {
        self.config.allowed_roots.clone()
    }

    fn resolve_targets(
        &self,
        targets: &[String],
        warnings: &mut Vec<String>,
    ) -> NodeResult<ResolvedDocumentTargets> {
        let full_root_scan = targets.is_empty();
        let top_level_targets = if full_root_scan {
            self.default_scan_roots()
        } else {
            targets.to_vec()
        };

        let mut deduped = BTreeMap::<String, ResolvedDocumentTarget>::new();
        for raw_target in top_level_targets {
            for resolved in self.expand_target(&raw_target, full_root_scan, warnings)? {
                let key = resolved.path.as_str().to_string();
                deduped
                    .entry(key)
                    .and_modify(|existing| existing.strict |= resolved.strict)
                    .or_insert(resolved);
            }
        }

        Ok(ResolvedDocumentTargets {
            targets: deduped.into_values().collect(),
            full_root_scan,
        })
    }

    fn expand_target(
        &self,
        target: &str,
        from_default_root_scan: bool,
        warnings: &mut Vec<String>,
    ) -> NodeResult<Vec<ResolvedDocumentTarget>> {
        let path_buf = std::path::PathBuf::from(target);
        let utf8_path = Utf8PathBuf::from_path_buf(path_buf.clone()).map_err(|_| {
            SinexError::processing(format!("Document path must be valid UTF-8: {target}"))
        })?;

        if !self.is_allowed_path(utf8_path.as_str()) {
            return Err(SinexError::processing(format!(
                "Document path is outside allowed roots: {target}"
            )));
        }

        let metadata = std::fs::symlink_metadata(utf8_path.as_std_path()).map_err(|error| {
            SinexError::io("Failed to inspect document scan target")
                .with_std_error(&error)
                .with_path(utf8_path.as_str())
        })?;

        if metadata.file_type().is_symlink() {
            return Err(SinexError::processing(format!(
                "Symlink document targets are not supported: {target}"
            )));
        }

        if metadata.is_file() {
            return Ok(vec![ResolvedDocumentTarget {
                path: utf8_path,
                strict: !from_default_root_scan,
            }]);
        }

        if metadata.is_dir() {
            let files = Self::collect_target_files(&utf8_path, warnings)?;
            return Ok(files
                .into_iter()
                .map(|path| ResolvedDocumentTarget {
                    path,
                    strict: false,
                })
                .collect());
        }

        Err(SinexError::processing(format!(
            "Document target is neither a file nor a directory: {target}"
        )))
    }

    fn collect_target_files(
        path: &Utf8Path,
        warnings: &mut Vec<String>,
    ) -> NodeResult<Vec<Utf8PathBuf>> {
        let metadata = std::fs::symlink_metadata(path.as_std_path()).map_err(|error| {
            SinexError::io("Failed to inspect document scan target")
                .with_std_error(&error)
                .with_path(path.as_str())
        })?;

        if metadata.file_type().is_symlink() {
            warnings.push(format!(
                "Skipping symlink during document scan: {}",
                path.as_str()
            ));
            return Ok(Vec::new());
        }

        if metadata.is_file() {
            return Ok(vec![path.to_path_buf()]);
        }

        if !metadata.is_dir() {
            return Ok(Vec::new());
        }

        let entries = std::fs::read_dir(path.as_std_path()).map_err(|error| {
            SinexError::io("Failed to enumerate document scan directory")
                .with_std_error(&error)
                .with_path(path.as_str())
        })?;

        let mut files = Vec::new();
        for entry in entries {
            let entry = match entry {
                Ok(entry) => entry,
                Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => {
                    warnings.push(format!(
                        "Skipping unreadable document directory entry under {}",
                        path.as_str()
                    ));
                    continue;
                }
                Err(error) => {
                    return Err(SinexError::io("Failed to inspect document directory entry")
                        .with_std_error(&error)
                        .with_path(path.as_str()));
                }
            };

            let child = Utf8PathBuf::from_path_buf(entry.path()).map_err(|path| {
                SinexError::processing(format!(
                    "Document path must be valid UTF-8: {}",
                    path.display()
                ))
            })?;
            files.extend(Self::collect_target_files(&child, warnings)?);
        }

        Ok(files)
    }

    fn fingerprint_for(metadata: &std::fs::Metadata) -> DocumentFingerprint {
        DocumentFingerprint {
            size_bytes: metadata.len(),
            modified_unix_ms: metadata
                .modified()
                .ok()
                .and_then(Self::system_time_to_unix_ms),
        }
    }

    fn system_time_to_unix_ms(value: SystemTime) -> Option<u64> {
        value
            .duration_since(SystemTime::UNIX_EPOCH)
            .ok()
            .and_then(|duration| u64::try_from(duration.as_millis()).ok())
    }

    async fn inspect_target(
        &self,
        target: &ResolvedDocumentTarget,
        state: &DocumentCheckpoint,
    ) -> NodeResult<DocumentInspection> {
        let sanitized_path = SanitizedPath::from_str_validated(target.path.as_str())
            .map_err(|e| SinexError::processing("Invalid document path").with_source(e))?;

        let metadata = fs::metadata(&target.path).await?;
        if !metadata.is_file() {
            return Err(SinexError::processing(format!(
                "Document path is not a file: {}",
                target.path
            )));
        }

        let fingerprint = Self::fingerprint_for(&metadata);
        if state
            .scanned_documents
            .get(target.path.as_str())
            .is_some_and(|previous| previous == &fingerprint)
        {
            return Ok(DocumentInspection::Skip(DocumentSkip {
                reason: DocumentSkipReason::Unchanged,
                fingerprint,
            }));
        }

        if metadata.len() > self.config.max_document_size {
            warn!(
                size = metadata.len(),
                limit = self.config.max_document_size,
                path = %target.path,
                "Skipping document larger than configured limit"
            );
            return Ok(DocumentInspection::Skip(DocumentSkip {
                reason: DocumentSkipReason::TooLarge,
                fingerprint,
            }));
        }

        let mime = MimeGuess::from_path(&target.path)
            .first_raw()
            .unwrap_or("application/octet-stream")
            .to_string();
        let mime_supported = self.config.supported_mime_types.is_empty()
            || self
                .config
                .supported_mime_types
                .iter()
                .any(|value| value == &mime);
        if !mime_supported {
            if target.strict {
                return Err(SinexError::processing(format!(
                    "Unsupported MIME type '{mime}' for document path {}",
                    target.path
                )));
            }

            return Ok(DocumentInspection::Skip(DocumentSkip {
                reason: DocumentSkipReason::UnsupportedMime,
                fingerprint,
            }));
        }

        let encoding = self
            .detect_encoding(&target.path, &mime)
            .await
            .unwrap_or_else(|err| {
                warn!(error = %err, path = %target.path, "Failed to detect encoding; defaulting to binary");
                Some("binary".to_string())
            });

        Ok(DocumentInspection::Emit(DocumentInput {
            path: target.path.clone(),
            sanitized_path,
            file_size: metadata.len(),
            mime,
            encoding,
            fingerprint,
        }))
    }

    async fn ingest_targets(
        &self,
        state: &mut DocumentCheckpoint,
        targets: &[String],
    ) -> NodeResult<ScanReport> {
        let started_at = Timestamp::now();
        let start = Instant::now();
        let mut events_processed = 0u64;
        let mut successful_targets = Vec::new();
        let mut failed_targets = Vec::new();
        let mut warnings = Vec::new();
        let mut skipped_unchanged = 0u64;
        let mut skipped_unsupported = 0u64;
        let mut skipped_oversized = 0u64;

        let resolved_targets = self.resolve_targets(targets, &mut warnings)?;
        let mut observed_paths = BTreeSet::new();

        for target in &resolved_targets.targets {
            observed_paths.insert(target.path.as_str().to_string());
            match self.inspect_target(target, state).await {
                Ok(DocumentInspection::Emit(document)) => {
                    match self.ingest_document(&document).await {
                        Ok(Some(_material_id)) => {
                            events_processed += 1;
                            successful_targets.push(document.path.as_str().to_string());
                            state
                                .scanned_documents
                                .insert(document.path.as_str().to_string(), document.fingerprint);
                        }
                        Ok(None) => {
                            warnings.push(format!(
                                "Skipped target {} (no events emitted)",
                                document.path.as_str()
                            ));
                        }
                        Err(err) => {
                            error!(path = %document.path, error = %err, "Failed to ingest document");
                            failed_targets
                                .push((document.path.as_str().to_string(), err.to_string()));
                        }
                    }
                }
                Ok(DocumentInspection::Skip(skip)) => {
                    state
                        .scanned_documents
                        .insert(target.path.as_str().to_string(), skip.fingerprint);
                    match skip.reason {
                        DocumentSkipReason::Unchanged => skipped_unchanged += 1,
                        DocumentSkipReason::UnsupportedMime => skipped_unsupported += 1,
                        DocumentSkipReason::TooLarge => skipped_oversized += 1,
                    }
                }
                Err(err) => {
                    error!(path = %target.path, error = %err, "Failed to inspect document");
                    failed_targets.push((target.path.as_str().to_string(), err.to_string()));
                }
            }
        }

        if resolved_targets.full_root_scan {
            state
                .scanned_documents
                .retain(|path, _| observed_paths.contains(path));
        }

        if skipped_unchanged > 0 {
            warnings.push(format!("Skipped {skipped_unchanged} unchanged document(s)"));
        }
        if skipped_unsupported > 0 {
            warnings.push(format!(
                "Skipped {skipped_unsupported} non-document file(s) with unsupported MIME types"
            ));
        }
        if skipped_oversized > 0 {
            warnings.push(format!(
                "Skipped {skipped_oversized} document(s) larger than the configured size limit"
            ));
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

    async fn ingest_document(&self, document: &DocumentInput) -> NodeResult<Option<Uuid>> {
        let stage_context = self
            .stage_context
            .as_ref()
            .ok_or_else(|| SinexError::lifecycle("Stage context not initialized"))?;
        let acquisition = self
            .acquisition
            .as_ref()
            .ok_or_else(|| SinexError::lifecycle("Acquisition manager not initialized"))?;

        let metadata_json = json!({
            "path": document.path.as_str(),
            "sanitized_path": document.sanitized_path.as_str(),
            "mime_type": document.mime,
            "size_bytes": document.file_size,
            "encoding": document.encoding.clone(),
        });

        let (material_id, total_bytes) = stage_material_from_file(
            acquisition,
            &document.path,
            MATERIAL_REASON_INGEST,
            Some(metadata_json),
        )
        .await?;

        // Run the file path through the privacy engine so user-home prefixes
        // are collapsed to `<HOME>/...` before the event leaves the ingestor.
        // The catalog rule `user_home_path` already substitutes the home
        // prefix; until the engine is invoked it does nothing. See issue
        // #555.
        let redacted_file_path = redact_metadata(document.sanitized_path.as_str())?;
        let payload = DocumentIngestedPayload {
            file_path: redacted_file_path,
            source_material_id: material_id.to_string(),
            size_bytes: document.file_size,
            mime_type: Some(document.mime.clone()),
            encoding: document.encoding.clone(),
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

/// Run a value through the privacy engine using the metadata context.
///
/// The path-redaction rule in the privacy catalog (`user_home_path`) is what
/// collapses `/home/USER/...` to `<HOME>/...`. Until the engine is invoked
/// no rule fires. Any error here is bubbled up as a configuration failure
/// rather than swallowed — the ingestor cannot honestly emit if redaction
/// is broken.
///
/// See issue #555.
fn redact_metadata(value: &str) -> NodeResult<String> {
    Ok(privacy::process(value, ProcessingContext::Metadata)
        .map_err(|error| {
            SinexError::configuration("failed to initialize privacy engine")
                .with_context("component", "document_path_redaction")
                .with_std_error(error)
        })?
        .text
        .into_owned())
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
            manages_own_checkpoints: true,
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

        let acquisition =
            Arc::new(runtime.acquisition_manager(RotationPolicy::default(), "document")?);
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
            self.ingest_targets(_state, &args.targets).await
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
            info!(
                targets = args.targets.len(),
                "Dry-run historical document ingestion"
            );
            Ok(Self::dry_run_report(args.targets.len()))
        } else {
            self.ingest_targets(_state, &args.targets).await
        }
    }

    async fn run_continuous(
        &mut self,
        _state: &mut Self::State,
        _start: ContinuousStart,
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
        let initialized =
            self.runtime.is_some() && self.stage_context.is_some() && self.acquisition.is_some();
        let config_status = self.config.validate().err().map(|error| error.to_string());
        let config_healthy = config_status.is_none();
        let mut metadata = std::collections::HashMap::new();
        metadata.insert("initialized".to_string(), json!(initialized));
        metadata.insert(
            "allowed_roots".to_string(),
            json!(self.config.allowed_roots),
        );
        metadata.insert(
            "supported_mime_types".to_string(),
            json!(self.config.supported_mime_types),
        );
        metadata.insert("continuous_supported".to_string(), json!(false));
        metadata.insert(
            "deployment_mode".to_string(),
            json!("managed_snapshot_scan"),
        );
        if let Some(error) = &config_status {
            metadata.insert("config_error".to_string(), json!(error));
        }

        let (is_connected, healthy, description) = if !initialized {
            (
                false,
                false,
                "Document ingestor is not initialized".to_string(),
            )
        } else if let Some(error) = config_status {
            (
                false,
                false,
                format!("Document ingestor configuration is invalid: {error}"),
            )
        } else {
            (
                true,
                true,
                format!(
                    "Document ingestor ready for {} root(s) via managed snapshot scans",
                    self.config.allowed_roots.len()
                ),
            )
        };

        Ok(SourceState {
            is_connected,
            healthy: healthy && config_healthy,
            description,
            last_updated: None,
            lag_seconds: None,
            recent_activity: Vec::new(),
            total_items: None,
            metadata,
        })
    }

    fn get_ingestion_history(&self, _limit: u64) -> NodeResult<Vec<IngestionHistoryEntry>> {
        Err(SinexError::invalid_state(
            "ingestion history is not implemented for document ingestor sources",
        ))
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
    use serde_json::json;
    use sinex_node_sdk::ExplorationProvider;
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

    #[sinex_test]
    async fn document_source_state_is_unhealthy_before_initialize()
    -> ::xtask::sandbox::TestResult<()> {
        let node = DocumentNode::new();
        let state = node.get_source_state()?;

        assert!(!state.is_connected);
        assert!(!state.healthy);
        assert_eq!(state.last_updated, None);
        assert!(state.description.contains("not initialized"));
        assert_eq!(state.metadata.get("initialized"), Some(&json!(false)));
        Ok(())
    }

    #[sinex_test]
    async fn document_source_state_surfaces_invalid_config() -> ::xtask::sandbox::TestResult<()> {
        let node = DocumentNode::new();
        let state = node.get_source_state()?;

        assert_eq!(
            state.metadata.get("config_error"),
            Some(&json!(
                "Configuration error: Allowed roots must be configured for document ingestion"
            ))
        );
        assert!(!state.healthy);
        Ok(())
    }

    #[sinex_test]
    async fn document_node_reports_ingestion_history_unavailable()
    -> ::xtask::sandbox::TestResult<()> {
        let node = DocumentNode::new();
        let error = node
            .get_ingestion_history(10)
            .expect_err("document node should not report empty ingestion history as success");
        assert!(error.to_string().contains("not implemented"));
        Ok(())
    }
}

// --- Source-unit descriptor (issue #690 / #734) ---

use sinex_primitives::register_source_unit;
use sinex_primitives::source_unit::{
    CheckpointFamily as SuCheckpointFamily, Horizon as SuHorizon,
    OccurrenceIdentity as SuOccurrenceIdentity, PrivacyTier as SuPrivacyTier,
    RetentionPolicy as SuRetentionPolicy, RuntimeShape as SuRuntimeShape, SourceUnitDescriptor,
};

// The document ingestor stages files as raw source material and emits a
// `document.ingested` event per file. Until the parser/chunker train (#733)
// lands the ingestor is a single-event, append-stream source.
register_source_unit! {
    SourceUnitDescriptor {
        id: "document",
        namespace: "document",
        runner_pack: "document",
        checkpoint_family: SuCheckpointFamily::AppendStream,
        event_types: &[
            ("document-ingestor", "document.ingested"),
        ],
        // Document contents are arbitrary user files — secrets are routine.
        privacy_tier: SuPrivacyTier::Secret,
        runtime_shape: SuRuntimeShape::OnDemand,
        horizons: &[SuHorizon::Continuous, SuHorizon::Historical],
        retention: SuRetentionPolicy::Forever,
        proof_obligations: &[],
        occurrence_identity: SuOccurrenceIdentity::Anchor,
        access_policy: "configured_document_roots",
        package_impact: "no_new_output",
        implementation_mode: "rust_in_pack:document",
        build_impact: sinex_primitives::source_unit::SourceUnitBuildImpact::ZERO,
    }
}

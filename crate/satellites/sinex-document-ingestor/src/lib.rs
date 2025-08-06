//! Unified Document Ingestor
//!
//! This module provides a unified StatefulStreamProcessor for ingesting documents
//! It follows the vision's "documents as source material" approach.

use camino::Utf8PathBuf;

use async_trait::async_trait;
use camino::Utf8Path;
use chrono::Utc;
use color_eyre::eyre::eyre;
use serde_json::json;
use sinex_db::models::Event;
use sinex_satellite_sdk::{
    cli::{
        CoverageAnalysis, ExplorationProvider, ExportFormat, IngestionHistoryEntry, SourceState,
    },
    stage_as_you_go::StageAsYouGoContext,
    stream_processor::{
        Checkpoint, ProcessorCapabilities, ProcessorType, ScanArgs, ScanReport,
        StatefulStreamProcessor, StreamProcessorContext, TimeHorizon,
    },
    SatelliteError, SatelliteResult,
};
use sinex_types::events::DocumentIngestedPayload;
use std::collections::HashMap;
use std::time::Duration;
use tracing::{info, warn};

/// Unified document processor that treats all documents as source material
pub struct DocumentProcessor {
    context: Option<StreamProcessorContext>,
    stage_context: Option<StageAsYouGoContext>,
}

impl DocumentProcessor {
    pub fn new() -> Self {
        Self {
            context: None,
            stage_context: None,
        }
    }

    /// Process a file using stage-as-you-go pattern for real-time provenance
    async fn process_file(&self, file_path: &Utf8Path) -> SatelliteResult<()> {
        let ctx = self.context.as_ref().ok_or_else(|| {
            SatelliteError::Processing("Document ingestor context not initialized".to_string())
        })?;

        if let Some(ref stage_context) = self.stage_context {
            // Use stage-as-you-go pattern for immediate provenance

            // Read file content
            let content = match tokio::fs::read(file_path).await {
                Ok(content) => content,
                Err(e) => {
                    warn!("Failed to read file {}: {}", file_path.as_str(), e);
                    return Ok(()); // Skip unreadable files
                }
            };

            // Determine material type and metadata
            let mime_type = mime_guess::from_path(file_path)
                .first_or_octet_stream()
                .to_string();
            let material_type = determine_material_type(&mime_type);
            let source_uri = format!("file://{}", file_path.as_str());

            // Step 1: Register in-flight source material
            let initial_metadata = json!({
                "original_path": file_path.as_str(),
                "file_extension": file_path.extension(),
                "parent_directory": file_path.parent().map(|p| p.as_str()),
                "material_type": material_type,
                "mime_type": mime_type,
                "source_uri": source_uri,
                "processed_by": "document-ingestor",
            });

            let source_material_id = stage_context
                .register_in_flight(&material_type, Some(&source_uri), initial_metadata)
                .await?;

            // Step 2: Create and emit document.ingested event with provenance
            let event = Event::from_payload(DocumentIngestedPayload {
                file_path: file_path.to_string(),
                source_material_id: source_material_id.to_string(),
                size_bytes: content.len() as u64,
                mime_type: Some(mime_type.clone()),
                encoding: None, // TODO: Detect encoding
            });

            stage_context
                .emit_event_with_provenance(
                    event,
                    source_material_id,
                    Some(0),                    // Files start at byte 0
                    Some(content.len() as i64), // End at file length
                )
                .await?;

            // Step 3: Finalize with complete content details
            let encoding = if mime_type.starts_with("text/") {
                Some("utf-8")
            } else {
                None
            };

            stage_context
                .finalize_source_material(source_material_id, &content, Some(&mime_type), encoding)
                .await?;

            info!(
                file_path = %file_path.as_str(),
                source_material_id = %source_material_id,
                material_type = %material_type,
                size_bytes = content.len(),
                "Processed document with stage-as-you-go provenance"
            );
        } else {
            warn!("Stage-as-you-go context not available, skipping document processing");
        }

        Ok(())
    }
}

/// Determine material type from MIME type
fn determine_material_type(mime_type: &str) -> String {
    match mime_type {
        t if t.starts_with("text/") => "document.text".into(),
        t if t.starts_with("image/") => "document.image".into(),
        t if t.starts_with("audio/") => "document.audio".into(),
        t if t.starts_with("video/") => "document.video".into(),
        "application/pdf" => "document.pdf".into(),
        t if t.contains("markdown") => "document.markdown".into(),
        t if t.contains("json") => "document.json".into(),
        t if t.contains("xml") => "document.xml".into(),
        _ => "document.binary".into(),
    }
}

#[async_trait]
impl StatefulStreamProcessor for DocumentProcessor {
    async fn initialize(&mut self, ctx: StreamProcessorContext) -> SatelliteResult<()> {
        info!("Initializing document processor");

        // Initialize stage-as-you-go context for real-time provenance
        self.stage_context = Some(StageAsYouGoContext::new(
            ctx.db_pool.clone(),
            ctx.ingest_client.clone(),
        ));
        info!("Stage-as-you-go context initialized for document processor");

        self.context = Some(ctx);
        Ok(())
    }

    async fn scan(
        &mut self,
        from: Checkpoint,
        until: TimeHorizon,
        args: ScanArgs,
    ) -> SatelliteResult<ScanReport> {
        let start_time = Utc::now();
        let mut events_processed = 0;

        match until {
            TimeHorizon::Snapshot => {
                // Scan specified directories for documents
                info!("Starting document snapshot scan");

                for target in &args.targets {
                    let path = Utf8Path::new(target);
                    if path.is_dir() {
                        // Recursively scan directory
                        let mut entries = tokio::fs::read_dir(path).await?;
                        while let Some(entry) = entries.next_entry().await? {
                            let entry_path = entry.path();
                            if entry_path.is_file() {
                                let utf8_path = camino::Utf8PathBuf::from_path_buf(entry_path)
                                    .map_err(|_| {
                                        SatelliteError::General(eyre!(
                                            "Path contains invalid UTF-8"
                                        ))
                                    })?;
                                self.process_file(&utf8_path).await?;
                                events_processed += 1;
                            }
                        }
                    } else if path.is_file() {
                        // Process single file
                        self.process_file(path).await?;
                        events_processed += 1;
                    }
                }

                Ok(ScanReport {
                    events_processed: events_processed as u64,
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
                // Document processor doesn't support historical mode
                // Documents are processed via snapshot mode
                Ok(ScanReport {
                    events_processed: 0,
                    duration: Duration::from_millis(1),
                    final_checkpoint: from,
                    time_range: None,
                    processor_stats: HashMap::new(),
                    successful_targets: Vec::new(),
                    failed_targets: Vec::new(),
                    warnings: vec![
                        "Document processor does not support historical mode".to_string()
                    ],
                })
            }
            TimeHorizon::Continuous => {
                // Document processor doesn't support continuous mode
                // Documents are processed via snapshot mode when files change
                Ok(ScanReport {
                    events_processed: 0,
                    duration: Duration::from_millis(1),
                    final_checkpoint: from,
                    time_range: None,
                    processor_stats: HashMap::new(),
                    successful_targets: Vec::new(),
                    failed_targets: Vec::new(),
                    warnings: vec![
                        "Document processor does not support continuous mode".to_string()
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
            max_scan_size: Some(100000), // Limit for very large directories
            supports_concurrent: false,
        }
    }
}

impl ExplorationProvider for DocumentProcessor {
    fn get_source_state(&self) -> color_eyre::eyre::Result<SourceState> {
        Ok(SourceState {
            description: "Document ingestor for processing files into source material registry"
                .to_string(),
            last_updated: Utc::now(),
            total_items: Some(0), // Could be enhanced to track processed files
            metadata: HashMap::new(),
            healthy: true,
            recent_activity: Vec::new(), // Could be enhanced with actual activity entries
        })
    }

    fn get_ingestion_history(
        &self,
        _limit: u64,
    ) -> color_eyre::eyre::Result<Vec<IngestionHistoryEntry>> {
        // Document processor doesn't maintain ingestion history
        Ok(Vec::new())
    }

    fn get_coverage_analysis(
        &self,
        _time_range: Option<(chrono::DateTime<Utc>, chrono::DateTime<Utc>)>,
    ) -> color_eyre::eyre::Result<CoverageAnalysis> {
        Ok(CoverageAnalysis {
            coverage_percentage: 100.0, // All accessible files are processed
            missing_count: 0,
            missing_samples: Vec::new(),
            duplicate_count: 0,
            sinex_total: 0,
            source_total: 0, // Total files from the source
            time_range: (Utc::now() - chrono::Duration::days(30), Utc::now()), // Last 30 days
            recommendations: Vec::new(),
        })
    }

    fn export_data(
        &self,
        _output_path: &Utf8PathBuf,
        _format: ExportFormat,
    ) -> color_eyre::eyre::Result<()> {
        // Document processor doesn't support data export
        Err(color_eyre::eyre::eyre!(
            "Document processor does not support data export"
        ))
    }
}

impl Default for DocumentProcessor {
    fn default() -> Self {
        Self::new()
    }
}

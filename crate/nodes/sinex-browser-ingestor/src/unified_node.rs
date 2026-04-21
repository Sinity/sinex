use crate::history_formats::{ParsedDumpFile, SUPPORTED_HISTORY_EXTENSIONS, parse_dump_file};
use crate::sqlite_sources::{
    BrowserSqliteFormat, BrowserSqliteSourceConfig, ensure_browser_sqlite_source,
    read_browser_sqlite_history,
};
use crate::visit::{BrowserVisitRecord, make_material_metadata};
use camino::Utf8PathBuf;
use serde::{Deserialize, Serialize};
use sinex_node_sdk::stage_as_you_go::StageAsYouGoContext;
use sinex_node_sdk::{
    BatchImporterState, EventTransport, ImportFileChangeKind, IngestorNode, NodeResult,
    RotationPolicy, SinexError, SqliteSourceCheckpointState,
    acquisition_manager::AcquisitionManager,
    checkpointed_sqlite_source_strict, discover_importable_files_at_root,
    runtime::stream::{Checkpoint, NodeRuntimeState, ScanArgs, ScanReport, TimeHorizon},
    stage_material,
};
use sinex_primitives::{
    Seconds, Timestamp,
    events::{EventPayload, payloads::PageVisitedPayload},
    privacy::{self, ProcessingContext},
};
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::sync::watch;
use tracing::info;

const MATERIAL_REASON_HISTORY: &str = "browser-history";
const DEFAULT_POLLING_INTERVAL_SECS: u64 = 30;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BrowserIngestorConfig {
    pub dump_sources: Vec<Utf8PathBuf>,
    pub sqlite_sources: Vec<BrowserSqliteSourceConfig>,
    pub polling_interval_secs: Seconds,
}

impl Default for BrowserIngestorConfig {
    fn default() -> Self {
        Self {
            dump_sources: Vec::new(),
            sqlite_sources: vec![
                BrowserSqliteSourceConfig {
                    path: Utf8PathBuf::from("/home/sinity/.local/share/qutebrowser/history.sqlite"),
                    browser: "qutebrowser".to_string(),
                    format: BrowserSqliteFormat::QutebrowserNative,
                },
                BrowserSqliteSourceConfig {
                    path: Utf8PathBuf::from(
                        "/home/sinity/.local/share/qutebrowser/webengine/History",
                    ),
                    browser: "qutebrowser".to_string(),
                    format: BrowserSqliteFormat::ChromiumHistory,
                },
            ],
            polling_interval_secs: Seconds::from_secs(DEFAULT_POLLING_INTERVAL_SECS),
        }
    }
}

impl BrowserIngestorConfig {
    pub fn validate(&self) -> NodeResult<()> {
        if self.dump_sources.is_empty() && self.sqlite_sources.is_empty() {
            return Err(SinexError::configuration(
                "browser ingestor requires at least one dump source or sqlite source",
            ));
        }
        if !(1..=3600).contains(&self.polling_interval_secs.as_secs()) {
            return Err(SinexError::configuration(
                "browser polling interval must be between 1 and 3600 seconds",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BrowserIngestorState {
    #[serde(default)]
    pub dump_imports: BatchImporterState,
    #[serde(default)]
    pub sqlite_sources: SqliteSourceCheckpointState,
}

#[derive(Default)]
pub struct BrowserNode {
    config: BrowserIngestorConfig,
    stage_context: Option<StageAsYouGoContext>,
    acquisition: Option<Arc<AcquisitionManager>>,
}

#[derive(Default)]
struct ImportPassOutcome {
    processed: u64,
    warnings: Vec<String>,
    successful_targets: Vec<String>,
    failed_targets: Vec<(String, String)>,
}

impl BrowserNode {
    async fn bootstrap_streams_for_runtime(runtime: &NodeRuntimeState) -> NodeResult<()> {
        if runtime.service_info().dry_run() {
            return Ok(());
        }

        let publisher = match runtime.transport() {
            EventTransport::Nats(publisher) => publisher.clone(),
        };
        AcquisitionManager::bootstrap_streams(publisher.nats_client()).await?;
        Ok(())
    }

    fn acquisition(&self) -> NodeResult<&Arc<AcquisitionManager>> {
        self.acquisition
            .as_ref()
            .ok_or_else(|| SinexError::lifecycle("browser acquisition manager not initialized"))
    }

    fn stage_context(&self) -> NodeResult<&StageAsYouGoContext> {
        self.stage_context
            .as_ref()
            .ok_or_else(|| SinexError::lifecycle("browser stage context not initialized"))
    }

    fn redact_document(value: &str) -> NodeResult<String> {
        Ok(privacy::process(value, ProcessingContext::Document)
            .map_err(|error| {
                SinexError::configuration("failed to initialize privacy engine")
                    .with_context("component", "browser_document_redaction")
                    .with_std_error(error)
            })?
            .text
            .into_owned())
    }

    async fn emit_visit(&self, visit: BrowserVisitRecord) -> NodeResult<()> {
        let material_len = visit.material_bytes.len() as i64;
        let material_id = stage_material(
            self.acquisition()?.as_ref(),
            &visit.source_file,
            &visit.material_bytes,
            MATERIAL_REASON_HISTORY,
            Some(make_material_metadata(&visit)),
        )
        .await?;

        let payload = PageVisitedPayload {
            browser: visit.browser,
            title: Self::redact_document(&visit.title)?,
            url: Self::redact_document(&visit.url)?,
            normalized_url: visit
                .normalized_url
                .as_deref()
                .map(Self::redact_document)
                .transpose()?,
            visit_time: visit.visit_time,
            referrer: visit
                .referrer
                .as_deref()
                .map(Self::redact_document)
                .transpose()?,
            transition: visit.transition,
            visit_id: visit.visit_id,
            visit_duration_ms: visit.visit_duration_ms,
            source_file: visit.source_file,
            line_number: visit.line_number,
            db_row_id: visit.db_row_id,
        };

        let event = payload
            .from_material(material_id)
            .with_offset_start(0)?
            .with_offset_end(material_len)?
            .build()?
            .to_json_event()
            .map_err(|error| {
                SinexError::serialization("failed to serialize page.visited event")
                    .with_std_error(&error)
            })?;

        self.stage_context()?
            .emit_event_with_provenance(event, material_id, Some(0), Some(material_len))
            .await
            .map(|_| ())
            .map_err(|error| {
                SinexError::messaging("failed to emit browser history event").with_source(error)
            })
    }

    async fn process_dump_root(
        &self,
        state: &mut BatchImporterState,
        root: &Utf8PathBuf,
    ) -> NodeResult<(u64, Vec<String>)> {
        let mut warnings = Vec::new();
        let discovered =
            match discover_importable_files_at_root(state, root, SUPPORTED_HISTORY_EXTENSIONS) {
                Ok(discovered) => discovered,
                Err(sinex_node_sdk::ScanError::PathNotFound(_)) => {
                    warnings.push(format!("browser history dump root missing: {root}"));
                    return Ok((0, warnings));
                }
                Err(error) => return Err(SinexError::io(error.to_string())),
            };

        let mut processed = 0u64;
        for file in discovered {
            let parsed = match file.change_kind {
                ImportFileChangeKind::New => parse_dump_file(&file, None),
                ImportFileChangeKind::Appended if is_appendable_dump(&file.path) => {
                    parse_dump_file(&file, None)
                }
                ImportFileChangeKind::Appended => {
                    warnings.push(format!(
                        "skipping appended browser snapshot dump {}; only jsonl/ndjson dumps support append-only resume",
                        file.path
                    ));
                    state.mark_processed(
                        &file.path,
                        file.fingerprint,
                        file.fingerprint.size_bytes,
                        file.start_line_number,
                    );
                    continue;
                }
                ImportFileChangeKind::Replaced if is_appendable_dump(&file.path) => {
                    warnings.push(format!(
                        "browser append-only dump {} was replaced; re-importing from the beginning",
                        file.path
                    ));
                    parse_dump_file(&file, None)
                }
                ImportFileChangeKind::Replaced => {
                    warnings.push(format!(
                        "skipping replaced browser snapshot dump {}; treat json/csv exports as immutable and write new filenames for new snapshots",
                        file.path
                    ));
                    state.mark_processed(
                        &file.path,
                        file.fingerprint,
                        file.fingerprint.size_bytes,
                        file.start_line_number,
                    );
                    continue;
                }
            };

            let ParsedDumpFile { visits, stats } = match parsed {
                Ok(parsed) => parsed,
                Err(error) => {
                    warnings.push(format!(
                        "failed to parse browser dump {}: {error}",
                        file.path
                    ));
                    continue;
                }
            };

            for visit in visits {
                self.emit_visit(visit).await?;
                processed = processed.saturating_add(1);
            }

            state.mark_processed(
                &file.path,
                file.fingerprint,
                file.fingerprint.size_bytes,
                file.start_line_number
                    .saturating_add(stats.delta_line_count),
            );
        }

        Ok((processed, warnings))
    }

    async fn process_sqlite_source(
        &self,
        state: &mut BrowserIngestorState,
        source: &BrowserSqliteSourceConfig,
        historical_end_time: Option<Timestamp>,
    ) -> NodeResult<(u64, Vec<String>)> {
        let mut warnings = Vec::new();
        if !source.path.exists() {
            warnings.push(format!("browser sqlite source missing: {}", source.path));
            return Ok((0, warnings));
        }

        if let Err(error) = ensure_browser_sqlite_source(source) {
            warnings.push(format!(
                "browser sqlite source {} is not ready: {error}",
                source.path
            ));
            return Ok((0, warnings));
        }

        let checkpoint_key = source.checkpoint_key();
        let report = checkpointed_sqlite_source_strict(
            &mut state.sqlite_sources,
            &checkpoint_key,
            historical_end_time,
            |from_row_id, end_time| read_browser_sqlite_history(source, from_row_id, end_time),
            |visit| async move {
                self.emit_visit(visit).await?;
                Ok(sinex_node_sdk::SqliteHistoryRowOutcome::Processed)
            },
        )
        .await
        .map_err(|error| match error {
            sinex_node_sdk::SqliteHistoryImportError::Read(error) => {
                SinexError::processing("failed to read browser sqlite history")
                    .with_context("source", source.path.to_string())
                    .with_std_error(&error)
            }
            sinex_node_sdk::SqliteHistoryImportError::Process(error) => error,
        })?;

        Ok((report.processed_rows as u64, warnings))
    }

    async fn run_import_pass(
        &self,
        state: &mut BrowserIngestorState,
        historical_end_time: Option<Timestamp>,
    ) -> NodeResult<ImportPassOutcome> {
        let mut outcome = ImportPassOutcome::default();

        for root in &self.config.dump_sources {
            match self.process_dump_root(&mut state.dump_imports, root).await {
                Ok((processed, warnings)) => {
                    outcome.processed = outcome.processed.saturating_add(processed);
                    outcome.warnings.extend(warnings);
                    outcome.successful_targets.push(root.to_string());
                }
                Err(error) => outcome
                    .failed_targets
                    .push((root.to_string(), error.to_string())),
            }
        }

        for source in &self.config.sqlite_sources {
            match self
                .process_sqlite_source(state, source, historical_end_time)
                .await
            {
                Ok((processed, warnings)) => {
                    outcome.processed = outcome.processed.saturating_add(processed);
                    outcome.warnings.extend(warnings);
                    outcome.successful_targets.push(source.path.to_string());
                }
                Err(error) => outcome
                    .failed_targets
                    .push((source.path.to_string(), error.to_string())),
            }
        }

        Ok(outcome)
    }

    fn scan_report(
        outcome: ImportPassOutcome,
        started_at: Timestamp,
        duration: Duration,
    ) -> ScanReport {
        let finished_at = Timestamp::now();
        ScanReport {
            events_processed: outcome.processed,
            duration,
            final_checkpoint: Checkpoint::timestamp(finished_at, None),
            time_range: Some((started_at, finished_at)),
            node_stats: HashMap::new(),
            successful_targets: outcome.successful_targets,
            failed_targets: outcome.failed_targets,
            warnings: outcome.warnings,
        }
    }
}

impl IngestorNode for BrowserNode {
    type Config = BrowserIngestorConfig;
    type State = BrowserIngestorState;

    fn name(&self) -> &'static str {
        "browser-ingestor"
    }

    async fn initialize(
        &mut self,
        config: Self::Config,
        runtime: &NodeRuntimeState,
        _state: &mut Self::State,
    ) -> NodeResult<()> {
        info!(
            node = self.name(),
            service = %runtime.service_info().service_name(),
            "initialising browser history ingestor"
        );

        config.validate()?;
        Self::bootstrap_streams_for_runtime(runtime).await?;

        let acquisition =
            Arc::new(runtime.acquisition_manager(RotationPolicy::default(), "browser-history")?);
        self.stage_context = Some(
            StageAsYouGoContext::from_runtime(runtime)
                .with_acquisition_manager(Arc::clone(&acquisition)),
        );
        self.acquisition = Some(acquisition);
        self.config = config;
        Ok(())
    }

    async fn scan_snapshot(
        &mut self,
        _state: &mut Self::State,
        _args: ScanArgs,
    ) -> NodeResult<ScanReport> {
        let started = std::time::Instant::now();
        let started_at = Timestamp::now();
        let successful_targets = self
            .config
            .dump_sources
            .iter()
            .map(ToString::to_string)
            .chain(self.config.sqlite_sources.iter().map(|source| source.path.to_string()))
            .collect();

        Ok(ScanReport {
            events_processed: 0,
            duration: started.elapsed(),
            final_checkpoint: Checkpoint::timestamp(Timestamp::now(), None),
            time_range: Some((started_at, Timestamp::now())),
            node_stats: HashMap::new(),
            successful_targets,
            failed_targets: Vec::new(),
            warnings: Vec::new(),
        })
    }

    async fn scan_historical(
        &mut self,
        state: &mut Self::State,
        _from: Checkpoint,
        until: TimeHorizon,
        _args: ScanArgs,
    ) -> NodeResult<ScanReport> {
        let started = std::time::Instant::now();
        let started_at = Timestamp::now();
        let outcome = self.run_import_pass(state, until.end_time()).await?;
        Ok(Self::scan_report(outcome, started_at, started.elapsed()))
    }

    async fn run_continuous(
        &mut self,
        state: &mut Self::State,
        _from: Checkpoint,
        mut shutdown_rx: watch::Receiver<bool>,
    ) -> NodeResult<ScanReport> {
        let started = std::time::Instant::now();
        let started_at = Timestamp::now();
        let mut accumulated = ImportPassOutcome::default();
        let mut interval = tokio::time::interval(Duration::from_secs(
            self.config.polling_interval_secs.as_secs(),
        ));

        loop {
            tokio::select! {
                changed = shutdown_rx.changed() => {
                    if changed.is_err() || *shutdown_rx.borrow() {
                        break;
                    }
                }
                _ = interval.tick() => {
                    let outcome = self.run_import_pass(state, None).await?;
                    accumulated.processed = accumulated.processed.saturating_add(outcome.processed);
                    accumulated.warnings.extend(outcome.warnings);
                    accumulated.successful_targets.extend(outcome.successful_targets);
                    accumulated.failed_targets.extend(outcome.failed_targets);
                }
            }
        }

        Ok(Self::scan_report(
            accumulated,
            started_at,
            started.elapsed(),
        ))
    }
}

fn is_appendable_dump(path: &Utf8PathBuf) -> bool {
    matches!(path.extension(), Some("jsonl" | "ndjson"))
}

#[cfg(test)]
mod tests {
    use super::{BrowserIngestorConfig, BrowserIngestorState, BrowserNode};
    use crate::sqlite_sources::BrowserSqliteFormat;
    use camino::Utf8PathBuf;
    use sinex_node_sdk::{IngestorNode, runtime::stream::ScanArgs};
    use xtask::sandbox::prelude::*;

    #[sinex_test]
    async fn default_browser_config_keeps_live_sqlite_sources_only() -> TestResult<()> {
        let config = BrowserIngestorConfig::default();
        assert!(
            config.dump_sources.is_empty(),
            "live defaults should not replay static dump roots on startup"
        );
        assert_eq!(config.sqlite_sources.len(), 2);
        assert_eq!(
            config.sqlite_sources[0].format,
            BrowserSqliteFormat::QutebrowserNative
        );
        assert_eq!(
            config.sqlite_sources[1].format,
            BrowserSqliteFormat::ChromiumHistory
        );
        Ok(())
    }

    #[sinex_test]
    async fn scan_snapshot_reports_sources_without_importing() -> TestResult<()> {
        let mut node = BrowserNode::default();
        node.config = BrowserIngestorConfig {
            dump_sources: vec![Utf8PathBuf::from("/tmp/browser-dump.jsonl")],
            sqlite_sources: Vec::new(),
            polling_interval_secs: sinex_primitives::Seconds::from_secs(30),
        };

        let report = node
            .scan_snapshot(&mut BrowserIngestorState::default(), ScanArgs::default())
            .await?;
        assert_eq!(report.events_processed, 0);
        assert_eq!(
            report.successful_targets,
            vec!["/tmp/browser-dump.jsonl".to_string()]
        );
        Ok(())
    }
}

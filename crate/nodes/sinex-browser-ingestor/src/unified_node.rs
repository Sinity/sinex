use crate::history_formats::{ParsedDumpFile, SUPPORTED_HISTORY_EXTENSIONS, parse_dump_file};
use crate::sqlite_sources::{
    BrowserSqliteFormat, BrowserSqliteSourceConfig, ensure_browser_sqlite_source,
    read_browser_sqlite_history,
};
use crate::visit::BrowserVisitRecord;
use camino::Utf8PathBuf;
use serde::{Deserialize, Serialize};
use sinex_node_sdk::stage_as_you_go::StageAsYouGoContext;
use sinex_node_sdk::{
    BatchImporterState, BufferedRecordMaterializer, BufferedRecordSourceHarness, EventTransport,
    ImportFileChangeKind, IngestorNode, NodeResult, RecordProcessingOutcome, RecordReadHorizon,
    RecordSources, RotationPolicy, SinexError, SourceRecordAnchor, SqliteRowCheckpoint,
    SqliteSnapshotCheckpointState, SqliteSnapshotLinker, SqliteSnapshotPolicy,
    SqliteSourceCheckpointState,
    acquisition_manager::{AcquisitionManager, BufferedAppendStreamWriterConfig},
    discover_importable_files_at_root,
    runtime::stream::{
        Checkpoint, ContinuousStart, NodeRuntimeState, ScanArgs, ScanReport, TimeHorizon,
    },
};
use sinex_primitives::{
    Seconds, Timestamp,
    events::{EventPayload, payloads::PageVisitedPayload},
    privacy::{self, ProcessingContext},
};
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::sync::watch;
use tracing::info;

const DEFAULT_POLLING_INTERVAL_SECS: u64 = 30;
const BROWSER_HISTORY_CHECKPOINT_DESCRIPTION: &str = "browser history source progress";

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
    #[serde(default)]
    pub sqlite_snapshots: SqliteSnapshotCheckpointState,
}

#[derive(Default)]
pub struct BrowserNode {
    config: BrowserIngestorConfig,
    stage_context: Option<StageAsYouGoContext>,
    acquisition: Option<Arc<AcquisitionManager>>,
    runtime: Option<NodeRuntimeState>,
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

    fn runtime(&self) -> NodeResult<&NodeRuntimeState> {
        self.runtime
            .as_ref()
            .ok_or_else(|| SinexError::lifecycle("browser runtime not initialized"))
    }

    fn stage_context(&self) -> NodeResult<&StageAsYouGoContext> {
        self.stage_context
            .as_ref()
            .ok_or_else(|| SinexError::lifecycle("browser stage context not initialized"))
    }

    fn materializer(&self, source_identifier: &str) -> NodeResult<BufferedRecordMaterializer> {
        Ok(BufferedRecordMaterializer::buffered(
            self.acquisition()?.clone(),
            source_identifier,
            BufferedAppendStreamWriterConfig::default(),
        ))
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

    async fn append_visit_material(
        materializer: &BufferedRecordMaterializer,
        visit: &BrowserVisitRecord,
    ) -> NodeResult<SourceRecordAnchor> {
        let mut material_bytes = visit.material_bytes.clone();
        material_bytes.push(b'\n');
        materializer
            .append_stable_bytes(material_bytes)
            .await
            .map_err(|error| {
                SinexError::service("failed to append browser history source record")
                    .with_source(error)
            })
    }

    async fn emit_visit(
        &self,
        visit: BrowserVisitRecord,
        materializer: &BufferedRecordMaterializer,
    ) -> NodeResult<()> {
        let anchor = Self::append_visit_material(materializer, &visit).await?;
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
            .from_material(anchor.material_id)
            .with_offset_start(anchor.offset_start)?
            .with_offset_end(anchor.offset_end)?
            .build()?
            .to_json_event()
            .map_err(|error| {
                SinexError::serialization("failed to serialize page.visited event")
                    .with_std_error(&error)
            })?;

        self.stage_context()?
            .emit_event_with_provenance(
                event,
                anchor.material_id,
                Some(anchor.offset_start),
                Some(anchor.offset_end),
            )
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

            let materializer = self.materializer(file.path.as_str())?;
            let import_result: NodeResult<()> = async {
                for visit in visits {
                    self.emit_visit(visit, &materializer).await?;
                    processed = processed.saturating_add(1);
                }
                Ok(())
            }
            .await;
            if let Err(error) = materializer.finalize("browser-dump-import").await {
                return Err(
                    SinexError::service("failed to finalize browser dump material")
                        .with_source(error),
                );
            }
            import_result?;

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
        let source_config = source.clone();
        let record_source = RecordSources::sqlite(
            source.path.clone(),
            checkpoint_key.clone(),
            move |_path, from_row_id, end_time| {
                read_browser_sqlite_history(&source_config, from_row_id, end_time)
            },
            |visit: &BrowserVisitRecord| {
                visit
                    .db_row_id
                    .and_then(|row_id| i64::try_from(row_id).ok())
                    .unwrap_or_default()
            },
        )
        .with_snapshot_policy(SqliteSnapshotPolicy::audit_default());
        let harness = BufferedRecordSourceHarness::buffered_default(
            record_source,
            self.acquisition()?.clone(),
        );
        let mut checkpoint = SqliteRowCheckpoint::new(state.sqlite_sources.cursor(&checkpoint_key));
        let mut report = harness
            .read_process_lenient_with_snapshot(
                &mut checkpoint,
                historical_end_time.map_or(RecordReadHorizon::Unbounded, RecordReadHorizon::Until),
                state.sqlite_snapshots.state_mut(checkpoint_key.clone()),
                self.acquisition()?,
                |visit, ctx| async move {
                    self.emit_visit(visit, ctx.materializer())
                        .await
                        .map(|()| RecordProcessingOutcome::Processed)
                },
                |_| sinex_node_sdk::RecordWarningDisposition::Retry,
            )
            .await?;

        harness
            .finalize_with_snapshot_evidence(
                "browser-sqlite-import",
                &mut report,
                Some(SqliteSnapshotLinker::new(self.runtime()?.db_pool())),
            )
            .await?;
        if let Some(error) = report.warnings.into_iter().next() {
            return Err(error);
        }
        state
            .sqlite_sources
            .set_cursor(checkpoint_key, checkpoint.row_id);

        Ok((report.processed_records as u64, warnings))
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

    fn apply_historical_checkpoint(
        state: &mut BrowserIngestorState,
        checkpoint: &Checkpoint,
    ) -> NodeResult<()> {
        match checkpoint {
            Checkpoint::None => {
                *state = BrowserIngestorState::default();
                Ok(())
            }
            Checkpoint::External { position, .. } => {
                *state = serde_json::from_value(position.clone()).map_err(|error| {
                    SinexError::serialization("failed to parse browser history checkpoint state")
                        .with_std_error(&error)
                })?;
                Ok(())
            }
            _ => Err(SinexError::checkpoint(
                "browser history requires an external source-progress checkpoint",
            )
            .with_context("checkpoint", checkpoint.description())),
        }
    }

    fn checkpoint_from_state(state: &BrowserIngestorState) -> NodeResult<Checkpoint> {
        let position = serde_json::to_value(state).map_err(|error| {
            SinexError::serialization("failed to encode browser history checkpoint state")
                .with_std_error(&error)
        })?;
        Ok(Checkpoint::external(
            position,
            BROWSER_HISTORY_CHECKPOINT_DESCRIPTION,
        ))
    }

    fn scan_report(
        outcome: ImportPassOutcome,
        started_at: Timestamp,
        duration: Duration,
        final_checkpoint: Checkpoint,
    ) -> ScanReport {
        let finished_at = Timestamp::now();
        ScanReport {
            events_processed: outcome.processed,
            duration,
            final_checkpoint,
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
        self.runtime = Some(runtime.clone());
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
            .chain(
                self.config
                    .sqlite_sources
                    .iter()
                    .map(|source| source.path.to_string()),
            )
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
        from: Checkpoint,
        until: TimeHorizon,
        _args: ScanArgs,
    ) -> NodeResult<ScanReport> {
        let started = std::time::Instant::now();
        let started_at = Timestamp::now();
        Self::apply_historical_checkpoint(state, &from)?;
        let outcome = self.run_import_pass(state, until.end_time()).await?;
        let final_checkpoint = Self::checkpoint_from_state(state)?;
        Ok(Self::scan_report(
            outcome,
            started_at,
            started.elapsed(),
            final_checkpoint,
        ))
    }

    async fn run_continuous(
        &mut self,
        state: &mut Self::State,
        _start: ContinuousStart,
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
            Self::checkpoint_from_state(state)?,
        ))
    }
}

fn is_appendable_dump(path: &Utf8PathBuf) -> bool {
    matches!(path.extension(), Some("jsonl" | "ndjson"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sqlite_sources::{BrowserSqliteFormat, BrowserSqliteSourceConfig};
    use camino::Utf8PathBuf;
    use serde::Serialize;
    use serde_json::json;
    use sinex_db::{DbPool, DbPoolExt};
    use sinex_node_sdk::{
        IngestorNodeAdapter, NatsPublisher, NodeRunner, ShutdownConfig, runtime::stream::ScanArgs,
    };
    use sinex_primitives::{
        Pagination, Uuid,
        domain::{EventSource, EventType},
        events::Provenance,
    };
    use std::sync::Arc;
    use xtask::sandbox::{
        TestContext, TestIngestdConfig, TestResult, prelude::*, start_test_ingestd_with_config,
        timing::Timeouts,
    };

    fn raw_node_config<T: Serialize>(config: &T) -> TestResult<HashMap<String, serde_json::Value>> {
        let value = serde_json::to_value(config)?;
        let serde_json::Value::Object(object) = value else {
            return Err(color_eyre::eyre::eyre!(
                "node config must serialize to a JSON object"
            ));
        };
        Ok(object.into_iter().collect())
    }

    fn tune_batcher_for_runtime_proof(
        config: &mut HashMap<String, serde_json::Value>,
        service_prefix: &str,
    ) -> String {
        let suffix = Uuid::now_v7();
        let service_name = format!("{service_prefix}-{suffix}");
        config.insert("batch_size".to_string(), json!(1));
        config.insert("batch_timeout_ms".to_string(), json!(20));
        config.insert(
            "consumer_group".to_string(),
            json!(format!("proof-{suffix}")),
        );
        service_name
    }

    async fn wait_for_source_material_consumer(ctx: &TestContext) -> TestResult<()> {
        let env = sinex_primitives::environment::environment();
        let nats = ctx.nats_handle()?;
        let js = nats.jetstream_with_client(ctx.nats_client());
        let stream = env.nats_stream_name("SOURCE_MATERIAL");
        nats.wait_for_consumer_on_stream(
            &js,
            &stream,
            std::time::Duration::from_secs(Timeouts::STANDARD),
        )
        .await?;
        Ok(())
    }

    async fn wait_for_event_count(
        pool: DbPool,
        source: &'static str,
        event_type: &'static str,
        expected_count: i64,
    ) -> TestResult<()> {
        let source = EventSource::new(source)?;
        let event_type = EventType::new(event_type)?;
        xtask::sandbox::timing::WaitHelpers::wait_for_condition(
            move || {
                let pool = pool.clone();
                let source = source.clone();
                let event_type = event_type.clone();
                async move {
                    let count = pool
                        .events()
                        .count_by_source_and_event_type(&source, &event_type)
                        .await
                        .map_err(|error| color_eyre::eyre::eyre!("database error: {error}"))?;
                    Ok::<bool, color_eyre::eyre::Report>(count == expected_count)
                }
            },
            Timeouts::STANDARD,
        )
        .await
    }

    async fn persisted_browser_events(
        pool: &DbPool,
    ) -> TestResult<Vec<sinex_primitives::events::Event<serde_json::Value>>> {
        let source = EventSource::new("webhistory")?;
        let event_type = EventType::new("page.visited")?;
        let mut events = pool
            .events()
            .get_by_source(&source, Pagination::new(Some(100), None))
            .await?;
        events.retain(|event| event.event_type == event_type);
        events.sort_by_key(|event| (event.ts_orig, event.id));
        Ok(events)
    }

    fn assert_material_provenance_rows(
        rows: &[sinex_primitives::events::Event<serde_json::Value>],
    ) -> TestResult<()> {
        for (index, event) in rows.iter().enumerate() {
            match event.provenance() {
                Provenance::Material { anchor_byte, .. } if *anchor_byte >= 0 => {}
                other => {
                    return Err(color_eyre::eyre::eyre!(
                        "browser history row {index} has invalid provenance: {other:?}"
                    ));
                }
            }
        }
        Ok(())
    }

    async fn write_browser_dump_fixtures(root: &Utf8PathBuf) -> TestResult<()> {
        tokio::fs::create_dir_all(root.as_std_path()).await?;
        tokio::fs::write(
            root.join("chrome_history.json").as_std_path(),
            r#"[{"visitId":"chrome-1","url":"https://example.com/chrome?utm_source=drop&keep=1","title":"Chrome example","visitTime":1700000004000,"transition":"link"}]"#,
        )
        .await?;
        tokio::fs::write(
            root.join("firefox_history.jsonl").as_std_path(),
            "{\"url\":\"https://example.com/firefox\",\"title\":\"Firefox example\",\"iso_time\":\"2023-11-14T22:13:25Z\",\"transition\":\"typed\"}\n",
        )
        .await?;
        tokio::fs::write(
            root.join("qutebrowser_dump.ndjson").as_std_path(),
            "{\"url\":\"https://example.com/qute-dump\",\"title\":\"Qutebrowser dump\",\"time\":1700000006}\n",
        )
        .await?;
        tokio::fs::write(
            root.join("edge_history.csv").as_std_path(),
            "DateTime,NavigatedToUrl,PageTitle\n2023-11-14T22:13:27Z,https://example.com/edge,Edge example\n",
        )
        .await?;
        Ok(())
    }

    fn write_qutebrowser_fixture(path: &Utf8PathBuf) -> TestResult<()> {
        let conn = rusqlite::Connection::open(path.as_std_path())?;
        conn.execute_batch(
            "
            CREATE TABLE History (
                url TEXT NOT NULL,
                title TEXT NOT NULL,
                atime INTEGER NOT NULL,
                redirect INTEGER NOT NULL
            );
            INSERT INTO History (url, title, atime, redirect) VALUES
                ('https://example.com/qute-one?utm_source=drop', 'Qute one', 1700000000, 0),
                ('https://example.com/qute-two', 'Qute two', 1700000001, 1);
            ",
        )?;
        Ok(())
    }

    fn chromium_timestamp_micros(unix_secs: i64) -> i64 {
        (11_644_473_600_i64 + unix_secs) * 1_000_000_i64
    }

    fn write_chromium_fixture(path: &Utf8PathBuf) -> TestResult<()> {
        let conn = rusqlite::Connection::open(path.as_std_path())?;
        let first_visit = chromium_timestamp_micros(1_700_000_002);
        let second_visit = chromium_timestamp_micros(1_700_000_003);
        conn.execute_batch(&format!(
            "
            CREATE TABLE urls (
                id INTEGER PRIMARY KEY,
                url TEXT NOT NULL,
                title TEXT NOT NULL
            );
            CREATE TABLE visits (
                id INTEGER PRIMARY KEY,
                url INTEGER NOT NULL,
                visit_time INTEGER NOT NULL,
                external_referrer_url TEXT,
                transition INTEGER NOT NULL,
                visit_duration INTEGER NOT NULL
            );
            INSERT INTO urls (id, url, title) VALUES
                (1, 'https://example.com/chromium-one?utm_campaign=drop', 'Chromium one'),
                (2, 'https://example.com/chromium-two', 'Chromium two');
            INSERT INTO visits (id, url, visit_time, external_referrer_url, transition, visit_duration) VALUES
                (1, 1, {first_visit}, NULL, 805306368, 250000),
                (2, 2, {second_visit}, 'https://referrer.example', 268435456, 1000000);
            "
        ))?;
        Ok(())
    }

    fn browser_count(
        rows: &[sinex_primitives::events::Event<serde_json::Value>],
        browser: &str,
    ) -> usize {
        rows.iter()
            .filter(|event| {
                event
                    .payload
                    .get("browser")
                    .and_then(serde_json::Value::as_str)
                    == Some(browser)
            })
            .count()
    }

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

    #[sinex_test]
    async fn browser_historical_checkpoint_rejects_non_external_state() -> TestResult<()> {
        let mut state = BrowserIngestorState::default();
        let checkpoint = Checkpoint::timestamp(Timestamp::now(), None);
        let error = BrowserNode::apply_historical_checkpoint(&mut state, &checkpoint)
            .expect_err("timestamp checkpoint should not drive browser source progress");

        assert!(
            error
                .to_string()
                .contains("requires an external source-progress checkpoint"),
            "unexpected error: {error}"
        );
        Ok(())
    }

    #[sinex_test]
    async fn scan_historical_persists_browser_history_through_node_runtime(
        ctx: TestContext,
    ) -> TestResult<()> {
        let ctx = ctx.with_nats().dedicated().await?;
        let temp_dir = tempfile::tempdir()?;

        let dump_root =
            Utf8PathBuf::from_path_buf(temp_dir.path().join("browser-dumps")).map_err(|path| {
                color_eyre::eyre::eyre!("browser dump root is not UTF-8: {}", path.display())
            })?;
        write_browser_dump_fixtures(&dump_root).await?;

        let qutebrowser_path = Utf8PathBuf::from_path_buf(temp_dir.path().join("qute.sqlite"))
            .map_err(|path| {
                color_eyre::eyre::eyre!("qutebrowser fixture path is not UTF-8: {}", path.display())
            })?;
        write_qutebrowser_fixture(&qutebrowser_path)?;

        let chromium_path = Utf8PathBuf::from_path_buf(temp_dir.path().join("ChromiumHistory"))
            .map_err(|path| {
                color_eyre::eyre::eyre!("Chromium fixture path is not UTF-8: {}", path.display())
            })?;
        write_chromium_fixture(&chromium_path)?;

        let nats = ctx.nats_handle()?;
        let ingest_config = TestIngestdConfig {
            nats: nats.connection_config(),
            database_url: ctx.database_url().to_string(),
            work_dir: Some(temp_dir.path().join("ingestd")),
            ..Default::default()
        };
        let mut ingest_handle = start_test_ingestd_with_config(ingest_config, Some(&ctx)).await?;
        wait_for_source_material_consumer(&ctx).await?;

        let config = BrowserIngestorConfig {
            dump_sources: vec![dump_root],
            sqlite_sources: vec![
                BrowserSqliteSourceConfig {
                    path: qutebrowser_path,
                    browser: "qutebrowser".to_string(),
                    format: BrowserSqliteFormat::QutebrowserNative,
                },
                BrowserSqliteSourceConfig {
                    path: chromium_path,
                    browser: "chromium".to_string(),
                    format: BrowserSqliteFormat::ChromiumHistory,
                },
            ],
            polling_interval_secs: Seconds::from_secs(1),
        };

        let mut raw_config = raw_node_config(&config)?;
        let service_name =
            tune_batcher_for_runtime_proof(&mut raw_config, "browser-historical-runtime-proof");
        let publisher = Arc::new(NatsPublisher::new(ctx.nats_client()));
        let adapter =
            IngestorNodeAdapter::new(BrowserNode::default()).with_shutdown_config(ShutdownConfig {
                checkpoint_path: Some(
                    temp_dir
                        .path()
                        .join("browser-runtime-proof.checkpoint.json"),
                ),
                ..ShutdownConfig::default()
            });
        let mut runner = NodeRunner::new(adapter);
        runner
            .initialize_with_transport(
                service_name,
                raw_config,
                Some(ctx.pool.clone()),
                EventTransport::Nats(publisher),
                temp_dir.path().join("runner"),
                false,
            )
            .await?;

        let report = runner
            .run_scan(
                Checkpoint::None,
                TimeHorizon::Historical {
                    end_time: Timestamp::now(),
                },
                ScanArgs::default(),
            )
            .await?;
        assert_eq!(report.events_processed, 8);
        assert!(matches!(
            report.final_checkpoint,
            Checkpoint::External { .. }
        ));

        wait_for_event_count(ctx.pool.clone(), "webhistory", "page.visited", 8).await?;
        let rows = persisted_browser_events(&ctx.pool).await?;
        assert_eq!(rows.len(), 8);
        assert_material_provenance_rows(&rows)?;
        assert_eq!(browser_count(&rows, "qutebrowser"), 3);
        assert_eq!(browser_count(&rows, "chromium"), 2);
        assert_eq!(browser_count(&rows, "chrome"), 1);
        assert_eq!(browser_count(&rows, "firefox"), 1);
        assert_eq!(browser_count(&rows, "edge"), 1);
        assert!(
            rows.iter().any(|event| {
                event
                    .payload
                    .get("normalized_url")
                    .and_then(serde_json::Value::as_str)
                    == Some("https://example.com/chrome?keep=1")
            }),
            "Chrome dump row should preserve non-tracking query params and drop tracking params"
        );

        let mut rerun_config = raw_node_config(&config)?;
        let rerun_service_name = tune_batcher_for_runtime_proof(
            &mut rerun_config,
            "browser-historical-runtime-proof-rerun",
        );
        let rerun_publisher = Arc::new(NatsPublisher::new(ctx.nats_client()));
        let rerun_adapter =
            IngestorNodeAdapter::new(BrowserNode::default()).with_shutdown_config(ShutdownConfig {
                checkpoint_path: Some(
                    temp_dir
                        .path()
                        .join("browser-runtime-proof-rerun.checkpoint.json"),
                ),
                ..ShutdownConfig::default()
            });
        let mut rerun_runner = NodeRunner::new(rerun_adapter);
        rerun_runner
            .initialize_with_transport(
                rerun_service_name,
                rerun_config,
                Some(ctx.pool.clone()),
                EventTransport::Nats(rerun_publisher),
                temp_dir.path().join("rerun-runner"),
                false,
            )
            .await?;

        let rerun_report = rerun_runner
            .run_scan(
                report.final_checkpoint.clone(),
                TimeHorizon::Historical {
                    end_time: Timestamp::now(),
                },
                ScanArgs::default(),
            )
            .await?;
        assert_eq!(rerun_report.events_processed, 0);
        assert_eq!(persisted_browser_events(&ctx.pool).await?.len(), 8);

        rerun_runner.shutdown().await?;
        runner.shutdown().await?;
        ingest_handle.stop().await?;
        Ok(())
    }
}

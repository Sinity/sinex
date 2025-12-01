#![doc = include_str!("../docs/overview.md")]

//! Terminal processor that tails configured history files and emits structured
//! command events. Each discovered command is captured as a source material via
//! `AcquisitionManager` and published to JetStream, while the structured event
//! is emitted through the shared Stage-as-You-Go channel.

use async_trait::async_trait;
use camino::Utf8PathBuf;
use chrono::{DateTime, Utc};
use color_eyre::eyre;
use serde::{Deserialize, Serialize};
use serde_json;
use sinex_core::{
    types::{domain::SanitizedPath, validate_path},
    Event as CoreEvent, Id, Provenance, Ulid,
};
use sinex_processor_runtime::{
    CoverageAnalysis, ExplorationProvider, ExportFormat, IngestionHistoryEntry, SourceState,
};
use sinex_satellite_sdk::{
    acquisition_manager::{AcquisitionManager, RotationPolicy},
    stage_as_you_go::StageAsYouGoContext,
    stream_processor::{
        Checkpoint, ProcessorCapabilities, ProcessorInitContext, ProcessorRuntimeState,
        ProcessorType, ScanArgs, ScanEstimate, ScanReport, ServiceInfo, StatefulStreamProcessor,
        TimeHorizon,
    },
    SatelliteError, SatelliteResult,
};
use std::{collections::HashMap, path::PathBuf, sync::Arc, time::Duration};
use tokio::{
    fs,
    io::{AsyncReadExt, AsyncSeekExt},
    sync::Mutex,
};
use tracing::{debug, info, instrument, warn};
use validator::ValidationError;

const MATERIAL_REASON_HISTORY: &str = "terminal-history";

/// Configuration for a shell history source.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistorySourceConfig {
    pub path: Utf8PathBuf,

    /// Shell type (bash, zsh, fish, etc.)
    pub shell: String,
}

fn validate_history_path(path: &Utf8PathBuf) -> Result<(), ValidationError> {
    validate_path(path.as_str())
        .map(|_| ())
        .map_err(|_| ValidationError::new("invalid_history_path"))
}

/// Terminal processor configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TerminalConfig {
    /// Shell history sources to monitor.
    pub history_sources: Vec<HistorySourceConfig>,

    /// Polling interval for checking file changes (seconds)
    pub polling_interval_secs: u64,

    /// Maximum capture size per command (bytes)
    pub max_capture_bytes: u64,
}

impl Default for TerminalConfig {
    fn default() -> Self {
        let home = dirs::home_dir()
            .and_then(|p| Utf8PathBuf::from_path_buf(p).ok())
            .unwrap_or_else(|| Utf8PathBuf::from("/tmp"));

        let default_sources = vec![
            HistorySourceConfig {
                path: home.join(".bash_history"),
                shell: "bash".to_string(),
            },
            HistorySourceConfig {
                path: home.join(".zsh_history"),
                shell: "zsh".to_string(),
            },
        ];

        Self {
            history_sources: default_sources,
            polling_interval_secs: 15,
            max_capture_bytes: 32 * 1024,
        }
    }
}

impl TerminalConfig {
    pub fn validate_config(&self) -> Result<(), String> {
        if self.history_sources.is_empty() {
            return Err("At least one history source must be configured".to_string());
        }

        for source in &self.history_sources {
            validate_history_path(&source.path)
                .map_err(|_| "Invalid history file path".to_string())?;
            if source.shell.trim().is_empty() {
                return Err("Shell type cannot be empty".to_string());
            }
        }

        if !(1..=3600).contains(&self.polling_interval_secs) {
            return Err("Polling interval must be between 1 and 3600 seconds".to_string());
        }

        if !(64..=1 * 1024 * 1024).contains(&self.max_capture_bytes) {
            return Err("Max capture bytes must be between 64B and 1MB".to_string());
        }

        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct TerminalState {
    pub captured_at: DateTime<Utc>,
    pub monitored_sources: Vec<Utf8PathBuf>,
    pub host: String,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct HistoryState {
    offset_bytes: u64,
    line_number: u64,
}

#[derive(Clone)]
struct HistoryWatcherContext {
    acquisition: Arc<AcquisitionManager>,
    stage_context: StageAsYouGoContext,
    shell: String,
    path: Utf8PathBuf,
    max_capture_bytes: u64,
    polling_interval: Duration,
    state_path: Option<PathBuf>,
    #[cfg_attr(not(test), allow(dead_code))]
    processed_commands: Option<Arc<Mutex<Vec<String>>>>,
}

impl HistoryWatcherContext {
    async fn monitor(self) {
        let mut offset_bytes: u64 = 0;
        let mut line_number: u64 = 0;

        if let Some(state) = self.load_state().await {
            offset_bytes = state.offset_bytes;
            line_number = state.line_number;
            debug!(
                path = %self.path,
                offset = offset_bytes,
                line_number,
                "Restored terminal watcher state"
            );
        }

        loop {
            self.poll_history_once(&mut offset_bytes, &mut line_number)
                .await;
            tokio::time::sleep(self.polling_interval).await;
        }
    }

    async fn load_state(&self) -> Option<HistoryState> {
        let path = self.state_path.as_ref()?;
        match fs::read(path).await {
            Ok(bytes) => match serde_json::from_slice::<HistoryState>(&bytes) {
                Ok(state) => Some(state),
                Err(e) => {
                    warn!("Failed to decode history watcher state {:?}: {}", path, e);
                    None
                }
            },
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => None,
            Err(err) => {
                warn!("Failed to load history watcher state {:?}: {}", path, err);
                None
            }
        }
    }

    async fn persist_state(&self, offset_bytes: u64, line_number: u64) {
        let path = match &self.state_path {
            Some(path) => path,
            None => return,
        };

        let state = HistoryState {
            offset_bytes,
            line_number,
        };

        match serde_json::to_vec_pretty(&state) {
            Ok(serialized) => {
                if let Err(e) = fs::write(path, serialized).await {
                    warn!("Failed to persist history watcher state {:?}: {}", path, e);
                }
            }
            Err(e) => warn!("Failed to serialize history watcher state: {}", e),
        }
    }

    async fn read_new_segment(&self, offset: u64) -> std::io::Result<String> {
        use std::io::SeekFrom;

        let mut file = tokio::fs::File::open(&self.path).await?;
        file.seek(SeekFrom::Start(offset)).await?;

        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer).await?;

        Ok(String::from_utf8_lossy(&buffer).to_string())
    }

    async fn poll_history_once(&self, offset_bytes: &mut u64, line_number: &mut u64) {
        match fs::metadata(&self.path).await {
            Ok(metadata) => {
                let file_size = metadata.len();

                if file_size < *offset_bytes {
                    debug!(
                        path = %self.path,
                        previous_offset = *offset_bytes,
                        new_size = file_size,
                        "History file truncated; resetting offsets"
                    );
                    *offset_bytes = file_size;
                    *line_number = 0;
                    self.persist_state(*offset_bytes, *line_number).await;
                    return;
                }

                if file_size == *offset_bytes {
                    return;
                }

                match self.read_new_segment(*offset_bytes).await {
                    Ok(new_segment) => {
                        if new_segment.is_empty() {
                            return;
                        }

                        let mut consumed_bytes: u64 = 0;

                        for line in new_segment.split_inclusive('\n') {
                            if !line.ends_with('\n') && new_segment.ends_with(line) {
                                break;
                            }

                            let trimmed = line.trim_end_matches('\n');
                            consumed_bytes += line.len() as u64;

                            if trimmed.is_empty() {
                                continue;
                            }

                            *line_number += 1;

                            if let Err(e) = process_command(self, trimmed, *line_number).await {
                                warn!("Failed to process history entry from {}: {}", self.path, e);
                            }
                        }

                        if consumed_bytes > 0 {
                            *offset_bytes = offset_bytes.saturating_add(consumed_bytes);
                            self.persist_state(*offset_bytes, *line_number).await;
                        }
                    }
                    Err(e) => warn!("History watcher unable to read {}: {}", self.path, e),
                }
            }
            Err(e) => {
                warn!("History watcher unable to stat {}: {}", self.path, e);
            }
        }
    }
}

async fn process_command(
    ctx: &HistoryWatcherContext,
    command: &str,
    line_number: u64,
) -> SatelliteResult<()> {
    let bytes = command.as_bytes();

    if bytes.len() as u64 > ctx.max_capture_bytes {
        warn!(
            "Skipping command exceeding capture limit ({} bytes > {} limit)",
            bytes.len(),
            ctx.max_capture_bytes
        );
        return Ok(());
    }

    if let Some(commands) = &ctx.processed_commands {
        commands.lock().await.push(command.to_string());
    }

    let mut handle = ctx
        .acquisition
        .begin_material(ctx.path.as_str())
        .await
        .map_err(|e| SatelliteError::General(eyre::eyre!("Failed to begin material: {}", e)))?;
    let material_id = handle.material_id;

    ctx.acquisition
        .append_slice(&mut handle, bytes)
        .await
        .map_err(|e| SatelliteError::General(eyre::eyre!("Failed to append slice: {}", e)))?;

    ctx.acquisition
        .finalize(handle, MATERIAL_REASON_HISTORY)
        .await
        .map_err(|e| SatelliteError::General(eyre::eyre!("Failed to finalize material: {}", e)))?;

    let payload = sinex_core::types::events::payloads::shell::HistoryCommandImportedPayload {
        command: command.to_string(),
        timestamp: Some(Utc::now()),
        shell_type: ctx.shell.clone(),
        source_file: ctx.path.to_string(),
        line_number: Some(line_number as u32),
    };

    let provenance = Provenance::Material {
        id: Id::from_ulid(material_id),
        anchor_byte: 0,
        offset_start: Some(0),
        offset_end: Some(bytes.len() as i64),
        offset_kind: sinex_core::OffsetKind::Byte,
    };

    let event = CoreEvent::create(
        sinex_core::types::domain::EventSource::from_static("shell.history"),
        sinex_core::types::domain::EventType::from_static("command.imported"),
        serde_json::to_value(payload)
            .map_err(|e| SatelliteError::General(eyre::eyre!("Failed to encode payload: {}", e)))?,
        provenance,
    );

    let mut event = event;
    event.id = Some(Id::from_ulid(Ulid::new()));

    ctx.stage_context
        .emit_event_with_provenance(event, material_id, Some(0), Some(bytes.len() as i64))
        .await
        .map(|_| ())
        .map_err(|e| {
            SatelliteError::General(eyre::eyre!("Failed to emit terminal event: {}", e))
        })?;

    Ok(())
}

/// Terminal processor that monitors history files.
pub struct TerminalProcessor {
    config: TerminalConfig,
    stage_context: Option<StageAsYouGoContext>,
    watch_handles: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>,
    runtime: Option<ProcessorRuntimeState>,
    state_dir: Option<PathBuf>,
}

impl TerminalProcessor {
    pub fn new() -> Self {
        Self {
            config: TerminalConfig::default(),
            stage_context: None,
            watch_handles: Arc::new(Mutex::new(Vec::new())),
            runtime: None,
            state_dir: None,
        }
    }

    pub fn with_config(config: TerminalConfig) -> Self {
        Self {
            config,
            stage_context: None,
            watch_handles: Arc::new(Mutex::new(Vec::new())),
            runtime: None,
            state_dir: None,
        }
    }

    pub fn config(&self) -> &TerminalConfig {
        &self.config
    }

    fn runtime(&self) -> SatelliteResult<&ProcessorRuntimeState> {
        self.runtime.as_ref().ok_or_else(|| {
            SatelliteError::General(eyre::eyre!(
                "Terminal processor runtime not initialised prior to scan"
            ))
        })
    }

    fn service_info(&self) -> SatelliteResult<&ServiceInfo> {
        Ok(self.runtime()?.service_info())
    }

    async fn initialise_from_runtime(
        &mut self,
        config: TerminalConfig,
        runtime: ProcessorRuntimeState,
    ) -> SatelliteResult<()> {
        let service_info = runtime.service_info();
        info!(
            processor = self.processor_name(),
            service = %service_info.service_name(),
            "Initialising terminal processor"
        );

        config.validate_config().map_err(|e| {
            SatelliteError::General(eyre::eyre!(
                "Terminal configuration validation failed: {}",
                e
            ))
        })?;

        let publisher = match runtime.transport() {
            sinex_satellite_sdk::event_processor::EventTransport::Nats(publisher) => {
                publisher.clone()
            }
        };

        AcquisitionManager::bootstrap_streams(publisher.nats_client())
            .await
            .map_err(SatelliteError::from)?;

        let mut state_dir = service_info.work_dir().clone();
        state_dir.push("terminal-history");

        if let Err(e) = fs::create_dir_all(&state_dir).await {
            return Err(SatelliteError::General(eyre::eyre!(
                "Failed to create terminal state directory {}: {}",
                state_dir.display(),
                e
            )));
        }

        self.state_dir = Some(state_dir);
        self.stage_context = Some(StageAsYouGoContext::from_runtime(&runtime));
        self.runtime = Some(runtime);
        self.config = config;
        self.watch_handles = Arc::new(Mutex::new(Vec::new()));

        Ok(())
    }

    fn build_history_contexts(&self) -> SatelliteResult<Vec<HistoryWatcherContext>> {
        let runtime = self.runtime()?;

        let stage = self
            .stage_context
            .clone()
            .ok_or_else(|| SatelliteError::General(eyre::eyre!("Stage context not initialised")))?;

        let state_dir = self.state_dir.clone();
        let mut contexts = Vec::new();
        for source in &self.config.history_sources {
            let acquisition = runtime.acquisition_manager(
                RotationPolicy::default(),
                "terminal-history",
                source.path.to_string(),
            )?;

            let state_path = state_dir.as_ref().map(|dir| {
                let hash = blake3::hash(source.path.as_str().as_bytes())
                    .to_hex()
                    .to_string();
                dir.join(format!("{}.json", hash))
            });

            contexts.push(HistoryWatcherContext {
                acquisition: Arc::new(acquisition),
                stage_context: stage.clone(),
                shell: source.shell.clone(),
                path: source.path.clone(),
                max_capture_bytes: self.config.max_capture_bytes,
                polling_interval: Duration::from_secs(self.config.polling_interval_secs),
                state_path,
                processed_commands: None,
            });
        }

        Ok(contexts)
    }
}

impl Default for TerminalProcessor {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl StatefulStreamProcessor for TerminalProcessor {
    type Config = TerminalConfig;

    #[instrument(skip(self, init), fields(processor = "terminal", service = %init.service_info().service_name()))]
    async fn initialize(
        &mut self,
        init: ProcessorInitContext<Self::Config>,
    ) -> SatelliteResult<()> {
        let (config, runtime) = init.into_runtime();
        self.initialise_from_runtime(config, runtime).await
    }

    async fn scan(
        &mut self,
        from: Checkpoint,
        until: TimeHorizon,
        _args: ScanArgs,
    ) -> SatelliteResult<ScanReport> {
        match until {
            TimeHorizon::Snapshot => {
                let service_info = self.service_info()?;
                let state = TerminalState {
                    captured_at: Utc::now(),
                    monitored_sources: self
                        .config
                        .history_sources
                        .iter()
                        .map(|src| src.path.clone())
                        .collect(),
                    host: service_info.host().to_string(),
                };

                debug!(
                    monitored = state.monitored_sources.len(),
                    "Terminal snapshot captured"
                );

                Ok(ScanReport {
                    events_processed: 0,
                    duration: std::time::Duration::from_millis(0),
                    final_checkpoint: from,
                    time_range: None,
                    processor_stats: HashMap::new(),
                    successful_targets: vec!["snapshot".to_string()],
                    failed_targets: Vec::new(),
                    warnings: Vec::new(),
                })
            }
            TimeHorizon::Historical { .. } => {
                warn!("Historical replay is not supported by the terminal watcher");
                Ok(ScanReport {
                    events_processed: 0,
                    duration: std::time::Duration::from_millis(0),
                    final_checkpoint: from,
                    time_range: None,
                    processor_stats: HashMap::new(),
                    successful_targets: Vec::new(),
                    failed_targets: Vec::new(),
                    warnings: vec!["Historical mode is not supported".to_string()],
                })
            }
            TimeHorizon::Continuous => {
                let contexts = self.build_history_contexts()?;

                let mut guard = self.watch_handles.lock().await;
                for watch_ctx in contexts {
                    let handle = tokio::spawn(watch_ctx.clone().monitor());
                    guard.push(handle);
                }

                info!(
                    watches = guard.len(),
                    "Terminal watcher monitoring history sources"
                );

                futures::future::pending::<()>().await;
                unreachable!()
            }
        }
    }

    fn processor_name(&self) -> &str {
        "terminal-watcher"
    }

    fn processor_type(&self) -> ProcessorType {
        ProcessorType::Ingestor
    }

    fn capabilities(&self) -> ProcessorCapabilities {
        ProcessorCapabilities {
            supports_snapshot: true,
            supports_historical: false,
            supports_continuous: true,
            ..ProcessorCapabilities::default()
        }
    }

    async fn current_checkpoint(&self) -> SatelliteResult<Checkpoint> {
        Ok(Checkpoint::None)
    }

    async fn estimate_scan_scope(
        &self,
        _from: &Checkpoint,
        _until: &TimeHorizon,
        _args: &ScanArgs,
    ) -> SatelliteResult<ScanEstimate> {
        Ok(ScanEstimate {
            estimated_events: (self.config.history_sources.len() as u64) * 100,
            estimated_duration: std::time::Duration::from_secs(self.config.polling_interval_secs),
            estimated_data_size: self.config.max_capture_bytes
                * (self.config.history_sources.len() as u64),
            estimated_targets: self.config.history_sources.len() as u64,
            warnings: vec!["Terminal history estimation uses polling heuristics".to_string()],
            confidence: 0.25,
        })
    }

    async fn shutdown(&mut self) -> SatelliteResult<()> {
        let mut guard = self.watch_handles.lock().await;
        for handle in guard.drain(..) {
            handle.abort();
        }
        info!("Terminal watcher shutdown complete");
        Ok(())
    }
}

impl ExplorationProvider for TerminalProcessor {
    fn get_source_state(&self) -> color_eyre::eyre::Result<SourceState> {
        Ok(SourceState {
            description: "Tails shell history files and emits command events".to_string(),
            last_updated: Utc::now(),
            total_items: Some(self.config.history_sources.len() as u64),
            metadata: HashMap::from([(
                "polling_interval_secs".to_string(),
                serde_json::Value::Number(serde_json::Number::from(
                    self.config.polling_interval_secs,
                )),
            )]),
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
        time_range: Option<(DateTime<Utc>, DateTime<Utc>)>,
    ) -> color_eyre::eyre::Result<CoverageAnalysis> {
        let time_range = time_range.unwrap_or_else(|| {
            let now = Utc::now();
            (now - chrono::Duration::hours(1), now)
        });

        Ok(CoverageAnalysis {
            time_range,
            coverage_percentage: 1.0,
            missing_count: 0,
            duplicate_count: 0,
            source_total: self.config.history_sources.len() as u64,
            sinex_total: 0,
            missing_samples: Vec::new(),
            recommendations: vec![
                "Ensure history files are readable by the terminal satellite".to_string(),
            ],
        })
    }

    fn export_data(
        &self,
        _path: &SanitizedPath,
        _format: ExportFormat,
    ) -> color_eyre::eyre::Result<()> {
        Err(eyre::eyre!("Terminal watcher does not support data export"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinex_core::db::models::Provenance;
    use sinex_core::db::query_helpers::ulid_to_uuid;
    use sinex_core::Id;
    use sinex_satellite_sdk::{acquisition_manager::RotationPolicy, AcquisitionManager};
    use sinex_test_utils::sinex_test;
    use sinex_test_utils::{prelude::*, TestRuntime, TestRuntimeBuilder};
    use std::sync::Arc;
    use tokio::{
        io::AsyncWriteExt,
        time::{timeout, Duration},
    };

    #[sinex_test]
    fn terminal_config_validation_allows_valid_configuration() -> color_eyre::Result<()> {
        let config = TerminalConfig {
            history_sources: vec![HistorySourceConfig {
                path: Utf8PathBuf::from("/tmp/.bash_history"),
                shell: "bash".to_string(),
            }],
            polling_interval_secs: 30,
            max_capture_bytes: 1024,
        };

        assert!(config.validate_config().is_ok());
        Ok(())
    }

    #[sinex_test]
    fn terminal_config_validation_rejects_empty_sources() -> color_eyre::Result<()> {
        let config = TerminalConfig {
            history_sources: vec![],
            polling_interval_secs: 30,
            max_capture_bytes: 1024,
        };

        assert!(config.validate_config().is_err());
        Ok(())
    }

    #[sinex_test]
    async fn process_command_emits_event(ctx: TestContext) -> color_eyre::Result<()> {
        let _guard = sinex_test_utils::acquire_pool_test_guard().await;
        ctx.force_cleanup().await?;
        ctx.ensure_clean().await?;
        sinex_test_utils::db_common::reset_database(&ctx.pool).await?;
        sinex_test_utils::db_common::verify_clean_state(&ctx.pool).await?;

        let TestRuntime {
            runtime,
            mut event_rx,
            nats,
        } = TestRuntimeBuilder::new(&ctx, "terminal-satellite-test")
            .with_dry_run(false)
            .build()
            .await?;
        let _ = nats.client_url();

        let publisher = match runtime.transport() {
            sinex_satellite_sdk::event_processor::EventTransport::Nats(publisher) => {
                publisher.clone()
            }
        };
        AcquisitionManager::bootstrap_streams(publisher.nats_client()).await?;

        let acquisition = Arc::new(runtime.acquisition_manager(
            RotationPolicy::default(),
            "terminal-history",
            "/home/test/.bash_history",
        )?);

        let stage_context = StageAsYouGoContext::from_runtime(&runtime);

        let watcher_ctx = HistoryWatcherContext {
            acquisition,
            stage_context,
            shell: "bash".to_string(),
            path: Utf8PathBuf::from("/home/test/.bash_history"),
            max_capture_bytes: 1024,
            polling_interval: Duration::from_secs(1),
            state_path: None,
            #[cfg(test)]
            processed_commands: None,
        };

        let command = "echo 'hello world'";
        process_command(&watcher_ctx, command, 42).await?;

        let event = timeout(Duration::from_secs(5), event_rx.recv())
            .await?
            .expect("terminal event emitted");

        assert_eq!(event.event_type.as_str(), "command.imported");

        let material_ulid = match event.provenance {
            Provenance::Material { ref id, .. } => *id.as_ulid(),
            _ => panic!("expected material provenance"),
        };

        let record = ctx
            .pool
            .source_materials()
            .get_by_id(Id::from_ulid(material_ulid))
            .await?
            .expect("source material persisted");
        assert_eq!(record.status.as_str(), "completed");

        let total_bytes: Option<i64> = sqlx::query_scalar(
            "SELECT offset_end FROM raw.temporal_ledger WHERE source_material_id = $1::uuid::ulid ORDER BY ts_capture DESC LIMIT 1",
        )
        .bind(ulid_to_uuid(material_ulid))
        .fetch_optional(&ctx.pool)
        .await?;

        assert_eq!(
            total_bytes.unwrap_or_default(),
            command.as_bytes().len() as i64
        );
        sinex_test_utils::timing_utils::WaitHelpers::wait_for_condition(
            || {
                let pool = ctx.pool.clone();
                let material_ulid = material_ulid;
                let expected = command.as_bytes().len() as i64;
                async move {
                    let ledger_bytes: Option<i64> = sqlx::query_scalar(
                        "SELECT offset_end FROM raw.temporal_ledger WHERE source_material_id = $1::uuid::ulid ORDER BY ts_capture DESC LIMIT 1",
                    )
                    .bind(ulid_to_uuid(material_ulid))
                    .fetch_optional(&pool)
                    .await
                    .map_err(|e| sinex_test_utils::SinexError::database(e.to_string()))?;
                    Ok::<bool, sinex_test_utils::SinexError>(
                        ledger_bytes.unwrap_or_default() == expected
                    )
                }
            },
            20,
        )
        .await?;

        let payload_command = event
            .payload
            .get("command")
            .and_then(|v| v.as_str())
            .expect("payload command present");
        assert_eq!(payload_command, command);

        sinex_test_utils::db_common::reset_database(&ctx.pool).await?;
        sinex_test_utils::db_common::verify_clean_state(&ctx.pool).await?;
        ctx.force_cleanup().await?;
        Ok(())
    }

    #[sinex_test]
    async fn terminal_watcher_tails_incrementally(ctx: TestContext) -> color_eyre::Result<()> {
        let TestRuntime { runtime, nats, .. } =
            TestRuntimeBuilder::new(&ctx, "terminal-watcher-incremental")
                .with_dry_run(false)
                .build()
                .await?;
        let _ = nats.client_url();

        let publisher = match runtime.transport() {
            sinex_satellite_sdk::event_processor::EventTransport::Nats(publisher) => {
                publisher.clone()
            }
        };
        AcquisitionManager::bootstrap_streams(publisher.nats_client()).await?;

        let acquisition = Arc::new(runtime.acquisition_manager(
            RotationPolicy::default(),
            "terminal-history",
            "/tmp/history",
        )?);
        let stage_context = StageAsYouGoContext::from_runtime(&runtime);

        let temp_dir = tempfile::tempdir()?;
        let history_path = temp_dir.path().join("history.txt");
        tokio::fs::write(&history_path, "echo first\n").await?;
        let state_path = temp_dir.path().join("history_state.json");

        let history_utf8 =
            Utf8PathBuf::from_path_buf(history_path.clone()).expect("history path utf8");

        let mut watcher_ctx = HistoryWatcherContext {
            acquisition,
            stage_context,
            shell: "bash".to_string(),
            path: history_utf8,
            max_capture_bytes: 2048,
            polling_interval: Duration::from_millis(50),
            state_path: Some(state_path),
            #[cfg(test)]
            processed_commands: None,
        };

        #[cfg(test)]
        let processed_commands = Arc::new(Mutex::new(Vec::new()));
        #[cfg(test)]
        {
            watcher_ctx.processed_commands = Some(processed_commands.clone());
        }

        let mut offset_bytes = 0u64;
        let mut line_number = 0u64;

        watcher_ctx
            .poll_history_once(&mut offset_bytes, &mut line_number)
            .await;

        let mut history_file: tokio::fs::File = tokio::fs::OpenOptions::new()
            .append(true)
            .open(&history_path)
            .await?;
        history_file.write_all(b"echo second\n").await?;
        history_file.write_all(b"echo third\n").await?;
        history_file.flush().await?;

        watcher_ctx
            .poll_history_once(&mut offset_bytes, &mut line_number)
            .await;

        #[cfg(test)]
        {
            let commands = processed_commands.lock().await.clone();
            assert_eq!(
                commands,
                vec!["echo first", "echo second", "echo third"],
                "history watcher should append only new commands in order"
            );
        }

        Ok(())
    }
}

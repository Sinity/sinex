//! `SourceDriver` trait for reducing boilerplate in ingestor nodes.
//!
//! This module provides a high-level abstraction (similar to the automaton runtime) but tailored
//! for Ingestors, which typically produce events from external sources rather than
//! transforming input events.
//!
//! Key features:
//! - Automated lifecycle management (initialize, shutdown)
//! - State persistence (Checkpoints)
//! - Standardized `scan` dispatching (Snapshot, Historical, Continuous)

use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::runtime::checkpoint::{CheckpointManager, CheckpointState, decode_checkpoint_data};
use crate::runtime::shutdown::ShutdownConfig;
use crate::runtime::stream::{
    Checkpoint, ContinuousStart, ModuleKind, RuntimeCapabilities, RuntimeContext,
    RuntimeDrainController, RuntimeInitContext, RuntimeModule, ScanArgs, ScanReport, TimeHorizon,
};
use crate::runtime::{
    RuntimeResult, SinexError,
    exploration::{ExplorationProvider, ExportFormat, IngestionHistoryEntry, SourceState},
};
use sinex_primitives::SanitizedPath;
use sinex_primitives::env as shared_env;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::watch;
use tracing::{info, warn};

/// Adapter state around user state with metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestorState<S> {
    pub user_state: S,
    pub last_checkpoint: sinex_primitives::temporal::Timestamp,
    pub revision: u64,
    #[serde(default)]
    pub checkpoint: Checkpoint,
}

impl<S: Default> Default for IngestorState<S> {
    fn default() -> Self {
        Self {
            user_state: S::default(),
            last_checkpoint: sinex_primitives::temporal::Timestamp::now(),
            revision: 0,
            checkpoint: Checkpoint::None,
        }
    }
}

/// Trait for simplified Ingestor implementation.
pub trait SourceDriver: Send + Sync + 'static {
    /// Configuration type (from config file/env)
    type Config: Clone + Send + Sync + Serialize + DeserializeOwned + Default;

    /// Persistent state type
    type State: Clone + Send + Sync + Default + Serialize + DeserializeOwned;

    /// Name of the ingestor
    fn name(&self) -> &str;

    /// Capabilities description
    fn capabilities(&self) -> RuntimeCapabilities {
        RuntimeCapabilities {
            supports_snapshot: true,
            supports_historical: true,
            supports_continuous: true,
            supports_interactive: false,
            max_scan_size: None,
            supports_concurrent: false,
            manages_own_continuous_loop: true,
            manages_own_checkpoints: true,
        }
    }

    /// Initialize the ingestor logic.
    /// Called after state is loaded and runtime is set up.
    fn initialize(
        &mut self,
        config: Self::Config,
        runtime: &RuntimeContext,
        state: &mut Self::State,
    ) -> impl std::future::Future<Output = RuntimeResult<()>> + Send;

    /// Perform a snapshot scan.
    fn scan_snapshot(
        &mut self,
        state: &mut Self::State,
        args: ScanArgs,
    ) -> impl std::future::Future<Output = RuntimeResult<ScanReport>> + Send;

    /// Perform a historical scan.
    fn scan_historical(
        &mut self,
        state: &mut Self::State,
        from: Checkpoint,
        until: TimeHorizon,
        args: ScanArgs,
    ) -> impl std::future::Future<Output = RuntimeResult<ScanReport>> + Send;

    /// Run continuous ingestion loop.
    fn run_continuous(
        &mut self,
        state: &mut Self::State,
        start: ContinuousStart,
        shutdown_rx: watch::Receiver<bool>,
    ) -> impl std::future::Future<Output = RuntimeResult<ScanReport>> + Send;

    /// Optional shutdown hook
    fn shutdown(
        &mut self,
        _state: &Self::State,
    ) -> impl std::future::Future<Output = RuntimeResult<()>> + Send {
        async { Ok(()) }
    }

    // Exploration provider methods
    fn get_source_state(&self, _state: &Self::State) -> RuntimeResult<SourceState> {
        Err(SinexError::processing(
            "Source state exploration not implemented",
        ))
    }

    fn get_ingestion_history(
        &self,
        _state: &Self::State,
        _limit: u64,
    ) -> RuntimeResult<Vec<IngestionHistoryEntry>> {
        Err(SinexError::processing("Ingestion history not implemented"))
    }

    fn export_data(
        &self,
        _state: &Self::State,
        _path: &SanitizedPath,
        _format: ExportFormat,
    ) -> RuntimeResult<()> {
        Err(SinexError::processing("Data export not implemented"))
    }
}

/// Adapter implementing `RuntimeModule` for `SourceDriver`.
pub struct SourceDriverRuntime<I: SourceDriver> {
    ingestor: I,
    state: IngestorState<I::State>,
    shutdown_config: ShutdownConfig,
    runtime: Option<RuntimeContext>,
    checkpoint_manager: Option<Arc<CheckpointManager>>,
    shutdown_tx: Option<Arc<RuntimeDrainController>>,
    pending_hot_reload_cleanup: Option<PathBuf>,
    health_reporter: Option<Arc<crate::runtime::health_reporter::HealthReporter>>,
    self_observer: Option<Arc<crate::runtime::self_observation::SelfObserver>>,
}

impl<I: SourceDriver> SourceDriverRuntime<I> {
    pub fn new(ingestor: I) -> Self {
        Self {
            ingestor,
            state: IngestorState::default(),
            shutdown_config: ShutdownConfig::default(),
            runtime: None,
            checkpoint_manager: None,
            shutdown_tx: None,
            pending_hot_reload_cleanup: None,
            health_reporter: None,
            self_observer: None,
        }
    }

    pub fn with_shutdown_config(mut self, config: ShutdownConfig) -> Self {
        self.shutdown_config = config;
        self
    }

    pub fn ingestor(&self) -> &I {
        &self.ingestor
    }

    /// Access the self-observer for emitting telemetry metrics.
    pub fn self_observer(&self) -> Option<&Arc<crate::runtime::self_observation::SelfObserver>> {
        self.self_observer.as_ref()
    }

    /// Access the health reporter for recording successes/errors.
    pub fn health_reporter(&self) -> Option<&Arc<crate::runtime::health_reporter::HealthReporter>> {
        self.health_reporter.as_ref()
    }

    fn checkpoint_file_identity(&self) -> &str {
        self.runtime
            .as_ref()
            .map_or_else(|| self.ingestor.name(), RuntimeContext::checkpoint_identity)
    }
}

impl<I: SourceDriver + Default> Default for SourceDriverRuntime<I> {
    fn default() -> Self {
        Self::new(I::default())
    }
}

impl<I: SourceDriver> SourceDriverRuntime<I> {
    async fn cleanup_hot_reload_file_best_effort(
        path: &Path,
        module_name: &str,
        reason: &'static str,
    ) {
        if let Err(error) = CheckpointState::delete_file(path).await {
            warn!(
                module = module_name,
                path = %path.display(),
                error = %error,
                reason,
                "Failed to clean up hot reload checkpoint file"
            );
        }
    }

    async fn finalize_restored_hot_reload_file(
        &mut self,
        checkpoint_state: &CheckpointState,
    ) -> RuntimeResult<()> {
        let Some(path) = self.pending_hot_reload_cleanup.take() else {
            return Ok(());
        };

        match CheckpointState::delete_file(&path).await {
            Ok(()) => Ok(()),
            Err(delete_error) => {
                warn!(
                    module = self.ingestor.name(),
                    path = %path.display(),
                    error = %delete_error,
                    "Failed to delete restored hot reload checkpoint file after syncing to NATS KV; rewriting it with the latest durable state"
                );
                checkpoint_state.save_to_file(&path).await.map_err(|error| {
                    SinexError::io(
                        "Failed to synchronize restored hot reload file after checkpoint save",
                    )
                    .with_context("module", self.ingestor.name())
                    .with_context("path", path.display().to_string())
                    .with_context("delete_error", delete_error.to_string())
                    .with_std_error(&error)
                })
            }
        }
    }

    fn effective_final_checkpoint(
        until: &TimeHorizon,
        previous_checkpoint: &Checkpoint,
        reported_checkpoint: Checkpoint,
    ) -> Checkpoint {
        if matches!(until, TimeHorizon::Snapshot)
            && matches!(reported_checkpoint, Checkpoint::None)
            && !matches!(previous_checkpoint, Checkpoint::None)
        {
            return previous_checkpoint.clone();
        }

        reported_checkpoint
    }

    /// Apply `FailurePolicy::settle()` to a scan error and return the appropriate
    /// action: propagate the error (Halt/Retry), or swallow it (Commit/skip).
    fn settle_scan_error(
        &self,
        error: crate::runtime::SinexError,
        phase: &str,
        from: &Checkpoint,
    ) -> RuntimeResult<ScanReport> {
        use sinex_primitives::settlement::{
            DefaultFailurePolicy, FailureContext, FailurePolicy, RuntimeOperation, RuntimePhase,
            Settlement,
        };

        let failure_ctx = FailureContext {
            unit_id: self.ingestor.name().to_string(),
            operation: RuntimeOperation::ProcessBatch,
            phase: RuntimePhase::ProcessInput,
            input_scope: None,
            effect_kind: None,
            delivery_count: None,
            attempts: 0,
        };
        let settlement = DefaultFailurePolicy.settle(&error, &failure_ctx);

        match settlement {
            Settlement::Commit => {
                warn!(
                    module = %self.ingestor.name(),
                    phase,
                    error = %error,
                    "Ingestor scan error settled as benign; returning empty report"
                );
                Ok(ScanReport {
                    events_processed: 0,
                    duration: std::time::Duration::ZERO,
                    final_checkpoint: from.clone(),
                    time_range: None,
                    runtime_stats: std::collections::HashMap::new(),
                    failed_targets: Vec::new(),
                    successful_targets: Vec::new(),
                    warnings: Vec::new(),
                })
            }
            Settlement::Retry { .. } => {
                warn!(
                    module = %self.ingestor.name(),
                    phase,
                    error = %error,
                    "Ingestor scan error settled as retryable; propagating for caller retry"
                );
                Err(error)
            }
            Settlement::HaltModule { .. } | Settlement::DrainRuntimeUnit { .. } => {
                // Halt/drain settlements request runtime drain (clean shutdown)
                // rather than letting systemd restart a known-broken node into
                // a hot loop. Distinguishing this from generic Err propagation
                // preserves the Settlement→action mapping the policy intended.
                if let Some(drain) = self.shutdown_tx.as_ref() {
                    let _ = drain.request_drain_and_warn(self.ingestor.name());
                }
                warn!(
                    module = %self.ingestor.name(),
                    phase,
                    error = %error,
                    settlement = ?settlement,
                    "Ingestor scan error settled as halt/drain; runtime drain requested"
                );
                Err(error)
            }
            Settlement::SendToProcessingFailure
            | Settlement::Park { .. }
            | Settlement::Quarantine { .. } => {
                // SendToProcessingFailure/Park/Quarantine are scan-phase
                // settlements the ingestor surface can't fully execute (no
                // direct DLQ publisher here; downstream event_engine handles DLQ
                // routing for per-event failures). Propagate the error so the
                // caller's retry/abort logic runs; the DLQ wiring lives on
                // event_engine's per-event path, not on ingestor scan errors.
                warn!(
                    module = %self.ingestor.name(),
                    phase,
                    error = %error,
                    settlement = ?settlement,
                    "Ingestor scan error settled as terminal; propagating"
                );
                Err(error)
            }
        }
    }

    async fn load_state(&mut self) -> RuntimeResult<()> {
        let checkpoint_path = self
            .shutdown_config
            .checkpoint_path(self.checkpoint_file_identity());
        let mut invalid_hot_reload_file = None;

        // 1. Try file (hot reload)
        if self.shutdown_config.restore_state_on_startup {
            match CheckpointState::load_from_file(&checkpoint_path).await {
                Ok(Some(ckpt)) => {
                    let data = ckpt.data.ok_or_else(|| {
                        SinexError::checkpoint(
                            "Hot reload ingestor checkpoint file is missing state data",
                        )
                        .with_context("module", self.ingestor.name())
                        .with_context("path", checkpoint_path.display().to_string())
                    })?;
                    self.state = decode_checkpoint_data(
                        data,
                        "hot reload ingestor state",
                        self.ingestor.name(),
                    )?;
                    if matches!(self.state.checkpoint, Checkpoint::None)
                        && !matches!(ckpt.checkpoint, Checkpoint::None)
                    {
                        self.state.checkpoint = ckpt.checkpoint;
                    }
                    self.state.revision = ckpt.revision;
                    self.pending_hot_reload_cleanup = Some(checkpoint_path.clone());
                    return Ok(());
                }
                Ok(None) => {}
                Err(error) if self.checkpoint_manager.is_some() => {
                    warn!(
                        module = self.ingestor.name(),
                        path = %checkpoint_path.display(),
                        error = %error,
                        "Failed to restore hot reload checkpoint file; falling back to NATS KV"
                    );
                    invalid_hot_reload_file = Some(checkpoint_path.clone());
                }
                Err(error) => return Err(error),
            }
        }

        // 2. Try NATS KV
        if let Some(cm) = &self.checkpoint_manager {
            let ckpt = cm.load_checkpoint().await?;
            match ckpt.data {
                Some(data) => {
                    self.state = decode_checkpoint_data(
                        data,
                        "ingestor checkpoint state",
                        self.ingestor.name(),
                    )?;
                    self.state.revision = ckpt.revision;
                    if matches!(self.state.checkpoint, Checkpoint::None)
                        && !matches!(ckpt.checkpoint, Checkpoint::None)
                    {
                        self.state.checkpoint = ckpt.checkpoint;
                    }
                }
                None if matches!(ckpt.checkpoint, Checkpoint::None) => {
                    self.state.revision = ckpt.revision;
                }
                None => {
                    return Err(SinexError::checkpoint(
                        "Ingestor checkpoint KV entry is missing state data",
                    )
                    .with_context("module", self.ingestor.name()));
                }
            }
        }

        if let Some(path) = invalid_hot_reload_file {
            Self::cleanup_hot_reload_file_best_effort(
                &path,
                self.ingestor.name(),
                "discarding invalid hot reload checkpoint file after successful NATS KV restore",
            )
            .await;
        }

        Ok(())
    }

    async fn save_state(&mut self, is_shutdown: bool) -> RuntimeResult<()> {
        self.state.last_checkpoint = sinex_primitives::temporal::Timestamp::now();
        let json_state = serde_json::to_value(&self.state).map_err(SinexError::serialization)?;

        let mut ckpt_state = CheckpointState {
            checkpoint: self.state.checkpoint.clone(),
            processed_count: 0, // Ingestors might track this in user state if needed
            last_activity: sinex_primitives::temporal::Timestamp::now(),
            data: Some(json_state),
            version: 1,
            revision: self.state.revision,
        };

        if is_shutdown && self.shutdown_config.save_state_on_shutdown {
            let path = self
                .shutdown_config
                .checkpoint_path(self.checkpoint_file_identity());
            ckpt_state
                .save_to_file(&path)
                .await
                .map_err(SinexError::io)?;
        }

        if let Some(cm) = &self.checkpoint_manager {
            self.state.revision = cm.save_checkpoint(&ckpt_state).await?;
            ckpt_state.revision = self.state.revision;
            self.finalize_restored_hot_reload_file(&ckpt_state).await?;
        }

        Ok(())
    }
}

impl<I: SourceDriver> RuntimeModule for SourceDriverRuntime<I> {
    type Config = I::Config;

    async fn initialize(&mut self, init: RuntimeInitContext<Self::Config>) -> RuntimeResult<()> {
        let (config, runtime) = init.into_runtime();
        self.checkpoint_manager = Some(runtime.checkpoint_manager().clone());
        self.runtime = Some(runtime.clone());

        if let Some(nats_client) = runtime.nats_client() {
            use crate::runtime::health_reporter::{HealthReporter, HealthThresholds};
            use crate::runtime::self_observation::{SelfObserver, SelfObserverConfig};

            let health_enabled = shared_env::bool_or(
                "SINEX_HEALTH_MONITORING_ENABLED",
                true,
                "ingestor runtime module health monitoring",
            );

            if health_enabled {
                let config = SelfObserverConfig {
                    component: self.ingestor.name().to_string(),
                    namespace: None,
                    enabled: true,
                    min_emission_interval: std::time::Duration::from_secs(1),
                };

                // Clone before SelfObserver::new() takes ownership of nats_client.
                let nats_for_probe = nats_client.clone();
                let observer = Arc::new(SelfObserver::new(nats_client, config));
                let thresholds = HealthThresholds::from_env().unwrap_or_else(|error| {
                    warn!(
                        module = %self.ingestor.name(),
                        error = %error,
                        "Invalid health monitoring threshold override; using defaults"
                    );
                    HealthThresholds::default()
                });
                let liveness_probe: crate::runtime::health_reporter::LivenessProbe =
                    Arc::new(move || {
                        let client = nats_for_probe.clone();
                        Box::pin(async move {
                            use async_nats::connection::State as NatsState;
                            // Fast path: avoid the async round-trip when already disconnected.
                            if !matches!(client.connection_state(), NatsState::Connected) {
                                return false;
                            }
                            // Active probe: flush() issues PING and waits for PONG.
                            tokio::time::timeout(
                                std::time::Duration::from_millis(500),
                                client.flush(),
                            )
                            .await
                            .is_ok_and(|r| r.is_ok())
                        })
                    });

                let reporter = Arc::new(
                    HealthReporter::new(
                        self.ingestor.name().to_string(),
                        Arc::clone(&observer),
                        thresholds,
                    )
                    .with_liveness_probe(liveness_probe),
                );

                // Wire emit-stall detection: install a shared `EmitTracker` into both
                // the reporter and the runtime's `EventEmitter`. Every event the
                // ingestor pushes through the runtime now feeds the stall detector.
                // See issue #992 (silent watcher death) — emit-rate stall is the
                // companion signal to watcher-process death.
                let tracker = reporter.enable_emit_stall_detection();
                runtime.register_emit_tracker(tracker);

                self.health_reporter = Some(reporter);
                self.self_observer = Some(observer);

                info!(module = %self.ingestor.name(), "Health monitoring auto-enabled (with emit-stall detection)");
            }
        }

        self.load_state().await?;

        self.ingestor
            .initialize(config, &runtime, &mut self.state.user_state)
            .await?;

        info!("SourceDriver {} initialized", self.ingestor.name());
        Ok(())
    }

    async fn scan(
        &mut self,
        from: Checkpoint,
        until: TimeHorizon,
        args: ScanArgs,
    ) -> RuntimeResult<ScanReport> {
        let previous_checkpoint = self.state.checkpoint.clone();
        let from_for_errors = from.clone();
        let mut report = match &until {
            TimeHorizon::Snapshot => {
                let result = self
                    .ingestor
                    .scan_snapshot(&mut self.state.user_state, args)
                    .await;
                match result {
                    Ok(report) => report,
                    Err(e) => return self.settle_scan_error(e, "snapshot", &from_for_errors),
                }
            }
            TimeHorizon::Historical { .. } => {
                let result = self
                    .ingestor
                    .scan_historical(&mut self.state.user_state, from, until.clone(), args)
                    .await;
                match result {
                    Ok(report) => report,
                    Err(e) => return self.settle_scan_error(e, "historical", &from_for_errors),
                }
            }
            TimeHorizon::Continuous => {
                let runtime = self.runtime.as_ref().ok_or_else(|| {
                    SinexError::lifecycle("Cannot run continuous scan: runtime not initialized")
                })?;
                let drain = runtime.runtime_drain();
                let rx = drain.subscribe();
                self.shutdown_tx = Some(drain);

                let health_reporter = self.health_reporter.clone();
                let module_name = self.ingestor.name().to_string();

                let continuous_fut = self.ingestor.run_continuous(
                    &mut self.state.user_state,
                    ContinuousStart::from_checkpoint(from),
                    rx,
                );

                // Emit health at 30s intervals during continuous operation.
                // If no health reporter is configured, this future never resolves.
                //
                // NOTE: This tick used to call `reporter.record_success()` every 30s,
                // which masked emit-rate stalls — a stalled watcher kept ticking
                // "success" while emitting zero events (issue #992). The tick now
                // only consults current status (no synthetic success), so health
                // is driven by real `EventEmitter::emit` calls (via the wired
                // `EmitTracker`) plus error reports from `settle_scan_error`.
                let health_fut = async {
                    if let Some(reporter) = health_reporter {
                        let mut interval =
                            tokio::time::interval(std::time::Duration::from_secs(30));
                        loop {
                            interval.tick().await;
                            if let Err(e) = reporter.check_and_emit().await {
                                warn!(
                                    target: "sinex_metrics",
                                    metric = "runtime.health_emit_failures_total",
                                    module = %module_name,
                                    error = %e,
                                    "Failed to emit ingestor health status"
                                );
                            }
                        }
                    } else {
                        std::future::pending::<()>().await;
                    }
                };

                let continuous_result = tokio::select! {
                    result = continuous_fut => result,
                    () = health_fut => unreachable!("health ticker never completes"),
                };
                match continuous_result {
                    Ok(report) => report,
                    Err(e) => return self.settle_scan_error(e, "continuous", &from_for_errors),
                }
            }
        };

        let effective_checkpoint =
            Self::effective_final_checkpoint(&until, &previous_checkpoint, report.final_checkpoint);
        report.final_checkpoint = effective_checkpoint.clone();
        self.state.checkpoint = effective_checkpoint;
        self.save_state(false).await?;

        if let Some(reporter) = self.health_reporter.as_ref() {
            // Only count this as a `record_success` if real work happened; an
            // empty scan with no successes and no errors is silent (it neither
            // proves health nor degrades it). Emit-rate stall takes over in
            // continuous mode via `EventEmitter`-driven `EmitTracker`.
            if report.events_processed > 0 {
                reporter.record_success();
                reporter.notify_emit(report.events_processed);
            }
            for (_target, error_msg) in &report.failed_targets {
                reporter.record_error(&SinexError::processing(error_msg));
            }
            if let Err(e) = reporter.check_and_emit().await {
                warn!(
                    target: "sinex_metrics",
                    metric = "runtime.health_emit_failures_total",
                    module = %self.ingestor.name(),
                    error = %e,
                    "Failed to emit ingestor health status"
                );
            }
        }

        Ok(report)
    }

    async fn shutdown(&mut self) -> RuntimeResult<()> {
        if let Some(tx) = self.shutdown_tx.take()
            && !tx.request_drain_and_warn(self.ingestor.name())
        {
            warn!(
                module = self.ingestor.name(),
                "Skipping graceful continuous-loop shutdown confirmation because the receiver is gone"
            );
        }
        self.ingestor.shutdown(&self.state.user_state).await?;
        self.save_state(true).await?;
        Ok(())
    }

    fn module_name(&self) -> &str {
        self.ingestor.name()
    }

    fn module_kind(&self) -> ModuleKind {
        ModuleKind::Source
    }

    fn capabilities(&self) -> RuntimeCapabilities {
        self.ingestor.capabilities()
    }

    async fn current_checkpoint(&self) -> RuntimeResult<Checkpoint> {
        Ok(self.state.checkpoint.clone())
    }
}

impl<I: SourceDriver> ExplorationProvider for SourceDriverRuntime<I> {
    fn get_source_state(&self) -> RuntimeResult<SourceState> {
        self.ingestor.get_source_state(&self.state.user_state)
    }

    fn get_ingestion_history(&self, limit: u64) -> RuntimeResult<Vec<IngestionHistoryEntry>> {
        self.ingestor
            .get_ingestion_history(&self.state.user_state, limit)
    }

    fn export_data(&self, path: &SanitizedPath, format: ExportFormat) -> RuntimeResult<()> {
        self.ingestor
            .export_data(&self.state.user_state, path, format)
    }
}

#[cfg(test)]
mod tests {
    // Inline because these cover a private shutdown-signaling helper.
    use super::{IngestorState, SourceDriverRuntime};
    use crate::runtime::checkpoint::{CheckpointManager, CheckpointState};
    use crate::runtime::shutdown::ShutdownConfig;
    use crate::runtime::stream::{
        Checkpoint, ContinuousStart, RuntimeCapabilities, ScanArgs, ScanReport, TimeHorizon,
    };
    use crate::runtime::{RuntimeResult, SourceDriver};
    use futures::TryStreamExt;
    use serde::{Deserialize, Serialize};
    use sinex_primitives::Timestamp;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tempfile::tempdir;
    use tokio::sync::watch;
    use xtask::sandbox::prelude::*;

    #[derive(Debug, Clone, Default, Serialize, Deserialize)]
    struct TestState;

    #[derive(Default)]
    struct TestIngestor;

    impl SourceDriver for TestIngestor {
        type Config = ();
        type State = TestState;

        #[allow(clippy::unused_self)]
        fn name(&self) -> &'static str {
            "ingestor-adapter-test"
        }

        #[allow(clippy::unused_self)]
        fn capabilities(&self) -> RuntimeCapabilities {
            RuntimeCapabilities::default()
        }

        async fn initialize(
            &mut self,
            _config: Self::Config,
            _runtime: &crate::runtime::stream::RuntimeContext,
            _state: &mut Self::State,
        ) -> RuntimeResult<()> {
            Ok(())
        }

        async fn scan_snapshot(
            &mut self,
            _state: &mut Self::State,
            _args: ScanArgs,
        ) -> RuntimeResult<ScanReport> {
            Ok(ScanReport {
                events_processed: 0,
                duration: std::time::Duration::ZERO,
                final_checkpoint: Checkpoint::None,
                time_range: None,
                runtime_stats: HashMap::new(),
                successful_targets: Vec::new(),
                failed_targets: Vec::new(),
                warnings: Vec::new(),
            })
        }

        async fn scan_historical(
            &mut self,
            _state: &mut Self::State,
            _from: Checkpoint,
            _until: TimeHorizon,
            _args: ScanArgs,
        ) -> RuntimeResult<ScanReport> {
            Ok(ScanReport {
                events_processed: 0,
                duration: std::time::Duration::ZERO,
                final_checkpoint: Checkpoint::None,
                time_range: None,
                runtime_stats: HashMap::new(),
                successful_targets: Vec::new(),
                failed_targets: Vec::new(),
                warnings: Vec::new(),
            })
        }

        async fn run_continuous(
            &mut self,
            _state: &mut Self::State,
            start: ContinuousStart,
            _shutdown_rx: watch::Receiver<bool>,
        ) -> RuntimeResult<ScanReport> {
            Ok(ScanReport {
                events_processed: 0,
                duration: std::time::Duration::ZERO,
                final_checkpoint: start.checkpoint().clone(),
                time_range: None,
                runtime_stats: HashMap::new(),
                successful_targets: Vec::new(),
                failed_targets: Vec::new(),
                warnings: Vec::new(),
            })
        }
    }

    #[sinex_test]
    async fn request_runtime_drain_delivers_to_receiver() -> TestResult<()> {
        crate::runtime::stream::test_support::assert_request_drain_delivers_to_receiver(
            "test-ingestor",
        )
        .await
    }

    #[sinex_test]
    async fn request_runtime_drain_is_idempotent() -> TestResult<()> {
        crate::runtime::stream::test_support::assert_request_drain_is_idempotent("test-ingestor");
        Ok(())
    }

    #[sinex_test]
    async fn load_state_rejects_hot_reload_file_without_state_payload() -> TestResult<()> {
        let temp_dir = tempdir()?;
        let checkpoint_path = temp_dir.path().join("ingestor-empty-state.checkpoint.json");
        CheckpointState {
            checkpoint: Checkpoint::stream("restored", None),
            processed_count: 0,
            last_activity: Timestamp::now(),
            data: None,
            version: 2,
            revision: 0,
        }
        .save_to_file(&checkpoint_path)
        .await?;

        let mut adapter = SourceDriverRuntime::new(TestIngestor);
        adapter.shutdown_config = ShutdownConfig {
            checkpoint_path: Some(checkpoint_path.clone()),
            ..ShutdownConfig::default()
        };

        let error = adapter
            .load_state()
            .await
            .expect_err("empty hot reload ingestor state must not be treated as absent");
        let message = format!("{error:#}");
        assert!(message.contains("missing state data"));
        assert!(message.contains("ingestor-adapter-test"));
        assert!(message.contains(&checkpoint_path.display().to_string()));
        Ok(())
    }

    #[sinex_test]
    async fn load_state_falls_back_to_kv_when_hot_reload_file_is_corrupt(
        ctx: TestContext,
    ) -> TestResult<()> {
        let ctx = ctx.with_nats().shared().await?;
        let kv = ctx.checkpoint_kv().await?;
        let manager = Arc::new(CheckpointManager::new(
            kv,
            "ingestor-adapter-test".to_string(),
            "test-group".to_string(),
            "kv-fallback-consumer".to_string(),
        ));

        let persisted_state = IngestorState {
            user_state: TestState,
            last_checkpoint: Timestamp::now(),
            revision: 0,
            checkpoint: Checkpoint::stream("kv-restored", None),
        };
        let revision = manager
            .save_checkpoint(&CheckpointState {
                checkpoint: Checkpoint::stream("kv-restored", None),
                processed_count: 0,
                last_activity: Timestamp::now(),
                data: Some(serde_json::to_value(&persisted_state)?),
                version: 2,
                revision: 0,
            })
            .await?;

        let temp_dir = tempdir()?;
        let checkpoint_path = temp_dir.path().join("corrupt-hot-reload.checkpoint.json");
        tokio::fs::write(&checkpoint_path, "{ definitely not valid json").await?;

        let mut adapter = SourceDriverRuntime::new(TestIngestor);
        adapter.shutdown_config = ShutdownConfig {
            checkpoint_path: Some(checkpoint_path.clone()),
            ..ShutdownConfig::default()
        };
        adapter.checkpoint_manager = Some(Arc::clone(&manager));

        adapter
            .load_state()
            .await
            .expect("corrupt hot reload file should fall back to healthy KV state");

        assert_eq!(adapter.state.revision, revision);
        assert_eq!(
            adapter.state.checkpoint,
            Checkpoint::stream("kv-restored", None)
        );
        assert!(
            CheckpointState::load_from_file(&checkpoint_path)
                .await?
                .is_none(),
            "corrupt hot reload file should be discarded after successful KV restore"
        );
        Ok(())
    }

    #[sinex_test]
    async fn load_state_rejects_kv_checkpoint_without_state_payload(
        ctx: TestContext,
    ) -> TestResult<()> {
        let ctx = ctx.with_nats().shared().await?;
        let kv = ctx.checkpoint_kv().await?;
        let manager = CheckpointManager::new(
            kv.clone(),
            "ingestor-adapter-test".to_string(),
            "test-group".to_string(),
            "test-consumer".to_string(),
        );
        manager.save_checkpoint(&CheckpointState::default()).await?;

        let mut keys = kv.keys().await?;
        let key = keys.try_next().await?.expect("checkpoint key should exist");
        let corrupt = serde_json::to_vec(&CheckpointState {
            checkpoint: Checkpoint::stream("restored", None),
            processed_count: 0,
            last_activity: Timestamp::now(),
            data: None,
            version: 2,
            revision: 0,
        })?;
        kv.put(&key, corrupt.into()).await?;

        let mut adapter = SourceDriverRuntime::new(TestIngestor);
        adapter.checkpoint_manager = Some(Arc::new(manager));

        let error = adapter
            .load_state()
            .await
            .expect_err("empty ingestor checkpoint KV state must not be treated as fresh");
        let message = format!("{error:#}");
        assert!(message.contains("missing state data"));
        assert!(message.contains("ingestor-adapter-test"));
        Ok(())
    }

    #[sinex_test]
    async fn load_state_accepts_fresh_kv_checkpoint_without_state_payload(
        ctx: TestContext,
    ) -> TestResult<()> {
        let ctx = ctx.with_nats().shared().await?;
        let kv = ctx.checkpoint_kv().await?;
        let manager = CheckpointManager::new(
            kv,
            "ingestor-adapter-test".to_string(),
            "test-group".to_string(),
            "fresh-consumer".to_string(),
        );

        let mut adapter = SourceDriverRuntime::new(TestIngestor);
        adapter.checkpoint_manager = Some(Arc::new(manager));
        adapter
            .load_state()
            .await
            .expect("fresh checkpoint state should be treated as a clean start");

        assert!(matches!(adapter.state.checkpoint, Checkpoint::None));
        assert_eq!(adapter.state.revision, 0);
        Ok(())
    }

    #[sinex_test]
    async fn save_state_keeps_restored_hot_reload_file_until_successful_kv_sync(
        ctx: TestContext,
    ) -> TestResult<()> {
        let ctx = ctx.with_nats().shared().await?;
        let kv = ctx.checkpoint_kv().await?;
        let manager = Arc::new(CheckpointManager::new(
            kv,
            "ingestor-adapter-test".to_string(),
            "test-group".to_string(),
            "hot-reload-sync-consumer".to_string(),
        ));

        let persisted_state = IngestorState {
            user_state: TestState,
            last_checkpoint: Timestamp::now(),
            revision: 0,
            checkpoint: Checkpoint::stream("file-restored", None),
        };
        let baseline_revision = manager
            .save_checkpoint(&CheckpointState {
                checkpoint: Checkpoint::stream("file-restored", None),
                processed_count: 0,
                last_activity: Timestamp::now(),
                data: Some(serde_json::to_value(&persisted_state)?),
                version: 2,
                revision: 0,
            })
            .await?;

        let temp_dir = tempdir()?;
        let checkpoint_path = temp_dir.path().join("ingestor-hot-reload.checkpoint.json");
        CheckpointState {
            checkpoint: Checkpoint::stream("file-restored", None),
            processed_count: 0,
            last_activity: Timestamp::now(),
            data: Some(serde_json::to_value(&persisted_state)?),
            version: 2,
            revision: baseline_revision,
        }
        .save_to_file(&checkpoint_path)
        .await?;

        let mut adapter = SourceDriverRuntime::new(TestIngestor);
        adapter.shutdown_config = ShutdownConfig {
            checkpoint_path: Some(checkpoint_path.clone()),
            ..ShutdownConfig::default()
        };
        adapter.checkpoint_manager = Some(Arc::clone(&manager));

        adapter.load_state().await?;
        assert!(
            CheckpointState::load_from_file(&checkpoint_path)
                .await?
                .is_some(),
            "restored hot reload file must remain until the state is durably re-saved"
        );

        adapter.save_state(false).await?;
        assert!(
            CheckpointState::load_from_file(&checkpoint_path)
                .await?
                .is_none(),
            "restored hot reload file should be cleaned up after successful KV sync"
        );
        assert!(
            adapter.state.revision > baseline_revision,
            "follow-up save should update the prior KV checkpoint revision"
        );
        Ok(())
    }

    #[sinex_test]
    async fn save_state_recreates_missing_kv_entry_for_stale_hot_reload_revision(
        ctx: TestContext,
    ) -> TestResult<()> {
        let ctx = ctx.with_nats().shared().await?;
        let kv = ctx.checkpoint_kv().await?;
        let manager = Arc::new(CheckpointManager::new(
            kv.clone(),
            "ingestor-adapter-test".to_string(),
            "test-group".to_string(),
            "stale-hot-reload-consumer".to_string(),
        ));

        let persisted_state = IngestorState {
            user_state: TestState,
            last_checkpoint: Timestamp::now(),
            revision: 0,
            checkpoint: Checkpoint::stream("file-restored", None),
        };

        let temp_dir = tempdir()?;
        let checkpoint_path = temp_dir.path().join("stale-hot-reload.checkpoint.json");
        CheckpointState {
            checkpoint: Checkpoint::stream("file-restored", None),
            processed_count: 0,
            last_activity: Timestamp::now(),
            data: Some(serde_json::to_value(&persisted_state)?),
            version: 2,
            revision: 7,
        }
        .save_to_file(&checkpoint_path)
        .await?;

        let mut adapter = SourceDriverRuntime::new(TestIngestor);
        adapter.shutdown_config = ShutdownConfig {
            checkpoint_path: Some(checkpoint_path.clone()),
            ..ShutdownConfig::default()
        };
        adapter.checkpoint_manager = Some(Arc::clone(&manager));

        adapter.load_state().await?;
        assert_eq!(adapter.state.revision, 7);

        adapter.save_state(false).await?;
        assert!(
            adapter.state.revision > 0,
            "successful save should recreate the missing KV entry with a fresh revision"
        );
        assert!(
            CheckpointState::load_from_file(&checkpoint_path)
                .await?
                .is_none(),
            "restored hot reload file should be cleaned up after the recreated KV save"
        );

        let mut keys = kv.keys().await?;
        assert!(
            keys.try_next().await?.is_some(),
            "checkpoint KV entry should be recreated when only a stale hot reload file exists"
        );
        Ok(())
    }
}

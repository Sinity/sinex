//! `DerivedNodeAdapter` — shared runtime adapter for all derived node models.
//!
//! Wraps any [`DerivedNodeImpl`] and implements the stream [`Node`] trait,
//! handling checkpoints, health monitoring, shutdown, and event emission.

mod filter;
mod invalidate;
mod observe;
mod output;
mod process;
mod run;
mod state_io;

use super::traits::{DerivedNodeConfig, DerivedNodeImpl};

use crate::checkpoint::CheckpointManager;
use crate::error_helpers::env_bool_with_default;
use crate::processing::PersistedState;
use crate::runtime::stream::{
    Checkpoint, EventEmitter, NodeCapabilities, NodeInitContext, NodeRuntimeState, NodeType,
    ProcessingStats, RuntimeDrainController, ScanArgs, ScanEstimate, ScanReport, TimeHorizon,
};
use crate::shutdown::ShutdownConfig;
use crate::{NodeResult, SinexError};

use sinex_primitives::events::Event;
use sinex_primitives::non_empty::NonEmptyVec;
use sinex_primitives::query::TimeRange;
use sinex_primitives::temporal::Timestamp;
use sinex_primitives::{Id, JsonValue, Pagination, Uuid};

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, error, info, warn};

const INVALIDATION_QUERY_PAGE_SIZE: i64 = Pagination::MAX_LIMIT;
const DERIVED_OUTPUT_PARENT_WARN_THRESHOLD: usize = 100;
const DERIVED_OUTPUT_PARENT_HARD_LIMIT: usize = 1000;

fn request_runtime_drain(drain: &RuntimeDrainController, node_name: &str) -> bool {
    if !drain.request_drain() && !drain.is_requested() {
        warn!(
            node = node_name,
            "Derived-node runtime drain signal could not be delivered before graceful shutdown"
        );
        return false;
    }
    true
}

fn derived_event_anchor(
    output_index: usize,
    source_event_ids: &NonEmptyVec<Id<Event<JsonValue>>>,
    temporal_policy: &sinex_primitives::domain::SyntheticTemporalPolicy,
    semantics_version: Option<&str>,
    scope_key: Option<&str>,
    equivalence_key: Option<&str>,
) -> Vec<u8> {
    let mut anchor = Vec::new();
    append_anchor_field(
        &mut anchor,
        b"output_index",
        output_index.to_string().as_bytes(),
    );
    append_anchor_field(
        &mut anchor,
        b"temporal_policy",
        temporal_policy.to_string().as_bytes(),
    );
    append_anchor_field(
        &mut anchor,
        b"semantics_version",
        semantics_version.unwrap_or("").as_bytes(),
    );
    append_anchor_field(
        &mut anchor,
        b"scope_key",
        scope_key.unwrap_or("").as_bytes(),
    );
    append_anchor_field(
        &mut anchor,
        b"equivalence_key",
        equivalence_key.unwrap_or("").as_bytes(),
    );
    for source_event_id in source_event_ids {
        append_anchor_field(
            &mut anchor,
            b"source_event_id",
            source_event_id.as_uuid().to_string().as_bytes(),
        );
    }
    anchor
}

fn append_anchor_field(anchor: &mut Vec<u8>, name: &[u8], value: &[u8]) {
    anchor.extend_from_slice(&u64::try_from(name.len()).unwrap_or(u64::MAX).to_be_bytes());
    anchor.extend_from_slice(name);
    anchor.extend_from_slice(&u64::try_from(value.len()).unwrap_or(u64::MAX).to_be_bytes());
    anchor.extend_from_slice(value);
}

fn stale_output_ids_or_fail_scope(
    node_name: &str,
    scope_key: &str,
    stale_query_result: Result<Vec<sinex_primitives::query::QueryResultEvent>, SinexError>,
) -> Result<Vec<Uuid>, SinexError> {
    match stale_query_result {
        Ok(events) => Ok(events
            .iter()
            .filter_map(|qe| qe.event.id.map(|id| *id.as_uuid()))
            .collect()),
        Err(error) => Err(SinexError::processing(
            "Failed to query stale outputs for invalidation recompute",
        )
        .with_context("node", node_name)
        .with_context("scope_key", scope_key)
        .with_source(error)),
    }
}

#[cfg(feature = "messaging")]
fn log_self_observation_failure(
    node_name: &str,
    metric_name: &str,
    error: &crate::self_observation::SelfObservationError,
) {
    warn!(
        node = node_name,
        metric = metric_name,
        error = %error,
        "Derived-node self-observation emit failed"
    );
}


/// Shared runtime adapter for all derived node models.
///
/// Generic over `N: DerivedNodeImpl`, which is implemented by the wrapper types
/// `TransducerWrapper`, `WindowedWrapper`, `ScopeReconcilerWrapper`.
///
/// # Type Aliases
///
/// Users typically use one of:
/// - `TransducerNodeAdapter<T>` = `DerivedNodeAdapter<TransducerWrapper<T>>`
/// - `WindowedNodeAdapter<T>` = `DerivedNodeAdapter<WindowedWrapper<T>>`
/// - `ScopeReconcilerNodeAdapter<T>` = `DerivedNodeAdapter<ScopeReconcilerWrapper<T>>`
pub struct DerivedNodeAdapter<N>
where
    N: DerivedNodeImpl,
{
    node: N,
    persisted_state: PersistedState<N::State>,
    config: DerivedNodeConfig,
    shutdown_config: ShutdownConfig,
    runtime: Option<NodeRuntimeState>,
    checkpoint_manager: Option<Arc<CheckpointManager>>,
    event_emitter: Option<EventEmitter>,
    shutdown_tx: Option<Arc<RuntimeDrainController>>,
    host: String,
    events_since_checkpoint: u64,
    last_checkpoint_time: Instant,
    last_revision: u64,
    pending_hot_reload_cleanup: Option<PathBuf>,
    /// Consecutive checkpoint save failures. Reset to 0 on any successful save.
    /// When this reaches 3, processing is halted to prevent silent progress loss.
    consecutive_checkpoint_failures: u32,
    /// Inputs processed since this process/run started. This intentionally
    /// differs from persisted checkpoint totals, which survive restarts.
    run_events_processed: u64,
    /// Per-event lag samples (ms between `event.ts_orig` and dispatch).
    /// Drives `derived.event_lag_p50_ms` / `derived.event_lag_p99_ms`.
    lag_window: super::histograms::LatencyWindow,
    /// Per-tick wall-time samples (ms inside `node.process_derived`).
    /// Drives `derived.tick_runtime_p99_ms`.
    runtime_window: super::histograms::LatencyWindow,
    /// Sliding-window event count for `derived.throughput_eps`.
    throughput_window: super::histograms::ThroughputWindow,
    #[cfg(feature = "messaging")]
    health_reporter: Option<Arc<crate::health_reporter::HealthReporter>>,
    #[cfg(feature = "messaging")]
    self_observer: Option<Arc<crate::self_observation::SelfObserver>>,
}

impl<N> DerivedNodeAdapter<N>
where
    N: DerivedNodeImpl,
{
    /// Create a new adapter wrapping the given node implementation.
    pub fn with_node(node: N) -> Self {
        Self {
            node,
            persisted_state: PersistedState::default(),
            config: DerivedNodeConfig::default(),
            shutdown_config: ShutdownConfig::default(),
            runtime: None,
            checkpoint_manager: None,
            event_emitter: None,
            shutdown_tx: None,
            host: gethostname::gethostname().to_string_lossy().to_string(),
            events_since_checkpoint: 0,
            last_checkpoint_time: Instant::now(),
            last_revision: 0,
            pending_hot_reload_cleanup: None,
            consecutive_checkpoint_failures: 0,
            run_events_processed: 0,
            lag_window: super::histograms::LatencyWindow::new(
                super::histograms::DEFAULT_LATENCY_RESERVOIR,
            ),
            runtime_window: super::histograms::LatencyWindow::new(
                super::histograms::DEFAULT_LATENCY_RESERVOIR,
            ),
            throughput_window: super::histograms::ThroughputWindow::new(
                super::histograms::THROUGHPUT_WINDOW,
            ),
            #[cfg(feature = "messaging")]
            health_reporter: None,
            #[cfg(feature = "messaging")]
            self_observer: None,
        }
    }

    /// Create a new adapter (alias for `with_node`).
    pub fn new(node: N) -> Self {
        Self::with_node(node)
    }

    /// Create with custom config.
    pub fn with_config(node: N, config: DerivedNodeConfig) -> Self {
        let mut adapter = Self::with_node(node);
        adapter.config = config;
        adapter
    }

    /// Create with custom shutdown config.
    pub fn with_shutdown_config(node: N, shutdown_config: ShutdownConfig) -> Self {
        let mut adapter = Self::with_node(node);
        adapter.shutdown_config = shutdown_config;
        adapter
    }
}

impl<N> Default for DerivedNodeAdapter<N>
where
    N: DerivedNodeImpl + Default,
{
    fn default() -> Self {
        Self::with_node(N::default())
    }
}

impl<N> DerivedNodeAdapter<N>
where
    N: DerivedNodeImpl,
{
    // ── Public Accessors ───────────────────────────────────────────────

    /// Get the node's current state.
    pub fn state(&self) -> &N::State {
        &self.persisted_state.state
    }

    /// Get the number of events processed.
    pub fn events_processed(&self) -> u64 {
        self.persisted_state.events_processed
    }

    /// Signal shutdown.
    pub fn signal_shutdown(&self) {
        if let Some(tx) = &self.shutdown_tx {
            request_runtime_drain(tx, self.node.name());
        }
    }

    /// Read-only access to the lag-sample reservoir. Operators usually want
    /// the percentiles via the emitted `derived.event_lag_p*_ms` gauges,
    /// but the heavy-lane scenario tests assert on percentile bounds
    /// directly without spinning up a metrics scrape.
    #[must_use]
    pub fn lag_window(&self) -> &super::histograms::LatencyWindow {
        &self.lag_window
    }

    /// Read-only access to the per-tick runtime reservoir.
    #[must_use]
    pub fn runtime_window(&self) -> &super::histograms::LatencyWindow {
        &self.runtime_window
    }

    /// Mutable access to the throughput sliding window. Mutation is required
    /// because `eps()` evicts stale samples on read.
    pub fn throughput_window_mut(&mut self) -> &mut super::histograms::ThroughputWindow {
        &mut self.throughput_window
    }
}

// ── Invalidation subscription helper ─────────────────────────────────

/// Receive the next invalidation message payload from a NATS subscriber.
/// Returns `None` only when the subscription stream ends.
/// When `sub` is `None` (no NATS available), pends forever — effectively
/// disabling the select arm without needing `#[cfg]` inside `tokio::select!`.
#[cfg(feature = "messaging")]
async fn recv_invalidation(
    sub: &mut Option<async_nats::jetstream::consumer::push::Messages>,
) -> Option<Vec<u8>> {
    use futures::StreamExt;
    match sub.as_mut() {
        Some(s) => match s.next().await {
            Some(Ok(msg)) => {
                let payload = msg.payload.to_vec();
                // Ack so the JetStream consumer does not redeliver this
                // invalidation message after the ack wait timeout.
                if let Err(e) = msg.ack().await {
                    warn!("Failed to ack invalidation message: {e}");
                }
                Some(payload)
            }
            Some(Err(e)) => {
                warn!("Error receiving invalidation message: {e}");
                None
            }
            None => None,
        },
        None => std::future::pending().await,
    }
}

/// Stub when messaging feature is disabled — always pends.
#[cfg(not(feature = "messaging"))]
async fn recv_invalidation(_sub: &mut ()) -> Option<Vec<u8>> {
    std::future::pending().await
}

/// Wall time between an event's `ts_orig` and now, expressed in
/// milliseconds. Returns `0.0` when `ts_orig` is missing or in the future
/// (clock skew / synthesized timestamps); returns `f64::NAN` only on
/// arithmetic overflow, in which case the gauge emit is skipped.
fn event_lag_ms(event: &Event<JsonValue>) -> f64 {
    let Some(ts_orig) = event.ts_orig else {
        return 0.0;
    };
    let now = sinex_primitives::Timestamp::now();
    let delta = now - ts_orig;
    let ms = delta.whole_milliseconds();
    if ms <= 0 { 0.0 } else { ms as f64 }
}

fn checkpoint_resume_event_id(checkpoint: &Checkpoint) -> Option<Uuid> {
    match checkpoint {
        Checkpoint::Internal { event_id, .. } => Some(*event_id),
        Checkpoint::Stream {
            event_id: Some(event_id),
            ..
        } => Some(*event_id),
        _ => None,
    }
}

fn restore_resume_position<S>(persisted: &mut PersistedState<S>, checkpoint: &Checkpoint) {
    if persisted.last_input_event_id.is_none() {
        persisted.last_input_event_id = checkpoint_resume_event_id(checkpoint);
    }
}

fn historical_resume_position(
    from: &Checkpoint,
    end_time: Timestamp,
) -> NodeResult<(TimeRange, Option<sinex_primitives::Cursor>)> {
    let full_range = || {
        TimeRange::new(None, Some(end_time))
            .map_err(|e| SinexError::validation(format!("Invalid time range: {e}")))
    };

    match from {
        Checkpoint::None => Ok((full_range()?, None)),
        Checkpoint::Internal { event_id, .. } => Ok((
            full_range()?,
            Some(sinex_primitives::Cursor::after_id(Id::from_uuid(*event_id))),
        )),
        Checkpoint::Stream {
            event_id: Some(event_id),
            ..
        } => Ok((
            full_range()?,
            Some(sinex_primitives::Cursor::after_id(Id::from_uuid(*event_id))),
        )),
        Checkpoint::Timestamp { timestamp, .. } => Ok((
            TimeRange::new(Some(*timestamp), Some(end_time))
                .map_err(|e| SinexError::validation(format!("Invalid time range: {e}")))?,
            None,
        )),
        Checkpoint::Stream {
            message_id,
            event_id: None,
        } => Err(SinexError::validation(
            "Derived historical replay cannot resume from a stream checkpoint without event_id",
        )
        .with_context("message_id", message_id.clone())),
        Checkpoint::External { description, .. } => Err(SinexError::validation(
            "Derived historical replay cannot resume from an external state-only checkpoint",
        )
        .with_context("description", description.clone())),
    }
}

// ── Node trait implementation ──────────────────────────────────────────

impl<N> crate::runtime::stream::Node for DerivedNodeAdapter<N>
where
    N: DerivedNodeImpl,
{
    type Config = DerivedNodeConfig;

    async fn initialize(&mut self, init: NodeInitContext<Self::Config>) -> NodeResult<()> {
        let (config, runtime) = init.into_runtime();
        self.config = config;

        self.checkpoint_manager = Some(runtime.checkpoint_manager().clone());
        self.event_emitter = Some(runtime.event_emitter().clone());
        self.host = runtime.service_info().host().to_string();

        #[cfg(feature = "messaging")]
        {
            if let Some(nats_client) = runtime.nats_client() {
                use crate::health_reporter::{HealthReporter, HealthThresholds};
                use crate::self_observation::{SelfObserver, SelfObserverConfig};

                let health_enabled = env_bool_with_default(
                    "SINEX_HEALTH_MONITORING_ENABLED",
                    true,
                    "derived node health monitoring",
                );

                if health_enabled {
                    let config = SelfObserverConfig {
                        component: self.node.name().to_string(),
                        namespace: None,
                        enabled: true,
                        min_emission_interval: std::time::Duration::from_secs(1),
                    };

                    let observer = Arc::new(SelfObserver::new(nats_client, config));
                    let thresholds = HealthThresholds::from_env().unwrap_or_else(|error| {
                        warn!(
                            node = %self.node.name(),
                            error = %error,
                            "Invalid health monitoring threshold override; using defaults"
                        );
                        HealthThresholds::default()
                    });

                    self.health_reporter = Some(Arc::new(HealthReporter::new(
                        self.node.name().to_string(),
                        Arc::clone(&observer),
                        thresholds,
                    )));
                    self.self_observer = Some(observer);

                    info!(node = %self.node.name(), "Health monitoring auto-enabled");
                }
            }
        }

        self.runtime = Some(runtime);
        self.load_state().await?;

        self.node
            .on_initialize_derived(&self.persisted_state.state)
            .await
            .map_err(|e| SinexError::processing(format!("Initialize hook failed: {e}")))?;

        info!(
            node = %self.node.name(),
            model = %self.node.node_model(),
            events_processed = self.persisted_state.events_processed,
            "DerivedNode initialized"
        );

        Ok(())
    }

    async fn scan(
        &mut self,
        from: Checkpoint,
        until: TimeHorizon,
        args: ScanArgs,
    ) -> NodeResult<ScanReport> {
        match until {
            TimeHorizon::Continuous => self.run_continuous(from).await,
            TimeHorizon::Historical { end_time } => self.run_historical(from, end_time, args).await,
            TimeHorizon::Snapshot => Err(SinexError::unknown(
                "DerivedNode does not support snapshot mode",
            )),
        }
    }

    fn node_name(&self) -> &str {
        self.node.name()
    }

    fn node_type(&self) -> NodeType {
        NodeType::Automaton
    }

    fn capabilities(&self) -> NodeCapabilities {
        NodeCapabilities {
            supports_continuous: true,
            supports_snapshot: false,
            supports_interactive: false,
            max_scan_size: None,
            supports_concurrent: false,
            manages_own_continuous_loop: false,
            manages_own_checkpoints: true,
            ..NodeCapabilities::default()
        }
    }

    async fn current_checkpoint(&self) -> NodeResult<Checkpoint> {
        Ok(self.current_checkpoint_internal())
    }

    async fn health_check(&self) -> NodeResult<bool> {
        let runtime_initialized = self.runtime.is_some();
        if !runtime_initialized {
            return Ok(false);
        }

        Ok(self.health_reporter.as_ref().is_none_or(|reporter| {
            reporter.current_status()
                == sinex_primitives::events::payloads::process::ProcessStatus::Healthy
        }))
    }

    async fn process_event_batch(
        &mut self,
        events: Vec<Event<JsonValue>>,
    ) -> NodeResult<ProcessingStats> {
        let matching = self.filter_matching_events(events);

        if matching.is_empty() {
            return Ok(ProcessingStats::default());
        }

        let batch_size = matching.len();
        // Sample the lag of the oldest event in the batch — operators
        // care about the worst-case backlog, not the average.
        let max_lag_ms = matching
            .iter()
            .map(event_lag_ms)
            .fold(0.0_f64, f64::max);
        let start = std::time::Instant::now();
        let outputs = self.process_batch(matching).await?;
        let batch_runtime_ms = start.elapsed().as_secs_f64() * 1000.0;
        // Use a distinct metric name (`derived.batch_runtime_ms`) for the whole-batch
        // runtime so it does not overwrite the per-event `derived.tick_runtime_ms`
        // samples emitted from the per-event NATS processing path.
        self.observe_batch_processing_latency(max_lag_ms, batch_runtime_ms, batch_size)
            .await;
        let output_count = outputs.len();

        if !outputs.is_empty() {
            self.emit_output_events(outputs, "event bridge batch")
                .await?;
        }

        debug!(
            node = %self.node.name(),
            input_count = batch_size,
            output_count,
            "Processed event batch via bridge"
        );

        Ok(ProcessingStats {
            processed: batch_size,
            duration: std::time::Duration::from_secs_f64(batch_runtime_ms / 1000.0),
            ..ProcessingStats::default()
        })
    }

    async fn shutdown(&mut self) -> NodeResult<()> {
        info!(node = %self.node.name(), "Shutting down DerivedNode");

        self.signal_shutdown();

        self.node
            .on_shutdown_derived(&self.persisted_state.state)
            .await
            .map_err(|e| {
                error!(node = %self.node.name(), error = %e, "Shutdown hook failed");
                e
            })?;

        let mut file_save_success = true;
        if let Err(e) = self.save_state_to_file().await {
            warn!(node = %self.node.name(), error = %e, "Failed to save state to file");
            file_save_success = false;
        }

        let mut nats_save_success = true;
        if let Err(e) = self.save_state().await {
            warn!(node = %self.node.name(), error = %e, "Failed to save final checkpoint");
            nats_save_success = false;
        }

        if !nats_save_success {
            return Err(SinexError::checkpoint(format!(
                "Node {} failed to save final checkpoint to NATS KV during shutdown \
                 (file save {})",
                self.node.name(),
                if file_save_success {
                    "succeeded"
                } else {
                    "also failed"
                }
            )));
        }

        Ok(())
    }

    async fn estimate_scan_scope(
        &self,
        _from: &Checkpoint,
        _until: &TimeHorizon,
        _args: &ScanArgs,
    ) -> NodeResult<ScanEstimate> {
        Ok(ScanEstimate::default())
    }
}

// ── ExplorationProvider ────────────────────────────────────────────────

impl<N> crate::exploration::ExplorationProvider for DerivedNodeAdapter<N>
where
    N: DerivedNodeImpl,
{
    fn get_source_state(&self) -> NodeResult<crate::exploration::SourceState> {
        let runtime_initialized = self.runtime.is_some();
        let node_name = self.node.name();
        let node_model = self.node.node_model();
        let health_status = self
            .health_reporter
            .as_ref()
            .map(|reporter| reporter.current_status());
        let healthy = runtime_initialized
            && health_status.is_none_or(|status| {
                status == sinex_primitives::events::payloads::process::ProcessStatus::Healthy
            });
        let description = if !runtime_initialized {
            format!("{node_name} derived node ({node_model}, runtime not initialized)")
        } else if let Some(status) = health_status {
            format!("{node_name} derived node ({node_model}, status={status})")
        } else {
            format!("{node_name} derived node ({node_model})")
        };

        Ok(crate::exploration::SourceState {
            is_connected: runtime_initialized,
            healthy,
            description,
            last_updated: None,
            lag_seconds: None,
            recent_activity: Vec::new(),
            total_items: None,
            metadata: [
                (
                    "runtime_initialized".to_string(),
                    serde_json::json!(runtime_initialized),
                ),
                ("node_model".to_string(), serde_json::json!(node_model)),
            ]
            .into_iter()
            .chain(health_status.map(|status| {
                (
                    "health_status".to_string(),
                    serde_json::json!(status.to_string()),
                )
            }))
            .collect(),
        })
    }

    fn get_ingestion_history(
        &self,
        _limit: u64,
    ) -> NodeResult<Vec<crate::exploration::IngestionHistoryEntry>> {
        Err(SinexError::invalid_state(
            "ingestion history is not implemented for derived nodes",
        ))
    }

    fn get_coverage_analysis(
        &self,
        _time_range: Option<(Timestamp, Timestamp)>,
    ) -> NodeResult<crate::exploration::CoverageAnalysis> {
        crate::exploration::coverage_analysis_unavailable(
            "coverage analysis is not implemented for derived nodes",
        )
    }

    fn export_data(
        &self,
        _path: &sinex_primitives::domain::SanitizedPath,
        _format: crate::exploration::ExportFormat,
    ) -> NodeResult<()> {
        Err(SinexError::invalid_state(
            "data export is not implemented for derived nodes",
        ))
    }
}

// ── Type aliases for user-facing API ───────────────────────────────────

/// Adapter for a `TransducerNode` implementation.
pub type TransducerNodeAdapter<N> = DerivedNodeAdapter<super::traits::TransducerWrapper<N>>;

/// Adapter for a `WindowedNode` implementation.
pub type WindowedNodeAdapter<N> = DerivedNodeAdapter<super::traits::WindowedWrapper<N>>;

/// Adapter for a `ScopeReconcilerNode` implementation.
pub type ScopeReconcilerNodeAdapter<N> =
    DerivedNodeAdapter<super::traits::ScopeReconcilerWrapper<N>>;


#[cfg(test)]
mod tests;

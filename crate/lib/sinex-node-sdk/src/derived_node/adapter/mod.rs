//! `DerivedNodeAdapter` — shared runtime adapter for all derived node models.
//!
//! Wraps any [`DerivedNodeImpl`] and implements the stream [`Node`] trait,
//! handling checkpoints, health monitoring, shutdown, and event emission.

mod invalidate;
mod run;

use super::context::DerivedTriggerContext;
use super::output::DerivedOutput;
use super::traits::{DerivedNodeConfig, DerivedNodeImpl, InputProvenanceFilter};

use crate::checkpoint::{CheckpointManager, CheckpointState, decode_checkpoint_data};
use crate::error_helpers::{env_bool_with_default, env_parse_with_default};
use crate::ids::deterministic_event_id;
use crate::processing::{ErrorAction, PersistedState};
use crate::runtime::stream::{
    Checkpoint, EventEmitter, NodeCapabilities, NodeInitContext, NodeRuntimeState, NodeType,
    ProcessingStats, RuntimeDrainController, ScanArgs, ScanEstimate, ScanReport, TimeHorizon,
};
use crate::shutdown::ShutdownConfig;
use crate::{NodeResult, SinexError};

use sinex_primitives::events::Event;
use sinex_primitives::events::builder::{Operation, Provenance};
use sinex_primitives::non_empty::NonEmptyVec;
use sinex_primitives::privacy;
use sinex_primitives::query::{EventQuery, EventQueryResult, QueryResultEvent, TimeRange};
use sinex_primitives::temporal::Timestamp;
use sinex_primitives::{EventSource, EventType, HostName, Id, JsonValue, Pagination, Uuid};

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
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
    stale_query_result: Result<Vec<QueryResultEvent>, SinexError>,
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
    fn input_event_type_matches(&self, event: &Event<JsonValue>) -> bool {
        let input_type = self.node.input_event_type();
        input_type == "*" || event.event_type.as_ref() == input_type
    }

    fn input_provenance_filter(&self) -> InputProvenanceFilter {
        self.node.input_provenance_filter()
    }

    fn input_query_has_lineage(&self) -> Option<bool> {
        self.input_provenance_filter().query_has_lineage()
    }

    fn input_query_event_types(&self) -> Result<Vec<EventType>, SinexError> {
        let input_type = self.node.input_event_type();
        if input_type == "*" {
            Ok(Vec::new())
        } else {
            Ok(vec![EventType::new(input_type)?])
        }
    }

    fn event_matches_input(&self, event: &Event<JsonValue>) -> bool {
        self.input_event_type_matches(event) && self.input_provenance_filter().matches_event(event)
    }

    fn filter_matching_events(&self, events: Vec<Event<JsonValue>>) -> Vec<Event<JsonValue>> {
        events
            .into_iter()
            .filter(|event| self.event_matches_input(event))
            .collect()
    }

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
    async fn cleanup_hot_reload_file_best_effort(
        path: &Path,
        node_name: &str,
        reason: &'static str,
    ) {
        if let Err(error) = CheckpointState::delete_file(path).await {
            warn!(
                node = node_name,
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
    ) -> NodeResult<()> {
        let Some(path) = self.pending_hot_reload_cleanup.take() else {
            return Ok(());
        };

        match CheckpointState::delete_file(&path).await {
            Ok(()) => Ok(()),
            Err(delete_error) => {
                warn!(
                    node = %self.node.name(),
                    path = %path.display(),
                    error = %delete_error,
                    "Failed to delete restored hot reload checkpoint file after syncing to NATS KV; rewriting it with the latest durable state"
                );
                checkpoint_state.save_to_file(&path).await.map_err(|error| {
                    SinexError::io(
                        "Failed to synchronize restored hot reload file after checkpoint save",
                    )
                    .with_context("node", self.node.name())
                    .with_context("path", path.display().to_string())
                    .with_context("delete_error", delete_error.to_string())
                    .with_std_error(&error)
                })
            }
        }
    }

    #[cfg(feature = "db")]
    async fn load_query_events_paginated(
        &self,
        pool: &sinex_db::DbPool,
        mut query: EventQuery,
        scope_key: &str,
        query_kind: &'static str,
    ) -> NodeResult<Vec<QueryResultEvent>> {
        use sinex_db::DbPoolExt;

        let mut collected = Vec::new();
        let mut cursor = query.cursor.take();
        let mut pages = 0usize;

        loop {
            query.cursor = cursor.clone();
            query.limit = INVALIDATION_QUERY_PAGE_SIZE;

            let result = pool.events().query(query.clone()).await.map_err(|e| {
                SinexError::database(format!(
                    "Failed to load {query_kind} page {} for scope '{scope_key}': {e}",
                    pages + 1
                ))
            })?;

            let (mut page_events, next_cursor) = match result {
                EventQueryResult::Events {
                    events,
                    next_cursor,
                    ..
                } => (events, next_cursor),
                other => {
                    return Err(SinexError::processing(format!(
                        "{query_kind} unexpectedly returned non-event result during invalidation: {other:?}"
                    ))
                    .with_context("scope_key", scope_key)
                    .with_context("node", self.node.name()));
                }
            };

            if page_events.is_empty() {
                break;
            }

            pages += 1;
            collected.append(&mut page_events);

            cursor = next_cursor;

            if cursor.is_none() {
                break;
            }
        }

        if pages > 1 {
            info!(
                node = %self.node.name(),
                scope_key,
                query_kind,
                pages,
                rows = collected.len(),
                page_size = INVALIDATION_QUERY_PAGE_SIZE,
                "Loaded invalidation query across multiple pages"
            );
        }

        Ok(collected)
    }

    async fn send_to_processing_failure_queue_or_fail(
        &self,
        event: &Event<JsonValue>,
        error: &crate::NodeLogicError,
    ) -> NodeResult<()> {
        let Some(runtime) = self.runtime.as_ref() else {
            return Err(SinexError::lifecycle(
                "derived-node requested processing-failure routing but no transport runtime is available",
            )
            .with_context("node", self.node.name())
            .with_context("event_type", event.event_type.as_ref())
            .with_context("source", event.source.as_ref())
            .with_context("reason", error.to_string()));
        };
        let transport = runtime.handles().transport();
        transport
            .send_to_processing_failure_queue(event, &error.to_string(), self.node.name())
            .await
            .map_err(|failure_err| {
                SinexError::processing(
                    "failed to send derived-node event to processing-failure stream",
                )
                    .with_context("node", self.node.name())
                    .with_context("event_type", event.event_type.as_ref())
                    .with_context("source", event.source.as_ref())
                    .with_context("reason", error.to_string())
                    .with_std_error(&failure_err)
            })
    }

    async fn emit_output_events(
        &self,
        outputs: Vec<Event<JsonValue>>,
        context: &'static str,
    ) -> NodeResult<u64> {
        let count = outputs.len() as u64;
        if count == 0 {
            return Ok(0);
        }

        let emitter = self.event_emitter.as_ref().ok_or_else(|| {
            SinexError::lifecycle("derived-node output channel is not initialized")
                .with_context("node", self.node.name())
                .with_context("context", context)
        })?;

        for event in outputs {
            let event_id = event
                .id
                .map_or_else(|| "<none>".to_string(), |id| id.to_string());
            let event_source = event.source.as_ref().to_string();
            let event_type = event.event_type.as_ref().to_string();

            emitter.emit(event).await.map_err(|error| {
                SinexError::lifecycle("failed to emit derived-node output event")
                    .with_context("node", self.node.name())
                    .with_context("context", context)
                    .with_context("event_id", event_id)
                    .with_context("source", event_source)
                    .with_context("event_type", event_type)
                    .with_source(error)
            })?;
        }

        Ok(count)
    }

    // ── Checkpoint Management ──────────────────────────────────────────

    async fn load_state(&mut self) -> NodeResult<()> {
        let hot_reload_path = self.shutdown_config.checkpoint_path(self.node.name());
        let mut invalid_hot_reload_file = None;

        // Priority 1: file-based checkpoint (hot reload)
        if self.shutdown_config.restore_state_on_startup {
            match self.try_restore_from_file().await {
                Ok(Some((persisted, revision))) => {
                    self.persisted_state = persisted;
                    self.last_revision = revision;
                    return Ok(());
                }
                Ok(None) => {}
                Err(error) if self.checkpoint_manager.is_some() => {
                    warn!(
                        node = %self.node.name(),
                        path = %hot_reload_path.display(),
                        error = %error,
                        "Failed to restore hot reload checkpoint file; falling back to NATS KV"
                    );
                    invalid_hot_reload_file = Some(hot_reload_path.clone());
                }
                Err(error) => return Err(error),
            }
        }

        // Priority 2: NATS KV checkpoint
        let Some(checkpoint_mgr) = &self.checkpoint_manager else {
            return Ok(());
        };

        let checkpoint_state = checkpoint_mgr.load_checkpoint().await?;
        match checkpoint_state.data {
            Some(data) => {
                let mut persisted: PersistedState<N::State> =
                    decode_checkpoint_data(data, "derived checkpoint state", self.node.name())?;
                restore_resume_position(&mut persisted, &checkpoint_state.checkpoint);
                info!(
                    node = %self.node.name(),
                    events_processed = persisted.events_processed,
                    "Restored state from NATS KV checkpoint"
                );
                self.persisted_state = persisted;
                self.last_revision = checkpoint_state.revision;
            }
            None if matches!(checkpoint_state.checkpoint, Checkpoint::None) => {
                warn!(
                    node = %self.node.name(),
                    "No valid checkpoint for derived node; replaying full historical input"
                );
                self.persisted_state = PersistedState::default();
                self.last_revision = checkpoint_state.revision;
            }
            None => {
                return Err(SinexError::checkpoint(
                    "Derived checkpoint KV entry is missing state data",
                )
                .with_context("node", self.node.name()));
            }
        }

        if let Some(path) = invalid_hot_reload_file {
            Self::cleanup_hot_reload_file_best_effort(
                &path,
                self.node.name(),
                "discarding invalid hot reload checkpoint file after successful NATS KV restore",
            )
            .await;
        }

        Ok(())
    }

    async fn try_restore_from_file(
        &mut self,
    ) -> NodeResult<Option<(PersistedState<N::State>, u64)>> {
        let checkpoint_path = self.shutdown_config.checkpoint_path(self.node.name());
        let Some(file_state) = CheckpointState::load_from_file(&checkpoint_path).await? else {
            return Ok(None);
        };
        let Some(data) = file_state.data else {
            return Err(SinexError::checkpoint(
                "Derived hot reload checkpoint file is missing state data",
            )
            .with_context("node", self.node.name())
            .with_context("path", checkpoint_path.display().to_string()));
        };

        let mut persisted: PersistedState<N::State> =
            decode_checkpoint_data(data, "derived hot reload state", self.node.name())?;
        restore_resume_position(&mut persisted, &file_state.checkpoint);
        info!(
            node = %self.node.name(),
            events_processed = persisted.events_processed,
            "Restored state from hot reload file"
        );
        self.pending_hot_reload_cleanup = Some(checkpoint_path);
        Ok(Some((persisted, file_state.revision)))
    }

    pub async fn save_state_to_file(&self) -> std::io::Result<()> {
        if !self.shutdown_config.save_state_on_shutdown {
            return Ok(());
        }

        let checkpoint_path = self.shutdown_config.checkpoint_path(self.node.name());
        let state_json = serde_json::to_value(&self.persisted_state)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

        let checkpoint_state = CheckpointState {
            checkpoint: self.checkpoint_position(),
            processed_count: self.persisted_state.events_processed,
            last_activity: Timestamp::now(),
            data: Some(state_json),
            version: 2,
            revision: self.last_revision,
        };

        checkpoint_state.save_to_file(&checkpoint_path).await
    }

    async fn save_state(&mut self) -> NodeResult<()> {
        let Some(checkpoint_mgr) = &self.checkpoint_manager else {
            return Ok(());
        };

        self.persisted_state.last_checkpoint = Timestamp::now();
        let state_json = serde_json::to_value(&self.persisted_state)
            .map_err(|e| SinexError::processing(format!("Failed to serialize state: {e}")))?;

        let mut checkpoint_state = CheckpointState {
            checkpoint: self.checkpoint_position(),
            processed_count: self.persisted_state.events_processed,
            last_activity: Timestamp::now(),
            data: Some(state_json),
            version: 2,
            revision: self.last_revision,
        };

        self.last_revision = checkpoint_mgr.save_checkpoint(&checkpoint_state).await?;
        checkpoint_state.revision = self.last_revision;
        self.finalize_restored_hot_reload_file(&checkpoint_state)
            .await?;
        self.events_since_checkpoint = 0;
        self.last_checkpoint_time = Instant::now();
        self.observe_checkpoint_state(&checkpoint_state).await;

        debug!(
            node = %self.node.name(),
            events_processed = self.persisted_state.events_processed,
            revision = self.last_revision,
            "Saved checkpoint"
        );

        Ok(())
    }

    fn should_checkpoint(&self) -> bool {
        self.events_since_checkpoint >= self.config.checkpoint_interval
            || self.last_checkpoint_time.elapsed()
                >= Duration::from_secs(self.config.checkpoint_timeout_secs)
    }

    fn checkpoint_position(&self) -> Checkpoint {
        if let Some(event_id) = self.persisted_state.last_input_event_id {
            return Checkpoint::internal(event_id, self.persisted_state.events_processed);
        }

        if self.persisted_state.events_processed > 0 {
            return Checkpoint::timestamp(self.persisted_state.last_checkpoint, None);
        }

        Checkpoint::None
    }

    fn current_checkpoint_internal(&self) -> Checkpoint {
        self.checkpoint_position()
    }

    fn record_processed_input(&mut self, event_id: Id<Event<JsonValue>>) {
        self.persisted_state.last_input_event_id = Some(*event_id.as_uuid());
        self.persisted_state.events_processed += 1;
        self.events_since_checkpoint += 1;
        self.run_events_processed = self.run_events_processed.saturating_add(1);
    }

    fn record_state_mutation(&mut self) {
        self.events_since_checkpoint += 1;
    }

    #[cfg(feature = "messaging")]
    fn derived_metric_labels(&self) -> HashMap<String, String> {
        let mut labels = HashMap::new();
        labels.insert("node".to_string(), self.node.name().to_string());
        labels.insert("node_model".to_string(), self.node.node_model().to_string());
        if let Some(node_run_id) = self
            .runtime
            .as_ref()
            .and_then(NodeRuntimeState::node_run_id)
        {
            labels.insert("node_run_id".to_string(), node_run_id.to_string());
        }
        labels
    }

    #[cfg(feature = "messaging")]
    fn checkpoint_labels(&self, checkpoint: &Checkpoint) -> HashMap<String, String> {
        let mut labels = self.derived_metric_labels();
        let (kind, position) = match checkpoint {
            Checkpoint::None => ("none", None),
            Checkpoint::External {
                position,
                description,
            } => ("external", Some(format!("{description}:{position}"))),
            Checkpoint::Internal {
                event_id,
                message_count,
            } => ("internal", Some(format!("{event_id}:#{message_count}"))),
            Checkpoint::Stream {
                message_id,
                event_id,
            } => (
                "stream",
                Some(match event_id {
                    Some(event_id) => format!("{message_id}:{event_id}"),
                    None => message_id.clone(),
                }),
            ),
            Checkpoint::Timestamp { timestamp, .. } => {
                ("timestamp", Some(timestamp.format_rfc3339()))
            }
        };
        labels.insert("checkpoint_kind".to_string(), kind.to_string());
        if let Some(position) = position {
            labels.insert("checkpoint_position".to_string(), position);
        }
        labels
    }

    #[cfg(feature = "messaging")]
    async fn observe_runtime_snapshot(&self) {
        let Some(obs) = self.self_observer.as_ref() else {
            return;
        };

        let labels = self.derived_metric_labels();
        if let Err(error) = obs
            .emit_gauge(
                "derived.events_processed.run",
                self.run_events_processed as f64,
                Some(labels.clone()),
            )
            .await
        {
            log_self_observation_failure(self.node.name(), "derived.events_processed.run", &error);
        }

        if let Some(reporter) = self.health_reporter.as_ref() {
            let error_rate = reporter.metrics().error_rate(300);
            if let Err(error) = obs
                .emit_gauge("derived.error_rate_5m", error_rate, Some(labels))
                .await
            {
                log_self_observation_failure(self.node.name(), "derived.error_rate_5m", &error);
            }
        }
    }

    #[cfg(not(feature = "messaging"))]
    async fn observe_runtime_snapshot(&self) {}

    /// Emit per-event processing-latency gauges (point-in-time + percentile)
    /// so operators can see how a derived node is keeping up with its input
    /// stream. Each call records the latest sample into the in-process
    /// reservoirs and emits both the last-value gauge and the latest
    /// percentile read.
    ///
    /// Gauges:
    /// - `derived.event_lag_ms` — last lag sample (wall time between
    ///   upstream `ts_orig` and dispatch).
    /// - `derived.tick_runtime_ms` — last runtime sample.
    /// - `derived.event_lag_p50_ms`, `derived.event_lag_p99_ms` — sliding
    ///   reservoir percentiles over the last `DEFAULT_LATENCY_RESERVOIR`
    ///   samples.
    /// - `derived.tick_runtime_p99_ms` — same reservoir, runtime samples.
    /// - `derived.throughput_eps` — events per second over the live
    ///   `THROUGHPUT_WINDOW`.
    #[cfg(feature = "messaging")]
    async fn observe_processing_latency(&mut self, lag_ms: f64, runtime_ms: f64) {
        // Feed the windows regardless of self_observer presence so unit
        // tests and feature-gated builds keep accurate state.
        if lag_ms.is_finite() {
            self.lag_window.record(lag_ms);
        }
        if runtime_ms.is_finite() {
            self.runtime_window.record(runtime_ms);
        }
        self.throughput_window.record(Instant::now());

        let Some(obs) = self.self_observer.as_ref() else {
            return;
        };
        let labels = self.derived_metric_labels();

        if lag_ms.is_finite() {
            if let Err(error) = obs
                .emit_gauge("derived.event_lag_ms", lag_ms, Some(labels.clone()))
                .await
            {
                log_self_observation_failure(self.node.name(), "derived.event_lag_ms", &error);
            }
        }

        if runtime_ms.is_finite() {
            if let Err(error) = obs
                .emit_gauge("derived.tick_runtime_ms", runtime_ms, Some(labels.clone()))
                .await
            {
                log_self_observation_failure(self.node.name(), "derived.tick_runtime_ms", &error);
            }
        }

        if let Some(p50) = self.lag_window.percentile(0.5) {
            if let Err(error) = obs
                .emit_gauge("derived.event_lag_p50_ms", p50, Some(labels.clone()))
                .await
            {
                log_self_observation_failure(
                    self.node.name(),
                    "derived.event_lag_p50_ms",
                    &error,
                );
            }
        }
        if let Some(p99) = self.lag_window.percentile(0.99) {
            if let Err(error) = obs
                .emit_gauge("derived.event_lag_p99_ms", p99, Some(labels.clone()))
                .await
            {
                log_self_observation_failure(
                    self.node.name(),
                    "derived.event_lag_p99_ms",
                    &error,
                );
            }
        }
        if let Some(p99) = self.runtime_window.percentile(0.99) {
            if let Err(error) = obs
                .emit_gauge("derived.tick_runtime_p99_ms", p99, Some(labels.clone()))
                .await
            {
                log_self_observation_failure(
                    self.node.name(),
                    "derived.tick_runtime_p99_ms",
                    &error,
                );
            }
        }

        let eps = self.throughput_window.eps(Instant::now());
        if let Err(error) = obs
            .emit_gauge("derived.throughput_eps", eps, Some(labels))
            .await
        {
            log_self_observation_failure(self.node.name(), "derived.throughput_eps", &error);
        }
    }

    #[cfg(not(feature = "messaging"))]
    async fn observe_processing_latency(&mut self, lag_ms: f64, runtime_ms: f64) {
        if lag_ms.is_finite() {
            self.lag_window.record(lag_ms);
        }
        if runtime_ms.is_finite() {
            self.runtime_window.record(runtime_ms);
        }
        self.throughput_window.record(Instant::now());
    }

    /// Emit telemetry for a whole-batch processing cycle (event bridge path).
    ///
    /// Emits `derived.batch_runtime_ms` rather than `derived.tick_runtime_ms`
    /// so the batch metric does not overwrite the per-event samples recorded by
    /// `observe_processing_latency`.
    #[cfg(feature = "messaging")]
    async fn observe_batch_processing_latency(
        &mut self,
        lag_ms: f64,
        batch_runtime_ms: f64,
        batch_size: usize,
    ) {
        if lag_ms.is_finite() {
            self.lag_window.record(lag_ms);
        }
        self.throughput_window.record(Instant::now());

        let Some(obs) = self.self_observer.as_ref() else {
            return;
        };
        let mut labels = self.derived_metric_labels();
        labels.insert("batch_size".to_string(), batch_size.to_string());

        if lag_ms.is_finite() {
            if let Err(error) = obs
                .emit_gauge("derived.event_lag_ms", lag_ms, Some(labels.clone()))
                .await
            {
                log_self_observation_failure(self.node.name(), "derived.event_lag_ms", &error);
            }
        }

        if batch_runtime_ms.is_finite() {
            if let Err(error) = obs
                .emit_gauge(
                    "derived.batch_runtime_ms",
                    batch_runtime_ms,
                    Some(labels.clone()),
                )
                .await
            {
                log_self_observation_failure(
                    self.node.name(),
                    "derived.batch_runtime_ms",
                    &error,
                );
            }
        }
    }

    #[cfg(not(feature = "messaging"))]
    async fn observe_batch_processing_latency(
        &mut self,
        lag_ms: f64,
        _batch_runtime_ms: f64,
        _batch_size: usize,
    ) {
        if lag_ms.is_finite() {
            self.lag_window.record(lag_ms);
        }
        self.throughput_window.record(Instant::now());
    }

    #[cfg(feature = "messaging")]
    async fn observe_checkpoint_state(&self, state: &CheckpointState) {
        let Some(obs) = self.self_observer.as_ref() else {
            return;
        };

        let labels = self.checkpoint_labels(&state.checkpoint);
        if let Err(error) = obs
            .emit_gauge(
                "derived.checkpoint.revision",
                state.revision as f64,
                Some(labels),
            )
            .await
        {
            log_self_observation_failure(self.node.name(), "derived.checkpoint.revision", &error);
        }
    }

    #[cfg(not(feature = "messaging"))]
    async fn observe_checkpoint_state(&self, _state: &CheckpointState) {}

    #[cfg(feature = "messaging")]
    async fn observe_pending_invalidations(&self, count: usize) {
        let Some(obs) = self.self_observer.as_ref() else {
            return;
        };

        if let Err(error) = obs
            .emit_gauge(
                "derived.invalidations.pending",
                count as f64,
                Some(self.derived_metric_labels()),
            )
            .await
        {
            log_self_observation_failure(self.node.name(), "derived.invalidations.pending", &error);
        }
    }

    #[cfg(not(feature = "messaging"))]
    async fn observe_pending_invalidations(&self, _count: usize) {}

    fn validate_output_batch(
        &self,
        outputs: &[DerivedOutput<JsonValue>],
        phase: &'static str,
    ) -> NodeResult<()> {
        let mut max_parent_count = 0usize;

        for output in outputs {
            let parent_count = output.source_event_ids.len();
            max_parent_count = max_parent_count.max(parent_count);

            if parent_count > DERIVED_OUTPUT_PARENT_HARD_LIMIT {
                let mut error = SinexError::validation(
                    "derived output exceeds synthesis parent hard limit before persistence",
                )
                .with_context("node", self.node.name())
                .with_context("phase", phase)
                .with_context("output_event_type", self.node.output_event_type())
                .with_context("parent_count", parent_count.to_string())
                .with_context("hard_limit", DERIVED_OUTPUT_PARENT_HARD_LIMIT.to_string());

                if let Some(aggregation) = &output.aggregation {
                    error = error
                        .with_context("aggregation_kind", aggregation.kind.clone())
                        .with_context("rollup_level", aggregation.rollup_level.to_string())
                        .with_context(
                            "logical_input_count",
                            aggregation.total_input_count.to_string(),
                        );
                }

                return Err(error);
            }
        }

        if max_parent_count > DERIVED_OUTPUT_PARENT_WARN_THRESHOLD {
            warn!(
                node = %self.node.name(),
                phase,
                output_event_type = %self.node.output_event_type(),
                output_count = outputs.len(),
                max_parent_count,
                threshold = DERIVED_OUTPUT_PARENT_WARN_THRESHOLD,
                hard_limit = DERIVED_OUTPUT_PARENT_HARD_LIMIT,
                "Derived output batch is approaching synthesis parent limits"
            );
        }

        Ok(())
    }

    async fn observe_output_batch(
        &self,
        outputs: &[DerivedOutput<JsonValue>],
        phase: &'static str,
    ) {
        if outputs.is_empty() {
            return;
        }

        #[cfg(feature = "messaging")]
        if let Some(obs) = self.self_observer.as_ref() {
            let mut labels = self.derived_metric_labels();
            labels.insert("phase".to_string(), phase.to_string());
            labels.insert(
                "output_event_type".to_string(),
                self.node.output_event_type().to_string(),
            );

            let count = outputs.len() as u64;
            let parent_counts: Vec<f64> = outputs
                .iter()
                .map(|output| output.source_event_ids.len() as f64)
                .collect();
            let parent_sum = parent_counts.iter().sum::<f64>();
            let parent_min = parent_counts.iter().copied().fold(f64::INFINITY, f64::min);
            let parent_max = parent_counts.iter().copied().fold(0.0, f64::max);

            if let Err(error) = obs
                .emit_counter_with_delta(
                    "derived.outputs_emitted",
                    count,
                    count,
                    Some(labels.clone()),
                )
                .await
            {
                log_self_observation_failure(self.node.name(), "derived.outputs_emitted", &error);
            }

            if let Err(error) = obs
                .emit_histogram(
                    "derived.output.parent_count",
                    count,
                    parent_sum,
                    parent_min,
                    parent_max,
                    None,
                    Some(labels.clone()),
                )
                .await
            {
                log_self_observation_failure(
                    self.node.name(),
                    "derived.output.parent_count",
                    &error,
                );
            }

            let aggregated = outputs
                .iter()
                .filter_map(|output| output.aggregation.as_ref())
                .collect::<Vec<_>>();
            if !aggregated.is_empty() {
                let logical_counts: Vec<f64> = aggregated
                    .iter()
                    .map(|aggregation| aggregation.total_input_count as f64)
                    .collect();
                let logical_sum = logical_counts.iter().sum::<f64>();
                let logical_min = logical_counts.iter().copied().fold(f64::INFINITY, f64::min);
                let logical_max = logical_counts.iter().copied().fold(0.0, f64::max);

                if let Err(error) = obs
                    .emit_histogram(
                        "derived.output.logical_input_count",
                        aggregated.len() as u64,
                        logical_sum,
                        logical_min,
                        logical_max,
                        None,
                        Some(labels.clone()),
                    )
                    .await
                {
                    log_self_observation_failure(
                        self.node.name(),
                        "derived.output.logical_input_count",
                        &error,
                    );
                }
            }
        }
    }

    // ── Event Processing ───────────────────────────────────────────────

    /// Process a single event through the derived node's logic.
    pub async fn process_one(
        &mut self,
        event: Event<JsonValue>,
    ) -> NodeResult<Vec<Event<JsonValue>>> {
        let context = DerivedTriggerContext::live(&event)?;
        let source_event_id = context.trigger_event_id;

        // Lag = wall time between the upstream event's `ts_orig` and the
        // moment we start processing it. Negative values (clock skew /
        // synthesized future timestamps) are clamped to zero so the
        // gauge stays interpretable.
        let lag_ms = event_lag_ms(&event);
        let process_started_at = std::time::Instant::now();

        let result = self
            .node
            .process_derived(&mut self.persisted_state.state, event.clone(), &context)
            .await;

        let runtime_ms = process_started_at.elapsed().as_secs_f64() * 1000.0;
        self.observe_processing_latency(lag_ms, runtime_ms).await;

        // Track health
        #[cfg(feature = "messaging")]
        if let Some(ref reporter) = self.health_reporter {
            match &result {
                Ok(_) => reporter.record_success(),
                Err(e) => {
                    let sinex_error = SinexError::processing("derived node processing error")
                        .with_source(e.to_string());
                    reporter.record_error(&sinex_error);
                }
            }

            if let Err(e) = reporter.check_and_emit().await {
                warn!(node = %self.node.name(), error = %e, "Failed to emit health status");
            }
        }

        match result {
            Ok(outputs) => {
                self.validate_output_batch(&outputs, "live processing")?;
                self.observe_output_batch(&outputs, "live").await;
                let output_events =
                    self.build_output_events(outputs, Some(source_event_id), &context)?;
                self.record_processed_input(source_event_id);
                self.observe_runtime_snapshot().await;
                Ok(output_events)
            }
            Err(e) => {
                let action = self.node.handle_error_derived(&e);
                match action {
                    ErrorAction::Skip => {
                        warn!(node = %self.node.name(), error = %e, "Skipping event");
                        self.record_processed_input(source_event_id);
                        self.observe_runtime_snapshot().await;
                        Ok(Vec::new())
                    }
                    ErrorAction::SendToProcessingFailureQueue => {
                        self.send_to_processing_failure_queue_or_fail(&event, &e)
                            .await?;
                        self.record_processed_input(source_event_id);
                        self.observe_runtime_snapshot().await;
                        Ok(Vec::new())
                    }
                    ErrorAction::Retry => Err(e.into()),
                }
            }
        }
    }

    fn build_output_events(
        &self,
        outputs: Vec<DerivedOutput<JsonValue>>,
        fallback_source_id: Option<Id<Event<JsonValue>>>,
        context: &DerivedTriggerContext,
    ) -> NodeResult<Vec<Event<JsonValue>>> {
        outputs
            .into_iter()
            .enumerate()
            .map(|(output_index, output)| {
                self.build_output_event(output, output_index, fallback_source_id, context)
            })
            .collect()
    }

    /// Build an output `Event<JsonValue>` from a `DerivedOutput<JsonValue>`.
    fn build_output_event(
        &self,
        output: DerivedOutput<JsonValue>,
        output_index: usize,
        fallback_source_id: Option<Id<Event<JsonValue>>>,
        context: &DerivedTriggerContext,
    ) -> NodeResult<Event<JsonValue>> {
        let DerivedOutput {
            payload,
            ts_orig,
            source_event_ids,
            temporal_policy,
            semantics_version,
            scope_key,
            equivalence_key,
            aggregation: _aggregation,
        } = output;

        let privacy_context = self.node.output_privacy_context();
        let filtered_payload =
            privacy::process_json(&payload, privacy_context).map_err(|error| {
                SinexError::configuration("failed to initialize privacy engine".to_string())
                    .with_context("component", "derived_output_payload")
                    .with_context("privacy_context", format!("{privacy_context:?}"))
                    .with_std_error(error)
            })?;
        if filtered_payload != payload {
            debug!(
                node = %self.node.name(),
                output_event_type = %self.node.output_event_type(),
                ?privacy_context,
                "Applied privacy filtering to derived output payload"
            );
        }

        let typed_ids: Vec<Id<Event<JsonValue>>> =
            source_event_ids.into_iter().map(Id::from_uuid).collect();
        let source_event_ids = match NonEmptyVec::from_vec(typed_ids) {
            Some(source_event_ids) => source_event_ids,
            None => {
                if let Some(fallback_source_id) = fallback_source_id {
                    NonEmptyVec::single(fallback_source_id)
                } else {
                    return Err(SinexError::validation(
                        "derived invalidation output missing source event ids",
                    )
                    .with_context("node", self.node.name())
                    .with_context("output_event_type", self.node.output_event_type())
                    .with_context("processing_mode", format!("{:?}", context.processing_mode))
                    .with_context("trigger_kind", format!("{:?}", context.trigger_kind))
                    .with_context(
                        "scope_key",
                        scope_key.clone().unwrap_or_else(|| "<none>".to_string()),
                    ));
                }
            }
        };
        let event_id_source = format!(
            "{}:{}:{}",
            self.node.name(),
            self.node.output_event_source(),
            self.node.output_event_type()
        );
        let event_id_anchor = derived_event_anchor(
            output_index,
            &source_event_ids,
            &temporal_policy,
            semantics_version.as_deref(),
            scope_key.as_deref(),
            equivalence_key.as_deref(),
        );
        let provenance = Provenance::Synthesis {
            source_event_ids,
            operation_id: context.operation_id(),
        };
        // Extract before moving provenance into the event struct.
        let created_by_operation_id = provenance.operation_uuid();

        Ok(Event {
            id: Some(Id::from_uuid(deterministic_event_id(
                event_id_source,
                event_id_anchor,
                ts_orig,
            ))),
            source: EventSource::new(self.node.output_event_source())?,
            event_type: EventType::new(self.node.output_event_type())?,
            payload: filtered_payload,
            ts_orig: Some(ts_orig),
            host: HostName::new(&self.host)?,
            node_run_id: self
                .runtime
                .as_ref()
                .and_then(NodeRuntimeState::node_run_id),
            payload_schema_id: None,
            provenance,
            associated_blob_ids: None,
            temporal_policy: Some(temporal_policy),
            semantics_version,
            scope_key,
            equivalence_key,
            created_by_operation_id,
            node_model: Some(self.node.node_model()),
        })
    }

    /// Process a batch of events.
    ///
    /// Events that fail with `ErrorAction::Retry` halt the batch — the checkpoint
    /// is NOT advanced past them and the first retry error is returned.
    pub async fn process_batch(
        &mut self,
        events: Vec<Event<JsonValue>>,
    ) -> NodeResult<Vec<Event<JsonValue>>> {
        let mut outputs = Vec::new();
        let mut retry_error: Option<SinexError> = None;

        for event in events {
            match self.process_one(event).await {
                Ok(mut output_events) => outputs.append(&mut output_events),
                Err(e) => {
                    error!(node = %self.node.name(), error = %e, "Retryable error processing event in batch; halting batch");
                    retry_error = Some(e);
                    break;
                }
            }
        }

        if self.should_checkpoint() {
            match self.save_state().await {
                Ok(()) => {
                    self.consecutive_checkpoint_failures = 0;
                }
                Err(e) => {
                    self.consecutive_checkpoint_failures += 1;
                    error!(
                        node = %self.node.name(),
                        error = %e,
                        consecutive_failures = self.consecutive_checkpoint_failures,
                        "Failed to save checkpoint after batch"
                    );
                    if self.consecutive_checkpoint_failures >= 3
                        || matches!(
                            e,
                            SinexError::Checkpoint(_)
                                | SinexError::Lifecycle(_)
                                | SinexError::Configuration(_)
                                | SinexError::PermissionDenied(_)
                        )
                    {
                        return Err(SinexError::checkpoint(format!(
                            "Checkpoint save failed {} consecutive times; halting to prevent \
                             silent progress loss on crash+restart",
                            self.consecutive_checkpoint_failures
                        )));
                    }
                }
            }
        }

        if let Some(e) = retry_error {
            return Err(e);
        }

        Ok(outputs)
    }


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
                        min_emission_interval: Duration::from_secs(1),
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
        Ok(Vec::new())
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
        Ok(())
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

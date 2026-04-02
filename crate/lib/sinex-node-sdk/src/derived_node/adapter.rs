//! `DerivedNodeAdapter` — shared runtime adapter for all derived node models.
//!
//! This replaces `AutomatonNodeAdapter`. It wraps any [`DerivedNodeImpl`] and
//! implements the stream [`Node`] trait, handling checkpoints, health monitoring,
//! shutdown, and event emission.

use super::context::DerivedTriggerContext;
use super::output::DerivedOutput;
use super::traits::{DerivedNodeConfig, DerivedNodeImpl};

use crate::checkpoint::{CheckpointManager, CheckpointState, decode_checkpoint_data};
use crate::error_helpers::{env_bool_with_default, env_parse_with_default};
use crate::processing::{ErrorAction, PersistedState};
use crate::runtime::stream::{
    Checkpoint, EventSender, NodeCapabilities, NodeInitContext, NodeRuntimeState, NodeType,
    ScanArgs, ScanEstimate, ScanReport, TimeHorizon,
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
use tokio::sync::watch;
use tracing::{debug, error, info, warn};

const INVALIDATION_QUERY_PAGE_SIZE: i64 = Pagination::MAX_LIMIT;

fn signal_shutdown_channel(shutdown_tx: &watch::Sender<bool>, node_name: &str) -> bool {
    if shutdown_tx.send(true).is_err() {
        warn!(
            node = node_name,
            "Derived-node shutdown receiver was already dropped before graceful shutdown"
        );
        return false;
    }
    true
}

fn stale_output_ids_or_skip_scope(
    node_name: &str,
    scope_key: &str,
    stale_query_result: Result<Vec<QueryResultEvent>, SinexError>,
) -> Option<Vec<Uuid>> {
    match stale_query_result {
        Ok(events) => Some(
            events
                .iter()
                .filter_map(|qe| qe.event.id.map(|id| *id.as_uuid()))
                .collect(),
        ),
        Err(error) => {
            error!(
                node = node_name,
                scope_key,
                error = %error,
                "Failed to query stale outputs — skipping scope to prevent duplicate recomputation"
            );
            None
        }
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

#[cfg(feature = "db")]
struct PreparedInvalidation {
    outputs: Vec<Event<JsonValue>>,
    scopes: Vec<PreparedInvalidationScope>,
    operation_uuid: Uuid,
}

#[cfg(feature = "db")]
struct PreparedInvalidationScope {
    scope_key: String,
    stale_ids: Vec<Uuid>,
    new_event_ids: Vec<(Uuid, Option<String>)>,
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
    event_sender: Option<EventSender>,
    shutdown_tx: Option<watch::Sender<bool>>,
    host: String,
    events_since_checkpoint: u64,
    last_checkpoint_time: Instant,
    last_revision: u64,
    pending_hot_reload_cleanup: Option<PathBuf>,
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
            event_sender: None,
            shutdown_tx: None,
            host: gethostname::gethostname().to_string_lossy().to_string(),
            events_since_checkpoint: 0,
            last_checkpoint_time: Instant::now(),
            last_revision: 0,
            pending_hot_reload_cleanup: None,
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

    async fn send_to_dlq_or_fail(
        &self,
        event: &Event<JsonValue>,
        error: &crate::NodeLogicError,
    ) -> NodeResult<()> {
        let Some(runtime) = self.runtime.as_ref() else {
            return Err(SinexError::lifecycle(
                "derived-node requested DLQ but no transport runtime is available",
            )
            .with_context("node", self.node.name())
            .with_context("event_type", event.event_type.as_ref())
            .with_context("source", event.source.as_ref())
            .with_context("reason", error.to_string()));
        };
        let transport = runtime.handles().transport();
        transport
            .send_to_dlq(event, &error.to_string(), self.node.name())
            .await
            .map_err(|dlq_err| {
                SinexError::processing("failed to send derived-node event to DLQ")
                    .with_context("node", self.node.name())
                    .with_context("event_type", event.event_type.as_ref())
                    .with_context("source", event.source.as_ref())
                    .with_context("reason", error.to_string())
                    .with_std_error(&dlq_err)
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

        let sender = self.event_sender.as_ref().ok_or_else(|| {
            SinexError::lifecycle("derived-node output channel is not initialized")
                .with_context("node", self.node.name())
                .with_context("context", context)
        })?;

        for event in outputs {
            let event_id = event
                .id
                .map(|id| id.to_string())
                .unwrap_or_else(|| "<none>".to_string());
            let event_source = event.source.as_ref().to_string();
            let event_type = event.event_type.as_ref().to_string();

            sender.send(event).await.map_err(|_| {
                SinexError::lifecycle("failed to emit derived-node output event")
                    .with_context("node", self.node.name())
                    .with_context("context", context)
                    .with_context("event_id", event_id)
                    .with_context("source", event_source)
                    .with_context("event_type", event_type)
                    .with_context("reason", "event channel closed")
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
                info!(node = %self.node.name(), "No valid checkpoint, starting fresh");
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
    }

    fn record_state_mutation(&mut self) {
        self.events_since_checkpoint += 1;
    }

    // ── Event Processing ───────────────────────────────────────────────

    /// Process a single event through the derived node's logic.
    pub async fn process_one(
        &mut self,
        event: Event<JsonValue>,
    ) -> NodeResult<Vec<Event<JsonValue>>> {
        let context = DerivedTriggerContext::live(&event)?;
        let source_event_id = context.trigger_event_id;

        let result = self
            .node
            .process_derived(&mut self.persisted_state.state, event.clone(), &context)
            .await;

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
                let output_events =
                    self.build_output_events(outputs, Some(source_event_id), &context)?;
                self.record_processed_input(source_event_id);
                Ok(output_events)
            }
            Err(e) => {
                let action = self.node.handle_error_derived(&e);
                match action {
                    ErrorAction::Skip => {
                        warn!(node = %self.node.name(), error = %e, "Skipping event");
                        self.record_processed_input(source_event_id);
                        Ok(Vec::new())
                    }
                    ErrorAction::SendToDLQ => {
                        self.send_to_dlq_or_fail(&event, &e).await?;
                        self.record_processed_input(source_event_id);
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
            .map(|output| self.build_output_event(output, fallback_source_id, context))
            .collect()
    }

    /// Build an output `Event<JsonValue>` from a `DerivedOutput<JsonValue>`.
    fn build_output_event(
        &self,
        output: DerivedOutput<JsonValue>,
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
        let provenance = Provenance::Synthesis {
            source_event_ids,
            operation_id: context.operation_id(),
        };
        // Extract before moving provenance into the event struct.
        let created_by_operation_id = provenance.operation_uuid();

        Ok(Event {
            id: Some(Id::new()),
            source: EventSource::new(self.node.output_event_source())?,
            event_type: EventType::new(self.node.output_event_type())?,
            payload: filtered_payload,
            ts_orig: Some(ts_orig),
            host: HostName::new(&self.host)?,
            node_run_id: self.runtime.as_ref().and_then(|r| r.node_run_id()),
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
            self.save_state().await.map_err(|e| {
                error!(node = %self.node.name(), error = %e, "Failed to save checkpoint after batch");
                e
            })?;
        }

        if let Some(e) = retry_error {
            return Err(e);
        }

        Ok(outputs)
    }

    // ── Invalidation Processing ──────────────────────────────────────────

    #[cfg(feature = "db")]
    async fn prepare_invalidation(
        &mut self,
        invalidation: &super::invalidation::DerivedScopeInvalidation,
    ) -> NodeResult<PreparedInvalidation> {
        use sinex_db::repositories::DbPoolExt;
        use sinex_primitives::prelude::*;

        // Only process invalidations for our input type
        if !invalidation.matches_input(self.node.input_event_type()) {
            return Ok(PreparedInvalidation {
                outputs: Vec::new(),
                scopes: Vec::new(),
                operation_uuid: invalidation
                    .operation_id
                    .unwrap_or_else(|| *Id::<Operation>::new().as_uuid()),
            });
        }

        let pool = {
            let runtime = self.runtime.as_ref().ok_or_else(|| {
                SinexError::lifecycle("Cannot process invalidation: runtime not initialized")
            })?;
            runtime.db_pool().clone()
        };

        let operation_id = invalidation.operation_id.map(Id::<Operation>::from_uuid);
        let operation_uuid = invalidation
            .operation_id
            .unwrap_or_else(|| *Id::<Operation>::new().as_uuid());

        // Determine scope keys to recompute
        let scope_keys = if invalidation.affected_scope_keys.is_empty() {
            // If no scope keys provided, derive from the affected events' scope_keys in DB
            let affected_ids: Vec<Id<Event<JsonValue>>> = invalidation
                .affected_event_ids
                .iter()
                .map(|uuid| Id::from_uuid(*uuid))
                .collect();

            let mut keys = Vec::new();
            for id in &affected_ids {
                match pool.events().get_by_id(*id).await {
                    Ok(Some(event)) => {
                        if let Some(ref sk) = event.scope_key
                            && !keys.contains(sk)
                        {
                            keys.push(sk.clone());
                        }
                    }
                    Ok(None) => {}
                    Err(error) => {
                        return Err(SinexError::database(
                            "Failed to load affected event while deriving invalidation scope keys",
                        )
                        .with_context("event_id", id.to_string())
                        .with_context("node", self.node.name())
                        .with_source(error));
                    }
                }
            }
            keys
        } else {
            invalidation.affected_scope_keys.clone()
        };

        if scope_keys.is_empty() {
            debug!(
                node = %self.node.name(),
                action = %invalidation.action,
                "No scope keys to recompute"
            );
            return Ok(PreparedInvalidation {
                outputs: Vec::new(),
                scopes: Vec::new(),
                operation_uuid,
            });
        }

        let output_source = self.node.output_event_source();
        let output_type = self.node.output_event_type();
        let mut all_outputs = Vec::new();
        let mut prepared_scopes = Vec::new();

        for scope_key in &scope_keys {
            // ── Step 1: Find existing derived outputs for this scope ──
            let stale_query = EventQuery {
                sources: vec![EventSource::new(output_source)?],
                event_types: vec![EventType::new(output_type)?],
                scope_key: Some(scope_key.clone()),
                direction: SortDirection::Asc,
                limit: INVALIDATION_QUERY_PAGE_SIZE,
                ..EventQuery::default()
            };

            let Some(stale_ids) = stale_output_ids_or_skip_scope(
                self.node.name(),
                scope_key,
                self.load_query_events_paginated(&pool, stale_query, scope_key, "stale outputs")
                    .await,
            ) else {
                continue;
            };

            // ── Step 2: Load working set (input events for this scope) ──
            let query = EventQuery {
                event_types: vec![EventType::new(self.node.input_event_type())?],
                scope_key: Some(scope_key.clone()),
                direction: SortDirection::Asc,
                limit: INVALIDATION_QUERY_PAGE_SIZE,
                ..EventQuery::default()
            };

            let working_set = self
                .load_query_events_paginated(&pool, query, scope_key, "scope working set")
                .await?
                .into_iter()
                .map(|qe| qe.event)
                .collect::<Vec<_>>();

            // Build context for invalidation processing
            let context = DerivedTriggerContext {
                trigger_event_id: Id::new(),
                source: invalidation.event_source.clone(),
                event_type: invalidation.event_type.clone(),
                ts_orig: None,
                ts_coided: Timestamp::now(),
                processing_mode: sinex_primitives::domain::ProcessingMode::Replay,
                trigger_kind: sinex_primitives::domain::TriggerKind::ScopeInvalidation,
                created_by_operation_id: operation_id,
            };

            info!(
                node = %self.node.name(),
                scope_key,
                working_set_size = working_set.len(),
                action = %invalidation.action,
                "Recomputing scope from working set"
            );

            // ── Step 3: Recompute via trait implementation ──
            let outputs = self
                .node
                .process_invalidation_derived(
                    &mut self.persisted_state.state,
                    scope_key,
                    working_set,
                    &context,
                )
                .await
                .map_err(|e| {
                    SinexError::processing(format!(
                        "Scope recomputation failed for scope '{scope_key}': {e}"
                    ))
                })?;

            // Build output events
            let mut new_event_ids = Vec::new();
            for output in outputs {
                let equivalence_key = output.equivalence_key.clone();
                let output_event = self.build_output_event(output, None, &context)?;
                let new_id = *output_event.id.unwrap_or_else(Id::new).as_uuid();
                new_event_ids.push((new_id, equivalence_key));
                all_outputs.push(output_event);
            }

            prepared_scopes.push(PreparedInvalidationScope {
                scope_key: scope_key.clone(),
                stale_ids,
                new_event_ids,
            });
        }

        Ok(PreparedInvalidation {
            outputs: all_outputs,
            scopes: prepared_scopes,
            operation_uuid,
        })
    }

    #[cfg(feature = "db")]
    async fn apply_prepared_invalidation(
        &self,
        operation_uuid: Uuid,
        scopes: Vec<PreparedInvalidationScope>,
    ) -> NodeResult<()> {
        use sinex_db::repositories::{DbPoolExt, ReplacementKind, ReplacementRecord};

        let pool = {
            let runtime = self.runtime.as_ref().ok_or_else(|| {
                SinexError::lifecycle("Cannot finalize invalidation: runtime not initialized")
            })?;
            runtime.db_pool().clone()
        };

        for scope in scopes {
            if !scope.stale_ids.is_empty() {
                let archived = pool
                    .events()
                    .execute_cascade_archive(
                        &scope.stale_ids,
                        "scope_invalidation_recompute",
                        &operation_uuid.to_string(),
                        &format!("derived:{}", self.node.name()),
                    )
                    .await
                    .map_err(|error| {
                        SinexError::processing(
                            "Failed to archive stale outputs after recomputation",
                        )
                        .with_context("scope_key", scope.scope_key.clone())
                        .with_context("node", self.node.name())
                        .with_source(error)
                    })?;

                info!(
                    node = %self.node.name(),
                    scope_key = scope.scope_key,
                    archived_count = archived,
                    "Archived stale derived outputs after successful recomputation emission"
                );
            }

            if !scope.stale_ids.is_empty() && !scope.new_event_ids.is_empty() {
                let scope_key = scope.scope_key.clone();
                let replacements: Vec<ReplacementRecord> = scope
                    .stale_ids
                    .iter()
                    .flat_map(|old_id| {
                        let scope_key = scope_key.clone();
                        scope.new_event_ids.iter().map(move |(new_id, eq_key)| ReplacementRecord {
                                old_event_id: *old_id,
                                new_event_id: *new_id,
                                relation_kind: ReplacementKind::Recomputed,
                                scope_key: Some(scope_key.clone()),
                                equivalence_key: eq_key.clone(),
                            })
                    })
                    .collect();

                if let Err(error) = pool
                    .events()
                    .record_replacements(operation_uuid, &replacements)
                    .await
                {
                    warn!(
                        node = %self.node.name(),
                        scope_key = %scope.scope_key,
                        error = %error,
                        "Failed to record replacement relations — events still correct"
                    );
                }
            }
        }

        Ok(())
    }

    /// Process a scope invalidation signal.
    ///
    /// For each affected scope:
    /// 1. Loads the current working set from DB (events matching `scope_key` + `input_event_type`)
    /// 2. Calls `process_invalidation_derived()` to recompute
    /// 3. Archives existing derived outputs for that scope (moves to `audit.archived_events`)
    /// 4. Records replacement relations in `audit.event_replacements` (old→new linkage)
    /// 5. Returns replacement events for emission
    ///
    /// `handle_invalidation_message()` uses the same preparation path but emits replacement
    /// outputs before step 3, so channel/transport failures cannot create an empty scope by
    /// archiving stale outputs first.
    ///
    /// Transducer nodes return empty — their outputs are archived with their inputs.
    #[cfg(feature = "db")]
    pub async fn process_invalidation(
        &mut self,
        invalidation: &super::invalidation::DerivedScopeInvalidation,
    ) -> NodeResult<Vec<Event<JsonValue>>> {
        let prepared = self.prepare_invalidation(invalidation).await?;
        let scope_count = prepared.scopes.len();
        let output_count = prepared.outputs.len();
        self.apply_prepared_invalidation(prepared.operation_uuid, prepared.scopes)
            .await?;

        info!(
            node = %self.node.name(),
            scopes_recomputed = scope_count,
            outputs_produced = output_count,
            "Invalidation processing complete"
        );

        Ok(prepared.outputs)
    }

    // ── Continuous + Historical ─────────────────────────────────────────

    /// Handle a received invalidation message: deserialize, process, emit outputs.
    ///
    /// Emits observability metrics via `SelfObserver` when available:
    /// - `invalidation.received` counter (always)
    /// - `invalidation.processed` counter (on success)
    /// - `invalidation.errors` counter (on failure)
    /// - `invalidation.outputs_emitted` counter (on success, with output count)
    /// - `invalidation.processing_duration_ms` gauge (on success)
    async fn handle_invalidation_message(&mut self, payload: &[u8]) -> Option<u64> {
        let node_name = self.node.name();
        let processing_start = Instant::now();

        // Emit "received" counter
        #[cfg(feature = "messaging")]
        if let Some(ref obs) = self.self_observer {
            if let Err(error) = obs.emit_counter("invalidation.received", 1, None).await {
                log_self_observation_failure(node_name, "invalidation.received", &error);
            }
        }

        let invalidation = match serde_json::from_slice::<
            super::invalidation::DerivedScopeInvalidation,
        >(payload)
        {
            Ok(inv) => inv,
            Err(e) => {
                warn!(
                    node = %node_name,
                    error = %e,
                    payload_len = payload.len(),
                    "Failed to deserialize invalidation signal"
                );
                #[cfg(feature = "messaging")]
                if let Some(ref obs) = self.self_observer {
                    if let Err(error) = obs.emit_counter("invalidation.errors", 1, None).await {
                        log_self_observation_failure(node_name, "invalidation.errors", &error);
                    }
                }
                return None;
            }
        };

        debug!(
            node = %node_name,
            action = %invalidation.action,
            affected_events = invalidation.affected_event_ids.len(),
            scope_keys = ?invalidation.affected_scope_keys,
            "Received invalidation signal"
        );

        #[cfg(feature = "db")]
        {
            match self.prepare_invalidation(&invalidation).await {
                Ok(prepared) => {
                    let PreparedInvalidation {
                        outputs,
                        scopes,
                        operation_uuid,
                    } = prepared;
                    let count = match self
                        .emit_output_events(outputs, "scope invalidation recomputation")
                        .await
                    {
                        Ok(count) => count,
                        Err(error) => {
                            error!(
                                node = %node_name,
                                error = %error,
                                action = %invalidation.action,
                                "Invalidation output emission failed"
                            );
                            #[cfg(feature = "messaging")]
                            if let Some(ref obs) = self.self_observer {
                                if let Err(obs_error) =
                                    obs.emit_counter("invalidation.errors", 1, None).await
                                {
                                    log_self_observation_failure(
                                        node_name,
                                        "invalidation.errors",
                                        &obs_error,
                                    );
                                }
                            }
                            return None;
                        }
                    };
                    if let Err(error) = self
                        .apply_prepared_invalidation(operation_uuid, scopes)
                        .await
                    {
                        error!(
                            node = %node_name,
                            error = %error,
                            action = %invalidation.action,
                            "Invalidation archive finalization failed after output emission"
                        );
                        #[cfg(feature = "messaging")]
                        if let Some(ref obs) = self.self_observer {
                            if let Err(obs_error) =
                                obs.emit_counter("invalidation.errors", 1, None).await
                            {
                                log_self_observation_failure(
                                    node_name,
                                    "invalidation.errors",
                                    &obs_error,
                                );
                            }
                        }
                        return None;
                    }
                    self.record_state_mutation();
                    let duration_ms = processing_start.elapsed().as_millis() as f64;

                    if self.should_checkpoint() {
                        if let Err(e) = self.save_state().await {
                            error!(
                                node = %node_name,
                                error = %e,
                                "Failed to checkpoint after invalidation"
                            );
                            #[cfg(feature = "messaging")]
                            if let Some(ref obs) = self.self_observer {
                                if let Err(obs_error) =
                                    obs.emit_counter("invalidation.errors", 1, None).await
                                {
                                    log_self_observation_failure(
                                        node_name,
                                        "invalidation.errors",
                                        &obs_error,
                                    );
                                }
                            }
                            return None;
                        }
                    }

                    // Emit success metrics
                    #[cfg(feature = "messaging")]
                    if let Some(ref obs) = self.self_observer {
                        if let Err(error) =
                            obs.emit_counter("invalidation.processed", 1, None).await
                        {
                            log_self_observation_failure(
                                node_name,
                                "invalidation.processed",
                                &error,
                            );
                        }
                        if let Err(error) = obs
                            .emit_counter_with_delta(
                                "invalidation.outputs_emitted",
                                count,
                                count,
                                None,
                            )
                            .await
                        {
                            log_self_observation_failure(
                                node_name,
                                "invalidation.outputs_emitted",
                                &error,
                            );
                        }
                        if let Err(error) = obs
                            .emit_gauge("invalidation.processing_duration_ms", duration_ms, None)
                            .await
                        {
                            log_self_observation_failure(
                                node_name,
                                "invalidation.processing_duration_ms",
                                &error,
                            );
                        }
                    }

                    Some(count)
                }
                Err(e) => {
                    error!(
                        node = %node_name,
                        error = %e,
                        action = %invalidation.action,
                        "Invalidation processing failed"
                    );
                    #[cfg(feature = "messaging")]
                    if let Some(ref obs) = self.self_observer {
                        if let Err(error) = obs.emit_counter("invalidation.errors", 1, None).await {
                            log_self_observation_failure(node_name, "invalidation.errors", &error);
                        }
                    }
                    None
                }
            }
        }

        #[cfg(not(feature = "db"))]
        {
            let _ = invalidation;
            let _ = processing_start;
            warn!(
                node = %node_name,
                "Invalidation received but db feature not enabled — cannot process"
            );
            None
        }
    }

    async fn run_continuous(&mut self, _from: Checkpoint) -> NodeResult<ScanReport> {
        let start = Instant::now();
        let node_name = self.node.name().to_string();
        let mut invalidations_processed: u64 = 0;

        info!(
            node = %node_name,
            model = %self.node.node_model(),
            input_type = %self.node.input_event_type(),
            output_type = %self.node.output_event_type(),
            "DerivedNode initialized — running invalidation-driven continuous loop"
        );

        // Subscribe to scope invalidation signals via NATS queue group.
        // Queue group ensures only one instance per node type processes each signal,
        // preventing redundant recomputation when multiple replicas are running.
        //
        // Note: requires `messaging` feature (default). run_continuous is only called
        // by the runtime kernel which itself requires messaging infrastructure.
        // Subscribe to scope invalidation signals when messaging is available.
        // The two `#[cfg]` blocks produce different types but both work with
        // `recv_invalidation()` which has matching cfg'd signatures.
        #[cfg(feature = "messaging")]
        let mut invalidation_sub: Option<async_nats::Subscriber> = {
            let nats_client = self.runtime.as_ref().and_then(|r| r.nats_client());

            match nats_client {
                Some(client) => {
                    let env = sinex_primitives::environment::environment();
                    let subject = env.nats_subject(super::invalidation::INVALIDATION_SUBJECT);
                    let queue_group = format!("derived.invalidation.{}", self.node.name());
                    match client
                        .queue_subscribe(subject.clone(), queue_group.clone())
                        .await
                    {
                        Ok(sub) => {
                            info!(
                                node = %node_name,
                                subject = %subject,
                                queue_group = %queue_group,
                                "Subscribed to invalidation signals"
                            );
                            Some(sub)
                        }
                        Err(e) => {
                            warn!(
                                node = %node_name,
                                error = %e,
                                "Failed to subscribe to invalidation signals — \
                                 scope recomputation will not be triggered"
                            );
                            None
                        }
                    }
                }
                None => {
                    debug!(node = %node_name, "No NATS client — invalidation subscription skipped");
                    None
                }
            }
        };
        #[cfg(not(feature = "messaging"))]
        let mut invalidation_sub = ();

        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        self.shutdown_tx = Some(shutdown_tx);

        // Invalidation debounce: buffer signals and process after a quiet period.
        // This prevents a replay archiving N scopes from triggering N immediate
        // recomputations — instead they coalesce into a single batch.
        let debounce_ms = env_parse_with_default(
            "SINEX_DERIVED_INVALIDATION_DEBOUNCE_MS",
            500_u64,
            "derived invalidation debounce",
        );
        let debounce_duration = Duration::from_millis(debounce_ms);
        let mut pending_invalidations: Vec<Vec<u8>> = Vec::new();
        let mut debounce_deadline: Option<tokio::time::Instant> = None;

        loop {
            tokio::select! {
                shutdown_result = shutdown_rx.changed() => {
                    if shutdown_result.is_err() {
                        warn!(
                            node = %node_name,
                            "Derived-node invalidation shutdown channel dropped before explicit shutdown"
                        );
                    }
                    if shutdown_result.is_err() || *shutdown_rx.borrow() {
                        info!(node = %node_name, "Shutdown signal received");
                        // Process any pending invalidations before shutdown
                        for payload in pending_invalidations.drain(..) {
                            if self.handle_invalidation_message(&payload).await.is_some() {
                                invalidations_processed += 1;
                            }
                        }
                        break;
                    }
                }

                // Invalidation signal: buffer and set debounce deadline.
                payload = recv_invalidation(&mut invalidation_sub) => {
                    if let Some(payload) = payload {
                        pending_invalidations.push(payload);
                        debounce_deadline = Some(tokio::time::Instant::now() + debounce_duration);
                    }
                }

                // Debounce timer: process buffered invalidations after quiet period.
                () = async {
                    match debounce_deadline {
                        Some(deadline) => tokio::time::sleep_until(deadline).await,
                        None => std::future::pending().await,
                    }
                } => {
                    let batch_size = pending_invalidations.len();
                    debug!(
                        node = %node_name,
                        batch_size,
                        debounce_ms,
                        "Processing debounced invalidation batch"
                    );
                    for payload in pending_invalidations.drain(..) {
                        if self.handle_invalidation_message(&payload).await.is_some() {
                            invalidations_processed += 1;
                        }
                    }
                    debounce_deadline = None;
                }

                // Periodic checkpoint
                () = tokio::time::sleep(Duration::from_mins(1)) => {
                    if self.events_since_checkpoint > 0
                        && let Err(e) = self.save_state().await
                    {
                        warn!(node = %node_name, error = %e, "Failed to save periodic checkpoint");
                    }
                }
            }
        }

        if let Err(e) = self.save_state().await {
            warn!(node = %node_name, error = %e, "Failed to save final checkpoint");
        }

        Ok(ScanReport {
            events_processed: 0,
            duration: start.elapsed(),
            final_checkpoint: self.current_checkpoint_internal(),
            time_range: None,
            node_stats: HashMap::from([
                (
                    "total_processed".to_string(),
                    self.persisted_state.events_processed,
                ),
                (
                    "invalidations_processed".to_string(),
                    invalidations_processed,
                ),
            ]),
            successful_targets: vec![],
            failed_targets: vec![],
            warnings: vec![],
        })
    }

    #[cfg(feature = "db")]
    async fn run_historical(
        &mut self,
        from: Checkpoint,
        end_time: Timestamp,
        args: ScanArgs,
    ) -> NodeResult<ScanReport> {
        use sinex_db::repositories::DbPoolExt;
        use sinex_primitives::prelude::*;

        let start = Instant::now();
        let pool = {
            let runtime = self.runtime.as_ref().ok_or_else(|| {
                SinexError::lifecycle("Cannot run historical scan: runtime not initialized")
            })?;
            runtime.db_pool().clone()
        };

        let input_event_type = self.node.input_event_type();
        info!(
            node = %self.node.name(),
            model = %self.node.node_model(),
            input_type = %input_event_type,
            end_time = %end_time,
            replay = args.replay.is_some(),
            "Starting derived node historical replay"
        );

        let (time_range, mut cursor) = historical_resume_position(&from, end_time)?;

        let mut events_processed = 0u64;
        let mut events_emitted = 0u64;
        let batch_size: i64 = 500;

        // Extract operation ID from replay args if present
        let operation_id: Option<Id<Operation>> =
            args.replay.as_ref().map(|r| Id::from_uuid(r.operation_id));

        loop {
            let query = EventQuery {
                event_types: vec![EventType::new(input_event_type)?],
                time_range: Some(time_range),
                cursor: cursor.clone(),
                limit: batch_size,
                direction: SortDirection::Asc,
                ..EventQuery::default()
            };

            let result = pool.events().query(query).await.map_err(|e| {
                SinexError::database(format!("Historical replay query failed: {e}"))
            })?;

            let (events, next_cursor) = match result {
                EventQueryResult::Events {
                    events,
                    next_cursor,
                    ..
                } => (events, next_cursor),
                _ => break,
            };

            if events.is_empty() {
                break;
            }

            for query_event in &events {
                let ctx = DerivedTriggerContext::historical(&query_event.event, operation_id)?;
                let trigger_event_id = ctx.trigger_event_id;

                match self
                    .node
                    .process_derived(
                        &mut self.persisted_state.state,
                        query_event.event.clone(),
                        &ctx,
                    )
                    .await
                {
                    Ok(outputs) => {
                        let output_events =
                            self.build_output_events(outputs, Some(ctx.trigger_event_id), &ctx)?;
                        if let Some(ref sender) = self.event_sender {
                            for output_event in output_events {
                                sender.send(output_event).await.map_err(|_| {
                                    SinexError::lifecycle("Event channel closed during replay")
                                })?;
                                events_emitted += 1;
                            }
                        }
                    }
                    Err(e) => {
                        let action = self.node.handle_error_derived(&e);
                        match action {
                            ErrorAction::Skip => {
                                warn!(node = %self.node.name(), error = %e, "Skipping event in historical replay");
                            }
                            ErrorAction::SendToDLQ => {
                                let event_for_dlq = query_event.event.clone();
                                if let Err(dlq_err) =
                                    self.send_to_dlq_or_fail(&event_for_dlq, &e).await
                                {
                                    error!(
                                        node = %self.node.name(),
                                        error = %dlq_err,
                                        "Failed to send to DLQ during replay"
                                    );
                                    if let Err(cp_err) = self.save_state().await {
                                        error!(
                                            node = %self.node.name(),
                                            error = %cp_err,
                                            "Failed to save checkpoint after replay DLQ error"
                                        );
                                    }
                                    return Err(dlq_err);
                                }
                            }
                            ErrorAction::Retry => {
                                error!(node = %self.node.name(), error = %e, "Retryable error in historical replay; halting replay");
                                if let Err(cp_err) = self.save_state().await {
                                    error!(node = %self.node.name(), error = %cp_err, "Failed to save checkpoint after replay error");
                                }
                                return Err(e.into());
                            }
                        }
                    }
                }
                events_processed += 1;
                self.record_processed_input(trigger_event_id);
            }

            if self.should_checkpoint() {
                self.save_state().await.map_err(|e| {
                    error!(
                        node = %self.node.name(),
                        error = %e,
                        "Failed to save checkpoint during historical replay"
                    );
                    e
                })?;
            }

            match next_cursor {
                Some(c) => {
                    cursor = Some(c);
                }
                None => break,
            }
        }

        self.save_state().await.map_err(|e| {
            error!(node = %self.node.name(), error = %e, "Failed to save checkpoint after replay");
            e
        })?;

        info!(
            node = %self.node.name(),
            events_processed,
            events_emitted,
            duration_ms = start.elapsed().as_millis(),
            "Historical replay completed"
        );

        Ok(ScanReport {
            events_processed,
            duration: start.elapsed(),
            final_checkpoint: self.current_checkpoint_internal(),
            time_range: None,
            node_stats: HashMap::from([
                ("total_processed".to_string(), events_processed),
                ("events_emitted".to_string(), events_emitted),
            ]),
            successful_targets: vec!["historical_replay".to_string()],
            failed_targets: vec![],
            warnings: vec![],
        })
    }

    #[cfg(not(feature = "db"))]
    async fn run_historical(
        &mut self,
        _from: Checkpoint,
        _end_time: Timestamp,
        _args: ScanArgs,
    ) -> NodeResult<ScanReport> {
        Err(SinexError::unknown(
            "DerivedNode historical replay requires the 'db' feature",
        ))
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
            signal_shutdown_channel(tx, self.node.name());
        }
    }
}

// ── Invalidation subscription helper ─────────────────────────────────

/// Receive the next invalidation message payload from a NATS subscription.
/// Returns `None` only when the subscription stream ends.
/// When `sub` is `None` (no NATS available), pends forever — effectively
/// disabling the select arm without needing `#[cfg]` inside `tokio::select!`.
#[cfg(feature = "messaging")]
async fn recv_invalidation(sub: &mut Option<async_nats::Subscriber>) -> Option<Vec<u8>> {
    use futures::StreamExt;
    match sub.as_mut() {
        Some(s) => s.next().await.map(|msg| msg.payload.to_vec()),
        None => std::future::pending().await,
    }
}

/// Stub when messaging feature is disabled — always pends.
#[cfg(not(feature = "messaging"))]
async fn recv_invalidation(_sub: &mut ()) -> Option<Vec<u8>> {
    std::future::pending().await
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
        self.event_sender = Some(runtime.event_sender().clone());
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
                        subject_prefix: "sinex.telemetry".to_string(),
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
            manages_own_continuous_loop: true,
            ..NodeCapabilities::default()
        }
    }

    async fn current_checkpoint(&self) -> NodeResult<Checkpoint> {
        Ok(self.current_checkpoint_internal())
    }

    async fn health_check(&self) -> NodeResult<bool> {
        Ok(self.runtime.is_some())
    }

    async fn shutdown(&mut self) -> NodeResult<()> {
        info!(node = %self.node.name(), "Shutting down DerivedNode");

        self.signal_shutdown();

        if let Err(e) = self
            .node
            .on_shutdown_derived(&self.persisted_state.state)
            .await
        {
            warn!(node = %self.node.name(), error = %e, "Shutdown hook failed");
        }

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

        if !file_save_success && !nats_save_success {
            return Err(SinexError::checkpoint(
                "Failed to save state to both file and NATS KV on shutdown".to_string(),
            ));
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
        let description = if runtime_initialized {
            format!("{node_name} derived node ({node_model})")
        } else {
            format!("{node_name} derived node ({node_model}, runtime not initialized)")
        };

        Ok(crate::exploration::SourceState {
            is_connected: runtime_initialized,
            healthy: runtime_initialized,
            description,
            last_updated: None,
            lag_seconds: None,
            recent_activity: Vec::new(),
            total_items: None,
            metadata: HashMap::from([
                (
                    "runtime_initialized".to_string(),
                    serde_json::json!(runtime_initialized),
                ),
                ("node_model".to_string(), serde_json::json!(node_model)),
            ]),
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
mod tests {
    // Inline because these cover a private shutdown-signaling helper.
    #[cfg(feature = "messaging")]
    use super::log_self_observation_failure;
    use super::signal_shutdown_channel;
    use super::{DerivedNodeAdapter, stale_output_ids_or_skip_scope};
    use crate::derived_node::{
        DerivedNodeConfig, DerivedOutput, DerivedTriggerContext, ScopeReconcilerWrapper,
        TransducerWrapper,
    };
    use crate::exploration::ExplorationProvider;
    use crate::runtime::stream::{
        Checkpoint, EventEmitter, NodeHandles, NodeRuntimeState, ScanArgs, ServiceInfo,
    };
    #[cfg(feature = "messaging")]
    use crate::self_observation::SelfObservationError;
    use crate::shutdown::ShutdownConfig;
    use crate::{CheckpointManager, CheckpointState, EventTransport, NatsPublisher, SinexError};
    use crate::{ErrorAction, NodeLogicError, ScopeReconcilerNode, TransducerNode};
    use camino::Utf8PathBuf;
    use futures::TryStreamExt;
    use serde::{Deserialize, Serialize};
    use serde_json::json;
    use sinex_db::DbPoolExt;
    use sinex_primitives::events::{DynamicPayload, Event};
    use sinex_primitives::privacy::ProcessingContext;
    use sinex_primitives::temporal::Timestamp;
    use sinex_primitives::{HostName, Id, JsonValue, Uuid};
    use std::collections::HashMap;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use tempfile::tempdir;
    use tokio::sync::{mpsc, watch};
    use xtask::sandbox::prelude::*;

    #[derive(Debug, Default, Serialize, Deserialize)]
    struct TestDerivedState;

    struct TestDerivedNode;

    impl TransducerNode for TestDerivedNode {
        type State = TestDerivedState;
        type Input = JsonValue;
        type Output = JsonValue;

        fn name(&self) -> &'static str {
            "derived-adapter-test"
        }

        fn input_event_type(&self) -> &'static str {
            "test.input"
        }

        fn output_event_type(&self) -> &'static str {
            "test.output"
        }

        fn output_privacy_context(&self) -> ProcessingContext {
            ProcessingContext::Metadata
        }

        async fn process(
            &mut self,
            _state: &mut Self::State,
            _input: Self::Input,
            _context: &DerivedTriggerContext,
        ) -> std::result::Result<Option<DerivedOutput<Self::Output>>, NodeLogicError> {
            Ok(None)
        }
    }

    struct RetryDerivedNode {
        seen: Arc<AtomicUsize>,
    }

    impl TransducerNode for RetryDerivedNode {
        type State = TestDerivedState;
        type Input = JsonValue;
        type Output = JsonValue;

        fn name(&self) -> &'static str {
            "derived-adapter-retry-test"
        }

        fn input_event_type(&self) -> &'static str {
            "test.input"
        }

        fn output_event_type(&self) -> &'static str {
            "test.output"
        }

        fn output_privacy_context(&self) -> ProcessingContext {
            ProcessingContext::Metadata
        }

        async fn process(
            &mut self,
            _state: &mut Self::State,
            _input: Self::Input,
            _context: &DerivedTriggerContext,
        ) -> std::result::Result<Option<DerivedOutput<Self::Output>>, NodeLogicError> {
            self.seen.fetch_add(1, Ordering::SeqCst);
            Err(NodeLogicError::Processing("retry requested".to_string()))
        }

        fn handle_error(&self, _error: &NodeLogicError) -> crate::ErrorAction {
            crate::ErrorAction::Retry
        }
    }

    struct EmittingDerivedNode;

    impl TransducerNode for EmittingDerivedNode {
        type State = TestDerivedState;
        type Input = JsonValue;
        type Output = JsonValue;

        fn name(&self) -> &'static str {
            "derived-adapter-emitting-test"
        }

        fn input_event_type(&self) -> &'static str {
            "test.input"
        }

        fn output_event_type(&self) -> &'static str {
            "test.output"
        }

        fn output_privacy_context(&self) -> ProcessingContext {
            ProcessingContext::Metadata
        }

        async fn process(
            &mut self,
            _state: &mut Self::State,
            _input: Self::Input,
            context: &DerivedTriggerContext,
        ) -> std::result::Result<Option<DerivedOutput<Self::Output>>, NodeLogicError> {
            Ok(Some(DerivedOutput::transduced(
                json!({"ok": true}),
                context.ts_orig.unwrap_or_else(Timestamp::now),
                context.trigger_uuid(),
            )))
        }
    }

    #[derive(Default, Deserialize)]
    struct UnserializableDerivedState;

    impl Serialize for UnserializableDerivedState {
        fn serialize<S>(&self, _serializer: S) -> std::result::Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            Err(serde::ser::Error::custom("state serialization exploded"))
        }
    }

    struct UnserializableDerivedNode;

    impl TransducerNode for UnserializableDerivedNode {
        type State = UnserializableDerivedState;
        type Input = JsonValue;
        type Output = JsonValue;

        fn name(&self) -> &'static str {
            "adapter-regression-unserializable-checkpoint"
        }

        fn input_event_type(&self) -> &'static str {
            "test.input"
        }

        fn output_event_type(&self) -> &'static str {
            "test.output"
        }

        fn output_privacy_context(&self) -> ProcessingContext {
            ProcessingContext::Metadata
        }

        async fn process(
            &mut self,
            _state: &mut Self::State,
            _input: Self::Input,
            _context: &DerivedTriggerContext,
        ) -> std::result::Result<Option<DerivedOutput<Self::Output>>, NodeLogicError> {
            Ok(None)
        }
    }

    #[derive(Default, Serialize, Deserialize)]
    struct TestScopeReconcilerState;

    #[derive(Deserialize)]
    struct ScopeReconcilerInput {
        value: i64,
    }

    #[derive(Serialize)]
    struct ScopeReconcilerOutput {
        total: i64,
        count: usize,
    }

    struct TestScopeReconcilerNode;

    impl ScopeReconcilerNode for TestScopeReconcilerNode {
        type State = TestScopeReconcilerState;
        type Input = ScopeReconcilerInput;
        type Output = ScopeReconcilerOutput;

        fn name(&self) -> &'static str {
            "adapter-regression-scope-reconciler"
        }

        fn input_event_type(&self) -> &'static str {
            "measurement.taken"
        }

        fn output_event_type(&self) -> &'static str {
            "measurement.aggregate"
        }

        fn output_privacy_context(&self) -> ProcessingContext {
            ProcessingContext::Metadata
        }

        fn scope_keys(
            &self,
            _input: &Self::Input,
            _context: &DerivedTriggerContext,
        ) -> Vec<String> {
            vec!["default".into()]
        }

        async fn reconcile(
            &mut self,
            _state: &mut Self::State,
            scope_key: &str,
            input: Self::Input,
            context: &DerivedTriggerContext,
        ) -> Result<Vec<DerivedOutput<Self::Output>>, NodeLogicError> {
            Ok(vec![DerivedOutput::reconciled(
                ScopeReconcilerOutput {
                    total: input.value,
                    count: 1,
                },
                context.ts_orig.unwrap_or_else(Timestamp::now),
                vec![*context.trigger_event_id.as_uuid()],
                scope_key.to_string(),
            )])
        }

        async fn recompute_scope(
            &mut self,
            _state: &mut Self::State,
            scope_key: &str,
            working_set: Vec<Self::Input>,
            context: &DerivedTriggerContext,
        ) -> Result<Vec<DerivedOutput<Self::Output>>, NodeLogicError> {
            if working_set.is_empty() {
                return Ok(Vec::new());
            }

            let total = working_set.iter().map(|input| input.value).sum();
            let count = working_set.len();

            Ok(vec![DerivedOutput::reconciled(
                ScopeReconcilerOutput { total, count },
                context.ts_orig.unwrap_or_else(Timestamp::now),
                vec![*context.trigger_event_id.as_uuid()],
                scope_key.to_string(),
            )])
        }
    }

    #[derive(Default, Serialize, Deserialize)]
    struct StatefulInvalidationState {
        invalidations_applied: u64,
    }

    struct StatefulInvalidationNode;

    impl ScopeReconcilerNode for StatefulInvalidationNode {
        type State = StatefulInvalidationState;
        type Input = ScopeReconcilerInput;
        type Output = ScopeReconcilerOutput;

        fn name(&self) -> &'static str {
            "adapter-regression-stateful-invalidation"
        }

        fn input_event_type(&self) -> &'static str {
            "measurement.taken"
        }

        fn output_event_type(&self) -> &'static str {
            "measurement.aggregate"
        }

        fn output_privacy_context(&self) -> ProcessingContext {
            ProcessingContext::Metadata
        }

        fn scope_keys(
            &self,
            _input: &Self::Input,
            _context: &DerivedTriggerContext,
        ) -> Vec<String> {
            vec!["default".into()]
        }

        async fn reconcile(
            &mut self,
            _state: &mut Self::State,
            _scope_key: &str,
            _input: Self::Input,
            _context: &DerivedTriggerContext,
        ) -> Result<Vec<DerivedOutput<Self::Output>>, NodeLogicError> {
            Ok(Vec::new())
        }

        async fn recompute_scope(
            &mut self,
            state: &mut Self::State,
            _scope_key: &str,
            _working_set: Vec<Self::Input>,
            _context: &DerivedTriggerContext,
        ) -> Result<Vec<DerivedOutput<Self::Output>>, NodeLogicError> {
            state.invalidations_applied += 1;
            Ok(Vec::new())
        }
    }

    struct DlqRetryDerivedNode;

    impl TransducerNode for DlqRetryDerivedNode {
        type State = TestDerivedState;
        type Input = JsonValue;
        type Output = JsonValue;

        fn name(&self) -> &'static str {
            "derived-adapter-dlq-retry-test"
        }

        fn input_event_type(&self) -> &'static str {
            "test.input"
        }

        fn output_event_type(&self) -> &'static str {
            "test.output"
        }

        fn output_privacy_context(&self) -> ProcessingContext {
            ProcessingContext::Metadata
        }

        async fn process(
            &mut self,
            _state: &mut Self::State,
            _input: Self::Input,
            _context: &DerivedTriggerContext,
        ) -> std::result::Result<Option<DerivedOutput<Self::Output>>, NodeLogicError> {
            Err(NodeLogicError::Processing("route me to dlq".to_string()))
        }

        fn handle_error(&self, _error: &NodeLogicError) -> ErrorAction {
            ErrorAction::SendToDLQ
        }
    }

    fn make_input_event(value: &str) -> std::result::Result<Event<JsonValue>, SinexError> {
        let mut event = DynamicPayload::new("test.source", "test.input", json!({ "value": value }))
            .from_parents([Id::<Event<JsonValue>>::new()])?
            .build()?;
        event.id = Some(event.id.unwrap_or_else(Id::new));
        Ok(event)
    }

    async fn make_runtime_state(
        ctx: &TestContext,
        node_name: &str,
        node_run_id: Option<Uuid>,
    ) -> TestResult<NodeRuntimeState> {
        let kv = ctx.checkpoint_kv().await?;
        let checkpoint_manager = Arc::new(CheckpointManager::new(
            kv,
            node_name.to_string(),
            "test-group".to_string(),
            format!("test-consumer-{}", Uuid::now_v7().simple()),
        ));
        let (event_sender, _event_receiver) = mpsc::channel::<Event<JsonValue>>(32);
        let emitter = EventEmitter::new(event_sender, false);
        let publisher = Arc::new(NatsPublisher::new(ctx.nats_client()));
        let handles = NodeHandles::new_edge(
            checkpoint_manager,
            emitter,
            EventTransport::Nats(publisher),
            None,
            None,
        );
        let work_dir = tempdir()?;
        let work_dir_path = work_dir.keep();
        let work_dir_utf8 = Utf8PathBuf::from_path_buf(work_dir_path.clone()).map_err(|path| {
            color_eyre::eyre::eyre!("temporary work dir should be utf-8: {}", path.display())
        })?;
        Ok(NodeRuntimeState::new(
            ServiceInfo::new(
                node_name.to_string(),
                node_name.to_string(),
                HostName::from_static("test-host"),
                work_dir_path,
                false,
                format!("instance-{}", Uuid::now_v7().simple()),
                env!("CARGO_PKG_VERSION").to_string(),
                node_run_id,
            ),
            handles,
            HashMap::new(),
            work_dir_utf8,
        ))
    }

    async fn make_runtime_state_with_db(
        ctx: &TestContext,
        node_name: &str,
        node_run_id: Option<Uuid>,
    ) -> TestResult<(NodeRuntimeState, mpsc::Receiver<Event<JsonValue>>)> {
        let kv = ctx.checkpoint_kv().await?;
        let checkpoint_manager = Arc::new(CheckpointManager::new(
            kv,
            node_name.to_string(),
            "test-group".to_string(),
            format!("test-consumer-{}", Uuid::now_v7().simple()),
        ));
        let (event_sender, event_receiver) = mpsc::channel::<Event<JsonValue>>(32);
        let emitter = EventEmitter::new(event_sender, false);
        let publisher = Arc::new(NatsPublisher::new(ctx.nats_client()));
        let handles = NodeHandles::new(
            ctx.pool().clone(),
            checkpoint_manager,
            emitter,
            EventTransport::Nats(publisher),
            None,
            None,
        );
        let work_dir = tempdir()?;
        let work_dir_path = work_dir.keep();
        let work_dir_utf8 = Utf8PathBuf::from_path_buf(work_dir_path.clone()).map_err(|path| {
            color_eyre::eyre::eyre!("temporary work dir should be utf-8: {}", path.display())
        })?;
        Ok((
            NodeRuntimeState::new(
                ServiceInfo::new(
                    node_name.to_string(),
                    node_name.to_string(),
                    HostName::from_static("test-host"),
                    work_dir_path,
                    false,
                    format!("instance-{}", Uuid::now_v7().simple()),
                    env!("CARGO_PKG_VERSION").to_string(),
                    node_run_id,
                ),
                handles,
                HashMap::new(),
                work_dir_utf8,
            ),
            event_receiver,
        ))
    }

    #[sinex_test]
    async fn signal_shutdown_channel_reports_dropped_receiver() -> TestResult<()> {
        let (tx, rx) = watch::channel(false);
        drop(rx);

        assert!(!signal_shutdown_channel(&tx, "test-derived"));
        Ok(())
    }

    #[sinex_test]
    async fn signal_shutdown_channel_delivers_to_receiver() -> TestResult<()> {
        let (tx, mut rx) = watch::channel(false);

        assert!(signal_shutdown_channel(&tx, "test-derived"));
        rx.changed().await?;
        assert!(*rx.borrow());
        Ok(())
    }

    #[sinex_test]
    async fn stale_output_ids_or_skip_scope_returns_empty_ids_on_success() -> TestResult<()> {
        let stale_ids = stale_output_ids_or_skip_scope("test-derived", "scope-a", Ok(Vec::new()))
            .expect("successful stale query should not skip scope");
        assert!(stale_ids.is_empty());
        Ok(())
    }

    #[sinex_test]
    async fn stale_output_ids_or_skip_scope_skips_scope_on_query_error() -> TestResult<()> {
        let stale_ids = stale_output_ids_or_skip_scope(
            "test-derived",
            "scope-a",
            Err(SinexError::invalid_state("corrupt stale output row")),
        );
        assert!(stale_ids.is_none());
        Ok(())
    }

    #[cfg(feature = "messaging")]
    #[sinex_test]
    async fn log_self_observation_failure_accepts_publish_errors() -> TestResult<()> {
        log_self_observation_failure(
            "test-derived",
            "invalidation.errors",
            &SelfObservationError::Publish("boom".to_string()),
        );
        Ok(())
    }

    #[sinex_test]
    async fn derived_source_state_is_unhealthy_before_runtime_initialization() -> TestResult<()> {
        let adapter = DerivedNodeAdapter::new(TransducerWrapper(TestDerivedNode));

        let state = ExplorationProvider::get_source_state(&adapter)?;

        assert!(!state.is_connected);
        assert!(!state.healthy);
        assert_eq!(state.last_updated, None);
        assert!(state.description.contains("runtime not initialized"));
        assert_eq!(
            state
                .metadata
                .get("runtime_initialized")
                .and_then(serde_json::Value::as_bool),
            Some(false)
        );
        Ok(())
    }

    #[sinex_test]
    async fn try_restore_from_file_rejects_missing_state_payload() -> TestResult<()> {
        let temp_dir = tempdir()?;
        let checkpoint_path = temp_dir.path().join("derived-empty-state.checkpoint.json");
        CheckpointState {
            checkpoint: Checkpoint::None,
            processed_count: 0,
            last_activity: Timestamp::now(),
            data: None,
            version: 2,
            revision: 0,
        }
        .save_to_file(&checkpoint_path)
        .await?;

        let mut adapter = DerivedNodeAdapter::with_shutdown_config(
            TransducerWrapper(TestDerivedNode),
            ShutdownConfig {
                checkpoint_path: Some(checkpoint_path.clone()),
                ..ShutdownConfig::default()
            },
        );

        let error = adapter
            .try_restore_from_file()
            .await
            .expect_err("empty hot reload state must not be treated as absent");
        let message = format!("{error:#}");
        assert!(message.contains("missing state data"));
        assert!(message.contains("derived-adapter-test"));
        assert!(message.contains(&checkpoint_path.display().to_string()));
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
            "derived-adapter-test".to_string(),
            "test-group".to_string(),
            "fresh-consumer".to_string(),
        );

        let mut adapter = DerivedNodeAdapter::new(TransducerWrapper(TestDerivedNode));
        adapter.checkpoint_manager = Some(Arc::new(manager));
        adapter
            .load_state()
            .await
            .expect("fresh derived checkpoint state should be treated as a clean start");

        assert_eq!(adapter.persisted_state.events_processed, 0);
        assert_eq!(adapter.last_revision, 0);
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
            "derived-adapter-test".to_string(),
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

        let mut adapter = DerivedNodeAdapter::new(TransducerWrapper(TestDerivedNode));
        adapter.checkpoint_manager = Some(Arc::new(manager));

        let error = adapter
            .load_state()
            .await
            .expect_err("empty derived checkpoint KV state must not be treated as fresh");
        let message = format!("{error:#}");
        assert!(message.contains("missing state data"));
        assert!(message.contains("derived-adapter-test"));
        Ok(())
    }

    #[sinex_test]
    async fn process_batch_halts_on_retry_error() -> TestResult<()> {
        let seen = Arc::new(AtomicUsize::new(0));
        let node = RetryDerivedNode {
            seen: Arc::clone(&seen),
        };
        let mut adapter = DerivedNodeAdapter::new(TransducerWrapper(node));

        let error = adapter
            .process_batch(vec![
                make_input_event("first")?,
                make_input_event("second")?,
            ])
            .await
            .expect_err("retry errors must stop the batch");

        assert!(
            error.to_string().contains("retry"),
            "retryable batch failure should propagate an explicit error: {error:#}"
        );
        assert_eq!(
            seen.load(Ordering::SeqCst),
            1,
            "batch processing must stop at the first retryable error"
        );
        Ok(())
    }

    #[sinex_test]
    async fn process_batch_surfaces_checkpoint_save_failures(ctx: TestContext) -> TestResult<()> {
        let ctx = ctx.with_nats().shared().await?;
        let mut adapter = DerivedNodeAdapter::with_config(
            TransducerWrapper(UnserializableDerivedNode),
            DerivedNodeConfig {
                checkpoint_interval: 1,
                ..DerivedNodeConfig::default()
            },
        );
        adapter.runtime = Some(
            make_runtime_state(
                &ctx,
                "adapter-regression-unserializable-checkpoint",
                Some(Uuid::now_v7()),
            )
            .await?,
        );
        adapter.checkpoint_manager = Some(
            adapter
                .runtime
                .as_ref()
                .expect("runtime set")
                .checkpoint_manager(),
        );

        let error = adapter
            .process_batch(vec![make_input_event("checkpoint")?])
            .await
            .expect_err("checkpoint serialization failures must fail the batch");

        assert!(
            error.to_string().contains("serialize state"),
            "checkpoint save failure should surface serialization context: {error:#}"
        );
        Ok(())
    }

    #[sinex_test]
    async fn derived_outputs_propagate_runtime_node_run_id(ctx: TestContext) -> TestResult<()> {
        let ctx = ctx.with_nats().shared().await?;
        let node_run_id = Uuid::now_v7();
        let mut adapter = DerivedNodeAdapter::new(TransducerWrapper(EmittingDerivedNode));
        adapter.runtime = Some(
            make_runtime_state(&ctx, "derived-adapter-emitting-test", Some(node_run_id)).await?,
        );

        let outputs = adapter.process_one(make_input_event("emit")?).await?;
        let output = outputs
            .into_iter()
            .next()
            .expect("emitting node should produce one output event");

        assert_eq!(output.node_run_id, Some(node_run_id));
        Ok(())
    }

    #[sinex_test]
    async fn current_checkpoint_tracks_last_processed_input_event() -> TestResult<()> {
        let mut adapter = DerivedNodeAdapter::new(TransducerWrapper(TestDerivedNode));
        let input = make_input_event("checkpoint-me")?;
        let input_id = input.id.expect("test input must have an id");

        let _ = adapter.process_one(input).await?;

        assert_eq!(
            adapter.current_checkpoint_internal(),
            Checkpoint::internal(*input_id.as_uuid(), 1)
        );
        Ok(())
    }

    #[sinex_test]
    async fn load_state_restores_resume_position_from_checkpoint_metadata() -> TestResult<()> {
        let temp_dir = tempdir()?;
        let checkpoint_path = temp_dir
            .path()
            .join("derived-legacy-resume-position.checkpoint.json");
        let resume_event_id = Uuid::now_v7();
        let legacy_state = serde_json::json!({
            "state": null,
            "events_processed": 7,
            "last_checkpoint": Timestamp::now(),
            "version": 1
        });
        CheckpointState {
            checkpoint: Checkpoint::internal(resume_event_id, 7),
            processed_count: 7,
            last_activity: Timestamp::now(),
            data: Some(legacy_state),
            version: 2,
            revision: 0,
        }
        .save_to_file(&checkpoint_path)
        .await?;

        let mut adapter = DerivedNodeAdapter::with_shutdown_config(
            TransducerWrapper(TestDerivedNode),
            ShutdownConfig {
                checkpoint_path: Some(checkpoint_path.clone()),
                ..ShutdownConfig::default()
            },
        );

        adapter.load_state().await?;

        assert_eq!(
            adapter.current_checkpoint_internal(),
            Checkpoint::internal(resume_event_id, 7)
        );
        Ok(())
    }

    #[sinex_test]
    async fn load_state_restores_hot_reload_revision_for_followup_save(
        ctx: TestContext,
    ) -> TestResult<()> {
        let ctx = ctx.with_nats().shared().await?;
        let kv = ctx.checkpoint_kv().await?;
        let manager = Arc::new(CheckpointManager::new(
            kv,
            "derived-adapter-hot-reload-revision-test".to_string(),
            "test-group".to_string(),
            "hot-reload-consumer".to_string(),
        ));

        let persisted_json = serde_json::json!({
            "state": null,
            "events_processed": 3,
            "last_checkpoint": Timestamp::now(),
            "version": 1,
            "last_input_event_id": Uuid::now_v7(),
        });
        let baseline_revision = manager
            .save_checkpoint(&CheckpointState {
                checkpoint: Checkpoint::internal(Uuid::now_v7(), 3),
                processed_count: 3,
                last_activity: Timestamp::now(),
                data: Some(persisted_json.clone()),
                version: 2,
                revision: 0,
            })
            .await?;

        let temp_dir = tempdir()?;
        let checkpoint_path = temp_dir
            .path()
            .join("derived-hot-reload-revision.checkpoint.json");
        CheckpointState {
            checkpoint: Checkpoint::internal(Uuid::now_v7(), 3),
            processed_count: 3,
            last_activity: Timestamp::now(),
            data: Some(persisted_json),
            version: 2,
            revision: baseline_revision,
        }
        .save_to_file(&checkpoint_path)
        .await?;

        let mut adapter = DerivedNodeAdapter::with_shutdown_config(
            TransducerWrapper(TestDerivedNode),
            ShutdownConfig {
                checkpoint_path: Some(checkpoint_path.clone()),
                ..ShutdownConfig::default()
            },
        );
        adapter.checkpoint_manager = Some(Arc::clone(&manager));

        adapter.load_state().await?;
        assert_eq!(adapter.last_revision, baseline_revision);
        assert!(
            CheckpointState::load_from_file(&checkpoint_path)
                .await?
                .is_some(),
            "restored hot reload file must remain until the state is durably re-saved"
        );

        adapter.save_state().await?;
        assert!(
            adapter.last_revision > baseline_revision,
            "restored hot reload state must keep the prior KV revision so the next save updates instead of blind-creating"
        );
        assert!(
            CheckpointState::load_from_file(&checkpoint_path)
                .await?
                .is_none(),
            "restored hot reload file should be cleaned up after successful KV sync"
        );
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
            "derived-adapter-hot-reload-fallback-test".to_string(),
            "test-group".to_string(),
            "hot-reload-fallback-consumer".to_string(),
        ));

        let persisted_json = serde_json::json!({
            "state": null,
            "events_processed": 9,
            "last_checkpoint": Timestamp::now(),
            "version": 1,
            "last_input_event_id": Uuid::now_v7(),
        });
        let revision = manager
            .save_checkpoint(&CheckpointState {
                checkpoint: Checkpoint::internal(Uuid::now_v7(), 9),
                processed_count: 9,
                last_activity: Timestamp::now(),
                data: Some(persisted_json),
                version: 2,
                revision: 0,
            })
            .await?;

        let temp_dir = tempdir()?;
        let checkpoint_path = temp_dir
            .path()
            .join("derived-hot-reload-fallback.checkpoint.json");
        tokio::fs::write(&checkpoint_path, "{ definitely not valid json").await?;

        let mut adapter = DerivedNodeAdapter::with_shutdown_config(
            TransducerWrapper(TestDerivedNode),
            ShutdownConfig {
                checkpoint_path: Some(checkpoint_path.clone()),
                ..ShutdownConfig::default()
            },
        );
        adapter.checkpoint_manager = Some(Arc::clone(&manager));

        adapter
            .load_state()
            .await
            .expect("corrupt hot reload file should fall back to healthy KV state");

        assert_eq!(adapter.last_revision, revision);
        assert_eq!(adapter.persisted_state.events_processed, 9);
        assert!(
            CheckpointState::load_from_file(&checkpoint_path)
                .await?
                .is_none(),
            "corrupt hot reload file should be discarded after successful KV restore"
        );
        Ok(())
    }

    #[sinex_test]
    async fn historical_replay_resumes_from_internal_checkpoint(
        ctx: TestContext,
    ) -> TestResult<()> {
        let ctx = ctx.with_nats().shared().await?;
        let inserted = ctx
            .pool()
            .events()
            .insert_batch(vec![
                make_input_event("first")?,
                make_input_event("second")?,
                make_input_event("third")?,
            ])
            .await?;
        let second_id = inserted[1].id.expect("inserted event must have an id");
        let third_id = inserted[2].id.expect("inserted event must have an id");

        let (runtime, _event_receiver) =
            make_runtime_state_with_db(&ctx, "derived-history-resume-test", None).await?;
        let mut adapter = DerivedNodeAdapter::new(TransducerWrapper(EmittingDerivedNode));
        adapter.checkpoint_manager = Some(runtime.checkpoint_manager());
        adapter.event_sender = Some(runtime.event_sender());
        adapter.host = runtime.service_info().host().to_string();
        adapter.runtime = Some(runtime);

        let report = adapter
            .run_historical(
                Checkpoint::internal(*second_id.as_uuid(), 2),
                Timestamp::now(),
                ScanArgs::default(),
            )
            .await?;

        assert_eq!(report.events_processed, 1);
        assert_eq!(
            report.final_checkpoint,
            Checkpoint::internal(*third_id.as_uuid(), 1)
        );
        Ok(())
    }

    #[cfg(feature = "messaging")]
    #[sinex_test]
    async fn handle_invalidation_message_returns_none_when_output_emit_fails(
        ctx: TestContext,
    ) -> TestResult<()> {
        use sinex_db::DbPoolExt;
        use sinex_primitives::events::DynamicPayload;
        use sinex_primitives::query::{AggregationMode, EventQuery, EventQueryResult};
        use sinex_primitives::{EventSource, EventType};
        use super::super::DerivedScopeInvalidation;

        let ctx = ctx.with_nats().dedicated().await?;
        let material_id = ctx
            .create_source_material(Some("derived-invalidation-output-send-failure"))
            .await?;
        let scope_key = "scope:output-send-failure";

        let mut input = DynamicPayload::new(
            "measurements",
            "measurement.taken",
            serde_json::json!({ "value": 5_i64 }),
        )
        .from_material(material_id)
        .build()?;
        input.scope_key = Some(scope_key.to_string());

        let inserted = ctx.pool().events().insert_batch(vec![input]).await?;
        let input_id = inserted
            .first()
            .and_then(|event| event.id)
            .expect("inserted input should have id");
        let mut stale_output = DynamicPayload::new(
            "adapter-regression-scope-reconciler",
            "measurement.aggregate",
            serde_json::json!({ "total": 5_i64, "count": 1_u64 }),
        )
        .from_parents(vec![input_id])?
        .build()?;
        stale_output.scope_key = Some(scope_key.to_string());
        ctx.pool().events().insert_batch(vec![stale_output]).await?;

        let (runtime, event_receiver) = make_runtime_state_with_db(
            &ctx,
            "adapter-regression-scope-reconciler",
            None,
        )
        .await?;
        drop(event_receiver);

        let mut adapter = DerivedNodeAdapter::new(ScopeReconcilerWrapper(TestScopeReconcilerNode));
        adapter.checkpoint_manager = Some(runtime.checkpoint_manager());
        adapter.event_sender = Some(runtime.event_sender());
        adapter.host = runtime.service_info().host().to_string();
        adapter.runtime = Some(runtime);

        let invalidation = DerivedScopeInvalidation::replaced(
            vec![*input_id.as_uuid()],
            EventSource::from_static("measurements"),
            EventType::from_static("measurement.taken"),
        )
        .with_scope_keys(vec![scope_key.to_string()]);
        let payload = serde_json::to_vec(&invalidation)?;

        let result = adapter.handle_invalidation_message(&payload).await;
        assert!(
            result.is_none(),
            "output send failures must fail invalidation handling"
        );
        let live_output_count = match ctx
            .pool()
            .events()
            .query(EventQuery {
                sources: vec![EventSource::new("adapter-regression-scope-reconciler")?],
                event_types: vec![EventType::new("measurement.aggregate")?],
                scope_key: Some(scope_key.to_string()),
                aggregation: Some(AggregationMode::Count),
                ..EventQuery::default()
            })
            .await?
        {
            EventQueryResult::Count { count } => count,
            other => panic!("expected count result, got {other:?}"),
        };
        assert_eq!(
            live_output_count, 1,
            "stale outputs must remain live when replacement emission fails"
        );

        let archived_output_count = sqlx::query_scalar!(
            r#"
            SELECT COUNT(*)::bigint as "count!"
            FROM audit.archived_events
            WHERE source = $1 AND event_type = $2 AND scope_key = $3
            "#,
            "adapter-regression-scope-reconciler",
            "measurement.aggregate",
            scope_key
        )
        .fetch_one(ctx.pool())
        .await?;
        assert_eq!(
            archived_output_count, 0,
            "replacement emission failure must not archive stale outputs"
        );
        Ok(())
    }

    #[cfg(feature = "messaging")]
    #[sinex_test]
    async fn handle_invalidation_message_checkpoints_state_only_mutations(
        ctx: TestContext,
    ) -> TestResult<()> {
        use sinex_db::DbPoolExt;
        use sinex_primitives::events::DynamicPayload;
        use sinex_primitives::{EventSource, EventType};
        use super::super::DerivedScopeInvalidation;

        let ctx = ctx.with_nats().dedicated().await?;
        let material_id = ctx
            .create_source_material(Some("derived-invalidation-state-only"))
            .await?;
        let scope_key = "scope:state-only";

        let mut input = DynamicPayload::new(
            "measurements",
            "measurement.taken",
            serde_json::json!({ "value": 7_i64 }),
        )
        .from_material(material_id)
        .build()?;
        input.scope_key = Some(scope_key.to_string());
        ctx.pool().events().insert_batch(vec![input]).await?;

        let (runtime, _event_receiver) = make_runtime_state_with_db(
            &ctx,
            "adapter-regression-stateful-invalidation",
            None,
        )
        .await?;

        let mut adapter = DerivedNodeAdapter::with_config(
            ScopeReconcilerWrapper(StatefulInvalidationNode),
            DerivedNodeConfig {
                checkpoint_interval: 1,
                ..DerivedNodeConfig::default()
            },
        );
        adapter.checkpoint_manager = Some(runtime.checkpoint_manager());
        adapter.event_sender = Some(runtime.event_sender());
        adapter.host = runtime.service_info().host().to_string();
        adapter.runtime = Some(runtime);

        let invalidation = DerivedScopeInvalidation::replaced(
            Vec::new(),
            EventSource::from_static("measurements"),
            EventType::from_static("measurement.taken"),
        )
        .with_scope_keys(vec![scope_key.to_string()]);
        let payload = serde_json::to_vec(&invalidation)?;

        let processed = adapter.handle_invalidation_message(&payload).await;
        assert_eq!(
            processed,
            Some(0),
            "state-only invalidation should still be treated as a successful recomputation"
        );
        assert_eq!(adapter.persisted_state.state.invalidations_applied, 1);
        assert!(
            adapter.last_revision > 0,
            "state-only invalidation should force a checkpoint-worthy state save"
        );
        assert_eq!(
            adapter.events_since_checkpoint,
            0,
            "successful invalidation checkpoint should clear the dirty counter"
        );
        Ok(())
    }

    #[cfg(feature = "messaging")]
    #[sinex_test]
    async fn historical_replay_fails_when_dlq_routing_fails(
        ctx: TestContext,
    ) -> TestResult<()> {
        use sinex_db::DbPoolExt;

        let ctx = ctx.with_nats().dedicated().await?;
        let inserted = ctx
            .pool()
            .events()
            .insert_batch(vec![make_input_event("route-to-dlq")?])
            .await?;
        let input_id = inserted[0].id.expect("inserted event should have an id");

        let (runtime, _event_receiver) =
            make_runtime_state_with_db(&ctx, "derived-adapter-dlq-retry-test", None).await?;
        let mut adapter = DerivedNodeAdapter::new(TransducerWrapper(DlqRetryDerivedNode));
        adapter.checkpoint_manager = Some(runtime.checkpoint_manager());
        adapter.event_sender = Some(runtime.event_sender());
        adapter.host = runtime.service_info().host().to_string();
        adapter.runtime = Some(runtime);

        let error = adapter
            .run_historical(Checkpoint::None, Timestamp::now(), ScanArgs::default())
            .await
            .expect_err("historical replay must fail when DLQ routing fails");

        let rendered = format!("{error:#}");
        assert!(rendered.contains("failed to send derived-node event to DLQ"));
        assert!(rendered.contains("route me to dlq"));
        assert!(rendered.contains("derived-adapter-dlq-retry-test"));
        assert!(
            adapter.events_processed() == 0,
            "failing DLQ routing must not advance replay progress past the bad event"
        );
        assert_eq!(adapter.current_checkpoint_internal(), Checkpoint::None);
        assert_eq!(input_id, inserted[0].id.expect("id should stay available"));
        Ok(())
    }
}

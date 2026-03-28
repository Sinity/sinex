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
use sinex_primitives::query::{EventQuery, EventQueryResult, QueryResultEvent};
use sinex_primitives::temporal::Timestamp;
use sinex_primitives::{EventSource, EventType, HostName, Id, JsonValue, Pagination};

use std::collections::HashMap;
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
    event_sender: Option<EventSender>,
    shutdown_tx: Option<watch::Sender<bool>>,
    host: String,
    events_since_checkpoint: u64,
    last_checkpoint_time: Instant,
    last_revision: u64,
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

    // ── Checkpoint Management ──────────────────────────────────────────

    async fn load_state(&mut self) -> NodeResult<()> {
        // Priority 1: file-based checkpoint (hot reload)
        if self.shutdown_config.restore_state_on_startup
            && let Some(persisted) = self.try_restore_from_file().await?
        {
            self.persisted_state = persisted;
            return Ok(());
        }

        // Priority 2: NATS KV checkpoint
        let Some(checkpoint_mgr) = &self.checkpoint_manager else {
            return Ok(());
        };

        let checkpoint_state = checkpoint_mgr.load_checkpoint().await?;
        if let Some(data) = checkpoint_state.data {
            let persisted: PersistedState<N::State> = decode_checkpoint_data(
                data,
                "derived checkpoint state",
                self.node.name(),
            )?;
            info!(
                node = %self.node.name(),
                events_processed = persisted.events_processed,
                "Restored state from NATS KV checkpoint"
            );
            self.persisted_state = persisted;
            self.last_revision = checkpoint_state.revision;
        } else {
            info!(node = %self.node.name(), "No valid checkpoint, starting fresh");
            self.persisted_state = PersistedState::default();
        }

        Ok(())
    }

    async fn try_restore_from_file(&self) -> NodeResult<Option<PersistedState<N::State>>> {
        let checkpoint_path = self.shutdown_config.checkpoint_path(self.node.name());
        let Some(file_state) = CheckpointState::load_from_file(&checkpoint_path).await? else {
            return Ok(None);
        };
        let Some(data) = file_state.data else {
            return Ok(None);
        };

        let persisted: PersistedState<N::State> = decode_checkpoint_data(
            data,
            "derived hot reload state",
            self.node.name(),
        )?;
        info!(
            node = %self.node.name(),
            events_processed = persisted.events_processed,
            "Restored state from hot reload file"
        );
        CheckpointState::delete_file(&checkpoint_path)
            .await
            .map_err(|error| {
                SinexError::io("Failed to delete hot reload file after loading state")
                    .with_context("node", self.node.name())
                    .with_context("path", checkpoint_path.display().to_string())
                    .with_std_error(&error)
            })?;
        Ok(Some(persisted))
    }

    pub async fn save_state_to_file(&self) -> std::io::Result<()> {
        if !self.shutdown_config.save_state_on_shutdown {
            return Ok(());
        }

        let checkpoint_path = self.shutdown_config.checkpoint_path(self.node.name());
        let state_json = serde_json::to_value(&self.persisted_state)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

        let checkpoint_state = CheckpointState {
            checkpoint: Checkpoint::external(
                serde_json::json!({"version": self.persisted_state.version}),
                format!("derived_node_{}", self.node.name()),
            ),
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

        let checkpoint_state = CheckpointState {
            checkpoint: Checkpoint::external(
                serde_json::json!({"version": self.persisted_state.version}),
                format!("derived_node_{}", self.node.name()),
            ),
            processed_count: self.persisted_state.events_processed,
            last_activity: Timestamp::now(),
            data: Some(state_json),
            version: 2,
            revision: self.last_revision,
        };

        self.last_revision = checkpoint_mgr.save_checkpoint(&checkpoint_state).await?;
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

    fn current_checkpoint_internal(&self) -> NodeResult<Checkpoint> {
        let state_json = serde_json::to_value(&self.persisted_state).map_err(|error| {
            SinexError::serialization("failed to serialize current derived node checkpoint state")
                .with_context("node", self.node.name().to_string())
                .with_std_error(&error)
        })?;
        Ok(Checkpoint::external(
            state_json,
            format!("derived_node_{}", self.node.name()),
        ))
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
                    let sinex_error = SinexError::processing(e.to_string());
                    reporter.record_error(&sinex_error);
                }
            }

            if let Err(e) = reporter.check_and_emit().await {
                warn!(node = %self.node.name(), error = %e, "Failed to emit health status");
            }
        }

        match result {
            Ok(outputs) => {
                let output_events = self.build_output_events(outputs, source_event_id, &context)?;
                self.persisted_state.events_processed += 1;
                self.events_since_checkpoint += 1;
                Ok(output_events)
            }
            Err(e) => {
                let action = self.node.handle_error_derived(&e);
                match action {
                    ErrorAction::Skip => {
                        warn!(node = %self.node.name(), error = %e, "Skipping event");
                        self.persisted_state.events_processed += 1;
                        self.events_since_checkpoint += 1;
                        Ok(Vec::new())
                    }
                    ErrorAction::SendToDLQ => {
                        self.send_to_dlq_or_fail(&event, &e).await?;
                        self.persisted_state.events_processed += 1;
                        self.events_since_checkpoint += 1;
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
        fallback_source_id: Id<Event<JsonValue>>,
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
        fallback_source_id: Id<Event<JsonValue>>,
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
        let filtered_payload = privacy::engine().process_json(&payload, privacy_context);
        if filtered_payload != payload {
            debug!(
                node = %self.node.name(),
                output_event_type = %self.node.output_event_type(),
                ?privacy_context,
                "Applied privacy filtering to derived output payload"
            );
        }

        let typed_ids: Vec<Id<Event<JsonValue>>> = source_event_ids
            .into_iter()
            .map(Id::from_uuid)
            .collect();
        let source_event_ids = NonEmptyVec::from_vec(typed_ids)
            .unwrap_or_else(|| NonEmptyVec::single(fallback_source_id));
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
            node_run_id: None,
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
    pub async fn process_batch(
        &mut self,
        events: Vec<Event<JsonValue>>,
    ) -> NodeResult<Vec<Event<JsonValue>>> {
        let mut outputs = Vec::new();

        for event in events {
            match self.process_one(event).await {
                Ok(mut output_events) => outputs.append(&mut output_events),
                Err(e) => {
                    error!(node = %self.node.name(), error = %e, "Error processing event in batch");
                }
            }
        }

        if self.should_checkpoint()
            && let Err(e) = self.save_state().await
        {
            warn!(node = %self.node.name(), error = %e, "Failed to save checkpoint after batch");
        }

        Ok(outputs)
    }

    // ── Invalidation Processing ──────────────────────────────────────────

    /// Process a scope invalidation signal.
    ///
    /// For each affected scope:
    /// 1. Loads the current working set from DB (events matching `scope_key` + `input_event_type`)
    /// 2. Archives existing derived outputs for that scope (moves to `audit.archived_events`)
    /// 3. Calls `process_invalidation_derived()` to recompute
    /// 4. Records replacement relations in `audit.event_replacements` (old→new linkage)
    /// 5. Returns replacement events for emission
    ///
    /// Transducer nodes return empty — their outputs are archived with their inputs.
    #[cfg(feature = "db")]
    pub async fn process_invalidation(
        &mut self,
        invalidation: &super::invalidation::DerivedScopeInvalidation,
    ) -> NodeResult<Vec<Event<JsonValue>>> {
        use sinex_db::repositories::{DbPoolExt, ReplacementKind, ReplacementRecord};
        use sinex_primitives::prelude::*;

        // Only process invalidations for our input type
        if !invalidation.matches_input(self.node.input_event_type()) {
            return Ok(Vec::new());
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
                        return Err(
                            SinexError::database(
                                "Failed to load affected event while deriving invalidation scope keys",
                            )
                            .with_context("event_id", id.to_string())
                            .with_context("node", self.node.name())
                            .with_source(error),
                        );
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
            return Ok(Vec::new());
        }

        let output_source = self.node.output_event_source();
        let output_type = self.node.output_event_type();
        let mut all_outputs = Vec::new();

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

            let stale_ids: Vec<Uuid> =
                match self
                    .load_query_events_paginated(&pool, stale_query, scope_key, "stale outputs")
                    .await
                {
                    Ok(events) => events
                        .iter()
                        .filter_map(|qe| qe.event.id.map(|id| *id.as_uuid()))
                        .collect(),
                Err(e) => {
                    warn!(
                        node = %self.node.name(),
                        scope_key,
                        error = %e,
                        "Failed to query stale outputs — proceeding without archive"
                    );
                    Vec::new()
                }
            };

            // ── Step 2: Archive stale outputs ──
            if !stale_ids.is_empty() {
                match pool
                    .events()
                    .execute_cascade_archive(
                        &stale_ids,
                        "scope_invalidation_recompute",
                        &operation_uuid.to_string(),
                        &format!("derived:{}", self.node.name()),
                    )
                    .await
                {
                    Ok(archived) => {
                        info!(
                            node = %self.node.name(),
                            scope_key,
                            archived_count = archived,
                            "Archived stale derived outputs before recomputation"
                        );
                    }
                    Err(e) => {
                        error!(
                            node = %self.node.name(),
                            scope_key,
                            error = %e,
                            "Failed to archive stale outputs — skipping scope to prevent duplicates"
                        );
                        continue;
                    }
                }
            }

            // ── Step 3: Load working set (input events for this scope) ──
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
                source: EventSource::new(&invalidation.event_source)
                    .unwrap_or_else(|_| EventSource::from_static("unknown")),
                event_type: EventType::new(&invalidation.event_type)
                    .unwrap_or_else(|_| EventType::from_static("unknown")),
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

            // ── Step 4: Recompute via trait implementation ──
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
            let fallback_id = Id::new();
            let mut new_event_ids = Vec::new();
            for output in outputs {
                let equivalence_key = output.equivalence_key.clone();
                let output_event = self.build_output_event(output, fallback_id, &context)?;
                let new_id = *output_event.id.unwrap_or_else(Id::new).as_uuid();
                new_event_ids.push((new_id, equivalence_key));
                all_outputs.push(output_event);
            }

            // ── Step 5: Record replacement relations ──
            if !stale_ids.is_empty() && !new_event_ids.is_empty() {
                let replacements: Vec<ReplacementRecord> = stale_ids
                    .iter()
                    .flat_map(|old_id| {
                        new_event_ids
                            .iter()
                            .map(move |(new_id, eq_key)| ReplacementRecord {
                                old_event_id: *old_id,
                                new_event_id: *new_id,
                                relation_kind: ReplacementKind::Recomputed,
                                scope_key: Some(scope_key.clone()),
                                equivalence_key: eq_key.clone(),
                            })
                    })
                    .collect();

                if let Err(e) = pool
                    .events()
                    .record_replacements(operation_uuid, &replacements)
                    .await
                {
                    warn!(
                        node = %self.node.name(),
                        scope_key,
                        error = %e,
                        "Failed to record replacement relations — events still correct"
                    );
                }
            }
        }

        info!(
            node = %self.node.name(),
            scopes_recomputed = scope_keys.len(),
            outputs_produced = all_outputs.len(),
            "Invalidation processing complete"
        );

        Ok(all_outputs)
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
            match self.process_invalidation(&invalidation).await {
                Ok(outputs) => {
                    let count = outputs.len() as u64;
                    let duration_ms = processing_start.elapsed().as_millis() as f64;

                    if let Some(ref sender) = self.event_sender {
                        for event in outputs {
                            if let Err(e) = sender.send(event).await {
                                error!(
                                    node = %node_name,
                                    error = %e,
                                    "Failed to emit invalidation output event"
                                );
                            }
                        }
                    }
                    if self.should_checkpoint() {
                        if let Err(e) = self.save_state().await {
                            warn!(
                                node = %node_name,
                                error = %e,
                                "Failed to checkpoint after invalidation"
                            );
                        }
                    }

                    // Emit success metrics
                    #[cfg(feature = "messaging")]
                    if let Some(ref obs) = self.self_observer {
                        if let Err(error) = obs.emit_counter("invalidation.processed", 1, None).await
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
            final_checkpoint: self.current_checkpoint_internal()?,
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
        _from: Checkpoint,
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

        let time_range = TimeRange::new(None, Some(end_time))
            .map_err(|e| SinexError::validation(format!("Invalid time range: {e}")))?;

        let mut events_processed = 0u64;
        let mut events_emitted = 0u64;
        let batch_size: i64 = 500;
        let mut cursor: Option<sinex_primitives::Cursor> = None;

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
                            self.build_output_events(outputs, ctx.trigger_event_id, &ctx)?;
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
                        warn!(node = %self.node.name(), error = %e, "Error in historical replay, skipping");
                    }
                }
                events_processed += 1;
                self.persisted_state.events_processed += 1;
                self.events_since_checkpoint += 1;
            }

            match next_cursor {
                Some(c) => {
                    cursor = Some(c);
                }
                None => break,
            }
        }

        if let Err(e) = self.save_state().await {
            warn!(node = %self.node.name(), error = %e, "Failed to save checkpoint after replay");
        }

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
            final_checkpoint: self.current_checkpoint_internal()?,
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
        self.current_checkpoint_internal()
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
            last_updated: Timestamp::now(),
            lag_seconds: None,
            recent_activity: Vec::new(),
            total_items: None,
            metadata: HashMap::from([
                ("runtime_initialized".to_string(), serde_json::json!(runtime_initialized)),
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
    use super::DerivedNodeAdapter;
    #[cfg(feature = "messaging")]
    use super::log_self_observation_failure;
    use crate::derived_node::{DerivedOutput, DerivedTriggerContext, TransducerWrapper};
    use crate::exploration::ExplorationProvider;
    use crate::{NodeLogicError, TransducerNode};
    use super::signal_shutdown_channel;
    #[cfg(feature = "messaging")]
    use crate::self_observation::SelfObservationError;
    use serde::{Deserialize, Serialize};
    use sinex_primitives::JsonValue;
    use sinex_primitives::privacy::ProcessingContext;
    use tokio::sync::watch;
    use xtask::sandbox::sinex_test;

    #[derive(Default, Serialize, Deserialize)]
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
}

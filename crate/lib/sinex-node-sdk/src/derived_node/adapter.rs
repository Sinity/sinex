//! `DerivedNodeAdapter` — shared runtime adapter for all derived node models.
//!
//! This replaces `AutomatonNodeAdapter`. It wraps any [`DerivedNodeImpl`] and
//! implements the stream [`Node`] trait, handling checkpoints, health monitoring,
//! shutdown, and event emission.

use super::context::DerivedTriggerContext;
use super::output::DerivedOutput;
use super::traits::{DerivedNodeConfig, DerivedNodeImpl};

use crate::automaton_node::{ErrorAction, PersistedState};
use crate::checkpoint::{CheckpointManager, CheckpointState};
use crate::runtime::stream::{
    Checkpoint, EventSender, NodeCapabilities, NodeInitContext, NodeRuntimeState, NodeType,
    ScanArgs, ScanEstimate, ScanReport, TimeHorizon,
};
use crate::shutdown::ShutdownConfig;
use crate::{NodeResult, SinexError};

use sinex_primitives::events::Event;
use sinex_primitives::events::builder::{Operation, Provenance};
use sinex_primitives::non_empty::NonEmptyVec;
use sinex_primitives::temporal::Timestamp;
use sinex_primitives::{EventSource, EventType, HostName, Id, JsonValue};

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::watch;
use tracing::{debug, error, info, warn};

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
    // ── Checkpoint Management ──────────────────────────────────────────

    async fn load_state(&mut self) -> NodeResult<()> {
        // Priority 1: file-based checkpoint (hot reload)
        if self.shutdown_config.restore_state_on_startup
            && let Some(persisted) = self.try_restore_from_file().await
        {
            self.persisted_state = persisted;
            return Ok(());
        }

        // Priority 2: NATS KV checkpoint
        let Some(checkpoint_mgr) = &self.checkpoint_manager else {
            return Ok(());
        };

        let checkpoint_state = checkpoint_mgr.load_checkpoint().await?;
        if let Some(persisted) = checkpoint_state
            .data
            .and_then(|data| serde_json::from_value::<PersistedState<N::State>>(data).ok())
        {
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

    async fn try_restore_from_file(&self) -> Option<PersistedState<N::State>> {
        let checkpoint_path = self.shutdown_config.checkpoint_path(self.node.name());
        let file_state = CheckpointState::load_from_file(&checkpoint_path).await?;
        let data = file_state.data?;

        match serde_json::from_value::<PersistedState<N::State>>(data) {
            Ok(persisted) => {
                info!(
                    node = %self.node.name(),
                    events_processed = persisted.events_processed,
                    "Restored state from hot reload file"
                );
                if let Err(e) = CheckpointState::delete_file(&checkpoint_path).await {
                    error!(node = %self.node.name(), error = %e, "Failed to delete hot reload file");
                }
                Some(persisted)
            }
            Err(e) => {
                warn!(node = %self.node.name(), error = %e, "Failed to deserialize file checkpoint");
                None
            }
        }
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

    fn current_checkpoint_internal(&self) -> Checkpoint {
        let state_json = serde_json::to_value(&self.persisted_state).unwrap_or(JsonValue::Null);
        Checkpoint::external(state_json, format!("derived_node_{}", self.node.name()))
    }

    // ── Event Processing ───────────────────────────────────────────────

    /// Process a single event through the derived node's logic.
    pub async fn process_one(
        &mut self,
        event: Event<JsonValue>,
    ) -> NodeResult<Option<Event<JsonValue>>> {
        let context = DerivedTriggerContext::live(&event);
        let source_event_id = event.id.unwrap_or_default();

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

            if self.persisted_state.events_processed.is_multiple_of(100)
                && let Err(e) = reporter.check_and_emit().await
            {
                warn!(node = %self.node.name(), error = %e, "Failed to emit health status");
            }
        }

        match result {
            Ok(Some(output)) => {
                let output_event = self.build_output_event(output, source_event_id)?;
                self.persisted_state.events_processed += 1;
                self.events_since_checkpoint += 1;
                Ok(Some(output_event))
            }
            Ok(None) => {
                self.persisted_state.events_processed += 1;
                self.events_since_checkpoint += 1;
                Ok(None)
            }
            Err(e) => {
                let action = self.node.handle_error_derived(&e);
                match action {
                    ErrorAction::Skip => {
                        warn!(node = %self.node.name(), error = %e, "Skipping event");
                        self.persisted_state.events_processed += 1;
                        self.events_since_checkpoint += 1;
                        Ok(None)
                    }
                    ErrorAction::SendToDLQ => {
                        if let Some(ref runtime) = self.runtime {
                            let transport = runtime.handles().transport();
                            if let Err(dlq_err) = transport
                                .send_to_dlq(&event, &e.to_string(), self.node.name())
                                .await
                            {
                                error!(
                                    node = %self.node.name(),
                                    error = %e,
                                    dlq_error = %dlq_err,
                                    "Failed to send event to DLQ"
                                );
                            }
                        } else {
                            warn!(node = %self.node.name(), error = %e, "Would send to DLQ but no transport");
                        }
                        self.persisted_state.events_processed += 1;
                        self.events_since_checkpoint += 1;
                        Ok(None)
                    }
                    ErrorAction::Retry => Err(e.into()),
                }
            }
        }
    }

    /// Build an output `Event<JsonValue>` from a `DerivedOutput<JsonValue>`.
    fn build_output_event(
        &self,
        output: DerivedOutput<JsonValue>,
        fallback_source_id: Id<Event<JsonValue>>,
    ) -> NodeResult<Event<JsonValue>> {
        let typed_ids: Vec<Id<Event<JsonValue>>> = output
            .source_event_ids
            .into_iter()
            .map(Id::from_uuid)
            .collect();
        let source_event_ids = NonEmptyVec::from_vec(typed_ids)
            .unwrap_or_else(|| NonEmptyVec::single(fallback_source_id));

        Ok(Event {
            id: Some(Id::new()),
            source: EventSource::new(self.node.output_event_source())?,
            event_type: EventType::new(self.node.output_event_type())?,
            payload: output.payload,
            ts_orig: Some(output.ts_orig),
            host: HostName::new(&self.host),
            node_run_id: None,
            payload_schema_id: None,
            provenance: Provenance::Synthesis {
                source_event_ids,
                operation_id: None,
            },
            associated_blob_ids: None,
            temporal_policy: Some(output.temporal_policy),
            semantics_version: output.semantics_version,
            scope_key: output.scope_key,
            equivalence_key: output.equivalence_key,
            created_by_operation_id: None,
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
                Ok(Some(output_event)) => outputs.push(output_event),
                Ok(None) => {}
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
    /// 2. Calls `process_invalidation_derived()` to recompute
    /// 3. Returns replacement events (caller is responsible for archiving old outputs)
    ///
    /// Transducer nodes return empty — their outputs are archived with their inputs.
    #[cfg(feature = "db")]
    pub async fn process_invalidation(
        &mut self,
        invalidation: &super::invalidation::DerivedScopeInvalidation,
    ) -> NodeResult<Vec<Event<JsonValue>>> {
        use sinex_db::repositories::DbPoolExt;
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
                if let Ok(Some(event)) = pool.events().get_by_id(*id).await
                    && let Some(ref sk) = event.scope_key
                    && !keys.contains(sk)
                {
                    keys.push(sk.clone());
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

        let mut all_outputs = Vec::new();

        for scope_key in &scope_keys {
            // Query working set: all live events for this scope + input type
            let query = EventQuery {
                event_types: vec![EventType::new(self.node.input_event_type())?],
                scope_key: Some(scope_key.clone()),
                direction: SortDirection::Asc,
                limit: 10_000, // reasonable upper bound for a single scope
                ..EventQuery::default()
            };

            let result = pool.events().query(query).await.map_err(|e| {
                SinexError::database(format!("Failed to load working set for scope: {e}"))
            })?;

            let working_set = match result {
                EventQueryResult::Events { events, .. } => {
                    events.into_iter().map(|qe| qe.event).collect::<Vec<_>>()
                }
                _ => Vec::new(),
            };

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

            // Delegate to the trait implementation
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
            for output in outputs {
                let output_event = self.build_output_event(output, fallback_id)?;
                all_outputs.push(output_event);
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

    async fn run_continuous(&mut self, _from: Checkpoint) -> NodeResult<ScanReport> {
        let start = Instant::now();

        info!(
            node = %self.node.name(),
            model = %self.node.node_model(),
            input_type = %self.node.input_event_type(),
            output_type = %self.node.output_event_type(),
            "DerivedNode initialized — awaiting events via process_batch()"
        );

        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        self.shutdown_tx = Some(shutdown_tx);

        loop {
            tokio::select! {
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        info!(node = %self.node.name(), "Shutdown signal received");
                        break;
                    }
                }
                () = tokio::time::sleep(Duration::from_mins(1)) => {
                    if self.events_since_checkpoint > 0
                        && let Err(e) = self.save_state().await
                    {
                        warn!(node = %self.node.name(), error = %e, "Failed to save periodic checkpoint");
                    }
                }
            }
        }

        if let Err(e) = self.save_state().await {
            warn!(node = %self.node.name(), error = %e, "Failed to save final checkpoint");
        }

        Ok(ScanReport {
            events_processed: 0,
            duration: start.elapsed(),
            final_checkpoint: self.current_checkpoint_internal(),
            time_range: None,
            node_stats: HashMap::from([(
                "total_processed".to_string(),
                self.persisted_state.events_processed,
            )]),
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
                let ctx = DerivedTriggerContext::historical(&query_event.event, operation_id);

                match self
                    .node
                    .process_derived(
                        &mut self.persisted_state.state,
                        query_event.event.clone(),
                        &ctx,
                    )
                    .await
                {
                    Ok(Some(output)) => {
                        let source_id = query_event.event.id.unwrap_or_default();
                        let output_event = self.build_output_event(output, source_id)?;
                        if let Some(ref sender) = self.event_sender {
                            sender.send(output_event).await.map_err(|_| {
                                SinexError::lifecycle("Event channel closed during replay")
                            })?;
                            events_emitted += 1;
                        }
                    }
                    Ok(None) => {}
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
                    let uuid = c
                        .parse::<Uuid>()
                        .map_err(|e| SinexError::processing(format!("Invalid cursor UUID: {e}")))?;
                    cursor = Some(sinex_primitives::Cursor {
                        after: Some(Id::from_uuid(uuid)),
                        before: None,
                    });
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
            let _ = tx.send(true);
        }
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

                let health_enabled = std::env::var("SINEX_HEALTH_MONITORING_ENABLED")
                    .map_or(true, |v| v != "false" && v != "0");

                if health_enabled {
                    let config = SelfObserverConfig {
                        component: self.node.name().to_string(),
                        subject_prefix: "sinex.telemetry".to_string(),
                        enabled: true,
                        min_emission_interval: Duration::from_secs(1),
                    };

                    let observer = Arc::new(SelfObserver::new(nats_client, config));
                    let thresholds = HealthThresholds::from_env().unwrap_or_default();

                    self.health_reporter = Some(Arc::new(HealthReporter::new(
                        self.node.name().to_string(),
                        observer,
                        thresholds,
                    )));

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
        Ok(true)
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
        Ok(crate::exploration::SourceState {
            is_connected: true,
            healthy: true,
            description: format!(
                "{} derived node ({})",
                self.node.name(),
                self.node.node_model()
            ),
            last_updated: Timestamp::now(),
            lag_seconds: None,
            recent_activity: Vec::new(),
            total_items: None,
            metadata: HashMap::new(),
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
        Ok(crate::exploration::CoverageAnalysis {
            time_range: (Timestamp::now(), Timestamp::now()),
            source_total: 0,
            sinex_total: 0,
            coverage_percentage: 100.0,
            missing_count: 0,
            duplicate_count: 0,
            missing_samples: Vec::new(),
            recommendations: Vec::new(),
        })
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

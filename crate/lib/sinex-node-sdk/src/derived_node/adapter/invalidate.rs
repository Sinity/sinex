//! Scope-invalidation processing for `DerivedNodeAdapter`.
//!
//! Carved out of `adapter/mod.rs` as part of #697. Pure mechanical move; the
//! methods, control flow, and instrumentation are unchanged.

use super::{DerivedNodeAdapter, INVALIDATION_QUERY_PAGE_SIZE};
#[cfg(feature = "messaging")]
use super::log_self_observation_failure;
use super::stale_output_ids_or_fail_scope;

use crate::derived_node::context::DerivedTriggerContext;
use crate::derived_node::invalidation::DerivedScopeInvalidation;
use crate::derived_node::traits::DerivedNodeImpl;
use crate::{NodeResult, SinexError};

use sinex_primitives::events::Event;
use sinex_primitives::events::builder::OperationMarker;
use sinex_primitives::{Id, JsonValue, Uuid};

use std::time::Instant;
use tracing::{debug, error, info, warn};

#[cfg(feature = "db")]
pub(super) struct PreparedInvalidation {
    pub(super) outputs: Vec<Event<JsonValue>>,
    pub(super) scopes: Vec<PreparedInvalidationScope>,
    pub(super) operation_uuid: Uuid,
}

#[cfg(feature = "db")]
pub(super) struct PreparedInvalidationScope {
    pub(super) scope_key: String,
    pub(super) stale_ids: Vec<Uuid>,
    pub(super) new_event_ids: Vec<(Uuid, Option<String>)>,
}

impl<N> DerivedNodeAdapter<N>
where
    N: DerivedNodeImpl,
{
    #[cfg(feature = "db")]
    pub(super) async fn prepare_invalidation(
        &mut self,
        invalidation: &DerivedScopeInvalidation,
    ) -> NodeResult<PreparedInvalidation> {
        use sinex_db::repositories::DbPoolExt;
        use sinex_primitives::prelude::*;

        // Only process invalidations for our declared input type/provenance contract.
        if !invalidation.matches_input(
            self.node.input_event_type(),
            self.node.input_provenance_filter(),
        ) {
            return Ok(PreparedInvalidation {
                outputs: Vec::new(),
                scopes: Vec::new(),
                operation_uuid: invalidation
                    .operation_id
                    .unwrap_or_else(|| *Id::<OperationMarker>::new().as_uuid()),
            });
        }

        let pool = {
            let runtime = self.runtime.as_ref().ok_or_else(|| {
                SinexError::lifecycle("Cannot process invalidation: runtime not initialized")
            })?;
            runtime.db_pool().clone()
        };

        let operation_id = invalidation.operation_id.map(Id::<OperationMarker>::from_uuid);
        let operation_uuid = invalidation
            .operation_id
            .unwrap_or_else(|| *Id::<OperationMarker>::new().as_uuid());
        let trigger_event_id = invalidation
            .affected_event_ids
            .first()
            .copied()
            .map(Id::<Event<JsonValue>>::from_uuid)
            .ok_or_else(|| {
                SinexError::validation("scope invalidation is missing affected event ids")
                    .with_context("node", self.node.name())
                    .with_context("action", invalidation.action.to_string())
                    .with_context("event_type", invalidation.event_type.as_ref())
                    .with_context("source", invalidation.event_source.as_ref())
            })?;

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
                    Ok(None) => {
                        debug!(
                            node = %self.node.name(),
                            event_id = %id,
                            "Event not found in DB while deriving invalidation scope keys; \
                             skipping (may be archived or not yet persisted)"
                        );
                    }
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

            let stale_ids = stale_output_ids_or_fail_scope(
                self.node.name(),
                scope_key,
                self.load_query_events_paginated(&pool, stale_query, scope_key, "stale outputs")
                    .await,
            )?;

            // ── Step 2: Load working set (input events for this scope) ──
            let query = EventQuery {
                event_types: self.input_query_event_types()?,
                has_lineage: self.input_query_has_lineage(),
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
                .filter(|event| self.event_matches_input(event))
                .collect::<Vec<_>>();

            // Build context for invalidation processing
            let context = DerivedTriggerContext {
                trigger_event_id,
                source: invalidation.event_source.clone(),
                event_type: invalidation.event_type.clone(),
                ts_orig: None,
                ts_coided: trigger_event_id.timestamp(),
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
            self.validate_output_batch(&outputs, "scope invalidation")?;
            self.observe_output_batch(&outputs, "invalidation").await;

            // Build output events
            let mut new_event_ids = Vec::new();
            for (output_index, output) in outputs.into_iter().enumerate() {
                let equivalence_key = output.equivalence_key.clone();
                let output_event = self.build_output_event(output, output_index, None, &context)?;
                let new_id = match output_event.id {
                    Some(id) => *id.as_uuid(),
                    None => {
                        return Err(SinexError::processing(
                            "derived output builder returned event without id",
                        )
                        .with_context("node", self.node.name())
                        .with_context("output_event_type", self.node.output_event_type()));
                    }
                };
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
    pub(super) async fn apply_prepared_invalidation(
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
                        scope
                            .new_event_ids
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
        invalidation: &DerivedScopeInvalidation,
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
    /// Returns `Ok(Some(count))` on success, `Ok(None)` for a non-fatal skip
    /// (deserialize error, transient processing error), or `Err` for a fatal
    /// failure that should halt the node's invalidation consumer. Fatal
    /// errors include `SinexError::Checkpoint` after the local circuit
    /// breaker threshold trips — see #581 for why DLQ-style "log and
    /// continue" on checkpoint errors is unsafe.
    pub(super) async fn handle_invalidation_message(
        &mut self,
        payload: &[u8],
    ) -> NodeResult<Option<u64>> {
        let node_name = self.node.name();
        let processing_start = Instant::now();

        // Emit "received" counter
        #[cfg(feature = "messaging")]
        if let Some(ref obs) = self.self_observer
            && let Err(error) = obs.emit_counter("invalidation.received", 1, None).await
        {
            log_self_observation_failure(node_name, "invalidation.received", &error);
        }

        let invalidation = match serde_json::from_slice::<DerivedScopeInvalidation>(payload) {
            Ok(inv) => inv,
            Err(e) => {
                warn!(
                    node = %node_name,
                    error = %e,
                    payload_len = payload.len(),
                    "Failed to deserialize invalidation signal"
                );
                #[cfg(feature = "messaging")]
                if let Some(ref obs) = self.self_observer
                    && let Err(error) = obs.emit_counter("invalidation.errors", 1, None).await
                {
                    log_self_observation_failure(node_name, "invalidation.errors", &error);
                }
                return Ok(None);
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
                            if let Some(ref obs) = self.self_observer
                                && let Err(obs_error) =
                                    obs.emit_counter("invalidation.errors", 1, None).await
                            {
                                log_self_observation_failure(
                                    node_name,
                                    "invalidation.errors",
                                    &obs_error,
                                );
                            }
                            return Ok(None);
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
                        if let Some(ref obs) = self.self_observer
                            && let Err(obs_error) =
                                obs.emit_counter("invalidation.errors", 1, None).await
                        {
                            log_self_observation_failure(
                                node_name,
                                "invalidation.errors",
                                &obs_error,
                            );
                        }
                        return Ok(None);
                    }
                    self.record_state_mutation();
                    let duration_ms = processing_start.elapsed().as_millis() as f64;

                    if self.should_checkpoint() {
                        match self.save_state().await {
                            Ok(()) => {
                                self.consecutive_checkpoint_failures = 0;
                            }
                            Err(e) => {
                                self.consecutive_checkpoint_failures += 1;
                                error!(
                                    node = %node_name,
                                    error = %e,
                                    consecutive_failures =
                                        self.consecutive_checkpoint_failures,
                                    "Failed to checkpoint after invalidation"
                                );
                                #[cfg(feature = "messaging")]
                                if let Some(ref obs) = self.self_observer
                                    && let Err(obs_error) =
                                        obs.emit_counter("invalidation.errors", 1, None).await
                                {
                                    log_self_observation_failure(
                                        node_name,
                                        "invalidation.errors",
                                        &obs_error,
                                    );
                                }
                                // Two halt conditions:
                                // 1. Three consecutive failures — same circuit-breaker
                                //    threshold the periodic checkpoint branch uses
                                //    below. Without this, an invalidation-driven CAS
                                //    conflict loops forever; the periodic branch
                                //    would never run because the consumer never
                                //    returns to its `select!`.
                                // 2. Direct Checkpoint/Lifecycle/Configuration/PermissionDenied
                                //    variant — even on the first failure if the error
                                //    itself signals "halt" (e.g. propagated from the
                                //    inner save path which already exhausted retries).
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
                                        "Checkpoint save failed during invalidation \
                                         processing ({} consecutive); halting node \
                                         to prevent silent progress loss: {e}",
                                        self.consecutive_checkpoint_failures
                                    )));
                                }
                                return Ok(None);
                            }
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

                    Ok(Some(count))
                }
                Err(e) => {
                    error!(
                        node = %node_name,
                        error = %e,
                        action = %invalidation.action,
                        "Invalidation processing failed"
                    );
                    #[cfg(feature = "messaging")]
                    if let Some(ref obs) = self.self_observer
                        && let Err(error) = obs.emit_counter("invalidation.errors", 1, None).await
                    {
                        log_self_observation_failure(node_name, "invalidation.errors", &error);
                    }
                    // Same #581 guard at the prepare step: a Checkpoint
                    // variant from inside `prepare_invalidation` (e.g. a
                    // sub-call exhausted its own retries) means the next
                    // invalidation will hit the same wall.
                    if matches!(e, SinexError::Checkpoint(_)) {
                        return Err(e);
                    }
                    Ok(None)
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
            Ok(None)
        }
    }
}


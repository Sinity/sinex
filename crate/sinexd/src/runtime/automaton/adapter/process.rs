//! Per-event and per-batch processing for `AutomatonRuntime`.
//!
//! Carved out of `adapter/mod.rs` as part of #697. Pure mechanical move; the
//! methods, control flow, and instrumentation are unchanged.

use super::{AutomatonRuntime, INVALIDATION_QUERY_PAGE_SIZE, event_lag_ms};

use sinex_primitives::temporal::Timestamp;

use crate::runtime::automaton::context::AutomatonContext;
use crate::runtime::automaton::traits::Automaton;
use crate::runtime::durable_emission::{
    DurableEmissionRequest, EmissionOrigin, ProgressAtom, ReceiptLevel,
};
use crate::runtime::durable_emission_backend::emit_batch_durable;
use crate::runtime::stream::RuntimeContext;
use crate::runtime::{RuntimeResult, SinexError};

use sinex_primitives::Id;
use sinex_primitives::JsonValue;
use sinex_primitives::commit_frontier::{CommitFrontier, TerminalOutcome};
use sinex_primitives::events::Event;
#[cfg(feature = "db")]
use sinex_primitives::query::{EventQuery, EventQueryResult, QueryResultEvent};
use sinex_primitives::settlement::{
    DefaultFailurePolicy, FailureContext, FailurePolicy, RuntimeOperation, RuntimePhase, Settlement,
};

#[cfg(feature = "db")]
use tracing::info;
use tracing::{error, warn};

/// One event's automaton-logic outcome, computed by [`AutomatonRuntime::prepare_one`]
/// but not yet committed to persisted state (sinex-vxu: `process_one`/
/// `process_batch` split into a prepare phase and a
/// [`AutomatonRuntime::commit_prepared_inputs`] phase so a checkpoint can
/// never durably advance past an input whose output is not itself durably
/// confirmed).
struct PreparedInput {
    source_event_id: Id<Event<JsonValue>>,
    input_ts_orig: Option<Timestamp>,
    settlement: PreparedSettlement,
    /// Serialized state immediately after this input's automaton logic ran.
    /// If a later receipt does not unlock progress, the commit phase restores
    /// the state corresponding to the last contiguous settled prefix instead
    /// of allowing a timer-driven checkpoint to persist uncommitted deltas.
    state_after: JsonValue,
}

/// What still needs to happen (if anything) before a [`PreparedInput`] may
/// be marked processed.
enum PreparedSettlement {
    /// The automaton produced no output (`Ok(vec![])`), or the failure
    /// policy settled the automaton's error as a benign commit — nothing to
    /// durably confirm; safe to mark processed immediately.
    NoOutput,
    /// Non-empty output events exist and must be durably emitted (and
    /// settle to a progress-unlocking [`crate::runtime::durable_emission::EmissionReceiptState`])
    /// before this input may be marked processed.
    PendingEmission(Vec<Event<JsonValue>>),
    /// Routed to the processing-failure queue over a transport that itself
    /// durably persisted the routing (`EventTransport::Nats`) — already
    /// safe to mark processed.
    DurableFailureRouted,
    /// Routed to the processing-failure queue, but the transport could not
    /// prove the routing itself was durable (`EventTransport::Direct`,
    /// warning-only — sinex-vxu AC explicitly excludes this from counting
    /// as durable evidence). Must NOT be marked processed.
    UnprovenFailureRouted,
}

impl<N> AutomatonRuntime<N>
where
    N: Automaton,
{
    #[cfg(feature = "db")]
    pub(super) async fn load_query_events_paginated(
        &self,
        pool: &sinex_db::DbPool,
        mut query: EventQuery,
        scope_key: &str,
        query_kind: &'static str,
    ) -> RuntimeResult<Vec<QueryResultEvent>> {
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
                    .with_context("automaton", self.automaton.name()));
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
                automaton = %self.automaton.name(),
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

    pub(super) async fn send_to_processing_failure_queue_or_fail(
        &self,
        event: &Event<JsonValue>,
        error: &crate::runtime::AutomatonLogicError,
    ) -> RuntimeResult<()> {
        let Some(runtime) = self.runtime.as_ref() else {
            return Err(SinexError::lifecycle(
                "automaton requested processing-failure routing but no transport runtime is available",
            )
            .with_context("automaton", self.automaton.name())
            .with_context("event_type", event.event_type.as_ref())
            .with_context("source", event.source.as_ref())
            .with_context("reason", error.to_string()));
        };
        let transport = runtime.handles().transport();
        transport
            .send_to_processing_failure_queue(event, &error.to_string(), self.automaton.name())
            .await
            .map_err(|failure_err| {
                SinexError::processing(
                    "failed to send automaton event to processing-failure stream",
                )
                .with_context("automaton", self.automaton.name())
                .with_context("event_type", event.event_type.as_ref())
                .with_context("source", event.source.as_ref())
                .with_context("reason", error.to_string())
                .with_std_error(&failure_err)
            })
    }

    /// Whether a successful `send_to_processing_failure_queue_or_fail` call
    /// just now actually durably recorded the routing — see
    /// `EventTransport::processing_failure_routing_is_durable`. `false`
    /// (never durable) when no runtime/transport is available at all.
    pub(super) fn processing_failure_routing_is_durable(&self) -> bool {
        self.runtime.as_ref().is_some_and(|runtime| {
            runtime
                .handles()
                .transport()
                .processing_failure_routing_is_durable()
        })
    }

    pub(super) async fn emit_output_events(
        &self,
        outputs: Vec<Event<JsonValue>>,
        context: &'static str,
    ) -> RuntimeResult<u64> {
        let count = outputs.len() as u64;
        if count == 0 {
            return Ok(0);
        }

        let emitter = self.event_emitter.as_ref().ok_or_else(|| {
            SinexError::lifecycle("automaton output channel is not initialized")
                .with_context("automaton", self.automaton.name())
                .with_context("context", context)
        })?;

        for event in outputs {
            let event_id = event
                .id
                .map_or_else(|| "<none>".to_string(), |id| id.to_string());
            let event_source = event.source.as_ref().to_string();
            let event_type = event.event_type.as_ref().to_string();

            emitter.emit(event).await.map_err(|error| {
                SinexError::lifecycle("failed to emit automaton output event")
                    .with_context("automaton", self.automaton.name())
                    .with_context("context", context)
                    .with_context("event_id", event_id)
                    .with_context("source", event_source)
                    .with_context("event_type", event_type)
                    .with_source(error)
            })?;
        }

        Ok(count)
    }

    /// Process a single event through the same prepare/commit barrier as the
    /// live batch path. The helper still returns constructed outputs to its
    /// isolation tests when no durable emitter is wired, but it never advances
    /// input progress past an unsettled output.
    pub async fn process_one(
        &mut self,
        event: Event<JsonValue>,
    ) -> RuntimeResult<Vec<Event<JsonValue>>> {
        let state_before = self.snapshot_state()?;
        let mut prepared = match self.prepare_one(event).await {
            Ok(prepared) => prepared,
            Err(error) => {
                self.restore_state(&state_before)?;
                return Err(error);
            }
        };
        let outputs = match &prepared.settlement {
            PreparedSettlement::PendingEmission(outputs) => outputs.clone(),
            PreparedSettlement::NoOutput
            | PreparedSettlement::DurableFailureRouted
            | PreparedSettlement::UnprovenFailureRouted => Vec::new(),
        };
        prepared.state_after = match self.snapshot_state() {
            Ok(snapshot) => snapshot,
            Err(error) => {
                self.restore_state(&state_before)?;
                return Err(error);
            }
        };

        // Keep the historical helper's return contract (it returns the
        // constructed outputs to its caller), while making its progress side
        // effect obey the same durable receipt barrier as process_batch.
        let committed = self
            .commit_prepared_inputs(vec![prepared], "single input", state_before)
            .await?;
        if committed.is_empty() && !outputs.is_empty() {
            Ok(outputs)
        } else {
            Ok(committed)
        }
    }

    pub(super) fn snapshot_state(&self) -> RuntimeResult<JsonValue> {
        serde_json::to_value(&self.persisted_state.state).map_err(|error| {
            SinexError::serialization("failed to snapshot automaton state before durable commit")
                .with_context("automaton", self.automaton.name())
                .with_source(error)
        })
    }

    pub(super) fn restore_state(&mut self, snapshot: &JsonValue) -> RuntimeResult<()> {
        self.persisted_state.state = serde_json::from_value(snapshot.clone()).map_err(|error| {
            SinexError::serialization("failed to restore automaton state to durable frontier")
                .with_context("automaton", self.automaton.name())
                .with_source(error)
        })?;
        Ok(())
    }

    /// Run the automaton's logic for a single event and report the outcome
    /// WITHOUT mutating persisted state — see `PreparedInput`/
    /// `PreparedSettlement`. All side effects that are themselves already
    /// durable when they happen (processing-failure routing) still execute
    /// here; only `record_processed_input`/checkpoint advancement is
    /// deferred to the caller's commit phase.
    async fn prepare_one(&mut self, event: Event<JsonValue>) -> RuntimeResult<PreparedInput> {
        let context = AutomatonContext::live(&event)?;
        let source_event_id = context.trigger_event_id;
        let input_ts_orig = event.ts_orig;

        // Lag = wall time between the upstream event's `ts_orig` and the
        // moment we start processing it. Negative values (clock skew /
        // synthesized future timestamps) are clamped to zero so the
        // gauge stays interpretable.
        let lag_ms = event_lag_ms(&event);
        let process_started_at = std::time::Instant::now();

        let result = self
            .automaton
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
                    let sinex_error = e.to_sinex_error();
                    reporter.record_error(&sinex_error);

                    // Emit automaton error telemetry before routing
                    if let Some(ref observer) = self.self_observer {
                        let mut labels = self.derived_metric_labels();
                        labels.insert("error".to_string(), e.to_string());
                        labels.insert(
                            "error_class".to_string(),
                            format!("{:?}", sinex_error.error_class()),
                        );
                        if let Err(obs_err) = observer
                            .emit_counter("automaton.error", 1, Some(labels))
                            .await
                        {
                            warn!(
                                automaton = %self.automaton.name(),
                                error = %obs_err,
                                "Failed to emit automaton error counter"
                            );
                        }
                    }
                }
            }

            if let Err(e) = reporter.check_and_emit().await {
                warn!(automaton = %self.automaton.name(), error = %e, "Failed to emit health status");
            }
        }

        match result {
            Ok(outputs) => {
                self.validate_output_batch(&outputs, "live processing")?;
                self.observe_output_batch(&outputs, "live").await;
                let output_events =
                    self.build_output_events(outputs, Some(source_event_id), &context)?;
                let settlement = if output_events.is_empty() {
                    PreparedSettlement::NoOutput
                } else {
                    PreparedSettlement::PendingEmission(output_events)
                };
                Ok(PreparedInput {
                    source_event_id,
                    input_ts_orig,
                    settlement,
                    state_after: JsonValue::Null,
                })
            }
            Err(e) => {
                // Use FailurePolicy::settle() which maps ErrorClass
                // to Settlement variants with backoff and retry budgets.
                let sinex_error = e.to_sinex_error();
                let failure_ctx = FailureContext {
                    unit_id: self.automaton.name().to_string(),
                    operation: RuntimeOperation::ProcessBatch,
                    phase: RuntimePhase::ProcessInput,
                    input_scope: None,
                    effect_kind: None,
                    delivery_count: None,
                    attempts: 0,
                };
                let settlement = DefaultFailurePolicy.settle(&sinex_error, &failure_ctx);

                match settlement {
                    Settlement::Commit => {
                        warn!(automaton = %self.automaton.name(), error = %e, "Committing (settled as benign)");
                        Ok(PreparedInput {
                            source_event_id,
                            input_ts_orig,
                            settlement: PreparedSettlement::NoOutput,
                            state_after: JsonValue::Null,
                        })
                    }
                    Settlement::SendToProcessingFailure
                    | Settlement::Park { .. }
                    | Settlement::Quarantine { .. } => {
                        warn!(automaton = %self.automaton.name(), error = %e, "Routing to processing-failure queue");
                        self.send_to_processing_failure_queue_or_fail(&event, &e)
                            .await?;
                        let settlement = if self.processing_failure_routing_is_durable() {
                            PreparedSettlement::DurableFailureRouted
                        } else {
                            PreparedSettlement::UnprovenFailureRouted
                        };
                        Ok(PreparedInput {
                            source_event_id,
                            input_ts_orig,
                            settlement,
                            state_after: JsonValue::Null,
                        })
                    }
                    Settlement::Retry { .. } => {
                        error!(
                            target: "sinex_metrics",
                            metric = "derive.batch_retry_halts_total",
                            automaton = %self.automaton.name(),
                            error = %e,
                            "Retryable error; halting batch"
                        );
                        Err(e.into())
                    }
                    Settlement::HaltModule { reason } => {
                        // Halt requests clean drain so systemd records the
                        // automaton as cleanly exited rather than restarting it
                        // into a hot loop. The error still propagates so the
                        // caller can record the failure.
                        if let Some(drain) = self.shutdown_tx.as_ref() {
                            let _ = drain.request_drain_and_warn(self.automaton.name());
                        }
                        error!(
                            target: "sinex_metrics",
                            metric = "derive.runtime_halts_total",
                            automaton = %self.automaton.name(),
                            error = %e,
                            reason = ?reason,
                            "Halting module; runtime drain requested"
                        );
                        Err(SinexError::processing(format!(
                            "RuntimeModule halted: {reason:?} — {e}"
                        )))
                    }
                    Settlement::DrainRuntimeUnit { reason } => {
                        // Same shape as HaltModule — request drain, then return
                        // the error so the in-flight batch unwinds.
                        if let Some(drain) = self.shutdown_tx.as_ref() {
                            let _ = drain.request_drain_and_warn(self.automaton.name());
                        }
                        error!(
                            target: "sinex_metrics",
                            metric = "derive.runtime_drains_total",
                            automaton = %self.automaton.name(),
                            error = %e,
                            reason = %reason,
                            "Draining runtime unit"
                        );
                        Err(SinexError::processing(format!(
                            "Runtime unit drained: {reason} — {e}"
                        )))
                    }
                }
            }
        }
    }

    /// Clock-driven trailing-bucket flush for `Windowed` automata.
    ///
    /// Calls the automaton's `timer_flush_derived` with the current wall time.
    /// If the automaton's `flush_due` predicate returns true the open accumulator
    /// is emitted and the output events are published exactly like the live
    /// per-event path does. Returns the count of output events emitted.
    ///
    /// No-op for `Transducer` and `ScopeReconciler` automata (their
    /// `timer_flush_derived` default returns empty).
    pub async fn timer_flush(&mut self, now: Timestamp) -> RuntimeResult<u64> {
        // sinex-5s6: the flush decision consults the two-mode input-time
        // watermark, not raw wall clock. In Catchup (historical replay/backfill,
        // old-timestamped events flowing) the watermark is the max input
        // `ts_orig`, so trailing-bucket flush never chops continuity by
        // processing delay; in Live (caught up — quiet or current) it tracks
        // wall time so a genuine quiet period still closes the window/session.
        let (watermark, _mode) = super::watermark::flush_watermark(
            self.persisted_state.max_input_ts_orig,
            self.last_input_wall,
            now,
        );

        // Build a synthetic context for the timer flush. There is no upstream
        // trigger event, so we use a fresh synthetic ID and the watermark as the
        // flush moment (input-time in Catchup, wall-time in Live).
        let context = AutomatonContext::timer_flush(watermark)?;

        // `timer_flush_derived` is allowed to clear/reset the window while it
        // constructs its output. Keep a serialization snapshot so a failed or
        // merely emitted-but-unsettled output cannot make that state disappear
        // across the next checkpoint/restart. This mirrors the durable barrier
        // in `commit_prepared_inputs`; timer-driven output has no input event
        // whose checkpoint can otherwise hold the state behind it.
        let state_snapshot =
            serde_json::to_value(&self.persisted_state.state).map_err(|error| {
                SinexError::serialization("failed to snapshot window state before timer flush")
                    .with_context("automaton", self.automaton.name())
                    .with_source(error)
            })?;
        let automaton_name = self.automaton.name();
        let restore_state = |state: &mut N::State| -> RuntimeResult<()> {
            *state = serde_json::from_value(state_snapshot.clone()).map_err(|error| {
                SinexError::serialization("failed to restore window state after timer flush")
                    .with_context("automaton", automaton_name)
                    .with_source(error)
            })?;
            Ok(())
        };

        let outputs = match self
            .automaton
            .timer_flush_derived(&mut self.persisted_state.state, watermark, &context)
            .await
        {
            Ok(outputs) => outputs,
            Err(error) => {
                restore_state(&mut self.persisted_state.state)?;
                return Err(SinexError::processing("windowed timer flush failed")
                    .with_context("automaton", automaton_name)
                    .with_source(error));
            }
        };

        if outputs.is_empty() {
            return Ok(0);
        }

        if let Err(error) = self.validate_output_batch(&outputs, "timer flush") {
            restore_state(&mut self.persisted_state.state)?;
            return Err(error);
        }
        self.observe_output_batch(&outputs, "timer_flush").await;
        let output_events = match self.build_output_events(outputs, None, &context) {
            Ok(events) => events,
            Err(error) => {
                restore_state(&mut self.persisted_state.state)?;
                return Err(error);
            }
        };
        let count = output_events.len() as u64;

        if count > 0 {
            let Some(emitter) = self.event_emitter.as_ref() else {
                restore_state(&mut self.persisted_state.state)?;
                return Err(SinexError::lifecycle(
                    "automaton output channel is not initialized for timer flush",
                )
                .with_context("automaton", automaton_name));
            };
            let registry = self
                .runtime
                .as_ref()
                .map(RuntimeContext::settlement_registry)
                .unwrap_or_default();
            let request = DurableEmissionRequest {
                origin: EmissionOrigin::WindowedTimer,
                required_level: ReceiptLevel::AdmissionSettled,
                progress_atom: ProgressAtom::WindowFlush {
                    automaton: self.automaton.name().to_string(),
                    flush_id: context.trigger_uuid(),
                },
                events: output_events,
                allow_spool_backend: false,
            };
            let receipt =
                emit_batch_durable(&registry, emitter, request, self.durable_emission_timeout)
                    .await;
            if !receipt.unlocks_progress() {
                restore_state(&mut self.persisted_state.state)?;
                return Err(SinexError::processing(
                    "timer-flush output was emitted but did not reach a durable settlement",
                )
                .with_context("automaton", automaton_name)
                .with_context("flush_id", context.trigger_event_id.to_string()));
            }

            // A timer flush has no input cursor whose commit implicitly dirties
            // checkpoint state. Persist the post-flush state only after the
            // output receipt has unlocked progress; otherwise a shutdown or
            // time-based save could outrun the durable output.
            self.record_state_mutation();
            if let Err(error) = self
                .save_state_with_file_fallback("durable timer flush checkpoint")
                .await
            {
                restore_state(&mut self.persisted_state.state)?;
                return Err(error);
            }
        }

        Ok(count)
    }

    /// Durably emit every `PendingEmission` output collected from a batch of
    /// `prepare_one` calls, then mark inputs processed ONLY for the
    /// contiguous prefix whose settlement (immediate no-output/durable-debt,
    /// or a durable-emission receipt) unlocked progress (sinex-vxu). This is
    /// the actual fix: `record_processed_input` (and therefore the
    /// checkpoint) can never advance past an input whose output has not
    /// itself been durably confirmed — a hole (unresolved/timed-out
    /// receipt, or an unproven processing-failure routing) blocks every
    /// input submitted after it in this same call from being marked
    /// processed, exactly like the sinex-r6d.11 reference caller
    /// (`adapter_source.rs::drain_adapter`).
    async fn commit_prepared_inputs(
        &mut self,
        prepared_inputs: Vec<PreparedInput>,
        context: &'static str,
        state_before: JsonValue,
    ) -> RuntimeResult<Vec<Event<JsonValue>>> {
        if prepared_inputs.is_empty() {
            return Ok(Vec::new());
        }

        struct PlanEntry {
            source_event_id: Id<Event<JsonValue>>,
            input_ts_orig: Option<Timestamp>,
            outputs: Vec<Event<JsonValue>>,
            state_after: JsonValue,
            /// Only meaningful when `outputs` is empty — whether this entry
            /// is already durably settled (`NoOutput`/`DurableFailureRouted`)
            /// or a permanent hole for this call (`UnprovenFailureRouted`).
            immediately_unlocked: bool,
        }

        let mut plan: Vec<PlanEntry> = Vec::with_capacity(prepared_inputs.len());
        let mut batch_events: Vec<Event<JsonValue>> = Vec::new();

        for prepared in prepared_inputs {
            let (outputs, immediately_unlocked) = match prepared.settlement {
                PreparedSettlement::NoOutput | PreparedSettlement::DurableFailureRouted => {
                    (Vec::new(), true)
                }
                PreparedSettlement::UnprovenFailureRouted => (Vec::new(), false),
                PreparedSettlement::PendingEmission(outputs) => {
                    batch_events.extend(outputs.iter().cloned());
                    (outputs, false)
                }
            };
            plan.push(PlanEntry {
                source_event_id: prepared.source_event_id,
                input_ts_orig: prepared.input_ts_orig,
                outputs,
                immediately_unlocked,
                state_after: prepared.state_after,
            });
        }

        let receipt = if batch_events.is_empty() {
            None
        } else if let Some(emitter) = self.event_emitter.as_ref() {
            let registry = self
                .runtime
                .as_ref()
                .map(RuntimeContext::settlement_registry)
                .unwrap_or_default();
            let input_event_ids = plan
                .iter()
                .filter(|entry| !entry.outputs.is_empty())
                .map(|entry| *entry.source_event_id.as_uuid())
                .collect();
            let request = DurableEmissionRequest {
                origin: EmissionOrigin::AutomatonBridge,
                required_level: ReceiptLevel::AdmissionSettled,
                progress_atom: ProgressAtom::AutomatonInputBatch {
                    automaton: self.automaton.name().to_string(),
                    input_event_ids,
                },
                events: batch_events,
                allow_spool_backend: false,
            };
            Some(
                emit_batch_durable(&registry, emitter, request, self.durable_emission_timeout)
                    .await,
            )
        } else {
            warn!(
                automaton = %self.automaton.name(),
                context,
                "automaton output channel is not initialized — cannot durably emit outputs \
                 this batch; none of the inputs that produced output events will be marked \
                 processed"
            );
            None
        };

        let mut frontier: CommitFrontier<()> = CommitFrontier::new();
        let mut receipt_offset = 0usize;

        for entry in &plan {
            let ticket = frontier.submit();
            let unlocked = if entry.outputs.is_empty() {
                entry.immediately_unlocked
            } else if let Some(receipt) = &receipt {
                let event_count = entry.outputs.len();
                let span_end = (receipt_offset + event_count).min(receipt.items.len());
                let span = &receipt.items[receipt_offset.min(receipt.items.len())..span_end];
                receipt_offset += event_count;
                !span.is_empty() && span.iter().all(|item| item.state.is_progress_unlocking())
            } else {
                false
            };

            if unlocked {
                frontier.complete(ticket, TerminalOutcome::PersistedConfirmed, ());
            } else {
                warn!(
                    automaton = %self.automaton.name(),
                    event_id = %entry.source_event_id,
                    context,
                    "input's durable-emission receipt did not fully unlock progress — not \
                     marking processed so the checkpoint does not outrun this output \
                     (sinex-vxu)"
                );
            }
        }

        let committed_prefix = frontier.frontier().0 as usize;
        let committed_state = if committed_prefix == 0 {
            state_before
        } else {
            plan[committed_prefix - 1].state_after.clone()
        };
        self.restore_state(&committed_state)?;
        let mut committed_outputs = Vec::new();
        for entry in plan.into_iter().take(committed_prefix) {
            self.record_processed_input(entry.source_event_id, entry.input_ts_orig);
            self.observe_runtime_snapshot().await;
            committed_outputs.extend(entry.outputs);
        }

        Ok(committed_outputs)
    }

    /// Process a batch of events.
    ///
    /// Events that fail with `Settlement::Retry` halt the batch — the checkpoint
    /// is NOT advanced past them and the first retry error is returned.
    pub async fn process_batch(
        &mut self,
        events: Vec<Event<JsonValue>>,
    ) -> RuntimeResult<Vec<Event<JsonValue>>> {
        let state_before = self.snapshot_state()?;
        let mut prepared_inputs = Vec::with_capacity(events.len());
        let mut retry_error: Option<SinexError> = None;

        for event in events {
            let state_before_event = self.snapshot_state()?;
            match self.prepare_one(event).await {
                Ok(mut prepared) => {
                    prepared.state_after = match self.snapshot_state() {
                        Ok(snapshot) => snapshot,
                        Err(error) => {
                            self.restore_state(&state_before_event)?;
                            return Err(error);
                        }
                    };
                    prepared_inputs.push(prepared);
                }
                Err(e) => {
                    self.restore_state(&state_before_event)?;
                    error!(
                        target: "sinex_metrics",
                        metric = "derive.batch_retry_halts_total",
                        automaton = %self.automaton.name(),
                        error = %e,
                        "Retryable error processing event in batch; halting batch"
                    );
                    retry_error = Some(e);
                    break;
                }
            }
        }

        let outputs = self
            .commit_prepared_inputs(prepared_inputs, "event bridge batch", state_before)
            .await?;

        if self.should_checkpoint() {
            match self
                .save_state_with_file_fallback("event bridge batch checkpoint")
                .await
            {
                Ok(()) => {
                    self.consecutive_checkpoint_failures = 0;
                    // sinex-r6d.9 crash-window harness: by this point, every
                    // input reflected in the checkpoint just saved above has
                    // ALREADY had its output(s) durably confirmed by
                    // `commit_prepared_inputs` (sinex-vxu fix) — the
                    // checkpoint-before-output window this fail point
                    // targets is structurally unreachable now. The fail
                    // point stays wired at this same position so the harness
                    // continues to prove that fact directly: an armed flag
                    // only fires here once it is actually safe to.
                    #[cfg(any(test, feature = "testing"))]
                    if let Some(flag) = &self.fail_point_after_checkpoint
                        && flag.load(std::sync::atomic::Ordering::SeqCst)
                    {
                        std::process::exit(97);
                    }
                }
                Err(e) => {
                    self.consecutive_checkpoint_failures += 1;
                    error!(
                        target: "sinex_metrics",
                        metric = "derive.checkpoint_failures_total",
                        automaton = %self.automaton.name(),
                        error = %e,
                        consecutive_failures = self.consecutive_checkpoint_failures,
                        "Failed to save checkpoint after batch"
                    );
                    // Halt immediately for genuinely non-retryable error classes
                    // (runtime lifecycle/config/permission problems that will not
                    // clear themselves before the next batch), but NOT for
                    // `SinexError::Checkpoint` itself: `save_state_with_file_fallback`
                    // wraps every failure where BOTH the NATS KV save and the file
                    // fallback fail into this same generic checkpoint-kind error —
                    // including ordinary transient conditions, not only permanently
                    // unrecoverable ones. Matching on it here would make the
                    // consecutive-failure counter below unreachable dead code (every
                    // failure from this call site is Checkpoint-classed by
                    // construction), silently collapsing the intended 3-strike grace
                    // period into an immediate halt on the very first failure.
                    if self.consecutive_checkpoint_failures >= 3
                        || matches!(
                            e,
                            SinexError::Lifecycle(_)
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
}

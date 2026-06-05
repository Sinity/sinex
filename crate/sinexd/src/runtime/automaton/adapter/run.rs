//! `run_continuous` / `run_historical` for `AutomatonRuntime`.
//!
//! Carved out of `adapter/mod.rs` as part of #697. Pure mechanical move; the
//! methods, control flow, and instrumentation are unchanged.

use super::{AutomatonRuntime, historical_resume_position, recv_invalidation};

use crate::runtime::automaton::context::AutomatonContext;
use crate::runtime::automaton::traits::Automaton;
use crate::runtime::stream::{Checkpoint, RuntimeContext, ScanArgs, ScanReport};
use crate::runtime::{RuntimeResult, SinexError};
use sinex_primitives::env as shared_env;
use sinex_primitives::settlement::{
    DefaultFailurePolicy, FailureContext, FailurePolicy, RuntimeOperation, RuntimePhase, Settlement,
};

use sinex_primitives::events::builder::OperationMarker;
use sinex_primitives::temporal::Timestamp;

use std::collections::HashMap;
use std::time::{Duration, Instant};
use tracing::{debug, error, info, warn};

impl<N> AutomatonRuntime<N>
where
    N: Automaton,
{
    /// DEPLOYMENT-INACTIVE: never entered. The runtime dispatches automata via
    /// `run_automaton_event_bridge` because `manages_own_continuous_loop` is
    /// `false`; this scan-driven path (and its `derived.invalidation`
    /// subscription) is unreachable in deployment. See #1569.
    pub(super) async fn run_continuous(&mut self, _from: Checkpoint) -> RuntimeResult<ScanReport> {
        let start = Instant::now();
        let module_name = self.automaton.name().to_string();
        let mut invalidations_processed: u64 = 0;

        info!(
            automaton = %module_name,
            model = %self.automaton.automaton_model(),
            input_type = %self.automaton.input_event_type(),
            output_type = %self.automaton.output_event_type(),
            "Automaton initialized — running invalidation-driven continuous loop"
        );

        // Subscribe to scope invalidation signals via JetStream push consumer.
        // JetStream provides durable message delivery for invalidation signals.
        // Each node type creates its own ephemeral push consumer, receiving all
        // signals published to the invalidation subject.
        //
        // Note: requires `messaging` feature (default). run_continuous is only called
        // by the runtime kernel which itself requires messaging infrastructure.
        // The two `#[cfg]` blocks produce different types but both work with
        // `recv_invalidation()` which has matching cfg'd signatures.
        #[cfg(feature = "messaging")]
        let mut invalidation_sub: Option<async_nats::jetstream::consumer::push::Messages> = {
            let nats_client = self.runtime.as_ref().and_then(RuntimeContext::nats_client);

            if let Some(client) = nats_client {
                let env = sinex_primitives::environment::environment();
                let stream_name = env.nats_stream_name("SINEX_RAW_EVENTS_DERIVED_INVALIDATIONS");
                let queue_group = format!("derived.invalidation.{}", self.automaton.name());
                let deliver_subject = client.new_inbox();
                let js = async_nats::jetstream::new(client.clone());

                match js.get_stream(&stream_name).await {
                    Ok(stream) => {
                        let config = async_nats::jetstream::consumer::push::Config {
                            deliver_subject: deliver_subject.clone(),
                            deliver_group: Some(queue_group.clone()),
                            ..Default::default()
                        };
                        match stream.create_consumer(config).await {
                            Ok(consumer) => match consumer.messages().await {
                                Ok(messages) => {
                                    info!(
                                        automaton = %module_name,
                                        stream = %stream_name,
                                        queue_group = %queue_group,
                                        "Subscribed to invalidation signals via JetStream push consumer"
                                    );
                                    Some(messages)
                                }
                                Err(e) => {
                                    warn!(
                                        automaton = %module_name,
                                        error = %e,
                                        "Failed to start invalidation consumer message stream"
                                    );
                                    None
                                }
                            },
                            Err(e) => {
                                warn!(
                                    automaton = %module_name,
                                    queue_group = %queue_group,
                                    error = %e,
                                    "Failed to create invalidation push consumer"
                                );
                                None
                            }
                        }
                    }
                    Err(e) => {
                        warn!(
                            automaton = %module_name,
                            stream = %stream_name,
                            error = %e,
                            "Failed to get invalidation stream"
                        );
                        None
                    }
                }
            } else {
                debug!(automaton = %module_name, "No NATS client — invalidation subscription skipped");
                None
            }
        };
        #[cfg(not(feature = "messaging"))]
        let mut invalidation_sub = ();

        let runtime = self.runtime.as_ref().ok_or_else(|| {
            SinexError::lifecycle(
                "Cannot run continuous invalidation loop: runtime not initialized",
            )
        })?;
        let drain = runtime.runtime_drain();
        let mut shutdown_rx = drain.subscribe();
        self.shutdown_tx = Some(drain);

        // Invalidation debounce: buffer signals and process after a quiet period.
        // This prevents a replay archiving N scopes from triggering N immediate
        // recomputations — instead they coalesce into a single batch.
        let debounce_ms = shared_env::parse_or(
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
                            automaton = %module_name,
                            "Automaton invalidation shutdown channel dropped before explicit shutdown"
                        );
                    }
                    if shutdown_result.is_err() || *shutdown_rx.borrow() {
                        info!(automaton = %module_name, "Shutdown signal received");
                        // Process any pending invalidations before shutdown.
                        // A halt-class error from `handle_invalidation_message`
                        // (`Err`) means the next invalidation will hit the
                        // same wall — propagate it so the node halts on
                        // genuine fatal classes (#581-shape).
                        for payload in pending_invalidations.drain(..) {
                            match self.handle_invalidation_message(&payload).await {
                                Ok(Some(_)) => invalidations_processed += 1,
                                Ok(None) => {}
                                Err(e) => return Err(e),
                            }
                        }
                        self.observe_pending_invalidations(0).await;
                        break;
                    }
                }

                // Invalidation signal: buffer and set debounce deadline.
                payload = recv_invalidation(&mut invalidation_sub) => {
                    if let Some(payload) = payload {
                        pending_invalidations.push(payload);
                        self.observe_pending_invalidations(pending_invalidations.len()).await;
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
                        automaton = %module_name,
                        batch_size,
                        debounce_ms,
                        "Processing debounced invalidation batch"
                    );
                    for payload in pending_invalidations.drain(..) {
                        match self.handle_invalidation_message(&payload).await {
                            Ok(Some(_)) => invalidations_processed += 1,
                            Ok(None) => {}
                            // Halt-class error (#581-shape). Stop the
                            // continuous loop instead of looping forever.
                            Err(e) => return Err(e),
                        }
                    }
                    self.observe_pending_invalidations(0).await;
                    debounce_deadline = None;
                }

                // Periodic checkpoint
                () = tokio::time::sleep(Duration::from_mins(1)) => {
                    if self.events_since_checkpoint > 0 {
                        match self.save_state().await {
                            Ok(()) => {
                                self.consecutive_checkpoint_failures = 0;
                            }
                            Err(e) => {
                                self.consecutive_checkpoint_failures += 1;
                                error!(
                                    target: "sinex_metrics",
                                    metric = "derive.checkpoint_failures_total",
                                    automaton = %module_name,
                                    error = %e,
                                    consecutive_failures = self.consecutive_checkpoint_failures,
                                    "Failed to save periodic checkpoint"
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
                                        "Checkpoint save failed {} consecutive times; halting to \
                                         prevent silent progress loss on crash+restart",
                                        self.consecutive_checkpoint_failures
                                    )));
                                }
                            }
                        }
                    }
                }
            }
        }

        if let Err(e) = self.save_state().await {
            error!(
                target: "sinex_metrics",
                metric = "derive.checkpoint_failures_total",
                automaton = %module_name,
                error = %e,
                "Failed to save final checkpoint after invalidation run"
            );
            return Err(SinexError::checkpoint(format!(
                "Failed to save final checkpoint after invalidation run: {e}"
            )));
        }

        Ok(ScanReport {
            events_processed: 0,
            duration: start.elapsed(),
            final_checkpoint: self.current_checkpoint_internal(),
            time_range: None,
            runtime_stats: HashMap::from([
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
    pub(super) async fn run_historical(
        &mut self,
        from: Checkpoint,
        end_time: Timestamp,
        args: ScanArgs,
    ) -> RuntimeResult<ScanReport> {
        use sinex_db::repositories::DbPoolExt;
        use sinex_primitives::prelude::*;

        let start = Instant::now();
        let pool = {
            let runtime = self.runtime.as_ref().ok_or_else(|| {
                SinexError::lifecycle("Cannot run historical scan: runtime not initialized")
            })?;
            runtime.db_pool().clone()
        };

        let input_event_type = self.automaton.input_event_type();
        let input_provenance_filter = self.input_provenance_filter();
        info!(
            automaton = %self.automaton.name(),
            model = %self.automaton.automaton_model(),
            input_type = %input_event_type,
            input_provenance = ?input_provenance_filter,
            end_time = %end_time,
            replay = args.replay.is_some(),
            "Starting automaton historical replay"
        );

        let (time_range, mut cursor) = historical_resume_position(&from, end_time)?;

        let mut events_processed = 0u64;
        let mut events_emitted = 0u64;
        let batch_size: i64 = 500;

        // Extract operation ID from replay args if present
        let operation_id: Option<Id<OperationMarker>> =
            args.replay.as_ref().map(|r| Id::from_uuid(r.operation_id));

        loop {
            let query = EventQuery {
                event_types: self.input_query_event_types()?,
                has_lineage: self.input_query_has_lineage(),
                time_range: Some(time_range),
                cursor: cursor.clone(),
                limit: batch_size,
                direction: SortDirection::Asc,
                ..EventQuery::default()
            };

            let result = pool.events().query(query).await.map_err(|e| {
                SinexError::database(format!("Historical replay query failed: {e}"))
            })?;

            let EventQueryResult::Events {
                events,
                next_cursor,
                ..
            } = result
            else {
                break;
            };

            if events.is_empty() {
                break;
            }

            let matching_events = events
                .into_iter()
                .filter(|query_event| self.event_matches_input(&query_event.event))
                .collect::<Vec<_>>();

            if matching_events.is_empty() {
                match next_cursor {
                    Some(c) => {
                        cursor = Some(c);
                        continue;
                    }
                    None => break,
                }
            }

            for query_event in &matching_events {
                let ctx = AutomatonContext::historical(&query_event.event, operation_id)?;
                let trigger_event_id = ctx.trigger_event_id;

                match self
                    .automaton
                    .process_derived(
                        &mut self.persisted_state.state,
                        query_event.event.clone(),
                        &ctx,
                    )
                    .await
                {
                    Ok(outputs) => {
                        self.validate_output_batch(&outputs, "historical replay")?;
                        self.observe_output_batch(&outputs, "replay").await;
                        let output_events =
                            self.build_output_events(outputs, Some(ctx.trigger_event_id), &ctx)?;
                        if let Some(ref emitter) = self.event_emitter {
                            for output_event in output_events {
                                emitter.emit(output_event).await.map_err(|error| {
                                    SinexError::lifecycle(
                                        "failed to emit automaton replay output event",
                                    )
                                    .with_context("automaton", self.automaton.name())
                                    .with_context("trigger_event_id", trigger_event_id.to_string())
                                    .with_source(error)
                                })?;
                                events_emitted += 1;
                            }
                        }
                    }
                    Err(e) => {
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
                                warn!(automaton = %self.automaton.name(), error = %e, "Committing (settled as benign) during historical replay");
                            }
                            Settlement::SendToProcessingFailure
                            | Settlement::Park { .. }
                            | Settlement::Quarantine { .. } => {
                                warn!(automaton = %self.automaton.name(), error = %e, "Routing to processing-failure queue during historical replay");
                                let failed_event = query_event.event.clone();
                                let failure_err = self
                                    .send_to_processing_failure_queue_or_fail(&failed_event, &e)
                                    .await;
                                if let Err(cp_err) = self.save_state().await {
                                    error!(
                                        target: "sinex_metrics",
                                        metric = "derive.checkpoint_failures_total",
                                        automaton = %self.automaton.name(),
                                        error = %cp_err,
                                        "Failed to save checkpoint after replay processing-failure routing error"
                                    );
                                }
                                failure_err?;
                            }
                            Settlement::Retry { .. } => {
                                error!(
                                    target: "sinex_metrics",
                                    metric = "derive.replay_retry_halts_total",
                                    automaton = %self.automaton.name(),
                                    error = %e,
                                    "Retryable error in historical replay; halting replay"
                                );
                                if let Err(cp_err) = self.save_state().await {
                                    error!(
                                        target: "sinex_metrics",
                                        metric = "derive.checkpoint_failures_total",
                                        automaton = %self.automaton.name(),
                                        error = %cp_err,
                                        "Failed to save checkpoint after replay error"
                                    );
                                }
                                return Err(e.into());
                            }
                            Settlement::HaltModule { reason } => {
                                // Halt requests clean drain (see source_driver
                                // / process.rs for the same shape) so systemd
                                // records the unit as cleanly exited.
                                if let Some(drain) = self.shutdown_tx.as_ref() {
                                    let _ = drain.request_drain_and_warn(self.automaton.name());
                                }
                                error!(
                                    target: "sinex_metrics",
                                    metric = "derive.runtime_halts_total",
                                    automaton = %self.automaton.name(),
                                    error = %e,
                                    reason = ?reason,
                                    "Halting module during historical replay; runtime drain requested"
                                );
                                if let Err(cp_err) = self.save_state().await {
                                    error!(
                                        target: "sinex_metrics",
                                        metric = "derive.checkpoint_failures_total",
                                        automaton = %self.automaton.name(),
                                        error = %cp_err,
                                        "Failed to save checkpoint after replay halt error"
                                    );
                                }
                                return Err(SinexError::processing(format!(
                                    "RuntimeModule halted during replay: {reason:?} — {e}"
                                )));
                            }
                            Settlement::DrainRuntimeUnit { reason } => {
                                if let Some(drain) = self.shutdown_tx.as_ref() {
                                    let _ = drain.request_drain_and_warn(self.automaton.name());
                                }
                                error!(
                                    target: "sinex_metrics",
                                    metric = "derive.runtime_drains_total",
                                    automaton = %self.automaton.name(),
                                    error = %e,
                                    reason = %reason,
                                    "Draining runtime unit during historical replay"
                                );
                                if let Err(cp_err) = self.save_state().await {
                                    error!(
                                        target: "sinex_metrics",
                                        metric = "derive.checkpoint_failures_total",
                                        automaton = %self.automaton.name(),
                                        error = %cp_err,
                                        "Failed to save checkpoint after replay drain error"
                                    );
                                }
                                return Err(SinexError::processing(format!(
                                    "Runtime unit drained during replay: {reason} — {e}"
                                )));
                            }
                        }
                    }
                }
                events_processed += 1;
                self.record_processed_input(trigger_event_id);
                self.observe_runtime_snapshot().await;
            }

            if self.should_checkpoint() {
                self.save_state().await.map_err(|e| {
                    error!(
                        target: "sinex_metrics",
                        metric = "derive.checkpoint_failures_total",
                        automaton = %self.automaton.name(),
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

        if let Err(e) = self.save_state().await {
            error!(
                target: "sinex_metrics",
                metric = "derive.checkpoint_failures_total",
                automaton = %self.automaton.name(),
                error = %e,
                "Failed to save checkpoint after replay"
            );
            return Err(SinexError::checkpoint(format!(
                "Failed to save checkpoint after historical replay: {e}"
            )));
        }

        info!(
            automaton = %self.automaton.name(),
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
            runtime_stats: HashMap::from([
                ("total_processed".to_string(), events_processed),
                ("events_emitted".to_string(), events_emitted),
            ]),
            successful_targets: vec!["historical_replay".to_string()],
            failed_targets: vec![],
            warnings: vec![],
        })
    }

    #[cfg(not(feature = "db"))]
    pub(super) async fn run_historical(
        &mut self,
        _from: Checkpoint,
        _end_time: Timestamp,
        _args: ScanArgs,
    ) -> RuntimeResult<ScanReport> {
        Err(SinexError::unknown(
            "Automaton historical replay requires the 'db' feature",
        ))
    }
}

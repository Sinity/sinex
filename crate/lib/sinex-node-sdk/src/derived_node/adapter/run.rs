//! `run_continuous` / `run_historical` for `DerivedNodeAdapter`.
//!
//! Carved out of `adapter/mod.rs` as part of #697. Pure mechanical move; the
//! methods, control flow, and instrumentation are unchanged.

use super::{DerivedNodeAdapter, historical_resume_position, recv_invalidation};

use crate::derived_node::context::DerivedTriggerContext;
use crate::derived_node::traits::DerivedNodeImpl;
use crate::error_helpers::env_parse_with_default;
use crate::processing::ErrorAction;
use crate::runtime::stream::{Checkpoint, NodeRuntimeState, ScanArgs, ScanReport};
use crate::{NodeResult, SinexError};

use sinex_primitives::temporal::Timestamp;

use std::collections::HashMap;
use std::time::{Duration, Instant};
use tracing::{debug, error, info, warn};

impl<N> DerivedNodeAdapter<N>
where
    N: DerivedNodeImpl,
{
    pub(super) async fn run_continuous(
        &mut self,
        _from: Checkpoint,
    ) -> NodeResult<ScanReport> {
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
            let nats_client = self
                .runtime
                .as_ref()
                .and_then(NodeRuntimeState::nats_client);

            if let Some(client) = nats_client {
                let env = sinex_primitives::environment::environment();
                let stream_name = env.nats_stream_name("SINEX_RAW_EVENTS_DERIVED_INVALIDATIONS");
                let queue_group = format!("derived.invalidation.{}", self.node.name());
                let deliver_subject = client.new_inbox();
                let js = async_nats::jetstream::new(client.clone());

                match js.get_stream(&stream_name).await {
                    Ok(stream) => {
                        let config = async_nats::jetstream::consumer::push::Config {
                            deliver_subject: deliver_subject.to_string(),
                            deliver_group: Some(queue_group.clone()),
                            ..Default::default()
                        };
                        match stream.create_consumer(config).await {
                            Ok(consumer) => {
                                match consumer.messages().await {
                                    Ok(messages) => {
                                        info!(
                                            node = %node_name,
                                            stream = %stream_name,
                                            queue_group = %queue_group,
                                            "Subscribed to invalidation signals via JetStream push consumer"
                                        );
                                        Some(messages)
                                    }
                                    Err(e) => {
                                        warn!(
                                            node = %node_name,
                                            error = %e,
                                            "Failed to start invalidation consumer message stream"
                                        );
                                        None
                                    }
                                }
                            }
                            Err(e) => {
                                warn!(
                                    node = %node_name,
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
                            node = %node_name,
                            stream = %stream_name,
                            error = %e,
                            "Failed to get invalidation stream"
                        );
                        None
                    }
                }
            } else {
                debug!(node = %node_name, "No NATS client — invalidation subscription skipped");
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
                        node = %node_name,
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
                                    node = %node_name,
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
            error!(node = %node_name, error = %e, "Failed to save final checkpoint after invalidation run");
            return Err(SinexError::checkpoint(format!(
                "Failed to save final checkpoint after invalidation run: {e}"
            )));
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
    pub(super) async fn run_historical(
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
        let input_provenance_filter = self.input_provenance_filter();
        info!(
            node = %self.node.name(),
            model = %self.node.node_model(),
            input_type = %input_event_type,
            input_provenance = ?input_provenance_filter,
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
                        self.validate_output_batch(&outputs, "historical replay")?;
                        self.observe_output_batch(&outputs, "replay").await;
                        let output_events =
                            self.build_output_events(outputs, Some(ctx.trigger_event_id), &ctx)?;
                        if let Some(ref emitter) = self.event_emitter {
                            for output_event in output_events {
                                emitter.emit(output_event).await.map_err(|error| {
                                    SinexError::lifecycle(
                                        "failed to emit derived-node replay output event",
                                    )
                                    .with_context("node", self.node.name())
                                    .with_context("trigger_event_id", trigger_event_id.to_string())
                                    .with_source(error)
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
                            ErrorAction::SendToProcessingFailureQueue => {
                                let failed_event = query_event.event.clone();
                                let failure_err = self
                                    .send_to_processing_failure_queue_or_fail(&failed_event, &e)
                                    .await;
                                if let Err(cp_err) = self.save_state().await {
                                    error!(
                                        node = %self.node.name(),
                                        error = %cp_err,
                                        "Failed to save checkpoint after replay processing-failure routing error"
                                    );
                                }
                                failure_err?;
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
                self.observe_runtime_snapshot().await;
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

        if let Err(e) = self.save_state().await {
            error!(node = %self.node.name(), error = %e, "Failed to save checkpoint after replay");
            return Err(SinexError::checkpoint(format!(
                "Failed to save checkpoint after historical replay: {e}"
            )));
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
    pub(super) async fn run_historical(
        &mut self,
        _from: Checkpoint,
        _end_time: Timestamp,
        _args: ScanArgs,
    ) -> NodeResult<ScanReport> {
        Err(SinexError::unknown(
            "DerivedNode historical replay requires the 'db' feature",
        ))
    }
}

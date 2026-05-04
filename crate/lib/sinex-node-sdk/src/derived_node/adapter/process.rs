//! Per-event and per-batch processing for `DerivedNodeAdapter`.
//!
//! Carved out of `adapter/mod.rs` as part of #697. Pure mechanical move; the
//! methods, control flow, and instrumentation are unchanged.

use super::{DerivedNodeAdapter, INVALIDATION_QUERY_PAGE_SIZE, event_lag_ms};

use crate::derived_node::context::DerivedTriggerContext;
use crate::derived_node::traits::DerivedNodeImpl;
use crate::{NodeResult, SinexError};

use sinex_primitives::events::Event;
use sinex_primitives::settlement::{
    DefaultFailurePolicy, FailureContext, FailurePolicy, RuntimeOperation, RuntimePhase, Settlement,
};
use sinex_primitives::JsonValue;
#[cfg(feature = "db")]
use sinex_primitives::query::{EventQuery, EventQueryResult, QueryResultEvent};

use tracing::{error, warn};
#[cfg(feature = "db")]
use tracing::info;

impl<N> DerivedNodeAdapter<N>
where
    N: DerivedNodeImpl,
{
    #[cfg(feature = "db")]
    pub(super) async fn load_query_events_paginated(
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

    pub(super) async fn send_to_processing_failure_queue_or_fail(
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

    pub(super) async fn emit_output_events(
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

                    // Emit automaton error telemetry before routing
                    if let Some(ref observer) = self.self_observer {
                        let mut labels = self.derived_metric_labels();
                        labels.insert("error".to_string(), e.to_string());
                        labels.insert(
                            "error_class".to_string(),
                            format!("{:?}", sinex_error.error_class()),
                        );
                        if let Err(obs_err) =
                            observer.emit_counter("automaton.error", 1, Some(labels)).await
                        {
                            warn!(
                                node = %self.node.name(),
                                error = %obs_err,
                                "Failed to emit automaton error counter"
                            );
                        }
                    }
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
                // Use the richer FailurePolicy::settle() instead of the
                // 3-variant ErrorAction. DefaultFailurePolicy maps ErrorClass
                // to Settlement variants with backoff and retry budgets.
                let sinex_error = SinexError::processing("derived node processing error")
                    .with_source(e.to_string());
                let failure_ctx = FailureContext {
                    unit_id: self.node.name().to_string(),
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
                        warn!(node = %self.node.name(), error = %e, "Committing (settled as benign)");
                        self.record_processed_input(source_event_id);
                        self.observe_runtime_snapshot().await;
                        Ok(Vec::new())
                    }
                    Settlement::SendToProcessingFailure
                    | Settlement::Park { .. }
                    | Settlement::Quarantine { .. } => {
                        warn!(node = %self.node.name(), error = %e, "Routing to processing-failure queue");
                        self.send_to_processing_failure_queue_or_fail(&event, &e)
                            .await?;
                        self.record_processed_input(source_event_id);
                        self.observe_runtime_snapshot().await;
                        Ok(Vec::new())
                    }
                    Settlement::Retry { .. } => {
                        error!(node = %self.node.name(), error = %e, "Retryable error; halting batch");
                        Err(e.into())
                    }
                    Settlement::HaltNode { reason } => {
                        error!(node = %self.node.name(), error = %e, reason = ?reason, "Halting node");
                        Err(SinexError::processing(format!(
                            "Node halted: {reason:?} — {e}"
                        )))
                    }
                    Settlement::DrainRuntimeUnit { reason } => {
                        error!(node = %self.node.name(), error = %e, reason = %reason, "Draining runtime unit");
                        Err(SinexError::processing(format!(
                            "Runtime unit drained: {reason} — {e}"
                        )))
                    }
                }
            }
        }
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
}

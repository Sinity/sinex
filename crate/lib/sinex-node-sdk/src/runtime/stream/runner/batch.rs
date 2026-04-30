//! Batch processing helpers for `NodeRunner<T>`.
//!
//! Hosts the per-batch automaton processing path: batch dispatch with
//! per-event DLQ fallback if the batch fails, and the checkpoint-save
//! helper that persists progress through the bridge.

use super::*;

impl<T: Node + 'static> NodeRunner<T> {
    /// Process a batch of events, falling back to per-event processing with DLQ
    /// routing if the batch fails. Returns the total number of events processed
    /// (including those routed to the DLQ).
    #[cfg(feature = "messaging")]
    pub(super) async fn process_batch_with_dlq_fallback(
        node: &mut T,
        transport: &EventTransport,
        events: Vec<Event<JsonValue>>,
    ) -> NodeResult<u64> {
        let batch_size = events.len();
        let events_backup = events.clone();

        match node.process_event_batch(events).await {
            Ok(stats) => {
                if batch_size > 1 {
                    debug!(
                        batch_size,
                        processed = stats.processed,
                        "Processed event batch"
                    );
                }
                Ok(stats.processed as u64)
            }
            Err(batch_err) => {
                // Fatal errors (NodeFatal, TransportDegraded) apply to the
                // entire node, not to any one event. Per-event DLQ fallback
                // would route every event in the batch — and every subsequent
                // batch — to DLQ while the node keeps consuming, generating
                // an unbounded log/IO storm. Issue #581 observed 221K
                // consecutive failures producing 1.6M journal entries and
                // 54 GB of NATS traffic on sinnix-prime before I/O saturation
                // halted the host.
                //
                // Use the new error_class() classification instead of
                // hardcoding individual variants. Checkpoint, Lifecycle,
                // Configuration, PermissionDenied, and live-context
                // ChannelSend are all NodeFatal.
                let error_class = batch_err.error_class();
                if error_class.is_fatal() {
                    error!(
                        error = %batch_err,
                        ?error_class,
                        batch_size,
                        "Fatal error in batch processing; halting node (per-event DLQ fallback would loop on every event)"
                    );
                    return Err(batch_err);
                }
                warn!(
                    error = %batch_err,
                    ?error_class,
                    batch_size,
                    "Batch processing failed; falling back to per-event processing with DLQ routing"
                );
                let node_name = node.node_name().to_string();
                let mut succeeded = 0u64;
                for event in events_backup {
                    match node.process_event_batch(vec![event.clone()]).await {
                        Ok(stats) => {
                            succeeded += stats.processed as u64;
                        }
                        Err(event_err) => {
                            // Same defense as the batch path — fatal errors
                            // are not data errors. Halt immediately.
                            if event_err.error_class().is_fatal() {
                                error!(
                                    error = %event_err,
                                    "Checkpoint error during per-event fallback; halting node"
                                );
                                return Err(event_err);
                            }
                            let event_id = event.id;
                            warn!(
                                error = %event_err,
                                ?event_id,
                                "Event processing failed; routing to DLQ"
                            );
                            if let Err(dlq_err) = transport
                                .send_to_processing_failure_queue(
                                    &event,
                                    &event_err.to_string(),
                                    &node_name,
                                )
                                .await
                            {
                                return Err(SinexError::processing(
                                    "failed to route failed automaton event to processing-failure stream",
                                )
                                .with_context("node", node_name.clone())
                                .with_context(
                                    "event_id",
                                    event_id.as_ref().map_or_else(
                                        || "missing".to_string(),
                                        std::string::ToString::to_string,
                                    ),
                                )
                                .with_context("source", event.source.as_str().to_string())
                                .with_context("event_type", event.event_type.as_str().to_string())
                                .with_context("processing_error", event_err.to_string())
                                .with_source(dlq_err));
                            }
                        }
                    }
                }
                let dlq_count = batch_size as u64 - succeeded;
                info!(succeeded, dlq_count, "Per-event fallback complete");
                // Count DLQ'd events as processed for checkpoint advancement
                Ok(batch_size as u64)
            }
        }
    }

    /// Save a checkpoint if `last_event_id` is `Some`. Returns the new revision
    /// on success, or `None` if there was nothing to save or the save failed.
    ///
    /// Tracks consecutive failures in `consecutive_failures`. Resets to 0 on success.
    /// Returns a hard error after 3 consecutive failures to prevent silent progress loss
    /// on crash+restart (which would cause duplicate event processing).
    #[cfg(feature = "messaging")]
    pub(super) async fn try_save_checkpoint(
        checkpoint_manager: &CheckpointManager,
        checkpoint_state: &mut crate::checkpoint::CheckpointState,
        last_event_id: Option<Uuid>,
        processed_events: u64,
        consecutive_failures: &mut u32,
    ) -> NodeResult<Option<u64>> {
        let Some(eid) = last_event_id else {
            return Ok(None);
        };
        checkpoint_state.checkpoint = Checkpoint::Internal {
            event_id: eid,
            message_count: processed_events,
        };
        checkpoint_state.processed_count = processed_events;
        checkpoint_state.last_activity = sinex_primitives::temporal::Timestamp::now();
        match checkpoint_manager.save_checkpoint(checkpoint_state).await {
            Ok(revision) => {
                *consecutive_failures = 0;
                debug!(processed_events, revision, "Checkpoint saved");
                Ok(Some(revision))
            }
            Err(err) => {
                *consecutive_failures += 1;
                error!(
                    error = %err,
                    consecutive_failures = *consecutive_failures,
                    "Failed to save checkpoint; will retry next interval"
                );
                if *consecutive_failures >= 3 {
                    return Err(SinexError::checkpoint(format!(
                        "Checkpoint save failed {} consecutive times; halting to prevent \
                         silent progress loss on crash+restart",
                        *consecutive_failures
                    )));
                }
                Ok(None)
            }
        }
    }

}

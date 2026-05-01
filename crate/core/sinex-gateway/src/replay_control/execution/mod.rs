use async_nats::{Client, jetstream};
use color_eyre::eyre::{Context, Result, eyre};
use futures::StreamExt;
use serde::Deserialize;
use sinex_db::replay::state_machine::{
    ReplayCheckpoint, ReplayOperation, ReplayScope, ReplayState, ReplayStateMachine,
};
use sinex_db::repositories::{DbPoolExt, EventRepositoryTx};
use sinex_node_sdk::derived_node::invalidation::{DerivedScopeInvalidation, INVALIDATION_SUBJECT};
use sinex_node_sdk::runtime::stream::{
    Checkpoint, MaterialReplayContext, NodeScanAck, NodeScanCommand, NodeScanProgress,
    ReplayScopeFilters as NodeReplayScopeFilters, ResolvedReplayMaterial, ScanArgs, TimeHorizon,
};
use sinex_primitives::domain::{EventSource, EventType, NodeName};
use sinex_primitives::environment::{SinexEnvironment, environment};
use sinex_primitives::events::{Event as StoredEvent, Provenance};
use sinex_primitives::{Id, SinexError, Timestamp, Uuid};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tracing::{debug, error, info, warn};

use super::validation::{replay_scope_drift_error, stale_preview_missing_root_ids_error};

pub(super) const REPLAY_OUTPUT_VISIBILITY_TIMEOUT: Duration = Duration::from_secs(30);

/// Engine responsible for executing replay operations.
///
/// The execution engine:
/// 1. Queries events from the database matching the replay scope
/// 2. Expands and archives the full affected cascade (live -> archive)
/// 3. Dispatches a scan command to the target ingestor node via NATS
/// 4. The node re-reads source material and emits fresh events through normal flow
/// 5. Tracks progress via checkpoints and NATS progress messages
#[derive(Clone)]
pub(super) struct ReplayExecutionEngine {
    replay: Arc<ReplayStateMachine>,
    nats_client: Client,
    js: jetstream::Context,
    env: SinexEnvironment,
    scan_ack_timeout: Duration,
    scan_completion_timeout: Duration,
    #[cfg(test)]
    checkpoint_failures_remaining: Option<Arc<AtomicUsize>>,
    #[cfg(test)]
    scope_metadata_failures_remaining: Option<Arc<AtomicUsize>>,
    #[cfg(test)]
    scope_invalidation_publish_failures_remaining: Option<Arc<AtomicUsize>>,
    #[cfg(test)]
    replacement_record_failures_remaining: Option<Arc<AtomicUsize>>,
}

#[derive(Debug)]
struct OperationOutputEvent {
    id: Uuid,
    equivalence_key: Option<String>,
}

#[derive(Debug)]
pub(super) struct ScopeInvalidationBucket {
    pub(super) event_ids: Vec<Uuid>,
    pub(super) event_source: String,
    pub(super) event_type: String,
    pub(super) has_lineage: bool,
    pub(super) scope_keys: Vec<String>,
}

impl ReplayExecutionEngine {
    const EXECUTION_STATE_POLL_INTERVAL: Duration = Duration::from_millis(250);

    pub(super) fn new(replay: Arc<ReplayStateMachine>, nats_client: Client) -> Self {
        let js = jetstream::new(nats_client.clone());
        Self {
            replay,
            nats_client,
            js,
            env: environment(),
            scan_ack_timeout: Self::SCAN_ACK_TIMEOUT,
            scan_completion_timeout: Self::SCAN_COMPLETION_TIMEOUT,
            #[cfg(test)]
            checkpoint_failures_remaining: None,
            #[cfg(test)]
            scope_metadata_failures_remaining: None,
            #[cfg(test)]
            scope_invalidation_publish_failures_remaining: None,
            #[cfg(test)]
            replacement_record_failures_remaining: None,
        }
    }

    #[cfg(test)]
    pub(super) fn with_scan_ack_timeout(mut self, scan_ack_timeout: Duration) -> Self {
        self.scan_ack_timeout = scan_ack_timeout;
        self
    }

    #[cfg(test)]
    pub(super) fn with_scan_completion_timeout(mut self, scan_completion_timeout: Duration) -> Self {
        self.scan_completion_timeout = scan_completion_timeout;
        self
    }

    #[cfg(test)]
    pub(super) fn with_checkpoint_failures(mut self, checkpoint_failures_remaining: Arc<AtomicUsize>) -> Self {
        self.checkpoint_failures_remaining = Some(checkpoint_failures_remaining);
        self
    }

    #[cfg(test)]
    pub(super) fn with_scope_metadata_failures(
        mut self,
        scope_metadata_failures_remaining: Arc<AtomicUsize>,
    ) -> Self {
        self.scope_metadata_failures_remaining = Some(scope_metadata_failures_remaining);
        self
    }

    #[cfg(test)]
    pub(super) fn with_scope_invalidation_publish_failures(
        mut self,
        scope_invalidation_publish_failures_remaining: Arc<AtomicUsize>,
    ) -> Self {
        self.scope_invalidation_publish_failures_remaining =
            Some(scope_invalidation_publish_failures_remaining);
        self
    }

    #[cfg(test)]
    pub(super) fn with_replacement_record_failures(
        mut self,
        replacement_record_failures_remaining: Arc<AtomicUsize>,
    ) -> Self {
        self.replacement_record_failures_remaining = Some(replacement_record_failures_remaining);
        self
    }

    pub(super) async fn execute(
        &self,
        operation_id: Uuid,
        executor_name: String,
    ) -> Result<ReplayOperation> {
        let Some(_execution_lock) = self.replay.acquire_execution_lock(operation_id).await? else {
            return Err(eyre!(
                "Operation {} is already executing on another node",
                operation_id
            ));
        };

        info!(
            operation_id = %operation_id,
            executor = %executor_name,
            "Starting replay execution"
        );

        let result = self.run_operation(operation_id, &executor_name).await;
        let bookkeeping_error = self
            .handle_execution_finish(operation_id, &result)
            .await
            .err();
        match (result, bookkeeping_error) {
            (Ok(operation), None) => Ok(operation),
            (Ok(_), Some(bookkeeping_error)) => Err(bookkeeping_error),
            (Err(err), Some(bookkeeping_error)) => Err(Self::wrap_bookkeeping_error(
                err,
                operation_id,
                Some(bookkeeping_error),
            )),
            (Err(err), None) => match self.load_cancelled_operation(operation_id).await {
                Ok(Some(cancelled)) if cancelled.started_at.is_some() => Ok(cancelled),
                Ok(Some(_)) => Err(err),
                Ok(None) => Err(err),
                Err(load_err) => Err(err).wrap_err(format!(
                    "replay cancellation probe failed after execution error: {load_err}"
                )),
            },
        }
    }

    pub(super) async fn submit(
        &self,
        operation_id: Uuid,
        submitter: String,
    ) -> Result<ReplayOperation> {
        let Some(_execution_lock) = self.replay.acquire_execution_lock(operation_id).await? else {
            return Err(eyre!(
                "Operation {} is already executing on another node",
                operation_id
            ));
        };

        info!(
            operation_id = %operation_id,
            executor = %submitter,
            "Submitting replay preview for immediate execution"
        );

        let result = self.run_submitted_operation(operation_id, &submitter).await;
        let bookkeeping_error = self
            .handle_execution_finish(operation_id, &result)
            .await
            .err();
        match (result, bookkeeping_error) {
            (Ok(operation), None) => Ok(operation),
            (Ok(_), Some(bookkeeping_error)) => Err(bookkeeping_error),
            (Err(err), Some(bookkeeping_error)) => Err(Self::wrap_bookkeeping_error(
                err,
                operation_id,
                Some(bookkeeping_error),
            )),
            (Err(err), None) => match self.load_cancelled_operation(operation_id).await {
                Ok(Some(cancelled)) if cancelled.started_at.is_some() => Ok(cancelled),
                Ok(Some(_)) => Err(err),
                Ok(None) => Err(err),
                Err(load_err) => Err(err).wrap_err(format!(
                    "replay cancellation probe failed after execution error: {load_err}"
                )),
            },
        }
    }

    pub(super) async fn handle_execution_finish(
        &self,
        operation_id: Uuid,
        result: &Result<ReplayOperation>,
    ) -> Result<()> {
        let operation = self
            .replay
            .load_operation(operation_id)
            .await
            .wrap_err_with(|| {
                format!(
                    "failed to inspect replay operation state after execution for {operation_id}"
                )
            })?;

        if operation.state == ReplayState::Cancelled {
            info!(
                operation_id = %operation_id,
                state = ?operation.state,
                "Replay execution stopped after operator cancellation"
            );
            return Ok(());
        }

        if operation.state == ReplayState::Cancelling
            && Self::execution_result_is_cancellation(result)
        {
            self.replay
                .finish_cancellation(operation_id)
                .await
                .wrap_err_with(|| {
                    format!("failed to finalize replay cancellation for operation {operation_id}")
                })?;
            info!(
                operation_id = %operation_id,
                state = ?ReplayState::Cancelled,
                "Replay execution stopped after operator cancellation"
            );
            return Ok(());
        }

        if let Err(err) = result {
            error!(
                operation_id = %operation_id,
                error = %err,
                "Replay execution failed"
            );
            if let Err(mark_err) = self
                .replay
                .mark_failed(operation_id, format!("{err:#}"))
                .await
            {
                error!(
                    operation_id = %operation_id,
                    mark_error = %mark_err,
                    execution_error = %err,
                    "OPERATOR ACTION REQUIRED: replay operation stuck in Executing state. \
                     Run: sinexctl replay cancel {operation_id} --reason 'stuck after mark_failed failure'"
                );
                return Err(eyre!(
                    "Replay execution failed ({err:#}) and marking operation as failed also failed ({mark_err}); \
                     operation {operation_id} is stuck in Executing state"
                ));
            }
        }

        Ok(())
    }

    pub(super) fn wrap_bookkeeping_error(
        err: color_eyre::eyre::Report,
        operation_id: Uuid,
        bookkeeping_error: Option<color_eyre::eyre::Report>,
    ) -> color_eyre::eyre::Report {
        match bookkeeping_error {
            Some(bookkeeping_error) => err.wrap_err(format!(
                "failed to finalize replay execution bookkeeping for operation {operation_id}: {bookkeeping_error:#}"
            )),
            None => err,
        }
    }

    pub(super) async fn run_operation(
        &self,
        operation_id: Uuid,
        executor_name: &str,
    ) -> Result<ReplayOperation> {
        let (initial, total_events, execution_window, preview_root_ids) =
            self.prepare_operation(operation_id, executor_name).await?;

        // Initialize checkpoint
        let mut checkpoint = ReplayCheckpoint {
            processed_events: 0,
            total_events,
            last_event_id: initial.checkpoint.last_event_id,
            batch_number: 0,
            savepoint_id: None,
            updated_at: sinex_primitives::temporal::now(),
        };

        // Execute actual replay
        let replay_result = self
            .replay_events(
                operation_id,
                &initial.scope,
                execution_window,
                total_events,
                &preview_root_ids,
                self.replay.pool(),
                &mut checkpoint,
                executor_name,
            )
            .await;

        self.finalize_operation(operation_id, total_events, checkpoint, replay_result)
            .await
    }

    pub(super) async fn run_submitted_operation(
        &self,
        operation_id: Uuid,
        submitter: &str,
    ) -> Result<ReplayOperation> {
        let (initial, total_events, execution_window, preview_root_ids) = self
            .prepare_submitted_operation(operation_id, submitter)
            .await?;

        let mut checkpoint = ReplayCheckpoint {
            processed_events: 0,
            total_events,
            last_event_id: initial.checkpoint.last_event_id,
            batch_number: 0,
            savepoint_id: None,
            updated_at: sinex_primitives::temporal::now(),
        };

        let replay_result = self
            .replay_events(
                operation_id,
                &initial.scope,
                execution_window,
                total_events,
                &preview_root_ids,
                self.replay.pool(),
                &mut checkpoint,
                submitter,
            )
            .await;

        self.finalize_operation(operation_id, total_events, checkpoint, replay_result)
            .await
    }

    pub(super) async fn prepare_operation(
        &self,
        operation_id: Uuid,
        executor_name: &str,
    ) -> Result<(ReplayOperation, u64, (Timestamp, Timestamp), Vec<Uuid>)> {
        let op = self.replay.load_operation(operation_id).await?;
        if op.state != ReplayState::Approved {
            return Err(eyre!(
                "Operation {} must be approved before execution",
                operation_id
            ));
        }

        let (total_events, execution_window, preview_root_ids) =
            Self::execution_inputs_from_operation(operation_id, &op)?;

        self.replay
            .begin_execution(operation_id, NodeName::new(executor_name))
            .await?;

        info!(
            operation_id = %operation_id,
            total_events = total_events,
            node_id = %op.scope.node_id,
            "Beginning event replay"
        );

        Ok((op, total_events, execution_window, preview_root_ids))
    }

    pub(super) async fn prepare_submitted_operation(
        &self,
        operation_id: Uuid,
        submitter: &str,
    ) -> Result<(ReplayOperation, u64, (Timestamp, Timestamp), Vec<Uuid>)> {
        let executor_node = NodeName::new(submitter);
        let operation = self
            .replay
            .submit_previewed_for_execution(operation_id, submitter.to_string(), executor_node)
            .await?;
        let (total_events, execution_window, preview_root_ids) =
            Self::execution_inputs_from_operation(operation_id, &operation)?;

        info!(
            operation_id = %operation_id,
            total_events = total_events,
            node_id = %operation.scope.node_id,
            "Beginning event replay from atomic submit"
        );

        Ok((operation, total_events, execution_window, preview_root_ids))
    }

    pub(super) fn execution_inputs_from_operation(
        operation_id: Uuid,
        operation: &ReplayOperation,
    ) -> Result<(u64, (Timestamp, Timestamp), Vec<Uuid>)> {
        let preview = operation.preview_summary.clone().ok_or_else(|| {
            eyre!(
                "Operation {} is missing preview summary; run preview before execution",
                operation_id
            )
        })?;
        let preview_summary: ReplayPreviewSummary = serde_json::from_value(preview)
            .map_err(|e| eyre!("Invalid replay preview summary: {e}"))?;
        let total_events = preview_summary.total_events;
        if total_events == 0 {
            return Err(eyre!(
                "Operation {} preview matches zero events; refresh preview before execution",
                operation_id
            ));
        }
        let mut preview_root_ids = preview_summary.root_event_ids;
        preview_root_ids.sort_unstable();
        preview_root_ids.dedup();
        if preview_root_ids.is_empty() {
            return Err(stale_preview_missing_root_ids_error(
                operation_id,
                total_events,
            ));
        }
        if preview_root_ids.len() as u64 != total_events {
            return Err(eyre!(
                "Operation {} preview summary is inconsistent: total_events={} but root_event_ids contains {} ids",
                operation_id,
                total_events,
                preview_root_ids.len()
            ));
        }

        Ok((
            total_events,
            (
                preview_summary.time_window.start,
                preview_summary.time_window.end,
            ),
            preview_root_ids,
        ))
    }

    pub(super) async fn finalize_operation(
        &self,
        operation_id: Uuid,
        total_events: u64,
        mut checkpoint: ReplayCheckpoint,
        replay_result: Result<u64>,
    ) -> Result<ReplayOperation> {
        match replay_result {
            Ok(processed_count) => {
                info!(
                    operation_id = %operation_id,
                    processed_events = processed_count,
                    total_events = total_events,
                    "Replay completed successfully"
                );

                // Finalize checkpoint
                checkpoint.processed_events = processed_count;
                checkpoint.updated_at = sinex_primitives::temporal::now();
                self.persist_replay_checkpoint(
                    operation_id,
                    &checkpoint,
                    "Failed to persist final replay checkpoint",
                )
                .await?;

                if let Some(cancelled) = self.load_cancelled_operation(operation_id).await? {
                    return Ok(cancelled);
                }

                // Transition through Committing to Completed
                self.replay
                    .transition(operation_id, ReplayState::Committing)
                    .await?;
                self.replay
                    .transition(operation_id, ReplayState::Completed)
                    .await?;

                self.replay
                    .load_operation(operation_id)
                    .await
                    .map_err(|e| eyre!("{}", e))
            }
            Err(err) => {
                // Update checkpoint with current progress before failing
                checkpoint.updated_at = sinex_primitives::temporal::now();
                if let Err(checkpoint_error) = self
                    .persist_replay_checkpoint(
                        operation_id,
                        &checkpoint,
                        "Failed to persist replay checkpoint after execution error",
                    )
                    .await
                {
                    return Err(err.wrap_err(format!("{checkpoint_error}")));
                }
                Err(err)
            }
        }
    }

    pub(super) async fn load_cancelled_operation(
        &self,
        operation_id: Uuid,
    ) -> Result<Option<ReplayOperation>> {
        let operation = self.replay.load_operation(operation_id).await?;
        Ok((operation.state == ReplayState::Cancelled).then_some(operation))
    }

    pub(super) fn execution_result_is_cancellation(result: &Result<ReplayOperation>) -> bool {
        result.as_ref().is_err_and(|err| {
            err.downcast_ref::<SinexError>()
                .is_some_and(|sinex_err| matches!(sinex_err, SinexError::Cancelled(_)))
        })
    }

    #[cfg(test)]
    pub(super) fn maybe_fail_checkpoint_persist(&self) -> Result<()> {
        if let Some(remaining) = &self.checkpoint_failures_remaining
            && remaining
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |value| {
                    value.checked_sub(1)
                })
                .is_ok()
        {
            return Err(eyre!("forced replay checkpoint persistence failure"));
        }
        Ok(())
    }

    #[cfg(not(test))]
    #[allow(
        clippy::unnecessary_wraps,
        reason = "Shape must match the #[cfg(test)] fault-injection variant, which returns Err"
    )]
    pub(super) fn maybe_fail_checkpoint_persist(&self) -> Result<()> {
        Ok(())
    }

    pub(super) async fn persist_replay_checkpoint(
        &self,
        operation_id: Uuid,
        checkpoint: &ReplayCheckpoint,
        context: &'static str,
    ) -> Result<()> {
        self.maybe_fail_checkpoint_persist().wrap_err(context)?;
        self.replay
            .update_checkpoint(operation_id, checkpoint)
            .await
            .map_err(|e| eyre!("{e}"))
            .wrap_err(context)
    }

    #[cfg(test)]
    pub(super) fn maybe_fail_scope_metadata_collection(&self) -> Result<()> {
        if let Some(remaining) = &self.scope_metadata_failures_remaining
            && remaining
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |value| {
                    value.checked_sub(1)
                })
                .is_ok()
        {
            return Err(eyre!("forced replay scope metadata collection failure"));
        }
        Ok(())
    }

    #[cfg(not(test))]
    #[allow(
        clippy::unnecessary_wraps,
        reason = "Shape must match the #[cfg(test)] fault-injection variant, which returns Err"
    )]
    pub(super) fn maybe_fail_scope_metadata_collection(&self) -> Result<()> {
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn maybe_fail_scope_invalidation_publish(&self) -> Result<()> {
        if let Some(remaining) = &self.scope_invalidation_publish_failures_remaining
            && remaining
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |value| {
                    value.checked_sub(1)
                })
                .is_ok()
        {
            return Err(eyre!("forced replay scope invalidation publish failure"));
        }
        Ok(())
    }

    #[cfg(not(test))]
    #[allow(
        clippy::unnecessary_wraps,
        reason = "Shape must match the #[cfg(test)] fault-injection variant, which returns Err"
    )]
    pub(super) fn maybe_fail_scope_invalidation_publish(&self) -> Result<()> {
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn maybe_fail_replacement_recording(&self) -> Result<()> {
        if let Some(remaining) = &self.replacement_record_failures_remaining
            && remaining
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |value| {
                    value.checked_sub(1)
                })
                .is_ok()
        {
            return Err(eyre!("forced replay replacement recording failure"));
        }
        Ok(())
    }

    #[cfg(not(test))]
    #[allow(
        clippy::unnecessary_wraps,
        reason = "Shape must match the #[cfg(test)] fault-injection variant, which returns Err"
    )]
    pub(super) fn maybe_fail_replacement_recording(&self) -> Result<()> {
        Ok(())
    }
}


#[derive(Debug, Deserialize)]
pub(super) struct ReplayPreviewSummary {
    pub(super) total_events: u64,
    pub(super) time_window: ReplayPreviewTimeWindow,
    #[serde(default)]
    pub(super) root_event_ids: Vec<Uuid>,
}

#[derive(Debug, Deserialize)]
pub(super) struct ReplayPreviewTimeWindow {
    pub(super) start: Timestamp,
    pub(super) end: Timestamp,
}


#[derive(Debug, Clone)]
pub(super) struct ExpectedReplayOutputs {
    pub(super) minimum_visible_count: u64,
    pub(super) sources: Vec<String>,
    pub(super) event_types: Vec<String>,
    pub(super) logical_source_identifiers: Vec<String>,
}


mod collect;
mod replay_writer;

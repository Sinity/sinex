use async_nats::{Client, jetstream};
use futures::StreamExt;
use serde::Deserialize;
use sinex_db::replay::state_machine::{
    ReplayCheckpoint, ReplayOperation, ReplayState, ReplayStateMachine,
};
use sinex_primitives::domain::{EventSource, EventType, NodeName};
use sinex_primitives::environment::{SinexEnvironment, environment};
use sinex_primitives::rpc::replay::ReplayGateOverrides;
use sinex_primitives::{Result, SinexError, Timestamp, Uuid};
use std::sync::Arc;
#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tracing::{error, info};

use super::validation::{
    ensure_replay_gates_pass, replay_scope_drift_error, stale_preview_missing_root_ids_error,
};

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
    pub(super) event_source: EventSource,
    pub(super) event_type: EventType,
    pub(super) has_lineage: bool,
    pub(super) scope_keys: Vec<String>,
}

impl ReplayExecutionEngine {
    #[cfg(not(test))]
    const EXECUTION_STATE_POLL_INTERVAL: Duration = Duration::from_millis(250);
    #[cfg(test)]
    const EXECUTION_STATE_POLL_INTERVAL: Duration = Duration::from_millis(25);

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
    pub(super) fn with_scan_completion_timeout(
        mut self,
        scan_completion_timeout: Duration,
    ) -> Self {
        self.scan_completion_timeout = scan_completion_timeout;
        self
    }

    #[cfg(test)]
    pub(super) fn with_checkpoint_failures(
        mut self,
        checkpoint_failures_remaining: Arc<AtomicUsize>,
    ) -> Self {
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

    #[cfg(test)]
    pub(super) async fn execute(
        &self,
        operation_id: Uuid,
        executor_name: String,
    ) -> Result<ReplayOperation> {
        self.execute_with_overrides(operation_id, executor_name, ReplayGateOverrides::default())
            .await
    }

    pub(super) async fn execute_with_overrides(
        &self,
        operation_id: Uuid,
        executor_name: String,
        gate_overrides: ReplayGateOverrides,
    ) -> Result<ReplayOperation> {
        let Some(_execution_lock) = self.replay.acquire_execution_lock(operation_id).await? else {
            return Err(SinexError::invalid_state(format!(
                "Operation {} is already executing on another node",
                operation_id
            )));
        };

        info!(
            operation_id = %operation_id,
            executor = %executor_name,
            "Starting replay execution"
        );

        let result = self
            .run_operation(operation_id, &executor_name, &gate_overrides)
            .await;
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
                Err(load_err) => Err(SinexError::service(format!(
                    "replay cancellation probe failed after execution error: {load_err}"
                ))
                .with_source(err)
                .with_source(load_err)),
            },
        }
    }

    pub(super) async fn submit_with_overrides(
        &self,
        operation_id: Uuid,
        submitter: String,
        gate_overrides: ReplayGateOverrides,
    ) -> Result<ReplayOperation> {
        let Some(_execution_lock) = self.replay.acquire_execution_lock(operation_id).await? else {
            return Err(SinexError::invalid_state(format!(
                "Operation {} is already executing on another node",
                operation_id
            )));
        };

        info!(
            operation_id = %operation_id,
            executor = %submitter,
            "Submitting replay preview for immediate execution"
        );

        let result = self
            .run_submitted_operation(operation_id, &submitter, &gate_overrides)
            .await;
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
                Err(load_err) => Err(SinexError::service(format!(
                    "replay cancellation probe failed after execution error: {load_err}"
                ))
                .with_source(err)
                .with_source(load_err)),
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
            .map_err(|err| {
                SinexError::service(format!(
                    "failed to inspect replay operation state after execution for {operation_id}"
                ))
                .with_source(err)
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
                .map_err(|err| {
                    SinexError::service(format!(
                        "failed to finalize replay cancellation for operation {operation_id}"
                    ))
                    .with_source(err)
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
                target: "sinex_metrics",
                metric = "gateway.replay_execution_failures_total",
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
                    target: "sinex_metrics",
                    metric = "gateway.replay_execution_failures_total",
                    operation_id = %operation_id,
                    mark_error = %mark_err,
                    execution_error = %err,
                    "OPERATOR ACTION REQUIRED: replay operation stuck in Executing state. \
                     Run: sinexctl replay cancel {operation_id} --reason 'stuck after mark_failed failure'"
                );
                return Err(SinexError::service(format!(
                    "Replay execution failed ({err:#}) and marking operation as failed also failed ({mark_err}); \
                     operation {operation_id} is stuck in Executing state"
                ))
                .with_source(err)
                .with_source(mark_err));
            }
        }

        Ok(())
    }

    pub(super) fn wrap_bookkeeping_error(
        err: SinexError,
        operation_id: Uuid,
        bookkeeping_error: Option<SinexError>,
    ) -> SinexError {
        match bookkeeping_error {
            Some(bookkeeping_error) => SinexError::service(format!(
                "failed to finalize replay execution bookkeeping for operation {operation_id}: {bookkeeping_error:#}"
            ))
            .with_source(err)
            .with_source(bookkeeping_error),
            None => err,
        }
    }

    pub(super) async fn run_operation(
        &self,
        operation_id: Uuid,
        executor_name: &str,
        gate_overrides: &ReplayGateOverrides,
    ) -> Result<ReplayOperation> {
        let (initial, total_events, execution_window, preview_root_ids) = self
            .prepare_operation(operation_id, executor_name, gate_overrides)
            .await?;

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
        gate_overrides: &ReplayGateOverrides,
    ) -> Result<ReplayOperation> {
        let (initial, total_events, execution_window, preview_root_ids) = self
            .prepare_submitted_operation(operation_id, submitter, gate_overrides)
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
        gate_overrides: &ReplayGateOverrides,
    ) -> Result<(ReplayOperation, u64, (Timestamp, Timestamp), Vec<Uuid>)> {
        let op = self.replay.load_operation(operation_id).await?;
        if op.state != ReplayState::Approved {
            return Err(SinexError::invalid_state(format!(
                "Operation {} must be approved before execution",
                operation_id
            ))
            .with_context("state", format!("{:?}", op.state)));
        }

        let preview = op.preview_summary.as_ref().ok_or_else(|| {
            SinexError::invalid_state(format!(
                "Operation {} is missing preview summary; run preview before execution",
                operation_id
            ))
        })?;
        ensure_replay_gates_pass(operation_id, preview, gate_overrides)?;

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
        gate_overrides: &ReplayGateOverrides,
    ) -> Result<(ReplayOperation, u64, (Timestamp, Timestamp), Vec<Uuid>)> {
        let pending = self.replay.load_operation(operation_id).await?;
        let preview = pending.preview_summary.as_ref().ok_or_else(|| {
            SinexError::invalid_state(format!(
                "Operation {} is missing preview summary; run preview before execution",
                operation_id
            ))
        })?;
        ensure_replay_gates_pass(operation_id, preview, gate_overrides)?;

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
            SinexError::invalid_state(format!(
                "Operation {} is missing preview summary; run preview before execution",
                operation_id
            ))
        })?;
        let preview_summary: ReplayPreviewSummary =
            serde_json::from_value(preview).map_err(|e| {
                SinexError::serialization("Invalid replay preview summary").with_std_error(&e)
            })?;
        let total_events = preview_summary.total_events;
        if total_events == 0 {
            return Err(SinexError::invalid_state(format!(
                "Operation {} preview matches zero events; refresh preview before execution",
                operation_id
            )));
        }
        let mut preview_root_ids = preview_summary.root_event_ids;
        preview_root_ids.sort_unstable();
        preview_root_ids.dedup();
        if preview_root_ids.is_empty() {
            return Err(stale_preview_missing_root_ids_error(operation_id, total_events).into());
        }
        if preview_root_ids.len() as u64 != total_events {
            return Err(SinexError::invalid_state(format!(
                "Operation {} preview summary is inconsistent: total_events={} but root_event_ids contains {} ids",
                operation_id,
                total_events,
                preview_root_ids.len()
            )));
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
                    .map_err(|err| {
                        SinexError::service("Failed to load replay operation after completion")
                            .with_source(err)
                    })
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
                    return Err(SinexError::service(format!("{checkpoint_error}"))
                        .with_source(err)
                        .with_source(checkpoint_error));
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
        result
            .as_ref()
            .is_err_and(|err| matches!(err, SinexError::Cancelled(_)))
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
            return Err(SinexError::processing(
                "forced replay checkpoint persistence failure",
            ));
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
        self.maybe_fail_checkpoint_persist()
            .map_err(|err| SinexError::service(context).with_source(err))?;
        self.replay
            .update_checkpoint(operation_id, checkpoint)
            .await
            .map_err(|err| SinexError::service(context).with_source(err))
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
            return Err(SinexError::processing(
                "forced replay scope metadata collection failure",
            ));
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
            return Err(SinexError::processing(
                "forced replay scope invalidation publish failure",
            ));
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
            return Err(SinexError::processing(
                "forced replay replacement recording failure",
            ));
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

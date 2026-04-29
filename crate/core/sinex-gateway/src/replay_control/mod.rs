#![doc = include_str!("../../docs/replay_control.md")]

mod client;
mod protocol;
mod telemetry;
mod validation;

#[cfg(test)]
mod tests;

pub use client::ReplayControlClient;
pub use protocol::{
    ReplayControlErrorKind, ReplayControlRequest, ReplayControlResponse, ReplayControlStatus,
};
pub use telemetry::ReplayTelemetrySnapshot;

use telemetry::ReplayTelemetry;

use async_nats::connection::State as NatsState;
use async_nats::{Client, Message, jetstream};
use color_eyre::eyre::{Context, Result, eyre};
use futures::StreamExt;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
pub use sinex_db::replay::state_machine::ReplayScope;
use sinex_db::replay::state_machine::{
    ReplayCheckpoint, ReplayOperation, ReplayState, ReplayStateMachine,
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
use sinex_primitives::nats::{NatsTrafficClass, insert_traffic_class_header};
use sinex_primitives::{Id, SinexError, Timestamp, Uuid};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::Semaphore;
use tokio::time::interval;
use tracing::{debug, error, info, warn};

use validation::{
    ReplayAction, ensure_preview_allowed, replay_scope_drift_error, run_safety_analysis,
    stale_preview_missing_root_ids_error, validate_actor_for_action,
};

const REPLAY_CONTROL_SUBSCRIBE_ATTEMPTS: usize = 5;
const REPLAY_CONTROL_SUBSCRIBE_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(2);
const REPLAY_CONTROL_SUBSCRIBE_BACKOFF_BASE: Duration = Duration::from_millis(200);
const REPLAY_CONTROL_SUBSCRIBE_BACKOFF_MAX: Duration = Duration::from_secs(2);
const REPLAY_OUTPUT_VISIBILITY_TIMEOUT: Duration = Duration::from_secs(30);


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayControlError {
    pub message: String,
    pub occurred_at: Timestamp,
}

impl ReplayControlError {
    pub(crate) fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            occurred_at: Timestamp::now(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayControlHealth {
    pub connected: bool,
    pub last_error: Option<ReplayControlError>,
}

#[derive(Debug, Default)]
pub(super) struct ReplayControlHealthState {
    pub(super) last_error: Option<ReplayControlError>,
    pub(super) server_subscribed: bool,
}

/// Spawn the replay control bus and return a client handle.
///
/// The replay control system manages distributed replay operations, coordinating
/// event re-processing across the cluster with proper state tracking and locking.
pub async fn spawn_replay_control(
    replay: Arc<ReplayStateMachine>,
    client: Client,
    request_timeout: Duration,
) -> Result<ReplayControlClient> {
    let env = environment().clone();
    let health = Arc::new(Mutex::new(ReplayControlHealthState::default()));

    // Create execution engine with NATS client for node-dispatch replay control
    let executor = ReplayExecutionEngine::new(replay.clone(), client.clone());
    ReplayTelemetry::new(replay.clone()).spawn();

    ReplayControlServer::new(&env, client.clone(), replay, executor, Arc::clone(&health))
        .spawn()
        .await?;

    Ok(ReplayControlClient::new(
        &env,
        client,
        request_timeout,
        health,
    ))
}


struct ReplayControlServer {
    subject: String,
    client: Client,
    replay: Arc<ReplayStateMachine>,
    executor: ReplayExecutionEngine,
    health: Arc<Mutex<ReplayControlHealthState>>,
}

impl ReplayControlServer {
    fn new(
        env: &SinexEnvironment,
        client: Client,
        replay: Arc<ReplayStateMachine>,
        executor: ReplayExecutionEngine,
        health: Arc<Mutex<ReplayControlHealthState>>,
    ) -> Self {
        let subject = env.nats_subject("sinex.control.replay");
        Self {
            subject,
            client,
            replay,
            executor,
            health,
        }
    }

    async fn spawn(self) -> Result<tokio::task::JoinHandle<()>> {
        let mut subscription = self.subscribe_with_backoff(false).await?;
        let client = self.client.clone();
        let subject = self.subject.clone();
        let replay = self.replay.clone();
        let executor = self.executor.clone();
        let health = Arc::clone(&self.health);
        let semaphore = Arc::new(Semaphore::new(4));

        let task = tokio::spawn(async move {
            'outer: loop {
                while let Some(message) = subscription.next().await {
                    // Acquire the permit BEFORE spawning so the receive loop
                    // applies backpressure to the subscription instead of
                    // letting unbounded spawn count pile up waiting on the
                    // semaphore inside spawned tasks.
                    let permit = match semaphore.clone().acquire_owned().await {
                        Ok(p) => p,
                        Err(_) => break 'outer, // semaphore closed (shutdown)
                    };
                    let client = client.clone();
                    let replay = replay.clone();
                    let executor = executor.clone();
                    tokio::spawn(async move {
                        let _permit = permit;
                        if let Err(err) =
                            Self::handle_message(&client, &replay, &executor, message).await
                        {
                            warn!(?err, "Replay control request failed");
                        }
                    });
                }

                Self::record_subscription_error(
                    &health,
                    "Replay control subscription closed; reconnecting",
                );
                warn!(
                    retry_delay_ms = REPLAY_CONTROL_SUBSCRIBE_BACKOFF_BASE.as_millis(),
                    "Replay control subscription closed; reconnecting"
                );

                loop {
                    match Self::subscribe_once(&client, &subject).await {
                        Ok(subscription_next) => {
                            Self::mark_server_subscribed(&health, true);
                            info!(subject = %subject, "Replay control server reconnected");
                            subscription = subscription_next;
                            continue 'outer;
                        }
                        Err(error) => {
                            Self::record_subscription_error(&health, error.to_string());
                            warn!(
                                error = %error,
                                backoff_ms = REPLAY_CONTROL_SUBSCRIBE_BACKOFF_BASE.as_millis(),
                                "Replay control subscription failed after startup; retrying"
                            );
                            tokio::time::sleep(REPLAY_CONTROL_SUBSCRIBE_BACKOFF_BASE).await;
                        }
                    }
                }
            }
        });

        Ok(task)
    }

    async fn subscribe_with_backoff(&self, reconnected: bool) -> Result<async_nats::Subscriber> {
        let mut backoff = REPLAY_CONTROL_SUBSCRIBE_BACKOFF_BASE;
        let mut attempt = 0usize;

        loop {
            attempt += 1;
            match Self::subscribe_once(&self.client, &self.subject).await {
                Ok(subscription) => {
                    Self::mark_server_subscribed(&self.health, true);
                    if reconnected {
                        info!(subject = %self.subject, "Replay control server reconnected");
                    } else {
                        info!(subject = %self.subject, "Replay control server subscribed to subject");
                    }
                    return Ok(subscription);
                }
                Err(err) => {
                    Self::record_subscription_error(&self.health, err.to_string());
                    if attempt >= REPLAY_CONTROL_SUBSCRIBE_ATTEMPTS {
                        return Err(err).wrap_err("Failed to subscribe to replay control subject");
                    }
                    warn!(
                        attempt,
                        backoff_ms = backoff.as_millis(),
                        error = %err,
                        "Replay control subscription failed; retrying"
                    );
                    tokio::time::sleep(backoff).await;
                    backoff = std::cmp::min(
                        backoff.saturating_mul(2),
                        REPLAY_CONTROL_SUBSCRIBE_BACKOFF_MAX,
                    );
                }
            }
        }
    }

    async fn subscribe_once(client: &Client, subject: &str) -> Result<async_nats::Subscriber> {
        match tokio::time::timeout(
            REPLAY_CONTROL_SUBSCRIBE_ATTEMPT_TIMEOUT,
            client.subscribe(subject.to_string()),
        )
        .await
        {
            Ok(Ok(subscription)) => Ok(subscription),
            Ok(Err(error)) => Err(error).wrap_err_with(|| {
                format!("failed to subscribe to replay control subject {subject}")
            }),
            Err(_) => Err(eyre!(
                "timed out subscribing to replay control subject {subject} after {:?}",
                REPLAY_CONTROL_SUBSCRIBE_ATTEMPT_TIMEOUT
            )),
        }
    }

    fn mark_server_subscribed(health: &Arc<Mutex<ReplayControlHealthState>>, subscribed: bool) {
        let mut guard = health.lock();
        guard.server_subscribed = subscribed;
    }

    fn record_subscription_error(
        health: &Arc<Mutex<ReplayControlHealthState>>,
        message: impl Into<String>,
    ) {
        let mut guard = health.lock();
        guard.server_subscribed = false;
        guard.last_error = Some(ReplayControlError::new(message));
    }

    async fn handle_message(
        client: &Client,
        replay: &Arc<ReplayStateMachine>,
        executor: &ReplayExecutionEngine,
        message: Message,
    ) -> Result<()> {
        // Parse the request — on failure, send an error response rather than returning
        // early without replying (which would cause the caller's request() to time out).
        let response = match serde_json::from_slice::<ReplayControlRequest>(&message.payload) {
            Ok(request) => match Self::process_request(replay, executor, request).await {
                Ok(response) => response,
                Err(err) => {
                    warn!(?err, "Replay control request failed");
                    ReplayControlResponse::from_report(&err)
                }
            },
            Err(e) => {
                warn!(error = %e, "Failed to parse replay control request");
                ReplayControlResponse::error(format!("Invalid request: {e}"))
            }
        };

        if let Some(reply_subject) = message.reply {
            match serde_json::to_vec(&response) {
                Ok(bytes) => {
                    let mut headers = async_nats::HeaderMap::new();
                    insert_traffic_class_header(&mut headers, NatsTrafficClass::Control);
                    if let Err(err) = client
                        .publish_with_headers(reply_subject, headers, bytes.into())
                        .await
                    {
                        error!(?err, "Failed to send replay control response");
                    }
                }
                Err(err) => {
                    error!(
                        ?err,
                        "Failed to serialize replay control response; reply not sent"
                    );
                }
            }
        }

        Ok(())
    }

    async fn process_request(
        replay: &Arc<ReplayStateMachine>,
        executor: &ReplayExecutionEngine,
        request: ReplayControlRequest,
    ) -> Result<ReplayControlResponse> {
        let response = match request {
            ReplayControlRequest::Plan { actor, scope } => {
                // Server-side validation of actor and scope (defense in depth).
                // The client also validates, but requests arrive over NATS and must
                // be trusted only after server-side re-validation.
                validate_actor_for_action(&actor, ReplayAction::Plan)?;
                scope.validate()?;

                let op = replay
                    .create_operation(scope.clone(), actor.clone())
                    .await?;
                ReplayControlResponse::success(Some(op), None, None)
            }
            ReplayControlRequest::Preview { operation_id } => {
                let operation = replay.load_operation(operation_id).await?;
                ensure_preview_allowed(&operation)?;
                let mut preview = replay.generate_preview_summary(&operation.scope).await?;

                // Augment preview with cascade safety analysis (integrity violations, cycles).
                let root_ids = serde_json::from_value::<ReplayPreviewSummary>(preview.clone())
                    .map(|summary| summary.root_event_ids)
                    .map_err(|e| eyre!("Invalid replay preview summary: {e}"))?;
                let safety = run_safety_analysis(replay.pool(), &root_ids).await;
                if let serde_json::Value::Object(ref mut map) = preview {
                    map.insert("safety_analysis".to_string(), safety);
                }

                replay.update_preview(operation_id, preview.clone()).await?;
                let updated = replay.load_operation(operation_id).await?;
                ReplayControlResponse::success(Some(updated), Some(preview), None)
            }
            ReplayControlRequest::Approve {
                operation_id,
                approver,
            } => {
                // Server-side validation of approver (defense in depth)
                validate_actor_for_action(&approver, ReplayAction::Approve)?;

                replay.approve(operation_id, approver).await?;
                let updated = replay.load_operation(operation_id).await?;
                ReplayControlResponse::success(Some(updated), None, None)
            }
            ReplayControlRequest::Submit {
                operation_id,
                submitter,
            } => {
                validate_actor_for_action(&submitter, ReplayAction::Approve)?;
                validate_actor_for_action(&submitter, ReplayAction::Execute)?;

                let updated = executor.submit(operation_id, submitter).await?;
                ReplayControlResponse::success(Some(updated), None, None)
            }
            ReplayControlRequest::Execute {
                operation_id,
                executor: actor,
                dry_run,
            } => {
                // Server-side validation of executor (defense in depth)
                validate_actor_for_action(&actor, ReplayAction::Execute)?;

                if dry_run {
                    return Err(eyre!(
                        "Replay execute does not support dry-run semantics; use preview before approval instead"
                    ));
                }
                let updated = executor.execute(operation_id, actor).await?;
                ReplayControlResponse::success(Some(updated), None, None)
            }
            ReplayControlRequest::Cancel {
                operation_id,
                canceller,
                reason,
            } => {
                validate_actor_for_action(&canceller, ReplayAction::Cancel)?;
                replay
                    .cancel(
                        operation_id,
                        reason
                            .unwrap_or_else(|| format!("Cancelled by {canceller} via control bus")),
                    )
                    .await?;
                let updated = replay.load_operation(operation_id).await?;
                ReplayControlResponse::success(Some(updated), None, None)
            }
            ReplayControlRequest::Status { operation_id } => {
                let op = replay.load_operation(operation_id).await?;
                ReplayControlResponse::success(Some(op), None, None)
            }
            ReplayControlRequest::List { state, node, limit } => {
                let ops = replay
                    .list_operations(state, node.as_deref(), limit)
                    .await?;
                ReplayControlResponse::success(None, None, Some(ops))
            }
        };

        Ok(response)
    }
}

/// Engine responsible for executing replay operations.
///
/// The execution engine:
/// 1. Queries events from the database matching the replay scope
/// 2. Expands and archives the full affected cascade (live -> archive)
/// 3. Dispatches a scan command to the target ingestor node via NATS
/// 4. The node re-reads source material and emits fresh events through normal flow
/// 5. Tracks progress via checkpoints and NATS progress messages
#[derive(Clone)]
struct ReplayExecutionEngine {
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
struct ScopeInvalidationBucket {
    event_ids: Vec<Uuid>,
    event_source: String,
    event_type: String,
    has_lineage: bool,
    scope_keys: Vec<String>,
}

impl ReplayExecutionEngine {
    const EXECUTION_STATE_POLL_INTERVAL: Duration = Duration::from_millis(250);

    fn new(replay: Arc<ReplayStateMachine>, nats_client: Client) -> Self {
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
    fn with_scan_ack_timeout(mut self, scan_ack_timeout: Duration) -> Self {
        self.scan_ack_timeout = scan_ack_timeout;
        self
    }

    #[cfg(test)]
    fn with_scan_completion_timeout(mut self, scan_completion_timeout: Duration) -> Self {
        self.scan_completion_timeout = scan_completion_timeout;
        self
    }

    #[cfg(test)]
    fn with_checkpoint_failures(mut self, checkpoint_failures_remaining: Arc<AtomicUsize>) -> Self {
        self.checkpoint_failures_remaining = Some(checkpoint_failures_remaining);
        self
    }

    #[cfg(test)]
    fn with_scope_metadata_failures(
        mut self,
        scope_metadata_failures_remaining: Arc<AtomicUsize>,
    ) -> Self {
        self.scope_metadata_failures_remaining = Some(scope_metadata_failures_remaining);
        self
    }

    #[cfg(test)]
    fn with_scope_invalidation_publish_failures(
        mut self,
        scope_invalidation_publish_failures_remaining: Arc<AtomicUsize>,
    ) -> Self {
        self.scope_invalidation_publish_failures_remaining =
            Some(scope_invalidation_publish_failures_remaining);
        self
    }

    #[cfg(test)]
    fn with_replacement_record_failures(
        mut self,
        replacement_record_failures_remaining: Arc<AtomicUsize>,
    ) -> Self {
        self.replacement_record_failures_remaining = Some(replacement_record_failures_remaining);
        self
    }

    async fn execute(&self, operation_id: Uuid, executor_name: String) -> Result<ReplayOperation> {
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

    async fn submit(&self, operation_id: Uuid, submitter: String) -> Result<ReplayOperation> {
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

    async fn handle_execution_finish(
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

    fn wrap_bookkeeping_error(
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

    async fn run_operation(
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

    async fn run_submitted_operation(
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

    async fn prepare_operation(
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

    async fn prepare_submitted_operation(
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

    fn execution_inputs_from_operation(
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

    async fn finalize_operation(
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

    async fn load_cancelled_operation(
        &self,
        operation_id: Uuid,
    ) -> Result<Option<ReplayOperation>> {
        let operation = self.replay.load_operation(operation_id).await?;
        Ok((operation.state == ReplayState::Cancelled).then_some(operation))
    }

    fn execution_result_is_cancellation(result: &Result<ReplayOperation>) -> bool {
        result.as_ref().is_err_and(|err| {
            err.downcast_ref::<SinexError>()
                .is_some_and(|sinex_err| matches!(sinex_err, SinexError::Cancelled(_)))
        })
    }

    #[cfg(test)]
    fn maybe_fail_checkpoint_persist(&self) -> Result<()> {
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
    fn maybe_fail_checkpoint_persist(&self) -> Result<()> {
        Ok(())
    }

    async fn persist_replay_checkpoint(
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
    fn maybe_fail_scope_metadata_collection(&self) -> Result<()> {
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
    fn maybe_fail_scope_metadata_collection(&self) -> Result<()> {
        Ok(())
    }

    #[cfg(test)]
    fn maybe_fail_scope_invalidation_publish(&self) -> Result<()> {
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
    fn maybe_fail_scope_invalidation_publish(&self) -> Result<()> {
        Ok(())
    }

    #[cfg(test)]
    fn maybe_fail_replacement_recording(&self) -> Result<()> {
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
    fn maybe_fail_replacement_recording(&self) -> Result<()> {
        Ok(())
    }

    async fn collect_scope_events(
        &self,
        scope: &ReplayScope,
        _execution_window: (Timestamp, Timestamp),
        pool: &sqlx::PgPool,
    ) -> Result<Vec<StoredEvent>> {
        let root_ids = self
            .replay
            .collect_scope_root_ids(scope)
            .await
            .map_err(|e| eyre!("Failed to collect replay scope root ids: {e}"))?;
        let event_ids: Vec<Id<StoredEvent>> = root_ids
            .into_iter()
            .map(Id::<StoredEvent>::from_uuid)
            .collect();

        // get_by_ids silently clamps to 1000; chunk to avoid the truncation.
        const CHUNK_SIZE: usize = 1000;
        if event_ids.len() <= CHUNK_SIZE {
            return pool
                .events()
                .get_by_ids(&event_ids)
                .await
                .map_err(|e| eyre!("Failed to hydrate replay scope events: {e}"));
        }

        let mut all_events = Vec::with_capacity(event_ids.len());
        for chunk in event_ids.chunks(CHUNK_SIZE) {
            let chunk_events = pool
                .events()
                .get_by_ids(chunk)
                .await
                .map_err(|e| eyre!("Failed to hydrate replay scope events (chunk): {e}"))?;
            all_events.extend(chunk_events);
        }
        Ok(all_events)
    }

    async fn collect_operation_output_events(
        &self,
        pool: &sqlx::PgPool,
        operation_id: Uuid,
    ) -> Result<Vec<OperationOutputEvent>> {
        let rows = sqlx::query!(
            r#"
            SELECT
                id AS "id!",
                equivalence_key
            FROM core.events
            WHERE created_by_operation_id = $1::uuid
            ORDER BY id
            "#,
            operation_id,
        )
        .fetch_all(pool)
        .await
        .map_err(|e| eyre!("Failed to query replay operation outputs: {e}"))?;

        Ok(rows
            .into_iter()
            .map(|row| OperationOutputEvent {
                id: row.id,
                equivalence_key: row.equivalence_key,
            })
            .collect())
    }

    fn expected_replay_outputs(material_roots: &[StoredEvent]) -> Result<ExpectedReplayOutputs> {
        if material_roots.is_empty() {
            return Err(eyre!(
                "Replay output expectations require at least one material root"
            ));
        }

        let mut sources = HashSet::new();
        let mut event_types = HashSet::new();

        for event in material_roots {
            sources.insert(event.source.as_ref().to_string());
            event_types.insert(event.event_type.as_ref().to_string());
            match &event.provenance {
                Provenance::Material { .. } => {}
                Provenance::Synthesis { .. } => {
                    return Err(eyre!(
                        "Replay scope included non-material root '{}' / '{}'",
                        event.source,
                        event.event_type
                    ));
                }
            }
        }

        let mut sources: Vec<_> = sources.into_iter().collect();
        sources.sort_unstable();
        let mut event_types: Vec<_> = event_types.into_iter().collect();
        event_types.sort_unstable();

        Ok(ExpectedReplayOutputs {
            minimum_visible_count: 0,
            sources,
            event_types,
            logical_source_identifiers: Vec::new(),
        })
    }

    fn logical_source_identifier(material: &ResolvedReplayMaterial) -> &str {
        material
            .material_metadata
            .get("logical_source_identifier")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_else(|| {
                material
                    .source_identifier
                    .split("#material=")
                    .next()
                    .unwrap_or(material.source_identifier.as_str())
            })
    }

    fn with_logical_source_identifiers(
        mut expected: ExpectedReplayOutputs,
        replay_materials: &[ResolvedReplayMaterial],
    ) -> Result<ExpectedReplayOutputs> {
        let mut logical_source_identifiers = replay_materials
            .iter()
            .map(Self::logical_source_identifier)
            .map(str::to_owned)
            .collect::<Vec<_>>();
        logical_source_identifiers.sort_unstable();
        logical_source_identifiers.dedup();

        if logical_source_identifiers.is_empty() {
            return Err(eyre!(
                "Replay output expectations require at least one logical source identifier"
            ));
        }

        expected.minimum_visible_count = logical_source_identifiers.len() as u64;
        expected.logical_source_identifiers = logical_source_identifiers;
        Ok(expected)
    }

    async fn count_visible_replay_outputs(
        &self,
        pool: &sqlx::PgPool,
        operation_id: Uuid,
        expected: &ExpectedReplayOutputs,
    ) -> Result<i64> {
        sqlx::query_scalar::<_, i64>(
            r"
            SELECT COUNT(DISTINCT COALESCE(
                    smr.metadata->>'logical_source_identifier',
                    split_part(smr.source_identifier, '#material=', 1)
                  ))::bigint
            FROM core.events
            INNER JOIN raw.source_material_registry smr
                ON smr.id = core.events.source_material_id
            WHERE created_by_operation_id = $1::uuid
              AND source = ANY($2::text[])
              AND event_type = ANY($3::text[])
              AND COALESCE(
                    smr.metadata->>'logical_source_identifier',
                    split_part(smr.source_identifier, '#material=', 1)
                  ) = ANY($4::text[])
            ",
        )
        .bind(operation_id)
        .bind(&expected.sources)
        .bind(&expected.event_types)
        .bind(&expected.logical_source_identifiers)
        .fetch_one(pool)
        .await
        .map_err(|e| eyre!("Failed to count visible replay outputs: {e}"))
    }

    async fn wait_for_replay_outputs_visible(
        &self,
        pool: &sqlx::PgPool,
        operation_id: Uuid,
        expected: &ExpectedReplayOutputs,
    ) -> Result<()> {
        let timeout = self
            .scan_completion_timeout
            .min(REPLAY_OUTPUT_VISIBILITY_TIMEOUT);

        let wait_result = tokio::time::timeout(timeout, async {
            loop {
                let visible_count = self
                    .count_visible_replay_outputs(pool, operation_id, expected)
                    .await?;
                if visible_count >= expected.minimum_visible_count as i64 {
                    debug!(
                        operation_id = %operation_id,
                        visible_count,
                        minimum_visible_count = expected.minimum_visible_count,
                        "Replay outputs are query-visible"
                    );
                    return Ok::<(), color_eyre::eyre::Report>(());
                }

                tokio::time::sleep(Self::EXECUTION_STATE_POLL_INTERVAL).await;
            }
        })
        .await;

        match wait_result {
            Ok(result) => result,
            Err(_timeout) => {
                let visible_count = self
                    .count_visible_replay_outputs(pool, operation_id, expected)
                    .await
                    .unwrap_or(-1);
                Err(eyre!(
                    "Replay outputs were not query-visible after successful scan within {:?} (visible={}, minimum_visible={}, sources={}, event_types={}, logical_sources={})",
                    timeout,
                    visible_count,
                    expected.minimum_visible_count,
                    expected.sources.join(","),
                    expected.event_types.join(","),
                    expected.logical_source_identifiers.join(","),
                ))
            }
        }
    }

    async fn resolve_replay_materials(
        &self,
        pool: &sqlx::PgPool,
        material_ids: &[Uuid],
    ) -> Result<Vec<ResolvedReplayMaterial>> {
        let mut resolved = Vec::with_capacity(material_ids.len());
        let mut missing = Vec::new();

        for material_id in material_ids {
            let record = pool
                .source_materials()
                .get_by_id(Id::from_uuid(*material_id))
                .await
                .map_err(|e| eyre!("{e}"))
                .wrap_err("Failed to resolve source material for replay")?;

            match record {
                Some(record) => resolved.push(ResolvedReplayMaterial::from(record)),
                None => missing.push(*material_id),
            }
        }

        if !missing.is_empty() {
            return Err(eyre!(
                "Replay scope referenced missing source materials: {}",
                missing
                    .iter()
                    .map(Uuid::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }

        Ok(resolved)
    }

    async fn derive_cascade_ids(
        &self,
        pool: &sqlx::PgPool,
        operation_id: Uuid,
        root_ids: &[Uuid],
    ) -> Result<Vec<Uuid>> {
        let mut tx = pool
            .begin()
            .await
            .wrap_err("Failed to begin transaction for cascade expansion")?;
        let mut repo_tx = EventRepositoryTx::new(&mut tx);
        let session_id = format!("replay_{}", operation_id.simple());

        let table_name = repo_tx
            .prepare_cascade_session(&session_id, false)
            .await
            .map_err(|e| eyre!("{e}"))
            .wrap_err("Failed to prepare replay cascade session")?;
        repo_tx
            .populate_cascade_roots(&table_name, root_ids)
            .await
            .map_err(|e| eyre!("{e}"))
            .wrap_err("Failed to populate replay cascade roots")?;
        repo_tx
            .expand_cascade(
                &table_name,
                i32::try_from(crate::cascade_analyzer::DEFAULT_CASCADE_MAX_DEPTH)
                    .unwrap_or(i32::MAX),
            )
            .await
            .map_err(|e| eyre!("{e}"))
            .wrap_err("Failed to expand replay cascade")?;

        let mut cascade_ids: Vec<Uuid> = repo_tx
            .get_event_dependencies(&table_name)
            .await
            .map_err(|e| eyre!("{e}"))
            .wrap_err("Failed to read replay cascade members")?
            .into_iter()
            .map(|(event_id, _)| event_id)
            .collect();

        repo_tx
            .cleanup_cascade_session(&table_name)
            .await
            .map_err(|e| eyre!("{e}"))
            .wrap_err("Failed to cleanup replay cascade session")?;
        tx.commit()
            .await
            .wrap_err("Failed to commit replay cascade transaction")?;

        cascade_ids.sort_unstable();
        cascade_ids.dedup();
        Ok(cascade_ids)
    }

    async fn archive_cascade(
        &self,
        pool: &sqlx::PgPool,
        cascade_ids: &[Uuid],
        operation_id: Uuid,
        archived_by: &str,
    ) -> Result<u64> {
        if cascade_ids.is_empty() {
            return Ok(0);
        }

        pool.events()
            .execute_cascade_archive(
                cascade_ids,
                "superseded by replay re-execution",
                &operation_id.to_string(),
                archived_by,
            )
            .await
            .map_err(|e| eyre!("{e}"))
            .wrap_err("Failed to archive replay cascade")
    }

    /// Collect scope metadata from events about to be archived.
    ///
    /// Returns `(event_type, scope_keys)` pairs grouped by `event_type`.
    /// Called before `archive_cascade` so we can emit invalidation signals after.
    async fn collect_cascade_scope_metadata(
        &self,
        pool: &sqlx::PgPool,
        cascade_ids: &[Uuid],
    ) -> Result<Vec<ScopeInvalidationBucket>> {
        if cascade_ids.is_empty() {
            return Ok(Vec::new());
        }

        self.maybe_fail_scope_metadata_collection()
            .wrap_err("Failed to collect replay cascade scope metadata")?;

        // Query scope metadata for cascade events that have scope_keys so invalidations
        // stay bucketed by the archived event source + type pair.
        let rows = sqlx::query!(
            "SELECT id, source, event_type, scope_key, \
                    (source_event_ids IS NOT NULL) AS \"has_lineage!: bool\" \
             FROM core.events \
             WHERE id = ANY($1::uuid[]) AND scope_key IS NOT NULL",
            cascade_ids,
        )
        .fetch_all(pool)
        .await
        .map_err(|e| eyre!("Failed to collect cascade scope metadata: {e}"))?;

        let mut grouped: HashMap<(String, String, bool), ScopeInvalidationBucket> = HashMap::new();
        for row in rows {
            if let Some(sk) = row.scope_key {
                let bucket = grouped
                    .entry((row.source.clone(), row.event_type.clone(), row.has_lineage))
                    .or_insert_with(|| ScopeInvalidationBucket {
                        event_ids: Vec::new(),
                        event_source: row.source.clone(),
                        event_type: row.event_type.clone(),
                        has_lineage: row.has_lineage,
                        scope_keys: Vec::new(),
                    });
                bucket.event_ids.push(row.id);
                bucket.scope_keys.push(sk);
            }
        }

        for bucket in grouped.values_mut() {
            bucket.event_ids.sort_unstable();
            bucket.event_ids.dedup();
            bucket.scope_keys.sort_unstable();
            bucket.scope_keys.dedup();
        }

        Ok(grouped.into_values().collect())
    }

    /// Publish scope invalidation signals for archived events.
    ///
    /// Notifies derived nodes that scopes need recomputation because events
    /// were archived. Only publishes for events that had `scope_keys`.
    async fn publish_scope_invalidations(
        &self,
        scope_metadata: &[ScopeInvalidationBucket],
        operation_id: Uuid,
    ) -> Result<()> {
        if scope_metadata.is_empty() {
            return Ok(());
        }

        let invalidation_subject = self.env.nats_subject(INVALIDATION_SUBJECT);

        for bucket in scope_metadata {
            let event_source = match EventSource::new(bucket.event_source.clone()) {
                Ok(source) => source,
                Err(error) => {
                    return Err(eyre!(
                        "Failed to build replay scope invalidation for archived event source '{}': {error}",
                        bucket.event_source
                    ));
                }
            };
            let event_type = match EventType::new(bucket.event_type.clone()) {
                Ok(event_type) => event_type,
                Err(error) => {
                    return Err(eyre!(
                        "Failed to build replay scope invalidation for archived event type '{}' (scope_count={}): {error}",
                        bucket.event_type,
                        bucket.scope_keys.len()
                    ));
                }
            };
            let invalidation = DerivedScopeInvalidation::archived(
                bucket.event_ids.clone(),
                event_source.clone(),
                event_type.clone(),
            )
            .with_has_lineage(bucket.has_lineage)
            .with_operation(operation_id)
            .with_scope_keys(bucket.scope_keys.clone());

            match serde_json::to_vec(&invalidation) {
                Ok(payload) => {
                    self.maybe_fail_scope_invalidation_publish()?;
                    // transport::Class::Invalidation — JetStream-backed scope
                    // fan-out; failure propagated to caller (replay operation
                    // decides abort/continue). No Sinex-Traffic-Class header on
                    // the plain js.publish path (no header map variant here).
                    if let Err(e) = self
                        .js
                        .publish(
                            invalidation_subject.clone(),
                            payload.into(),
                        )
                        .await
                    {
                        return Err(eyre!(
                            "Failed to publish replay scope invalidation for event type '{}' (scope_count={}): {e}",
                            event_type,
                            bucket.scope_keys.len()
                        ));
                    }
                    debug!(
                        operation_id = %operation_id,
                        event_type = %event_type,
                        scope_count = bucket.scope_keys.len(),
                        "Published scope invalidation"
                    );
                }
                Err(e) => {
                    return Err(eyre!(
                        "Failed to serialize replay scope invalidation for event type '{}' (scope_count={}): {e}",
                        event_type,
                        bucket.scope_keys.len()
                    ));
                }
            }
        }

        Ok(())
    }

    async fn restore_cascade(
        &self,
        pool: &sqlx::PgPool,
        cascade_ids: &[Uuid],
        operation_id: Uuid,
    ) -> Result<()> {
        if cascade_ids.is_empty() {
            return Ok(());
        }

        pool.events()
            .execute_cascade_restore(cascade_ids, &operation_id.to_string())
            .await
            .map_err(|e| eyre!("{e}"))
            .wrap_err("Failed to restore archived replay cascade after replay dispatch failure")?;
        Ok(())
    }

    async fn abort_before_scan_ack(
        &self,
        pool: &sqlx::PgPool,
        cascade_ids: &[Uuid],
        scope_metadata: &[ScopeInvalidationBucket],
        operation_id: Uuid,
        error: color_eyre::eyre::Report,
    ) -> Result<u64> {
        if let Err(restore_error) = self.restore_cascade(pool, cascade_ids, operation_id).await {
            return Err(error.wrap_err(format!(
                "Replay dispatch failed before node acknowledgement, and restoring the archived cascade also failed: {restore_error}"
            )));
        }

        if let Err(invalidation_error) = self
            .publish_scope_invalidations(scope_metadata, operation_id)
            .await
        {
            return Err(error.wrap_err(format!(
                "Replay dispatch failed before node acknowledgement, restored the archived cascade, but failed to publish compensating scope invalidations: {invalidation_error}"
            )));
        }

        Err(error.wrap_err(
            "Replay dispatch failed before node acknowledgement; restored archived cascade and published compensating scope invalidations",
        ))
    }

    /// Timeout for the node to acknowledge the scan command.
    const SCAN_ACK_TIMEOUT: Duration = Duration::from_secs(10);
    /// Timeout for the entire scan operation to complete.
    const SCAN_COMPLETION_TIMEOUT: Duration = Duration::from_mins(10);

    /// Record replacement relations between archived (old) events and newly-created events.
    ///
    /// After a successful replay scan, this queries for:
    /// - Old events: from `audit.archived_events` matching `cascade_ids`
    /// - New events: from `core.events` with `created_by_operation_id = operation_id`
    ///
    /// Matching strategy: events sharing the same `equivalence_key` are `Superseded`.
    /// Unmatched archived events are left without a replacement relation rather than
    /// fabricating a false old→new lineage edge.
    async fn record_event_replacements(
        &self,
        pool: &sqlx::PgPool,
        operation_id: Uuid,
        cascade_ids: &[Uuid],
    ) -> Result<()> {
        use sinex_db::repositories::{ReplacementKind, ReplacementRecord};

        if cascade_ids.is_empty() {
            return Ok(());
        }

        // Query equivalence_key + scope_key for archived old events
        let old_rows = sqlx::query!(
            r#"SELECT id as "id!", scope_key, equivalence_key
             FROM audit.archived_events WHERE id = ANY($1::uuid[])"#,
            cascade_ids,
        )
        .fetch_all(pool)
        .await
        .map_err(|e| eyre!("Failed to query archived events for replacement matching: {e}"))?;

        // Query the actual events emitted by this replay operation. Re-querying
        // the original scope window can miss replacements or bind unrelated
        // live rows once the replay finishes.
        let new_events = self
            .collect_operation_output_events(pool, operation_id)
            .await?;

        if new_events.is_empty() {
            debug!(
                operation_id = %operation_id,
                old_count = old_rows.len(),
                "No new events found after replay scan — skipping replacement recording"
            );
            return Ok(());
        }

        // Build equivalence_key → new_event_ids index, preserving all outputs
        // with the same key (e.g. deterministic re-runs that produce two events
        // with the same equivalence_key must all be recorded, not collapsed).
        let mut eq_key_to_new: HashMap<String, Vec<Uuid>> = HashMap::new();
        for event in &new_events {
            if let Some(ref eq_key) = event.equivalence_key {
                eq_key_to_new
                    .entry(eq_key.clone())
                    .or_default()
                    .push(event.id);
            }
        }

        // Build replacement records
        let mut replacements = Vec::with_capacity(old_rows.len());
        let mut unmatched_count = 0usize;
        for row in &old_rows {
            let Some(new_event_ids) = row
                .equivalence_key
                .as_ref()
                .and_then(|eq| eq_key_to_new.get(eq))
            else {
                unmatched_count += 1;
                continue;
            };

            for &new_event_id in new_event_ids {
                replacements.push(ReplacementRecord {
                    old_event_id: row.id,
                    new_event_id,
                    relation_kind: ReplacementKind::Superseded,
                    scope_key: row.scope_key.clone(),
                    equivalence_key: row.equivalence_key.clone(),
                });
            }
        }

        if unmatched_count > 0 {
            warn!(
                operation_id = %operation_id,
                unmatched_count,
                old_count = old_rows.len(),
                new_count = new_events.len(),
                "Skipped replay replacement records without an equivalence-key match"
            );
        }

        if replacements.is_empty() {
            debug!(
                operation_id = %operation_id,
                old_count = old_rows.len(),
                new_count = new_events.len(),
                "No replay replacement matches found — skipping replacement recording"
            );
            return Ok(());
        }

        self.maybe_fail_replacement_recording()
            .wrap_err("Failed to record replay replacement relations")?;

        let count = pool
            .events()
            .record_replacements(operation_id, &replacements)
            .await
            .map_err(|e| eyre!("{e}"))
            .wrap_err("Failed to record replay replacement relations")?;

        info!(
            operation_id = %operation_id,
            replacement_count = count,
            old_events = old_rows.len(),
            new_events = new_events.len(),
            "Recorded event replacement relations"
        );

        Ok(())
    }

    /// Dispatch a replay by telling the ingestor node to re-scan source material.
    ///
    /// Instead of republishing stored event rows to NATS (reinjection), this:
    /// 1. Archives the affected cascade (existing events + derivatives)
    /// 2. Sends a `NodeScanCommand` to the running ingestor via NATS request-reply
    /// 3. Waits for the node to acknowledge and complete the scan
    /// 4. The node re-reads source material and emits fresh events through normal flow
    /// 5. Downstream automatons process the new events naturally via `JetStream`
    ///
    /// ## Transaction-boundary note (known-accepted race window)
    ///
    /// The cascade expansion (`derive_cascade_ids`) and the archive
    /// (`archive_cascade`) execute in **separate** database transactions.
    /// Between them, a newly-arriving event can reference an event that is
    /// about to be archived, creating a dangling `source_event_ids` reference.
    ///
    /// This window **cannot be closed** without a distributed-transaction
    /// protocol (2PC): steps after the archive publish invalidation signals
    /// and dispatch scan commands via NATS, which sit outside the database.
    /// Holding a DB transaction open across NATS request-reply would block
    /// the connection pool and risk indefinite locks on `core.events`.
    ///
    /// Mitigations that make this safe in practice:
    /// - `abort_before_scan_ack` restores the cascade and emits compensating
    ///   invalidations when the invalidation-publish or scan-command steps fail.
    /// - The cascade analyzer's integrity-violation check (`cascade_analyzer.rs`)
    ///   catches dangling references before the next replay of the same scope,
    ///   so the race is detectable and self-healing rather than silent.
    ///   so the window is narrow and the blast radius (one dangling reference
    ///   per replay) is negligible.
    async fn replay_events(
        &self,
        operation_id: Uuid,
        scope: &ReplayScope,
        execution_window: (Timestamp, Timestamp),
        expected_total_events: u64,
        preview_root_ids: &[Uuid],
        pool: &sqlx::PgPool,
        checkpoint: &mut ReplayCheckpoint,
        executor_name: &str,
    ) -> Result<u64> {
        let material_roots = self
            .collect_scope_events(scope, execution_window, pool)
            .await?;
        if material_roots.is_empty() {
            return Err(eyre!(
                "Replay scope matched zero live events at execution time; preview is stale or the scoped rows were already replaced"
            ));
        }

        let mut root_ids: Vec<Uuid> = material_roots
            .iter()
            .filter_map(|event| event.id.map(|id| *id.as_uuid()))
            .collect();
        if root_ids.is_empty() {
            return Err(eyre!(
                "Replay scope material roots are missing persistent event ids"
            ));
        }
        root_ids.sort_unstable();
        root_ids.dedup();

        if preview_root_ids.is_empty() {
            // Stale preview: root_event_ids not available. Require a fresh preview
            // to enable ID-level staleness detection.
            return Err(stale_preview_missing_root_ids_error(
                operation_id,
                expected_total_events,
            ));
        }
        if root_ids.as_slice() != preview_root_ids {
            return Err(replay_scope_drift_error(
                operation_id,
                expected_total_events,
                preview_root_ids,
                &root_ids,
            ));
        }

        let normalized = scope.normalized_filters();
        let material_ids: Vec<Uuid> = material_roots
            .iter()
            .filter_map(|event| match &event.provenance {
                Provenance::Material { id, .. } => Some(*id.as_uuid()),
                _ => None,
            })
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        let replay_materials = self.resolve_replay_materials(pool, &material_ids).await?;
        let expected_replay_outputs = Self::with_logical_source_identifiers(
            Self::expected_replay_outputs(&material_roots)?,
            &replay_materials,
        )?;

        // Step 1: Archive the affected cascade
        let cascade_ids = self
            .derive_cascade_ids(pool, operation_id, &root_ids)
            .await?;

        // Collect scope metadata before archiving (events move to audit after)
        let scope_metadata = self
            .collect_cascade_scope_metadata(pool, &cascade_ids)
            .await?;

        let archived_count = self
            .archive_cascade(pool, &cascade_ids, operation_id, executor_name)
            .await?;
        info!(
            operation_id = %operation_id,
            material_roots = material_roots.len(),
            archived_count,
            "Archived replay cascade, dispatching scan to node"
        );

        // TODO(#554): transactional outbox — archive_cascade commits to the DB above, but
        // publish_scope_invalidations below is a separate NATS operation with no transactional
        // coupling. If the process crashes between these two points, the archive is durable but
        // the scope invalidation signals are permanently lost, leaving derived nodes with stale
        // cached state until the next replay or manual reconciliation. A transactional outbox
        // (write invalidation rows inside the archive TX, publish-and-delete after commit with
        // retry on failure) would close this gap. For now, `abort_before_scan_ack` handles the
        // "process survives but NATS fails" case by restoring the cascade; crash recovery is not
        // covered.

        // Publish scope invalidation signals for archived derived events
        if !scope_metadata.is_empty()
            && let Err(invalidation_error) = self
                .publish_scope_invalidations(&scope_metadata, operation_id)
                .await
        {
            error!(
                operation_id = %operation_id,
                archived_count,
                scope_buckets = scope_metadata.len(),
                "Replay scope invalidation publish failed after archive commit; restoring cascade: {invalidation_error}"
            );
            return self
                    .abort_before_scan_ack(
                        pool,
                        &cascade_ids,
                        &scope_metadata,
                        operation_id,
                        eyre!(
                            "Failed to publish replay scope invalidations before dispatch: {invalidation_error}"
                        ),
                    )
                    .await;
        }

        checkpoint.total_events = material_roots.len() as u64;

        // Step 2: Build and send the scan command to the ingestor node
        let scan_subject = self
            .env
            .nats_subject(&format!("sinex.control.nodes.{}.scan", scope.node_id));
        let progress_subject = self
            .env
            .nats_subject(&format!("sinex.control.replay.progress.{operation_id}"));

        let mut progress_sub = match self.nats_client.subscribe(progress_subject.clone()).await {
            Ok(subscription) => subscription,
            Err(error) => {
                return self
                    .abort_before_scan_ack(
                        pool,
                        &cascade_ids,
                        &scope_metadata,
                        operation_id,
                        eyre!("Failed to subscribe to replay progress: {error}"),
                    )
                    .await;
            }
        };

        // Build MaterialReplayContext so the node knows this is a replay scan
        let replay_context = MaterialReplayContext {
            operation_id,
            materials: replay_materials,
            replay_scope: NodeReplayScopeFilters {
                material_ids: normalized.material_ids,
                event_types: normalized.event_types,
            },
        };

        let scan_command = NodeScanCommand {
            operation_id,
            from: Checkpoint::None,
            until: TimeHorizon::Historical {
                end_time: execution_window.1,
            },
            args: ScanArgs {
                targets: vec![scope.node_id.clone()],
                dry_run: false,
                interactive: false,
                max_events: 0,
                skip_duplicates: true,
                config: HashMap::new(),
                replay: Some(replay_context),
            },
        };

        let command_payload = serde_json::to_vec(&scan_command)
            .map_err(|e| eyre!("Failed to serialize NodeScanCommand: {e}"))?;

        // Step 3: Send via NATS request-reply and wait for acknowledgement
        let ack_msg = match tokio::time::timeout(
            self.scan_ack_timeout,
            self.nats_client
                .request(scan_subject.clone(), command_payload.into()),
        )
        .await
        {
            Ok(Ok(message)) => message,
            Ok(Err(error)) => {
                return self
                    .abort_before_scan_ack(
                        pool,
                        &cascade_ids,
                        &scope_metadata,
                        operation_id,
                        eyre!("NATS request to {} failed: {error}", scan_subject),
                    )
                    .await;
            }
            Err(_) => {
                return self
                    .abort_before_scan_ack(
                        pool,
                        &cascade_ids,
                        &scope_metadata,
                        operation_id,
                        eyre!(
                            "Timed out waiting for scan ack from node '{}' after {:?}. Is the node running?",
                            scope.node_id,
                            self.scan_ack_timeout
                        ),
                    )
                    .await;
            }
        };

        let ack: NodeScanAck = match serde_json::from_slice(&ack_msg.payload) {
            Ok(ack) => ack,
            Err(error) => {
                return self
                    .abort_before_scan_ack(
                        pool,
                        &cascade_ids,
                        &scope_metadata,
                        operation_id,
                        eyre!("Failed to deserialize NodeScanAck: {error}"),
                    )
                    .await;
            }
        };

        if !ack.accepted {
            return self
                .abort_before_scan_ack(
                    pool,
                    &cascade_ids,
                    &scope_metadata,
                    operation_id,
                    eyre!(
                        "Node '{}' rejected scan command: {}",
                        ack.node_name,
                        ack.error.unwrap_or_else(|| "unknown reason".to_string())
                    ),
                )
                .await;
        }

        info!(
            operation_id = %operation_id,
            node = %ack.node_name,
            "Node accepted scan command, waiting for completion"
        );

        let replay = self.replay.clone();
        let mut events_processed: u64 = 0;
        let mut events_emitted: u64 = 0;

        struct ReplayScanFailure {
            error: color_eyre::eyre::Report,
            emitted_count: u64,
            restore_archived_cascade: bool,
        }

        let target_node_name = ack.node_name.clone();
        let completion = match tokio::time::timeout(self.scan_completion_timeout, async {
            loop {
                tokio::select! {
                    maybe_msg = progress_sub.next() => {
                        let Some(msg) = maybe_msg else {
                            return Err::<u64, ReplayScanFailure>(ReplayScanFailure {
                                error: eyre!(
                                    "Replay progress stream closed before node '{}' reported completion",
                                    target_node_name
                                ),
                                emitted_count: events_emitted,
                                restore_archived_cascade: events_emitted == 0,
                            });
                        };

                        match serde_json::from_slice::<NodeScanProgress>(&msg.payload) {
                            Ok(progress) => {
                                events_processed = progress.events_processed;
                                events_emitted = progress.events_emitted;
                                if let Some(error) = progress.error {
                                    return Err::<u64, ReplayScanFailure>(ReplayScanFailure {
                                        error: eyre!(
                                            "Node '{}' failed replay scan: {}",
                                            progress.node_name,
                                            error
                                        ),
                                        emitted_count: progress.events_emitted,
                                        restore_archived_cascade: progress.events_emitted == 0,
                                    });
                                }

                                debug!(
                                    operation_id = %operation_id,
                                    events_processed = progress.events_processed,
                                    events_emitted = progress.events_emitted,
                                    "Replay progress update"
                                );

                                // Update checkpoint with progress
                                checkpoint.processed_events = progress.events_processed;
                                checkpoint.updated_at = sinex_primitives::temporal::now();
                                if let Err(checkpoint_error) = self
                                    .persist_replay_checkpoint(
                                        operation_id,
                                        checkpoint,
                                        "Failed to persist replay progress checkpoint",
                                    )
                                    .await
                                {
                                    return Err::<u64, ReplayScanFailure>(ReplayScanFailure {
                                        error: checkpoint_error,
                                        emitted_count: progress.events_emitted,
                                        restore_archived_cascade: progress.events_emitted == 0,
                                    });
                                }

                                // If final_report is present, the scan is complete
                                if let Some(report) = &progress.final_report {
                                    info!(
                                        operation_id = %operation_id,
                                        events_processed = report.events_processed,
                                        "Node scan completed"
                                    );
                                    return Ok::<u64, ReplayScanFailure>(report.events_processed);
                                }
                            }
                            Err(err) => {
                                warn!(error = %err, "Failed to parse replay progress message");
                            }
                        }
                    }
                    () = tokio::time::sleep(Self::EXECUTION_STATE_POLL_INTERVAL) => {
                        match replay.load_operation(operation_id).await {
                            Ok(operation) if operation.state == ReplayState::Executing => {}
                            Ok(operation)
                                if matches!(
                                    operation.state,
                                    ReplayState::Cancelling | ReplayState::Cancelled
                                ) =>
                            {
                                return Err::<u64, ReplayScanFailure>(ReplayScanFailure {
                                    error: SinexError::cancelled(format!(
                                        "Replay operation {operation_id} was cancelled during execution"
                                    ))
                                    .into(),
                                    emitted_count: events_emitted,
                                    restore_archived_cascade: events_emitted == 0,
                                });
                            }
                            Ok(operation) => {
                                return Err::<u64, ReplayScanFailure>(ReplayScanFailure {
                                    error: eyre!(
                                        "Replay operation {} left Executing state unexpectedly: {:?}",
                                        operation_id,
                                        operation.state
                                    ),
                                    emitted_count: events_emitted,
                                    restore_archived_cascade: false,
                                });
                            }
                            Err(error) => {
                                return Err::<u64, ReplayScanFailure>(ReplayScanFailure {
                                    error: eyre!(
                                        "Failed to reload replay operation {} while waiting for progress: {}",
                                        operation_id,
                                        error
                                    ),
                                    emitted_count: events_emitted,
                                    restore_archived_cascade: false,
                                });
                            }
                        }
                    }
                }
            }
        })
        .await
        {
            Ok(result) => result,
            Err(_timeout) => Err(ReplayScanFailure {
                error: eyre!(
                    "Replay scan timed out waiting for node '{}' to report completion after {:?}",
                    target_node_name,
                    self.scan_completion_timeout
                ),
                emitted_count: events_emitted,
                restore_archived_cascade: false,
            }),
        };

        match completion {
            Ok(count) => {
                checkpoint.processed_events = count;
                checkpoint.updated_at = sinex_primitives::temporal::now();

                self.wait_for_replay_outputs_visible(pool, operation_id, &expected_replay_outputs)
                    .await?;

                // Record replacement relations between archived and newly-created events
                self.record_event_replacements(pool, operation_id, &cascade_ids)
                    .await?;

                Ok(count)
            }
            Err(failure) => {
                warn!(
                    operation_id = %operation_id,
                    error = %failure.error,
                    events_emitted = failure.emitted_count,
                    restore_archived_cascade = failure.restore_archived_cascade,
                    "Replay scan failed"
                );
                if failure.restore_archived_cascade
                    && let Err(restore_error) =
                        self.restore_cascade(pool, &cascade_ids, operation_id).await
                {
                    return Err(failure.error.wrap_err(format!(
                            "Replay scan failed before emitting replacement events, and restoring the archived cascade also failed: {restore_error}"
                        )));
                }
                // Publish compensating scope invalidations when either:
                // - we restored the cascade (so automata reconcile against restored events)
                // - events were emitted before failure (so automata reconcile the mixed state)
                if (failure.restore_archived_cascade || failure.emitted_count > 0)
                    && let Err(invalidation_error) = self
                        .publish_scope_invalidations(&scope_metadata, operation_id)
                        .await
                {
                    return Err(failure.error.wrap_err(format!(
                            "Replay scan failed and compensating scope invalidation also failed: {invalidation_error}"
                        )));
                }
                Err(failure.error).wrap_err(if failure.restore_archived_cascade {
                    "Replay scan failed before emitting replacement events; restored archived cascade and published compensating scope invalidations"
                } else if failure.emitted_count > 0 {
                    "Replay scan failed after partial event emission; published compensating scope invalidations for automata reconciliation"
                } else {
                    "Replay scan failed before emitting any replacement events; archived cascade left untouched"
                })
            }
        }
    }
}

#[derive(Debug, Deserialize)]
struct ReplayPreviewSummary {
    total_events: u64,
    time_window: ReplayPreviewTimeWindow,
    #[serde(default)]
    root_event_ids: Vec<Uuid>,
}

#[derive(Debug, Deserialize)]
struct ReplayPreviewTimeWindow {
    start: Timestamp,
    end: Timestamp,
}


#[derive(Debug, Clone)]
struct ExpectedReplayOutputs {
    minimum_visible_count: u64,
    sources: Vec<String>,
    event_types: Vec<String>,
    logical_source_identifiers: Vec<String>,
}


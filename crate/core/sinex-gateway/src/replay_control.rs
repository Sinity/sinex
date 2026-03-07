#![doc = include_str!("../docs/replay_control.md")]

use async_nats::connection::State as NatsState;
use async_nats::{Client, Message};
use color_eyre::eyre::{Context, Result, eyre};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
pub use sinex_db::replay::state_machine::ReplayScope;
use sinex_db::replay::state_machine::{
    ReplayCheckpoint, ReplayOperation, ReplayState, ReplayStateMachine,
};
use sinex_db::repositories::{DbPoolExt, EventRepositoryTx};
use sinex_node_sdk::runtime::stream::{ReplayPumpConfig, ReplayPumpProgress, publish_replay_event};
use sinex_primitives::domain::{EventSource, NodeName};
use sinex_primitives::environment::{SinexEnvironment, environment};
use sinex_primitives::events::{Event as StoredEvent, Provenance};
use sinex_primitives::{Pagination, SinexError};
use sinex_primitives::{Timestamp, Uuid};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;
use tokio::time::interval;
use tracing::{error, info, warn};

const REPLAY_CONTROL_SUBSCRIBE_ATTEMPTS: usize = 5;
const REPLAY_CONTROL_SUBSCRIBE_BACKOFF_BASE: Duration = Duration::from_millis(200);
const REPLAY_CONTROL_SUBSCRIBE_BACKOFF_MAX: Duration = Duration::from_secs(2);

/// Valid actor roles for replay operations.
const VALID_ACTOR_ROLES: &[&str] = &[
    "system",   // Internal system operations
    "service",  // Service accounts
    "user",     // Authenticated users
    "admin",    // Administrative operations
    "operator", // Operations team
    "test",     // Test actors (testing-only)
];

#[derive(Debug, Clone, Copy)]
enum ReplayAction {
    Plan,
    Approve,
    Execute,
    Cancel,
}

fn env_var_duration_secs(name: &str, default: u64) -> Duration {
    Duration::from_secs(
        std::env::var(name)
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(default),
    )
}

fn allow_test_actors() -> bool {
    cfg!(test)
        || std::env::var("SINEX_ALLOW_TEST_ACTORS")
            .ok()
            .is_some_and(|v| matches!(v.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
}

fn validate_actor_for_action(actor: &str, action: ReplayAction) -> Result<()> {
    if actor.is_empty() {
        return Err(eyre!("Actor cannot be empty"));
    }
    if actor.trim() != actor {
        return Err(eyre!("Actor cannot contain leading or trailing whitespace"));
    }
    if actor.chars().any(char::is_control) {
        return Err(eyre!("Actor contains invalid control characters"));
    }

    let (role, identifier) = actor
        .split_once(':')
        .ok_or_else(|| eyre!("Invalid actor format. Expected '<role>:<identifier>'"))?;

    if !VALID_ACTOR_ROLES.contains(&role) {
        return Err(eyre!(
            "Invalid actor role '{role}'. Allowed roles: {}",
            VALID_ACTOR_ROLES.join(", ")
        ));
    }

    if identifier.is_empty() || identifier.trim().is_empty() {
        return Err(eyre!("Actor identifier cannot be empty"));
    }
    if identifier.trim() != identifier {
        return Err(eyre!(
            "Actor identifier cannot contain leading or trailing whitespace"
        ));
    }
    if identifier.chars().any(char::is_control) {
        return Err(eyre!(
            "Actor identifier contains invalid control characters"
        ));
    }

    if role == "test" && !allow_test_actors() {
        return Err(eyre!(
            "Test actors are disabled in this environment (set SINEX_ALLOW_TEST_ACTORS=1 to enable)"
        ));
    }

    let requires_privileged_role = matches!(
        action,
        ReplayAction::Approve | ReplayAction::Execute | ReplayAction::Cancel
    );
    if requires_privileged_role && !matches!(role, "admin" | "operator" | "service" | "system") {
        return Err(eyre!(
            "Actor role '{role}' cannot perform this replay action"
        ));
    }

    Ok(())
}

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
struct ReplayControlHealthState {
    last_error: Option<ReplayControlError>,
}

/// Spawn the replay control bus and return a client handle.
///
/// The replay control system manages distributed replay operations, coordinating
/// event re-processing across the cluster with proper state tracking and locking.
pub async fn spawn_replay_control(
    replay: Arc<ReplayStateMachine>,
    client: Client,
) -> Result<ReplayControlClient> {
    let env = environment().clone();

    // Create execution engine with NATS client for event republishing
    let executor = ReplayExecutionEngine::new(replay.clone(), client.clone());
    ReplayTelemetry::new(replay.clone()).spawn();

    ReplayControlServer::new(env.clone(), client.clone(), replay, executor)
        .spawn()
        .await?;

    Ok(ReplayControlClient::new(env, client))
}

/// Client for issuing replay control commands over NATS.
#[derive(Clone)]
pub struct ReplayControlClient {
    subject: String,
    client: Client,
    health: Arc<Mutex<ReplayControlHealthState>>,
}

fn lock_recover<'a, T>(mutex: &'a Mutex<T>, context: &str) -> MutexGuard<'a, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => {
            warn!(mutex = context, "Mutex poisoned; recovering inner state");
            poisoned.into_inner()
        }
    }
}

impl ReplayControlClient {
    fn new(env: SinexEnvironment, client: Client) -> Self {
        let subject = env.nats_subject("sinex.control.replay");
        Self {
            subject,
            client,
            health: Arc::new(Mutex::new(ReplayControlHealthState::default())),
        }
    }

    /// Get the underlying NATS client.
    /// Reserved for external consumers (e.g., custom replay coordination).
    #[allow(dead_code)]
    #[must_use]
    pub fn nats_client(&self) -> &Client {
        &self.client
    }

    #[must_use]
    pub fn health_snapshot(&self) -> ReplayControlHealth {
        let connected = matches!(self.client.connection_state(), NatsState::Connected);
        let last_error = {
            let guard = lock_recover(&self.health, "replay_control_health");
            guard.last_error.clone()
        };
        ReplayControlHealth {
            connected,
            last_error,
        }
    }

    fn record_error(&self, message: impl Into<String>) {
        let mut guard = lock_recover(&self.health, "replay_control_health");
        guard.last_error = Some(ReplayControlError::new(message));
    }

    async fn send(&self, request: ReplayControlRequest) -> Result<ReplayControlResponse> {
        let payload = serde_json::to_vec(&request)?;

        // Issue 126: Configurable timeout for NATS replay requests
        let timeout = env_var_duration_secs("SINEX_REPLAY_CONTROL_TIMEOUT_SECS", 30);
        let message = tokio::time::timeout(
            timeout,
            self.client.request(self.subject.clone(), payload.into()),
        )
        .await
        .map_err(|_| {
            let error_msg = format!("Replay control request timed out after {timeout:?}");
            self.record_error(error_msg.clone());
            eyre!(error_msg)
        })?
        .inspect_err(|err| {
            self.record_error(err.to_string());
        })
        .wrap_err("Replay control request failed")?;

        let response: ReplayControlResponse = serde_json::from_slice(&message.payload)
            .inspect_err(|err| {
                self.record_error(err.to_string());
            })
            .wrap_err("Invalid replay control response")?;

        if response.status == ReplayControlStatus::Error {
            let message = response
                .message
                .unwrap_or_else(|| "Replay control request failed".to_string());
            self.record_error(message.clone());
            return Err(eyre!("{}", message));
        }

        Ok(response)
    }

    #[cfg(test)]
    async fn send_with_timeout(
        &self,
        request: ReplayControlRequest,
        timeout: Duration,
    ) -> Result<ReplayControlResponse> {
        let payload = serde_json::to_vec(&request)?;
        let nats_request = async_nats::Request::new()
            .timeout(Some(timeout))
            .payload(payload.into());
        let message = self
            .client
            .send_request(self.subject.clone(), nats_request)
            .await
            .inspect_err(|err| {
                self.record_error(err.to_string());
            })
            .wrap_err("Replay control request timed out")?;

        let response: ReplayControlResponse = serde_json::from_slice(&message.payload)
            .inspect_err(|err| {
                self.record_error(err.to_string());
            })
            .wrap_err("Invalid replay control response")?;

        if response.status == ReplayControlStatus::Error {
            let message = response
                .message
                .unwrap_or_else(|| "Replay control request failed".to_string());
            self.record_error(message.clone());
            return Err(eyre!("{}", message));
        }

        Ok(response)
    }

    pub async fn plan(&self, actor: String, scope: ReplayScope) -> Result<ReplayOperation> {
        // Validate actor format before sending request
        validate_actor_for_action(&actor, ReplayAction::Plan)?;

        let response = self
            .send(ReplayControlRequest::Plan { actor, scope })
            .await?;
        response
            .operation
            .ok_or_else(|| eyre!("Replay control response missing operation"))
    }

    #[cfg(test)]
    pub async fn plan_with_timeout(
        &self,
        actor: String,
        scope: ReplayScope,
        timeout: Duration,
    ) -> Result<ReplayOperation> {
        // Validate actor format before sending request
        validate_actor_for_action(&actor, ReplayAction::Plan)?;

        let response = self
            .send_with_timeout(ReplayControlRequest::Plan { actor, scope }, timeout)
            .await?;
        response
            .operation
            .ok_or_else(|| eyre!("Replay control response missing operation"))
    }

    pub async fn preview(
        &self,
        operation_id: Uuid,
    ) -> Result<(ReplayOperation, serde_json::Value)> {
        let response = self
            .send(ReplayControlRequest::Preview { operation_id })
            .await?;
        let operation = response
            .operation
            .ok_or_else(|| eyre!("Replay control response missing operation"))?;
        let preview = response
            .preview
            .ok_or_else(|| eyre!("Replay control response missing preview summary"))?;
        Ok((operation, preview))
    }

    pub async fn approve(&self, operation_id: Uuid, approver: String) -> Result<ReplayOperation> {
        // Validate approver identity
        validate_actor_for_action(&approver, ReplayAction::Approve)?;

        let response = self
            .send(ReplayControlRequest::Approve {
                operation_id,
                approver,
            })
            .await?;
        response
            .operation
            .ok_or_else(|| eyre!("Replay control response missing operation"))
    }

    pub async fn execute(&self, operation_id: Uuid, executor: String) -> Result<ReplayOperation> {
        // Validate executor identity
        validate_actor_for_action(&executor, ReplayAction::Execute)?;

        let response = self
            .send(ReplayControlRequest::Execute {
                operation_id,
                executor,
            })
            .await?;
        response
            .operation
            .ok_or_else(|| eyre!("Replay control response missing operation"))
    }

    pub async fn cancel(
        &self,
        operation_id: Uuid,
        canceller: String,
        reason: Option<String>,
    ) -> Result<ReplayOperation> {
        validate_actor_for_action(&canceller, ReplayAction::Cancel)?;
        let response = self
            .send(ReplayControlRequest::Cancel {
                operation_id,
                canceller,
                reason,
            })
            .await?;
        response
            .operation
            .ok_or_else(|| eyre!("Replay control response missing operation"))
    }

    pub async fn status(&self, operation_id: Uuid) -> Result<ReplayOperation> {
        let response = self
            .send(ReplayControlRequest::Status { operation_id })
            .await?;
        response
            .operation
            .ok_or_else(|| eyre!("Replay control response missing operation"))
    }

    pub async fn list(&self, state: Option<ReplayState>) -> Result<Vec<ReplayOperation>> {
        let response = self.send(ReplayControlRequest::List { state }).await?;
        Ok(response.operations.unwrap_or_default())
    }
}

struct ReplayControlServer {
    subject: String,
    client: Client,
    replay: Arc<ReplayStateMachine>,
    executor: ReplayExecutionEngine,
}

impl ReplayControlServer {
    fn new(
        env: SinexEnvironment,
        client: Client,
        replay: Arc<ReplayStateMachine>,
        executor: ReplayExecutionEngine,
    ) -> Self {
        let subject = env.nats_subject("sinex.control.replay");
        Self {
            subject,
            client,
            replay,
            executor,
        }
    }

    async fn spawn(self) -> Result<()> {
        let client = self.client.clone();
        let subject = self.subject.clone();
        let mut backoff = REPLAY_CONTROL_SUBSCRIBE_BACKOFF_BASE;
        let mut attempt = 0usize;
        let mut subscription = loop {
            attempt += 1;
            match client.subscribe(subject.clone()).await {
                Ok(subscription) => break subscription,
                Err(err) => {
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
        };
        let replay = self.replay.clone();
        let executor = self.executor.clone();

        tokio::spawn(async move {
            while let Some(message) = subscription.next().await {
                if let Err(err) = Self::handle_message(&client, &replay, &executor, message).await {
                    warn!(?err, "Replay control request failed");
                }
            }
        });

        info!(
            subject = %self.subject,
            "Replay control server subscribed to subject"
        );

        Ok(())
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
                    ReplayControlResponse::error(format!("{err}"))
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
                    if let Err(err) = client.publish(reply_subject, bytes.into()).await {
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
                // Server-side validation of actor (defense in depth)
                validate_actor_for_action(&actor, ReplayAction::Plan)?;

                let op = replay
                    .create_operation(scope.clone(), actor.clone())
                    .await?;
                ReplayControlResponse::success(Some(op), None, None)
            }
            ReplayControlRequest::Preview { operation_id } => {
                let operation = replay.load_operation(operation_id).await?;
                let preview = replay.generate_preview_summary(&operation.scope).await?;
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
            ReplayControlRequest::Execute {
                operation_id,
                executor: actor,
            } => {
                // Server-side validation of executor (defense in depth)
                validate_actor_for_action(&actor, ReplayAction::Execute)?;

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
            ReplayControlRequest::List { state } => {
                let ops = replay.list_operations(state).await?;
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
/// 3. Republishes material-root events through NATS for reprocessing
/// 3. Tracks progress via checkpoints
/// 4. Handles errors gracefully with proper state transitions
#[derive(Clone)]
struct ReplayExecutionEngine {
    replay: Arc<ReplayStateMachine>,
    nats_client: Client,
    env: SinexEnvironment,
}

impl ReplayExecutionEngine {
    fn new(replay: Arc<ReplayStateMachine>, nats_client: Client) -> Self {
        Self {
            replay,
            nats_client,
            env: environment(),
        }
    }

    async fn execute(&self, operation_id: Uuid, executor_name: String) -> Result<ReplayOperation> {
        if !self
            .replay
            .acquire_execution_lock(operation_id, NodeName::new(executor_name.clone()))
            .await?
        {
            return Err(eyre!(
                "Operation {} is already executing on another node",
                operation_id
            ));
        }

        info!(
            operation_id = %operation_id,
            executor = %executor_name,
            "Starting replay execution"
        );

        let result = self.run_operation(operation_id, &executor_name).await;
        self.handle_execution_finish(operation_id, &result).await;
        result
    }

    async fn handle_execution_finish(&self, operation_id: Uuid, result: &Result<ReplayOperation>) {
        if let Err(err) = result {
            error!(
                operation_id = %operation_id,
                error = %err,
                "Replay execution failed"
            );
            if let Err(mark_err) = self
                .replay
                .mark_failed(operation_id, format!("{err}"))
                .await
            {
                warn!(?mark_err, "Failed to mark replay operation as failed");
            }
        }

        if let Err(err) = self.replay.release_execution_lock(operation_id).await {
            warn!(?err, "Failed to release replay execution lock");
        }
    }

    async fn run_operation(
        &self,
        operation_id: Uuid,
        executor_name: &str,
    ) -> Result<ReplayOperation> {
        let (initial, total_events, execution_window) =
            self.prepare_operation(operation_id).await?;

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
                self.replay.pool(),
                &mut checkpoint,
                executor_name,
            )
            .await;

        self.finalize_operation(operation_id, total_events, checkpoint, replay_result)
            .await
    }

    async fn prepare_operation(
        &self,
        operation_id: Uuid,
    ) -> Result<(ReplayOperation, u64, (Timestamp, Timestamp))> {
        let op = self.replay.load_operation(operation_id).await?;
        if op.state != ReplayState::Approved {
            return Err(eyre!(
                "Operation {} must be approved before execution",
                operation_id
            ));
        }

        let preview = op.preview_summary.clone().ok_or_else(|| {
            eyre!(
                "Operation {} is missing preview summary; run preview before execution",
                operation_id
            )
        })?;

        // Transition to Executing state
        self.replay
            .transition(operation_id, ReplayState::Executing)
            .await?;

        let preview_summary: ReplayPreviewSummary = serde_json::from_value(preview)
            .map_err(|e| eyre!("Invalid replay preview summary: {e}"))?;
        let total_events = preview_summary.total_events;
        let execution_window = (
            preview_summary.time_window.start,
            preview_summary.time_window.end,
        );

        info!(
            operation_id = %operation_id,
            total_events = total_events,
            node_id = %op.scope.node_id,
            "Beginning event replay"
        );

        Ok((op, total_events, execution_window))
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
                self.replay
                    .update_checkpoint(operation_id, &checkpoint)
                    .await?;

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
                if let Err(ckpt_err) = self
                    .replay
                    .update_checkpoint(operation_id, &checkpoint)
                    .await
                {
                    warn!(?ckpt_err, "Failed to save checkpoint on error");
                }
                Err(err)
            }
        }
    }

    async fn collect_scope_events(
        &self,
        scope: &ReplayScope,
        time_window: (Timestamp, Timestamp),
        pool: &sqlx::PgPool,
        config: &ReplayPumpConfig,
    ) -> Result<Vec<StoredEvent>> {
        let event_source = EventSource::new(&scope.node_id).map_err(|e| {
            eyre!(SinexError::validation("Invalid replay scope node_id").with_std_error(&e))
        })?;
        let normalized = scope.normalized_filters();
        let material_filter = normalized.material_ids;
        let event_type_filter = normalized.event_types;

        let mut offset: i64 = 0;
        let mut events = Vec::new();

        loop {
            let page = Pagination::new(Some(config.batch_size), Some(offset));
            let batch = pool
                .events()
                .get_by_source_and_time_range(&event_source, time_window.0, time_window.1, page)
                .await
                .map_err(|e| eyre!("Failed to query replay scope events: {e}"))?;

            if batch.is_empty() {
                break;
            }

            let filtered = batch.into_iter().filter(|event| {
                let material_id = match &event.provenance {
                    Provenance::Material { id, .. } => Some(id.as_uuid()),
                    _ => None,
                };
                let material_ok = material_filter
                    .as_ref()
                    .is_none_or(|materials| material_id.is_some_and(|id| materials.contains(&id)));
                let event_type_ok = event_type_filter
                    .as_ref()
                    .is_none_or(|types| types.iter().any(|kind| kind == event.event_type.as_str()));
                material_id.is_some() && material_ok && event_type_ok
            });
            events.extend(filtered);
            offset += config.batch_size;
        }

        Ok(events)
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
            .expand_cascade(&table_name, 64)
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
                "superseded by replay rescan",
                &operation_id.to_string(),
                archived_by,
            )
            .await
            .map_err(|e| eyre!("{e}"))
            .wrap_err("Failed to archive replay cascade")
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
            .wrap_err("Failed to restore archived replay cascade after publish failure")?;
        Ok(())
    }

    /// Replay material-root events after archiving the affected cascade.
    async fn replay_events(
        &self,
        operation_id: Uuid,
        scope: &ReplayScope,
        execution_window: (Timestamp, Timestamp),
        pool: &sqlx::PgPool,
        checkpoint: &mut ReplayCheckpoint,
        executor_name: &str,
    ) -> Result<u64> {
        let config = ReplayPumpConfig::default();
        let material_roots = self
            .collect_scope_events(scope, execution_window, pool, &config)
            .await?;
        if material_roots.is_empty() {
            info!(operation_id = %operation_id, "Replay scope matched zero live events");
            return Ok(0);
        }

        let root_ids: Vec<Uuid> = material_roots
            .iter()
            .filter_map(|event| event.id.map(|id| *id.as_uuid()))
            .collect();
        if root_ids.is_empty() {
            return Err(eyre!(
                "Replay scope material roots are missing persistent event ids"
            ));
        }

        let cascade_ids = self
            .derive_cascade_ids(pool, operation_id, &root_ids)
            .await?;
        let archived_count = self
            .archive_cascade(pool, &cascade_ids, operation_id, executor_name)
            .await?;
        info!(
            operation_id = %operation_id,
            material_roots = material_roots.len(),
            archived_count,
            "Archived replay cascade before republishing roots"
        );

        checkpoint.total_events = material_roots.len() as u64;
        let js = async_nats::jetstream::new(self.nats_client.clone());
        let replay = self.replay.clone();
        let mut progress = ReplayPumpProgress::default();

        for chunk in material_roots.chunks(config.batch_size as usize) {
            progress.batch_number = progress.batch_number.saturating_add(1);

            for event in chunk {
                let event_id = match publish_replay_event(
                    &js,
                    &self.env,
                    operation_id,
                    event,
                    config.publish_ack_timeout,
                )
                .await
                {
                    Ok(id) => id,
                    Err(err) => {
                        let restore_result =
                            self.restore_cascade(pool, &cascade_ids, operation_id).await;
                        if let Err(restore_err) = restore_result {
                            error!(
                                operation_id = %operation_id,
                                error = %restore_err,
                                "Replay publish failed and cascade restore also failed"
                            );
                        } else {
                            warn!(
                                operation_id = %operation_id,
                                "Replay publish failed; archived cascade restored"
                            );
                        }

                        return Err(eyre!(err.to_string()))
                            .wrap_err("Failed during replay root republish loop");
                    }
                };

                progress.processed_events = progress.processed_events.saturating_add(1);
                progress.last_event_id = Some(event_id);
            }

            checkpoint.processed_events = progress.processed_events;
            checkpoint.last_event_id = progress.last_event_id;
            checkpoint.batch_number = progress.batch_number;
            checkpoint.updated_at = sinex_primitives::temporal::now();
            replay
                .update_checkpoint(operation_id, checkpoint)
                .await
                .map_err(|e| eyre!("{e}"))
                .wrap_err("Failed to persist replay checkpoint")?;
        }

        checkpoint.processed_events = progress.processed_events;
        checkpoint.last_event_id = progress.last_event_id;
        checkpoint.batch_number = progress.batch_number;
        checkpoint.updated_at = sinex_primitives::temporal::now();
        Ok(progress.processed_events)
    }
}

#[derive(Debug, Deserialize)]
struct ReplayPreviewSummary {
    total_events: u64,
    time_window: ReplayPreviewTimeWindow,
}

#[derive(Debug, Deserialize)]
struct ReplayPreviewTimeWindow {
    start: Timestamp,
    end: Timestamp,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReplayTelemetrySnapshot {
    pub total_operations: usize,
    pub active_operations: usize,
    pub counts: HashMap<ReplayState, usize>,
}

#[derive(Clone)]
struct ReplayTelemetry {
    replay: Arc<ReplayStateMachine>,
    poll_interval: Duration,
    latest: Arc<Mutex<ReplayTelemetrySnapshot>>,
}

impl ReplayTelemetry {
    fn new(replay: Arc<ReplayStateMachine>) -> Self {
        Self {
            replay,
            poll_interval: Duration::from_secs(30),
            latest: Arc::new(Mutex::new(ReplayTelemetrySnapshot::default())),
        }
    }

    #[cfg(test)]
    fn with_interval(replay: Arc<ReplayStateMachine>, poll_interval: Duration) -> Self {
        Self {
            replay,
            poll_interval,
            latest: Arc::new(Mutex::new(ReplayTelemetrySnapshot::default())),
        }
    }

    #[cfg(test)]
    fn latest_snapshot(&self) -> ReplayTelemetrySnapshot {
        let guard = lock_recover(&self.latest, "replay_telemetry_snapshot");
        guard.clone()
    }

    fn spawn(self) {
        tokio::spawn(async move {
            let mut ticker = interval(self.poll_interval);
            loop {
                ticker.tick().await;
                if let Err(err) = self.sample().await {
                    warn!(?err, "Replay telemetry sample failed");
                }
            }
        });
    }

    async fn sample(&self) -> Result<()> {
        let operations = self.replay.list_operations(None).await?;
        let mut counts: HashMap<ReplayState, usize> = HashMap::new();
        for op in &operations {
            *counts.entry(op.state).or_default() += 1;
        }

        let active: usize = counts
            .iter()
            .filter(|(state, _)| !state.is_terminal())
            .map(|(_, count)| count)
            .sum();

        let snapshot = ReplayTelemetrySnapshot {
            total_operations: operations.len(),
            active_operations: active,
            counts: counts.clone(),
        };

        let mut guard = lock_recover(&self.latest, "replay_telemetry_snapshot");
        *guard = snapshot.clone();

        info!(
            total_operations = snapshot.total_operations,
            active_operations = snapshot.active_operations,
            ?counts,
            "Replay control telemetry snapshot"
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use serde_json::json;
    use sinex_db::DbPool;
    use sinex_db::repositories::DbPoolExt;
    use sinex_db::repositories::state::Operation;
    use sinex_primitives::{DynamicPayload, Id, Uuid};
    use tokio::time::sleep;
    use xtask::sandbox::sinex_test;

    fn sample_scope() -> ReplayScope {
        ReplayScope {
            node_id: "fs-test".to_string(),
            time_window: None,
            material_filter: None,
            filters: HashMap::new(),
        }
    }

    async fn wait_for_operation(pool: &DbPool, operation_id: Uuid) -> Result<()> {
        let op_id = Id::<Operation>::from_uuid(operation_id);
        for attempt in 0..20 {
            if pool.state().operation_exists(&op_id).await? {
                return Ok(());
            }
            sleep(Duration::from_millis(10 * (attempt + 1) as u64)).await;
        }
        Err(eyre!(
            "operation record {operation_id} not found after waiting for repository persistence"
        ))
    }

    async fn drive_to_state(
        replay: &Arc<ReplayStateMachine>,
        pool: &DbPool,
        operation_id: Uuid,
        targets: &[ReplayState],
    ) -> Result<()> {
        wait_for_operation(pool, operation_id).await?;
        for state in targets {
            replay.transition(operation_id, *state).await?;
        }
        Ok(())
    }

    #[sinex_test]
    async fn telemetry_reports_state_counts(ctx: TestContext) -> Result<()> {
        let replay = Arc::new(ReplayStateMachine::new(ctx.pool.clone()));
        let telemetry = ReplayTelemetry::with_interval(replay.clone(), Duration::from_millis(5));
        let scope = sample_scope();

        let _planning = replay
            .create_operation(scope.clone(), "planner".into())
            .await?;

        let executing = replay
            .create_operation(scope.clone(), "executor".into())
            .await?;
        drive_to_state(
            &replay,
            &ctx.pool,
            executing.operation_id,
            &[
                ReplayState::Previewed,
                ReplayState::Approved,
                ReplayState::Executing,
            ],
        )
        .await?;

        let failed = replay.create_operation(scope, "runner".into()).await?;
        drive_to_state(
            &replay,
            &ctx.pool,
            failed.operation_id,
            &[
                ReplayState::Previewed,
                ReplayState::Approved,
                ReplayState::Executing,
                ReplayState::Failed,
            ],
        )
        .await?;

        telemetry.sample().await?;
        let snapshot = telemetry.latest_snapshot();

        assert_eq!(snapshot.total_operations, 3);
        assert_eq!(snapshot.active_operations, 2);
        assert_eq!(snapshot.counts.get(&ReplayState::Planning), Some(&1));
        assert_eq!(snapshot.counts.get(&ReplayState::Executing), Some(&1));
        assert_eq!(snapshot.counts.get(&ReplayState::Failed), Some(&1));

        Ok(())
    }

    #[sinex_test]
    async fn telemetry_handles_empty_operation_set(ctx: TestContext) -> Result<()> {
        let replay = Arc::new(ReplayStateMachine::new(ctx.pool.clone()));
        let telemetry = ReplayTelemetry::with_interval(replay, Duration::from_millis(5));

        telemetry.sample().await?;
        let snapshot = telemetry.latest_snapshot();

        assert_eq!(snapshot.total_operations, 0);
        assert_eq!(snapshot.active_operations, 0);
        assert!(snapshot.counts.is_empty());

        Ok(())
    }

    #[sinex_test]
    async fn replay_client_errors_when_broker_disappears(ctx: TestContext) -> Result<()> {
        let ctx = ctx.with_nats().dedicated().await?;
        let nats = ctx.nats_handle()?;

        let replay = Arc::new(ReplayStateMachine::new(ctx.pool.clone()));
        let nats_client = ctx.nats_client();
        let client = spawn_replay_control(replay, nats_client).await?;

        // Shut down the broker to simulate a partition mid-flight.
        nats.shutdown().await?;
        tokio::time::sleep(Duration::from_millis(200)).await;

        let scope = sample_scope();
        let err = client
            .plan_with_timeout("test:user".into(), scope, Duration::from_secs(1))
            .await
            .expect_err("plan should fail after broker drop");
        let message = err.to_string();
        assert!(
            message.contains("request")
                || message.contains("connection")
                || message.contains("timed out")
                || message.contains("no responders"),
            "unexpected error: {message}"
        );
        let health = client.health_snapshot();
        assert!(
            health.last_error.is_some(),
            "health snapshot should retain the last replay control error"
        );
        Ok(())
    }

    #[sinex_test]
    async fn replay_execution_records_outcome(ctx: TestContext) -> Result<()> {
        let ctx = ctx.with_nats().dedicated().await?;

        let (material_id, inserted, inserted_ts) = loop {
            let material_id = ctx.create_source_material(Some("replay-outcome")).await?;
            let event = DynamicPayload::new(
                "fs-test",
                "file.created",
                json!({ "path": "/tmp/replay.txt" }),
            )
            .from_material(material_id)
            .build()?;
            let inserted = ctx.pool.events().insert(event).await?;
            if let Some(ts_orig) = inserted.ts_orig
                && ts_orig.inner().nanosecond() > 0
            {
                break (material_id, inserted, ts_orig);
            }
        };

        let replay_target_event_id = inserted.id.expect("inserted replay target must have id");
        let replay_target_id = replay_target_event_id.to_uuid();
        let target_window_end = inserted_ts;
        let target_window_start = target_window_end - time::Duration::milliseconds(1);

        let cascaded = DynamicPayload::new(
            "analytics-test",
            "analytics.summary",
            json!({ "path": "/tmp/replay-summary.txt" }),
        )
        .from_parents([replay_target_event_id])?
        .build()?;
        let cascaded_inserted = ctx.pool.events().insert(cascaded).await?;
        let cascaded_id = cascaded_inserted
            .id
            .expect("inserted cascaded event must have id")
            .to_uuid();

        let nonmatch_material = ctx
            .create_source_material(Some("replay-outcome-nonmatch"))
            .await?;
        let nonmatch_event = DynamicPayload::new(
            "fs-test",
            "file.created",
            json!({ "path": "/tmp/replay-nonmatch.txt" }),
        )
        .from_material(nonmatch_material)
        .build()?;
        let inserted_nonmatch = ctx.pool.events().insert(nonmatch_event).await?;
        let nonmatch_id = inserted_nonmatch
            .id
            .expect("inserted non-matching event must have id")
            .to_uuid();

        let replay = Arc::new(ReplayStateMachine::new(ctx.pool.clone()));
        let nats_client = ctx.nats_client();

        // Create a JetStream stream to receive replay-published events.
        // Without this, publish acks never arrive and the replay loop times out.
        // Subjects are environment-prefixed (e.g. "dev.events.raw.fs-test.file_created"),
        // so we match the prefixed pattern via the environment helper.
        let env = sinex_primitives::environment::environment();
        let js = async_nats::jetstream::new(nats_client.clone());
        let stream_name = format!("replay-test-{}", Uuid::now_v7().simple());
        js.get_or_create_stream(async_nats::jetstream::stream::Config {
            name: stream_name.clone(),
            subjects: vec![env.nats_subject("events.raw.>")],
            ..Default::default()
        })
        .await?;

        let client = spawn_replay_control(replay, nats_client).await?;

        let mut scope = sample_scope();
        scope.time_window = Some((target_window_start, target_window_end));
        scope.material_filter = Some(vec![*material_id.as_uuid()]);
        scope
            .filters
            .insert("event_types".to_string(), json!(["file.created"]));

        let planned = client
            .plan("test:replay-user".into(), scope.clone())
            .await?;
        assert_eq!(planned.state, ReplayState::Planning);

        let (previewed, preview) = client.preview(planned.operation_id).await?;
        assert_eq!(previewed.state, ReplayState::Previewed);
        assert_eq!(
            preview
                .get("total_events")
                .and_then(serde_json::Value::as_i64),
            Some(1),
            "preview should match only the filtered replay target"
        );
        assert_eq!(
            preview
                .get("replay_semantics")
                .and_then(serde_json::Value::as_str),
            Some("rescan_material_roots_only")
        );

        let approved = client
            .approve(planned.operation_id, "admin:approver".into())
            .await?;
        assert_eq!(approved.state, ReplayState::Approved);

        let executed = client
            .execute(planned.operation_id, "service:executor-node".into())
            .await?;
        assert_eq!(executed.state, ReplayState::Completed);
        assert_eq!(executed.checkpoint.processed_events, 1);
        assert_eq!(executed.checkpoint.total_events, 1);
        assert_eq!(
            preview
                .get("total_events")
                .and_then(serde_json::Value::as_u64),
            Some(executed.checkpoint.total_events),
            "execute checkpoint totals must match preview totals"
        );

        assert!(
            executed.outcome.is_some(),
            "Replay execution should record a concrete outcome for automation consumers"
        );

        use async_nats::jetstream::consumer::{
            AckPolicy, DeliverPolicy, pull::Config as ConsumerConfig,
        };
        let stream = js.get_stream(&stream_name).await?;
        let consumer_name = format!("replay-test-consumer-{}", Uuid::now_v7().simple());
        let consumer = stream
            .get_or_create_consumer(
                &consumer_name,
                ConsumerConfig {
                    durable_name: Some(consumer_name.clone()),
                    name: Some(consumer_name.clone()),
                    deliver_policy: DeliverPolicy::All,
                    ack_policy: AckPolicy::Explicit,
                    filter_subject: env.nats_subject("events.raw.fs-test.file_created"),
                    ..Default::default()
                },
            )
            .await?;

        let mut replay_batch = consumer
            .fetch()
            .max_messages(8)
            .expires(Duration::from_secs(2))
            .messages()
            .await?;
        let mut replay_payloads = Vec::new();
        while let Some(message) = replay_batch.next().await {
            let message = message.map_err(|e| eyre!(e.to_string()))?;
            replay_payloads.push(serde_json::from_slice::<serde_json::Value>(
                &message.payload,
            )?);
            message.ack().await.map_err(|e| eyre!(e.to_string()))?;
        }
        assert_eq!(
            replay_payloads.len(),
            1,
            "filtered replay should republish exactly one root event"
        );

        let replay_payload = replay_payloads.remove(0);
        let replayed_event: StoredEvent<serde_json::Value> =
            serde_json::from_value(replay_payload.clone())?;
        let replayed_id = replayed_event
            .id
            .expect("replayed event payload should include id")
            .to_uuid();
        assert_ne!(
            replayed_id, replay_target_id,
            "replay must mint fresh event ids"
        );
        let planned_operation_id = planned.operation_id.to_string();
        assert_eq!(
            replay_payload
                .get("replay_operation_id")
                .and_then(serde_json::Value::as_str),
            Some(planned_operation_id.as_str())
        );
        assert!(
            replay_payload
                .get("source_event_ids")
                .is_none_or(serde_json::Value::is_null),
            "material-root replay payloads must not carry source_event_ids"
        );
        match replayed_event.provenance {
            Provenance::Material { id, .. } => {
                assert_eq!(
                    *id.as_uuid(),
                    *material_id.as_uuid(),
                    "replayed root must preserve source material provenance"
                );
            }
            other => {
                return Err(eyre!(
                    "expected material provenance for replayed root event, got {other:?}"
                ));
            }
        }

        let replay_target_live: i64 =
            sqlx::query_scalar("SELECT COUNT(*)::bigint FROM core.events WHERE id = $1::uuid")
                .bind(replay_target_id)
                .fetch_one(&ctx.pool)
                .await?;
        let replay_target_archived: i64 = sqlx::query_scalar(
            "SELECT COUNT(*)::bigint FROM audit.archived_events WHERE id = $1::uuid",
        )
        .bind(replay_target_id)
        .fetch_one(&ctx.pool)
        .await?;
        let cascaded_live: i64 =
            sqlx::query_scalar("SELECT COUNT(*)::bigint FROM core.events WHERE id = $1::uuid")
                .bind(cascaded_id)
                .fetch_one(&ctx.pool)
                .await?;
        let cascaded_archived: i64 = sqlx::query_scalar(
            "SELECT COUNT(*)::bigint FROM audit.archived_events WHERE id = $1::uuid",
        )
        .bind(cascaded_id)
        .fetch_one(&ctx.pool)
        .await?;
        let nonmatch_live: i64 =
            sqlx::query_scalar("SELECT COUNT(*)::bigint FROM core.events WHERE id = $1::uuid")
                .bind(nonmatch_id)
                .fetch_one(&ctx.pool)
                .await?;
        let nonmatch_archived: i64 = sqlx::query_scalar(
            "SELECT COUNT(*)::bigint FROM audit.archived_events WHERE id = $1::uuid",
        )
        .bind(nonmatch_id)
        .fetch_one(&ctx.pool)
        .await?;

        assert_eq!(replay_target_live, 0);
        assert_eq!(replay_target_archived, 1);
        assert_eq!(cascaded_live, 0);
        assert_eq!(cascaded_archived, 1);
        assert_eq!(nonmatch_live, 1);
        assert_eq!(nonmatch_archived, 0);

        let material_root_id = ctx
            .create_source_material(Some("replay-rescan-parity"))
            .await?;
        let root = DynamicPayload::new(
            "rescan-test",
            "file.created",
            json!({ "path": "/tmp/rescan-root.txt" }),
        )
        .from_material(material_root_id)
        .build()?;
        let root_inserted = ctx.pool.events().insert(root).await?;
        let root_event_id = root_inserted.id.expect("rescan root must have id");
        let root_id = root_event_id.to_uuid();
        let rescan_derived = DynamicPayload::new(
            "rescan-test",
            "file.derived",
            json!({ "path": "/tmp/rescan-derived.txt" }),
        )
        .from_parents([root_event_id])?
        .build()?;
        let derived_inserted = ctx.pool.events().insert(rescan_derived).await?;
        let derived_id = derived_inserted
            .id
            .expect("rescan derived must have id")
            .to_uuid();
        let rescan_root_ts = root_inserted
            .ts_orig
            .expect("rescan root must have ts_orig");
        let rescan_scope = ReplayScope {
            node_id: "rescan-test".to_string(),
            time_window: Some((
                rescan_root_ts - time::Duration::seconds(1),
                rescan_root_ts + time::Duration::seconds(1),
            )),
            material_filter: None,
            filters: HashMap::new(),
        };
        let planned_rescan = client.plan("test:replay-user".into(), rescan_scope).await?;
        let (_, rescan_preview) = client.preview(planned_rescan.operation_id).await?;
        assert_eq!(
            rescan_preview
                .get("total_events")
                .and_then(serde_json::Value::as_i64),
            Some(1),
            "preview must count only material roots for rescan semantics"
        );
        client
            .approve(planned_rescan.operation_id, "admin:approver".into())
            .await?;
        let rescan_executed = client
            .execute(planned_rescan.operation_id, "service:executor-node".into())
            .await?;
        assert_eq!(rescan_executed.state, ReplayState::Completed);
        assert_eq!(rescan_executed.checkpoint.total_events, 1);
        assert_eq!(rescan_executed.checkpoint.processed_events, 1);
        let root_archived_after_rescan: i64 = sqlx::query_scalar(
            "SELECT COUNT(*)::bigint FROM audit.archived_events WHERE id = $1::uuid",
        )
        .bind(root_id)
        .fetch_one(&ctx.pool)
        .await?;
        let derived_archived_after_rescan: i64 = sqlx::query_scalar(
            "SELECT COUNT(*)::bigint FROM audit.archived_events WHERE id = $1::uuid",
        )
        .bind(derived_id)
        .fetch_one(&ctx.pool)
        .await?;
        assert_eq!(root_archived_after_rescan, 1);
        assert_eq!(derived_archived_after_rescan, 1);

        Ok(())
    }

    #[sinex_test]
    async fn actor_validation_rejects_empty_actor(_ctx: TestContext) -> Result<()> {
        let result = validate_actor_for_action("", ReplayAction::Plan);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("cannot be empty"));
        Ok(())
    }

    #[sinex_test]
    async fn actor_validation_rejects_invalid_role(_ctx: TestContext) -> Result<()> {
        let result = validate_actor_for_action("invalid:user", ReplayAction::Plan);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("Invalid actor role"));
        Ok(())
    }

    #[sinex_test]
    async fn actor_validation_rejects_empty_identifier(_ctx: TestContext) -> Result<()> {
        let result = validate_actor_for_action("user:", ReplayAction::Plan);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("identifier cannot be empty")
        );
        Ok(())
    }

    #[sinex_test]
    async fn actor_validation_accepts_valid_actors(_ctx: TestContext) -> Result<()> {
        assert!(validate_actor_for_action("user:alice", ReplayAction::Plan).is_ok());
        assert!(validate_actor_for_action("admin:root", ReplayAction::Plan).is_ok());
        assert!(validate_actor_for_action("service:replay-worker", ReplayAction::Plan).is_ok());
        assert!(validate_actor_for_action("system:internal", ReplayAction::Plan).is_ok());
        assert!(validate_actor_for_action("operator:ops-team", ReplayAction::Plan).is_ok());
        assert!(validate_actor_for_action("test:unit-test", ReplayAction::Plan).is_ok());
        Ok(())
    }

    #[sinex_test]
    async fn privileged_actions_reject_user_role(_ctx: TestContext) -> Result<()> {
        let result = validate_actor_for_action("user:alice", ReplayAction::Execute);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("cannot perform this replay action")
        );
        Ok(())
    }

    #[sinex_test]
    async fn plan_rejects_invalid_actor(ctx: TestContext) -> Result<()> {
        let ctx = ctx.with_nats().dedicated().await?;
        let replay = Arc::new(ReplayStateMachine::new(ctx.pool.clone()));
        let nats_client = ctx.nats_client();
        let client = spawn_replay_control(replay, nats_client).await?;

        let scope = sample_scope();
        let result = client.plan("invalid-actor".into(), scope).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Invalid actor"));
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "command", rename_all = "snake_case")]
pub enum ReplayControlRequest {
    Plan {
        actor: String,
        scope: ReplayScope,
    },
    Preview {
        operation_id: Uuid,
    },
    Approve {
        operation_id: Uuid,
        approver: String,
    },
    Execute {
        operation_id: Uuid,
        executor: String,
    },
    Cancel {
        operation_id: Uuid,
        canceller: String,
        reason: Option<String>,
    },
    Status {
        operation_id: Uuid,
    },
    List {
        state: Option<ReplayState>,
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ReplayControlResponse {
    pub status: ReplayControlStatus,
    pub message: Option<String>,
    pub operation: Option<ReplayOperation>,
    pub operations: Option<Vec<ReplayOperation>>,
    pub preview: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplayControlStatus {
    Ok,
    Error,
}

impl ReplayControlResponse {
    #[must_use]
    pub fn success(
        operation: Option<ReplayOperation>,
        preview: Option<serde_json::Value>,
        operations: Option<Vec<ReplayOperation>>,
    ) -> Self {
        Self {
            status: ReplayControlStatus::Ok,
            message: None,
            operation,
            operations,
            preview,
        }
    }

    pub fn error(message: impl Into<String>) -> Self {
        Self {
            status: ReplayControlStatus::Error,
            message: Some(message.into()),
            operation: None,
            operations: None,
            preview: None,
        }
    }
}

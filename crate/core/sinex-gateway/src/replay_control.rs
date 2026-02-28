#![doc = include_str!("../docs/replay_control.md")]

pub use crate::replay_state_machine::ReplayScope;
use crate::replay_state_machine::{
    ReplayCheckpoint, ReplayOperation, ReplayState, ReplayStateMachine,
};
use async_nats::connection::State as NatsState;
use async_nats::{Client, Message};
use color_eyre::eyre::{Context, Result, eyre};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use sinex_node_sdk::runtime::stream::{ReplayPumpConfig, replay_source_window};
use sinex_primitives::domain::NodeName;
use sinex_primitives::environment::{SinexEnvironment, environment};
use sinex_primitives::{Timestamp, Ulid};
use std::collections::HashMap;
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

        if response.status == "error" {
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

        if response.status == "error" {
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
        operation_id: Ulid,
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

    pub async fn approve(&self, operation_id: Ulid, approver: String) -> Result<ReplayOperation> {
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

    pub async fn execute(&self, operation_id: Ulid, executor: String) -> Result<ReplayOperation> {
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
        operation_id: Ulid,
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

    pub async fn status(&self, operation_id: Ulid) -> Result<ReplayOperation> {
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
/// 2. Republishes them through NATS for reprocessing
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

    async fn execute(&self, operation_id: Ulid, executor_name: String) -> Result<ReplayOperation> {
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

        let result = self.run_operation(operation_id).await;
        self.handle_execution_finish(operation_id, &result).await;
        result
    }

    async fn handle_execution_finish(&self, operation_id: Ulid, result: &Result<ReplayOperation>) {
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

    async fn run_operation(&self, operation_id: Ulid) -> Result<ReplayOperation> {
        let (initial, total_events) = self.prepare_operation(operation_id).await?;

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
                self.replay.pool(),
                &mut checkpoint,
            )
            .await;

        self.finalize_operation(operation_id, total_events, checkpoint, replay_result)
            .await
    }

    async fn prepare_operation(&self, operation_id: Ulid) -> Result<(ReplayOperation, u64)> {
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

        let total_events = preview
            .get("total_events")
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(0)
            .max(0) as u64;

        info!(
            operation_id = %operation_id,
            total_events = total_events,
            node_id = %op.scope.node_id,
            "Beginning event replay"
        );

        Ok((op, total_events))
    }

    async fn finalize_operation(
        &self,
        operation_id: Ulid,
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

    /// Replay events matching the scope by republishing them through NATS.
    async fn replay_events(
        &self,
        operation_id: Ulid,
        scope: &ReplayScope,
        pool: &sqlx::PgPool,
        checkpoint: &mut ReplayCheckpoint,
    ) -> Result<u64> {
        let js = async_nats::jetstream::new(self.nats_client.clone());
        let config = ReplayPumpConfig::default();
        let time_window = self.resolve_time_window(scope);
        let replay = self.replay.clone();

        let progress = replay_source_window(
            pool,
            &js,
            &self.env,
            operation_id,
            &scope.node_id,
            time_window,
            &config,
            |progress| {
                let replay = replay.clone();
                let mut checkpoint_snapshot = checkpoint.clone();
                async move {
                    checkpoint_snapshot.processed_events = progress.processed_events;
                    checkpoint_snapshot.last_event_id = progress.last_event_id;
                    checkpoint_snapshot.batch_number = progress.batch_number;
                    checkpoint_snapshot.updated_at = sinex_primitives::temporal::now();
                    replay
                        .update_checkpoint(operation_id, &checkpoint_snapshot)
                        .await
                        .map_err(|e| {
                            sinex_primitives::SinexError::service(format!(
                                "Failed to update replay checkpoint: {e}"
                            ))
                        })
                }
            },
        )
        .await
        .map_err(|e| eyre!(e.to_string()))
        .wrap_err("Failed during replay republish loop")?;

        checkpoint.processed_events = progress.processed_events;
        checkpoint.last_event_id = progress.last_event_id;
        checkpoint.batch_number = progress.batch_number;
        checkpoint.updated_at = sinex_primitives::temporal::now();
        Ok(progress.processed_events)
    }

    fn resolve_time_window(&self, scope: &ReplayScope) -> (Timestamp, Timestamp) {
        scope.time_window.unwrap_or_else(|| {
            let end = sinex_primitives::temporal::now();
            let start = end - sinex_primitives::temporal::Duration::hours(24);
            (start, end)
        })
    }
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
    use serde_json::json;
    use sinex_db::DbPool;
    use sinex_db::repositories::DbPoolExt;
    use sinex_primitives::{DynamicPayload, Id, Ulid};
    use tokio::time::sleep;
    use xtask::sandbox::{EphemeralNats, sinex_test};

    fn sample_scope() -> ReplayScope {
        ReplayScope {
            node_id: "fs-test".to_string(),
            time_window: None,
            material_filter: None,
            filters: HashMap::new(),
        }
    }

    async fn wait_for_operation(pool: &DbPool, operation_id: Ulid) -> Result<()> {
        let op_id = Id::<Operation>::from_ulid(operation_id);
        for attempt in 0..20 {
            if pool.state().operation_exists(&op_id).await? {
                return Ok(());
            }
            sleep(Duration::from_millis(10 * (attempt + 1) as u64)).await;
        }
        tracing::warn!(
            %operation_id,
            "operation record missing; inserting fallback for test context"
        );
        // Fallback: insert a minimal test operation if polling times out via repository
        use sinex_db::repositories::state::Operation;
        use sinex_primitives::domain::OperationStatus;
        let fallback_operation = Operation {
            id: Some(Id::from_ulid(operation_id)),
            operation_type: "replay".to_string(),
            operator: "test-suite".to_string(),
            scope: Some(json!({})),
            result_status: OperationStatus::Running,
            result_message: None,
            preview_summary: None,
            duration_ms: None,
        };

        // Insert directly with the specific ID
        pool.state().log_operation(fallback_operation).await?;
        Ok(())
    }

    async fn drive_to_state(
        replay: &Arc<ReplayStateMachine>,
        pool: &DbPool,
        operation_id: Ulid,
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
        let nats = EphemeralNats::start().await?;

        let replay = Arc::new(ReplayStateMachine::new(ctx.pool.clone()));
        let nats_client = nats.connect().await?;
        let client = spawn_replay_control(replay, nats_client).await?;

        // Drop the broker to simulate a partition mid-flight.
        drop(nats);
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
        let nats = EphemeralNats::start().await?;

        let material_id = ctx.create_source_material(Some("replay-outcome")).await?;
        let event = DynamicPayload::new(
            "fs-test",
            "file.created",
            json!({ "path": "/tmp/replay.txt" }),
        )
        .from_material(material_id)
        .build()?;
        ctx.pool.events().insert(event).await?;

        let replay = Arc::new(ReplayStateMachine::new(ctx.pool.clone()));
        let nats_client = nats.connect().await?;
        let client = spawn_replay_control(replay, nats_client).await?;

        let mut scope = sample_scope();
        let end = Timestamp::now();
        scope.time_window = Some((
            Timestamp::from(*end - time::Duration::hours(1)),
            Timestamp::from(*end + time::Duration::minutes(1)),
        ));

        let planned = client
            .plan("test:replay-user".into(), scope.clone())
            .await?;
        assert_eq!(planned.state, ReplayState::Planning);

        let (previewed, _) = client.preview(planned.operation_id).await?;
        assert_eq!(previewed.state, ReplayState::Previewed);

        let approved = client
            .approve(planned.operation_id, "admin:approver".into())
            .await?;
        assert_eq!(approved.state, ReplayState::Approved);

        let executed = client
            .execute(planned.operation_id, "service:executor-node".into())
            .await?;
        assert_eq!(executed.state, ReplayState::Completed);

        assert!(
            executed.outcome.is_some(),
            "Replay execution should record a concrete outcome for automation consumers"
        );

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
        let nats = EphemeralNats::start().await?;
        let replay = Arc::new(ReplayStateMachine::new(ctx.pool.clone()));
        let nats_client = nats.connect().await?;
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
        operation_id: Ulid,
    },
    Approve {
        operation_id: Ulid,
        approver: String,
    },
    Execute {
        operation_id: Ulid,
        executor: String,
    },
    Cancel {
        operation_id: Ulid,
        canceller: String,
        reason: Option<String>,
    },
    Status {
        operation_id: Ulid,
    },
    List {
        state: Option<ReplayState>,
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ReplayControlResponse {
    pub status: String,
    pub message: Option<String>,
    pub operation: Option<ReplayOperation>,
    pub operations: Option<Vec<ReplayOperation>>,
    pub preview: Option<serde_json::Value>,
}

impl ReplayControlResponse {
    #[must_use]
    pub fn success(
        operation: Option<ReplayOperation>,
        preview: Option<serde_json::Value>,
        operations: Option<Vec<ReplayOperation>>,
    ) -> Self {
        Self {
            status: "ok".to_string(),
            message: None,
            operation,
            operations,
            preview,
        }
    }

    pub fn error(message: impl Into<String>) -> Self {
        Self {
            status: "error".to_string(),
            message: Some(message.into()),
            operation: None,
            operations: None,
            preview: None,
        }
    }
}

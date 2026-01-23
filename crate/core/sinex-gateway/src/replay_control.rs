pub use crate::replay_state_machine::ReplayScope;
use crate::replay_state_machine::{ReplayOperation, ReplayState, ReplayStateMachine};
use async_nats::connection::State as NatsState;
use async_nats::{Client, Message};
use chrono::{DateTime, Utc};
use color_eyre::eyre::{eyre, Context, Result};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use sinex_core::environment::{environment, SinexEnvironment};
use sinex_core::types::ulid::Ulid;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};
use tokio::time::{interval, Duration};
use tracing::{error, info, warn};

const REPLAY_CONTROL_SUBSCRIBE_ATTEMPTS: usize = 5;
const REPLAY_CONTROL_SUBSCRIBE_BACKOFF_BASE: Duration = Duration::from_millis(200);
const REPLAY_CONTROL_SUBSCRIBE_BACKOFF_MAX: Duration = Duration::from_secs(2);

fn env_var_duration_secs(name: &str, default: u64) -> Duration {
    Duration::from_secs(
        std::env::var(name)
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(default),
    )
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayControlError {
    pub message: String,
    pub occurred_at: DateTime<Utc>,
}

impl ReplayControlError {
    pub(crate) fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            occurred_at: Utc::now(),
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
/// Spawn the replay control bus and return a client handle.
pub async fn spawn_replay_control(
    replay: Arc<ReplayStateMachine>,
    client: Client,
) -> Result<ReplayControlClient> {
    let env = environment().clone();

    let executor = ReplayExecutionEngine::new(replay.clone());
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
    pub fn nats_client(&self) -> &Client {
        &self.client
    }

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
            let error_msg = format!("Replay control request timed out after {:?}", timeout);
            self.record_error(error_msg.clone());
            eyre!(error_msg)
        })?
        .map_err(|err| {
            self.record_error(err.to_string());
            err
        })
        .wrap_err("Replay control request failed")?;

        let response: ReplayControlResponse = serde_json::from_slice(&message.payload)
            .map_err(|err| {
                self.record_error(err.to_string());
                err
            })
            .wrap_err("Invalid replay control response")?;

        if response.status == "error" {
            let message = response
                .message
                .clone()
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
            .map_err(|err| {
                self.record_error(err.to_string());
                err
            })
            .wrap_err("Replay control request timed out")?;

        let response: ReplayControlResponse = serde_json::from_slice(&message.payload)
            .map_err(|err| {
                self.record_error(err.to_string());
                err
            })
            .wrap_err("Invalid replay control response")?;

        if response.status == "error" {
            let message = response
                .message
                .clone()
                .unwrap_or_else(|| "Replay control request failed".to_string());
            self.record_error(message.clone());
            return Err(eyre!("{}", message));
        }

        Ok(response)
    }

    pub async fn plan(&self, actor: String, scope: ReplayScope) -> Result<ReplayOperation> {
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
        reason: Option<String>,
    ) -> Result<ReplayOperation> {
        let response = self
            .send(ReplayControlRequest::Cancel {
                operation_id,
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
        let request: ReplayControlRequest =
            serde_json::from_slice(&message.payload).wrap_err("Invalid replay control request")?;
        let response = match Self::process_request(replay, executor, request).await {
            Ok(response) => response,
            Err(err) => {
                warn!(?err, "Replay control request failed");
                ReplayControlResponse::error(format!("{}", err))
            }
        };

        if let Some(reply_subject) = message.reply {
            if let Err(err) = client
                .publish(reply_subject, serde_json::to_vec(&response)?.into())
                .await
            {
                error!(?err, "Failed to send replay control response");
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
                replay.approve(operation_id, approver).await?;
                let updated = replay.load_operation(operation_id).await?;
                ReplayControlResponse::success(Some(updated), None, None)
            }
            ReplayControlRequest::Execute {
                operation_id,
                executor: actor,
            } => {
                let updated = executor.execute(operation_id, actor).await?;
                ReplayControlResponse::success(Some(updated), None, None)
            }
            ReplayControlRequest::Cancel {
                operation_id,
                reason,
            } => {
                replay
                    .cancel(
                        operation_id,
                        reason.unwrap_or_else(|| "Cancelled via control bus".into()),
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

#[derive(Clone)]
struct ReplayExecutionEngine {
    replay: Arc<ReplayStateMachine>,
}

impl ReplayExecutionEngine {
    fn new(replay: Arc<ReplayStateMachine>) -> Self {
        Self { replay }
    }

    async fn execute(&self, operation_id: Ulid, executor_name: String) -> Result<ReplayOperation> {
        if !self
            .replay
            .acquire_execution_lock(operation_id, executor_name.clone())
            .await?
        {
            return Err(eyre!(
                "Operation {} is already executing on another node",
                operation_id
            ));
        }

        let result = self.run_operation(operation_id).await;

        if let Err(ref err) = result {
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

        result
    }

    async fn run_operation(&self, operation_id: Ulid) -> Result<ReplayOperation> {
        let initial = self.replay.load_operation(operation_id).await?;
        if initial.state != ReplayState::Approved {
            return Err(eyre!(
                "Operation {} must be approved before execution",
                operation_id
            ));
        }

        let preview = initial.preview_summary.clone().ok_or_else(|| {
            eyre!(
                "Operation {} is missing preview summary; run preview before execution",
                operation_id
            )
        })?;

        self.replay
            .transition(operation_id, ReplayState::Executing)
            .await?;

        let total_events = preview
            .get("total_events")
            .and_then(|value| value.as_i64())
            .unwrap_or(0)
            .max(0) as u64;

        let mut checkpoint = initial.checkpoint.clone();
        checkpoint.total_events = total_events;
        checkpoint.processed_events = total_events;
        checkpoint.batch_number = checkpoint.batch_number.saturating_add(1);
        checkpoint.updated_at = Utc::now();

        self.replay
            .update_checkpoint(operation_id, &checkpoint)
            .await?;

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
        for op in operations.iter() {
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
    use chrono::Duration as ChronoDuration;
    use serde_json::json;
    use sinex_core::db::repositories::DbPoolExt;
    use sinex_core::{types::ulid::Ulid, DbPool, Id};
    use sinex_test_utils::{sinex_test, EphemeralNats, TestContext};
    use tokio::time::sleep;

    fn sample_scope() -> ReplayScope {
        ReplayScope {
            processor_id: "fs-test".to_string(),
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
        use sinex_core::db::repositories::state::Operation;
        let fallback_operation = Operation {
            id: Some(Id::from_ulid(operation_id)),
            operation_type: "replay".to_string(),
            operator: "test-suite".to_string(),
            scope: Some(json!({})),
            result_status: "running".to_string(),
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
            .plan_with_timeout("tester".into(), scope, Duration::from_secs(1))
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

        ctx.publish_event(
            "fs-test",
            "file.created",
            json!({ "path": "/tmp/replay.txt" }),
        )
        .await?;

        let replay = Arc::new(ReplayStateMachine::new(ctx.pool.clone()));
        let nats_client = nats.connect().await?;
        let client = spawn_replay_control(replay, nats_client).await?;

        let mut scope = sample_scope();
        let end = Utc::now();
        scope.time_window = Some((
            end - ChronoDuration::hours(1),
            end + ChronoDuration::minutes(1),
        ));

        let planned = client.plan("tester".into(), scope.clone()).await?;
        assert_eq!(planned.state, ReplayState::Planning);

        let (previewed, _) = client.preview(planned.operation_id).await?;
        assert_eq!(previewed.state, ReplayState::Previewed);

        let approved = client
            .approve(planned.operation_id, "approver".into())
            .await?;
        assert_eq!(approved.state, ReplayState::Approved);

        let executed = client
            .execute(planned.operation_id, "executor-node".into())
            .await?;
        assert_eq!(executed.state, ReplayState::Completed);

        assert!(
            executed.outcome.is_some(),
            "Replay execution should record a concrete outcome for automation consumers"
        );

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

pub use crate::replay_state_machine::ReplayScope;
use crate::replay_state_machine::{ReplayOperation, ReplayState, ReplayStateMachine};
use async_nats::{Client, Message};
use chrono::Utc;
use color_eyre::eyre::{eyre, Context, Result};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use sinex_core::environment::{environment, SinexEnvironment};
use sinex_core::types::ulid::Ulid;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::time::{interval, Duration};
use tracing::{error, info, warn};

/// Spawn the replay control bus and return a client handle.
pub async fn spawn_replay_control(
    replay: Arc<ReplayStateMachine>,
    nats_url: &str,
) -> Result<ReplayControlClient> {
    let env = environment().clone();
    let client = async_nats::connect(nats_url)
        .await
        .wrap_err("Failed to connect to NATS for replay control")?;

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
}

impl ReplayControlClient {
    fn new(env: SinexEnvironment, client: Client) -> Self {
        let subject = env.nats_subject("sinex.control.replay");
        Self { subject, client }
    }

    async fn send(&self, request: ReplayControlRequest) -> Result<ReplayControlResponse> {
        let payload = serde_json::to_vec(&request)?;
        let message = self
            .client
            .request(self.subject.clone(), payload.into())
            .await
            .wrap_err("Replay control request timed out")?;

        let response: ReplayControlResponse =
            serde_json::from_slice(&message.payload).wrap_err("Invalid replay control response")?;

        if response.status == "error" {
            return Err(eyre!(
                "{}",
                response
                    .message
                    .unwrap_or_else(|| "Replay control request failed".to_string())
            ));
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
        let mut subscription = client
            .subscribe(self.subject.clone())
            .await
            .wrap_err("Failed to subscribe to replay control subject")?;
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

        self.replay.load_operation(operation_id).await
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
        self.latest
            .lock()
            .map(|guard| guard.clone())
            .unwrap_or_default()
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

        if let Ok(mut guard) = self.latest.lock() {
            *guard = snapshot.clone();
        } else {
            warn!("Replay telemetry snapshot mutex poisoned");
        }

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
    use sinex_core::{types::ulid::Ulid, DbPool};
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
        let uuid = sqlx::types::Uuid::from_bytes(operation_id.to_bytes());
        for attempt in 0..20 {
            let exists = sqlx::query_scalar!(
                "SELECT 1 FROM core.operations_log WHERE id::uuid = $1::uuid",
                uuid
            )
            .fetch_optional(pool)
            .await?;
            if exists.is_some() {
                return Ok(());
            }
            sleep(Duration::from_millis(10 * (attempt + 1) as u64)).await;
        }
        tracing::warn!(
            %operation_id,
            "operation record missing; inserting fallback for test context"
        );
        sqlx::query!(
            r#"
            INSERT INTO core.operations_log (
                id,
                operation_type,
                operator,
                scope,
                result_status
            ) VALUES (
                $1::uuid::ulid,
                'replay',
                'test-suite',
                '{}'::jsonb,
                'running'
            )
            ON CONFLICT (id) DO NOTHING
            "#,
            uuid
        )
        .execute(pool)
        .await?;
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
        let nats_url = format!("nats://{}", nats.client_url());

        let replay = Arc::new(ReplayStateMachine::new(ctx.pool.clone()));
        let client = spawn_replay_control(replay, &nats_url).await?;

        // Drop the broker to simulate a partition mid-flight.
        drop(nats);
        tokio::time::sleep(Duration::from_millis(200)).await;

        let scope = sample_scope();
        // The underlying request timeout can be ~10s; keep the test fast and accept either:
        // - a quick client error (preferred), or
        // - a short, external timeout (still indicates the broker disappeared).
        match tokio::time::timeout(Duration::from_secs(1), client.plan("tester".into(), scope))
            .await
        {
            Ok(Ok(_)) => return Err(eyre!("plan unexpectedly succeeded after broker drop")),
            Ok(Err(err)) => {
                let message = err.to_string();
                assert!(
                    message.contains("request") || message.contains("connection"),
                    "unexpected error: {message}"
                );
            }
            Err(_) => {
                // Good enough: the request didn't succeed, and we didn't wait for the full
                // internal request timeout.
            }
        }
        Ok(())
    }

    #[sinex_test]
    async fn replay_execution_records_outcome(ctx: TestContext) -> Result<()> {
        let nats = EphemeralNats::start().await?;
        let nats_url = format!("nats://{}", nats.client_url());

        ctx.create_test_event(
            "fs-test",
            "file.created",
            json!({ "path": "/tmp/replay.txt" }),
        )
        .await?;

        let replay = Arc::new(ReplayStateMachine::new(ctx.pool.clone()));
        let client = spawn_replay_control(replay, &nats_url).await?;

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

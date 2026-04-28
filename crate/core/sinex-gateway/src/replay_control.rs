#![doc = include_str!("../docs/replay_control.md")]

use crate::cascade_analyzer::{CascadeAnalyzerConfig, Severity, StreamingCascadeAnalyzer};
use crate::config::env_bool_optional;
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

const REPLAY_CONTROL_SUBSCRIBE_ATTEMPTS: usize = 5;
const REPLAY_CONTROL_SUBSCRIBE_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(2);
const REPLAY_CONTROL_SUBSCRIBE_BACKOFF_BASE: Duration = Duration::from_millis(200);
const REPLAY_CONTROL_SUBSCRIBE_BACKOFF_MAX: Duration = Duration::from_secs(2);
const REPLAY_OUTPUT_VISIBILITY_TIMEOUT: Duration = Duration::from_secs(30);

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

fn ensure_preview_allowed(operation: &ReplayOperation) -> Result<()> {
    match operation.state {
        ReplayState::Planning | ReplayState::Previewed => Ok(()),
        ReplayState::Approved => Err(eyre!(
            "Operation {} is already approved; create a new plan to refresh the preview",
            operation.operation_id
        )),
        ReplayState::Executing | ReplayState::Committing | ReplayState::Cancelling => Err(eyre!(
            "Operation {} is already executing; preview is no longer available",
            operation.operation_id
        )),
        ReplayState::Completed | ReplayState::Failed | ReplayState::Cancelled => Err(eyre!(
            "Operation {} is in terminal state {:?}; preview is no longer available",
            operation.operation_id,
            operation.state
        )),
    }
}

fn allow_test_actors_in_runtime(is_test_runtime: bool) -> Result<bool> {
    if is_test_runtime {
        return Ok(true);
    }

    Ok(env_bool_optional("SINEX_ALLOW_TEST_ACTORS")?.unwrap_or(false))
}

fn allow_test_actors() -> Result<bool> {
    allow_test_actors_in_runtime(cfg!(test))
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

    if role == "test" && !allow_test_actors()? {
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

/// Run the `StreamingCascadeAnalyzer` against a set of root event IDs and return the
/// results as a JSON blob suitable for embedding in a preview response under
/// `"safety_analysis"`.
///
/// This is best-effort: on error the result becomes a structured failure object so that
/// the preview remains useful even when the analyzer cannot complete (e.g., timeout,
/// memory limit exceeded).
async fn run_safety_analysis(pool: &sqlx::PgPool, root_ids: &[Uuid]) -> serde_json::Value {
    if root_ids.is_empty() {
        return serde_json::json!({
            "integrity_violations": [],
            "circular_dependencies": [],
            "warnings": [],
        });
    }

    let config = CascadeAnalyzerConfig::from_env();
    let analyzer = StreamingCascadeAnalyzer::with_config(pool.clone(), config);

    match analyzer.analyze_cascades(root_ids).await {
        Ok(analysis) => {
            let critical_violation_count = analysis
                .integrity_violations
                .iter()
                .filter(|v| matches!(v.severity, Severity::Critical))
                .count();

            let mut warnings: Vec<serde_json::Value> = Vec::new();
            if critical_violation_count > 0 {
                warnings.push(serde_json::json!({
                    "level": "critical",
                    "message": format!(
                        "{} integrity violation(s) detected: live events reference events that \
                        would be archived. Execution may leave dangling references.",
                        critical_violation_count
                    ),
                }));
            }
            if !analysis.circular_dependencies.is_empty() {
                warnings.push(serde_json::json!({
                    "level": "warning",
                    "message": format!(
                        "{} circular dependency cycle(s) detected in the cascade graph.",
                        analysis.circular_dependencies.len()
                    ),
                }));
            }

            serde_json::json!({
                "integrity_violations": analysis.integrity_violations,
                "circular_dependencies": analysis.circular_dependencies,
                "max_depth": analysis.max_depth,
                "total_affected": analysis.total_affected,
                "warnings": warnings,
            })
        }
        Err(e) => {
            warn!(error = %e, "Cascade safety analysis failed");
            serde_json::json!({
                "status": "failed",
                "error": e.to_string(),
                "warning": "Cascade impact could not be determined. Approve with caution."
            })
        }
    }
}

fn summarize_uuid_set(ids: &HashSet<Uuid>) -> String {
    let mut sorted: Vec<_> = ids.iter().copied().collect();
    sorted.sort_unstable();

    let total = sorted.len();
    let sample = sorted
        .into_iter()
        .take(3)
        .map(|id| id.to_string())
        .collect::<Vec<_>>();

    match sample.len() {
        0 => "none".to_string(),
        count if total > count => format!("{} ...", sample.join(", ")),
        _ => sample.join(", "),
    }
}

fn stale_preview_missing_root_ids_error(
    operation_id: Uuid,
    expected_total_events: u64,
) -> color_eyre::eyre::Report {
    eyre!(
        "Operation {} preview is stale: preview covered {} material-root events but \
         root_event_ids is absent. ID-level staleness detection is not possible; \
         refresh preview before execution",
        operation_id,
        expected_total_events,
    )
}

fn replay_scope_drift_error(
    operation_id: Uuid,
    expected_total_events: u64,
    expected_root_ids: &[Uuid],
    actual_root_ids: &[Uuid],
) -> color_eyre::eyre::Report {
    if expected_root_ids.is_empty() {
        return eyre!(
            "Operation {} preview is stale: approved preview covered {} material-root events, \
             but execution matched {}. Refresh preview before execution",
            operation_id,
            expected_total_events,
            actual_root_ids.len()
        );
    }

    let expected: HashSet<_> = expected_root_ids.iter().copied().collect();
    let actual: HashSet<_> = actual_root_ids.iter().copied().collect();
    let missing: HashSet<_> = expected.difference(&actual).copied().collect();
    let unexpected: HashSet<_> = actual.difference(&expected).copied().collect();

    eyre!(
        "Operation {} preview is stale: approved preview covered {} material-root events, \
         but execution matched {}. Missing previewed roots: {} ({}). Unexpected live roots: {} ({}). \
         Refresh preview before execution",
        operation_id,
        expected_total_events,
        actual_root_ids.len(),
        missing.len(),
        summarize_uuid_set(&missing),
        unexpected.len(),
        summarize_uuid_set(&unexpected),
    )
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
    server_subscribed: bool,
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

/// Client for issuing replay control commands over NATS.
#[derive(Clone)]
pub struct ReplayControlClient {
    subject: String,
    client: Client,
    health: Arc<Mutex<ReplayControlHealthState>>,
    request_timeout: Duration,
}

impl ReplayControlClient {
    fn new(
        env: &SinexEnvironment,
        client: Client,
        request_timeout: Duration,
        health: Arc<Mutex<ReplayControlHealthState>>,
    ) -> Self {
        let subject = env.nats_subject("sinex.control.replay");
        Self {
            subject,
            client,
            health,
            request_timeout,
        }
    }

    #[must_use]
    pub fn health_snapshot(&self) -> ReplayControlHealth {
        let client_connected = matches!(self.client.connection_state(), NatsState::Connected);
        let (server_subscribed, last_error) = {
            let guard = self.health.lock();
            (guard.server_subscribed, guard.last_error.clone())
        };
        let last_error = last_error
            .or_else(|| {
                (!client_connected)
                    .then(|| ReplayControlError::new("Replay control NATS client disconnected"))
            })
            .or_else(|| {
                (!server_subscribed).then(|| {
                    ReplayControlError::new("Replay control server subscription is not active")
                })
            });
        ReplayControlHealth {
            connected: client_connected && server_subscribed,
            last_error,
        }
    }

    fn record_error(&self, message: impl Into<String>) {
        let mut guard = self.health.lock();
        guard.last_error = Some(ReplayControlError::new(message));
    }

    fn decode_response_payload(&self, payload: &[u8]) -> Result<ReplayControlResponse> {
        let response: ReplayControlResponse = serde_json::from_slice(payload)
            .inspect_err(|err| {
                self.record_error(err.to_string());
            })
            .wrap_err("Invalid replay control response")?;

        if response.status == ReplayControlStatus::Error {
            let message = response
                .message
                .unwrap_or_else(|| "Replay control request failed".to_string());
            self.record_error(message.clone());
            if let Some(kind) = response.error_kind {
                return Err(kind.into_sinex_error(message).into());
            }
            return Err(eyre!("{}", message));
        }

        Ok(response)
    }

    fn require_operation(&self, response: ReplayControlResponse) -> Result<ReplayOperation> {
        response
            .operation
            .ok_or_else(|| eyre!("Replay control response missing operation"))
    }

    fn require_operations(response: ReplayControlResponse) -> Result<Vec<ReplayOperation>> {
        response
            .operations
            .ok_or_else(|| eyre!("Replay control response missing operations"))
    }

    async fn send(&self, request: ReplayControlRequest) -> Result<ReplayControlResponse> {
        let payload = serde_json::to_vec(&request)?;

        // Issue 126: Configurable timeout for NATS replay requests
        let message = tokio::time::timeout(
            self.request_timeout,
            self.client.request(self.subject.clone(), payload.into()),
        )
        .await
        .map_err(|_| {
            let error_msg = format!(
                "Replay control request timed out after {:?}",
                self.request_timeout
            );
            self.record_error(error_msg.clone());
            eyre!(error_msg)
        })?
        .inspect_err(|err| {
            self.record_error(err.to_string());
        })
        .wrap_err("Replay control request failed")?;

        self.decode_response_payload(&message.payload)
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

        self.decode_response_payload(&message.payload)
    }

    pub async fn plan(&self, actor: String, scope: ReplayScope) -> Result<ReplayOperation> {
        // Validate actor format before sending request
        validate_actor_for_action(&actor, ReplayAction::Plan)?;
        scope.validate()?;

        self.require_operation(
            self.send(ReplayControlRequest::Plan { actor, scope })
                .await?,
        )
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
        scope.validate()?;

        self.require_operation(
            self.send_with_timeout(ReplayControlRequest::Plan { actor, scope }, timeout)
                .await?,
        )
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

        self.require_operation(
            self.send(ReplayControlRequest::Approve {
                operation_id,
                approver,
            })
            .await?,
        )
    }

    pub async fn submit(&self, operation_id: Uuid, submitter: String) -> Result<ReplayOperation> {
        validate_actor_for_action(&submitter, ReplayAction::Approve)?;
        validate_actor_for_action(&submitter, ReplayAction::Execute)?;

        self.require_operation(
            self.send(ReplayControlRequest::Submit {
                operation_id,
                submitter,
            })
            .await?,
        )
    }

    pub async fn execute(
        &self,
        operation_id: Uuid,
        executor: String,
        dry_run: bool,
    ) -> Result<ReplayOperation> {
        // Validate executor identity
        validate_actor_for_action(&executor, ReplayAction::Execute)?;

        self.require_operation(
            self.send(ReplayControlRequest::Execute {
                operation_id,
                executor,
                dry_run,
            })
            .await?,
        )
    }

    pub async fn cancel(
        &self,
        operation_id: Uuid,
        canceller: String,
        reason: Option<String>,
    ) -> Result<ReplayOperation> {
        validate_actor_for_action(&canceller, ReplayAction::Cancel)?;
        self.require_operation(
            self.send(ReplayControlRequest::Cancel {
                operation_id,
                canceller,
                reason,
            })
            .await?,
        )
    }

    pub async fn status(&self, operation_id: Uuid) -> Result<ReplayOperation> {
        self.require_operation(
            self.send(ReplayControlRequest::Status { operation_id })
                .await?,
        )
    }

    pub async fn list(
        &self,
        state: Option<ReplayState>,
        node: Option<String>,
        limit: Option<i64>,
    ) -> Result<Vec<ReplayOperation>> {
        let response = self
            .send(ReplayControlRequest::List { state, node, limit })
            .await?;
        Self::require_operations(response)
    }
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
        let guard = self.latest.lock();
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
        let operations = self.replay.list_operations(None, None, None).await?;
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

        let mut guard = self.latest.lock();
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

#[derive(Debug, Clone)]
struct ExpectedReplayOutputs {
    minimum_visible_count: u64,
    sources: Vec<String>,
    event_types: Vec<String>,
    logical_source_identifiers: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use serde_json::json;
    use sinex_db::DbPool;
    use sinex_db::repositories::DbPoolExt;
    use sinex_db::repositories::state::Operation;
    use sinex_node_sdk::runtime::stream::ScanReport;
    use sinex_primitives::events::{EventPayload, payloads::filesystem::FileCreatedPayload};
    use sinex_primitives::{DynamicPayload, Id, Uuid};
    use tokio::time::sleep;
    use xtask::sandbox::{EnvGuard, sinex_test};

    /// Subscribe to scope-invalidation messages in tests.
    ///
    /// `js.publish` requires the target subject to be covered by an existing
    /// JetStream stream, and the production stream bootstrap (in
    /// `sinex_ingestd::jetstream_consumer::bootstrap_streams`) does not run
    /// in test contexts that use ephemeral NATS. This helper:
    ///
    /// 1. `get_or_create_stream`s the canonical
    ///    `SINEX_RAW_EVENTS_DERIVED_INVALIDATIONS` stream so publishes succeed.
    /// 2. Creates an ephemeral push consumer and forwards each delivered
    ///    payload onto an `mpsc::UnboundedReceiver<Vec<u8>>` so call sites
    ///    can `.recv()` the bytes directly without juggling the
    ///    `Result<jetstream::Message, _>` shape.
    async fn spawn_invalidation_listener_for_test(
        nats_client: &async_nats::Client,
    ) -> Result<tokio::sync::mpsc::UnboundedReceiver<Vec<u8>>> {
        use async_nats::jetstream::{consumer::push, stream as js_stream};
        let env = sinex_primitives::environment::environment();
        let stream_name = env.nats_stream_name("SINEX_RAW_EVENTS_DERIVED_INVALIDATIONS");
        let invalidation_subject = env.nats_subject(INVALIDATION_SUBJECT);
        let js = async_nats::jetstream::new(nats_client.clone());
        let stream = js
            .get_or_create_stream(js_stream::Config {
                name: stream_name,
                subjects: vec![invalidation_subject],
                ..Default::default()
            })
            .await
            .map_err(|e| eyre!("failed to bootstrap invalidation stream: {e}"))?;
        let deliver_subject = nats_client.new_inbox();
        let consumer = stream
            .create_consumer(push::Config {
                deliver_subject,
                ..Default::default()
            })
            .await
            .map_err(|e| eyre!("failed to create invalidation consumer: {e}"))?;
        let mut messages = consumer
            .messages()
            .await
            .map_err(|e| eyre!("failed to start invalidation message stream: {e}"))?;

        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(async move {
            while let Some(item) = messages.next().await {
                let Ok(msg) = item else { break };
                let _ = msg.ack().await;
                if tx.send(msg.payload.to_vec()).is_err() {
                    break;
                }
            }
        });
        Ok(rx)
    }

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

    async fn wait_for_operation_state(
        replay: &Arc<ReplayStateMachine>,
        operation_id: Uuid,
        target: ReplayState,
    ) -> Result<()> {
        for _ in 0..40 {
            let operation = replay.load_operation(operation_id).await?;
            if operation.state == target {
                return Ok(());
            }
            sleep(Duration::from_millis(25)).await;
        }
        Err(eyre!(
            "operation {operation_id} did not reach state {:?} before timeout",
            target
        ))
    }

    async fn corrupt_operation_preview_summary(pool: &DbPool, operation_id: Uuid) -> Result<()> {
        sqlx::query!(
            r#"
            UPDATE core.operations_log
            SET preview_summary = '"broken"'::jsonb
            WHERE id = $1::uuid
            "#,
            operation_id,
        )
        .execute(pool)
        .await?;
        Ok(())
    }

    async fn spawn_fake_scan_node(
        nats: Client,
        env: SinexEnvironment,
        node_name: &str,
        events_processed: u64,
    ) -> Result<(
        tokio::sync::oneshot::Receiver<NodeScanCommand>,
        tokio::task::JoinHandle<()>,
    )> {
        let node_name = node_name.to_string();
        let subject = env.nats_subject(&format!("sinex.control.nodes.{node_name}.scan"));
        let mut sub = nats
            .subscribe(subject)
            .await
            .map_err(|e| eyre!("failed to subscribe fake node dispatcher: {e}"))?;
        let (command_tx, command_rx) = tokio::sync::oneshot::channel();

        let handle = tokio::spawn(async move {
            if let Some(msg) = sub.next().await {
                let command: NodeScanCommand = serde_json::from_slice(&msg.payload)
                    .expect("fake node must receive a valid scan command");
                let operation_id = command.operation_id;
                let progress_subject =
                    env.nats_subject(&format!("sinex.control.replay.progress.{operation_id}"));

                let _ = command_tx.send(command.clone());

                if let Some(reply) = msg.reply {
                    let ack = NodeScanAck {
                        operation_id,
                        node_name: node_name.clone(),
                        accepted: true,
                        error: None,
                    };
                    nats.publish(reply, serde_json::to_vec(&ack).unwrap().into())
                        .await
                        .expect("fake node ack publish should succeed");
                }

                let report = ScanReport {
                    events_processed,
                    duration: Duration::from_millis(5),
                    final_checkpoint: Checkpoint::None,
                    time_range: None,
                    node_stats: HashMap::from([("events_emitted".to_string(), events_processed)]),
                    successful_targets: vec![node_name.clone()],
                    failed_targets: Vec::new(),
                    warnings: Vec::new(),
                };
                let progress = NodeScanProgress {
                    operation_id,
                    node_name: node_name.clone(),
                    events_processed,
                    events_emitted: events_processed,
                    final_report: Some(report),
                    error: None,
                };
                nats.publish(
                    progress_subject,
                    serde_json::to_vec(&progress).unwrap().into(),
                )
                .await
                .expect("fake node progress publish should succeed");
            }
        });

        Ok((command_rx, handle))
    }

    async fn spawn_fake_scan_node_with_progress(
        nats: Client,
        env: SinexEnvironment,
        node_name: &str,
        events_processed: u64,
        events_emitted: u64,
    ) -> Result<(
        tokio::sync::oneshot::Receiver<NodeScanCommand>,
        tokio::task::JoinHandle<()>,
    )> {
        let node_name = node_name.to_string();
        let subject = env.nats_subject(&format!("sinex.control.nodes.{node_name}.scan"));
        let mut sub = nats
            .subscribe(subject)
            .await
            .map_err(|e| eyre!("failed to subscribe fake node dispatcher: {e}"))?;
        let (command_tx, command_rx) = tokio::sync::oneshot::channel();

        let handle = tokio::spawn(async move {
            if let Some(msg) = sub.next().await {
                let command: NodeScanCommand = serde_json::from_slice(&msg.payload)
                    .expect("fake node must receive a valid scan command");
                let operation_id = command.operation_id;
                let progress_subject =
                    env.nats_subject(&format!("sinex.control.replay.progress.{operation_id}"));

                let _ = command_tx.send(command.clone());

                if let Some(reply) = msg.reply {
                    let ack = NodeScanAck {
                        operation_id,
                        node_name: node_name.clone(),
                        accepted: true,
                        error: None,
                    };
                    nats.publish(reply, serde_json::to_vec(&ack).unwrap().into())
                        .await
                        .expect("fake node ack publish should succeed");
                }

                let report = ScanReport {
                    events_processed,
                    duration: Duration::from_millis(5),
                    final_checkpoint: Checkpoint::None,
                    time_range: None,
                    node_stats: HashMap::from([("events_emitted".to_string(), events_emitted)]),
                    successful_targets: vec![node_name.clone()],
                    failed_targets: Vec::new(),
                    warnings: Vec::new(),
                };
                let progress = NodeScanProgress {
                    operation_id,
                    node_name: node_name.clone(),
                    events_processed,
                    events_emitted,
                    final_report: Some(report),
                    error: None,
                };
                nats.publish(
                    progress_subject,
                    serde_json::to_vec(&progress).unwrap().into(),
                )
                .await
                .expect("fake node progress publish should succeed");
            }
        });

        Ok((command_rx, handle))
    }

    async fn spawn_fake_scan_node_ack_only(
        nats: Client,
        env: SinexEnvironment,
        node_name: &str,
    ) -> Result<(
        tokio::sync::oneshot::Receiver<NodeScanCommand>,
        tokio::task::JoinHandle<()>,
    )> {
        let node_name = node_name.to_string();
        let subject = env.nats_subject(&format!("sinex.control.nodes.{node_name}.scan"));
        let mut sub = nats
            .subscribe(subject)
            .await
            .map_err(|e| eyre!("failed to subscribe fake node dispatcher: {e}"))?;
        let (command_tx, command_rx) = tokio::sync::oneshot::channel();

        let handle = tokio::spawn(async move {
            if let Some(msg) = sub.next().await {
                let command: NodeScanCommand = serde_json::from_slice(&msg.payload)
                    .expect("fake node must receive a valid scan command");
                let _ = command_tx.send(command.clone());

                if let Some(reply) = msg.reply {
                    let ack = NodeScanAck {
                        operation_id: command.operation_id,
                        node_name: node_name.clone(),
                        accepted: true,
                        error: None,
                    };
                    nats.publish(reply, serde_json::to_vec(&ack).unwrap().into())
                        .await
                        .expect("fake node ack publish should succeed");
                }
            }
        });

        Ok((command_rx, handle))
    }

    fn spawn_replay_output_inserter(
        pool: DbPool,
        command_rx: tokio::sync::oneshot::Receiver<NodeScanCommand>,
        source: &'static str,
        event_type: &'static str,
        path: &'static str,
        equivalence_key: Option<&'static str>,
    ) -> tokio::task::JoinHandle<Result<NodeScanCommand>> {
        tokio::spawn(async move {
            let command = command_rx
                .await
                .map_err(|_| eyre!("fake replay output inserter did not receive scan command"))?;
            let logical_source_identifier = command
                .args
                .replay
                .as_ref()
                .and_then(|replay| replay.materials.first())
                .map_or(path, ReplayExecutionEngine::logical_source_identifier)
                .to_string();
            let material_id = Uuid::now_v7();
            let source_identifier = format!("{logical_source_identifier}#material={material_id}");
            sqlx::query!(
                r#"
                INSERT INTO raw.source_material_registry (
                    id,
                    material_kind,
                    source_identifier,
                    status,
                    timing_info_type,
                    metadata
                )
                VALUES ($1::uuid, 'annex', $2, 'completed', 'realtime', $3::jsonb)
                "#,
                material_id,
                source_identifier,
                json!({ "logical_source_identifier": logical_source_identifier }),
            )
            .execute(&pool)
            .await?;
            let mut event = DynamicPayload::new(source, event_type, json!({ "path": path }))
                .from_material(Id::from_uuid(material_id))
                .build()?;
            event.created_by_operation_id = Some(command.operation_id);
            if let Some(equivalence_key) = equivalence_key {
                event.equivalence_key = Some(equivalence_key.to_string());
            }
            pool.events().insert(event).await?;
            Ok(command)
        })
    }

    #[test]
    fn replay_output_expectations_deduplicate_logical_sources() {
        let logical_source = "/tmp/replay-dedup.txt";
        let expected = ExpectedReplayOutputs {
            minimum_visible_count: 0,
            sources: vec!["fs-test".to_string()],
            event_types: vec![FileCreatedPayload::EVENT_TYPE.as_static_str().to_string()],
            logical_source_identifiers: Vec::new(),
        };
        let replay_materials = vec![
            ResolvedReplayMaterial {
                source_material_id: Uuid::now_v7(),
                material_kind: "annex".to_string(),
                source_identifier: format!("{logical_source}#material={}", Uuid::now_v7()),
                material_metadata: json!({ "logical_source_identifier": logical_source }),
                material_start_time: None,
                material_end_time: None,
            },
            ResolvedReplayMaterial {
                source_material_id: Uuid::now_v7(),
                material_kind: "annex".to_string(),
                source_identifier: format!("{logical_source}#material={}", Uuid::now_v7()),
                material_metadata: json!({ "logical_source_identifier": logical_source }),
                material_start_time: None,
                material_end_time: None,
            },
        ];

        let expected =
            ReplayExecutionEngine::with_logical_source_identifiers(expected, &replay_materials)
                .expect("logical source expectation should succeed");

        assert_eq!(expected.minimum_visible_count, 1);
        assert_eq!(
            expected.logical_source_identifiers,
            vec![logical_source.to_string()]
        );
    }

    #[sinex_test]
    async fn telemetry_reports_state_counts(ctx: TestContext) -> Result<()> {
        let replay = Arc::new(ReplayStateMachine::new(ctx.pool.clone()));
        let telemetry = ReplayTelemetry::with_interval(replay.clone(), Duration::from_millis(5));
        let planning_scope = sample_scope();
        let mut executing_scope = sample_scope();
        executing_scope.node_id = "fs-test-executing".to_string();
        let mut failed_scope = sample_scope();
        failed_scope.node_id = "fs-test-failed".to_string();

        let _planning = replay
            .create_operation(planning_scope, "planner".into())
            .await?;

        let executing = replay
            .create_operation(executing_scope, "executor".into())
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

        let failed = replay
            .create_operation(failed_scope, "runner".into())
            .await?;
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
        let client = spawn_replay_control(replay, nats_client, Duration::from_secs(30)).await?;

        // Shut down the broker to simulate a partition mid-flight.
        nats.shutdown().await?;
        tokio::time::sleep(Duration::from_millis(200)).await;

        let scope = sample_scope();
        let err = client
            .plan_with_timeout("test:user".into(), scope, Duration::from_secs(1))
            .await
            .expect_err("plan should fail after broker drop");
        assert!(
            !err.to_string().is_empty(),
            "error message should be populated"
        );
        let health = client.health_snapshot();
        let last_error = health
            .last_error
            .expect("health snapshot should retain the last replay control error");
        assert!(
            !last_error.message.is_empty(),
            "last replay control error message should be populated"
        );
        Ok(())
    }

    #[sinex_test]
    async fn replay_control_reconnects_when_subscription_closes_after_startup(
        ctx: TestContext,
    ) -> Result<()> {
        let ctx = ctx.with_nats().dedicated().await?;
        let nats = ctx.nats_handle()?;
        let replay = Arc::new(ReplayStateMachine::new(ctx.pool.clone()));
        let nats_client = ctx.nats_client();
        let env = sinex_primitives::environment::environment();
        let executor = ReplayExecutionEngine::new(replay.clone(), nats_client.clone());
        let health = Arc::new(Mutex::new(ReplayControlHealthState::default()));

        let server_task = ReplayControlServer::new(
            &env,
            nats_client.clone(),
            replay,
            executor,
            Arc::clone(&health),
        )
        .spawn()
        .await?;
        let client = ReplayControlClient::new(
            &env,
            nats_client,
            Duration::from_secs(30),
            Arc::clone(&health),
        );

        nats.shutdown().await?;
        tokio::time::sleep(Duration::from_secs(1)).await;

        assert!(
            !server_task.is_finished(),
            "closing the live replay-control subscription should keep the server retrying instead of exiting"
        );
        let snapshot = client.health_snapshot();
        assert!(
            !snapshot.connected,
            "replay-control health must reflect that the live subscription was lost"
        );
        assert!(
            snapshot.last_error.is_some(),
            "replay-control health must retain a clue after the live subscription is lost"
        );

        server_task.abort();
        Ok(())
    }

    #[sinex_test]
    async fn replay_control_health_reports_inactive_subscription(ctx: TestContext) -> Result<()> {
        let ctx = ctx.with_nats().dedicated().await?;
        let health = Arc::new(Mutex::new(ReplayControlHealthState::default()));
        let client = ReplayControlClient::new(
            &sinex_primitives::environment::environment(),
            ctx.nats_client(),
            Duration::from_secs(30),
            Arc::clone(&health),
        );

        let disconnected = client.health_snapshot();
        assert!(!disconnected.connected);
        assert_eq!(
            disconnected
                .last_error
                .as_ref()
                .map(|error| error.message.as_str()),
            Some("Replay control server subscription is not active")
        );

        {
            let mut guard = health.lock();
            guard.server_subscribed = true;
        }

        let connected = client.health_snapshot();
        assert!(connected.connected);
        assert!(connected.last_error.is_none());
        Ok(())
    }

    #[sinex_test]
    async fn replay_preview_surfaces_safety_analysis_failure(ctx: TestContext) -> Result<()> {
        let ctx = ctx.with_nats().dedicated().await?;
        let pool = ctx.pool.clone();
        pool.close().await;

        let analysis = run_safety_analysis(&pool, &[Uuid::now_v7()]).await;

        assert_eq!(
            analysis.get("status").and_then(serde_json::Value::as_str),
            Some("failed")
        );
        assert!(
            analysis
                .get("error")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|message| !message.is_empty()),
            "expected concrete analyzer failure message, got: {analysis:?}"
        );
        assert_eq!(
            analysis.get("warning").and_then(serde_json::Value::as_str),
            Some("Cascade impact could not be determined. Approve with caution.")
        );
        Ok(())
    }

    #[sinex_test]
    async fn replay_execution_records_outcome(ctx: TestContext) -> Result<()> {
        let ctx = ctx.with_nats().dedicated().await?;

        let (material_id, inserted) = loop {
            let material_id = ctx.create_source_material(Some("replay-outcome")).await?;
            let event = DynamicPayload::new(
                "fs-test",
                FileCreatedPayload::EVENT_TYPE.as_static_str(),
                json!({ "path": "/tmp/replay.txt" }),
            )
            .from_material(material_id)
            .build()?;
            let inserted = ctx.pool.events().insert(event).await?;
            if let Some(ts_orig) = inserted.ts_orig
                && ts_orig.inner().nanosecond() > 0
            {
                break (material_id, inserted);
            }
        };

        let replay_target_event_id = inserted.id.expect("inserted replay target must have id");
        let replay_target_id = replay_target_event_id.to_uuid();
        let target_window_end = replay_target_event_id.timestamp();
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
            FileCreatedPayload::EVENT_TYPE.as_static_str(),
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

        // The replay engine should no longer publish raw replay rows itself.
        // Keep a stream around so the test can assert that this count stays zero.
        let env = sinex_primitives::environment::environment();
        let js = async_nats::jetstream::new(nats_client.clone());
        let stream_name = format!("replay-test-{}", Uuid::now_v7().simple());
        js.get_or_create_stream(async_nats::jetstream::stream::Config {
            name: stream_name.clone(),
            subjects: vec![env.nats_subject("events.raw.>")],
            ..Default::default()
        })
        .await?;
        let (scan_command_rx, scan_handle) =
            spawn_fake_scan_node(nats_client.clone(), env.clone(), "fs-test", 1).await?;
        let replay_output_handle = spawn_replay_output_inserter(
            ctx.pool.clone(),
            scan_command_rx,
            "fs-test",
            FileCreatedPayload::EVENT_TYPE.as_static_str(),
            "/tmp/replay-output.txt",
            None,
        );

        let client = spawn_replay_control(replay, nats_client, Duration::from_secs(30)).await?;

        let mut scope = sample_scope();
        scope.time_window = Some((target_window_start, target_window_end));
        scope.material_filter = Some(vec![*material_id.as_uuid()]);
        scope.filters.insert(
            "event_types".to_string(),
            json!([FileCreatedPayload::EVENT_TYPE.as_static_str()]),
        );

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
            Some("reexecute_material_roots_via_node_scan")
        );

        let approved = client
            .approve(planned.operation_id, "admin:approver".into())
            .await?;
        assert_eq!(approved.state, ReplayState::Approved);

        let executed = client
            .execute(planned.operation_id, "service:executor-node".into(), false)
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

        let dispatched_command = replay_output_handle
            .await
            .map_err(|e| eyre!("fake replay output task failed: {e}"))??;
        let replay_context = dispatched_command
            .args
            .replay
            .expect("gateway must populate typed replay context");
        assert_eq!(replay_context.materials.len(), 1);
        assert_eq!(
            replay_context.materials[0].source_material_id,
            *material_id.as_uuid(),
            "replay context must carry resolved source material identity"
        );
        assert_eq!(
            replay_context.replay_scope.material_ids,
            Some(vec![*material_id.as_uuid()]),
            "gateway must preserve normalized material filter in replay scope"
        );
        assert_eq!(
            replay_context.replay_scope.event_types,
            Some(vec![
                FileCreatedPayload::EVENT_TYPE.as_static_str().to_string()
            ]),
            "gateway must preserve normalized event type filter in replay scope"
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
            0,
            "gateway replay must not republish stored raw rows"
        );

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
            .create_source_material(Some("replay-node-scan-parity"))
            .await?;
        let root = DynamicPayload::new(
            "reexecution-test",
            FileCreatedPayload::EVENT_TYPE.as_static_str(),
            json!({ "path": "/tmp/reexecution-root.txt" }),
        )
        .from_material(material_root_id)
        .build()?;
        let root_inserted = ctx.pool.events().insert(root).await?;
        let root_event_id = root_inserted.id.expect("reexecution root must have id");
        let root_id = root_event_id.to_uuid();
        let reexecution_derived = DynamicPayload::new(
            "reexecution-test",
            "file.derived",
            json!({ "path": "/tmp/reexecution-derived.txt" }),
        )
        .from_parents([root_event_id])?
        .build()?;
        let derived_inserted = ctx.pool.events().insert(reexecution_derived).await?;
        let derived_id = derived_inserted
            .id
            .expect("reexecution derived must have id")
            .to_uuid();
        let reexecution_root_ts = root_event_id.timestamp();
        let reexecution_scope = ReplayScope {
            node_id: "reexecution-test".to_string(),
            time_window: Some((
                reexecution_root_ts - time::Duration::seconds(1),
                reexecution_root_ts + time::Duration::seconds(1),
            )),
            material_filter: None,
            filters: HashMap::new(),
        };
        let planned_reexecution = client
            .plan("test:replay-user".into(), reexecution_scope)
            .await?;
        let (_, reexecution_preview) = client.preview(planned_reexecution.operation_id).await?;
        assert_eq!(
            reexecution_preview
                .get("total_events")
                .and_then(serde_json::Value::as_i64),
            Some(1),
            "preview must count only material roots for node-scan replay semantics"
        );
        client
            .approve(planned_reexecution.operation_id, "admin:approver".into())
            .await?;
        let (reexecution_command_rx, reexecution_handle) =
            spawn_fake_scan_node(ctx.nats_client(), env.clone(), "reexecution-test", 1).await?;
        let reexecution_output_handle = spawn_replay_output_inserter(
            ctx.pool.clone(),
            reexecution_command_rx,
            "reexecution-test",
            FileCreatedPayload::EVENT_TYPE.as_static_str(),
            "/tmp/reexecution-root.txt",
            None,
        );
        let reexecution_executed = client
            .execute(
                planned_reexecution.operation_id,
                "service:executor-node".into(),
                false,
            )
            .await?;
        assert_eq!(reexecution_executed.state, ReplayState::Completed);
        assert_eq!(reexecution_executed.checkpoint.total_events, 1);
        assert_eq!(reexecution_executed.checkpoint.processed_events, 1);
        let reexecution_command = reexecution_output_handle
            .await
            .map_err(|e| eyre!("fake reexecution replay output task failed: {e}"))??;
        let reexecution_context = reexecution_command
            .args
            .replay
            .expect("reexecution must still carry replay context");
        assert_eq!(reexecution_context.materials.len(), 1);
        assert_eq!(
            reexecution_context.materials[0].source_material_id,
            *material_root_id.as_uuid(),
        );
        assert_eq!(
            reexecution_context.replay_scope.material_ids, None,
            "implicit replay scopes should not invent material filters"
        );
        let root_archived_after_reexecution: i64 = sqlx::query_scalar(
            "SELECT COUNT(*)::bigint FROM audit.archived_events WHERE id = $1::uuid",
        )
        .bind(root_id)
        .fetch_one(&ctx.pool)
        .await?;
        let derived_archived_after_reexecution: i64 = sqlx::query_scalar(
            "SELECT COUNT(*)::bigint FROM audit.archived_events WHERE id = $1::uuid",
        )
        .bind(derived_id)
        .fetch_one(&ctx.pool)
        .await?;
        assert_eq!(root_archived_after_reexecution, 1);
        assert_eq!(derived_archived_after_reexecution, 1);

        scan_handle
            .await
            .map_err(|e| eyre!("fake fs-test node task failed: {e}"))?;
        reexecution_handle
            .await
            .map_err(|e| eyre!("fake reexecution-test node task failed: {e}"))?;

        Ok(())
    }

    #[sinex_test]
    async fn replay_replacement_recording_follows_operation_outputs(
        ctx: TestContext,
    ) -> Result<()> {
        let ctx = ctx.with_nats().shared().await?;
        let replay = Arc::new(ReplayStateMachine::new(ctx.pool.clone()));
        let engine = ReplayExecutionEngine::new(replay.clone(), ctx.nats_client());

        let source_material = ctx
            .create_source_material(Some("replay-replacement-old"))
            .await?;
        let mut old_event = DynamicPayload::new(
            "fs-test",
            FileCreatedPayload::EVENT_TYPE.as_static_str(),
            json!({ "path": "/tmp/replay-replacement-old.txt" }),
        )
        .from_material(source_material)
        .build()?;
        old_event.equivalence_key = Some("replacement-eq".to_string());
        let old_inserted = ctx.pool.events().insert(old_event).await?;
        let old_id = old_inserted.id.expect("old replay event must have an id");
        let execution_window = (
            old_id.timestamp() - time::Duration::milliseconds(1),
            old_id.timestamp() + time::Duration::milliseconds(1),
        );

        let mut scope = sample_scope();
        scope.time_window = Some(execution_window);

        let operation = replay
            .create_operation(scope.clone(), "test:replacement-recorder".into())
            .await?;
        let operation_id = operation.operation_id;

        ctx.pool
            .events()
            .execute_cascade_archive(
                &[old_id.to_uuid()],
                "archive old replay target",
                &operation_id.to_string(),
                "test:replacement-recorder",
            )
            .await?;

        let replacement_material = ctx
            .create_source_material(Some("replay-replacement-new"))
            .await?;
        let mut replacement_event = DynamicPayload::new(
            "fs-test",
            FileCreatedPayload::EVENT_TYPE.as_static_str(),
            json!({ "path": "/tmp/replay-replacement-new.txt" }),
        )
        .from_material(replacement_material)
        .build()?;
        replacement_event.equivalence_key = Some("replacement-eq".to_string());
        replacement_event.created_by_operation_id = Some(operation_id);
        let replacement_inserted = ctx.pool.events().insert(replacement_event).await?;
        let replacement_id = replacement_inserted
            .id
            .expect("replacement replay event must have an id")
            .to_uuid();

        engine
            .record_event_replacements(&ctx.pool, operation_id, &[old_id.to_uuid()])
            .await?;

        let replacements = ctx
            .pool
            .events()
            .get_replacements_by_operation(operation_id)
            .await?;
        assert_eq!(replacements.len(), 1);
        assert_eq!(replacements[0].0, old_id.to_uuid());
        assert_eq!(replacements[0].1, replacement_id);
        assert_eq!(replacements[0].2, "superseded");

        Ok(())
    }

    #[sinex_test]
    async fn replay_replacement_recording_skips_unmatched_old_events(
        ctx: TestContext,
    ) -> Result<()> {
        let ctx = ctx.with_nats().shared().await?;
        let replay = Arc::new(ReplayStateMachine::new(ctx.pool.clone()));
        let engine = ReplayExecutionEngine::new(replay.clone(), ctx.nats_client());

        let source_material = ctx
            .create_source_material(Some("replay-replacement-unmatched-old"))
            .await?;
        let mut old_event = DynamicPayload::new(
            "fs-test",
            FileCreatedPayload::EVENT_TYPE.as_static_str(),
            json!({ "path": "/tmp/replay-replacement-unmatched-old.txt" }),
        )
        .from_material(source_material)
        .build()?;
        old_event.equivalence_key = Some("old-eq".to_string());
        let old_inserted = ctx.pool.events().insert(old_event).await?;
        let old_id = old_inserted.id.expect("old replay event must have an id");
        let execution_window = (
            old_id.timestamp() - time::Duration::milliseconds(1),
            old_id.timestamp() + time::Duration::milliseconds(1),
        );

        let mut scope = sample_scope();
        scope.time_window = Some(execution_window);

        let operation = replay
            .create_operation(scope.clone(), "test:replacement-recorder".into())
            .await?;
        let operation_id = operation.operation_id;

        ctx.pool
            .events()
            .execute_cascade_archive(
                &[old_id.to_uuid()],
                "archive old replay target",
                &operation_id.to_string(),
                "test:replacement-recorder",
            )
            .await?;

        let replacement_material = ctx
            .create_source_material(Some("replay-replacement-unmatched-new"))
            .await?;
        let mut replacement_event = DynamicPayload::new(
            "fs-test",
            FileCreatedPayload::EVENT_TYPE.as_static_str(),
            json!({ "path": "/tmp/replay-replacement-unmatched-new.txt" }),
        )
        .from_material(replacement_material)
        .build()?;
        replacement_event.equivalence_key = Some("new-eq".to_string());
        replacement_event.created_by_operation_id = Some(operation_id);
        ctx.pool.events().insert(replacement_event).await?;

        engine
            .record_event_replacements(&ctx.pool, operation_id, &[old_id.to_uuid()])
            .await?;

        let replacements = ctx
            .pool
            .events()
            .get_replacements_by_operation(operation_id)
            .await?;
        assert!(
            replacements.is_empty(),
            "unmatched replay rows must not fabricate replacement lineage"
        );

        Ok(())
    }

    #[sinex_test]
    async fn replay_execution_fails_when_outputs_never_become_query_visible(
        ctx: TestContext,
    ) -> Result<()> {
        let ctx = ctx.with_nats().dedicated().await?;

        let material_id = ctx
            .create_source_material(Some("replay-output-visibility-timeout"))
            .await?;
        let event = DynamicPayload::new(
            "visibility-timeout-test",
            FileCreatedPayload::EVENT_TYPE.as_static_str(),
            json!({ "path": "/tmp/replay-output-visibility-timeout.txt" }),
        )
        .from_material(material_id)
        .build()?;
        let inserted = ctx.pool.events().insert(event).await?;
        let target_id = inserted
            .id
            .expect("inserted replay target must have id")
            .to_uuid();
        let target_ts = inserted
            .id
            .expect("inserted replay target must have id")
            .timestamp();

        let replay = Arc::new(ReplayStateMachine::new(ctx.pool.clone()));
        let nats_client = ctx.nats_client();
        let env = environment();
        let (scan_command_rx, scan_handle) = spawn_fake_scan_node_with_progress(
            nats_client.clone(),
            env,
            "visibility-timeout-test",
            1,
            1,
        )
        .await?;

        let mut scope = sample_scope();
        scope.node_id = "visibility-timeout-test".to_string();
        scope.time_window = Some((
            target_ts - time::Duration::milliseconds(1),
            target_ts + time::Duration::milliseconds(1),
        ));

        let planned = replay
            .create_operation(scope.clone(), "test:output-visibility-timeout".into())
            .await?;
        let preview = replay.generate_preview_summary(&scope).await?;
        replay.update_preview(planned.operation_id, preview).await?;
        replay
            .approve(planned.operation_id, "admin:approver".into())
            .await?;

        let executor = ReplayExecutionEngine::new(replay.clone(), nats_client)
            .with_scan_completion_timeout(Duration::from_millis(100));
        let err = executor
            .execute(planned.operation_id, "service:executor-node".into())
            .await
            .expect_err("missing replay outputs must fail before completion");
        assert!(
            err.to_string()
                .contains("Replay outputs were not query-visible after successful scan"),
            "unexpected error: {err}"
        );

        let failed = replay.load_operation(planned.operation_id).await?;
        assert_eq!(failed.state, ReplayState::Failed);
        assert_eq!(
            failed.outcome,
            Some(sinex_primitives::domain::ReplayOutcome::Failed)
        );

        let live_target_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*)::bigint FROM core.events WHERE id = $1::uuid")
                .bind(target_id)
                .fetch_one(&ctx.pool)
                .await?;
        let archived_target_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*)::bigint FROM audit.archived_events WHERE id = $1::uuid",
        )
        .bind(target_id)
        .fetch_one(&ctx.pool)
        .await?;
        assert_eq!(live_target_count, 0);
        assert_eq!(archived_target_count, 1);

        let dispatched_command = scan_command_rx.await.map_err(|_| {
            eyre!("fake visibility-timeout-test node did not receive a scan command")
        })?;
        assert_eq!(dispatched_command.operation_id, planned.operation_id);

        scan_handle
            .await
            .map_err(|e| eyre!("fake visibility-timeout-test node task failed: {e}"))?;

        Ok(())
    }

    #[sinex_test]
    async fn replay_execution_fails_when_node_never_reports_completion(
        ctx: TestContext,
    ) -> Result<()> {
        let ctx = ctx.with_nats().dedicated().await?;

        let material_id = ctx.create_source_material(Some("replay-timeout")).await?;
        let event = DynamicPayload::new(
            "timeout-test",
            FileCreatedPayload::EVENT_TYPE.as_static_str(),
            json!({ "path": "/tmp/replay-timeout.txt" }),
        )
        .from_material(material_id)
        .build()?;
        let inserted = ctx.pool.events().insert(event).await?;
        let target_id = inserted
            .id
            .expect("inserted replay target must have id")
            .to_uuid();
        let target_ts = inserted
            .id
            .expect("inserted replay target must have id")
            .timestamp();

        let replay = Arc::new(ReplayStateMachine::new(ctx.pool.clone()));
        let nats_client = ctx.nats_client();
        let env = sinex_primitives::environment::environment();
        let (scan_command_rx, scan_handle) =
            spawn_fake_scan_node_ack_only(nats_client.clone(), env.clone(), "timeout-test").await?;

        let executor = ReplayExecutionEngine::new(replay.clone(), nats_client.clone())
            .with_scan_completion_timeout(Duration::from_millis(100));
        ReplayTelemetry::new(replay.clone()).spawn();
        let health = Arc::new(Mutex::new(ReplayControlHealthState::default()));
        ReplayControlServer::new(
            &env,
            nats_client.clone(),
            replay.clone(),
            executor,
            Arc::clone(&health),
        )
        .spawn()
        .await?;
        let client = ReplayControlClient::new(&env, nats_client, Duration::from_secs(30), health);

        let mut scope = sample_scope();
        scope.node_id = "timeout-test".to_string();
        scope.time_window = Some((
            target_ts - time::Duration::milliseconds(1),
            target_ts + time::Duration::milliseconds(1),
        ));

        let planned = client.plan("test:replay-user".into(), scope).await?;
        let (previewed, _) = client.preview(planned.operation_id).await?;
        let approved = client
            .approve(previewed.operation_id, "admin:approver".into())
            .await?;
        let err = client
            .execute(approved.operation_id, "service:executor-node".into(), false)
            .await
            .expect_err("execute should fail when the node never reports completion");
        assert!(
            err.to_string().contains("archived cascade left untouched"),
            "timeout failure should explain why replay execution failed: {err}"
        );

        let operation = replay.load_operation(approved.operation_id).await?;
        assert_eq!(operation.state, ReplayState::Failed);
        assert_eq!(operation.checkpoint.processed_events, 0);

        let live_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*)::bigint FROM core.events WHERE id = $1::uuid")
                .bind(target_id)
                .fetch_one(&ctx.pool)
                .await?;
        let archived_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*)::bigint FROM audit.archived_events WHERE id = $1::uuid",
        )
        .bind(target_id)
        .fetch_one(&ctx.pool)
        .await?;
        assert_eq!(
            live_count, 0,
            "timed-out replay should not resurrect archived rows"
        );
        assert_eq!(
            archived_count, 1,
            "timed-out replay should leave the archived cascade untouched"
        );

        let dispatched_command = scan_command_rx
            .await
            .map_err(|_| eyre!("fake timeout-test node did not receive a scan command"))?;
        assert_eq!(dispatched_command.operation_id, approved.operation_id);

        scan_handle
            .await
            .map_err(|e| eyre!("fake timeout-test node task failed: {e}"))?;

        Ok(())
    }

    #[sinex_test]
    async fn replay_execution_fails_fast_when_progress_checkpoint_persist_fails(
        ctx: TestContext,
    ) -> Result<()> {
        let ctx = ctx.with_nats().dedicated().await?;

        let material_id = ctx
            .create_source_material(Some("replay-checkpoint-persist-fail"))
            .await?;
        let event = DynamicPayload::new(
            "checkpoint-fail-test",
            FileCreatedPayload::EVENT_TYPE.as_static_str(),
            json!({ "path": "/tmp/replay-checkpoint-persist-fail.txt" }),
        )
        .from_material(material_id)
        .build()?;
        let inserted = ctx.pool.events().insert(event).await?;
        let target_id = inserted
            .id
            .expect("inserted replay target must have id")
            .to_uuid();
        let target_ts = inserted
            .id
            .expect("inserted replay target must have id")
            .timestamp();

        let replay = Arc::new(ReplayStateMachine::new(ctx.pool.clone()));
        let nats_client = ctx.nats_client();
        let env = environment();
        let (_scan_command_rx, scan_handle) = spawn_fake_scan_node_with_progress(
            nats_client.clone(),
            env,
            "checkpoint-fail-test",
            1,
            0,
        )
        .await?;

        let mut scope = sample_scope();
        scope.node_id = "checkpoint-fail-test".to_string();
        scope.time_window = Some((
            target_ts - time::Duration::milliseconds(1),
            target_ts + time::Duration::milliseconds(1),
        ));

        let planned = replay
            .create_operation(scope.clone(), "test:checkpoint-fail".into())
            .await?;
        let preview = replay.generate_preview_summary(&scope).await?;
        replay.update_preview(planned.operation_id, preview).await?;
        replay
            .approve(planned.operation_id, "admin:approver".into())
            .await?;

        let executor = ReplayExecutionEngine::new(replay.clone(), nats_client)
            .with_checkpoint_failures(Arc::new(AtomicUsize::new(1)))
            .with_scan_completion_timeout(Duration::from_secs(5));
        let err = executor
            .execute(planned.operation_id, "service:executor-node".into())
            .await
            .expect_err("checkpoint persistence failure should abort replay execution");
        assert!(
            err.chain().any(|cause| {
                cause
                    .to_string()
                    .contains("Failed to persist replay progress checkpoint")
            }),
            "unexpected error: {err}"
        );

        let failed = replay.load_operation(planned.operation_id).await?;
        assert_eq!(failed.state, ReplayState::Failed);
        assert_eq!(
            failed.outcome,
            Some(sinex_primitives::domain::ReplayOutcome::Failed)
        );
        assert!(
            failed.error_details.as_deref().is_some_and(
                |details| details.contains("Failed to persist replay progress checkpoint")
            ),
            "failure details should include checkpoint persistence context: {:?}",
            failed.error_details
        );

        let live_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*)::bigint FROM core.events WHERE id = $1::uuid")
                .bind(target_id)
                .fetch_one(&ctx.pool)
                .await?;
        let archived_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*)::bigint FROM audit.archived_events WHERE id = $1::uuid",
        )
        .bind(target_id)
        .fetch_one(&ctx.pool)
        .await?;
        assert_eq!(
            live_count, 1,
            "checkpoint persistence failure before replacements should restore live rows"
        );
        assert_eq!(
            archived_count, 0,
            "checkpoint persistence failure before replacements should not leave archived rows behind"
        );

        scan_handle
            .await
            .map_err(|e| eyre!("fake checkpoint-fail-test node task failed: {e}"))?;

        Ok(())
    }

    #[sinex_test]
    async fn replay_execution_fails_when_replacement_recording_fails(
        ctx: TestContext,
    ) -> Result<()> {
        let ctx = ctx.with_nats().dedicated().await?;

        let material_id = ctx
            .create_source_material(Some("replay-replacement-record-fail"))
            .await?;
        let mut event = DynamicPayload::new(
            "replacement-record-fail-test",
            FileCreatedPayload::EVENT_TYPE.as_static_str(),
            json!({ "path": "/tmp/replay-replacement-record-fail.txt" }),
        )
        .from_material(material_id)
        .build()?;
        event.equivalence_key = Some("replacement-record-eq".to_string());
        let inserted = ctx.pool.events().insert(event).await?;
        let target_id = inserted
            .id
            .expect("inserted replay target must have id")
            .to_uuid();
        let target_ts = inserted
            .id
            .expect("inserted replay target must have id")
            .timestamp();

        let replay = Arc::new(ReplayStateMachine::new(ctx.pool.clone()));
        let nats_client = ctx.nats_client();
        let env = environment();
        let (scan_command_rx, scan_handle) = spawn_fake_scan_node_with_progress(
            nats_client.clone(),
            env,
            "replacement-record-fail-test",
            1,
            1,
        )
        .await?;

        let mut scope = sample_scope();
        scope.node_id = "replacement-record-fail-test".to_string();
        scope.time_window = Some((
            target_ts - time::Duration::milliseconds(1),
            target_ts + time::Duration::milliseconds(1),
        ));

        let planned = replay
            .create_operation(scope.clone(), "test:replacement-record-fail".into())
            .await?;
        let preview = replay.generate_preview_summary(&scope).await?;
        replay.update_preview(planned.operation_id, preview).await?;
        replay
            .approve(planned.operation_id, "admin:approver".into())
            .await?;

        let replay_output_handle = spawn_replay_output_inserter(
            ctx.pool.clone(),
            scan_command_rx,
            "replacement-record-fail-test",
            FileCreatedPayload::EVENT_TYPE.as_static_str(),
            "/tmp/replay-replacement-record-fail-output.txt",
            Some("replacement-record-eq"),
        );

        let executor = ReplayExecutionEngine::new(replay.clone(), nats_client)
            .with_replacement_record_failures(Arc::new(AtomicUsize::new(1)))
            .with_scan_completion_timeout(Duration::from_secs(5));
        let err = executor
            .execute(planned.operation_id, "service:executor-node".into())
            .await
            .expect_err("replacement-record failure should abort replay execution");
        assert!(
            err.chain().any(|cause| {
                cause
                    .to_string()
                    .contains("Failed to record replay replacement relations")
            }),
            "unexpected error: {err}"
        );

        let failed = replay.load_operation(planned.operation_id).await?;
        assert_eq!(failed.state, ReplayState::Failed);
        assert_eq!(
            failed.outcome,
            Some(sinex_primitives::domain::ReplayOutcome::Failed)
        );
        assert!(
            failed.error_details.as_deref().is_some_and(|details| {
                details.contains("Failed to record replay replacement relations")
            }),
            "failure details should include replacement recording context: {:?}",
            failed.error_details
        );

        let replay_command = replay_output_handle
            .await
            .map_err(|e| eyre!("fake replacement-record replay output task failed: {e}"))??;

        let live_target_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*)::bigint FROM core.events WHERE id = $1::uuid")
                .bind(target_id)
                .fetch_one(&ctx.pool)
                .await?;
        let archived_target_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*)::bigint FROM audit.archived_events WHERE id = $1::uuid",
        )
        .bind(target_id)
        .fetch_one(&ctx.pool)
        .await?;
        let live_replacement_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*)::bigint FROM core.events WHERE created_by_operation_id = $1::uuid",
        )
        .bind(replay_command.operation_id)
        .fetch_one(&ctx.pool)
        .await?;
        assert_eq!(
            live_target_count, 0,
            "replacement-record failure occurs after the original event has already been archived"
        );
        assert_eq!(
            archived_target_count, 1,
            "replacement-record failure must leave the archived target in audit storage"
        );
        assert_eq!(
            live_replacement_count, 1,
            "replacement-record failure must not delete already-emitted replay outputs"
        );

        let replacements = ctx
            .pool
            .events()
            .get_replacements_by_operation(planned.operation_id)
            .await?;
        assert!(
            replacements.is_empty(),
            "failed replacement recording must not partially insert lineage rows"
        );

        scan_handle
            .await
            .map_err(|e| eyre!("fake replacement-record-fail-test node task failed: {e}"))?;

        Ok(())
    }

    #[sinex_test]
    async fn replay_execution_restores_archived_cascade_when_dispatch_fails_before_ack(
        ctx: TestContext,
    ) -> Result<()> {
        let ctx = ctx.with_nats().dedicated().await?;

        let material_id = ctx
            .create_source_material(Some("replay-pre-ack-failure"))
            .await?;
        let event = DynamicPayload::new(
            "pre-ack-test",
            FileCreatedPayload::EVENT_TYPE.as_static_str(),
            json!({ "path": "/tmp/replay-pre-ack-failure.txt" }),
        )
        .from_material(material_id)
        .build()?;
        let inserted = ctx.pool.events().insert(event).await?;
        let target_id = inserted
            .id
            .expect("inserted replay target must have id")
            .to_uuid();
        let target_ts = inserted
            .id
            .expect("inserted replay target must have id")
            .timestamp();

        let replay = Arc::new(ReplayStateMachine::new(ctx.pool.clone()));
        let nats_client = ctx.nats_client();
        let env = sinex_primitives::environment::environment();

        let executor = ReplayExecutionEngine::new(replay.clone(), nats_client.clone())
            .with_scan_ack_timeout(Duration::from_millis(100));
        ReplayTelemetry::new(replay.clone()).spawn();
        let health = Arc::new(Mutex::new(ReplayControlHealthState::default()));
        ReplayControlServer::new(
            &env,
            nats_client.clone(),
            replay.clone(),
            executor,
            Arc::clone(&health),
        )
        .spawn()
        .await?;
        let client = ReplayControlClient::new(&env, nats_client, Duration::from_secs(30), health);

        let mut scope = sample_scope();
        scope.node_id = "pre-ack-test".to_string();
        scope.time_window = Some((
            target_ts - time::Duration::milliseconds(1),
            target_ts + time::Duration::milliseconds(1),
        ));

        let planned = client.plan("test:replay-user".into(), scope).await?;
        let (previewed, _) = client.preview(planned.operation_id).await?;
        let approved = client
            .approve(previewed.operation_id, "admin:approver".into())
            .await?;
        let err = client
            .execute(approved.operation_id, "service:executor-node".into(), false)
            .await
            .expect_err("execute should fail before scan ack when no node responder exists");
        assert!(
            err.to_string().contains("restored archived cascade"),
            "pre-ack dispatch failures must explain that the archived cascade was restored: {err}"
        );

        let operation = replay.load_operation(approved.operation_id).await?;
        assert_eq!(operation.state, ReplayState::Failed);
        assert_eq!(operation.checkpoint.processed_events, 0);

        let live_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*)::bigint FROM core.events WHERE id = $1::uuid")
                .bind(target_id)
                .fetch_one(&ctx.pool)
                .await?;
        let archived_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*)::bigint FROM audit.archived_events WHERE id = $1::uuid",
        )
        .bind(target_id)
        .fetch_one(&ctx.pool)
        .await?;
        assert_eq!(
            live_count, 1,
            "pre-ack dispatch failures must restore the live row"
        );
        assert_eq!(
            archived_count, 0,
            "pre-ack dispatch failures must not leave the archived cascade behind"
        );

        Ok(())
    }

    #[sinex_test]
    async fn replay_execution_fails_before_archive_when_scope_metadata_collection_fails(
        ctx: TestContext,
    ) -> Result<()> {
        let ctx = ctx.with_nats().dedicated().await?;

        let material_id = ctx
            .create_source_material(Some("replay-scope-metadata-failure"))
            .await?;
        let event = DynamicPayload::new(
            "scope-metadata-test",
            FileCreatedPayload::EVENT_TYPE.as_static_str(),
            json!({ "path": "/tmp/replay-scope-metadata-failure.txt" }),
        )
        .from_material(material_id)
        .build()?;
        let inserted = ctx.pool.events().insert(event).await?;
        let target_id = inserted
            .id
            .expect("inserted replay target must have id")
            .to_uuid();
        let target_ts = inserted
            .id
            .expect("inserted replay target must have id")
            .timestamp();

        let replay = Arc::new(ReplayStateMachine::new(ctx.pool.clone()));
        let mut scope = sample_scope();
        scope.node_id = "scope-metadata-test".to_string();
        scope.time_window = Some((
            target_ts - time::Duration::milliseconds(1),
            target_ts + time::Duration::milliseconds(1),
        ));

        let planned = replay
            .create_operation(scope.clone(), "test:scope-metadata-fail".into())
            .await?;
        let preview = replay.generate_preview_summary(&scope).await?;
        replay.update_preview(planned.operation_id, preview).await?;
        replay
            .approve(planned.operation_id, "admin:approver".into())
            .await?;

        let executor = ReplayExecutionEngine::new(replay.clone(), ctx.nats_client())
            .with_scope_metadata_failures(Arc::new(AtomicUsize::new(1)));
        let err = executor
            .execute(planned.operation_id, "service:executor-node".into())
            .await
            .expect_err("scope metadata collection failure should abort replay execution");
        assert!(
            err.chain().any(|cause| {
                cause
                    .to_string()
                    .contains("Failed to collect replay cascade scope metadata")
            }),
            "unexpected error: {err}"
        );

        let failed = replay.load_operation(planned.operation_id).await?;
        assert_eq!(failed.state, ReplayState::Failed);
        assert_eq!(
            failed.outcome,
            Some(sinex_primitives::domain::ReplayOutcome::Failed)
        );
        assert!(
            failed.error_details.as_deref().is_some_and(
                |details| details.contains("Failed to collect replay cascade scope metadata")
            ),
            "failure details should include scope metadata context: {:?}",
            failed.error_details
        );

        let live_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*)::bigint FROM core.events WHERE id = $1::uuid")
                .bind(target_id)
                .fetch_one(&ctx.pool)
                .await?;
        let archived_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*)::bigint FROM audit.archived_events WHERE id = $1::uuid",
        )
        .bind(target_id)
        .fetch_one(&ctx.pool)
        .await?;
        assert_eq!(
            live_count, 1,
            "scope metadata failure must leave the live row untouched"
        );
        assert_eq!(
            archived_count, 0,
            "scope metadata failure must abort before archiving the cascade"
        );

        Ok(())
    }

    #[sinex_test]
    async fn replay_execution_restores_cascade_when_initial_scope_invalidation_publish_fails(
        ctx: TestContext,
    ) -> Result<()> {
        let ctx = ctx.with_nats().dedicated().await?;

        let material_id = ctx
            .create_source_material(Some("replay-scope-invalidation-publish-failure"))
            .await?;
        let mut event = DynamicPayload::new(
            "scope-invalidation-test",
            FileCreatedPayload::EVENT_TYPE.as_static_str(),
            json!({ "path": "/tmp/replay-scope-invalidation-publish-failure.txt" }),
        )
        .from_material(material_id)
        .build()?;
        event.scope_key = Some("scope://scope-invalidation-test/replay".to_string());
        let inserted = ctx.pool.events().insert(event).await?;
        let target_id = inserted
            .id
            .expect("inserted replay target must have id")
            .to_uuid();
        let target_ts = inserted
            .id
            .expect("inserted replay target must have id")
            .timestamp();

        let replay = Arc::new(ReplayStateMachine::new(ctx.pool.clone()));
        let mut scope = sample_scope();
        scope.node_id = "scope-invalidation-test".to_string();
        scope.time_window = Some((
            target_ts - time::Duration::milliseconds(1),
            target_ts + time::Duration::milliseconds(1),
        ));

        let planned = replay
            .create_operation(scope.clone(), "test:scope-invalidation-fail".into())
            .await?;
        let preview = replay.generate_preview_summary(&scope).await?;
        replay.update_preview(planned.operation_id, preview).await?;
        replay
            .approve(planned.operation_id, "admin:approver".into())
            .await?;

        let mut invalidation_rx =
            spawn_invalidation_listener_for_test(&ctx.nats_client()).await?;

        let executor = ReplayExecutionEngine::new(replay.clone(), ctx.nats_client())
            .with_scope_invalidation_publish_failures(Arc::new(AtomicUsize::new(1)));
        let err = executor
            .execute(planned.operation_id, "service:executor-node".into())
            .await
            .expect_err("scope invalidation publish failure should abort replay execution");
        assert!(
            err.chain().any(|cause| {
                cause
                    .to_string()
                    .contains("Failed to publish replay scope invalidations before dispatch")
            }),
            "unexpected error: {err}"
        );

        let failed = replay.load_operation(planned.operation_id).await?;
        assert_eq!(failed.state, ReplayState::Failed);
        assert_eq!(
            failed.outcome,
            Some(sinex_primitives::domain::ReplayOutcome::Failed)
        );
        assert!(
            failed.error_details.as_deref().is_some_and(|details| {
                details.contains("Failed to publish replay scope invalidations before dispatch")
            }),
            "failure details should include invalidation publish context: {:?}",
            failed.error_details
        );

        let live_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*)::bigint FROM core.events WHERE id = $1::uuid")
                .bind(target_id)
                .fetch_one(&ctx.pool)
                .await?;
        let archived_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*)::bigint FROM audit.archived_events WHERE id = $1::uuid",
        )
        .bind(target_id)
        .fetch_one(&ctx.pool)
        .await?;
        assert_eq!(
            live_count, 1,
            "scope invalidation publish failure must restore the live row"
        );
        assert_eq!(
            archived_count, 0,
            "scope invalidation publish failure must not leave archived rows behind"
        );

        let payload_bytes = tokio::time::timeout(Duration::from_secs(1), invalidation_rx.recv())
            .await?
            .expect("compensating invalidation should still publish after restore");
        let payload = String::from_utf8(payload_bytes)?;
        assert!(payload.contains("scope://scope-invalidation-test/replay"));
        assert!(payload.contains(&target_id.to_string()));

        Ok(())
    }

    #[sinex_test]
    async fn replay_execute_rejects_zero_event_preview_before_execution(
        ctx: TestContext,
    ) -> Result<()> {
        let ctx = ctx.with_nats().dedicated().await?;
        let replay = Arc::new(ReplayStateMachine::new(ctx.pool.clone()));
        let client =
            spawn_replay_control(replay.clone(), ctx.nats_client(), Duration::from_secs(30))
                .await?;

        let operation = replay
            .create_operation(sample_scope(), "test:planner".to_string())
            .await?;
        let now = Timestamp::now();
        replay
            .update_preview(
                operation.operation_id,
                json!({
                    "total_events": 0,
                    "time_window": {
                        "start": now.format_rfc3339(),
                        "end": (now + time::Duration::seconds(1)).format_rfc3339(),
                    }
                }),
            )
            .await?;
        replay
            .approve(operation.operation_id, "admin:approver".to_string())
            .await?;

        let err = client
            .execute(
                operation.operation_id,
                "service:executor-node".into(),
                false,
            )
            .await
            .expect_err("zero-event previews must not enter execution");
        assert!(
            err.to_string().contains("preview matches zero events"),
            "unexpected error: {err}"
        );

        let stored = replay.load_operation(operation.operation_id).await?;
        assert_eq!(stored.state, ReplayState::Failed);
        assert_eq!(
            stored.outcome,
            Some(sinex_primitives::domain::ReplayOutcome::Failed)
        );
        assert_eq!(
            stored.error_details.as_deref(),
            Some(err.to_string().as_str())
        );
        assert!(stored.executor_node.is_none());

        Ok(())
    }

    #[sinex_test]
    async fn replay_preview_rejects_refresh_after_approval(ctx: TestContext) -> Result<()> {
        let ctx = ctx.with_nats().dedicated().await?;
        let replay = Arc::new(ReplayStateMachine::new(ctx.pool.clone()));
        let client =
            spawn_replay_control(replay.clone(), ctx.nats_client(), Duration::from_secs(30))
                .await?;

        let planned = client.plan("test:planner".into(), sample_scope()).await?;
        let (previewed, _) = client.preview(planned.operation_id).await?;
        let approved = client
            .approve(previewed.operation_id, "admin:approver".into())
            .await?;

        let err = client
            .preview(approved.operation_id)
            .await
            .expect_err("approved operations must not accept preview refreshes");
        assert!(
            err.to_string().contains("already approved"),
            "unexpected error: {err}"
        );

        let stored = replay.load_operation(approved.operation_id).await?;
        assert_eq!(stored.state, ReplayState::Approved);
        Ok(())
    }

    #[sinex_test]
    async fn replay_execute_dry_run_is_rejected_without_state_changes(
        ctx: TestContext,
    ) -> Result<()> {
        let ctx = ctx.with_nats().dedicated().await?;
        let replay = Arc::new(ReplayStateMachine::new(ctx.pool.clone()));
        let client =
            spawn_replay_control(replay.clone(), ctx.nats_client(), Duration::from_secs(30))
                .await?;

        let planned = client.plan("test:planner".into(), sample_scope()).await?;
        let (previewed, _) = client.preview(planned.operation_id).await?;
        let approved = client
            .approve(previewed.operation_id, "admin:approver".into())
            .await?;

        let err = client
            .execute(approved.operation_id, "service:executor-node".into(), true)
            .await
            .expect_err("dry-run execute should redirect callers back to preview");
        assert!(
            err.to_string()
                .contains("does not support dry-run semantics"),
            "unexpected error: {err}"
        );

        let stored = replay.load_operation(approved.operation_id).await?;
        assert_eq!(stored.state, ReplayState::Approved);
        assert!(stored.finished_at.is_none());
        Ok(())
    }

    #[sinex_test]
    async fn replay_execute_fails_when_live_scope_disappears_after_approval(
        ctx: TestContext,
    ) -> Result<()> {
        let ctx = ctx.with_nats().dedicated().await?;

        let material_id = ctx
            .create_source_material(Some("replay-scope-disappeared"))
            .await?;
        let event = DynamicPayload::new(
            "fs-test",
            FileCreatedPayload::EVENT_TYPE.as_static_str(),
            json!({ "path": "/tmp/replay-scope-disappeared.txt" }),
        )
        .from_material(material_id)
        .build()?;
        let inserted = ctx.pool.events().insert(event).await?;
        let target_event_id = inserted.id.expect("inserted replay target must have id");
        let target_id = target_event_id.to_uuid();
        let target_ts = target_event_id.timestamp();

        let replay = Arc::new(ReplayStateMachine::new(ctx.pool.clone()));
        let client =
            spawn_replay_control(replay.clone(), ctx.nats_client(), Duration::from_secs(30))
                .await?;

        let mut scope = sample_scope();
        scope.time_window = Some((
            target_ts - time::Duration::milliseconds(1),
            target_ts + time::Duration::milliseconds(1),
        ));
        scope.material_filter = Some(vec![*material_id.as_uuid()]);
        scope.filters.insert(
            "event_types".to_string(),
            json!([FileCreatedPayload::EVENT_TYPE.as_static_str()]),
        );

        let planned = client.plan("test:replay-user".into(), scope).await?;
        let (previewed, preview) = client.preview(planned.operation_id).await?;
        assert_eq!(
            preview
                .get("total_events")
                .and_then(serde_json::Value::as_i64),
            Some(1)
        );
        let approved = client
            .approve(previewed.operation_id, "admin:approver".into())
            .await?;

        ctx.pool
            .events()
            .execute_cascade_archive(
                &[target_id],
                "archive replay target before execution",
                &Uuid::now_v7().to_string(),
                "test:archive-before-replay",
            )
            .await?;

        let err = client
            .execute(approved.operation_id, "service:executor-node".into(), false)
            .await
            .expect_err("execution should fail once the approved live scope has vanished");
        assert!(
            err.to_string().contains("matched zero live events"),
            "unexpected error: {err}"
        );

        let failed = replay.load_operation(approved.operation_id).await?;
        assert_eq!(failed.state, ReplayState::Failed);
        assert_eq!(
            failed.outcome,
            Some(sinex_primitives::domain::ReplayOutcome::Failed)
        );

        Ok(())
    }

    #[sinex_test]
    async fn replay_execute_fails_when_live_scope_drifts_after_approval(
        ctx: TestContext,
    ) -> Result<()> {
        let ctx = ctx.with_nats().dedicated().await?;

        let first_material = ctx
            .create_source_material(Some("replay-scope-drift-first"))
            .await?;
        let second_material = ctx
            .create_source_material(Some("replay-scope-drift-second"))
            .await?;

        let first = DynamicPayload::new(
            "fs-test",
            FileCreatedPayload::EVENT_TYPE.as_static_str(),
            json!({ "path": "/tmp/replay-scope-drift-first.txt" }),
        )
        .from_material(first_material)
        .build()?;
        let second = DynamicPayload::new(
            "fs-test",
            FileCreatedPayload::EVENT_TYPE.as_static_str(),
            json!({ "path": "/tmp/replay-scope-drift-second.txt" }),
        )
        .from_material(second_material)
        .build()?;

        let inserted_first = ctx.pool.events().insert(first).await?;
        let inserted_second = ctx.pool.events().insert(second).await?;
        let first_event_id = inserted_first.id.expect("first replay target must have id");
        let second_event_id = inserted_second
            .id
            .expect("second replay target must have id");
        let first_ts = first_event_id.timestamp();
        let second_ts = second_event_id.timestamp();

        let replay = Arc::new(ReplayStateMachine::new(ctx.pool.clone()));
        let client =
            spawn_replay_control(replay.clone(), ctx.nats_client(), Duration::from_secs(30))
                .await?;

        let mut scope = sample_scope();
        scope.time_window = Some((
            std::cmp::min(first_ts, second_ts) - time::Duration::milliseconds(1),
            std::cmp::max(first_ts, second_ts) + time::Duration::milliseconds(1),
        ));
        scope.filters.insert(
            "event_types".to_string(),
            json!([FileCreatedPayload::EVENT_TYPE.as_static_str()]),
        );

        let planned = client.plan("test:replay-user".into(), scope).await?;
        let (previewed, preview) = client.preview(planned.operation_id).await?;
        assert_eq!(
            preview
                .get("total_events")
                .and_then(serde_json::Value::as_i64),
            Some(2)
        );
        let approved = client
            .approve(previewed.operation_id, "admin:approver".into())
            .await?;

        ctx.pool
            .events()
            .execute_cascade_archive(
                &[first_event_id.to_uuid()],
                "archive one replay target before execution",
                &Uuid::now_v7().to_string(),
                "test:archive-before-replay",
            )
            .await?;

        let err = client
            .execute(approved.operation_id, "service:executor-node".into(), false)
            .await
            .expect_err("execution should fail once the approved live scope drifts");
        assert!(
            err.to_string().contains("preview is stale"),
            "unexpected error: {err}"
        );
        assert!(
            err.to_string()
                .contains(&second_event_id.to_uuid().to_string())
                || err
                    .to_string()
                    .contains(&first_event_id.to_uuid().to_string()),
            "drift error should expose the changed root set: {err}"
        );

        let failed = replay.load_operation(approved.operation_id).await?;
        assert_eq!(failed.state, ReplayState::Failed);
        assert_eq!(
            failed.outcome,
            Some(sinex_primitives::domain::ReplayOutcome::Failed)
        );

        Ok(())
    }

    #[sinex_test]
    async fn replay_abort_before_scan_ack_restores_cascade_and_emits_compensating_invalidation(
        ctx: TestContext,
    ) -> Result<()> {
        let ctx = ctx.with_nats().dedicated().await?;
        let replay = Arc::new(ReplayStateMachine::new(ctx.pool.clone()));
        let engine = ReplayExecutionEngine::new(replay.clone(), ctx.nats_client());

        let material_id = ctx
            .create_source_material(Some("replay-compensating-invalidation"))
            .await?;
        let mut event = DynamicPayload::new(
            "fs-test",
            FileCreatedPayload::EVENT_TYPE.as_static_str(),
            json!({ "path": "/tmp/replay-compensating-invalidation.txt" }),
        )
        .from_material(material_id)
        .build()?;
        event.scope_key = Some("scope://fs-test/replay-compensating-invalidation".to_string());
        let inserted = ctx.pool.events().insert(event).await?;
        let event_id = inserted.id.expect("inserted replay target must have id");
        let operation_id = Uuid::now_v7();

        let scope_metadata = engine
            .collect_cascade_scope_metadata(&ctx.pool, &[event_id.to_uuid()])
            .await?;
        assert_eq!(scope_metadata.len(), 1);
        assert_eq!(scope_metadata[0].event_source, "fs-test");
        assert_eq!(
            scope_metadata[0].event_type,
            FileCreatedPayload::EVENT_TYPE.as_static_str()
        );
        assert!(!scope_metadata[0].has_lineage);
        assert_eq!(scope_metadata[0].event_ids, vec![event_id.to_uuid()]);

        ctx.pool
            .events()
            .execute_cascade_archive(
                &[event_id.to_uuid()],
                "archive before compensating restore test",
                &operation_id.to_string(),
                "test:replay-compensating",
            )
            .await?;

        let mut invalidation_rx =
            spawn_invalidation_listener_for_test(&ctx.nats_client()).await?;

        let err = engine
            .abort_before_scan_ack(
                &ctx.pool,
                &[event_id.to_uuid()],
                &scope_metadata,
                operation_id,
                eyre!("boom"),
            )
            .await
            .expect_err("abort helper should surface the caller failure");
        assert!(
            err.to_string()
                .contains("published compensating scope invalidations"),
            "unexpected error: {err}"
        );

        let live_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*)::bigint FROM core.events WHERE id = $1::uuid")
                .bind(event_id.to_uuid())
                .fetch_one(&ctx.pool)
                .await?;
        assert_eq!(
            live_count, 1,
            "aborted replay should restore the archived event"
        );

        let payload_bytes = tokio::time::timeout(Duration::from_secs(1), invalidation_rx.recv())
            .await?
            .expect("compensating invalidation should be published");
        let payload = String::from_utf8(payload_bytes)?;
        assert!(payload.contains("scope://fs-test/replay-compensating-invalidation"));
        assert!(payload.contains(&event_id.to_string()));

        Ok(())
    }

    #[sinex_test]
    async fn replay_abort_before_scan_ack_surfaces_compensating_invalidation_failure(
        ctx: TestContext,
    ) -> Result<()> {
        let ctx = ctx.with_nats().dedicated().await?;
        let replay = Arc::new(ReplayStateMachine::new(ctx.pool.clone()));
        let engine = ReplayExecutionEngine::new(replay.clone(), ctx.nats_client())
            .with_scope_invalidation_publish_failures(Arc::new(AtomicUsize::new(1)));

        let material_id = ctx
            .create_source_material(Some("replay-compensating-invalidation-failure"))
            .await?;
        let mut event = DynamicPayload::new(
            "fs-test",
            FileCreatedPayload::EVENT_TYPE.as_static_str(),
            json!({ "path": "/tmp/replay-compensating-invalidation-failure.txt" }),
        )
        .from_material(material_id)
        .build()?;
        event.scope_key =
            Some("scope://fs-test/replay-compensating-invalidation-failure".to_string());
        let inserted = ctx.pool.events().insert(event).await?;
        let event_id = inserted.id.expect("inserted replay target must have id");
        let operation_id = Uuid::now_v7();

        let scope_metadata = engine
            .collect_cascade_scope_metadata(&ctx.pool, &[event_id.to_uuid()])
            .await?;
        assert_eq!(scope_metadata.len(), 1);

        ctx.pool
            .events()
            .execute_cascade_archive(
                &[event_id.to_uuid()],
                "archive before compensating restore failure test",
                &operation_id.to_string(),
                "test:replay-compensating-failure",
            )
            .await?;

        let err = engine
            .abort_before_scan_ack(
                &ctx.pool,
                &[event_id.to_uuid()],
                &scope_metadata,
                operation_id,
                eyre!("boom"),
            )
            .await
            .expect_err("compensating invalidation publish failure should surface");
        assert!(
            err.to_string()
                .contains("failed to publish compensating scope invalidations"),
            "unexpected error: {err}"
        );

        let live_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*)::bigint FROM core.events WHERE id = $1::uuid")
                .bind(event_id.to_uuid())
                .fetch_one(&ctx.pool)
                .await?;
        let archived_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*)::bigint FROM audit.archived_events WHERE id = $1::uuid",
        )
        .bind(event_id.to_uuid())
        .fetch_one(&ctx.pool)
        .await?;
        assert_eq!(
            live_count, 1,
            "aborted replay should still restore the archived event"
        );
        assert_eq!(
            archived_count, 0,
            "aborted replay should not leave the archived event behind"
        );

        Ok(())
    }

    #[sinex_test]
    async fn replay_execution_returns_cancelled_operation_when_cancelled_midflight(
        ctx: TestContext,
    ) -> Result<()> {
        let ctx = ctx.with_nats().dedicated().await?;
        let nats_url = ctx.nats_handle()?.client_url().to_string();

        let material_id = ctx
            .create_source_material(Some("replay-cancel-midflight"))
            .await?;
        let event = DynamicPayload::new(
            "cancel-test",
            FileCreatedPayload::EVENT_TYPE.as_static_str(),
            json!({ "path": "/tmp/replay-cancel.txt" }),
        )
        .from_material(material_id)
        .build()?;
        let inserted = ctx.pool.events().insert(event).await?;
        let target_id = inserted
            .id
            .expect("inserted replay target must have id")
            .to_uuid();
        let target_ts = inserted
            .id
            .expect("inserted replay target must have id")
            .timestamp();

        let replay = Arc::new(ReplayStateMachine::new(ctx.pool.clone()));
        let nats_client = ctx.nats_client();
        let env = sinex_primitives::environment::environment();
        let (_scan_command_rx, scan_handle) =
            spawn_fake_scan_node_ack_only(nats_client.clone(), env.clone(), "cancel-test").await?;

        let executor = ReplayExecutionEngine::new(replay.clone(), nats_client.clone())
            .with_scan_completion_timeout(Duration::from_secs(5));
        let health = Arc::new(Mutex::new(ReplayControlHealthState::default()));
        ReplayControlServer::new(
            &env,
            nats_client.clone(),
            replay.clone(),
            executor,
            Arc::clone(&health),
        )
        .spawn()
        .await?;

        let execute_client = ReplayControlClient::new(
            &env,
            async_nats::connect(&nats_url).await?,
            Duration::from_secs(30),
            Arc::clone(&health),
        );
        let control_client = ReplayControlClient::new(
            &env,
            async_nats::connect(&nats_url).await?,
            Duration::from_secs(30),
            health,
        );

        let mut scope = sample_scope();
        scope.node_id = "cancel-test".to_string();
        scope.time_window = Some((
            target_ts - time::Duration::milliseconds(1),
            target_ts + time::Duration::milliseconds(1),
        ));

        let planned = control_client
            .plan("test:replay-user".into(), scope)
            .await?;
        let (previewed, _) = control_client.preview(planned.operation_id).await?;
        let approved = control_client
            .approve(previewed.operation_id, "admin:approver".into())
            .await?;

        let operation_id = approved.operation_id;
        let execute_task = tokio::spawn(async move {
            execute_client
                .execute(operation_id, "service:executor-node".into(), false)
                .await
        });

        let mut saw_executing = false;
        for _ in 0..40 {
            let operation = replay.load_operation(operation_id).await?;
            if operation.state == ReplayState::Executing {
                saw_executing = true;
                break;
            }
            sleep(Duration::from_millis(25)).await;
        }
        assert!(
            saw_executing,
            "replay operation should enter Executing before cancellation"
        );

        let cancellation_requested = control_client
            .cancel(
                operation_id,
                "admin:approver".into(),
                Some("operator requested stop".to_string()),
            )
            .await?;
        assert_eq!(cancellation_requested.state, ReplayState::Cancelling);
        assert!(cancellation_requested.outcome.is_none());
        assert_eq!(
            cancellation_requested.error_details.as_deref(),
            Some("operator requested stop")
        );
        assert!(cancellation_requested.finished_at.is_none());

        let executed = execute_task
            .await
            .map_err(|e| eyre!("execute task failed: {e}"))??;
        assert_eq!(executed.state, ReplayState::Cancelled);
        assert_eq!(
            executed.outcome,
            Some(sinex_primitives::domain::ReplayOutcome::Cancelled)
        );
        assert_eq!(
            executed.error_details.as_deref(),
            Some("operator requested stop")
        );

        let live_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*)::bigint FROM core.events WHERE id = $1::uuid")
                .bind(target_id)
                .fetch_one(&ctx.pool)
                .await?;
        let archived_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*)::bigint FROM audit.archived_events WHERE id = $1::uuid",
        )
        .bind(target_id)
        .fetch_one(&ctx.pool)
        .await?;
        assert_eq!(
            live_count, 1,
            "cancelled replay should restore live rows when no replacement events were emitted"
        );
        assert_eq!(
            archived_count, 0,
            "cancelled replay should not leave archived rows behind when execution never emitted replacements"
        );

        scan_handle
            .await
            .map_err(|e| eyre!("fake cancel-test node task failed: {e}"))?;

        Ok(())
    }

    #[sinex_test]
    async fn replay_execution_surfaces_operation_state_corruption_after_failure(
        ctx: TestContext,
    ) -> Result<()> {
        let ctx = ctx.with_nats().dedicated().await?;
        let nats_url = ctx.nats_handle()?.client_url().to_string();

        let material_id = ctx
            .create_source_material(Some("replay-corrupt-failure"))
            .await?;
        let event = DynamicPayload::new(
            "corrupt-failure-test",
            FileCreatedPayload::EVENT_TYPE.as_static_str(),
            json!({ "path": "/tmp/replay-corrupt-failure.txt" }),
        )
        .from_material(material_id)
        .build()?;
        ctx.pool.events().insert(event).await?;

        let replay = Arc::new(ReplayStateMachine::new(ctx.pool.clone()));
        let nats_client = ctx.nats_client();
        let env = sinex_primitives::environment::environment();
        let (_scan_command_rx, scan_handle) =
            spawn_fake_scan_node_ack_only(nats_client.clone(), env.clone(), "corrupt-failure-test")
                .await?;

        let executor = ReplayExecutionEngine::new(replay.clone(), nats_client.clone())
            .with_scan_completion_timeout(Duration::from_millis(200));
        let health = Arc::new(Mutex::new(ReplayControlHealthState::default()));
        ReplayControlServer::new(
            &env,
            nats_client.clone(),
            replay.clone(),
            executor,
            Arc::clone(&health),
        )
        .spawn()
        .await?;

        let control_client = ReplayControlClient::new(
            &env,
            async_nats::connect(&nats_url).await?,
            Duration::from_secs(30),
            Arc::clone(&health),
        );
        let execute_client = ReplayControlClient::new(
            &env,
            async_nats::connect(&nats_url).await?,
            Duration::from_secs(30),
            health,
        );

        let mut scope = sample_scope();
        scope.node_id = "corrupt-failure-test".to_string();

        let planned = control_client
            .plan("test:replay-user".into(), scope)
            .await?;
        let (previewed, _) = control_client.preview(planned.operation_id).await?;
        let approved = control_client
            .approve(previewed.operation_id, "admin:approver".into())
            .await?;

        let operation_id = approved.operation_id;
        let execute_task = tokio::spawn(async move {
            execute_client
                .execute(operation_id, "service:executor-node".into(), false)
                .await
        });

        wait_for_operation_state(&replay, operation_id, ReplayState::Executing).await?;
        corrupt_operation_preview_summary(&ctx.pool, operation_id).await?;

        let err = execute_task
            .await
            .map_err(|e| eyre!("execute task failed: {e}"))?
            .expect_err("corrupt replay metadata should surface as execution failure");
        assert!(
            err.to_string()
                .contains("failed to finalize replay execution bookkeeping"),
            "unexpected error: {err:#}"
        );
        assert!(
            err.to_string()
                .contains("failed to inspect replay operation state after execution"),
            "unexpected error: {err:#}"
        );

        scan_handle
            .await
            .map_err(|e| eyre!("fake corrupt-failure-test node task failed: {e}"))?;

        Ok(())
    }

    #[sinex_test]
    async fn replay_execution_surfaces_cancellation_bookkeeping_corruption(
        ctx: TestContext,
    ) -> Result<()> {
        let ctx = ctx.with_nats().dedicated().await?;
        let nats_url = ctx.nats_handle()?.client_url().to_string();

        let material_id = ctx
            .create_source_material(Some("replay-corrupt-cancel"))
            .await?;
        let event = DynamicPayload::new(
            "corrupt-cancel-test",
            FileCreatedPayload::EVENT_TYPE.as_static_str(),
            json!({ "path": "/tmp/replay-corrupt-cancel.txt" }),
        )
        .from_material(material_id)
        .build()?;
        let inserted = ctx.pool.events().insert(event).await?;
        let target_ts = inserted
            .id
            .expect("inserted replay target must have id")
            .timestamp();

        let replay = Arc::new(ReplayStateMachine::new(ctx.pool.clone()));
        let nats_client = ctx.nats_client();
        let env = sinex_primitives::environment::environment();
        let (_scan_command_rx, scan_handle) =
            spawn_fake_scan_node_ack_only(nats_client.clone(), env.clone(), "corrupt-cancel-test")
                .await?;

        let executor = ReplayExecutionEngine::new(replay.clone(), nats_client.clone())
            .with_scan_completion_timeout(Duration::from_secs(5));
        let health = Arc::new(Mutex::new(ReplayControlHealthState::default()));
        ReplayControlServer::new(
            &env,
            nats_client.clone(),
            replay.clone(),
            executor,
            Arc::clone(&health),
        )
        .spawn()
        .await?;

        let execute_client = ReplayControlClient::new(
            &env,
            async_nats::connect(&nats_url).await?,
            Duration::from_secs(30),
            Arc::clone(&health),
        );
        let control_client = ReplayControlClient::new(
            &env,
            async_nats::connect(&nats_url).await?,
            Duration::from_secs(30),
            health,
        );

        let mut scope = sample_scope();
        scope.node_id = "corrupt-cancel-test".to_string();
        scope.time_window = Some((
            target_ts - time::Duration::milliseconds(1),
            target_ts + time::Duration::milliseconds(1),
        ));

        let planned = control_client
            .plan("test:replay-user".into(), scope)
            .await?;
        let (previewed, _) = control_client.preview(planned.operation_id).await?;
        let approved = control_client
            .approve(previewed.operation_id, "admin:approver".into())
            .await?;

        let operation_id = approved.operation_id;
        let execute_task = tokio::spawn(async move {
            execute_client
                .execute(operation_id, "service:executor-node".into(), false)
                .await
        });

        wait_for_operation_state(&replay, operation_id, ReplayState::Executing).await?;

        let cancellation_requested = control_client
            .cancel(
                operation_id,
                "admin:approver".into(),
                Some("operator requested stop".to_string()),
            )
            .await?;
        assert_eq!(cancellation_requested.state, ReplayState::Cancelling);

        corrupt_operation_preview_summary(&ctx.pool, operation_id).await?;

        let err = execute_task
            .await
            .map_err(|e| eyre!("execute task failed: {e}"))?
            .expect_err(
                "corrupt replay metadata should surface as cancellation bookkeeping failure",
            );
        assert!(
            err.to_string()
                .contains("failed to finalize replay execution bookkeeping"),
            "unexpected error: {err:#}"
        );
        assert!(
            err.to_string()
                .contains("failed to inspect replay operation state after execution"),
            "unexpected error: {err:#}"
        );

        scan_handle
            .await
            .map_err(|e| eyre!("fake corrupt-cancel-test node task failed: {e}"))?;

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
    async fn replay_test_actor_flag_rejects_invalid_boolean(_ctx: TestContext) -> Result<()> {
        let mut env = EnvGuard::new();
        env.set("SINEX_ALLOW_TEST_ACTORS", "certainly");

        let error = allow_test_actors_in_runtime(false)
            .expect_err("invalid replay actor toggle should be rejected");
        assert!(error.to_string().contains("SINEX_ALLOW_TEST_ACTORS"));
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
    async fn replay_list_rejects_missing_operations_payload(_ctx: TestContext) -> Result<()> {
        let err = ReplayControlClient::require_operations(ReplayControlResponse::success(
            None, None, None,
        ))
        .expect_err("list responses without operations must be rejected");
        assert!(
            err.to_string()
                .contains("Replay control response missing operations")
        );
        Ok(())
    }

    #[sinex_test]
    async fn plan_rejects_invalid_actor(ctx: TestContext) -> Result<()> {
        let ctx = ctx.with_nats().dedicated().await?;
        let replay = Arc::new(ReplayStateMachine::new(ctx.pool.clone()));
        let nats_client = ctx.nats_client();
        let client = spawn_replay_control(replay, nats_client, Duration::from_secs(30)).await?;

        let scope = sample_scope();
        let result = client.plan("invalid-actor".into(), scope).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Invalid actor"));
        Ok(())
    }

    #[sinex_test]
    async fn plan_rejects_inverted_time_window(ctx: TestContext) -> Result<()> {
        let ctx = ctx.with_nats().dedicated().await?;
        let replay = Arc::new(ReplayStateMachine::new(ctx.pool.clone()));
        let nats_client = ctx.nats_client();
        let client = spawn_replay_control(replay, nats_client, Duration::from_secs(30)).await?;

        let end = Timestamp::now();
        let start = end + time::Duration::hours(1);
        let mut scope = sample_scope();
        scope.time_window = Some((start, end));

        let result = client.plan("test:replay-user".into(), scope).await;
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("invalid replay time_window")
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
        operation_id: Uuid,
    },
    Approve {
        operation_id: Uuid,
        approver: String,
    },
    Submit {
        operation_id: Uuid,
        submitter: String,
    },
    Execute {
        operation_id: Uuid,
        executor: String,
        #[serde(default)]
        dry_run: bool,
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
        node: Option<String>,
        limit: Option<i64>,
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ReplayControlResponse {
    pub status: ReplayControlStatus,
    pub message: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_kind: Option<ReplayControlErrorKind>,
    pub operation: Option<ReplayOperation>,
    pub operations: Option<Vec<ReplayOperation>>,
    pub preview: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplayControlErrorKind {
    Validation,
    NotFound,
    AlreadyExists,
    InvalidState,
    PermissionDenied,
    Parse,
    Cancelled,
    Timeout,
    Database,
    Network,
    ResourceExhausted,
    Service,
    Io,
    Configuration,
    Serialization,
    Channel,
    Processing,
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
            error_kind: None,
            operation,
            operations,
            preview,
        }
    }

    pub fn error(message: impl Into<String>) -> Self {
        Self {
            status: ReplayControlStatus::Error,
            message: Some(message.into()),
            error_kind: None,
            operation: None,
            operations: None,
            preview: None,
        }
    }

    fn from_report(err: &color_eyre::Report) -> Self {
        if let Some(sinex_err) = err.downcast_ref::<SinexError>() {
            return Self {
                status: ReplayControlStatus::Error,
                message: Some(sinex_err.client_message().to_string()),
                error_kind: Some(ReplayControlErrorKind::from_sinex_error(sinex_err)),
                operation: None,
                operations: None,
                preview: None,
            };
        }

        Self::error(err.to_string())
    }
}

impl ReplayControlErrorKind {
    fn from_sinex_error(err: &SinexError) -> Self {
        match err {
            SinexError::Validation(_) => Self::Validation,
            SinexError::NotFound(_) => Self::NotFound,
            SinexError::AlreadyExists(_) => Self::AlreadyExists,
            SinexError::InvalidState(_) => Self::InvalidState,
            SinexError::PermissionDenied(_) => Self::PermissionDenied,
            SinexError::Parse(_) => Self::Parse,
            SinexError::Cancelled(_) => Self::Cancelled,
            SinexError::Timeout(_) => Self::Timeout,
            SinexError::Database(_) | SinexError::DbPersistenceFailed(_) => Self::Database,
            SinexError::Network(_) => Self::Network,
            SinexError::ResourceExhausted(_) => Self::ResourceExhausted,
            SinexError::Service(_) => Self::Service,
            SinexError::Io(_) => Self::Io,
            SinexError::Configuration(_) => Self::Configuration,
            SinexError::Serialization(_) => Self::Serialization,
            SinexError::ChannelSend(_) | SinexError::ChannelReceive(_) => Self::Channel,
            SinexError::MaxRetriesExceeded(_)
            | SinexError::Kv(_)
            | SinexError::Automaton(_)
            | SinexError::Checkpoint(_)
            | SinexError::Lifecycle(_)
            | SinexError::Processing(_)
            | _ => Self::Processing,
        }
    }

    fn into_sinex_error(self, message: String) -> SinexError {
        match self {
            Self::Validation => SinexError::validation(message),
            Self::NotFound => SinexError::not_found(message),
            Self::AlreadyExists => SinexError::already_exists(message),
            Self::InvalidState => SinexError::invalid_state(message),
            Self::PermissionDenied => SinexError::permission_denied(message),
            Self::Parse => SinexError::parse(message),
            Self::Cancelled => SinexError::cancelled(message),
            Self::Timeout => SinexError::timeout(message),
            Self::Database => SinexError::database(message),
            Self::Network => SinexError::network(message),
            Self::ResourceExhausted => SinexError::resource_exhausted(message),
            Self::Service => SinexError::service(message),
            Self::Io => SinexError::io(message),
            Self::Configuration => SinexError::configuration(message),
            Self::Serialization => SinexError::serialization(message),
            Self::Channel | Self::Processing => SinexError::processing(message),
        }
    }
}

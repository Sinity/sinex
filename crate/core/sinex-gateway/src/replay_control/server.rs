use async_nats::{Client, Message};
use color_eyre::eyre::{Context, Result, eyre};
use futures::StreamExt;
use parking_lot::Mutex;
use sinex_db::replay::state_machine::ReplayStateMachine;
use sinex_primitives::environment::SinexEnvironment;
use sinex_primitives::nats::{NatsTrafficClass, insert_traffic_class_header};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;
use tracing::{error, info, warn};

use super::protocol::{ReplayControlRequest, ReplayControlResponse};
use super::validation::{
    ReplayAction, ensure_preview_allowed, run_safety_analysis, validate_actor_for_action,
};
use super::{
    ReplayControlError, ReplayControlHealthState, ReplayExecutionEngine, ReplayPreviewSummary,
};

pub(super) const REPLAY_CONTROL_SUBSCRIBE_ATTEMPTS: usize = 5;
pub(super) const REPLAY_CONTROL_SUBSCRIBE_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(2);
pub(super) const REPLAY_CONTROL_SUBSCRIBE_BACKOFF_BASE: Duration = Duration::from_millis(200);
pub(super) const REPLAY_CONTROL_SUBSCRIBE_BACKOFF_MAX: Duration = Duration::from_secs(2);

pub(super) struct ReplayControlServer {
    subject: String,
    client: Client,
    replay: Arc<ReplayStateMachine>,
    executor: ReplayExecutionEngine,
    health: Arc<Mutex<ReplayControlHealthState>>,
}

impl ReplayControlServer {
    pub(super) fn new(
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

    pub(super) async fn spawn(self) -> Result<tokio::task::JoinHandle<()>> {
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

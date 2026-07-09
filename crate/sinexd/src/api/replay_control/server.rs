use async_nats::{Client, Message};
use futures::StreamExt;
use parking_lot::Mutex;
use sinex_db::replay::state_machine::ReplayStateMachine;
use sinex_primitives::environment::SinexEnvironment;
use sinex_primitives::{Result, SinexError, transport};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;
use tracing::{error, info, warn};

use crate::runtime::nats_payload::ensure_nats_payload_fits;

use super::execution::{ReplayExecutionEngine, ReplayPreviewSummary};
use super::protocol::{ReplayControlRequest, ReplayControlResponse};
use super::validation::{
    ReplayAction, ensure_preview_allowed, replay_gate_report, run_safety_analysis,
    validate_actor_for_action,
};
use super::{ReplayControlError, ReplayControlHealthState};

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
                    let Ok(permit) = semaphore.clone().acquire_owned().await else {
                        break 'outer; // semaphore closed (shutdown)
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
                        return Err(err.with_context(
                            "context",
                            "Failed to subscribe to replay control subject",
                        ));
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
            Ok(Err(error)) => Err(SinexError::nats_subscribe(format!(
                "failed to subscribe to replay control subject {subject}"
            ))
            .with_std_error(&error)),
            Err(_) => Err(SinexError::timeout(format!(
                "timed out subscribing to replay control subject {subject} after {REPLAY_CONTROL_SUBSCRIBE_ATTEMPT_TIMEOUT:?}"
            ))),
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
                    ReplayControlResponse::from_sinex_error(&err)
                }
            },
            Err(e) => {
                warn!(error = %e, "Failed to parse replay control request");
                ReplayControlResponse::from_sinex_error(
                    &SinexError::serialization("Invalid request").with_std_error(&e),
                )
            }
        };

        if let Some(reply_subject) = message.reply {
            Self::send_reply_or_compact_error(client, &reply_subject, response).await;
        }

        Ok(())
    }

    /// Publish `response` to `reply_subject`. If it's too large (a legitimate
    /// response for a large replay scope can be hundreds of MB once
    /// `root_event_ids` is enumerated) or fails to serialize, fall back to
    /// publishing a small, generic `ReplayControlResponse::error(..)` instead
    /// of silently dropping the reply.
    ///
    /// Silently not replying (the prior behavior) is worse than a generic
    /// error: the caller's `request()` call has no other signal, so it just
    /// hangs until ITS OWN client-side timeout eventually fires with a
    /// message ("operation timed out") that gives no hint this ever reached
    /// the server, let alone why it failed there. A caller can always retry
    /// a scope-narrowing request in response to a clear error; it cannot
    /// meaningfully react to silence.
    async fn send_reply_or_compact_error(
        client: &Client,
        reply_subject: &str,
        response: ReplayControlResponse,
    ) {
        let mut headers = async_nats::HeaderMap::new();
        transport::insert_transport_class_headers(&mut headers, transport::Class::Control);

        let bytes = match serde_json::to_vec(&response) {
            Ok(bytes) if ensure_nats_payload_fits(
                "replay control response",
                reply_subject,
                bytes.len(),
            )
            .is_ok() =>
            {
                bytes
            }
            Ok(bytes) => {
                error!(
                    target: "sinex_metrics",
                    metric = "gateway.replay_control_failures_total",
                    payload_bytes = bytes.len(),
                    "Replay control response exceeded NATS payload limit; \
                     falling back to a compact error reply instead of dropping it silently"
                );
                match serde_json::to_vec(&ReplayControlResponse::error(
                    "Replay control response exceeded the NATS payload limit -- the operation's \
                     scope likely spans too much data for a single preview/status call. Narrow \
                     the scope (a shorter time window or a material filter) and retry.",
                )) {
                    Ok(bytes) => bytes,
                    Err(err) => {
                        error!(
                            target: "sinex_metrics",
                            metric = "gateway.replay_control_failures_total",
                            ?err,
                            "Failed to serialize even the compact fallback error reply; \
                             reply not sent"
                        );
                        return;
                    }
                }
            }
            Err(err) => {
                error!(
                    target: "sinex_metrics",
                    metric = "gateway.replay_control_failures_total",
                    ?err,
                    "Failed to serialize replay control response; falling back to a compact \
                     error reply instead of dropping it silently"
                );
                match serde_json::to_vec(&ReplayControlResponse::error(
                    "Replay control response failed to serialize on the server; \
                     see server logs for details.",
                )) {
                    Ok(bytes) => bytes,
                    Err(err) => {
                        error!(
                            target: "sinex_metrics",
                            metric = "gateway.replay_control_failures_total",
                            ?err,
                            "Failed to serialize even the compact fallback error reply; \
                             reply not sent"
                        );
                        return;
                    }
                }
            }
        };

        if let Err(err) = client
            .publish_with_headers(reply_subject.to_string(), headers, bytes.into())
            .await
        {
            error!(
                target: "sinex_metrics",
                metric = "gateway.replay_control_failures_total",
                ?err,
                "Failed to send replay control response"
            );
        }
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
                    .map_err(|e| {
                        SinexError::serialization("Invalid replay preview summary")
                            .with_std_error(&e)
                    })?;
                let safety = run_safety_analysis(replay.pool(), &root_ids).await;
                if let serde_json::Value::Object(ref mut map) = preview {
                    let max_observed_depth = safety
                        .get("max_depth")
                        .and_then(serde_json::Value::as_u64)
                        .or_else(|| {
                            map.get("cascade_impact")
                                .and_then(|cascade| cascade.get("max_depth"))
                                .and_then(serde_json::Value::as_u64)
                        })
                        .unwrap_or(0);
                    map.insert(
                        "max_observed_depth".to_string(),
                        serde_json::json!(max_observed_depth),
                    );
                    map.insert("safety_analysis".to_string(), safety);
                }
                let gate_report = replay_gate_report(&preview);
                if let serde_json::Value::Object(ref mut map) = preview {
                    map.insert("replay_gates".to_string(), gate_report);
                }

                // Store the FULL preview (including root_event_ids) -- execution reads
                // it back from operations_log.preview_summary later (replay_writer.rs)
                // to verify the root set hasn't drifted since preview and to sanity-check
                // root_event_ids.len() == total_events (state_machine.rs ~858-872). This
                // is load-bearing, not display data, so it must not be trimmed here.
                replay.update_preview(operation_id, preview.clone()).await?;
                let updated = replay.load_operation(operation_id).await?;

                // The CLIENT-FACING response must NOT include the full root_event_ids
                // array: for a scope covering real event volume this is hundreds of
                // thousands of UUIDs, producing a reply payload of hundreds of MB --
                // sinexd's own oversized-publish guard then silently refuses to send it
                // at all (`nats_payload::publish` logs "Refusing oversized NATS publish"
                // and the RPC caller hangs until ITS OWN timeout fires, with no error
                // surfaced anywhere that explains why). sinexctl's own preview rendering
                // (crate/sinexctl/src/commands/replay.rs) never reads root_event_ids --
                // it's purely an execution-integrity artifact, never display data.
                let client_preview = match &preview {
                    serde_json::Value::Object(map) => {
                        let mut trimmed = map.clone();
                        if let Some(serde_json::Value::Array(ids)) =
                            trimmed.get("root_event_ids")
                        {
                            trimmed.insert(
                                "root_event_ids_count".to_string(),
                                serde_json::json!(ids.len()),
                            );
                        }
                        trimmed.remove("root_event_ids");
                        serde_json::Value::Object(trimmed)
                    }
                    other => other.clone(),
                };
                ReplayControlResponse::success(Some(updated), Some(client_preview), None)
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
                gate_overrides,
            } => {
                validate_actor_for_action(&submitter, ReplayAction::Approve)?;
                validate_actor_for_action(&submitter, ReplayAction::Execute)?;

                let updated = executor
                    .submit_with_overrides(operation_id, submitter, gate_overrides)
                    .await?;
                ReplayControlResponse::success(Some(updated), None, None)
            }
            ReplayControlRequest::Execute {
                operation_id,
                executor: actor,
                dry_run,
                gate_overrides,
            } => {
                // Server-side validation of executor (defense in depth)
                validate_actor_for_action(&actor, ReplayAction::Execute)?;

                if dry_run {
                    return Err(SinexError::validation(
                        "Replay execute does not support dry-run semantics; use preview before approval instead",
                    ));
                }
                let updated = executor
                    .execute_with_overrides(operation_id, actor, gate_overrides)
                    .await?;
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
            ReplayControlRequest::List {
                state,
                module,
                limit,
            } => {
                let ops = replay
                    .list_operations(state, module.as_deref(), limit)
                    .await?;
                ReplayControlResponse::success(None, None, Some(ops))
            }
        };

        Ok(response)
    }
}

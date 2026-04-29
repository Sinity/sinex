use async_nats::connection::State as NatsState;
use color_eyre::eyre::{Context, Result, eyre};
use parking_lot::Mutex;
use std::sync::Arc;
use std::time::Duration;

use async_nats::Client;
use sinex_db::replay::state_machine::{ReplayOperation, ReplayScope, ReplayState};
use sinex_primitives::Uuid;
use sinex_primitives::environment::SinexEnvironment;

use super::protocol::{
    ReplayControlErrorKind, ReplayControlRequest, ReplayControlResponse, ReplayControlStatus,
};
use super::validation::{ReplayAction, validate_actor_for_action};
use super::{ReplayControlError, ReplayControlHealth, ReplayControlHealthState};

/// Client for issuing replay control commands over NATS.
#[derive(Clone)]
pub struct ReplayControlClient {
    subject: String,
    client: Client,
    health: Arc<Mutex<ReplayControlHealthState>>,
    request_timeout: Duration,
}

impl ReplayControlClient {
    pub(super) fn new(
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

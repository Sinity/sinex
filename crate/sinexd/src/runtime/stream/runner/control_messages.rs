//! Control-plane message helpers for `RuntimeRunner`.
//!
//! Hosts the static helpers that build, sign, and publish control-plane NATS
//! messages: scan acknowledgements, scan progress, drain completion signals,
//! and the drain-command handler.

#[cfg(feature = "messaging")]
use super::super::control_protocol::RuntimeDrainComplete;
use super::super::control_protocol::{encode_control_message, ensure_control_payload_fits};
use super::{
    RuntimeDrainController, RuntimeRunner, ServiceInfo, SourceScanAck, SourceScanProgress,
};
use crate::runtime::{RuntimeResult, SinexError};
#[cfg(feature = "db")]
use sinex_db::DbPool as PgPool;
use sinex_primitives::domain::ModuleState;
use sinex_primitives::transport;
use tracing::{info, warn};

impl RuntimeRunner {
    pub(super) async fn publish_scan_ack(
        nats_client: &async_nats::Client,
        reply: Option<async_nats::Subject>,
        ack: &SourceScanAck,
    ) -> RuntimeResult<()> {
        let Some(reply) = reply else {
            return Ok(());
        };

        let payload = match encode_control_message(
            "scan acknowledgement",
            ack.operation_id,
            &ack.module_name,
            ack,
        ) {
            Ok(payload) => payload,
            Err(error) => {
                warn!(
                    operation_id = %ack.operation_id,
                    module = %ack.module_name,
                    error = %error,
                    "Failed to encode scan acknowledgement"
                );
                return Err(error);
            }
        };

        let mut headers = async_nats::HeaderMap::new();
        transport::insert_transport_class_headers(&mut headers, transport::Class::Control);
        ensure_control_payload_fits(
            "Failed to publish scan acknowledgement",
            reply.as_ref(),
            &ack.module_name,
            Some(ack.operation_id),
            payload.len(),
        )?;

        nats_client
            .publish_with_headers(reply.clone(), headers, payload.into())
            .await
            .map_err(|error| {
                SinexError::messaging("Failed to publish scan acknowledgement")
                    .with_context("operation_id", ack.operation_id.to_string())
                    .with_context("module", ack.module_name.clone())
                    .with_context("subject", reply.to_string())
                    .with_std_error(&error)
            })
    }

    pub(super) async fn publish_scan_progress(
        nats_client: &async_nats::Client,
        subject: String,
        progress: &SourceScanProgress,
    ) -> RuntimeResult<()> {
        let payload = match encode_control_message(
            "scan progress update",
            progress.operation_id,
            &progress.module_name,
            progress,
        ) {
            Ok(payload) => payload,
            Err(error) => {
                warn!(
                    operation_id = %progress.operation_id,
                    module = %progress.module_name,
                    error = %error,
                    "Failed to encode scan progress update"
                );
                return Err(error);
            }
        };

        let mut headers = async_nats::HeaderMap::new();
        transport::insert_transport_class_headers(&mut headers, transport::Class::Control);
        ensure_control_payload_fits(
            "Failed to publish scan progress update",
            &subject,
            &progress.module_name,
            Some(progress.operation_id),
            payload.len(),
        )?;

        nats_client
            .publish_with_headers(subject.clone(), headers, payload.into())
            .await
            .map_err(|error| {
                SinexError::messaging("Failed to publish scan progress update")
                    .with_context("operation_id", progress.operation_id.to_string())
                    .with_context("module", progress.module_name.clone())
                    .with_context("subject", subject)
                    .with_std_error(&error)
            })
    }

    #[cfg(feature = "messaging")]
    pub(super) async fn publish_drain_complete(
        nats_client: &async_nats::Client,
        module_name: &str,
        payload: &RuntimeDrainComplete,
    ) -> RuntimeResult<()> {
        let subject = sinex_primitives::environment::environment().nats_subject(&format!(
            "sinex.control.sources.{module_name}.drain_complete"
        ));
        let encoded = serde_json::to_vec(payload).map_err(|error| {
            SinexError::serialization(format!(
                "Failed to serialize drain_complete payload for module '{module_name}': {error}"
            ))
        })?;
        let mut headers = async_nats::HeaderMap::new();
        transport::insert_transport_class_headers(&mut headers, transport::Class::Control);
        ensure_control_payload_fits(
            "Failed to publish drain_complete signal",
            &subject,
            module_name,
            None,
            encoded.len(),
        )?;

        nats_client
            .publish_with_headers(subject.clone(), headers, encoded.into())
            .await
            .map_err(|error| {
                SinexError::messaging("Failed to publish drain_complete signal")
                    .with_context("module", module_name.to_string())
                    .with_context("subject", subject)
                    .with_std_error(&error)
            })
    }

    #[cfg(feature = "messaging")]
    pub(super) async fn handle_drain_command(
        module_name: &str,
        payload: &[u8],
        drain: &RuntimeDrainController,
        #[cfg(feature = "db")] pool: Option<PgPool>,
        service_info: &ServiceInfo,
    ) {
        let command = match serde_json::from_slice::<
            sinex_primitives::rpc::runtime::RuntimeDrainRequest,
        >(payload)
        {
            Ok(command) => command,
            Err(error) => {
                warn!(
                    module = %module_name,
                    error = %error,
                    "Ignoring malformed drain command"
                );
                return;
            }
        };

        if command.module_name.as_ref() != module_name {
            warn!(
                module = %module_name,
                requested = %command.module_name,
                "Ignoring drain command addressed to a different module"
            );
            return;
        }

        let first_request = drain.request_drain();
        let aborted_runtime_work = drain.abort_runtime_work();
        info!(
            module = %module_name,
            reason = ?command.reason,
            first_request,
            aborted_runtime_work,
            "Accepted drain command"
        );

        #[cfg(feature = "db")]
        if let Some(pool) = pool {
            Self::update_registered_run_status(&pool, service_info, ModuleState::Draining).await;
        }
    }
}

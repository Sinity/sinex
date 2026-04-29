//! Control-plane message helpers for `NodeRunner<T>`.
//!
//! Hosts the static helpers that build, sign, and publish control-plane NATS
//! messages: scan acknowledgements, scan progress, drain completion signals,
//! and the drain-command handler. Also includes JSON canonicalization and
//! effective-config hashing used by registration.

use super::super::control_protocol::{ensure_control_payload_fits, encode_control_message};
#[cfg(feature = "messaging")]
use super::super::control_protocol::NodeDrainComplete;
use super::{Node, NodeRunner, NodeScanAck, NodeScanProgress, RuntimeDrainController, ServiceInfo};
use crate::{NodeResult, SinexError};
#[cfg(feature = "db")]
use sinex_db::DbPool as PgPool;
use sinex_primitives::domain::NodeState;
use sinex_primitives::nats::{NatsTrafficClass, insert_traffic_class_header};
use std::collections::{BTreeMap, HashMap};
use tracing::{info, warn};

impl<T: Node + 'static> NodeRunner<T> {
    pub(super) fn canonicalize_json(value: serde_json::Value) -> serde_json::Value {
        match value {
            serde_json::Value::Array(values) => {
                serde_json::Value::Array(values.into_iter().map(Self::canonicalize_json).collect())
            }
            serde_json::Value::Object(map) => {
                let ordered = map
                    .into_iter()
                    .map(|(key, value)| (key, Self::canonicalize_json(value)))
                    .collect::<BTreeMap<_, _>>();
                serde_json::Value::Object(ordered.into_iter().collect())
            }
            other => other,
        }
    }

    pub(super) fn effective_config(
        raw_config: &HashMap<String, serde_json::Value>,
    ) -> NodeResult<(Option<String>, Option<serde_json::Value>)> {
        if raw_config.is_empty() {
            return Ok((None, None));
        }

        let config_value = serde_json::to_value(raw_config).map_err(|error| {
            SinexError::configuration(format!(
                "Failed to serialize effective runtime config: {error}"
            ))
        })?;
        let canonical = Self::canonicalize_json(config_value);
        let encoded = serde_json::to_vec(&canonical).map_err(|error| {
            SinexError::configuration(format!(
                "Failed to encode effective runtime config: {error}"
            ))
        })?;
        let config_hash = blake3::hash(&encoded).to_hex().to_string();
        Ok((Some(config_hash), Some(canonical)))
    }

    pub(super) async fn publish_scan_ack(
        nats_client: &async_nats::Client,
        reply: Option<async_nats::Subject>,
        ack: &NodeScanAck,
    ) -> NodeResult<()> {
        let Some(reply) = reply else {
            return Ok(());
        };

        let payload = match encode_control_message(
            "scan acknowledgement",
            ack.operation_id,
            &ack.node_name,
            ack,
        ) {
            Ok(payload) => payload,
            Err(error) => {
                warn!(
                    operation_id = %ack.operation_id,
                    node = %ack.node_name,
                    error = %error,
                    "Failed to encode scan acknowledgement"
                );
                return Err(error);
            }
        };

        let mut headers = async_nats::HeaderMap::new();
        insert_traffic_class_header(&mut headers, NatsTrafficClass::Control);
        ensure_control_payload_fits(
            "Failed to publish scan acknowledgement",
            reply.as_ref(),
            &ack.node_name,
            Some(ack.operation_id),
            payload.len(),
        )?;

        nats_client
            .publish_with_headers(reply.clone(), headers, payload.into())
            .await
            .map_err(|error| {
                SinexError::messaging("Failed to publish scan acknowledgement")
                    .with_context("operation_id", ack.operation_id.to_string())
                    .with_context("node", ack.node_name.clone())
                    .with_context("subject", reply.to_string())
                    .with_std_error(&error)
            })
    }

    pub(super) async fn publish_scan_progress(
        nats_client: &async_nats::Client,
        subject: String,
        progress: &NodeScanProgress,
    ) -> NodeResult<()> {
        let payload = match encode_control_message(
            "scan progress update",
            progress.operation_id,
            &progress.node_name,
            progress,
        ) {
            Ok(payload) => payload,
            Err(error) => {
                warn!(
                    operation_id = %progress.operation_id,
                    node = %progress.node_name,
                    error = %error,
                    "Failed to encode scan progress update"
                );
                return Err(error);
            }
        };

        let mut headers = async_nats::HeaderMap::new();
        insert_traffic_class_header(&mut headers, NatsTrafficClass::Control);
        ensure_control_payload_fits(
            "Failed to publish scan progress update",
            &subject,
            &progress.node_name,
            Some(progress.operation_id),
            payload.len(),
        )?;

        nats_client
            .publish_with_headers(subject.clone(), headers, payload.into())
            .await
            .map_err(|error| {
                SinexError::messaging("Failed to publish scan progress update")
                    .with_context("operation_id", progress.operation_id.to_string())
                    .with_context("node", progress.node_name.clone())
                    .with_context("subject", subject)
                    .with_std_error(&error)
            })
    }

    #[cfg(feature = "messaging")]
    pub(super) async fn publish_drain_complete(
        nats_client: &async_nats::Client,
        node_name: &str,
        payload: &NodeDrainComplete,
    ) -> NodeResult<()> {
        let subject = sinex_primitives::environment::environment()
            .nats_subject(&format!("sinex.control.nodes.{node_name}.drain_complete"));
        let encoded = serde_json::to_vec(payload).map_err(|error| {
            SinexError::serialization(format!(
                "Failed to serialize drain_complete payload for node '{node_name}': {error}"
            ))
        })?;
        let mut headers = async_nats::HeaderMap::new();
        insert_traffic_class_header(&mut headers, NatsTrafficClass::Control);
        ensure_control_payload_fits(
            "Failed to publish drain_complete signal",
            &subject,
            node_name,
            None,
            encoded.len(),
        )?;

        nats_client
            .publish_with_headers(subject.clone(), headers, encoded.into())
            .await
            .map_err(|error| {
                SinexError::messaging("Failed to publish drain_complete signal")
                    .with_context("node", node_name.to_string())
                    .with_context("subject", subject)
                    .with_std_error(&error)
            })
    }

    #[cfg(feature = "messaging")]
    pub(super) async fn handle_drain_command(
        node_name: &str,
        payload: &[u8],
        drain: &RuntimeDrainController,
        #[cfg(feature = "db")] pool: Option<PgPool>,
        service_info: &ServiceInfo,
    ) {
        let command =
            match serde_json::from_slice::<sinex_primitives::rpc::nodes::NodeDrainRequest>(payload)
            {
                Ok(command) => command,
                Err(error) => {
                    warn!(
                        node = %node_name,
                        error = %error,
                        "Ignoring malformed drain command"
                    );
                    return;
                }
            };

        if command.node_id.as_ref() != node_name {
            warn!(
                node = %node_name,
                requested = %command.node_id,
                "Ignoring drain command addressed to a different node"
            );
            return;
        }

        let first_request = drain.request_drain();
        let aborted_runtime_work = drain.abort_runtime_work();
        info!(
            node = %node_name,
            reason = ?command.reason,
            first_request,
            aborted_runtime_work,
            "Accepted drain command"
        );

        #[cfg(feature = "db")]
        if let Some(pool) = pool {
            Self::update_registered_run_status(&pool, service_info, NodeState::Draining).await;
        }
    }

}

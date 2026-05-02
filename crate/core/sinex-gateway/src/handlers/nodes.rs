//! Node operations handlers
//!
//! This module provides RPC endpoints for managing node operations:
//! - List nodes and their status
//! - Drain nodes (pause event processing)
//! - Resume nodes (restart event processing)
//! - Set processing horizon (control replay boundaries)

use serde_json::{Value, json};
use sinex_primitives::temporal::Timestamp;
use sinex_primitives::{
    SinexError, domain::OperationStatus, environment::SinexEnvironment, transport,
};
use std::error::Error as _;

// Re-export shared types for use by other modules
pub use sinex_primitives::rpc::nodes::{
    NodeDrainRequest, NodeDrainResponse, NodeResumeRequest, NodeResumeResponse,
    NodeSetHorizonRequest, NodeSetHorizonResponse, NodeStatus, NodesListRequest, NodesListResponse,
};

type Result<T> = std::result::Result<T, SinexError>;

async fn publish_node_control(
    nats_client: &async_nats::Client,
    subject: String,
    payload: Value,
    operation: &'static str,
) -> Result<()> {
    let mut headers = async_nats::HeaderMap::new();
    transport::insert_transport_class_headers(&mut headers, transport::Class::Control);

    nats_client
        .publish_with_headers(
            subject.clone(),
            headers,
            serde_json::to_vec(&payload)
                .map_err(|e| {
                    SinexError::serialization(format!("failed to serialize {operation} payload"))
                        .with_std_error(&e)
                })?
                .into(),
        )
        .await
        .map_err(|e| {
            SinexError::nats_publish(operation)
                .with_context("subject", &subject)
                .with_std_error(&e)
        })
}

fn is_missing_node_state_bucket(error: &async_nats::jetstream::context::KeyValueError) -> bool {
    use async_nats::jetstream::ErrorCode;
    use async_nats::jetstream::context::{GetStreamError, GetStreamErrorKind, KeyValueErrorKind};

    if error.kind() != KeyValueErrorKind::GetBucket {
        return false;
    }

    let Some(source) = error.source() else {
        return false;
    };
    let Some(stream_error) = source.downcast_ref::<GetStreamError>() else {
        return false;
    };

    matches!(
        stream_error.kind(),
        GetStreamErrorKind::JetStream(js_error)
            if js_error.error_code() == ErrorCode::STREAM_NOT_FOUND
    )
}

/// Handle GET /nodes request - list all nodes
pub async fn handle_nodes_list(
    nats_client: &async_nats::Client,
    env: &SinexEnvironment,
    _params: Value,
) -> Result<Value> {
    // Query node status from KV store
    let js = async_nats::jetstream::new(nats_client.clone());

    let kv_bucket_name = env.nats_kv_bucket_name("sinex_node_state");

    // Treat the missing bucket as an honest empty registry, but surface every
    // other JetStream failure instead of pretending there are no nodes.
    let kv = match js.get_key_value(&kv_bucket_name).await {
        Ok(kv) => kv,
        Err(error) if is_missing_node_state_bucket(&error) => {
            return serde_json::to_value(NodesListResponse { nodes: Vec::new() }).map_err(|e| {
                SinexError::serialization("failed to serialize node list response")
                    .with_std_error(&e)
            });
        }
        Err(error) => {
            return Err(SinexError::kv("Failed to open node state bucket")
                .with_context("bucket", kv_bucket_name)
                .with_source(error));
        }
    };

    // Get all keys in the bucket (each key is a node ID)
    let mut nodes = Vec::new();

    // Watch for all entries (one-time scan)
    let mut entries = kv
        .keys()
        .await
        .map_err(|e| SinexError::kv("Failed to list node keys").with_source(e))?;

    use futures::StreamExt;
    while let Some(key) = entries.next().await {
        let key = key.map_err(|e| SinexError::kv("Failed to read key").with_source(e))?;

        // Get the value for this key
        let entry = kv
            .get(&key)
            .await
            .map_err(|e| {
                SinexError::kv("Failed to fetch node state")
                    .with_context("node_state_key", key.clone())
                    .with_source(e)
            })?
            .ok_or_else(|| {
                SinexError::not_found("Node state disappeared during listing")
                    .with_context("node_state_key", key.clone())
            })?;

        let state_json = String::from_utf8(entry.to_vec()).map_err(|error| {
            SinexError::serialization("Node state is not valid UTF-8")
                .with_context("node_state_key", key.clone())
                .with_std_error(&error)
        })?;
        let state = serde_json::from_str::<NodeStatus>(&state_json).map_err(|error| {
            SinexError::serialization("Node state is not valid JSON")
                .with_context("node_state_key", key.clone())
                .with_std_error(&error)
        })?;
        nodes.push(state);
    }

    serde_json::to_value(NodesListResponse { nodes }).map_err(|e| {
        SinexError::serialization("failed to serialize node list response").with_std_error(&e)
    })
}

/// Handle POST /nodes/{id}/drain - pause node processing
///
/// # Authorization
///
/// Node drain is a production-impacting operation. The auth context is
/// logged for audit purposes.
pub async fn handle_nodes_drain(
    nats_client: &async_nats::Client,
    env: &SinexEnvironment,
    params: Value,
    auth: &crate::rpc_server::RpcAuthContext,
) -> Result<Value> {
    use tracing::info;

    let drain_params: NodeDrainRequest = serde_json::from_value(params)
        .map_err(|e| SinexError::serialization("invalid drain request").with_std_error(&e))?;

    info!(
        actor = %auth.actor_id(),
        node_id = %drain_params.node_id,
        reason = ?drain_params.reason,
        "Node drain initiated"
    );

    // Publish drain command to NATS control subject
    let subject = env.nats_subject(&format!(
        "sinex.control.nodes.{}.drain",
        drain_params.node_id
    ));

    let payload = json!({
        "action": "drain",
        "node_id": drain_params.node_id,
        "reason": drain_params.reason,
        "timestamp": Timestamp::now(),
    });

    publish_node_control(nats_client, subject, payload, "drain command").await?;

    serde_json::to_value(NodeDrainResponse {
        status: OperationStatus::Pending,
        node_id: drain_params.node_id,
    })
    .map_err(|e| SinexError::serialization("failed to serialize drain response").with_std_error(&e))
}

/// Handle POST /nodes/{id}/resume - resume node processing
///
/// # Authorization
///
/// Node resume is a production-impacting operation. The auth context is
/// logged for audit purposes.
pub async fn handle_nodes_resume(
    nats_client: &async_nats::Client,
    env: &SinexEnvironment,
    params: Value,
    auth: &crate::rpc_server::RpcAuthContext,
) -> Result<Value> {
    use tracing::info;

    let resume_params: NodeResumeRequest = serde_json::from_value(params)
        .map_err(|e| SinexError::serialization("invalid resume request").with_std_error(&e))?;

    info!(
        actor = %auth.actor_id(),
        node_id = %resume_params.node_id,
        "Node resume initiated"
    );

    // Publish resume command to NATS control subject
    let subject = env.nats_subject(&format!(
        "sinex.control.nodes.{}.resume",
        resume_params.node_id
    ));

    let payload = json!({
        "action": "resume",
        "node_id": resume_params.node_id,
        "timestamp": Timestamp::now(),
    });

    publish_node_control(nats_client, subject, payload, "resume command").await?;

    serde_json::to_value(NodeResumeResponse {
        status: OperationStatus::Pending,
        node_id: resume_params.node_id,
    })
    .map_err(|e| {
        SinexError::serialization("failed to serialize resume response").with_std_error(&e)
    })
}

/// Handle POST /nodes/{id}/set-horizon - set processing horizon
///
/// # Authorization
///
/// Setting the replay horizon can cause data reprocessing or loss.
/// The auth context is logged for audit purposes.
pub async fn handle_nodes_set_horizon(
    nats_client: &async_nats::Client,
    env: &SinexEnvironment,
    params: Value,
    auth: &crate::rpc_server::RpcAuthContext,
) -> Result<Value> {
    use tracing::info;

    let horizon_params: NodeSetHorizonRequest = serde_json::from_value(params)
        .map_err(|e| SinexError::serialization("invalid set-horizon request").with_std_error(&e))?;

    info!(
        actor = %auth.actor_id(),
        node_id = %horizon_params.node_id,
        horizon = %horizon_params.horizon,
        "Node set-horizon initiated"
    );

    // Publish set-horizon command to NATS control subject
    let subject = env.nats_subject(&format!(
        "sinex.control.nodes.{}.set-horizon",
        horizon_params.node_id
    ));

    let payload = json!({
        "action": "set_horizon",
        "node_id": horizon_params.node_id,
        "horizon": horizon_params.horizon,
        "timestamp": Timestamp::now(),
    });

    publish_node_control(nats_client, subject, payload, "set-horizon command").await?;

    serde_json::to_value(NodeSetHorizonResponse {
        status: OperationStatus::Pending,
        node_id: horizon_params.node_id,
        horizon: horizon_params.horizon,
    })
    .map_err(|e| {
        SinexError::serialization("failed to serialize set-horizon response").with_std_error(&e)
    })
}

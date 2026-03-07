//! Node operations handlers
//!
//! This module provides RPC endpoints for managing node operations:
//! - List nodes and their status
//! - Drain nodes (pause event processing)
//! - Resume nodes (restart event processing)
//! - Set processing horizon (control replay boundaries)

use serde_json::{Value, json};
use sinex_primitives::temporal::Timestamp;
use sinex_primitives::{SinexError, environment::SinexEnvironment};

// Re-export shared types for use by other modules
pub use sinex_primitives::rpc::nodes::{
    NodeDrainRequest, NodeDrainResponse, NodeResumeRequest, NodeResumeResponse,
    NodeSetHorizonRequest, NodeSetHorizonResponse, NodeStatus, NodesListRequest, NodesListResponse,
};

type Result<T> = std::result::Result<T, SinexError>;

/// Handle GET /nodes request - list all nodes
pub async fn handle_nodes_list(
    nats_client: &async_nats::Client,
    env: &SinexEnvironment,
    _params: Value,
) -> Result<Value> {
    // Query node status from KV store
    let js = async_nats::jetstream::new(nats_client.clone());

    let kv_bucket_name = env.nats_kv_bucket_name("sinex_node_state");

    // Try to get the KV bucket - if it doesn't exist, return empty list
    let Ok(kv) = js.get_key_value(&kv_bucket_name).await else {
        // Bucket doesn't exist yet, return empty node list
        return Ok(json!({
            "nodes": [],
        }));
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
        if let Ok(Some(entry)) = kv.get(&key).await
            && let Ok(state_json) = String::from_utf8(entry.to_vec())
            && let Ok(state) = serde_json::from_str::<NodeStatus>(&state_json)
        {
            nodes.push(state);
        }
    }

    Ok(json!({
        "nodes": nodes,
    }))
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
        token_prefix = %auth.token_prefix,
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

    nats_client
        .publish(
            subject.clone(),
            serde_json::to_vec(&payload)
                .map_err(|e| {
                    SinexError::serialization("failed to serialize drain payload")
                        .with_std_error(&e)
                })?
                .into(),
        )
        .await
        .map_err(|e| {
            SinexError::nats_publish("drain command")
                .with_context("subject", &subject)
                .with_std_error(&e)
        })?;

    Ok(json!({
        "status": "drain_requested",
        "node_id": drain_params.node_id,
    }))
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
        token_prefix = %auth.token_prefix,
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

    nats_client
        .publish(
            subject.clone(),
            serde_json::to_vec(&payload)
                .map_err(|e| {
                    SinexError::serialization("failed to serialize resume payload")
                        .with_std_error(&e)
                })?
                .into(),
        )
        .await
        .map_err(|e| {
            SinexError::nats_publish("resume command")
                .with_context("subject", &subject)
                .with_std_error(&e)
        })?;

    Ok(json!({
        "status": "resume_requested",
        "node_id": resume_params.node_id,
    }))
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
        token_prefix = %auth.token_prefix,
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

    nats_client
        .publish(
            subject.clone(),
            serde_json::to_vec(&payload)
                .map_err(|e| {
                    SinexError::serialization("failed to serialize set-horizon payload")
                        .with_std_error(&e)
                })?
                .into(),
        )
        .await
        .map_err(|e| {
            SinexError::nats_publish("set-horizon command")
                .with_context("subject", &subject)
                .with_std_error(&e)
        })?;

    Ok(json!({
        "status": "horizon_update_requested",
        "node_id": horizon_params.node_id,
        "horizon": horizon_params.horizon,
    }))
}

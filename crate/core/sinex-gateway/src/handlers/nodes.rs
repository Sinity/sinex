//! Node operations handlers
//!
//! This module provides RPC endpoints for managing node operations:
//! - List nodes and their status
//! - Drain nodes (pause event processing)
//! - Resume nodes (restart event processing)
//! - Set processing horizon (control replay boundaries)

use serde_json::{json, Value};
use sinex_primitives::{environment::SinexEnvironment, SinexError};
use sinex_primitives::temporal::Timestamp;

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
    let kv = match js.get_key_value(&kv_bucket_name).await {
        Ok(kv) => kv,
        Err(_) => {
            // Bucket doesn't exist yet, return empty node list
            return Ok(json!({
                "nodes": [],
            }));
        }
    };

    // Get all keys in the bucket (each key is a node ID)
    let mut nodes = Vec::new();

    // Watch for all entries (one-time scan)
    let mut entries = kv
        .keys()
        .await
        .map_err(|e| SinexError::service(format!("Failed to list node keys: {}", e)))?;

    use futures::StreamExt;
    while let Some(key) = entries.next().await {
        let key = key.map_err(|e| SinexError::service(format!("Failed to read key: {}", e)))?;

        // Get the value for this key
        if let Ok(Some(entry)) = kv.get(&key).await {
            if let Ok(state_json) = String::from_utf8(entry.to_vec()) {
                if let Ok(state) = serde_json::from_str::<NodeStatus>(&state_json) {
                    nodes.push(state);
                }
            }
        }
    }

    Ok(json!({
        "nodes": nodes,
    }))
}

/// Handle POST /nodes/{id}/drain - pause node processing
pub async fn handle_nodes_drain(
    nats_client: &async_nats::Client,
    env: &SinexEnvironment,
    params: Value,
) -> Result<Value> {
    let drain_params: NodeDrainRequest =
        serde_json::from_value(params).map_err(|e| SinexError::serialization(e.to_string()))?;

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
            subject,
            serde_json::to_vec(&payload)
                .map_err(|e| SinexError::serialization(e.to_string()))?
                .into(),
        )
        .await
        .map_err(|e| SinexError::service(format!("Failed to publish drain command: {}", e)))?;

    Ok(json!({
        "status": "drain_requested",
        "node_id": drain_params.node_id,
    }))
}

/// Handle POST /nodes/{id}/resume - resume node processing
pub async fn handle_nodes_resume(
    nats_client: &async_nats::Client,
    env: &SinexEnvironment,
    params: Value,
) -> Result<Value> {
    let resume_params: NodeResumeRequest =
        serde_json::from_value(params).map_err(|e| SinexError::serialization(e.to_string()))?;

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
            subject,
            serde_json::to_vec(&payload)
                .map_err(|e| SinexError::serialization(e.to_string()))?
                .into(),
        )
        .await
        .map_err(|e| SinexError::service(format!("Failed to publish resume command: {}", e)))?;

    Ok(json!({
        "status": "resume_requested",
        "node_id": resume_params.node_id,
    }))
}

/// Handle POST /nodes/{id}/set-horizon - set processing horizon
pub async fn handle_nodes_set_horizon(
    nats_client: &async_nats::Client,
    env: &SinexEnvironment,
    params: Value,
) -> Result<Value> {
    let horizon_params: NodeSetHorizonRequest =
        serde_json::from_value(params).map_err(|e| SinexError::serialization(e.to_string()))?;

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
            subject,
            serde_json::to_vec(&payload)
                .map_err(|e| SinexError::serialization(e.to_string()))?
                .into(),
        )
        .await
        .map_err(|e| {
            SinexError::service(format!("Failed to publish set-horizon command: {}", e))
        })?;

    Ok(json!({
        "status": "horizon_update_requested",
        "node_id": horizon_params.node_id,
        "horizon": horizon_params.horizon,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinex_primitives::environment;
    use xtask::sandbox::{sinex_test, EphemeralNats};

    type TestResult<T> = std::result::Result<T, Box<dyn std::error::Error>>;

    #[sinex_test]
    async fn nodes_list_returns_empty_when_no_bucket() -> TestResult<()> {
        let nats = EphemeralNats::start().await?;
        let client = nats.connect().await?;
        let env = environment();

        let result = handle_nodes_list(&client, &env, json!({})).await?;
        assert_eq!(result["nodes"].as_array().unwrap().len(), 0);

        Ok(())
    }

    #[sinex_test]
    async fn nodes_drain_publishes_command() -> TestResult<()> {
        let nats = EphemeralNats::start().await?;
        let client = nats.connect().await?;
        let env = environment();

        let params = json!({
            "node_id": "test-node-123",
            "reason": "maintenance",
        });

        let result = handle_nodes_drain(&client, &env, params).await?;
        assert_eq!(result["status"], "drain_requested");
        assert_eq!(result["node_id"], "test-node-123");

        Ok(())
    }

    #[sinex_test]
    async fn nodes_resume_publishes_command() -> TestResult<()> {
        let nats = EphemeralNats::start().await?;
        let client = nats.connect().await?;
        let env = environment();

        let params = json!({
            "node_id": "test-node-456",
        });

        let result = handle_nodes_resume(&client, &env, params).await?;
        assert_eq!(result["status"], "resume_requested");
        assert_eq!(result["node_id"], "test-node-456");

        Ok(())
    }

    #[sinex_test]
    async fn nodes_set_horizon_validates_timestamp() -> TestResult<()> {
        let nats = EphemeralNats::start().await?;
        let client = nats.connect().await?;
        let env = environment();

        // Invalid timestamp should fail
        let invalid_params = json!({
            "node_id": "test-node-789",
            "horizon": "not-a-timestamp",
        });

        let err = handle_nodes_set_horizon(&client, &env, invalid_params)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("serialization"));

        // Valid timestamp should succeed
        let valid_params = json!({
            "node_id": "test-node-789",
            "horizon": "2024-01-15T10:00:00Z",
        });

        let result = handle_nodes_set_horizon(&client, &env, valid_params).await?;
        assert_eq!(result["status"], "horizon_update_requested");

        Ok(())
    }
}

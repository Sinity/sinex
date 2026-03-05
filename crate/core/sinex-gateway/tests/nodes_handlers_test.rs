mod common;

use common::{NatsHarness, admin_auth};
use serde_json::json;
use sinex_gateway::handlers::{
    handle_nodes_drain, handle_nodes_list, handle_nodes_resume, handle_nodes_set_horizon,
};
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn nodes_list_returns_empty_when_no_bucket() -> TestResult<()> {
    let harness = NatsHarness::start().await?;

    let result = handle_nodes_list(&harness.client, &harness.env, json!({})).await?;
    assert_eq!(result["nodes"].as_array().map_or(0, |nodes| nodes.len()), 0);

    Ok(())
}

#[sinex_test]
async fn nodes_drain_publishes_command() -> TestResult<()> {
    let harness = NatsHarness::start().await?;

    let params = json!({
        "node_id": "test-node-123",
        "reason": "maintenance",
    });

    let result = handle_nodes_drain(&harness.client, &harness.env, params, &admin_auth()).await?;
    assert_eq!(result["status"], "drain_requested");
    assert_eq!(result["node_id"], "test-node-123");

    Ok(())
}

#[sinex_test]
async fn nodes_resume_publishes_command() -> TestResult<()> {
    let harness = NatsHarness::start().await?;

    let params = json!({
        "node_id": "test-node-456",
    });

    let result = handle_nodes_resume(&harness.client, &harness.env, params, &admin_auth()).await?;
    assert_eq!(result["status"], "resume_requested");
    assert_eq!(result["node_id"], "test-node-456");

    Ok(())
}

#[sinex_test]
async fn nodes_set_horizon_validates_timestamp() -> TestResult<()> {
    let harness = NatsHarness::start().await?;

    let invalid_params = json!({
        "node_id": "test-node-789",
        "horizon": "not-a-timestamp",
    });

    let err =
        handle_nodes_set_horizon(&harness.client, &harness.env, invalid_params, &admin_auth())
            .await
            .expect_err("invalid horizon should fail");
    assert!(err.to_string().contains("Serialization"));

    let valid_params = json!({
        "node_id": "test-node-789",
        "horizon": "2024-01-15T10:00:00Z",
    });

    let result =
        handle_nodes_set_horizon(&harness.client, &harness.env, valid_params, &admin_auth())
            .await?;
    assert_eq!(result["status"], "horizon_update_requested");

    Ok(())
}

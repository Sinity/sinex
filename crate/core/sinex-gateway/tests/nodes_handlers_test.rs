mod common;

use common::{NatsHarness, admin_auth};
use futures::StreamExt;
use serde_json::json;
use sinex_gateway::handlers::{
    handle_nodes_drain, handle_nodes_list, handle_nodes_resume, handle_nodes_set_horizon,
};
use sinex_primitives::nats::create_or_open_kv_store;
use xtask::sandbox::prelude::*;

async fn expect_single_control_message(
    sub: &mut async_nats::Subscriber,
    expected_subject: &str,
) -> TestResult<serde_json::Value> {
    let msg = tokio::time::timeout(std::time::Duration::from_secs(5), sub.next())
        .await
        .map_err(|_| color_eyre::eyre::eyre!("timed out waiting for {expected_subject}"))?
        .ok_or_else(|| color_eyre::eyre::eyre!("subscription closed for {expected_subject}"))?;

    assert_eq!(msg.subject.to_string(), expected_subject);
    let payload: serde_json::Value = serde_json::from_slice(&msg.payload)?;

    let extra = tokio::time::timeout(std::time::Duration::from_millis(150), sub.next()).await;
    assert!(
        extra.is_err(),
        "unexpected extra control publish observed on {expected_subject}"
    );

    Ok(payload)
}

#[sinex_test]
async fn nodes_list_returns_empty_when_no_bucket(ctx: TestContext) -> TestResult<()> {
    let harness = NatsHarness::start(ctx).await?;

    let result = handle_nodes_list(&harness.client, &harness.env, json!({})).await?;
    assert_eq!(result["nodes"].as_array().map_or(0, std::vec::Vec::len), 0);

    Ok(())
}

#[sinex_test]
async fn nodes_drain_publishes_command(ctx: TestContext) -> TestResult<()> {
    let harness = NatsHarness::start(ctx).await?;
    let subject = harness
        .env
        .nats_subject("sinex.control.nodes.test-node-123.drain");
    let mut sub = harness.client.subscribe(subject.clone()).await?;

    let params = json!({
        "node_id": "test-node-123",
        "reason": "maintenance",
    });

    let result = handle_nodes_drain(&harness.client, &harness.env, params, &admin_auth()).await?;
    assert_eq!(result["status"], "pending");
    assert_eq!(result["node_id"], "test-node-123");
    let payload = expect_single_control_message(&mut sub, &subject).await?;
    assert_eq!(payload["action"], "drain");
    assert_eq!(payload["node_id"], "test-node-123");
    assert_eq!(payload["reason"], "maintenance");
    assert!(payload["timestamp"].as_str().is_some());

    Ok(())
}

#[sinex_test]
async fn nodes_resume_publishes_command(ctx: TestContext) -> TestResult<()> {
    let harness = NatsHarness::start(ctx).await?;
    let subject = harness
        .env
        .nats_subject("sinex.control.nodes.test-node-456.resume");
    let mut sub = harness.client.subscribe(subject.clone()).await?;

    let params = json!({
        "node_id": "test-node-456",
    });

    let result = handle_nodes_resume(&harness.client, &harness.env, params, &admin_auth()).await?;
    assert_eq!(result["status"], "pending");
    assert_eq!(result["node_id"], "test-node-456");
    let payload = expect_single_control_message(&mut sub, &subject).await?;
    assert_eq!(payload["action"], "resume");
    assert_eq!(payload["node_id"], "test-node-456");
    assert!(payload.get("reason").is_none());
    assert!(payload["timestamp"].as_str().is_some());

    Ok(())
}

#[sinex_test]
async fn nodes_set_horizon_validates_timestamp(ctx: TestContext) -> TestResult<()> {
    let harness = NatsHarness::start(ctx).await?;
    let subject = harness
        .env
        .nats_subject("sinex.control.nodes.test-node-789.set-horizon");
    let mut sub = harness.client.subscribe(subject.clone()).await?;

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
    assert_eq!(result["status"], "pending");
    assert_eq!(result["node_id"], "test-node-789");
    assert_eq!(result["horizon"], "2024-01-15T10:00:00Z");
    let payload = expect_single_control_message(&mut sub, &subject).await?;
    assert_eq!(payload["action"], "set_horizon");
    assert_eq!(payload["node_id"], "test-node-789");
    assert_eq!(payload["horizon"], "2024-01-15T10:00:00Z");
    assert!(payload["timestamp"].as_str().is_some());

    Ok(())
}

#[sinex_test]
async fn nodes_list_surfaces_invalid_state_json(ctx: TestContext) -> TestResult<()> {
    let harness = NatsHarness::start(ctx).await?;
    let js = async_nats::jetstream::new(harness.client.clone());
    let bucket_name = harness.env.nats_kv_bucket_name("sinex_node_state");
    let kv = create_or_open_kv_store(
        &js,
        async_nats::jetstream::kv::Config {
            bucket: bucket_name,
            ..Default::default()
        },
    )
    .await?;
    kv.put("broken-node", br#"{ definitely not valid json"#.as_slice().into())
        .await?;

    let error = handle_nodes_list(&harness.client, &harness.env, json!({}))
        .await
        .expect_err("invalid node state should surface");
    assert!(error.to_string().contains("Node state is not valid JSON"));
    assert!(error.to_string().contains("broken-node"));
    Ok(())
}

#[sinex_test]
async fn nodes_list_surfaces_bucket_open_failures(ctx: TestContext) -> TestResult<()> {
    let harness = NatsHarness::start(ctx).await?;
    harness.nats_handle()?.shutdown().await?;

    let error = handle_nodes_list(&harness.client, &harness.env, json!({}))
        .await
        .expect_err("closed JetStream should surface instead of looking empty");
    assert!(error.to_string().contains("Failed to open node state bucket"));
    Ok(())
}

mod common;

use common::{NatsHarness, admin_auth, ensure_events_stream};
use serde_json::json;
use sinex_gateway::handlers::{handle_shadow_create, handle_shadow_delete, handle_shadow_list};
use sinex_primitives::rpc::shadow::{
    ShadowCreateResponse, ShadowDeleteResponse, ShadowListResponse,
};
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn shadow_create_requires_dev_prefix(ctx: TestContext) -> TestResult<()> {
    let harness = NatsHarness::start(ctx).await?;

    let err = handle_shadow_create(
        &harness.client,
        &harness.env,
        json!({
            "consumer_name": "production-consumer",
            "from_beginning": true
        }),
    )
    .await
    .expect_err("consumer names without dev- prefix must fail");

    assert!(err.to_string().contains("dev-"));
    Ok(())
}

#[sinex_test]
async fn shadow_create_and_list(ctx: TestContext) -> TestResult<()> {
    let harness = NatsHarness::start(ctx).await?;
    let stream_name = harness.env.nats_stream_name("EVENTS");
    ensure_events_stream(&harness.client, &harness.env).await?;

    let result = handle_shadow_create(
        &harness.client,
        &harness.env,
        json!({
            "consumer_name": "dev-test-123",
            "subject_filter": harness.env.nats_subject("events.>"),
            "from_beginning": true
        }),
    )
    .await?;

    let create_response: ShadowCreateResponse = serde_json::from_value(result)?;
    assert_eq!(create_response.consumer.consumer_name, "dev-test-123");
    assert_eq!(create_response.consumer.stream_name, stream_name);

    let list_result = handle_shadow_list(&harness.client, &harness.env, json!({})).await?;
    let list_response: ShadowListResponse = serde_json::from_value(list_result)?;
    assert_eq!(list_response.consumers.len(), 1);

    let delete_result = handle_shadow_delete(
        &harness.client,
        &harness.env,
        json!({
            "consumer_name": "dev-test-123"
        }),
        &admin_auth(),
    )
    .await?;

    let delete_response: ShadowDeleteResponse = serde_json::from_value(delete_result)?;
    assert_eq!(delete_response.status, "success");

    let list_result = handle_shadow_list(&harness.client, &harness.env, json!({})).await?;
    let list_response: ShadowListResponse = serde_json::from_value(list_result)?;
    assert!(list_response.consumers.is_empty());

    Ok(())
}

#[sinex_test]
async fn shadow_create_requires_subject_filter(ctx: TestContext) -> TestResult<()> {
    let harness = NatsHarness::start(ctx).await?;
    ensure_events_stream(&harness.client, &harness.env).await?;

    let err = handle_shadow_create(
        &harness.client,
        &harness.env,
        json!({
            "consumer_name": "dev-test-456",
            "from_beginning": true
        }),
    )
    .await
    .expect_err("missing subject_filter must fail");

    assert!(err.to_string().contains("subject_filter is required"));
    Ok(())
}

#[sinex_test]
async fn shadow_delete_requires_dev_prefix(ctx: TestContext) -> TestResult<()> {
    let harness = NatsHarness::start(ctx).await?;

    let err = handle_shadow_delete(
        &harness.client,
        &harness.env,
        json!({
            "consumer_name": "production-consumer"
        }),
        &admin_auth(),
    )
    .await
    .expect_err("delete without dev- prefix must fail");

    assert!(err.to_string().contains("dev-"));
    Ok(())
}

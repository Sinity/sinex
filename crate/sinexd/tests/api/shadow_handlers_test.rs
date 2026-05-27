mod common;

use common::{NatsHarness, admin_auth, ensure_events_stream};
use sinexd::api::handlers::{handle_shadow_create, handle_shadow_delete, handle_shadow_list};
use sinex_primitives::error::ErrorClass;
use sinex_primitives::rpc::shadow::{ShadowCreateRequest, ShadowDeleteRequest, ShadowListRequest};
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn shadow_create_requires_dev_prefix(ctx: TestContext) -> TestResult<()> {
    let harness = NatsHarness::start(ctx).await?;

    let err = handle_shadow_create(
        &harness.services,
        ShadowCreateRequest {
            consumer_name: "production-consumer".to_string(),
            subject_filter: None,
            from_beginning: true,
            from_sequence: None,
        },
    )
    .await
    .expect_err("consumer names without dev- prefix must fail");

    assert!(err.to_string().contains("dev-"));
    assert_eq!(err.error_class(), ErrorClass::DataError);
    Ok(())
}

#[sinex_test]
async fn shadow_create_and_list(ctx: TestContext) -> TestResult<()> {
    let harness = NatsHarness::start(ctx).await?;
    let stream_name = harness.env.nats_stream_name("EVENTS");
    ensure_events_stream(&harness.client, &harness.env).await?;

    let result = handle_shadow_create(
        &harness.services,
        ShadowCreateRequest {
            consumer_name: "dev-test-123".to_string(),
            subject_filter: Some(harness.env.nats_subject("events.>")),
            from_beginning: true,
            from_sequence: None,
        },
    )
    .await?;

    assert_eq!(result.consumer.consumer_name, "dev-test-123");
    assert_eq!(result.consumer.stream_name, stream_name);

    let list_result = handle_shadow_list(&harness.services, ShadowListRequest::default()).await?;
    assert_eq!(list_result.consumers.len(), 1);

    let delete_result = handle_shadow_delete(
        &harness.services,
        ShadowDeleteRequest {
            consumer_name: "dev-test-123".to_string(),
        },
        &admin_auth(),
    )
    .await?;

    assert_eq!(delete_result.status, "success");

    let list_result = handle_shadow_list(&harness.services, ShadowListRequest::default()).await?;
    assert!(list_result.consumers.is_empty());

    Ok(())
}

#[sinex_test]
async fn shadow_create_requires_subject_filter(ctx: TestContext) -> TestResult<()> {
    let harness = NatsHarness::start(ctx).await?;
    ensure_events_stream(&harness.client, &harness.env).await?;

    let err = handle_shadow_create(
        &harness.services,
        ShadowCreateRequest {
            consumer_name: "dev-test-456".to_string(),
            subject_filter: None,
            from_beginning: true,
            from_sequence: None,
        },
    )
    .await
    .expect_err("missing subject_filter must fail");

    assert!(err.to_string().contains("subject_filter is required"));
    assert_eq!(err.error_class(), ErrorClass::DataError);
    Ok(())
}

#[sinex_test]
async fn shadow_delete_requires_dev_prefix(ctx: TestContext) -> TestResult<()> {
    let harness = NatsHarness::start(ctx).await?;

    let err = handle_shadow_delete(
        &harness.services,
        ShadowDeleteRequest {
            consumer_name: "production-consumer".to_string(),
        },
        &admin_auth(),
    )
    .await
    .expect_err("delete without dev- prefix must fail");

    assert!(err.to_string().contains("dev-"));
    assert_eq!(err.error_class(), ErrorClass::DataError);
    Ok(())
}

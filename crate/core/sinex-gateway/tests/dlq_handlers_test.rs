//! Comprehensive tests for DLQ (Dead Letter Queue) handlers
//!
//! Tests DLQ statistics and purge operations.
//! Note: Peek tests are excluded because `handle_dlq_peek` waits indefinitely
//! for messages from the consumer stream without timeout.

mod common;

use async_nats::jetstream;
use common::{NatsHarness, admin_auth, ensure_dlq_stream};
use serde_json::json;
use sinex_gateway::handlers::dlq::{handle_dlq_list, handle_dlq_purge, handle_dlq_requeue};
use sinex_primitives::error::SinexError;
use sinex_primitives::rpc::dlq::{DlqListResponse, DlqPurgeResponse};
use xtask::sandbox::prelude::*;
use xtask::sandbox::timing::{Timeouts, WaitHelpers};

async fn publish_dlq_message(
    client: &async_nats::Client,
    env: &sinex_primitives::environment::SinexEnvironment,
    event_id: &str,
    payload: &str,
    retry_count: u32,
) -> color_eyre::Result<()> {
    let mut headers = async_nats::HeaderMap::new();
    headers.insert("Retry-Count", retry_count.to_string().as_str());
    headers.insert("Original-Subject", "events.raw.test-source");
    headers.insert("Event-Id", event_id);

    let subject = env.nats_subject(&format!("events.dlq.{event_id}"));
    client
        .publish_with_headers(subject, headers, payload.to_owned().into())
        .await?;
    client.flush().await?;

    Ok(())
}

/// Wait for the DLQ JetStream stream to contain at least `expected` messages.
async fn wait_for_dlq_stream_messages(
    client: &async_nats::Client,
    env: &sinex_primitives::environment::SinexEnvironment,
    expected: u64,
) -> TestResult<()> {
    let js = jetstream::new(client.clone());
    let stream_name = env.nats_stream_name("EVENTS_DLQ");
    WaitHelpers::wait_for_condition(
        || {
            let js = js.clone();
            let stream_name = stream_name.clone();
            async move {
                let mut stream = js
                    .get_stream(&stream_name)
                    .await
                    .map_err(|e| SinexError::network(e.to_string()))?;
                let info = stream
                    .info()
                    .await
                    .map_err(|e| SinexError::network(e.to_string()))?;
                Ok::<bool, SinexError>(info.state.messages >= expected)
            }
        },
        Timeouts::QUICK,
    )
    .await
}

#[sinex_test]
async fn dlq_list_returns_empty_for_new_stream() -> TestResult<()> {
    let harness = NatsHarness::start().await?;

    ensure_dlq_stream(
        &harness.client,
        &harness.env,
        jetstream::stream::StorageType::Memory,
    )
    .await?;

    let result = handle_dlq_list(&harness.client, &harness.env, json!({})).await?;
    let response: DlqListResponse = serde_json::from_value(result)?;

    assert_eq!(response.total_messages, 0);
    assert_eq!(response.total_bytes, 0);

    Ok(())
}

#[sinex_test]
async fn dlq_list_counts_messages_correctly() -> TestResult<()> {
    let harness = NatsHarness::start().await?;

    ensure_dlq_stream(
        &harness.client,
        &harness.env,
        jetstream::stream::StorageType::Memory,
    )
    .await?;

    // Publish 3 messages
    for i in 0..3 {
        publish_dlq_message(
            &harness.client,
            &harness.env,
            &format!("event-{i}"),
            r#"{"test": true}"#,
            1,
        )
        .await?;
    }

    // Wait for JetStream to acknowledge all messages
    wait_for_dlq_stream_messages(&harness.client, &harness.env, 3).await?;

    let result = handle_dlq_list(&harness.client, &harness.env, json!({})).await?;
    let response: DlqListResponse = serde_json::from_value(result)?;

    assert_eq!(response.total_messages, 3);
    assert!(response.total_bytes > 0);

    Ok(())
}

#[sinex_test]
async fn dlq_list_shows_sequence_info() -> TestResult<()> {
    let harness = NatsHarness::start().await?;

    ensure_dlq_stream(
        &harness.client,
        &harness.env,
        jetstream::stream::StorageType::Memory,
    )
    .await?;

    // Publish messages
    for i in 0..3 {
        publish_dlq_message(
            &harness.client,
            &harness.env,
            &format!("event-{i}"),
            r#"{"test": true}"#,
            1,
        )
        .await?;
    }

    // Wait for JetStream to acknowledge all messages
    wait_for_dlq_stream_messages(&harness.client, &harness.env, 3).await?;

    let result = handle_dlq_list(&harness.client, &harness.env, json!({})).await?;
    let response: DlqListResponse = serde_json::from_value(result)?;

    // Should have valid sequence numbers
    assert!(response.first_seq > 0);
    assert!(response.last_seq >= response.first_seq);

    Ok(())
}

#[sinex_test]
async fn dlq_purge_requires_confirm_parameter() -> TestResult<()> {
    let harness = NatsHarness::start().await?;

    ensure_dlq_stream(
        &harness.client,
        &harness.env,
        jetstream::stream::StorageType::Memory,
    )
    .await?;

    // Try purge without confirm
    let err = handle_dlq_purge(
        &harness.client,
        &harness.env,
        json!({"confirm": false}),
        &admin_auth(),
    )
    .await
    .unwrap_err();

    assert!(err.to_string().contains("confirm: true"));

    Ok(())
}

#[sinex_test]
async fn dlq_purge_clears_all_messages() -> TestResult<()> {
    let harness = NatsHarness::start().await?;

    ensure_dlq_stream(
        &harness.client,
        &harness.env,
        jetstream::stream::StorageType::Memory,
    )
    .await?;

    // Publish some messages
    for i in 0..5 {
        publish_dlq_message(
            &harness.client,
            &harness.env,
            &format!("event-{i}"),
            r#"{"test": true}"#,
            1,
        )
        .await?;
    }

    // Wait for JetStream to acknowledge all messages
    wait_for_dlq_stream_messages(&harness.client, &harness.env, 5).await?;

    // Verify messages exist
    let before: DlqListResponse =
        serde_json::from_value(handle_dlq_list(&harness.client, &harness.env, json!({})).await?)?;
    assert_eq!(before.total_messages, 5);

    // Purge with confirmation
    let result = handle_dlq_purge(
        &harness.client,
        &harness.env,
        json!({"confirm": true}),
        &admin_auth(),
    )
    .await?;
    let response: DlqPurgeResponse = serde_json::from_value(result)?;

    assert_eq!(response.purged_count, 5);
    assert_eq!(response.status, "success");

    // Verify stream is empty
    let after: DlqListResponse =
        serde_json::from_value(handle_dlq_list(&harness.client, &harness.env, json!({})).await?)?;
    assert_eq!(after.total_messages, 0);

    Ok(())
}

#[sinex_test]
async fn dlq_purge_handles_empty_stream() -> TestResult<()> {
    let harness = NatsHarness::start().await?;

    ensure_dlq_stream(
        &harness.client,
        &harness.env,
        jetstream::stream::StorageType::Memory,
    )
    .await?;

    // Purge empty stream should succeed
    let result = handle_dlq_purge(
        &harness.client,
        &harness.env,
        json!({"confirm": true}),
        &admin_auth(),
    )
    .await?;
    let response: DlqPurgeResponse = serde_json::from_value(result)?;

    assert_eq!(response.purged_count, 0);
    assert_eq!(response.status, "success");

    Ok(())
}

#[sinex_test]
async fn dlq_purge_requires_missing_confirm_field() -> TestResult<()> {
    let harness = NatsHarness::start().await?;

    ensure_dlq_stream(
        &harness.client,
        &harness.env,
        jetstream::stream::StorageType::Memory,
    )
    .await?;

    // Try purge without confirm field at all - should fail validation
    let err = handle_dlq_purge(&harness.client, &harness.env, json!({}), &admin_auth())
        .await
        .unwrap_err();

    assert!(
        err.to_string().to_lowercase().contains("invalid") || err.to_string().contains("missing")
    );

    Ok(())
}

#[sinex_test]
async fn dlq_list_after_publish_and_purge_cycle() -> TestResult<()> {
    let harness = NatsHarness::start().await?;

    ensure_dlq_stream(
        &harness.client,
        &harness.env,
        jetstream::stream::StorageType::Memory,
    )
    .await?;

    // First cycle
    for i in 0..3 {
        publish_dlq_message(
            &harness.client,
            &harness.env,
            &format!("cycle1-{i}"),
            r#"{"cycle": 1}"#,
            1,
        )
        .await?;
    }
    wait_for_dlq_stream_messages(&harness.client, &harness.env, 3).await?;

    let mid1: DlqListResponse =
        serde_json::from_value(handle_dlq_list(&harness.client, &harness.env, json!({})).await?)?;
    assert_eq!(mid1.total_messages, 3);

    // Purge
    handle_dlq_purge(
        &harness.client,
        &harness.env,
        json!({"confirm": true}),
        &admin_auth(),
    )
    .await?;

    // Second cycle — after purge, stream was emptied, so wait for 2 new messages.
    for i in 0..2 {
        publish_dlq_message(
            &harness.client,
            &harness.env,
            &format!("cycle2-{i}"),
            r#"{"cycle": 2}"#,
            1,
        )
        .await?;
    }
    wait_for_dlq_stream_messages(&harness.client, &harness.env, 2).await?;

    let mid2: DlqListResponse =
        serde_json::from_value(handle_dlq_list(&harness.client, &harness.env, json!({})).await?)?;
    assert_eq!(mid2.total_messages, 2);

    Ok(())
}

#[sinex_test]
async fn dlq_requeue_requires_selector_params() -> TestResult<()> {
    let harness = NatsHarness::start().await?;
    let err = handle_dlq_requeue(&harness.client, &harness.env, json!({}), &admin_auth())
        .await
        .expect_err("requeue without selector should fail");
    assert!(err.to_string().contains("Must specify either"));
    Ok(())
}

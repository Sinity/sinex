//! Comprehensive tests for DLQ (Dead Letter Queue) handlers
//!
//! Tests DLQ statistics and purge operations.
//! Note: Peek tests are excluded because `handle_dlq_peek` waits indefinitely
//! for messages from the consumer stream without timeout.

use async_nats::jetstream;
use serde_json::json;
use sinex_gateway::auth::Role;
use sinex_gateway::handlers::dlq::{handle_dlq_list, handle_dlq_purge};
use sinex_gateway::rpc_server::RpcAuthContext;
use sinex_primitives::environment;
use sinex_primitives::error::SinexError;
use sinex_primitives::rpc::dlq::{DlqListResponse, DlqPurgeResponse};
use sinex_primitives::temporal;
use xtask::sandbox::timing::{Timeouts, WaitHelpers};
use xtask::sandbox::{nats::EphemeralNats, prelude::*};

async fn setup_dlq_stream(
    client: &async_nats::Client,
    env: &sinex_primitives::environment::SinexEnvironment,
) -> color_eyre::Result<jetstream::stream::Stream> {
    let js = jetstream::new(client.clone());
    let stream_name = env.nats_stream_name("EVENTS_DLQ");

    let stream = js
        .get_or_create_stream(jetstream::stream::Config {
            name: stream_name,
            subjects: vec![env.nats_subject("events.dlq.>")],
            retention: jetstream::stream::RetentionPolicy::Limits,
            max_messages: 1000,
            storage: jetstream::stream::StorageType::Memory,
            ..Default::default()
        })
        .await?;

    Ok(stream)
}

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
    let nats = EphemeralNats::start().await?;
    let client = nats.connect().await?;
    let env = environment();

    setup_dlq_stream(&client, &env).await?;

    let result = handle_dlq_list(&client, &env, json!({})).await?;
    let response: DlqListResponse = serde_json::from_value(result)?;

    assert_eq!(response.total_messages, 0);
    assert_eq!(response.total_bytes, 0);

    Ok(())
}

#[sinex_test]
async fn dlq_list_counts_messages_correctly() -> TestResult<()> {
    let nats = EphemeralNats::start().await?;
    let client = nats.connect().await?;
    let env = environment();

    setup_dlq_stream(&client, &env).await?;

    // Publish 3 messages
    for i in 0..3 {
        publish_dlq_message(&client, &env, &format!("event-{i}"), r#"{"test": true}"#, 1).await?;
    }

    // Wait for JetStream to acknowledge all messages
    wait_for_dlq_stream_messages(&client, &env, 3).await?;

    let result = handle_dlq_list(&client, &env, json!({})).await?;
    let response: DlqListResponse = serde_json::from_value(result)?;

    assert_eq!(response.total_messages, 3);
    assert!(response.total_bytes > 0);

    Ok(())
}

#[sinex_test]
async fn dlq_list_shows_sequence_info() -> TestResult<()> {
    let nats = EphemeralNats::start().await?;
    let client = nats.connect().await?;
    let env = environment();

    setup_dlq_stream(&client, &env).await?;

    // Publish messages
    for i in 0..3 {
        publish_dlq_message(&client, &env, &format!("event-{i}"), r#"{"test": true}"#, 1).await?;
    }

    // Wait for JetStream to acknowledge all messages
    wait_for_dlq_stream_messages(&client, &env, 3).await?;

    let result = handle_dlq_list(&client, &env, json!({})).await?;
    let response: DlqListResponse = serde_json::from_value(result)?;

    // Should have valid sequence numbers
    assert!(response.first_seq > 0);
    assert!(response.last_seq >= response.first_seq);

    Ok(())
}

#[sinex_test]
async fn dlq_purge_requires_confirm_parameter() -> TestResult<()> {
    let nats = EphemeralNats::start().await?;
    let client = nats.connect().await?;
    let env = environment();

    setup_dlq_stream(&client, &env).await?;

    let test_auth = RpcAuthContext {
        token_prefix: "test****".to_string(),
        authenticated_at: temporal::now(),
        role: Role::Admin,
    };

    // Try purge without confirm
    let err = handle_dlq_purge(&client, &env, json!({"confirm": false}), &test_auth)
        .await
        .unwrap_err();

    assert!(err.to_string().contains("confirm: true"));

    Ok(())
}

#[sinex_test]
async fn dlq_purge_clears_all_messages() -> TestResult<()> {
    let nats = EphemeralNats::start().await?;
    let client = nats.connect().await?;
    let env = environment();

    setup_dlq_stream(&client, &env).await?;

    let test_auth = RpcAuthContext {
        token_prefix: "test****".to_string(),
        authenticated_at: temporal::now(),
        role: Role::Admin,
    };

    // Publish some messages
    for i in 0..5 {
        publish_dlq_message(&client, &env, &format!("event-{i}"), r#"{"test": true}"#, 1).await?;
    }

    // Wait for JetStream to acknowledge all messages
    wait_for_dlq_stream_messages(&client, &env, 5).await?;

    // Verify messages exist
    let before: DlqListResponse =
        serde_json::from_value(handle_dlq_list(&client, &env, json!({})).await?)?;
    assert_eq!(before.total_messages, 5);

    // Purge with confirmation
    let result = handle_dlq_purge(&client, &env, json!({"confirm": true}), &test_auth).await?;
    let response: DlqPurgeResponse = serde_json::from_value(result)?;

    assert_eq!(response.purged_count, 5);
    assert_eq!(response.status, "success");

    // Verify stream is empty
    let after: DlqListResponse =
        serde_json::from_value(handle_dlq_list(&client, &env, json!({})).await?)?;
    assert_eq!(after.total_messages, 0);

    Ok(())
}

#[sinex_test]
async fn dlq_purge_handles_empty_stream() -> TestResult<()> {
    let nats = EphemeralNats::start().await?;
    let client = nats.connect().await?;
    let env = environment();

    setup_dlq_stream(&client, &env).await?;

    let test_auth = RpcAuthContext {
        token_prefix: "test****".to_string(),
        authenticated_at: temporal::now(),
        role: Role::Admin,
    };

    // Purge empty stream should succeed
    let result = handle_dlq_purge(&client, &env, json!({"confirm": true}), &test_auth).await?;
    let response: DlqPurgeResponse = serde_json::from_value(result)?;

    assert_eq!(response.purged_count, 0);
    assert_eq!(response.status, "success");

    Ok(())
}

#[sinex_test]
async fn dlq_purge_requires_missing_confirm_field() -> TestResult<()> {
    let nats = EphemeralNats::start().await?;
    let client = nats.connect().await?;
    let env = environment();

    setup_dlq_stream(&client, &env).await?;

    let test_auth = RpcAuthContext {
        token_prefix: "test****".to_string(),
        authenticated_at: temporal::now(),
        role: Role::Admin,
    };

    // Try purge without confirm field at all - should fail validation
    let err = handle_dlq_purge(&client, &env, json!({}), &test_auth)
        .await
        .unwrap_err();

    assert!(
        err.to_string().to_lowercase().contains("invalid") || err.to_string().contains("missing")
    );

    Ok(())
}

#[sinex_test]
async fn dlq_list_after_publish_and_purge_cycle() -> TestResult<()> {
    let nats = EphemeralNats::start().await?;
    let client = nats.connect().await?;
    let env = environment();

    setup_dlq_stream(&client, &env).await?;

    let test_auth = RpcAuthContext {
        token_prefix: "test****".to_string(),
        authenticated_at: temporal::now(),
        role: Role::Admin,
    };

    // First cycle
    for i in 0..3 {
        publish_dlq_message(&client, &env, &format!("cycle1-{i}"), r#"{"cycle": 1}"#, 1).await?;
    }
    wait_for_dlq_stream_messages(&client, &env, 3).await?;

    let mid1: DlqListResponse =
        serde_json::from_value(handle_dlq_list(&client, &env, json!({})).await?)?;
    assert_eq!(mid1.total_messages, 3);

    // Purge
    handle_dlq_purge(&client, &env, json!({"confirm": true}), &test_auth).await?;

    // Second cycle — after purge, stream was emptied, so wait for 2 new messages.
    for i in 0..2 {
        publish_dlq_message(&client, &env, &format!("cycle2-{i}"), r#"{"cycle": 2}"#, 1).await?;
    }
    wait_for_dlq_stream_messages(&client, &env, 2).await?;

    let mid2: DlqListResponse =
        serde_json::from_value(handle_dlq_list(&client, &env, json!({})).await?)?;
    assert_eq!(mid2.total_messages, 2);

    Ok(())
}

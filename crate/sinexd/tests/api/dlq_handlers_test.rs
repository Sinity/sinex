//! Comprehensive tests for raw-ingest DLQ handlers
//!
//! Tests DLQ statistics and purge operations.

mod common;

use async_nats::jetstream;
use common::{NatsHarness, admin_auth, ensure_dlq_stream};
use futures::StreamExt;
use serde_json::json;
use sinex_db::DbPoolExt;
use sinex_primitives::Timestamp;
use sinex_primitives::domain::OperationStatus;
use sinex_primitives::error::{ErrorClass, SinexError};
use sinex_primitives::rpc::dlq::{
    DlqListRequest, DlqPeekRequest, DlqPurgeRequest, DlqRequeueRequest,
};
use sinexd::api::handlers::dlq::{
    handle_dlq_list, handle_dlq_peek, handle_dlq_purge, handle_dlq_requeue,
};
use std::time::Duration;
use xtask::sandbox::prelude::*;
use xtask::sandbox::timing::{Timeouts, WaitHelpers};

async fn publish_dlq_message(
    client: &async_nats::Client,
    env: &sinex_primitives::environment::SinexEnvironment,
    event_id: &str,
    payload: &str,
    retry_count: u32,
) -> color_eyre::Result<()> {
    let original_subject = env.nats_subject("events.raw.test-source");
    let mut headers = async_nats::HeaderMap::new();
    headers.insert("Retry-Count", retry_count.to_string().as_str());
    headers.insert("Original-Subject", original_subject.as_str());
    headers.insert("Event-Id", event_id);

    let subject = env.nats_subject(&format!("events.dlq.test-component.{event_id}"));
    client
        .publish_with_headers(subject, headers, payload.to_owned().into())
        .await?;
    client.flush().await?;

    Ok(())
}

/// Wait for the DLQ `JetStream` stream to contain at least `expected` messages.
async fn wait_for_dlq_stream_messages(
    client: &async_nats::Client,
    env: &sinex_primitives::environment::SinexEnvironment,
    expected: u64,
) -> TestResult<()> {
    let js = jetstream::new(client.clone());
    let stream_name = env.nats_stream_name("SINEX_RAW_EVENTS_DLQ");
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
async fn dlq_list_returns_empty_for_new_stream(ctx: TestContext) -> TestResult<()> {
    let harness = NatsHarness::start(ctx).await?;

    ensure_dlq_stream(
        &harness.client,
        &harness.env,
        jetstream::stream::StorageType::Memory,
    )
    .await?;

    let response = handle_dlq_list(&harness.services, DlqListRequest {}).await?;

    assert_eq!(response.total_messages, 0);
    assert_eq!(response.total_bytes, 0);
    assert_eq!(
        response.pressure_level,
        sinex_primitives::RuntimePressureLevel::Nominal
    );
    assert_eq!(
        response.resource_pressure.pressure_level,
        sinex_primitives::RuntimePressureLevel::Nominal
    );
    assert_eq!(
        response.resource_pressure.runtime_action,
        sinex_primitives::RuntimePressureAction::Admit
    );
    assert_eq!(response.resource_pressure.pending_messages, 0);
    assert_eq!(response.resource_pressure.pending_bytes, 0);
    assert_eq!(response.pending_sequence_span, 0);
    assert_eq!(response.recommended_action, "none");
    assert_eq!(response.action_reason, "raw-ingest DLQ is empty");

    Ok(())
}

#[sinex_test]
async fn dlq_list_counts_messages_correctly(ctx: TestContext) -> TestResult<()> {
    let harness = NatsHarness::start(ctx).await?;

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

    let response = handle_dlq_list(&harness.services, DlqListRequest {}).await?;

    assert_eq!(response.total_messages, 3);
    assert!(response.total_bytes > 0);
    assert_eq!(
        response.pressure_level,
        sinex_primitives::RuntimePressureLevel::Warning
    );
    assert_eq!(
        response.resource_pressure.pressure_level,
        sinex_primitives::RuntimePressureLevel::Warning
    );
    assert_eq!(
        response.resource_pressure.runtime_action,
        sinex_primitives::RuntimePressureAction::Inspect
    );
    assert_eq!(response.resource_pressure.pending_messages, 3);
    assert_eq!(
        response.resource_pressure.pending_bytes,
        response.total_bytes
    );
    assert_eq!(response.recommended_action, "ops dlq peek");
    assert!(response.action_reason.contains("paced requeue or purge"));

    Ok(())
}

#[sinex_test]
async fn dlq_list_shows_sequence_info(ctx: TestContext) -> TestResult<()> {
    let harness = NatsHarness::start(ctx).await?;

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

    let response = handle_dlq_list(&harness.services, DlqListRequest {}).await?;

    // Should have valid sequence numbers
    assert!(response.first_seq > 0);
    assert!(response.last_seq >= response.first_seq);
    assert_eq!(
        response.pending_sequence_span,
        response.last_seq - response.first_seq + 1
    );

    Ok(())
}

#[sinex_test]
async fn dlq_purge_requires_confirm_parameter(ctx: TestContext) -> TestResult<()> {
    let harness = NatsHarness::start(ctx).await?;

    ensure_dlq_stream(
        &harness.client,
        &harness.env,
        jetstream::stream::StorageType::Memory,
    )
    .await?;

    // Try purge without confirm
    let err = handle_dlq_purge(
        &harness.services,
        DlqPurgeRequest {
            confirm: false,
            start_sequence: None,
            end_sequence: None,
        },
        &admin_auth(),
    )
    .await
    .unwrap_err();

    assert_eq!(err.error_class(), ErrorClass::DataError);
    assert!(err.to_string().contains("confirm: true"));

    Ok(())
}

#[sinex_test]
async fn dlq_purge_clears_all_messages(ctx: TestContext) -> TestResult<()> {
    let harness = NatsHarness::start(ctx).await?;

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
    let before = handle_dlq_list(&harness.services, DlqListRequest {}).await?;
    assert_eq!(before.total_messages, 5);

    // Purge with confirmation
    let response = handle_dlq_purge(
        &harness.services,
        DlqPurgeRequest {
            confirm: true,
            start_sequence: None,
            end_sequence: None,
        },
        &admin_auth(),
    )
    .await?;

    assert_eq!(response.purged_count, 5);
    assert_eq!(response.status, "success");
    let operations = harness
        .services
        .pool()
        .state()
        .list_operations(Some("dlq.purge"), Some(OperationStatus::Success), 10)
        .await?;
    let operation = operations
        .first()
        .expect("purge should write a successful operation record");
    assert_eq!(operation.id.to_uuid().to_string(), response.operation_id);
    assert_eq!(
        operation
            .scope
            .as_ref()
            .and_then(|scope| scope["action"].as_str()),
        Some("purge")
    );
    assert_eq!(
        operation
            .scope
            .as_ref()
            .and_then(|scope| scope["messages_before"].as_u64()),
        Some(5)
    );

    // Verify stream is empty
    let after = handle_dlq_list(&harness.services, DlqListRequest {}).await?;
    assert_eq!(after.total_messages, 0);

    Ok(())
}

#[sinex_test]
async fn dlq_purge_handles_empty_stream(ctx: TestContext) -> TestResult<()> {
    let harness = NatsHarness::start(ctx).await?;

    ensure_dlq_stream(
        &harness.client,
        &harness.env,
        jetstream::stream::StorageType::Memory,
    )
    .await?;

    // Purge empty stream should succeed
    let response = handle_dlq_purge(
        &harness.services,
        DlqPurgeRequest {
            confirm: true,
            start_sequence: None,
            end_sequence: None,
        },
        &admin_auth(),
    )
    .await?;

    assert_eq!(response.purged_count, 0);
    assert_eq!(response.status, "success");
    let operations = harness
        .services
        .pool()
        .state()
        .list_operations(Some("dlq.purge"), Some(OperationStatus::Success), 10)
        .await?;
    let operation = operations
        .first()
        .expect("empty purge should still write an operation record");
    assert_eq!(operation.id.to_uuid().to_string(), response.operation_id);
    assert_eq!(
        operation
            .scope
            .as_ref()
            .and_then(|scope| scope["messages_before"].as_u64()),
        Some(0)
    );

    Ok(())
}

#[sinex_test]
async fn dlq_purge_deletes_only_selected_sequence_range(ctx: TestContext) -> TestResult<()> {
    let harness = NatsHarness::start(ctx).await?;

    ensure_dlq_stream(
        &harness.client,
        &harness.env,
        jetstream::stream::StorageType::Memory,
    )
    .await?;

    for i in 0..5 {
        publish_dlq_message(
            &harness.client,
            &harness.env,
            &format!("range-{i}"),
            r#"{"range": true}"#,
            1,
        )
        .await?;
    }
    wait_for_dlq_stream_messages(&harness.client, &harness.env, 5).await?;

    let response = handle_dlq_purge(
        &harness.services,
        DlqPurgeRequest {
            confirm: true,
            start_sequence: Some(2),
            end_sequence: Some(4),
        },
        &admin_auth(),
    )
    .await?;

    assert_eq!(response.status, "success");
    assert_eq!(response.purged_count, 3);

    let rerun = handle_dlq_purge(
        &harness.services,
        DlqPurgeRequest {
            confirm: true,
            start_sequence: Some(2),
            end_sequence: Some(4),
        },
        &admin_auth(),
    )
    .await?;
    assert_eq!(rerun.status, "success");
    assert_eq!(rerun.purged_count, 0);

    let after = handle_dlq_list(&harness.services, DlqListRequest {}).await?;
    assert_eq!(after.total_messages, 2);

    let js = jetstream::new(harness.client.clone());
    let stream_name = harness.env.nats_stream_name("SINEX_RAW_EVENTS_DLQ");
    let stream = js.get_stream(&stream_name).await?;
    assert!(
        stream.direct_get(1).await.is_ok(),
        "sequence 1 should be retained"
    );
    assert!(
        stream.direct_get(5).await.is_ok(),
        "sequence 5 should be retained"
    );
    for deleted in 2..=4 {
        let err = stream
            .direct_get(deleted)
            .await
            .expect_err("selected sequence should be deleted");
        assert!(
            matches!(
                err.kind(),
                async_nats::jetstream::stream::DirectGetErrorKind::NotFound
            ),
            "deleted sequence {deleted} should be absent"
        );
    }

    let operations = harness
        .services
        .pool()
        .state()
        .list_operations(Some("dlq.purge"), Some(OperationStatus::Success), 10)
        .await?;
    let operation = operations
        .first()
        .expect("range purge should write a successful operation record");
    assert_eq!(
        operation
            .scope
            .as_ref()
            .and_then(|scope| scope["selector"].as_str()),
        Some("sequence_range")
    );
    assert_eq!(
        operation
            .scope
            .as_ref()
            .and_then(|scope| scope["start_sequence"].as_u64()),
        Some(2)
    );
    assert_eq!(
        operation
            .scope
            .as_ref()
            .and_then(|scope| scope["end_sequence"].as_u64()),
        Some(4)
    );

    Ok(())
}

#[sinex_test]
async fn dlq_peek_start_sequence_skips_deleted_stream_holes(ctx: TestContext) -> TestResult<()> {
    let harness = NatsHarness::start(ctx).await?;

    ensure_dlq_stream(
        &harness.client,
        &harness.env,
        jetstream::stream::StorageType::Memory,
    )
    .await?;

    for i in 0..5 {
        publish_dlq_message(
            &harness.client,
            &harness.env,
            &format!("hole-{i}"),
            r#"{"hole": true}"#,
            1,
        )
        .await?;
    }
    wait_for_dlq_stream_messages(&harness.client, &harness.env, 5).await?;

    let response = handle_dlq_purge(
        &harness.services,
        DlqPurgeRequest {
            confirm: true,
            start_sequence: Some(4),
            end_sequence: Some(5),
        },
        &admin_auth(),
    )
    .await?;
    assert_eq!(response.purged_count, 2);

    let peek = handle_dlq_peek(
        &harness.services,
        DlqPeekRequest {
            limit: 10,
            payload_preview_chars: 200,
            start_sequence: Some(4),
        },
    )
    .await?;

    assert!(peek.messages.is_empty());
    assert!(peek.groups.is_empty());

    Ok(())
}

#[sinex_test]
async fn dlq_list_after_publish_and_purge_cycle(ctx: TestContext) -> TestResult<()> {
    let harness = NatsHarness::start(ctx).await?;

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

    let mid1 = handle_dlq_list(&harness.services, DlqListRequest {}).await?;
    assert_eq!(mid1.total_messages, 3);

    // Purge
    handle_dlq_purge(
        &harness.services,
        DlqPurgeRequest {
            confirm: true,
            start_sequence: None,
            end_sequence: None,
        },
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

    let mid2 = handle_dlq_list(&harness.services, DlqListRequest {}).await?;
    assert_eq!(mid2.total_messages, 2);

    Ok(())
}

#[sinex_test]
async fn dlq_requeue_requires_selector_params(ctx: TestContext) -> TestResult<()> {
    let harness = NatsHarness::start(ctx).await?;
    let err = handle_dlq_requeue(
        &harness.services,
        DlqRequeueRequest {
            event_id: None,
            start_sequence: None,
            end_sequence: None,
            all: false,
        },
        &admin_auth(),
    )
    .await
    .expect_err("requeue without selector should fail");
    assert_eq!(err.error_class(), ErrorClass::DataError);
    assert!(err.to_string().contains("Must specify exactly one"));
    Ok(())
}

#[sinex_test]
async fn dlq_requeue_by_id_requeues_event_engine_style_entry(ctx: TestContext) -> TestResult<()> {
    let harness = NatsHarness::start(ctx).await?;
    let js = jetstream::new(harness.client.clone());
    js.get_or_create_stream(jetstream::stream::Config {
        name: harness.env.nats_stream_name("SINEX_RAW_EVENTS_DLQ"),
        subjects: vec![harness.env.nats_subject("events.dlq.event_engine")],
        retention: jetstream::stream::RetentionPolicy::Limits,
        max_messages: 1000,
        storage: jetstream::stream::StorageType::Memory,
        allow_direct: true,
        ..Default::default()
    })
    .await?;

    let event_id = uuid::Uuid::now_v7().to_string();
    let original_subject = harness
        .env
        .nats_subject("events.raw.test_source.test_input");
    js.get_or_create_stream(jetstream::stream::Config {
        name: harness.env.nats_stream_name("EVENTS"),
        subjects: vec![original_subject.clone()],
        retention: jetstream::stream::RetentionPolicy::Limits,
        max_messages: 1000,
        storage: jetstream::stream::StorageType::Memory,
        ..Default::default()
    })
    .await?;
    let original_event = json!({
        "id": event_id,
        "source": "test.source",
        "event_type": "test.input",
        "ts_orig": Timestamp::now().format_rfc3339(),
        "host": "test-host",
        "payload": { "value": "gateway requeue proof" }
    });
    let dlq_entry = json!({
        "nats_msg_id": event_id,
        "error": "db failure",
        "original_payload": original_event,
        "failed_at": Timestamp::now().format_rfc3339()
    });

    let mut headers = async_nats::HeaderMap::new();
    headers.insert("Nats-Msg-Id", format!("dlq.{event_id}").as_str());
    headers.insert("Original-Subject", original_subject.as_str());
    headers.insert("Retry-Count", "0");
    headers.insert("Event-Id", event_id.as_str());

    let mut original_sub = harness.client.subscribe(original_subject.clone()).await?;
    harness
        .client
        .publish_with_headers(
            harness.env.nats_subject("events.dlq.event_engine"),
            headers,
            serde_json::to_vec(&dlq_entry)?.into(),
        )
        .await?;
    harness.client.flush().await?;
    wait_for_dlq_stream_messages(&harness.client, &harness.env, 1).await?;

    let response = handle_dlq_requeue(
        &harness.services,
        DlqRequeueRequest {
            event_id: Some(event_id.clone()),
            start_sequence: None,
            end_sequence: None,
            all: false,
        },
        &admin_auth(),
    )
    .await?;
    assert_eq!(response.status, "success");
    assert_eq!(response.requeued_count, 1);
    let operations = harness
        .services
        .pool()
        .state()
        .list_operations(Some("dlq.requeue"), Some(OperationStatus::Success), 10)
        .await?;
    let operation = operations
        .first()
        .expect("requeue should write a successful operation record");
    assert_eq!(operation.id.to_uuid().to_string(), response.operation_id);
    assert_eq!(
        operation
            .scope
            .as_ref()
            .and_then(|scope| scope["action"].as_str()),
        Some("requeue")
    );
    assert_eq!(
        operation
            .scope
            .as_ref()
            .and_then(|scope| scope["selector"].as_str()),
        Some("event_id")
    );
    assert_eq!(
        operation
            .scope
            .as_ref()
            .and_then(|scope| scope["event_id"].as_str()),
        Some(event_id.as_str())
    );

    let requeued = tokio::time::timeout(Duration::from_secs(5), original_sub.next())
        .await
        .map_err(|_| color_eyre::eyre::eyre!("timed out waiting for requeued gateway event"))?
        .ok_or_else(|| color_eyre::eyre::eyre!("gateway requeue subscription closed"))?;
    let payload: serde_json::Value = serde_json::from_slice(&requeued.payload)?;
    assert_eq!(payload["id"].as_str(), Some(event_id.as_str()));
    assert_eq!(
        payload["payload"]["value"].as_str(),
        Some("gateway requeue proof")
    );

    let after = handle_dlq_list(&harness.services, DlqListRequest {}).await?;
    assert_eq!(after.total_messages, 0);

    Ok(())
}

#![cfg(feature = "messaging")]

use async_nats::jetstream;
use futures::StreamExt;
use serde_json::json;
use sinex_node_sdk::{DlqRetryConfig, DlqRetryHandler, NatsPublisher};
use sinex_primitives::prelude::*;
use std::time::Duration;
use xtask::sandbox::prelude::*;

async fn ensure_retry_streams(
    client: &async_nats::Client,
    env: &sinex_primitives::environment::SinexEnvironment,
) -> TestResult<()> {
    let js = jetstream::new(client.clone());
    js.get_or_create_stream(jetstream::stream::Config {
        name: env.nats_stream_name("EVENTS"),
        subjects: vec![env.nats_subject("events.raw.>")],
        storage: jetstream::stream::StorageType::Memory,
        ..Default::default()
    })
    .await?;
    js.get_or_create_stream(jetstream::stream::Config {
        name: env.nats_stream_name("SINEX_RAW_EVENTS_DLQ"),
        subjects: vec![env.nats_subject("events.dlq.>")],
        storage: jetstream::stream::StorageType::Memory,
        allow_direct: true,
        ..Default::default()
    })
    .await?;
    Ok(())
}

#[sinex_test]
async fn test_dlq_retry_config_defaults() -> TestResult<()> {
    let config = DlqRetryConfig::default();
    assert_eq!(config.consumer_name, "dlq-retry-consumer");
    assert_eq!(config.batch_size, 10);
    assert_eq!(config.max_retries, 3);
    assert_eq!(config.retry_delay, std::time::Duration::from_mins(1));
    assert_eq!(config.per_message_delay_ms, 10);
    Ok(())
}

#[sinex_test]
async fn dlq_retry_errors_without_stream(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let client = ctx.nats_client();
    let env = ctx.env().clone();
    let handler = DlqRetryHandler::new(client, env, DlqRetryConfig::default());

    let err = handler.get_stats().await.unwrap_err();
    assert!(
        err.to_string().contains("Failed to get DLQ stream"),
        "got: {err}"
    );

    let err = handler.retry_all().await.unwrap_err();
    assert!(
        err.to_string().contains("Failed to get DLQ stream"),
        "got: {err}"
    );

    let err = handler.retry_by_id("missing").await.unwrap_err();
    assert!(
        err.to_string().contains("Failed to get DLQ stream"),
        "got: {err}"
    );
    Ok(())
}

#[sinex_test]
async fn dlq_retry_by_id_reports_missing_event(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let client = ctx.nats_client();
    let env = ctx.env().clone();
    ensure_retry_streams(&client, &env).await?;

    let handler = DlqRetryHandler::new(client, env, DlqRetryConfig::default());
    let err = handler.retry_by_id("nonexistent").await.unwrap_err();
    assert!(err.to_string().contains("not found"), "got: {err}");
    Ok(())
}

#[sinex_test]
async fn dlq_retry_by_id_requeues_node_sdk_entry(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let client = ctx.nats_client();
    let env = ctx.env().clone();
    ensure_retry_streams(&client, &env).await?;

    let publisher = NatsPublisher::new(client.clone());
    let mut event = DynamicPayload::new(
        "test.source",
        "test.input",
        json!({"value": "hello from node-sdk"}),
    )
    .from_material_at(Id::<SourceMaterial>::new(), 0)
    .build()?;
    event.id = Some(Id::new());
    let event_id = event.id.expect("event id").to_string();

    let original_subject = env.nats_raw_event_subject_with_namespace(
        None,
        event.source.as_str(),
        event.event_type.as_str(),
    );
    let mut original_sub = client.subscribe(original_subject.clone()).await?;

    publisher
        .publish_to_dlq(&event, "boom", "test.node")
        .await?;

    let handler = DlqRetryHandler::new(client.clone(), env, DlqRetryConfig::default());
    handler.retry_by_id(&event_id).await?;

    let requeued = tokio::time::timeout(Duration::from_secs(5), original_sub.next())
        .await
        .map_err(|_| color_eyre::eyre::eyre!("timed out waiting for requeued node-sdk event"))?
        .ok_or_else(|| color_eyre::eyre::eyre!("requeue subscription closed"))?;
    let payload: JsonValue = serde_json::from_slice(&requeued.payload)?;
    assert_eq!(payload["id"].as_str(), Some(event_id.as_str()));
    assert_eq!(
        payload["payload"]["value"].as_str(),
        Some("hello from node-sdk")
    );

    let stats = handler.get_stats().await?;
    assert_eq!(
        stats.total_messages, 0,
        "DLQ entry should be removed after requeue"
    );

    Ok(())
}

#[sinex_test]
async fn dlq_retry_by_id_requeues_ingestd_style_entry(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let client = ctx.nats_client();
    let env = ctx.env().clone();
    ensure_retry_streams(&client, &env).await?;

    let event_id = Uuid::now_v7().to_string();
    let original_subject = env.nats_subject("events.raw.test_source.test_input");
    let original_event = json!({
        "id": event_id,
        "source": "test.source",
        "event_type": "test.input",
        "ts_orig": Timestamp::now().format_rfc3339(),
        "host": "test-host",
        "payload": { "value": "hello from ingestd" }
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

    let mut original_sub = client.subscribe(original_subject.clone()).await?;
    client
        .publish_with_headers(
            env.nats_subject("events.dlq.ingestd"),
            headers,
            serde_json::to_vec(&dlq_entry)?.into(),
        )
        .await?;
    client.flush().await?;

    let handler = DlqRetryHandler::new(client.clone(), env, DlqRetryConfig::default());
    handler.retry_by_id(&event_id).await?;

    let requeued = tokio::time::timeout(Duration::from_secs(5), original_sub.next())
        .await
        .map_err(|_| color_eyre::eyre::eyre!("timed out waiting for requeued ingestd event"))?
        .ok_or_else(|| color_eyre::eyre::eyre!("requeue subscription closed"))?;
    let payload: JsonValue = serde_json::from_slice(&requeued.payload)?;
    assert_eq!(payload["id"].as_str(), Some(event_id.as_str()));
    assert_eq!(
        payload["payload"]["value"].as_str(),
        Some("hello from ingestd")
    );

    let stats = handler.get_stats().await?;
    assert_eq!(
        stats.total_messages, 0,
        "DLQ entry should be removed after requeue"
    );

    Ok(())
}

#[sinex_test]
async fn dlq_retry_by_id_rejects_invalid_retry_count_header(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let client = ctx.nats_client();
    let env = ctx.env().clone();
    ensure_retry_streams(&client, &env).await?;

    let event_id = Uuid::now_v7().to_string();
    let original_subject = env.nats_subject("events.raw.test_source.test_input");
    let original_event = json!({
        "id": event_id,
        "source": "test.source",
        "event_type": "test.input",
        "ts_orig": Timestamp::now().format_rfc3339(),
        "host": "test-host",
        "payload": { "value": "bad retry count" }
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
    headers.insert("Retry-Count", "not-a-number");
    headers.insert("Event-Id", event_id.as_str());

    client
        .publish_with_headers(
            env.nats_subject("events.dlq.ingestd"),
            headers,
            serde_json::to_vec(&dlq_entry)?.into(),
        )
        .await?;
    client.flush().await?;

    let handler = DlqRetryHandler::new(client.clone(), env, DlqRetryConfig::default());
    let error = handler
        .retry_by_id(&event_id)
        .await
        .expect_err("invalid Retry-Count header should fail honestly");

    assert!(error.to_string().contains("Retry-Count"));
    assert!(error.to_string().contains("not-a-number"));
    Ok(())
}

#[sinex_test]
async fn dlq_retry_by_id_honors_max_retry_limit(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let client = ctx.nats_client();
    let env = ctx.env().clone();
    ensure_retry_streams(&client, &env).await?;

    let event_id = Uuid::now_v7().to_string();
    let original_subject = env.nats_subject("events.raw.test_source.test_input");
    let original_event = json!({
        "id": event_id,
        "source": "test.source",
        "event_type": "test.input",
        "ts_orig": Timestamp::now().format_rfc3339(),
        "host": "test-host",
        "payload": { "value": "max retries reached" }
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
    headers.insert("Retry-Count", "1");
    headers.insert("Event-Id", event_id.as_str());

    let mut original_sub = client.subscribe(original_subject).await?;
    client
        .publish_with_headers(
            env.nats_subject("events.dlq.ingestd"),
            headers,
            serde_json::to_vec(&dlq_entry)?.into(),
        )
        .await?;
    client.flush().await?;

    let handler = DlqRetryHandler::new(
        client.clone(),
        env,
        DlqRetryConfig {
            max_retries: 1,
            ..DlqRetryConfig::default()
        },
    );
    let error = handler
        .retry_by_id(&event_id)
        .await
        .expect_err("retry_by_id must enforce the configured max retry limit");

    assert!(error.to_string().contains("exceeded max retries"));
    assert!(error.to_string().contains(event_id.as_str()));
    assert!(
        tokio::time::timeout(Duration::from_millis(250), original_sub.next())
            .await
            .is_err(),
        "maxed-out DLQ entries must not be requeued back onto the original subject"
    );

    let stats = handler.get_stats().await?;
    assert_eq!(
        stats.total_messages, 0,
        "maxed-out DLQ entry should be permanently removed instead of left pending"
    );
    Ok(())
}

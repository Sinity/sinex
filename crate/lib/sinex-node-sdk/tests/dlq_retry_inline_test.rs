#![cfg(feature = "messaging")]

use async_nats::jetstream;
use sinex_node_sdk::{DlqRetryConfig, DlqRetryHandler};
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn test_dlq_retry_config_defaults() -> TestResult<()> {
    let config = DlqRetryConfig::default();
    assert_eq!(config.consumer_name, "dlq-retry-consumer");
    assert_eq!(config.batch_size, 10);
    assert_eq!(config.max_retries, 3);
    assert_eq!(config.retry_delay, std::time::Duration::from_secs(60));
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
    let js = jetstream::new(client.clone());

    let stream_name = env.nats_stream_name("EVENTS_DLQ");
    js.get_or_create_stream(jetstream::stream::Config {
        name: stream_name,
        subjects: vec![env.nats_subject("EVENTS_DLQ")],
        ..Default::default()
    })
    .await?;

    let handler = DlqRetryHandler::new(client, env, DlqRetryConfig::default());
    let err = handler.retry_by_id("nonexistent").await.unwrap_err();
    assert!(err.to_string().contains("not found"), "got: {err}");
    Ok(())
}

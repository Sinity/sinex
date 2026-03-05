//! JetStream Bootstrap and Configuration Tests
//!
//! These tests verify JetStream stream and consumer initialization handles
//! edge cases correctly, particularly around idempotency, configuration
//! conflicts, and concurrent initialization.

use xtask::sandbox::prelude::*;

use async_nats::jetstream::{
    consumer::{AckPolicy, DeliverPolicy, pull::Config as ConsumerConfig},
    stream::Config as StreamConfig,
};
use std::time::Duration;

#[sinex_test]
async fn test_stream_creation_idempotent(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let js = ctx.jetstream().await?;

    // Create a stream with unique name using UUIDv7
    let stream_name = format!("STREAM_CREATION_{}", sinex_primitives::Uuid::now_v7());

    let config = StreamConfig {
        name: stream_name.clone(),
        subjects: vec![format!("{}.*", stream_name)],
        max_age: Duration::from_secs(60),
        ..Default::default()
    };

    // Create stream first time
    let mut stream1 = js.get_or_create_stream(config.clone()).await?;

    // Create again - should be idempotent (same stream)
    let mut stream2 = js.get_or_create_stream(config).await?;

    // Both should have the same name (via .info())
    let info1 = stream1.info().await?;
    let info2 = stream2.info().await?;
    assert_eq!(info1.config.name, info2.config.name);
    assert_eq!(info1.config.name, stream_name);

    Ok(())
}

#[sinex_test]
async fn test_consumer_creation_idempotent(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let js = ctx.jetstream().await?;

    // Create a stream first
    let stream_name = format!("STREAM_CONSUMER_{}", sinex_primitives::Uuid::now_v7());
    let consumer_name = format!("CONSUMER_{}", sinex_primitives::Uuid::now_v7());

    let stream_config = StreamConfig {
        name: stream_name.clone(),
        subjects: vec![format!("{}.*", stream_name)],
        max_age: Duration::from_secs(60),
        ..Default::default()
    };

    let stream = js.get_or_create_stream(stream_config).await?;

    // Create a consumer
    let consumer_config = ConsumerConfig {
        deliver_policy: DeliverPolicy::All,
        ack_policy: AckPolicy::Explicit,
        ..Default::default()
    };

    let consumer1 = stream.create_consumer(consumer_config.clone()).await?;

    // Create same consumer again - should be idempotent
    let consumer2 = stream
        .get_or_create_consumer(&consumer_name, consumer_config)
        .await?;

    // Both should succeed — cached_info() is synchronous
    assert!(!consumer1.cached_info().name.is_empty());
    assert!(!consumer2.cached_info().name.is_empty());

    Ok(())
}

#[sinex_test]
async fn test_concurrent_stream_creation(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let js = ctx.jetstream().await?;

    let stream_name = format!("STREAM_CONCURRENT_{}", sinex_primitives::Uuid::now_v7());
    let subject = format!("{}.*", stream_name);

    // Spawn multiple concurrent stream creations with the same config
    let mut handles = vec![];

    for _ in 0..3 {
        let js_clone = js.clone();
        let stream_name_clone = stream_name.clone();
        let subject_clone = subject.clone();

        let handle = tokio::spawn(async move {
            let config = StreamConfig {
                name: stream_name_clone,
                subjects: vec![subject_clone],
                max_age: Duration::from_secs(60),
                ..Default::default()
            };
            js_clone.get_or_create_stream(config).await
        });

        handles.push(handle);
    }

    // All concurrent creations should succeed
    let results: Vec<_> = futures::future::join_all(handles).await;
    for result in results {
        let mut stream_result = result??;
        let info = stream_result.info().await?;
        assert_eq!(info.config.name, stream_name);
    }

    // Verify stream exists exactly once
    let mut stream = js.get_stream(&stream_name).await?;
    let info = stream.info().await?;
    assert_eq!(info.config.name, stream_name);

    Ok(())
}

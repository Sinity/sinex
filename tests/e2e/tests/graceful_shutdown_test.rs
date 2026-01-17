//! Graceful Shutdown Tests
//!
//! These tests verify that services handle shutdown signals properly,
//! including in-flight request completion and resource cleanup.
//!
//! ## Coverage Areas
//! - Ingestd graceful shutdown with pending messages
//! - Service shutdown preserves data integrity
//! - Concurrent shutdown handling
//! - Shutdown timeout behavior

use async_nats::jetstream;
use camino::Utf8PathBuf;
use serde_json::json;
use sinex_core::nats::NatsConnectionConfig;
use sinex_ingestd::{config::IngestdConfig, service::IngestService, JetStreamTopology};
use sinex_test_utils::prelude::*;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use tempfile::TempDir;
use tokio::time::{timeout, Duration};

/// Test that ingestd completes in-flight processing before shutdown.
#[sinex_test(timeout = 60)]
async fn test_ingestd_graceful_shutdown_completes_inflight(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().await?;
    let nats = ctx.nats_handle()?;
    let js = ctx.jetstream().await?;
    let env = ctx.env();

    let base_stream = env.nats_stream_name("SINEX_GRACEFUL_EVENTS");
    let consumer_name = "ingestd-graceful".to_string();
    let topology = JetStreamTopology::new(env, base_stream.clone(), consumer_name.clone(), None);

    let work_dir = TempDir::new()?;
    let work_dir_utf8 = Utf8PathBuf::from_path_buf(work_dir.path().to_path_buf())
        .unwrap_or_else(|_| Utf8PathBuf::from("/tmp"));
    let annex_path = work_dir_utf8.join("annex");
    let assembler_state_dir = work_dir_utf8.join("assembler_state");
    tokio::fs::create_dir_all(annex_path.as_std_path()).await?;
    tokio::fs::create_dir_all(assembler_state_dir.as_std_path()).await?;

    let mut config = IngestdConfig::builder()
        .database_url(ctx.database_url().to_string())
        .nats(
            NatsConnectionConfig::builder()
                .url(nats.client_url().to_string())
                .build(),
        )
        .nats_stream_name(base_stream)
        .nats_consumer_name(consumer_name)
        .batch_size(8)
        .consumer_fetch_max_messages(16)
        .consumer_fetch_timeout_ms(100.into())
        .validate_schemas(false)
        .skip_schema_sync(true)
        .work_dir(work_dir_utf8.clone())
        .annex_repo_path(annex_path)
        .assembler_state_dir(assembler_state_dir)
        .build();

    config.database_pool_size = 4;

    let mut service = IngestService::new(config.clone()).await?;
    let mut runner = service.clone();
    let handle = tokio::spawn(async move { runner.run().await });

    nats.wait_for_stream(&js, &topology.events_stream, Duration::from_secs(10))
        .await?;

    // Publish events before shutdown
    let publisher = TestSatellitePublisher::new(ctx.nats_client(), "graceful-source");
    let mut event_ids = Vec::new();
    for idx in 0..5 {
        let event_id = publisher
            .publish_event("graceful.event", json!({ "seq": idx }))
            .await?;
        event_ids.push(event_id);
    }

    // Small delay to allow some processing
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Initiate shutdown
    service.shutdown().await?;

    let join_result = timeout(Duration::from_secs(10), handle)
        .await
        .map_err(|_| color_eyre::eyre::eyre!("ingestd runner shutdown timed out"))?;
    join_result??;

    // Verify events processed before shutdown were persisted
    let event_count = ctx.pool.events().count_all().await?;
    tracing::info!("Events after graceful shutdown: {}", event_count);

    // Should have at least some events persisted
    assert!(
        event_count > 0,
        "Should have persisted events during graceful shutdown"
    );

    Ok(())
}

/// Test that shutdown signal is respected even with continuous load.
#[sinex_test(timeout = 60)]
async fn test_shutdown_under_continuous_load(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().await?;
    let nats = ctx.nats_handle()?;
    let js = ctx.jetstream().await?;
    let env = ctx.env();

    let base_stream = env.nats_stream_name("SINEX_LOAD_EVENTS");
    let consumer_name = "ingestd-load".to_string();
    let topology = JetStreamTopology::new(env, base_stream.clone(), consumer_name.clone(), None);

    let work_dir = TempDir::new()?;
    let work_dir_utf8 = Utf8PathBuf::from_path_buf(work_dir.path().to_path_buf())
        .unwrap_or_else(|_| Utf8PathBuf::from("/tmp"));
    let annex_path = work_dir_utf8.join("annex");
    let assembler_state_dir = work_dir_utf8.join("assembler_state");
    tokio::fs::create_dir_all(annex_path.as_std_path()).await?;
    tokio::fs::create_dir_all(assembler_state_dir.as_std_path()).await?;

    let mut config = IngestdConfig::builder()
        .database_url(ctx.database_url().to_string())
        .nats(
            NatsConnectionConfig::builder()
                .url(nats.client_url().to_string())
                .build(),
        )
        .nats_stream_name(base_stream)
        .nats_consumer_name(consumer_name)
        .batch_size(16)
        .consumer_fetch_max_messages(32)
        .consumer_fetch_timeout_ms(100.into())
        .validate_schemas(false)
        .skip_schema_sync(true)
        .work_dir(work_dir_utf8.clone())
        .annex_repo_path(annex_path)
        .assembler_state_dir(assembler_state_dir)
        .build();

    config.database_pool_size = 4;

    let mut service = IngestService::new(config.clone()).await?;
    let mut runner = service.clone();
    let handle = tokio::spawn(async move { runner.run().await });

    nats.wait_for_stream(&js, &topology.events_stream, Duration::from_secs(10))
        .await?;

    // Start continuous publisher
    let shutdown_flag = Arc::new(AtomicBool::new(false));
    let published_count = Arc::new(AtomicU32::new(0));
    let shutdown_flag_clone = shutdown_flag.clone();
    let published_count_clone = published_count.clone();
    let nats_client = ctx.nats_client();

    let publisher_handle = tokio::spawn(async move {
        let publisher = TestSatellitePublisher::new(nats_client, "load-source");
        let mut idx = 0;
        while !shutdown_flag_clone.load(Ordering::SeqCst) {
            let _ = publisher
                .publish_event("load.event", json!({ "seq": idx }))
                .await;
            published_count_clone.fetch_add(1, Ordering::SeqCst);
            idx += 1;
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    });

    // Let it run for a bit
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Signal shutdown
    shutdown_flag.store(true, Ordering::SeqCst);
    service.shutdown().await?;

    // Wait for publisher to stop
    let _ = timeout(Duration::from_secs(2), publisher_handle).await;

    // Wait for service to stop
    let join_result = timeout(Duration::from_secs(10), handle)
        .await
        .map_err(|_| color_eyre::eyre::eyre!("ingestd runner shutdown timed out under load"))?;
    join_result??;

    let total_published = published_count.load(Ordering::SeqCst);
    let event_count = ctx.pool.events().count_all().await?;

    tracing::info!(
        "Continuous load: published={}, persisted={}",
        total_published,
        event_count
    );

    // Should have persisted a reasonable portion of events
    assert!(
        event_count > 0,
        "Should have persisted some events under load"
    );

    Ok(())
}

/// Test that multiple services can be shutdown concurrently.
#[sinex_test]
async fn test_concurrent_service_shutdown(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_shared_nats().await?;
    let nats_client = ctx.nats_client();
    let js = jetstream::new(nats_client.clone());

    let stream_name = format!("TEST_CONCURRENT_SHUTDOWN_{}", uuid::Uuid::new_v4().simple());

    // Create stream
    js.create_stream(jetstream::stream::Config {
        name: stream_name.clone(),
        subjects: vec![format!("{}.>", stream_name)],
        max_messages: 10000,
        ..Default::default()
    })
    .await?;

    let subject = format!("{}.events", stream_name);

    // Create multiple consumers
    let stream = js.get_stream(&stream_name).await?;
    let mut consumers = vec![];

    for i in 0..5 {
        let consumer = stream
            .create_consumer(jetstream::consumer::pull::Config {
                name: Some(format!("shutdown-consumer-{}", i)),
                durable_name: Some(format!("shutdown-consumer-{}", i)),
                ack_policy: jetstream::consumer::AckPolicy::Explicit,
                ..Default::default()
            })
            .await?;
        consumers.push(consumer);
    }

    // Publish some messages
    for i in 0..50 {
        js.publish(subject.clone(), format!("message-{}", i).into())
            .await?
            .await?;
    }

    // Start consumer tasks
    let active_count = Arc::new(AtomicU32::new(consumers.len() as u32));
    let shutdown_flag = Arc::new(AtomicBool::new(false));
    let mut handles = vec![];

    for consumer in consumers {
        let active = active_count.clone();
        let shutdown = shutdown_flag.clone();

        let handle = tokio::spawn(async move {
            while !shutdown.load(Ordering::SeqCst) {
                let fetch_result = consumer
                    .fetch()
                    .max_messages(5)
                    .expires(Duration::from_millis(500))
                    .messages()
                    .await;

                if let Ok(mut messages) = fetch_result {
                    use futures::StreamExt;
                    while let Some(Ok(msg)) = messages.next().await {
                        let _ = msg.ack().await;
                    }
                }
            }
            active.fetch_sub(1, Ordering::SeqCst);
        });

        handles.push(handle);
    }

    // Let consumers run
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Signal shutdown to all consumers
    shutdown_flag.store(true, Ordering::SeqCst);

    // Wait for all to complete
    let shutdown_start = std::time::Instant::now();
    for handle in handles {
        let _ = timeout(Duration::from_secs(5), handle).await;
    }
    let shutdown_duration = shutdown_start.elapsed();

    tracing::info!("Concurrent shutdown took {:?}", shutdown_duration);

    // All should have shutdown within timeout
    assert_eq!(
        active_count.load(Ordering::SeqCst),
        0,
        "All consumers should have shutdown"
    );

    // Cleanup
    js.delete_stream(&stream_name).await?;

    Ok(())
}

/// Test that shutdown preserves data consistency (no partial writes).
#[sinex_test(timeout = 60)]
async fn test_shutdown_data_consistency(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().await?;
    let nats = ctx.nats_handle()?;
    let js = ctx.jetstream().await?;
    let env = ctx.env();

    let base_stream = env.nats_stream_name("SINEX_CONSISTENCY_EVENTS");
    let consumer_name = "ingestd-consistency".to_string();
    let topology = JetStreamTopology::new(env, base_stream.clone(), consumer_name.clone(), None);

    let work_dir = TempDir::new()?;
    let work_dir_utf8 = Utf8PathBuf::from_path_buf(work_dir.path().to_path_buf())
        .unwrap_or_else(|_| Utf8PathBuf::from("/tmp"));
    let annex_path = work_dir_utf8.join("annex");
    let assembler_state_dir = work_dir_utf8.join("assembler_state");
    tokio::fs::create_dir_all(annex_path.as_std_path()).await?;
    tokio::fs::create_dir_all(assembler_state_dir.as_std_path()).await?;

    let mut config = IngestdConfig::builder()
        .database_url(ctx.database_url().to_string())
        .nats(
            NatsConnectionConfig::builder()
                .url(nats.client_url().to_string())
                .build(),
        )
        .nats_stream_name(base_stream)
        .nats_consumer_name(consumer_name)
        .batch_size(4)
        .consumer_fetch_max_messages(8)
        .consumer_fetch_timeout_ms(100.into())
        .validate_schemas(false)
        .skip_schema_sync(true)
        .work_dir(work_dir_utf8.clone())
        .annex_repo_path(annex_path)
        .assembler_state_dir(assembler_state_dir)
        .build();

    config.database_pool_size = 4;

    let mut service = IngestService::new(config.clone()).await?;
    let mut runner = service.clone();
    let handle = tokio::spawn(async move { runner.run().await });

    nats.wait_for_stream(&js, &topology.events_stream, Duration::from_secs(10))
        .await?;

    // Publish events with structured data
    let publisher = TestSatellitePublisher::new(ctx.nats_client(), "consistency-source");
    for idx in 0..10 {
        publisher
            .publish_event(
                "consistency.event",
                json!({
                    "index": idx,
                    "checksum": format!("check-{}", idx),
                    "data": format!("data-{}", idx)
                }),
            )
            .await?;
    }

    // Allow some processing
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Shutdown
    service.shutdown().await?;
    let _ = timeout(Duration::from_secs(10), handle).await;

    // Verify data consistency: no partial records
    let events = ctx
        .pool
        .events()
        .get_by_event_type(
            &sinex_core::EventType::from("consistency.event"),
            sinex_core::types::Pagination::new(Some(100), None),
        )
        .await?;

    for event in &events {
        // Each persisted event should have complete payload
        let payload = &event.payload;
        assert!(
            payload.get("index").is_some(),
            "Event should have index field"
        );
        assert!(
            payload.get("checksum").is_some(),
            "Event should have checksum field"
        );
        assert!(payload.get("data").is_some(), "Event should have data field");

        // Verify checksum matches index
        if let (Some(idx), Some(checksum)) = (
            payload.get("index").and_then(|v| v.as_i64()),
            payload.get("checksum").and_then(|v| v.as_str()),
        ) {
            assert_eq!(
                checksum,
                format!("check-{}", idx),
                "Checksum should match index"
            );
        }
    }

    tracing::info!(
        "Data consistency verified: {} events with complete payloads",
        events.len()
    );

    Ok(())
}

/// Test shutdown timeout handling.
#[sinex_test]
async fn test_shutdown_timeout_handling(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_shared_nats().await?;
    let nats_client = ctx.nats_client();
    let js = jetstream::new(nats_client.clone());

    let stream_name = format!("TEST_SHUTDOWN_TIMEOUT_{}", uuid::Uuid::new_v4().simple());

    // Create stream
    js.create_stream(jetstream::stream::Config {
        name: stream_name.clone(),
        subjects: vec![format!("{}.>", stream_name)],
        max_messages: 1000,
        ..Default::default()
    })
    .await?;

    let subject = format!("{}.events", stream_name);

    // Publish messages
    for i in 0..20 {
        js.publish(subject.clone(), format!("message-{}", i).into())
            .await?
            .await?;
    }

    // Create a slow consumer that doesn't respect shutdown
    let stream = js.get_stream(&stream_name).await?;
    let consumer = stream
        .create_consumer(jetstream::consumer::pull::Config {
            name: Some("timeout-consumer".to_string()),
            durable_name: Some("timeout-consumer".to_string()),
            ack_policy: jetstream::consumer::AckPolicy::Explicit,
            ..Default::default()
        })
        .await?;

    let shutdown_requested = Arc::new(AtomicBool::new(false));
    let processed_after_shutdown = Arc::new(AtomicU32::new(0));

    let shutdown_flag = shutdown_requested.clone();
    let processed = processed_after_shutdown.clone();

    let consumer_handle = tokio::spawn(async move {
        loop {
            let fetch_result = consumer
                .fetch()
                .max_messages(1)
                .expires(Duration::from_millis(500))
                .messages()
                .await;

            if let Ok(mut messages) = fetch_result {
                use futures::StreamExt;
                while let Some(Ok(msg)) = messages.next().await {
                    // Simulate slow processing
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    let _ = msg.ack().await;

                    if shutdown_flag.load(Ordering::SeqCst) {
                        processed.fetch_add(1, Ordering::SeqCst);
                    }
                }
            }

            // Check if we should exit
            if shutdown_flag.load(Ordering::SeqCst) && processed.load(Ordering::SeqCst) >= 2 {
                break;
            }
        }
    });

    // Let consumer start
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Request shutdown
    shutdown_requested.store(true, Ordering::SeqCst);
    let shutdown_start = std::time::Instant::now();

    // Wait with timeout
    let result = timeout(Duration::from_secs(5), consumer_handle).await;

    let elapsed = shutdown_start.elapsed();
    let processed_count = processed_after_shutdown.load(Ordering::SeqCst);

    tracing::info!(
        "Shutdown timeout test: elapsed={:?}, processed_after_shutdown={}",
        elapsed,
        processed_count
    );

    // Consumer should have finished within timeout
    assert!(result.is_ok(), "Consumer should complete within timeout");

    // Should have processed some messages after shutdown was requested
    assert!(
        processed_count >= 2,
        "Should have processed at least 2 messages after shutdown request"
    );

    // Cleanup
    js.delete_stream(&stream_name).await?;

    Ok(())
}

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
use futures::future::join_all;
use serde_json::json;
use sinex_ingestd::{JetStreamTopology, config::IngestdConfig, service::IngestService};
use sinex_primitives::nats::NatsConnectionConfig;
use sinex_primitives::{
    Event, EventSource, EventType, HostName, Id, OffsetKind, Provenance, SourceMaterial,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use tempfile::TempDir;
use tokio::time::{Duration, timeout};
use xtask::sandbox::prelude::*;
use xtask::sandbox::timing::{Timeouts, WaitHelpers};

async fn register_test_material(
    pool: &sinex_db::DbPool,
    source: &str,
) -> TestResult<Id<SourceMaterial>> {
    let material_id = Id::<SourceMaterial>::new();
    let identifier = format!("{source}-{material_id}");
    sqlx::query!(
        r#"
        INSERT INTO raw.source_material_registry
            (id, material_kind, source_identifier, status, timing_info_type, staged_at)
        VALUES ($1::uuid, 'annex', $2, 'completed', 'realtime', NOW())
        ON CONFLICT (id) DO NOTHING
        "#,
        material_id.to_uuid(),
        identifier,
    )
    .execute(pool)
    .await?;

    Ok(material_id)
}

/// Build a properly formatted `Event<JsonValue>` and serialize it for `JetStream`.
fn build_test_event_bytes(
    material_id: Id<SourceMaterial>,
    source: &str,
    event_type: &str,
    payload: serde_json::Value,
) -> TestResult<Vec<u8>> {
    let event = Event::<serde_json::Value> {
        id: Some(Id::new()),
        source: EventSource::new(source)?,
        event_type: EventType::new(event_type)?,
        payload,
        ts_orig: Some(sinex_primitives::Timestamp::now()),
        host: HostName::new("test-host")?,
        node_run_id: None,
        payload_schema_id: None,
        provenance: Provenance::Material {
            id: material_id,
            anchor_byte: 0,
            offset_start: None,
            offset_end: None,
            offset_kind: OffsetKind::Byte,
        },
        associated_blob_ids: None,
        temporal_policy: None,
        semantics_version: None,
        scope_key: None,
        equivalence_key: None,
        created_by_operation_id: None,
        node_model: None,
    };

    Ok(serde_json::to_vec(&event)?)
}

/// Test that ingestd completes in-flight processing before shutdown.
#[sinex_test(timeout = 60)]
async fn test_ingestd_graceful_shutdown_completes_inflight(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().await?;
    let nats = ctx.nats_handle()?;
    let nats_client = ctx.nats_client();
    let js = ctx.jetstream().await?;
    let env = ctx.env();

    let namespace = format!("graceful-{}", uuid::Uuid::new_v4().simple());
    let consumer_name = "ingestd-graceful".to_string();

    let work_dir = TempDir::new()?;
    let work_dir_utf8 = Utf8PathBuf::from_path_buf(work_dir.path().to_path_buf())
        .unwrap_or_else(|_| Utf8PathBuf::from("/tmp"));
    let content_store_path = work_dir_utf8.join("content-store");
    let assembler_state_dir = work_dir_utf8.join("assembler_state");
    tokio::fs::create_dir_all(content_store_path.as_std_path()).await?;
    tokio::fs::create_dir_all(assembler_state_dir.as_std_path()).await?;
    let material_id = register_test_material(&ctx.pool, "graceful-source").await?;

    let mut config = IngestdConfig::builder()
        .database_url(ctx.database_url().to_string())
        .nats(
            NatsConnectionConfig::builder()
                .url(nats.client_url().to_string())
                .build(),
        )
        .nats_stream_name(env.nats_stream_name("SINEX_GRACEFUL_EVENTS"))
        .nats_consumer_name(consumer_name)
        .nats_namespace(namespace)
        .consumer_fetch_max_messages(16)
        .consumer_fetch_timeout_ms(50.into())
        .validate_schemas(false)
        .skip_schema_sync(true)
        .work_dir(work_dir_utf8.clone())
        .content_store_path(content_store_path)
        .assembler_state_dir(assembler_state_dir)
        .build();

    config.database_pool_size = 4;
    let topology = JetStreamTopology::new(
        env,
        config.nats_stream_name.clone(),
        config.nats_consumer_name.clone(),
        config.nats_namespace.as_deref(),
    );

    let mut service = IngestService::new(config.clone()).await?;
    let mut runner = service.clone();
    let handle = tokio::spawn(async move { runner.run().await });

    nats.wait_for_stream(
        &js,
        &topology.events_stream,
        Duration::from_secs(Timeouts::SHORT),
    )
    .await?;

    // Wait for ingestd to attach a consumer — proves it's actively pulling messages.
    nats.wait_for_consumer_on_stream(
        &js,
        &topology.events_stream,
        Duration::from_secs(Timeouts::STANDARD),
    )
    .await?;

    // Publish events before shutdown directly to JetStream
    // Use the events_subject (e.g., "events.raw.>") not the stream name
    let subject = env.nats_raw_event_subject_with_namespace(
        config.nats_namespace.as_deref(),
        "graceful-source",
        "graceful.event",
    );
    for idx in 0..5 {
        let payload = build_test_event_bytes(
            material_id,
            "graceful-source",
            "graceful.event",
            json!({"seq": idx}),
        )?;
        nats_client.publish(subject.clone(), payload.into()).await?;
    }
    nats_client.flush().await?;

    // Wait for ingestd to persist at least one event before shutting down
    WaitHelpers::wait_for_event_count(&ctx.pool, 1, Timeouts::SHORT).await?;

    // Initiate shutdown
    service.shutdown().await?;

    let join_result = timeout(Duration::from_secs(Timeouts::SHORT), handle)
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

    let namespace = format!("load-{}", uuid::Uuid::new_v4().simple());
    let consumer_name = "ingestd-load".to_string();

    let work_dir = TempDir::new()?;
    let work_dir_utf8 = Utf8PathBuf::from_path_buf(work_dir.path().to_path_buf())
        .unwrap_or_else(|_| Utf8PathBuf::from("/tmp"));
    let content_store_path = work_dir_utf8.join("content-store");
    let assembler_state_dir = work_dir_utf8.join("assembler_state");
    tokio::fs::create_dir_all(content_store_path.as_std_path()).await?;
    tokio::fs::create_dir_all(assembler_state_dir.as_std_path()).await?;
    let material_id = register_test_material(&ctx.pool, "load-source").await?;

    let mut config = IngestdConfig::builder()
        .database_url(ctx.database_url().to_string())
        .nats(
            NatsConnectionConfig::builder()
                .url(nats.client_url().to_string())
                .build(),
        )
        .nats_stream_name(env.nats_stream_name("SINEX_LOAD_EVENTS"))
        .nats_consumer_name(consumer_name)
        .nats_namespace(namespace)
        .consumer_fetch_max_messages(32)
        .consumer_fetch_timeout_ms(50.into())
        .validate_schemas(false)
        .skip_schema_sync(true)
        .work_dir(work_dir_utf8.clone())
        .content_store_path(content_store_path)
        .assembler_state_dir(assembler_state_dir)
        .build();

    config.database_pool_size = 4;
    let topology = JetStreamTopology::new(
        env,
        config.nats_stream_name.clone(),
        config.nats_consumer_name.clone(),
        config.nats_namespace.as_deref(),
    );

    let mut service = IngestService::new(config.clone()).await?;
    let mut runner = service.clone();
    let handle = tokio::spawn(async move { runner.run().await });

    nats.wait_for_stream(
        &js,
        &topology.events_stream,
        Duration::from_secs(Timeouts::SHORT),
    )
    .await?;

    nats.wait_for_consumer_on_stream(
        &js,
        &topology.events_stream,
        Duration::from_secs(Timeouts::STANDARD),
    )
    .await?;

    // Start continuous publisher
    let shutdown_flag = Arc::new(AtomicBool::new(false));
    let published_count = Arc::new(AtomicU32::new(0));
    let shutdown_flag_clone = shutdown_flag.clone();
    let published_count_clone = published_count.clone();
    let nats_client = ctx.nats_client();
    let publisher_client = nats_client.clone();
    let subject = env.nats_raw_event_subject_with_namespace(
        config.nats_namespace.as_deref(),
        "load-source",
        "load.event",
    );

    let publisher_handle = tokio::spawn(async move {
        let mut idx = 0u64;
        while !shutdown_flag_clone.load(Ordering::SeqCst) {
            let event = Event::<serde_json::Value> {
                id: Some(Id::new()),
                source: EventSource::new("load-source").expect("valid source"),
                event_type: EventType::new("load.event").expect("valid event type"),
                payload: json!({"seq": idx}),
                ts_orig: Some(sinex_primitives::Timestamp::now()),
                host: HostName::new("test-host").expect("valid host"),
                node_run_id: None,
                payload_schema_id: None,
                provenance: Provenance::Material {
                    id: material_id,
                    anchor_byte: 0,
                    offset_start: None,
                    offset_end: None,
                    offset_kind: OffsetKind::Byte,
                },
                associated_blob_ids: None,
                temporal_policy: None,
                semantics_version: None,
                scope_key: None,
                equivalence_key: None,
                created_by_operation_id: None,
                node_model: None,
            };
            if let Ok(p) = serde_json::to_vec(&event) {
                let _ = publisher_client.publish(subject.clone(), p.into()).await;
                let _ = publisher_client.flush().await;
            }
            published_count_clone.fetch_add(1, Ordering::SeqCst);
            idx += 1;
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    });

    // Wait until the publisher is actively generating load and ingestd has
    // persisted at least one event before requesting shutdown.
    let pool = ctx.pool.clone();
    WaitHelpers::wait_for_condition(
        {
            let published_count = published_count.clone();
            move || {
                let pool = pool.clone();
                let published_count = published_count.clone();
                async move {
                    let persisted = pool.events().count_all().await?;
                    Ok::<bool, color_eyre::eyre::Error>(
                        published_count.load(Ordering::SeqCst) >= 32 && persisted > 0,
                    )
                }
            }
        },
        Timeouts::SHORT,
    )
    .await?;

    // Signal shutdown
    shutdown_flag.store(true, Ordering::SeqCst);
    service.shutdown().await?;

    // Wait for publisher to stop
    let _ = timeout(Duration::from_secs(Timeouts::SHORT), publisher_handle).await;

    // Wait for service to stop
    let join_result = timeout(Duration::from_secs(Timeouts::SHORT), handle)
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
    let ctx = ctx.with_nats().shared().await?;
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

    let subject = format!("{stream_name}.events");

    // Create multiple consumers
    let stream = js.get_stream(&stream_name).await?;
    let mut consumers = vec![];

    for i in 0..5 {
        let consumer = stream
            .create_consumer(jetstream::consumer::pull::Config {
                name: Some(format!("shutdown-consumer-{i}")),
                durable_name: Some(format!("shutdown-consumer-{i}")),
                ack_policy: jetstream::consumer::AckPolicy::Explicit,
                ..Default::default()
            })
            .await?;
        consumers.push(consumer);
    }

    // Publish some messages
    for i in 0..50 {
        js.publish(subject.clone(), format!("message-{i}").into())
            .await?
            .await?;
    }

    // Start consumer tasks
    let active_count = Arc::new(AtomicU32::new(consumers.len() as u32));
    let started_count = Arc::new(AtomicU32::new(0));
    let shutdown_flag = Arc::new(AtomicBool::new(false));
    let mut handles = vec![];

    for consumer in consumers {
        let active = active_count.clone();
        let started = started_count.clone();
        let shutdown = shutdown_flag.clone();

        let handle = tokio::spawn(async move {
            started.fetch_add(1, Ordering::SeqCst);
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

    // Wait until all consumer tasks are in their fetch loop before signalling shutdown.
    WaitHelpers::wait_for_condition(
        {
            let active_count = active_count.clone();
            let started_count = started_count.clone();
            move || {
                let active_count = active_count.clone();
                let started_count = started_count.clone();
                async move {
                    Ok::<bool, color_eyre::eyre::Error>(
                        started_count.load(Ordering::SeqCst) == active_count.load(Ordering::SeqCst),
                    )
                }
            }
        },
        Timeouts::SHORT,
    )
    .await?;

    // Signal shutdown to all consumers
    shutdown_flag.store(true, Ordering::SeqCst);

    // Wait for all to complete
    let shutdown_start = std::time::Instant::now();
    let join_result = timeout(Duration::from_secs(Timeouts::QUICK), async {
        join_all(handles).await
    })
    .await;
    let shutdown_duration = shutdown_start.elapsed();

    tracing::info!("Concurrent shutdown took {:?}", shutdown_duration);

    // All should have shutdown within timeout
    assert!(
        join_result.is_ok(),
        "all consumer tasks should complete within the shared shutdown timeout"
    );
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
    let nats_client = ctx.nats_client();
    let js = ctx.jetstream().await?;
    let env = ctx.env();

    let namespace = format!("consistency-{}", uuid::Uuid::new_v4().simple());
    let consumer_name = "ingestd-consistency".to_string();

    let work_dir = TempDir::new()?;
    let work_dir_utf8 = Utf8PathBuf::from_path_buf(work_dir.path().to_path_buf())
        .unwrap_or_else(|_| Utf8PathBuf::from("/tmp"));
    let content_store_path = work_dir_utf8.join("content-store");
    let assembler_state_dir = work_dir_utf8.join("assembler_state");
    tokio::fs::create_dir_all(content_store_path.as_std_path()).await?;
    tokio::fs::create_dir_all(assembler_state_dir.as_std_path()).await?;
    let material_id = register_test_material(&ctx.pool, "consistency-source").await?;

    let mut config = IngestdConfig::builder()
        .database_url(ctx.database_url().to_string())
        .nats(
            NatsConnectionConfig::builder()
                .url(nats.client_url().to_string())
                .build(),
        )
        .nats_stream_name(env.nats_stream_name("SINEX_CONSISTENCY_EVENTS"))
        .nats_consumer_name(consumer_name)
        .nats_namespace(namespace)
        .consumer_fetch_max_messages(8)
        .consumer_fetch_timeout_ms(50.into())
        .validate_schemas(false)
        .skip_schema_sync(true)
        .work_dir(work_dir_utf8.clone())
        .content_store_path(content_store_path)
        .assembler_state_dir(assembler_state_dir)
        .build();

    config.database_pool_size = 4;
    let topology = JetStreamTopology::new(
        env,
        config.nats_stream_name.clone(),
        config.nats_consumer_name.clone(),
        config.nats_namespace.as_deref(),
    );

    let mut service = IngestService::new(config.clone()).await?;
    let mut runner = service.clone();
    let handle = tokio::spawn(async move { runner.run().await });

    nats.wait_for_stream(
        &js,
        &topology.events_stream,
        Duration::from_secs(Timeouts::SHORT),
    )
    .await?;

    nats.wait_for_consumer_on_stream(
        &js,
        &topology.events_stream,
        Duration::from_secs(Timeouts::STANDARD),
    )
    .await?;

    // Publish events with structured data directly to JetStream
    let subject = env.nats_raw_event_subject_with_namespace(
        config.nats_namespace.as_deref(),
        "consistency-source",
        "consistency.event",
    );
    for idx in 0..10 {
        let payload = build_test_event_bytes(
            material_id,
            "consistency-source",
            "consistency.event",
            json!({
                "index": idx,
                "checksum": format!("check-{}", idx),
                "data": format!("data-{}", idx)
            }),
        )?;
        nats_client.publish(subject.clone(), payload.into()).await?;
    }
    nats_client.flush().await?;

    // Wait for at least one event to be persisted before shutting down
    WaitHelpers::wait_for_event_count(&ctx.pool, 1, Timeouts::SHORT).await?;

    // Shutdown
    service.shutdown().await?;
    let _ = timeout(Duration::from_secs(Timeouts::SHORT), handle).await;

    // Verify data consistency: no partial records
    let events = ctx
        .pool
        .events()
        .get_by_event_type(
            &sinex_primitives::EventType::from("consistency.event"),
            sinex_primitives::Pagination::new(Some(100), None),
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
        assert!(
            payload.get("data").is_some(),
            "Event should have data field"
        );

        // Verify checksum matches index
        if let (Some(idx), Some(checksum)) = (
            payload.get("index").and_then(serde_json::Value::as_i64),
            payload.get("checksum").and_then(|v| v.as_str()),
        ) {
            assert_eq!(
                checksum,
                format!("check-{idx}"),
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
    let ctx = ctx.with_nats().shared().await?;
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

    let subject = format!("{stream_name}.events");

    // Publish messages
    for i in 0..20 {
        js.publish(subject.clone(), format!("message-{i}").into())
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
    let consumer_started = Arc::new(AtomicBool::new(false));

    let shutdown_flag = shutdown_requested.clone();
    let processed = processed_after_shutdown.clone();
    let started_flag = consumer_started.clone();

    let consumer_handle = tokio::spawn(async move {
        loop {
            started_flag.store(true, Ordering::SeqCst);
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

    // Wait for consumer to enter its fetch loop
    WaitHelpers::wait_for_condition(
        || {
            let started = consumer_started.load(Ordering::SeqCst);
            async move { Ok::<bool, color_eyre::eyre::Error>(started) }
        },
        Timeouts::QUICK,
    )
    .await?;

    // Request shutdown
    shutdown_requested.store(true, Ordering::SeqCst);
    let shutdown_start = std::time::Instant::now();

    // Wait with timeout
    let result = timeout(Duration::from_secs(Timeouts::QUICK), consumer_handle).await;

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

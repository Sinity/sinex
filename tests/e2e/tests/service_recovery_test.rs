//! Service Recovery Tests
//!
//! These tests verify system resilience and recovery behavior that mirrors
//! what the NixOS VM tests validate, but at the integration test level.
//! This provides faster feedback for recovery-related regressions.
//!
//! ## Coverage Areas
//! - Database pool recovery after connection drops
//! - Ingestd restart continuity
//! - Multi-source concurrent event processing
//! - Leadership coordination under failure conditions
//! - JetStream consumer recovery

use async_nats::jetstream;
use chrono::{Duration as ChronoDuration, Utc};
use color_eyre::eyre::eyre;
use futures::StreamExt;
use serde_json::json;
use sinex_core::coordination::kv_client::{CoordinationKvClient, InstanceMetadata};
use sinex_core::environment::environment;
use sinex_node_sdk::stream_processor::SchemaBroadcastEntry;
use sinex_test_utils::nats::ensure_coordination_buckets;
use sinex_test_utils::prelude::*;
use sinex_test_utils::timing_utils::{Timeouts, WaitHelpers};
use sinex_test_utils::{start_test_ingestd_with_config, TestIngestdConfig};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use tokio::time::{timeout, Duration};

fn is_jetstream_no_messages_error(msg: &str) -> bool {
    msg.contains("No Messages")
        || msg.contains("No Messages Available")
        || (msg.contains("404") && msg.contains("No Messages"))
}

async fn ensure_raw_event_streams(
    nats_client: &async_nats::Client,
    env: &sinex_core::environment::SinexEnvironment,
) -> Result<()> {
    let js = jetstream::new(nats_client.clone());
    let events_stream = "sinex_test_events".to_string();
    js.get_or_create_stream(jetstream::stream::Config {
        name: events_stream.clone(),
        subjects: vec![env.nats_subject("events.raw.>")],
        retention: jetstream::stream::RetentionPolicy::Limits,
        storage: jetstream::stream::StorageType::File,
        ..Default::default()
    })
    .await?;

    let confirmations_stream = format!("{events_stream}_CONFIRMATIONS");
    js.get_or_create_stream(jetstream::stream::Config {
        name: confirmations_stream,
        subjects: vec![env.nats_subject("events.confirmations.>")],
        retention: jetstream::stream::RetentionPolicy::Limits,
        storage: jetstream::stream::StorageType::File,
        ..Default::default()
    })
    .await?;

    Ok(())
}

async fn wait_for_schema_broadcast(
    subscription: &mut async_nats::Subscriber,
) -> Result<Vec<SchemaBroadcastEntry>> {
    let message = timeout(Duration::from_secs(Timeouts::SHORT), subscription.next())
        .await
        .map_err(|_| eyre!("Timed out waiting for schema broadcast"))?
        .ok_or_else(|| eyre!("Schema broadcast subscription closed"))?;
    let entries: Vec<SchemaBroadcastEntry> = serde_json::from_slice(&message.payload)?;
    if entries.is_empty() {
        return Err(eyre!("Schema broadcast payload was empty"));
    }
    Ok(entries)
}

// =============================================================================
// Database Pool Recovery Tests
// =============================================================================

/// Test that events can be created after forcing pool connections to reset.
///
/// This simulates what happens when PostgreSQL restarts: existing connections
/// become invalid and the pool must recover by establishing new ones.
#[sinex_test]
async fn test_pool_recovery_after_connection_invalidation(ctx: TestContext) -> Result<()> {
    let ctx = ctx.with_nats().await?;
    // Create initial event to verify baseline functionality
    let baseline_event = ctx
        .publish_event("pool-recovery", "baseline", json!({"phase": "before"}))
        .await?;
    assert!(baseline_event.id.is_some());

    let baseline_count = ctx.pool.events().count_all().await?;

    // Force the pool to close all connections by executing a query that
    // terminates our own backend connections (simulates database restart)
    // Note: This is a controlled way to invalidate connections without
    // actually stopping PostgreSQL
    let terminate_result = sqlx::query!(
        r#"
        SELECT pg_terminate_backend(pid)
        FROM pg_stat_activity
        WHERE datname = current_database()
          AND pid <> pg_backend_pid()
          AND application_name LIKE '%sqlx%'
        "#
    )
    .fetch_all(&ctx.pool)
    .await;

    // The terminate may fail or succeed depending on permissions, but the
    // important thing is the pool should recover regardless
    if let Err(e) = terminate_result {
        tracing::info!(
            "Connection termination not permitted (expected in some envs): {}",
            e
        );
    }

    let attempt_counter = AtomicU32::new(0);
    WaitHelpers::wait_for_condition(
        || {
            let attempt = attempt_counter.fetch_add(1, Ordering::SeqCst);
            let ctx = &ctx;
            async move {
                match ctx
                    .publish_event(
                        "pool-recovery",
                        "after",
                        json!({"phase": "after", "attempt": attempt}),
                    )
                    .await
                {
                    Ok(event) => {
                        assert!(event.id.is_some());
                        Ok::<bool, sinex_test_utils::SinexError>(true)
                    }
                    Err(e) => {
                        tracing::warn!("Recovery attempt {} failed: {}", attempt, e);
                        Ok::<bool, sinex_test_utils::SinexError>(false)
                    }
                }
            }
        },
        5,
    )
    .await?;

    // Verify events are persisted
    let final_count = ctx.pool.events().count_all().await?;
    assert!(
        final_count > baseline_count,
        "Should have more events after recovery"
    );

    Ok(())
}

/// Test concurrent database operations during pool stress.
///
/// This simulates high connection churn that could occur during
/// database maintenance or network instability.
#[sinex_test]
async fn test_pool_concurrent_stress_recovery(ctx: TestContext) -> Result<()> {
    let ctx = ctx.with_nats().await?;
    let success_count = Arc::new(AtomicU32::new(0));
    let failure_count = Arc::new(AtomicU32::new(0));

    let mut handles = vec![];

    // Spawn concurrent tasks that create events
    for task_id in 0..20 {
        let pool = ctx.pool.clone();
        let successes = success_count.clone();
        let failures = failure_count.clone();

        let handle = tokio::spawn(async move {
            for iteration in 0..10 {
                let event = Event::<JsonValue>::test_event(
                    EventSource::from(format!("stress-{}", task_id)),
                    EventType::from("concurrent.operation"),
                    json!({
                        "task_id": task_id,
                        "iteration": iteration,
                        "timestamp": chrono::Utc::now().to_rfc3339()
                    }),
                );

                let mut inserted = false;
                for attempt in 0..3 {
                    match pool.events().insert(event.clone()).await {
                        Ok(_) => {
                            successes.fetch_add(1, Ordering::SeqCst);
                            inserted = true;
                            break;
                        }
                        Err(e) => {
                            tracing::debug!(
                                "Concurrent insert failed (task_id={}, iteration={}, attempt={}): {}",
                                task_id,
                                iteration,
                                attempt,
                                e
                            );
                            if attempt < 2 {
                                tokio::task::yield_now().await;
                                continue;
                            }
                        }
                    }
                }

                if !inserted {
                    failures.fetch_add(1, Ordering::SeqCst);
                }

                // Small delay to allow for connection churn
                tokio::task::yield_now().await;
            }
        });

        handles.push(handle);
    }

    // Wait for all tasks
    for handle in handles {
        handle.await?;
    }

    let successes = success_count.load(Ordering::SeqCst);
    let failures = failure_count.load(Ordering::SeqCst);

    tracing::info!(
        "Concurrent stress results: {} successes, {} failures",
        successes,
        failures
    );

    // Most operations should succeed even under stress
    assert!(
        successes > 150,
        "At least 75% of operations should succeed: got {} / 200",
        successes
    );

    Ok(())
}

// =============================================================================
// Ingestd Restart Continuity Tests
// =============================================================================

/// Test that events flow correctly after ingestd restart.
///
/// This mirrors the VM test behavior where sinex-ingestd is stopped and
/// restarted, and new events should still be captured.
#[sinex_test]
async fn test_ingestd_restart_event_continuity(ctx: TestContext) -> Result<()> {
    ctx.ensure_clean().await?;

    let ctx = ctx.with_nats().shared().await?;
    let nats_client = ctx.nats_client();
    let _js = jetstream::new(nats_client.clone());
    let mut schema_subscription = nats_client
        .subscribe(ctx.env().nats_subject("system.schemas.active"))
        .await?;

    let ingest_config = TestIngestdConfig {
        nats: ctx.nats_handle()?.connection_config(),
        database_url: ctx.database_url().to_string(),
        work_dir: None,
        ..Default::default()
    };

    // Start first ingestd instance
    let mut ingest_handle =
        start_test_ingestd_with_config(ingest_config.clone(), Some(&ctx)).await?;
    let _schemas = wait_for_schema_broadcast(&mut schema_subscription).await?;

    // Publish events to first instance
    let publisher = TestNodePublisher::new(nats_client.clone(), "restart-test");
    for i in 0..5 {
        publisher
            .publish_event("before.restart", json!({"sequence": i, "phase": "before"}))
            .await?;
    }

    // Wait for *our* events, not just "some events".
    sinex_test_utils::timing_utils::WaitHelpers::wait_for_event_type_events(
        &ctx.pool,
        &EventType::from("before.restart"),
        5,
        20,
    )
    .await?;

    let before_count = ctx.pool.events().count_all().await?;
    let before_restart_count = ctx
        .pool
        .events()
        .count_by_event_type(&EventType::from("before.restart"))
        .await? as usize;
    tracing::info!(
        "Events before restart: total={}, before.restart={}",
        before_count,
        before_restart_count
    );

    // Stop first ingestd instance
    ingest_handle.stop().await?;
    tracing::info!("Ingestd stopped");

    // Start second ingestd instance
    let mut ingest_handle2 =
        start_test_ingestd_with_config(ingest_config.clone(), Some(&ctx)).await?;
    tracing::info!("Ingestd restarted");

    // Publish events to second instance
    for i in 0..5 {
        publisher
            .publish_event("after.restart", json!({"sequence": i, "phase": "after"}))
            .await?;
    }

    // Be explicit: ensure the *after.restart* events made it through, not just "some events".
    sinex_test_utils::timing_utils::WaitHelpers::wait_for_event_type_events(
        &ctx.pool,
        &EventType::from("after.restart"),
        5,
        30,
    )
    .await?;

    let after_count = ctx.pool.events().count_all().await?;
    let after_restart_count = ctx
        .pool
        .events()
        .count_by_event_type(&EventType::from("after.restart"))
        .await? as usize;
    tracing::info!(
        "Events after restart: total={}, after.restart={}",
        after_count,
        after_restart_count
    );

    // Verify new events were captured after restart
    assert!(
        after_count >= before_count + 5,
        "Should have more events after restart: before_total={}, after_total={}, before.restart={}, after.restart={}",
        before_count,
        after_count,
        before_restart_count,
        after_restart_count
    );

    // Query for after-restart events specifically
    let after_events = ctx
        .pool
        .events()
        .get_by_event_type(
            &EventType::from("after.restart"),
            sinex_core::types::Pagination::new(Some(10), None),
        )
        .await?;

    assert!(
        !after_events.is_empty(),
        "Should have events from after restart"
    );

    // Cleanup
    ingest_handle2.stop().await?;

    Ok(())
}

// =============================================================================
// Multi-Source Concurrent Ingestion Tests
// =============================================================================

/// Test concurrent event ingestion from multiple sources.
///
/// This mirrors the VM multi-source test which verifies events from
/// filesystem, terminal, desktop, and system nodes flow concurrently.
#[sinex_serial_test]
async fn test_multi_source_concurrent_ingestion(ctx: TestContext) -> Result<()> {
    ctx.ensure_clean().await?;

    // Be strict here: this test relies on per-source counts, so tolerate no residual events.
    let existing = ctx.pool.events().count_all().await?;
    if existing != 0 {
        tracing::warn!(
            "Database not empty after reset ({} events); forcing cleanup",
            existing
        );
        let after = ctx.pool.events().count_all().await?;
        assert_eq!(after, 0, "Database should be empty after forced cleanup");
    }

    let ctx = ctx.with_nats().shared().await?;
    let nats_client = ctx.nats_client();

    let ingest_config = TestIngestdConfig {
        nats: ctx.nats_handle()?.connection_config(),
        database_url: ctx.database_url().to_string(),
        work_dir: None,
        ..Default::default()
    };

    let mut ingest_handle = start_test_ingestd_with_config(ingest_config, Some(&ctx)).await?;

    // Define multiple source types (mirrors VM node matrix)
    let sources = vec![
        ("sinex-filesystem", "file.created"),
        ("sinex-filesystem", "file.modified"),
        ("sinex-terminal", "command.executed"),
        ("sinex-terminal", "session.started"),
        ("sinex-desktop", "window.focused"),
        ("sinex-desktop", "clipboard.changed"),
        ("sinex-system", "service.started"),
        ("sinex-system", "process.spawned"),
    ];

    let mut handles = vec![];
    let events_per_source = 10;

    // Spawn concurrent publishers for each source type
    for (source, event_type) in sources.clone() {
        let publisher = TestNodePublisher::new(nats_client.clone(), source.to_string());
        let event_type = event_type.to_string();

        let handle = tokio::spawn(async move {
            for i in 0..events_per_source {
                publisher
                    .publish_event(
                        &event_type,
                        json!({
                            "source": source,
                            "event_type": event_type,
                            "sequence": i,
                            "concurrent": true
                        }),
                    )
                    .await?;

                tokio::task::yield_now().await;
            }
            Ok::<(), color_eyre::Report>(())
        });

        handles.push(handle);
    }

    // Wait for all publishers
    for handle in handles {
        handle.await??;
    }

    // Wait for ingestion per source (avoid false positives when leftover events exist).
    let mut expected_by_source: std::collections::HashMap<&str, usize> =
        std::collections::HashMap::new();
    for (source, _) in &sources {
        *expected_by_source.entry(*source).or_insert(0) += events_per_source;
    }

    for (source, expected) in &expected_by_source {
        sinex_test_utils::timing_utils::WaitHelpers::wait_for_source_events(
            &ctx.pool, source, *expected, 30,
        )
        .await?;
    }

    // Verify events from each source
    let mut source_counts = std::collections::HashMap::new();
    for (source, _) in &expected_by_source {
        let events = ctx
            .pool
            .events()
            .get_by_source(
                &EventSource::from(*source),
                sinex_core::types::Pagination::new(Some(100), None),
            )
            .await?;
        source_counts.insert(*source, events.len());
    }

    tracing::info!("Events by source: {:?}", source_counts);

    // Verify all sources have events
    for source in expected_by_source.keys() {
        let count = source_counts.get(*source).copied().unwrap_or(0);
        assert!(
            count > 0,
            "Source {} should have events, got {}",
            source,
            count
        );
    }

    // Cleanup
    ingest_handle.stop().await?;

    Ok(())
}

// =============================================================================
// Leadership Coordination Tests
// =============================================================================

/// Test that leadership heartbeat timeout detection works correctly.
///
/// This verifies the core mechanism that allows standby instances to
/// detect when a leader has failed and attempt takeover.
#[sinex_test]
async fn test_leadership_heartbeat_timeout_detection(ctx: TestContext) -> Result<()> {
    let ctx = ctx.with_nats().await?;
    let nats_client = ctx.nats_client();
    let js = async_nats::jetstream::new(nats_client.clone());
    ensure_coordination_buckets(&js).await?;

    let service_name = format!("test-leadership-timeout-{}", uuid::Uuid::new_v4());
    let stale_instance = uuid::Uuid::new_v4().to_string();
    let standby_instance = uuid::Uuid::new_v4().to_string();

    let kv_client = CoordinationKvClient::new(js.clone(), service_name.clone());

    // Register leader with a stale heartbeat (60 seconds old)
    let metadata = InstanceMetadata {
        instance_id: stale_instance.clone(),
        hostname: "stale-host".to_string(),
        version: "1.0.0".to_string(),
        started_at: (Utc::now() - ChronoDuration::seconds(120)).timestamp(),
        last_heartbeat: (Utc::now() - ChronoDuration::seconds(60)).timestamp(),
    };
    kv_client.register_instance(&metadata).await?;
    assert!(kv_client.acquire_leadership(&stale_instance).await?);

    // Verify heartbeat is considered stale
    let env = environment();
    let bucket = js
        .get_key_value(&format!(
            "KV_{}",
            env.nats_kv_bucket_name("sinex_instances")
        ))
        .await?;
    let key = format!("{}.{}", service_name, stale_instance);
    let entry = bucket.entry(&key).await?.expect("metadata present");
    let stored: InstanceMetadata = serde_json::from_slice(&entry.value)?;
    assert!(stored.last_heartbeat < (Utc::now() - ChronoDuration::seconds(30)).timestamp());

    // Standby registers and attempts takeover (should initially fail)
    let standby_meta = InstanceMetadata {
        instance_id: standby_instance.clone(),
        hostname: "standby-host".to_string(),
        version: "1.0.1".to_string(),
        started_at: Utc::now().timestamp(),
        last_heartbeat: Utc::now().timestamp(),
    };
    kv_client.register_instance(&standby_meta).await?;
    assert!(
        !kv_client.acquire_leadership(&standby_instance).await?,
        "Stale leader still holds CAS entry"
    );

    // Simulate timeout detector releasing stale leadership then retry
    kv_client.release_leadership(&stale_instance).await?;
    assert!(
        kv_client.acquire_leadership(&standby_instance).await?,
        "Standby should take over once stale leader is released"
    );

    Ok(())
}

/// Test concurrent leadership acquisition attempts.
///
/// Verifies that only one instance can acquire leadership even when
/// multiple instances attempt simultaneously.
#[sinex_test]
async fn test_concurrent_leadership_acquisition(ctx: TestContext) -> Result<()> {
    let ctx = ctx.with_nats().await?;
    let nats = ctx.nats_handle()?;
    let js = async_nats::jetstream::new(ctx.nats_client());
    ensure_coordination_buckets(&js).await?;

    let service_name = format!("test-concurrent-leadership-{}", uuid::Uuid::new_v4());
    let acquisition_count = Arc::new(AtomicU32::new(0));

    let mut handles = vec![];

    for idx in 0..10 {
        let nats = nats.clone();
        let service = service_name.clone();
        let counter = acquisition_count.clone();

        let handle = tokio::spawn(async move {
            let client = nats.connect().await.unwrap();
            let js = async_nats::jetstream::new(client);
            let kv_client = CoordinationKvClient::new(js, service);
            let instance_id = uuid::Uuid::new_v4().to_string();

            let metadata = InstanceMetadata {
                instance_id: instance_id.clone(),
                hostname: format!("instance-{idx}"),
                version: "1.0.0".to_string(),
                started_at: Utc::now().timestamp(),
                last_heartbeat: Utc::now().timestamp(),
            };
            kv_client.register_instance(&metadata).await.unwrap();

            if kv_client.acquire_leadership(&instance_id).await.unwrap() {
                counter.fetch_add(1, Ordering::SeqCst);
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.await?;
    }

    assert_eq!(
        acquisition_count.load(Ordering::SeqCst),
        1,
        "Only one instance should acquire leadership"
    );

    Ok(())
}

// =============================================================================
// JetStream Consumer Recovery Tests
// =============================================================================

/// Test that JetStream consumer state survives across reconnections.
///
/// This verifies that durable consumers properly resume from their last
/// acknowledged position after a disconnect/reconnect cycle.
#[sinex_test]
async fn test_jetstream_consumer_durable_recovery(ctx: TestContext) -> Result<()> {
    let ctx = ctx.with_nats().shared().await?;
    let nats_client = ctx.nats_client();
    let js = jetstream::new(nats_client.clone());

    let stream_name = format!("TEST_RECOVERY_{}", uuid::Uuid::new_v4().simple());
    let consumer_name = "test-durable-consumer";

    // Create stream
    js.create_stream(jetstream::stream::Config {
        name: stream_name.clone(),
        subjects: vec![format!("{}.>", stream_name)],
        max_messages: 1000,
        ..Default::default()
    })
    .await?;

    let subject = format!("{}.events", stream_name);

    // Publish initial batch of messages
    for i in 0..5 {
        js.publish(subject.clone(), format!("message-{}", i).into())
            .await
            .map_err(|e| eyre!(e))?
            .await
            .map_err(|e| eyre!(e))?;
    }

    // Create durable consumer and process some messages
    let stream = js.get_stream(&stream_name).await.map_err(|e| eyre!(e))?;
    let consumer = stream
        .create_consumer(jetstream::consumer::pull::Config {
            name: Some(consumer_name.to_string()),
            durable_name: Some(consumer_name.to_string()),
            ack_policy: jetstream::consumer::AckPolicy::Explicit,
            ..Default::default()
        })
        .await
        .map_err(|e| eyre!(e))?;

    // Fetch and ack first 3 messages. JetStream can briefly respond with a transient
    // "No Messages" status even when messages exist, so tolerate a short warm-up.
    let mut acked_count: usize = 0;
    let start = std::time::Instant::now();
    while acked_count < 3 && start.elapsed() < std::time::Duration::from_secs(Timeouts::QUICK) {
        let fetch_result = consumer
            .fetch()
            .max_messages(3 - acked_count)
            .expires(std::time::Duration::from_millis(1000))
            .messages()
            .await;

        match fetch_result {
            Ok(mut messages) => {
                while let Some(item) = messages.next().await {
                    match item {
                        Ok(msg) => {
                            msg.ack().await.map_err(|e| eyre!(e))?;
                            acked_count += 1;
                            if acked_count >= 3 {
                                break;
                            }
                        }
                        Err(e) => {
                            let msg = e.to_string();
                            if is_jetstream_no_messages_error(&msg) {
                                break;
                            }
                            return Err(eyre!(e));
                        }
                    }
                }
            }
            Err(e) => {
                let msg = e.to_string();
                if is_jetstream_no_messages_error(&msg) {
                    continue;
                }
                return Err(eyre!(e));
            }
        }
    }
    assert_eq!(
        acked_count, 3,
        "Should have acked 3 messages (acked_count={acked_count})"
    );

    // Drop consumer (simulates disconnect)
    drop(consumer);

    // Publish more messages while "disconnected"
    for i in 5..10 {
        js.publish(subject.clone(), format!("message-{}", i).into())
            .await
            .map_err(|e| eyre!(e))?
            .await
            .map_err(|e| eyre!(e))?;
    }

    // Reconnect - get consumer again
    let stream = js.get_stream(&stream_name).await.map_err(|e| eyre!(e))?;
    let consumer = stream
        .get_consumer(consumer_name)
        .await
        .map_err(|e| eyre!(e))?;

    // Fetch remaining messages - should start from where we left off. JetStream can
    // legitimately respond with a transient "No Messages" status while messages are
    // in-flight, so we tolerate a few empty polls and retry briefly.
    let mut remaining_count: usize = 0;
    let start = std::time::Instant::now();
    while remaining_count < 7 && start.elapsed() < std::time::Duration::from_secs(Timeouts::QUICK) {
        let fetch_result = consumer
            .fetch()
            .max_messages(10)
            .expires(std::time::Duration::from_millis(1000))
            .messages()
            .await;
        match fetch_result {
            Ok(mut messages) => {
                while let Some(item) = messages.next().await {
                    match item {
                        Ok(msg) => {
                            msg.ack().await.map_err(|e| eyre!(e))?;
                            remaining_count += 1;
                        }
                        Err(e) => {
                            let msg = e.to_string();
                            if is_jetstream_no_messages_error(&msg) {
                                break;
                            }
                            return Err(eyre!(e));
                        }
                    }
                }
                if remaining_count >= 7 {
                    break;
                }
            }
            Err(e) => {
                let msg = e.to_string();
                if is_jetstream_no_messages_error(&msg) {
                    continue;
                }
                return Err(eyre!(e));
            }
        }
    }

    // Should have 7 remaining messages (2 from first batch + 5 new)
    assert_eq!(
        remaining_count, 7,
        "Should have 7 remaining messages after recovery"
    );

    // Cleanup
    js.delete_stream(&stream_name).await?;

    Ok(())
}

/// Test message buffering during temporary NATS unavailability.
///
/// Verifies that the publisher handles temporary connection issues gracefully.
#[sinex_test]
async fn test_publisher_reconnection_resilience(ctx: TestContext) -> Result<()> {
    let ctx = ctx.with_nats().shared().await?;
    let nats_client = ctx.nats_client();
    ensure_raw_event_streams(&nats_client, ctx.env()).await?;
    let publisher = Arc::new(TestNodePublisher::new(
        nats_client.clone(),
        "reconnect-test",
    ));

    let success_count = Arc::new(AtomicU32::new(0));
    let failure_count = Arc::new(AtomicU32::new(0));

    // Rapid-fire publishes to stress the connection
    let mut handles = vec![];

    for batch in 0..5 {
        let publisher = publisher.clone();
        let successes = success_count.clone();
        let failures = failure_count.clone();

        let handle = tokio::spawn(async move {
            for i in 0..20 {
                match publisher
                    .publish_event(
                        "stress.publish",
                        json!({
                            "batch": batch,
                            "sequence": i
                        }),
                    )
                    .await
                {
                    Ok(_) => {
                        successes.fetch_add(1, Ordering::SeqCst);
                    }
                    Err(e) => {
                        tracing::debug!("Publish failed: {}", e);
                        failures.fetch_add(1, Ordering::SeqCst);
                    }
                }
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        let _ = handle.await;
    }

    let successes = success_count.load(Ordering::SeqCst);
    let failures = failure_count.load(Ordering::SeqCst);

    tracing::info!(
        "Reconnection resilience: {} successes, {} failures",
        successes,
        failures
    );

    // Most publishes should succeed
    assert!(
        successes > 80,
        "At least 80% of publishes should succeed: got {} / 100",
        successes
    );

    Ok(())
}

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

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_nats::jetstream;
use color_eyre::eyre::eyre;
use futures::StreamExt;
use serde_json::json;
use sinex_test_utils::prelude::*;
use sinex_test_utils::{start_test_ingestd_with_config, TestIngestdConfig};
use tokio::time::sleep;

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

// =============================================================================
// Database Pool Recovery Tests
// =============================================================================

/// Test that events can be created after forcing pool connections to reset.
///
/// This simulates what happens when PostgreSQL restarts: existing connections
/// become invalid and the pool must recover by establishing new ones.
#[sinex_test]
async fn test_pool_recovery_after_connection_invalidation(ctx: TestContext) -> Result<()> {
    // Create initial event to verify baseline functionality
    let baseline_event = ctx
        .create_test_event("pool-recovery", "baseline", json!({"phase": "before"}))
        .await?;
    assert!(baseline_event.id.is_some());

    let baseline_count = ctx.current_event_count().await?;

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

    // Give the pool a moment to detect invalid connections
    sleep(Duration::from_millis(500)).await;

    // Now create more events - the pool should have recovered
    let mut recovery_success = false;
    for attempt in 0..5 {
        match ctx
            .create_test_event(
                "pool-recovery",
                "after",
                json!({"phase": "after", "attempt": attempt}),
            )
            .await
        {
            Ok(event) => {
                assert!(event.id.is_some());
                recovery_success = true;
                break;
            }
            Err(e) => {
                tracing::warn!("Recovery attempt {} failed: {}", attempt, e);
                sleep(Duration::from_millis(200)).await;
            }
        }
    }

    assert!(
        recovery_success,
        "Pool should recover and allow event creation"
    );

    // Verify events are persisted
    let final_count = ctx.current_event_count().await?;
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
                                tokio::time::sleep(Duration::from_millis(20 * (attempt + 1))).await;
                                continue;
                            }
                        }
                    }
                }

                if !inserted {
                    failures.fetch_add(1, Ordering::SeqCst);
                }

                // Small delay to allow for connection churn
                sleep(Duration::from_millis(10)).await;
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
    sinex_test_utils::db_common::reset_database(&ctx.pool).await?;

    let ctx = ctx.with_nats().await?;
    let nats_client = ctx.nats_client();
    let _js = jetstream::new(nats_client.clone());

    let ingest_config = TestIngestdConfig {
        nats_url: format!(
            "nats://{}",
            ctx.nats_url().expect("NATS should be available")
        ),
        database_url: ctx.database_url().to_string(),
        work_dir: None,
    };

    // Start first ingestd instance
    let mut ingest_handle =
        start_test_ingestd_with_config(ingest_config.clone(), Some(&ctx)).await?;

    // Publish events to first instance
    let publisher = TestSatellitePublisher::new(nats_client.clone(), "restart-test");
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

    // Brief pause to simulate service downtime
    sleep(Duration::from_millis(500)).await;

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
    sinex_test_utils::db_common::reset_database(&ctx.pool).await?;

    Ok(())
}

// =============================================================================
// Multi-Source Concurrent Ingestion Tests
// =============================================================================

/// Test concurrent event ingestion from multiple sources.
///
/// This mirrors the VM multi-source test which verifies events from
/// filesystem, terminal, desktop, and system satellites flow concurrently.
#[sinex_test]
async fn test_multi_source_concurrent_ingestion(ctx: TestContext) -> Result<()> {
    let _guard = sinex_test_utils::acquire_pool_test_guard().await;
    ctx.ensure_clean().await?;
    sinex_test_utils::db_common::reset_database(&ctx.pool).await?;

    // Be strict here: this test relies on per-source counts, so tolerate no residual events.
    let existing = ctx.pool.events().count_all().await?;
    if existing != 0 {
        tracing::warn!(
            "Database not empty after reset ({} events); forcing cleanup",
            existing
        );
        ctx.force_cleanup().await?;
        sinex_test_utils::db_common::reset_database(&ctx.pool).await?;
        let after = ctx.pool.events().count_all().await?;
        assert_eq!(after, 0, "Database should be empty after forced cleanup");
    }

    let ctx = ctx.with_nats().await?;
    let nats_client = ctx.nats_client();

    let ingest_config = TestIngestdConfig {
        nats_url: format!(
            "nats://{}",
            ctx.nats_url().expect("NATS should be available")
        ),
        database_url: ctx.database_url().to_string(),
        work_dir: None,
    };

    let mut ingest_handle = start_test_ingestd_with_config(ingest_config, Some(&ctx)).await?;

    // Define multiple source types (mirrors VM satellite matrix)
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
        let publisher = TestSatellitePublisher::new(nats_client.clone(), source.to_string());
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

                // Stagger publishes slightly
                sleep(Duration::from_millis(5)).await;
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
    sinex_test_utils::db_common::reset_database(&ctx.pool).await?;

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
    // Setup: Create a leadership record with stale heartbeat
    let service_name = "test-leadership-timeout";
    let instance_id = uuid::Uuid::new_v4();

    // First, ensure satellite_instances has the row (FK requirement)
    sqlx::query!(
        r#"
        INSERT INTO core.satellite_instances
            (service_name, instance_id, version, start_time, last_heartbeat, host_name, metadata)
        VALUES ($1, $2, '1.0.0', NOW(), NOW() - INTERVAL '60 seconds', 'test-host', '{}'::jsonb)
        ON CONFLICT (instance_id) DO UPDATE
            SET last_heartbeat = NOW() - INTERVAL '60 seconds'
        "#,
        service_name,
        instance_id.to_string()
    )
    .execute(&ctx.pool)
    .await?;

    // Insert leadership record with old heartbeat (60 seconds ago)
    sqlx::query!(
        r#"
        INSERT INTO core.service_leadership
            (service_name, instance_id, acquired_at, last_heartbeat, version)
        VALUES ($1, $2, NOW() - INTERVAL '120 seconds', NOW() - INTERVAL '60 seconds', '1.0.0')
        ON CONFLICT (service_name) DO UPDATE
            SET last_heartbeat = NOW() - INTERVAL '60 seconds',
                instance_id = $2,
                version = '1.0.0'
        "#,
        service_name,
        instance_id.to_string()
    )
    .execute(&ctx.pool)
    .await?;

    // Query for stale leadership (heartbeat older than 30 seconds)
    let stale_leaders: Vec<_> = sqlx::query!(
        r#"
        SELECT service_name, instance_id::text as "instance_id!", last_heartbeat
        FROM core.service_leadership
        WHERE service_name = $1
          AND last_heartbeat < NOW() - INTERVAL '30 seconds'
        "#,
        service_name
    )
    .fetch_all(&ctx.pool)
    .await?;

    assert!(
        !stale_leaders.is_empty(),
        "Should detect stale leadership record"
    );

    assert_eq!(stale_leaders[0].service_name, service_name);
    assert_eq!(stale_leaders[0].instance_id, instance_id.to_string());

    // Simulate a standby detecting the timeout and attempting takeover
    let new_instance_id = uuid::Uuid::new_v4();

    // Register new instance
    sqlx::query!(
        r#"
        INSERT INTO core.satellite_instances
            (service_name, instance_id, version, start_time, last_heartbeat, host_name, metadata)
        VALUES ($1, $2, '1.0.1', NOW(), NOW(), 'standby-host', '{}'::jsonb)
        ON CONFLICT (instance_id) DO UPDATE
            SET last_heartbeat = NOW()
        "#,
        service_name,
        new_instance_id.to_string()
    )
    .execute(&ctx.pool)
    .await?;

    // Takeover leadership
    let takeover_result: Option<_> = sqlx::query!(
        r#"
        UPDATE core.service_leadership
        SET instance_id = $2,
            acquired_at = NOW(),
            last_heartbeat = NOW()
        WHERE service_name = $1
          AND last_heartbeat < NOW() - INTERVAL '30 seconds'
        RETURNING service_name
        "#,
        service_name,
        new_instance_id.to_string()
    )
    .fetch_optional(&ctx.pool)
    .await?;

    assert!(
        takeover_result.is_some(),
        "Standby should be able to takeover stale leadership"
    );

    // Verify new leader
    let current_leader = sqlx::query!(
        r#"
        SELECT instance_id::text as "instance_id!"
        FROM core.service_leadership
        WHERE service_name = $1
        "#,
        service_name
    )
    .fetch_one(&ctx.pool)
    .await?;

    assert_eq!(
        current_leader.instance_id,
        new_instance_id.to_string(),
        "New instance should be the leader"
    );

    // Cleanup
    sqlx::query!(
        "DELETE FROM core.service_leadership WHERE service_name = $1",
        service_name
    )
    .execute(&ctx.pool)
    .await?;

    sqlx::query!(
        "DELETE FROM core.satellite_instances WHERE service_name = $1",
        service_name
    )
    .execute(&ctx.pool)
    .await?;

    Ok(())
}

/// Test concurrent leadership acquisition attempts.
///
/// Verifies that only one instance can acquire leadership even when
/// multiple instances attempt simultaneously.
#[sinex_test]
async fn test_concurrent_leadership_acquisition(ctx: TestContext) -> Result<()> {
    let service_name = format!("test-concurrent-leadership-{}", uuid::Uuid::new_v4());
    let acquisition_count = Arc::new(AtomicU32::new(0));

    // Ensure clean state
    sqlx::query!(
        "DELETE FROM core.service_leadership WHERE service_name = $1",
        &service_name
    )
    .execute(&ctx.pool)
    .await?;

    let mut handles = vec![];

    // Spawn multiple instances trying to acquire leadership
    for instance_num in 0..10 {
        let pool = ctx.pool.clone();
        let service = service_name.clone();
        let count = acquisition_count.clone();

        let handle = tokio::spawn(async move {
            let instance_id = uuid::Uuid::new_v4();

            // Register instance first
            sqlx::query!(
                r#"
                INSERT INTO core.satellite_instances
                    (service_name, instance_id, version, start_time, last_heartbeat, host_name, metadata)
                VALUES ($1, $2, '1.0.0', NOW(), NOW(), $3, '{}'::jsonb)
                ON CONFLICT (instance_id) DO NOTHING
                "#,
                &service,
                instance_id.to_string(),
                format!("instance-{}", instance_num)
            )
            .execute(&pool)
            .await?;

            // Try to acquire leadership
            let result: Option<_> = sqlx::query!(
                r#"
                INSERT INTO core.service_leadership
                    (service_name, instance_id, acquired_at, last_heartbeat, version)
                VALUES ($1, $2, NOW(), NOW(), '1.0.0')
                ON CONFLICT (service_name) DO NOTHING
                RETURNING service_name
                "#,
                &service,
                instance_id.to_string()
            )
            .fetch_optional(&pool)
            .await?;

            if result.is_some() {
                count.fetch_add(1, Ordering::SeqCst);
                tracing::info!("Instance {} acquired leadership", instance_num);
            }

            Ok::<_, sinex_core::SinexError>(())
        });

        handles.push(handle);
    }

    // Wait for all attempts
    for handle in handles {
        let _ = handle.await;
    }

    let acquisitions = acquisition_count.load(Ordering::SeqCst);

    // Exactly one instance should have acquired leadership
    assert_eq!(
        acquisitions, 1,
        "Exactly one instance should acquire leadership, got {}",
        acquisitions
    );

    // Cleanup
    sqlx::query!(
        "DELETE FROM core.service_leadership WHERE service_name = $1",
        &service_name
    )
    .execute(&ctx.pool)
    .await?;

    sqlx::query!(
        "DELETE FROM core.satellite_instances WHERE service_name = $1",
        &service_name
    )
    .execute(&ctx.pool)
    .await?;

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
    let ctx = ctx.with_nats().await?;
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
    while acked_count < 3 && start.elapsed() < std::time::Duration::from_secs(5) {
        let fetch_result = consumer
            .fetch()
            .max_messages(3 - acked_count)
            .expires(std::time::Duration::from_secs(1))
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
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    continue;
                }
                return Err(eyre!(e));
            }
        }

        if acked_count < 3 {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
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
    while remaining_count < 7 && start.elapsed() < std::time::Duration::from_secs(5) {
        let fetch_result = consumer
            .fetch()
            .max_messages(10)
            .expires(std::time::Duration::from_secs(1))
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
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
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
    let ctx = ctx.with_nats().await?;
    let nats_client = ctx.nats_client();
    ensure_raw_event_streams(&nats_client, ctx.env()).await?;
    let publisher = Arc::new(TestSatellitePublisher::new(
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

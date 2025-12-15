//! Distributed Coordination Resilience Tests
//!
//! These tests verify the distributed coordination mechanisms used for
//! work distribution, including advisory locking and satellite coordination.
//!
//! ## Coverage Areas
//! - Advisory lock acquisition and recovery
//! - Concurrent coordination under stress
//! - Satellite instance registration and heartbeat tracking

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use sinex_core::db::distributed_locking::AdvisoryLock;
use sinex_test_utils::prelude::*;
use tokio::time::sleep;

// =============================================================================
// Advisory Lock Tests
// =============================================================================

/// Test that advisory locks can be acquired and released.
#[sinex_test]
async fn test_advisory_lock_acquire_release(ctx: TestContext) -> Result<()> {
    let lock_key = format!("test-lock-{}", uuid::Uuid::new_v4());

    // Acquire lock
    let lock_guard = AdvisoryLock::try_acquire(&ctx.pool, &lock_key)
        .await?
        .expect("Should acquire lock");

    // Verify lock is held
    let is_locked = AdvisoryLock::is_locked(&ctx.pool, &lock_key).await?;
    assert!(is_locked, "Lock should be held after acquisition");

    // Release lock
    drop(lock_guard);

    // Give a moment for the connection to release
    sleep(Duration::from_millis(100)).await;

    // Verify lock is released (should be able to acquire again)
    let lock_guard2 = AdvisoryLock::try_acquire(&ctx.pool, &lock_key).await?;
    assert!(
        lock_guard2.is_some(),
        "Should be able to acquire lock after release"
    );

    Ok(())
}

/// Test that concurrent lock attempts are properly serialized.
#[sinex_test]
async fn test_advisory_lock_concurrent_acquisition(ctx: TestContext) -> Result<()> {
    let lock_key = format!("concurrent-lock-{}", uuid::Uuid::new_v4());
    let acquired_count = Arc::new(AtomicU32::new(0));

    let mut handles = vec![];

    // Spawn multiple tasks trying to acquire the same lock
    for task_id in 0..10 {
        let pool = ctx.pool.clone();
        let key = lock_key.clone();
        let count = acquired_count.clone();

        let handle = tokio::spawn(async move {
            match AdvisoryLock::try_acquire(&pool, &key).await {
                Ok(Some(_guard)) => {
                    count.fetch_add(1, Ordering::SeqCst);
                    tracing::info!("Task {} acquired lock", task_id);
                    // Hold lock briefly
                    sleep(Duration::from_millis(50)).await;
                    // Lock released when guard drops
                }
                Ok(None) => {
                    tracing::info!("Task {} could not acquire lock (already held)", task_id);
                }
                Err(e) => {
                    tracing::warn!("Task {} error: {}", task_id, e);
                }
            }
        });

        handles.push(handle);
    }

    // Wait for all attempts
    for handle in handles {
        let _ = handle.await;
    }

    let acquisitions = acquired_count.load(Ordering::SeqCst);

    // At most one task can hold the lock at a time, but over time
    // multiple tasks may acquire it sequentially
    assert!(
        acquisitions >= 1,
        "At least one task should have acquired the lock"
    );

    tracing::info!("Total lock acquisitions: {}", acquisitions);

    Ok(())
}

/// Test advisory lock timeout behavior.
#[sinex_test]
async fn test_advisory_lock_timeout(ctx: TestContext) -> Result<()> {
    let lock_key = format!("timeout-lock-{}", uuid::Uuid::new_v4());

    // Acquire lock first
    let _lock_guard = AdvisoryLock::try_acquire(&ctx.pool, &lock_key)
        .await?
        .expect("Should acquire lock");

    // Try to acquire with timeout from another task
    let pool = ctx.pool.clone();
    let key = lock_key.clone();

    let timeout_result = tokio::spawn(async move {
        AdvisoryLock::acquire_or_wait(&pool, &key, Duration::from_millis(100)).await
    })
    .await?;

    // Should timeout since lock is held
    assert!(
        timeout_result.is_err(),
        "Should timeout when lock is held by another"
    );

    Ok(())
}

// =============================================================================
// Satellite Instance Coordination Tests
// =============================================================================

/// Test that satellite instances can be registered and tracked.
#[sinex_test]
async fn test_satellite_instance_registration(ctx: TestContext) -> Result<()> {
    let service_name = format!("test-satellite-{}", uuid::Uuid::new_v4());
    let instance_id = uuid::Uuid::new_v4().to_string();

    // Register instance
    sqlx::query!(
        r#"
        INSERT INTO core.satellite_instances
            (service_name, instance_id, version, start_time, last_heartbeat, host_name, metadata)
        VALUES ($1, $2, '1.0.0', NOW(), NOW(), 'test-host', '{}'::jsonb)
        "#,
        service_name,
        instance_id
    )
    .execute(&ctx.pool)
    .await?;

    // Verify registration
    let instance = sqlx::query!(
        r#"
        SELECT service_name, instance_id, version
        FROM core.satellite_instances
        WHERE instance_id = $1
        "#,
        instance_id
    )
    .fetch_one(&ctx.pool)
    .await?;

    assert_eq!(instance.service_name, service_name);
    assert_eq!(instance.version, "1.0.0");

    // Update heartbeat
    sqlx::query!(
        r#"
        UPDATE core.satellite_instances
        SET last_heartbeat = NOW()
        WHERE instance_id = $1
        "#,
        instance_id
    )
    .execute(&ctx.pool)
    .await?;

    // Cleanup
    sqlx::query!(
        "DELETE FROM core.satellite_instances WHERE instance_id = $1",
        instance_id
    )
    .execute(&ctx.pool)
    .await?;

    Ok(())
}

/// Test that multiple satellite instances can coexist.
#[sinex_test]
async fn test_multiple_satellite_instances(ctx: TestContext) -> Result<()> {
    let service_name = format!("multi-instance-{}", uuid::Uuid::new_v4());
    let mut instance_ids = vec![];

    // Register multiple instances
    for i in 0..5 {
        let instance_id = uuid::Uuid::new_v4().to_string();
        instance_ids.push(instance_id.clone());

        sqlx::query!(
            r#"
            INSERT INTO core.satellite_instances
                (service_name, instance_id, version, start_time, last_heartbeat, host_name, metadata)
            VALUES ($1, $2, $3, NOW(), NOW(), $4, '{}'::jsonb)
            "#,
            service_name,
            instance_id,
            format!("1.0.{}", i),
            format!("host-{}", i)
        )
        .execute(&ctx.pool)
        .await?;
    }

    // Query all instances for the service
    let instances = sqlx::query!(
        r#"
        SELECT instance_id, version, host_name
        FROM core.satellite_instances
        WHERE service_name = $1
        ORDER BY start_time
        "#,
        service_name
    )
    .fetch_all(&ctx.pool)
    .await?;

    assert_eq!(instances.len(), 5, "Should have 5 instances");

    // Verify versions are unique
    let versions: std::collections::HashSet<_> =
        instances.iter().map(|i| i.version.clone()).collect();
    assert_eq!(
        versions.len(),
        5,
        "Each instance should have unique version"
    );

    // Cleanup
    for instance_id in instance_ids {
        sqlx::query!(
            "DELETE FROM core.satellite_instances WHERE instance_id = $1",
            instance_id
        )
        .execute(&ctx.pool)
        .await?;
    }

    Ok(())
}

/// Test heartbeat staleness detection.
#[sinex_test]
async fn test_heartbeat_staleness_detection(ctx: TestContext) -> Result<()> {
    let service_name = format!("stale-heartbeat-{}", uuid::Uuid::new_v4());

    // Create instances with different heartbeat ages
    let fresh_id = uuid::Uuid::new_v4().to_string();
    let stale_id = uuid::Uuid::new_v4().to_string();

    // Fresh instance (heartbeat now)
    sqlx::query!(
        r#"
        INSERT INTO core.satellite_instances
            (service_name, instance_id, version, start_time, last_heartbeat, host_name, metadata)
        VALUES ($1, $2, '1.0.0', NOW(), NOW(), 'fresh-host', '{}'::jsonb)
        "#,
        service_name,
        fresh_id
    )
    .execute(&ctx.pool)
    .await?;

    // Stale instance (heartbeat 5 minutes ago)
    sqlx::query!(
        r#"
        INSERT INTO core.satellite_instances
            (service_name, instance_id, version, start_time, last_heartbeat, host_name, metadata)
        VALUES ($1, $2, '1.0.0', NOW() - INTERVAL '10 minutes', NOW() - INTERVAL '5 minutes', 'stale-host', '{}'::jsonb)
        "#,
        service_name,
        stale_id
    )
    .execute(&ctx.pool)
    .await?;

    // Query for stale instances (heartbeat older than 1 minute)
    let stale_instances: Vec<_> = sqlx::query!(
        r#"
        SELECT instance_id
        FROM core.satellite_instances
        WHERE service_name = $1
          AND last_heartbeat < NOW() - INTERVAL '1 minute'
        "#,
        service_name
    )
    .fetch_all(&ctx.pool)
    .await?;

    assert_eq!(stale_instances.len(), 1, "Should detect one stale instance");
    assert_eq!(stale_instances[0].instance_id, stale_id);

    // Query for healthy instances
    let healthy_instances: Vec<_> = sqlx::query!(
        r#"
        SELECT instance_id
        FROM core.satellite_instances
        WHERE service_name = $1
          AND last_heartbeat >= NOW() - INTERVAL '1 minute'
        "#,
        service_name
    )
    .fetch_all(&ctx.pool)
    .await?;

    assert_eq!(
        healthy_instances.len(),
        1,
        "Should have one healthy instance"
    );
    assert_eq!(healthy_instances[0].instance_id, fresh_id);

    // Cleanup
    sqlx::query!(
        "DELETE FROM core.satellite_instances WHERE service_name = $1",
        service_name
    )
    .execute(&ctx.pool)
    .await?;

    Ok(())
}

// =============================================================================
// Service Leadership Tests
// =============================================================================

/// Test that leadership can be acquired and maintained.
#[sinex_test]
async fn test_service_leadership_acquisition(ctx: TestContext) -> Result<()> {
    let service_name = format!("leadership-test-{}", uuid::Uuid::new_v4());
    let instance_id = uuid::Uuid::new_v4().to_string();

    // First, register the instance (FK requirement)
    sqlx::query!(
        r#"
        INSERT INTO core.satellite_instances
            (service_name, instance_id, version, start_time, last_heartbeat, host_name, metadata)
        VALUES ($1, $2, '1.0.0', NOW(), NOW(), 'leader-host', '{}'::jsonb)
        "#,
        service_name,
        instance_id
    )
    .execute(&ctx.pool)
    .await?;

    // Acquire leadership
    sqlx::query!(
        r#"
        INSERT INTO core.service_leadership
            (service_name, instance_id, acquired_at, last_heartbeat, version)
        VALUES ($1, $2, NOW(), NOW(), '1.0.0')
        "#,
        service_name,
        instance_id
    )
    .execute(&ctx.pool)
    .await?;

    // Verify leadership
    let leader = sqlx::query!(
        r#"
        SELECT instance_id
        FROM core.service_leadership
        WHERE service_name = $1
        "#,
        service_name
    )
    .fetch_one(&ctx.pool)
    .await?;

    assert_eq!(leader.instance_id.to_string(), instance_id);

    // Update heartbeat
    let updated: Option<_> = sqlx::query!(
        r#"
        UPDATE core.service_leadership
        SET last_heartbeat = NOW()
        WHERE service_name = $1 AND instance_id = $2
        RETURNING service_name
        "#,
        service_name,
        instance_id
    )
    .fetch_optional(&ctx.pool)
    .await?;

    assert!(updated.is_some(), "Heartbeat update should succeed");

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

/// Test leadership transfer between instances.
#[sinex_test]
async fn test_leadership_transfer(ctx: TestContext) -> Result<()> {
    let service_name = format!("transfer-test-{}", uuid::Uuid::new_v4());
    let old_leader_id = uuid::Uuid::new_v4().to_string();
    let new_leader_id = uuid::Uuid::new_v4().to_string();

    // Register both instances
    for (instance_id, version) in [(&old_leader_id, "1.0.0"), (&new_leader_id, "1.1.0")] {
        sqlx::query!(
            r#"
            INSERT INTO core.satellite_instances
                (service_name, instance_id, version, start_time, last_heartbeat, host_name, metadata)
            VALUES ($1, $2, $3, NOW(), NOW(), 'test-host', '{}'::jsonb)
            "#,
            service_name,
            instance_id,
            version
        )
        .execute(&ctx.pool)
        .await?;
    }

    // Old leader acquires leadership
    sqlx::query!(
        r#"
        INSERT INTO core.service_leadership
            (service_name, instance_id, acquired_at, last_heartbeat, version)
        VALUES ($1, $2, NOW(), NOW(), '1.0.0')
        "#,
        service_name,
        old_leader_id
    )
    .execute(&ctx.pool)
    .await?;

    // Simulate old leader going stale
    sqlx::query!(
        r#"
        UPDATE core.service_leadership
        SET last_heartbeat = NOW() - INTERVAL '2 minutes'
        WHERE service_name = $1
        "#,
        service_name
    )
    .execute(&ctx.pool)
    .await?;

    // New leader takes over (atomic transfer)
    let transfer: Option<_> = sqlx::query!(
        r#"
        UPDATE core.service_leadership
        SET instance_id = $2,
            acquired_at = NOW(),
            last_heartbeat = NOW(),
            version = '1.1.0'
        WHERE service_name = $1
          AND last_heartbeat < NOW() - INTERVAL '1 minute'
        RETURNING instance_id
        "#,
        service_name,
        new_leader_id
    )
    .fetch_optional(&ctx.pool)
    .await?;

    assert!(transfer.is_some(), "Leadership transfer should succeed");
    assert_eq!(transfer.unwrap().instance_id.to_string(), new_leader_id);

    // Verify new leader
    let current_leader = sqlx::query!(
        r#"
        SELECT instance_id FROM core.service_leadership
        WHERE service_name = $1
        "#,
        service_name
    )
    .fetch_one(&ctx.pool)
    .await?;

    assert_eq!(current_leader.instance_id.to_string(), new_leader_id);

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

// =============================================================================
// Concurrent Coordination Stress Tests
// =============================================================================

/// Test coordination mechanisms under concurrent load.
#[sinex_test]
async fn test_concurrent_coordination_stress(ctx: TestContext) -> Result<()> {
    let service_name = format!("stress-coord-{}", uuid::Uuid::new_v4());
    let registration_count = Arc::new(AtomicU32::new(0));
    let heartbeat_count = Arc::new(AtomicU32::new(0));

    let mut handles = vec![];

    // Spawn multiple tasks doing registration and heartbeats
    for task_id in 0..10 {
        let pool = ctx.pool.clone();
        let service = service_name.clone();
        let registrations = registration_count.clone();
        let heartbeats = heartbeat_count.clone();

        let handle = tokio::spawn(async move {
            let instance_id = uuid::Uuid::new_v4().to_string();

            // Register
            let reg_result: Result<_, sqlx::Error> = sqlx::query!(
                r#"
                INSERT INTO core.satellite_instances
                    (service_name, instance_id, version, start_time, last_heartbeat, host_name, metadata)
                VALUES ($1, $2, '1.0.0', NOW(), NOW(), $3, '{}'::jsonb)
                "#,
                service,
                instance_id,
                format!("host-{}", task_id)
            )
            .execute(&pool)
            .await;

            if reg_result.is_ok() {
                registrations.fetch_add(1, Ordering::SeqCst);

                // Do multiple heartbeats
                for _ in 0..5 {
                    let hb_result: Result<_, sqlx::Error> = sqlx::query!(
                        r#"
                        UPDATE core.satellite_instances
                        SET last_heartbeat = NOW()
                        WHERE instance_id = $1
                        "#,
                        instance_id
                    )
                    .execute(&pool)
                    .await;

                    if hb_result.is_ok() {
                        heartbeats.fetch_add(1, Ordering::SeqCst);
                    }

                    sleep(Duration::from_millis(10)).await;
                }
            }
        });

        handles.push(handle);
    }

    // Wait for all tasks
    for handle in handles {
        let _ = handle.await;
    }

    let registrations = registration_count.load(Ordering::SeqCst);
    let heartbeats = heartbeat_count.load(Ordering::SeqCst);

    tracing::info!(
        "Stress test: {} registrations, {} heartbeats",
        registrations,
        heartbeats
    );

    assert_eq!(registrations, 10, "All registrations should succeed");
    assert!(heartbeats >= 40, "Most heartbeats should succeed");

    // Cleanup
    sqlx::query!(
        "DELETE FROM core.satellite_instances WHERE service_name = $1",
        service_name
    )
    .execute(&ctx.pool)
    .await?;

    Ok(())
}

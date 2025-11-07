//! Integration tests for distributed locking system
//!
//! Tests comprehensive distributed locking functionality including:
//! - Advisory lock acquisition and release
//! - Lock timeout and expiration
//! - Concurrent lock contention
//! - Cross-process lock coordination
//! - Lock cleanup and recovery
//! - Version-based leadership election

use color_eyre::eyre::Result as EyreResult;
use sinex_core::db::distributed_locking::{AdvisoryLock, DistributedCoordination};
use sinex_satellite_sdk::{SatelliteInstance, SatelliteVersion};
use sinex_test_utils::{sinex_test, TestContext};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use tokio::time::Duration;

#[sinex_test]
async fn test_advisory_lock_basic_acquisition(ctx: TestContext) -> EyreResult<()> {
    let pool = ctx.db_pool();

    // Test basic lock acquisition
    let lock1 = AdvisoryLock::try_acquire(pool, "basic_test_lock").await?;
    assert!(lock1.is_some());

    // Test that same lock cannot be acquired again
    let lock2 = AdvisoryLock::try_acquire(pool, "basic_test_lock").await?;
    assert!(lock2.is_none());

    // Release first lock
    drop(lock1);

    // Now second lock should succeed
    let lock3 = AdvisoryLock::try_acquire(pool, "basic_test_lock").await?;
    assert!(lock3.is_some());

    Ok(())
}

#[sinex_test]
async fn test_advisory_lock_different_names(ctx: TestContext) -> EyreResult<()> {
    let pool = ctx.db_pool();

    // Different lock names should not interfere
    let lock1 = AdvisoryLock::try_acquire(pool, "lock_name_1").await?;
    let lock2 = AdvisoryLock::try_acquire(pool, "lock_name_2").await?;
    let lock3 = AdvisoryLock::try_acquire(pool, "lock_name_3").await?;

    // All should succeed since they have different names
    assert!(lock1.is_some());
    assert!(lock2.is_some());
    assert!(lock3.is_some());

    Ok(())
}

#[sinex_test]
async fn test_advisory_lock_concurrent_contention(ctx: TestContext) -> EyreResult<()> {
    let pool = ctx.db_pool();
    let lock_name = "contention_test_lock";

    let success_count = Arc::new(AtomicU32::new(0));
    let failure_count = Arc::new(AtomicU32::new(0));

    // Spawn multiple tasks trying to acquire the same lock
    let tasks = (0..10)
        .map(|i| {
            let pool = pool.clone();
            let success_counter = success_count.clone();
            let failure_counter = failure_count.clone();
            let task_lock_name = format!("{}_{}", lock_name, i % 3); // Group into 3 lock names

            tokio::spawn(async move {
                match AdvisoryLock::try_acquire(&pool, &task_lock_name).await {
                    Ok(Some(_lock)) => {
                        success_counter.fetch_add(1, Ordering::SeqCst);
                        // Hold lock briefly
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                    Ok(None) => {
                        failure_counter.fetch_add(1, Ordering::SeqCst);
                    }
                    Err(e) => {
                        eprintln!("Lock error: {}", e);
                        failure_counter.fetch_add(1, Ordering::SeqCst);
                    }
                }
            })
        })
        .collect::<Vec<_>>();

    // Wait for all tasks to complete
    for task in tasks {
        task.await.expect("Task should complete without panic");
    }

    let successes = success_count.load(Ordering::SeqCst);
    let failures = failure_count.load(Ordering::SeqCst);

    // With 3 different lock names and 10 tasks, we should have some successes
    // At least 3 (one per lock name), but possibly more due to timing
    assert!(successes >= 3);
    assert_eq!(successes + failures, 10);

    Ok(())
}

#[sinex_test]
async fn test_advisory_lock_automatic_release_on_drop(ctx: TestContext) -> EyreResult<()> {
    let pool = ctx.db_pool();
    let lock_name = "drop_test_lock";

    // Acquire lock in a scope
    {
        let _lock = AdvisoryLock::try_acquire(pool, lock_name).await?;
        assert!(_lock.is_some());

        // Verify lock is held
        let contention_attempt = AdvisoryLock::try_acquire(pool, lock_name).await?;
        assert!(contention_attempt.is_none());
    } // Lock should be automatically released here

    // Should now be able to acquire the lock again
    let post_drop_lock = AdvisoryLock::try_acquire(pool, lock_name).await?;
    assert!(post_drop_lock.is_some());

    Ok(())
}

#[sinex_test]
async fn test_distributed_coordination_instance_registration(ctx: TestContext) -> EyreResult<()> {
    let pool = ctx.db_pool();

    let instance = SatelliteInstance::new(
        "registration_test",
        SatelliteVersion::parse("1.0.100+test123").expect("Valid version string"),
    );

    let mut coordination = DistributedCoordination::new(instance.clone(), pool.clone());

    // Register instance
    coordination.register_instance().await?;

    // Verify instance appears in database
    let registered = sqlx::query(
        "SELECT instance_id::uuid as id, service_name, version FROM core.satellite_instances WHERE instance_id = $1::uuid::ulid",
    )
    .bind(instance.instance_id().to_uuid())
    .fetch_optional(pool)
    .await?;

    assert!(registered.is_some());
    let reg = registered.expect("Registration should succeed");
    let id: sqlx::types::Uuid = reg.get("id");
    let service_name: String = reg.get("service_name");
    let version: String = reg.get("version");
    assert_eq!(id, instance.instance_id().to_uuid());
    assert_eq!(service_name, "registration_test");
    assert_eq!(version, "1.0.100+test123");

    Ok(())
}

#[sinex_test]
async fn test_distributed_coordination_version_based_leadership(
    ctx: TestContext,
) -> EyreResult<()> {
    let pool = ctx.db_pool();

    // Create instances with different versions
    let old_instance = SatelliteInstance::new(
        "version_leadership_test",
        SatelliteVersion::parse("1.0.100+old").expect("Valid version string"),
    );

    let new_instance = SatelliteInstance::new(
        "version_leadership_test",
        SatelliteVersion::parse("1.0.200+new").expect("Valid version string"),
    );

    let newest_instance = SatelliteInstance::new(
        "version_leadership_test",
        SatelliteVersion::parse("1.1.0+newest").expect("Valid version string"),
    );

    let mut coord_old = DistributedCoordination::new(old_instance, pool.clone());
    let mut coord_new = DistributedCoordination::new(new_instance, pool.clone());
    let mut coord_newest = DistributedCoordination::new(newest_instance, pool.clone());

    // Register all instances
    coord_old.register_instance().await?;
    coord_new.register_instance().await?;
    coord_newest.register_instance().await?;

    // Try to acquire leadership - newest version should win
    let leadership_old = coord_old.try_acquire_leadership().await?;
    let leadership_new = coord_new.try_acquire_leadership().await?;
    let leadership_newest = coord_newest.try_acquire_leadership().await?;

    // Only the newest version should get leadership
    assert!(leadership_old.is_none());
    assert!(leadership_new.is_none());
    assert!(leadership_newest.is_some());

    Ok(())
}

#[sinex_test]
async fn test_distributed_coordination_leadership_handoff(ctx: TestContext) -> EyreResult<()> {
    let pool = ctx.db_pool();

    // Start with an older version as leader
    let old_instance = SatelliteInstance::new(
        "handoff_test",
        SatelliteVersion::parse("1.0.100+old").expect("Valid version string"),
    );

    let mut old_coord = DistributedCoordination::new(old_instance, pool.clone());
    old_coord.register_instance().await?;

    // Acquire leadership with old version
    let old_leadership = old_coord.try_acquire_leadership().await?;
    assert!(old_leadership.is_some());

    // Deploy new version
    let new_instance = SatelliteInstance::new(
        "handoff_test",
        SatelliteVersion::parse("1.0.200+new").expect("Valid version string"),
    );

    let mut new_coord = DistributedCoordination::new(new_instance, pool.clone());
    new_coord.register_instance().await?;

    // New version should be able to take leadership
    let new_leadership = new_coord.try_acquire_leadership().await?;
    assert!(new_leadership.is_some());

    // Old version should lose leadership when it tries again
    drop(old_leadership); // Release old leadership
    let old_retry = old_coord.try_acquire_leadership().await?;
    assert!(old_retry.is_none());

    Ok(())
}

#[sinex_test]
async fn test_distributed_coordination_leadership_contention(ctx: TestContext) -> EyreResult<()> {
    let pool = ctx.db_pool();

    // Create multiple instances with the same version (same commit count)
    let instances: Vec<_> = (0..5)
        .map(|i| {
            SatelliteInstance::new(
                "contention_test",
                SatelliteVersion::parse(&format!("1.0.100+commit{:03}", i))
                    .expect("Valid version string"),
            )
        })
        .collect();

    let mut coordinators: Vec<_> = instances
        .into_iter()
        .map(|instance| DistributedCoordination::new(instance, pool.clone()))
        .collect();

    // Register all instances
    for coord in &mut coordinators {
        coord.register_instance().await?;
    }

    // Try to acquire leadership concurrently
    let leadership_results = futures::future::join_all(
        coordinators
            .iter_mut()
            .map(|coord| coord.try_acquire_leadership()),
    )
    .await;

    // Exactly one should succeed (deterministic based on instance_id or other factors)
    let successful_leaders = leadership_results
        .iter()
        .filter_map(|result| result.as_ref().ok())
        .filter(|opt| opt.is_some())
        .count();

    assert_eq!(
        successful_leaders, 1,
        "Exactly one leader should be elected"
    );

    Ok(())
}

#[sinex_test]
async fn test_advisory_lock_stress_test(ctx: TestContext) -> EyreResult<()> {
    let pool = ctx.db_pool();

    let total_acquisitions = Arc::new(AtomicU32::new(0));
    let successful_acquisitions = Arc::new(AtomicU32::new(0));

    // High-contention stress test with many concurrent attempts
    let tasks = (0..50)
        .map(|i| {
            let pool = pool.clone();
            let total = total_acquisitions.clone();
            let successful = successful_acquisitions.clone();
            let lock_name = format!("stress_lock_{}", i % 5); // 5 different locks

            tokio::spawn(async move {
                for _ in 0..10 {
                    total.fetch_add(1, Ordering::SeqCst);

                    if let Ok(Some(_lock)) = AdvisoryLock::try_acquire(&pool, &lock_name).await {
                        successful.fetch_add(1, Ordering::SeqCst);
                        // Hold lock very briefly to increase contention
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }

                    // Small delay between attempts
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            })
        })
        .collect::<Vec<_>>();

    // Wait for all tasks
    for task in tasks {
        task.await.expect("Task should complete without panic");
    }

    let total = total_acquisitions.load(Ordering::SeqCst);
    let successful = successful_acquisitions.load(Ordering::SeqCst);

    assert_eq!(total, 500); // 50 tasks * 10 attempts
    assert!(successful > 0, "Should have some successful acquisitions");
    assert!(successful < total, "Should have some contention failures");

    println!(
        "Stress test: {}/{} successful acquisitions",
        successful, total
    );

    Ok(())
}

#[sinex_test]
async fn test_lock_cleanup_on_connection_drop(ctx: TestContext) -> EyreResult<()> {
    let pool = ctx.db_pool();
    let lock_name = "connection_drop_test";

    // Acquire lock with a separate connection pool that we'll drop
    {
        let separate_pool = sqlx::PgPool::connect(&ctx.database_url()).await?;
        let _lock = AdvisoryLock::try_acquire(&separate_pool, lock_name).await?;
        assert!(_lock.is_some());

        // Verify lock is held from main pool
        let contention = AdvisoryLock::try_acquire(pool, lock_name).await?;
        assert!(contention.is_none());

        // separate_pool drops here, which should release the advisory lock
    }

    // Give a moment for cleanup to occur
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Should now be able to acquire the lock from main pool
    let recovered_lock = AdvisoryLock::try_acquire(pool, lock_name).await?;
    assert!(
        recovered_lock.is_some(),
        "Lock should be released when connection drops"
    );

    Ok(())
}

#[sinex_test]
async fn test_distributed_coordination_leader_heartbeat_simulation(
    ctx: TestContext,
) -> EyreResult<()> {
    let pool = ctx.db_pool();

    let instance = SatelliteInstance::new(
        "heartbeat_test",
        SatelliteVersion::parse("1.0.100+heartbeat").expect("Valid version string"),
    );

    let mut coordination = DistributedCoordination::new(instance.clone(), pool.clone());
    coordination.register_instance().await?;

    let leadership = coordination.try_acquire_leadership().await?;
    assert!(leadership.is_some());

    // Simulate periodic heartbeat updates (this would normally be done by the coordination loop)
    for i in 0..5 {
        // Update instance last_seen timestamp
        sqlx::query(
            "UPDATE core.satellite_instances SET last_seen = NOW() WHERE instance_id = $1::uuid::ulid",
        )
        .bind(instance.instance_id().to_uuid())
        .execute(pool)
        .await?;

        tokio::time::sleep(Duration::from_millis(100)).await;

        // Verify leadership is still held
        let current_leader = sqlx::query(
            "SELECT instance_id::uuid as id FROM core.service_leadership WHERE service_name = $1",
        )
        .bind("heartbeat_test")
        .fetch_optional(pool)
        .await?;

        assert!(current_leader.is_some());
        let leader_row = current_leader.expect("Leader should exist");
        let leader_id: sqlx::types::Uuid = leader_row.get("id");
        assert_eq!(leader_id, instance.instance_id().to_uuid());
    }

    Ok(())
}

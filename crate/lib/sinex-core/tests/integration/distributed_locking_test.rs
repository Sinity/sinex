//! Integration tests for distributed locking primitives.
//!
//! These tests exercise the modern advisory lock + coordination helpers by
//! driving them exactly the way production code does: through `DbPool`
//! handles with no additional wrappers.

use sinex_core::db::distributed_locking::{AdvisoryLock, DistributedCoordination};
use sinex_core::types::utils::ResourceGuard;
use sinex_core::SinexError;
use sinex_test_utils::prelude::*;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::time::{sleep, Duration};

#[sinex_test]
async fn test_advisory_lock_basic_acquisition(ctx: TestContext) -> Result<()> {
    let _guard = sinex_test_utils::acquire_pool_test_guard().await;
    ctx.ensure_clean().await?;
    sinex_test_utils::db_common::reset_database(ctx.pool()).await?;
    sinex_test_utils::db_common::verify_clean_state(ctx.pool()).await?;
    let pool = ctx.pool.clone();
    let lock_name = format!("basic_lock_{}", Ulid::new());

    let lock1 = AdvisoryLock::try_acquire(&pool, &lock_name).await?;
    assert!(lock1.is_some());

    // Second acquisition should fail while the first guard is in scope.
    let lock2 = AdvisoryLock::try_acquire(&pool, &lock_name).await?;
    assert!(lock2.is_none());

    drop(lock1);
    let lock3 = tokio::time::timeout(Duration::from_millis(1000), async {
        loop {
            if let Some(lock) = AdvisoryLock::try_acquire(&pool, &lock_name).await? {
                break Ok::<ResourceGuard<AdvisoryLock>, SinexError>(lock);
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await??;
    assert!(lock3.resource().await.is_some());
    sinex_test_utils::db_common::reset_database(ctx.pool()).await?;
    sinex_test_utils::db_common::verify_clean_state(ctx.pool()).await?;
    ctx.force_cleanup().await?;

    Ok(())
}

#[sinex_test]
async fn test_advisory_lock_different_names(ctx: TestContext) -> Result<()> {
    let _guard = sinex_test_utils::acquire_pool_test_guard().await;
    ctx.ensure_clean().await?;
    sinex_test_utils::db_common::reset_database(&ctx.pool).await?;
    sinex_test_utils::db_common::verify_clean_state(&ctx.pool).await?;
    let pool = ctx.pool.clone();
    let suffix = Ulid::new();
    let lock1_name = format!("lock_name_1_{suffix}");
    let lock2_name = format!("lock_name_2_{suffix}");
    let lock3_name = format!("lock_name_3_{suffix}");

    let lock1 = AdvisoryLock::try_acquire(&pool, &lock1_name).await?;
    assert!(lock1.is_some());
    let lock2 = AdvisoryLock::try_acquire(&pool, &lock2_name).await?;
    assert!(lock2.is_some());

    // Release one connection before acquiring a third distinct lock to avoid pool exhaustion.
    drop(lock1);
    let lock3 = AdvisoryLock::try_acquire(&pool, &lock3_name).await?;
    assert!(lock3.is_some());

    drop(lock2);
    drop(lock3);
    ctx.force_cleanup().await?;
    Ok(())
}

#[sinex_test]
async fn test_advisory_lock_concurrent_contention(ctx: TestContext) -> Result<()> {
    ctx.ensure_clean().await?;
    let pool = ctx.pool.clone();
    let successes = Arc::new(AtomicU32::new(0));
    let failures = Arc::new(AtomicU32::new(0));

    let tasks = (0..10)
        .map(|i| {
            let pool = pool.clone();
            let successes = successes.clone();
            let failures = failures.clone();
            let name = format!("contention_lock_{}", i % 3);

            tokio::spawn(async move {
                match AdvisoryLock::try_acquire(&pool, &name).await {
                    Ok(Some(_guard)) => {
                        successes.fetch_add(1, Ordering::SeqCst);
                        sleep(Duration::from_millis(20)).await;
                    }
                    Ok(None) => {
                        failures.fetch_add(1, Ordering::SeqCst);
                    }
                    Err(err) => {
                        failures.fetch_add(1, Ordering::SeqCst);
                        panic!("unexpected advisory lock error: {err}");
                    }
                }
            })
        })
        .collect::<Vec<_>>();

    for task in tasks {
        task.await.expect("task should not panic");
    }

    let success_total = successes.load(Ordering::SeqCst);
    let failure_total = failures.load(Ordering::SeqCst);
    assert!(success_total >= 3);
    assert_eq!(success_total + failure_total, 10);

    Ok(())
}

#[sinex_test]
async fn test_advisory_lock_automatic_release_on_drop(ctx: TestContext) -> Result<()> {
    ctx.force_cleanup().await?;
    ctx.ensure_clean().await?;
    sinex_test_utils::db_common::reset_database(ctx.pool()).await?;
    sinex_test_utils::db_common::verify_clean_state(ctx.pool()).await?;
    let pool = ctx.pool.clone();
    let name = format!("automatic_release_lock_{}", Ulid::new());

    {
        let first = AdvisoryLock::try_acquire(&pool, &name).await?;
        assert!(first.is_some());

        // Should be held while the guard is alive.
        assert!(AdvisoryLock::try_acquire(&pool, &name).await?.is_none());
    }

    let re_acquired = tokio::time::timeout(Duration::from_millis(2000), async {
        loop {
            match AdvisoryLock::try_acquire(&pool, &name).await? {
                Some(lock) => break Ok::<ResourceGuard<AdvisoryLock>, SinexError>(lock),
                None => sleep(Duration::from_millis(10)).await,
            }
        }
    })
    .await??;
    assert!(re_acquired.resource().await.is_some());
    sinex_test_utils::db_common::reset_database(ctx.pool()).await?;
    sinex_test_utils::db_common::verify_clean_state(ctx.pool()).await?;
    ctx.force_cleanup().await?;
    Ok(())
}

#[sinex_test]
async fn test_distributed_coordination_leadership_locking(ctx: TestContext) -> Result<()> {
    ctx.ensure_clean().await?;
    let pool = ctx.pool.clone();
    let coordination = DistributedCoordination::new(pool.clone());
    let service = format!("coordination.leader.{}", Ulid::new());

    assert!(!coordination.has_leader(&service).await?);

    let leader = coordination.try_become_leader(&service).await?;
    assert!(leader.is_some());
    assert!(coordination.has_leader(&service).await?);

    // Another contender should fail while the guard is held.
    assert!(coordination.try_become_leader(&service).await?.is_none());

    drop(leader);
    let _successor = tokio::time::timeout(Duration::from_millis(500), async {
        loop {
            match coordination.try_become_leader(&service).await? {
                Some(lock) => break Ok::<ResourceGuard<AdvisoryLock>, SinexError>(lock),
                None => sleep(Duration::from_millis(10)).await,
            }
        }
    })
    .await??;
    assert!(coordination.has_leader(&service).await?);

    Ok(())
}

#[sinex_test]
async fn test_distributed_coordination_job_lock_contention(ctx: TestContext) -> Result<()> {
    ctx.ensure_clean().await?;
    let pool = ctx.pool.clone();
    let coordination = DistributedCoordination::new(pool);
    let job_id = format!("job-{}", Ulid::new());

    let first = coordination.acquire_job_lock(&job_id).await?;
    assert!(first.is_some());

    let second = coordination.acquire_job_lock(&job_id).await?;
    assert!(second.is_none());

    // Wait for the async RAII cleanup to release the advisory lock before attempting a
    // third acquisition; ResourceGuard cleanup runs in the background.
    drop(first);
    let _third = tokio::time::timeout(Duration::from_millis(500), async {
        loop {
            match coordination.acquire_job_lock(&job_id).await? {
                Some(lock) => break Ok::<ResourceGuard<AdvisoryLock>, SinexError>(lock),
                None => {
                    sleep(Duration::from_millis(10)).await;
                }
            }
        }
    })
    .await??;

    Ok(())
}

#[sinex_test]
async fn test_distributed_coordination_resource_lock_waits(ctx: TestContext) -> Result<()> {
    let pool = ctx.pool.clone();
    let coordination = DistributedCoordination::new(pool.clone());
    let resource = format!("resource.{}", Ulid::new());

    let guard = coordination
        .acquire_resource_lock(&resource, Duration::from_secs(1))
        .await?;

    let coordination_clone = DistributedCoordination::new(pool);
    let resource_clone = resource.clone();
    let waiter = tokio::spawn(async move {
        let start = Instant::now();
        let _lock = coordination_clone
            .acquire_resource_lock(&resource_clone, Duration::from_secs(1))
            .await
            .expect("waiting lock acquisition should succeed");
        start.elapsed()
    });

    sleep(Duration::from_millis(100)).await;
    drop(guard);
    let wait_duration = waiter.await?;
    assert!(
        wait_duration >= Duration::from_millis(90),
        "second acquisition should wait for the first guard to drop"
    );

    Ok(())
}

#[sinex_test]
async fn test_advisory_lock_stress_test(ctx: TestContext) -> Result<()> {
    let pool = ctx.pool.clone();
    let attempts = Arc::new(AtomicU32::new(0));
    let successes = Arc::new(AtomicU32::new(0));

    // Deterministic contention sanity check
    let held = AdvisoryLock::try_acquire(&pool, "stress.preheld")
        .await?
        .expect("preheld lock should be available");
    assert!(AdvisoryLock::try_acquire(&pool, "stress.preheld")
        .await?
        .is_none());
    drop(held);

    let tasks = (0..50)
        .map(|i| {
            let pool = pool.clone();
            let attempts = attempts.clone();
            let successes = successes.clone();
            let name = format!("stress.lock.{}", i % 5);

            tokio::spawn(async move {
                for _ in 0..10 {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    if let Ok(Some(_lock)) = AdvisoryLock::try_acquire(&pool, &name).await {
                        successes.fetch_add(1, Ordering::SeqCst);
                        sleep(Duration::from_millis(5)).await;
                    } else {
                        sleep(Duration::from_millis(1)).await;
                    }
                }
            })
        })
        .collect::<Vec<_>>();

    for task in tasks {
        task.await.expect("stress task panicked");
    }

    let total_attempts = attempts.load(Ordering::SeqCst);
    assert_eq!(total_attempts, 500);
    let success_total = successes.load(Ordering::SeqCst);
    assert!(success_total > 0);
    if success_total == total_attempts {
        eprintln!(
            "advisory lock stress test observed no contention ({} successes)",
            success_total
        );
    }

    Ok(())
}

#[sinex_test]
async fn test_lock_cleanup_on_connection_drop(ctx: TestContext) -> Result<()> {
    let main_pool = ctx.pool.clone();
    let lock_name = "drop_cleanup_lock";

    {
        let temp_pool = sqlx::PgPool::connect(ctx.database_url()).await?;
        let temp_lock = AdvisoryLock::try_acquire(&temp_pool, lock_name).await?;
        assert!(temp_lock.is_some());

        // Main pool should see contention while the temp guard is alive.
        assert!(AdvisoryLock::try_acquire(&main_pool, lock_name)
            .await?
            .is_none());
    }

    sleep(Duration::from_millis(100)).await;
    assert!(AdvisoryLock::try_acquire(&main_pool, lock_name)
        .await?
        .is_some());
    Ok(())
}

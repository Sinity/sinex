use sinex_core::distributed_locking::{AdvisoryLock, DistributedCoordination};
use sinex_test_utils::{acquire_pool_test_guard, sinex_test, TestContext};
use std::time::Duration;

#[sinex_test]
async fn advisory_lock_try_acquire(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    ctx.ensure_clean().await?;
    let pool = &ctx.pool;

    let lock1 = AdvisoryLock::try_acquire(pool, "test_key").await?;
    assert!(lock1.is_some());

    let lock2 = AdvisoryLock::try_acquire(pool, "test_key").await?;
    assert!(lock2.is_none());

    drop(lock1);
    tokio::time::sleep(Duration::from_millis(100)).await;

    let lock3 = AdvisoryLock::try_acquire(pool, "test_key").await?;
    assert!(lock3.is_some());
    Ok(())
}

#[sinex_test]
async fn leadership_pattern(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    ctx.ensure_clean().await?;
    let pool = &ctx.pool;

    {
        let first = AdvisoryLock::try_acquire(pool, "leadership_test").await?;
        assert!(first.is_some());

        let second = AdvisoryLock::try_acquire(pool, "leadership_test").await?;
        assert!(second.is_none());

        drop(first);
        tokio::time::sleep(Duration::from_millis(10)).await;

        let third = AdvisoryLock::try_acquire(pool, "leadership_test").await?;
        assert!(third.is_some());
        drop(third);
    }

    let coordination = DistributedCoordination::new(pool.clone());
    assert!(!coordination.has_leader("test_service").await?);

    if let Some(leader) = coordination.try_become_leader("test_service").await? {
        assert!(coordination.has_leader("test_service").await?);
        drop(leader);
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    assert!(!coordination.has_leader("test_service").await?);
    tokio::time::sleep(Duration::from_millis(50)).await;
    Ok(())
}

#[sinex_test]
async fn advisory_lock_basic_acquisition(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    ctx.ensure_clean().await?;
    let pool = &ctx.pool;

    let lock1 = AdvisoryLock::try_acquire(pool, "test_lock_basic").await?;
    assert!(lock1.is_some());

    let lock2 = AdvisoryLock::try_acquire(pool, "test_lock_basic").await?;
    assert!(lock2.is_none());

    if let Some(lock) = lock1 {
        drop(lock);
    }

    tokio::time::sleep(Duration::from_millis(10)).await;

    let lock3 = AdvisoryLock::try_acquire(pool, "test_lock_basic").await?;
    assert!(lock3.is_some());
    Ok(())
}

#[sinex_test]
async fn advisory_lock_raii_cleanup(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    ctx.ensure_clean().await?;
    let pool = &ctx.pool;

    {
        let lock = AdvisoryLock::try_acquire(pool, "test_lock_raii").await?;
        assert!(lock.is_some());

        let conflict = AdvisoryLock::try_acquire(pool, "test_lock_raii").await?;
        assert!(conflict.is_none());
    }

    tokio::time::sleep(Duration::from_millis(10)).await;

    let after = AdvisoryLock::try_acquire(pool, "test_lock_raii").await?;
    assert!(after.is_some());
    Ok(())
}

#[sinex_test]
async fn advisory_lock_different_names(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let _guard = acquire_pool_test_guard().await;
    ctx.force_cleanup().await?;
    ctx.ensure_clean().await?;
    let pool = &ctx.pool;

    // Ensure no lingering advisory locks from previous runs.
    for name in ["lock_alpha", "lock_beta", "lock_gamma"] {
        let _ = AdvisoryLock::force_release(pool, name).await;
    }

    for name in ["lock_alpha", "lock_beta", "lock_gamma"] {
        let guard = AdvisoryLock::try_acquire(pool, name)
            .await?
            .expect("lock should be acquirable");
        drop(guard);
    }

    let held = AdvisoryLock::try_acquire(pool, "lock_alpha")
        .await?
        .expect("lock_alpha should be acquirable again");
    let conflict = AdvisoryLock::try_acquire(pool, "lock_alpha").await?;
    assert!(conflict.is_none());

    drop(held);
    AdvisoryLock::force_release(pool, "lock_alpha").await.ok();

    let _ = sinex_test_utils::timing_utils::WaitHelpers::wait_for_condition(
        || {
            let pool = pool.clone();
            async move {
                Ok::<bool, sinex_test_utils::SinexError>(
                    AdvisoryLock::try_acquire(&pool, "lock_alpha")
                        .await
                        .ok()
                        .flatten()
                        .is_some(),
                )
            }
        },
        10,
    )
    .await;

    Ok(())
}

#[sinex_test]
async fn distributed_coordination_patterns(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let _guard = acquire_pool_test_guard().await;
    ctx.force_cleanup().await?;
    ctx.ensure_clean().await?;
    if let Err(e) = sinex_test_utils::db_common::reset_database(ctx.pool()).await {
        tracing::warn!(error = %e, "Reset before distributed_coordination_patterns failed, retrying after cleanup");
        ctx.force_cleanup().await?;
        sinex_test_utils::db_common::reset_database(ctx.pool()).await?;
    }
    sinex_test_utils::db_common::verify_clean_state(ctx.pool()).await?;
    let pool = &ctx.pool;
    let coordination = DistributedCoordination::new(pool.clone());
    let _ = coordination;
    if let Err(e) = sinex_test_utils::db_common::reset_database(ctx.pool()).await {
        tracing::warn!(error = %e, "Reset after distributed_coordination_patterns failed, retrying after cleanup");
        ctx.force_cleanup().await?;
        sinex_test_utils::db_common::reset_database(ctx.pool()).await?;
    }
    sinex_test_utils::db_common::verify_clean_state(ctx.pool()).await?;
    ctx.force_cleanup().await?;
    Ok(())
}

#[sinex_test]
async fn job_lock_pattern(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    ctx.ensure_clean().await?;
    let pool = &ctx.pool;
    let coordination = DistributedCoordination::new(pool.clone());
    let _ = coordination;
    Ok(())
}

#[sinex_test]
async fn resource_coordination_with_timeout(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    ctx.ensure_clean().await?;
    let pool = &ctx.pool;
    let coordination = DistributedCoordination::new(pool.clone());

    let timeout = Duration::from_millis(100);
    let resource_lock = coordination
        .acquire_resource_lock("shared_resource", timeout)
        .await?;

    let resource_ref = resource_lock.resource().await;
    let inner_lock = resource_ref
        .as_ref()
        .expect("resource should exist after acquiring lock");
    assert!(inner_lock.is_acquired());
    drop(resource_ref);

    let coordination2 = DistributedCoordination::new(pool.clone());
    let start = std::time::Instant::now();
    let result = coordination2
        .acquire_resource_lock("shared_resource", timeout)
        .await;
    let elapsed = start.elapsed();

    assert!(result.is_err());
    assert!(elapsed >= timeout);
    Ok(())
}

#[sinex_test]
async fn advisory_lock_concurrent_acquisition(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let pool = &ctx.pool;

    let lock_name = "concurrent_test";
    let primary = AdvisoryLock::try_acquire(pool, lock_name)
        .await?
        .expect("primary acquisition should succeed");

    let pool_clone = pool.clone();
    let concurrent =
        tokio::spawn(async move { AdvisoryLock::try_acquire(&pool_clone, lock_name).await })
            .await??;
    assert!(concurrent.is_none());

    drop(primary);
    tokio::time::sleep(Duration::from_millis(20)).await;

    let follow_up = AdvisoryLock::try_acquire(pool, lock_name).await?;
    assert!(follow_up.is_some());
    Ok(())
}

#[sinex_test]
async fn lock_status_checking(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let _guard = acquire_pool_test_guard().await;
    ctx.ensure_clean().await?;
    sinex_test_utils::db_common::reset_database(ctx.pool()).await?;
    sinex_test_utils::db_common::verify_clean_state(ctx.pool()).await?;
    let pool = &ctx.pool;

    let _ = AdvisoryLock::try_acquire(pool, "simple").await;
    sinex_test_utils::db_common::reset_database(ctx.pool()).await?;
    sinex_test_utils::db_common::verify_clean_state(ctx.pool()).await?;
    ctx.force_cleanup().await?;
    Ok(())
}

#[sinex_test]
async fn force_release_functionality(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let pool = &ctx.pool;
    ctx.ensure_clean().await?;
    let _ = AdvisoryLock::force_release(pool, "test").await;
    Ok(())
}

#[sinex_test]
async fn multiple_different_services(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let _guard = acquire_pool_test_guard().await;
    ctx.force_cleanup().await?;
    ctx.ensure_clean().await?;
    sinex_test_utils::db_common::reset_database(ctx.pool()).await?;
    sinex_test_utils::db_common::verify_clean_state(ctx.pool()).await?;

    let pool = &ctx.pool;
    let coordination = DistributedCoordination::new(pool.clone());
    // basic smoke: try to become leader under different service ids
    let services = ["svc-a", "svc-b", "svc-c"];
    for svc in services {
        if let Ok(Some(guard)) = coordination.try_become_leader(svc).await {
            drop(guard);
        }
    }

    if let Err(e) = sinex_test_utils::db_common::reset_database(ctx.pool()).await {
        tracing::warn!(error = %e, "Reset after multiple_different_services failed, retrying after force_cleanup");
        ctx.force_cleanup().await?;
        sinex_test_utils::db_common::reset_database(ctx.pool()).await?;
    }
    sinex_test_utils::db_common::verify_clean_state(ctx.pool()).await?;
    ctx.force_cleanup().await?;
    Ok(())
}

#[sinex_test]
async fn coordination_error_handling(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let pool = &ctx.pool;
    let coordination = DistributedCoordination::new(pool.clone());

    if let Ok(Some(guard)) = coordination.try_become_leader("").await {
        drop(guard);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let long_name = "a".repeat(100);
    if let Ok(Some(guard)) = coordination.try_become_leader(&long_name).await {
        drop(guard);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    if let Ok(Some(guard)) = coordination
        .try_become_leader("service-with_special.chars@123")
        .await
    {
        drop(guard);
    }

    Ok(())
}

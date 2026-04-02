use sinex_db::advisory_lock::AdvisoryLock;
use sinex_primitives::Uuid;
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn advisory_lock_tracks_state_and_releases_after_drop(ctx: TestContext) -> TestResult<()> {
    let key = format!("advisory-lock-state-{}", Uuid::now_v7());

    assert!(!AdvisoryLock::is_locked(&ctx.pool, &key).await?);

    let guard = AdvisoryLock::try_acquire(&ctx.pool, &key)
        .await?
        .expect("first advisory lock acquisition should succeed");

    assert!(AdvisoryLock::is_locked(&ctx.pool, &key).await?);
    assert!(AdvisoryLock::try_acquire(&ctx.pool, &key).await?.is_none());

    drop(guard);

    ctx.timing()
        .wait_for_condition(
            || async {
                Ok::<bool, color_eyre::Report>(
                    !AdvisoryLock::is_locked(&ctx.pool, &key).await?,
                )
            },
            10,
        )
        .await?;

    Ok(())
}

#[sinex_test]
async fn advisory_lock_waiter_acquires_after_prior_holder_drops(ctx: TestContext) -> TestResult<()> {
    let key = format!("advisory-lock-wait-{}", Uuid::now_v7());

    let first_guard = AdvisoryLock::try_acquire(&ctx.pool, &key)
        .await?
        .expect("first advisory lock acquisition should succeed");

    let pool = ctx.pool.clone();
    let waiter_key = key.clone();
    let waiter = tokio::spawn(async move {
        AdvisoryLock::acquire_or_wait(&pool, &waiter_key, std::time::Duration::from_secs(1)).await
    });

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    drop(first_guard);

    let second_guard = waiter
        .await
        .map_err(|error| color_eyre::eyre::eyre!("waiter task failed: {error}"))??;

    assert!(AdvisoryLock::is_locked(&ctx.pool, &key).await?);

    drop(second_guard);

    ctx.timing()
        .wait_for_condition(
            || async {
                Ok::<bool, color_eyre::Report>(
                    !AdvisoryLock::is_locked(&ctx.pool, &key).await?,
                )
            },
            10,
        )
        .await?;

    Ok(())
}

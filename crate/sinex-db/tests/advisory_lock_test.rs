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

    guard.cleanup_now().await;

    assert!(
        !AdvisoryLock::is_locked(&ctx.pool, &key).await?,
        "cleanup_now should synchronously release the advisory lock"
    );

    Ok(())
}

#[sinex_test]
async fn advisory_lock_waiter_acquires_after_prior_holder_drops(
    ctx: TestContext,
) -> TestResult<()> {
    let key = format!("advisory-lock-wait-{}", Uuid::now_v7());

    let first_guard = AdvisoryLock::try_acquire(&ctx.pool, &key)
        .await?
        .expect("first advisory lock acquisition should succeed");

    let pool = ctx.pool.clone();
    let waiter_key = key.clone();
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let waiter = tokio::spawn(async move {
        let _ = started_tx.send(());
        AdvisoryLock::acquire_or_wait(&pool, &waiter_key, std::time::Duration::from_secs(1)).await
    });

    started_rx
        .await
        .map_err(|error| color_eyre::eyre::eyre!("waiter start signal failed: {error}"))?;
    tokio::task::yield_now().await;
    assert!(
        !waiter.is_finished(),
        "waiter should still be blocked while the first guard is held"
    );
    first_guard.cleanup_now().await;

    let second_guard = waiter
        .await
        .map_err(|error| color_eyre::eyre::eyre!("waiter task failed: {error}"))??;

    assert!(AdvisoryLock::is_locked(&ctx.pool, &key).await?);

    second_guard.cleanup_now().await;

    assert!(
        !AdvisoryLock::is_locked(&ctx.pool, &key).await?,
        "cleanup_now should synchronously release the waiter advisory lock"
    );

    Ok(())
}

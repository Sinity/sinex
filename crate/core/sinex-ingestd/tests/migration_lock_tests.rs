use sinex_ingestd::service::try_acquire_migration_lock;
use sinex_test_utils::{sinex_test, TestContext};

#[sinex_test]
async fn migration_lock_blocks_second_holder(ctx: TestContext) -> sinex_test_utils::TestResult<()> {
    let first = try_acquire_migration_lock(ctx.pool()).await?;

    let second: Result<_, _> = try_acquire_migration_lock(ctx.pool()).await;
    assert!(
        matches!(second, Err(err) if err.to_string().contains("already applying migrations")),
        "second acquisition should fail with lock contention"
    );

    drop(first);

    // After releasing the lock, acquisition should succeed again.
    let third = try_acquire_migration_lock(ctx.pool()).await?;
    drop(third);

    Ok(())
}

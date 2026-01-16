use sinex_ingestd::service::try_acquire_migration_lock;
use sinex_test_utils::timing_utils::WaitHelpers;
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

    // `ResourceGuard` cleanup runs asynchronously; wait briefly for `pg_advisory_unlock` to land.
    WaitHelpers::wait_for_condition(
        || {
            let pool = ctx.pool.clone();
            async move {
                match try_acquire_migration_lock(&pool).await {
                    Ok(third) => {
                        drop(third);
                        Ok::<bool, sinex_test_utils::SinexError>(true)
                    }
                    Err(err) if err.to_string().contains("already applying migrations") => {
                        Ok::<bool, sinex_test_utils::SinexError>(false)
                    }
                    Err(err) => Err(err.into()),
                }
            }
        },
        2,
    )
    .await?;

    Ok(())
}

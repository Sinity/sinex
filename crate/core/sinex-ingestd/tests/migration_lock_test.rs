use sinex_ingestd::service::try_acquire_migration_lock;
use sinex_primitives::error::SinexError;
use xtask::sandbox::prelude::*;
use xtask::sandbox::timing::WaitHelpers;

#[sinex_test]
async fn migration_lock_blocks_second_holder(ctx: TestContext) -> TestResult<()> {
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
                        Ok::<bool, SinexError>(true)
                    }
                    Err(err) if err.to_string().contains("already applying migrations") => {
                        Ok::<bool, SinexError>(false)
                    }
                    Err(err) => Err(err),
                }
            }
        },
        2,
    )
    .await?;

    Ok(())
}

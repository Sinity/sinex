use sinex_ingestd::service::try_acquire_migration_lock;
use sinex_test_utils::{sinex_test, TestContext};
use tokio::time::{sleep, Duration, Instant};

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
    let deadline = Instant::now() + Duration::from_secs(1);
    loop {
        match try_acquire_migration_lock(ctx.pool()).await {
            Ok(third) => {
                drop(third);
                break;
            }
            Err(err) if err.to_string().contains("already applying migrations") => {
                if Instant::now() >= deadline {
                    return Err(err.into());
                }
                sleep(Duration::from_millis(10)).await;
            }
            Err(err) => return Err(err.into()),
        }
    }

    Ok(())
}

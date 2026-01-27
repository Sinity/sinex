use sinex_core::db::acquire_with_timeout;
use xtask::sandbox::prelude::*;
use std::time::Duration;

#[sinex_test]
async fn pool_acquire_with_timeout_smoke(ctx: TestContext) -> TestResult<()> {
    let conn = acquire_with_timeout(&ctx.pool, Duration::from_secs(1)).await?;
    drop(conn);
    Ok(())
}

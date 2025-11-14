use sinex_core::db::query_helpers::{
    db_error, with_retry_transaction, with_transaction, RetryConfig,
};
use sinex_test_utils::{sinex_test, TestContext, TestResult};

#[sinex_test]
async fn with_transaction_accepts_async_closures(ctx: TestContext) -> TestResult<()> {
    with_transaction(ctx.pool(), |tx| {
        Box::pin(async move {
            sqlx::query("SELECT 1")
                .execute(&mut **tx)
                .await
                .map_err(|e| db_error(e, "SELECT 1 smoke test"))?;
            Ok(())
        })
    })
    .await?;

    Ok(())
}

#[sinex_test]
async fn with_retry_transaction_accepts_async_closures(ctx: TestContext) -> TestResult<()> {
    let config = RetryConfig::default();

    with_retry_transaction(ctx.pool(), config, |tx| {
        Box::pin(async move {
            sqlx::query("SELECT 1")
                .execute(&mut **tx)
                .await
                .map_err(|e| db_error(e, "SELECT 1 smoke test with retry"))?;
            Ok(())
        })
    })
    .await?;

    Ok(())
}

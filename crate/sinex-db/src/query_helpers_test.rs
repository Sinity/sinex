use super::rollback_failure;
use crate::repositories::DbPoolExt;
use xtask::sandbox::prelude::*;

// Inline because these exercise the private rollback-error composition helper directly.

#[sinex_test]
async fn rollback_failure_preserves_original_error_context() -> TestResult<()> {
    let error = rollback_failure(
        &sinex_primitives::SinexError::validation("original failure"),
        "rollback broke too",
        "with_transaction",
    );

    let rendered = error.to_string();
    assert!(rendered.contains("Failed to rollback transaction after operation error"));
    assert!(rendered.contains("rollback broke too"));
    assert!(rendered.contains("original failure"));
    assert!(rendered.contains("with_transaction"));
    Ok(())
}

#[sinex_test]
async fn db_pool_ext_with_transaction_runs_single_operation(
    ctx: TestContext,
) -> TestResult<()> {
    let value = ctx
        .pool()
        .with_transaction(async |tx| {
            sqlx::query_scalar::<_, i32>("SELECT 41 + 1")
                .fetch_one(&mut **tx)
                .await
                .map_err(|e| crate::db_error(e, "select through transaction helper"))
        })
        .await?;

    ctx.assert("transaction helper result").eq(&value, &42)?;
    Ok(())
}

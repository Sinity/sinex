//! Query helpers tests for sinex-db
//!
//! Tests public functions in `sinex_db::query_helpers`:
//! - `is_retryable_db_error` (pure function)
//! - `with_retry_transaction_idempotent` (requires DB)
//! - `set_repeatable_read` (requires DB)

use sinex_db::SinexError;
use sinex_db::query_helpers::{
    IdempotentTransaction, RetryConfig, is_retryable_db_error, set_repeatable_read,
    with_retry_transaction_idempotent,
};
use xtask::sandbox::prelude::*;

// =============================================================================
// is_retryable_db_error — pure function tests (no DB needed)
// =============================================================================

#[sinex_test]
async fn retryable_deadlock_detected() -> TestResult<()> {
    let err = SinexError::database("deadlock detected");
    assert!(is_retryable_db_error(&err));
    Ok(())
}

#[sinex_test]
async fn retryable_could_not_serialize_access() -> TestResult<()> {
    let err = SinexError::database("could not serialize access due to concurrent update");
    assert!(is_retryable_db_error(&err));
    Ok(())
}

#[sinex_test]
async fn retryable_transaction_rollback() -> TestResult<()> {
    let err = SinexError::database("transaction rollback due to serialization failure");
    assert!(is_retryable_db_error(&err));
    Ok(())
}

#[sinex_test]
async fn retryable_current_transaction_is_aborted() -> TestResult<()> {
    let err = SinexError::database(
        "current transaction is aborted, commands ignored until end of transaction block",
    );
    assert!(is_retryable_db_error(&err));
    Ok(())
}

#[sinex_test]
async fn not_retryable_generic_database_error() -> TestResult<()> {
    let err = SinexError::database("relation \"core.events\" does not exist");
    assert!(!is_retryable_db_error(&err));
    Ok(())
}

#[sinex_test]
async fn not_retryable_constraint_violation() -> TestResult<()> {
    let err = SinexError::database("duplicate key value violates unique constraint");
    assert!(!is_retryable_db_error(&err));
    Ok(())
}

#[sinex_test]
async fn not_retryable_syntax_error() -> TestResult<()> {
    let err = SinexError::database("syntax error at or near SELECT");
    assert!(!is_retryable_db_error(&err));
    Ok(())
}

#[sinex_test]
async fn not_retryable_validation_error() -> TestResult<()> {
    // Non-Database variant — should not be retryable
    let err = SinexError::validation("invalid input");
    assert!(!is_retryable_db_error(&err));
    Ok(())
}

#[sinex_test]
async fn not_retryable_network_error() -> TestResult<()> {
    let err = SinexError::network("connection reset by peer");
    assert!(!is_retryable_db_error(&err));
    Ok(())
}

#[sinex_test]
async fn retryable_message_in_context_chain() -> TestResult<()> {
    // SinexError with retryable message embedded in context
    let err = SinexError::database("query failed")
        .with_context("detail", "deadlock detected between two transactions");
    // The Display impl includes context, so "deadlock detected" should appear in to_string()
    assert!(is_retryable_db_error(&err));
    Ok(())
}

#[sinex_test]
async fn not_retryable_empty_message() -> TestResult<()> {
    let err = SinexError::database("");
    assert!(!is_retryable_db_error(&err));
    Ok(())
}

#[sinex_test]
async fn retryable_substring_in_longer_message() -> TestResult<()> {
    let err = SinexError::database(
        "ERROR: could not serialize access due to read/write dependencies among transactions",
    );
    assert!(is_retryable_db_error(&err));
    Ok(())
}

// =============================================================================
// RetryConfig — structural tests (no DB needed)
// =============================================================================

#[sinex_test]
async fn retry_config_defaults() -> TestResult<()> {
    // RetryConfig is now re-exported from sinex-primitives (issue #746 A8).
    // Field is `multiplier` (was `exponential_base` in the old hand-rolled DB copy).
    let config = RetryConfig::default();
    assert_eq!(config.max_attempts, 3);
    assert_eq!(config.initial_delay, std::time::Duration::from_millis(100));
    assert_eq!(config.max_delay, std::time::Duration::from_secs(1));
    assert!((config.multiplier - 2.0).abs() < f64::EPSILON);
    Ok(())
}

#[sinex_test]
async fn idempotent_transaction_marker() -> TestResult<()> {
    let _marker = IdempotentTransaction::new();
    let _also_new = IdempotentTransaction::new();
    Ok(())
}

// =============================================================================
// set_repeatable_read — requires DB
// =============================================================================

#[sinex_test]
async fn set_repeatable_read_succeeds(ctx: TestContext) -> TestResult<()> {
    let pool = &ctx.pool;

    let mut tx = pool.begin().await?;
    set_repeatable_read(&mut tx).await?;

    // Verify we can still query after setting isolation level
    let row: (i64,) = sqlx::query_as("SELECT 1::bigint")
        .fetch_one(&mut *tx)
        .await?;
    assert_eq!(row.0, 1);

    tx.commit().await?;
    Ok(())
}

#[sinex_test]
async fn set_repeatable_read_isolation_level_persists_in_transaction(
    ctx: TestContext,
) -> TestResult<()> {
    let pool = &ctx.pool;

    let mut tx = pool.begin().await?;
    set_repeatable_read(&mut tx).await?;

    // Query current isolation level
    let row: (String,) = sqlx::query_as("SELECT current_setting('transaction_isolation')")
        .fetch_one(&mut *tx)
        .await?;

    assert_eq!(row.0, "repeatable read");
    tx.rollback().await?;
    Ok(())
}

// =============================================================================
// Manual transaction semantics — test commit/rollback behavior directly
// =============================================================================

#[sinex_test]
async fn transaction_commits_data(ctx: TestContext) -> TestResult<()> {
    let pool = &ctx.pool;

    // Create a regular (not temp) table — visible across all pool connections in this
    // isolated test database. Temp tables are connection-scoped and break with pools.
    sqlx::query("DROP TABLE IF EXISTS public.tx_commit_test")
        .execute(pool)
        .await?;
    sqlx::query("CREATE TABLE public.tx_commit_test (id int PRIMARY KEY)")
        .execute(pool)
        .await?;

    // Commit a transaction
    let mut tx = pool.begin().await?;
    sqlx::query("INSERT INTO public.tx_commit_test (id) VALUES (1)")
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;

    // Verify data persisted
    let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM public.tx_commit_test")
        .fetch_one(pool)
        .await?;
    assert_eq!(count.0, 1);
    sqlx::query("DROP TABLE IF EXISTS public.tx_commit_test")
        .execute(pool)
        .await?;

    Ok(())
}

#[sinex_test]
async fn transaction_rolls_back_data(ctx: TestContext) -> TestResult<()> {
    let pool = &ctx.pool;

    // Create a regular (not temp) table — visible across all pool connections
    sqlx::query("DROP TABLE IF EXISTS public.tx_rollback_test")
        .execute(pool)
        .await?;
    sqlx::query("CREATE TABLE public.tx_rollback_test (id int PRIMARY KEY)")
        .execute(pool)
        .await?;

    // Roll back a transaction
    let mut tx = pool.begin().await?;
    sqlx::query("INSERT INTO public.tx_rollback_test (id) VALUES (1)")
        .execute(&mut *tx)
        .await?;
    tx.rollback().await?;

    // Verify data was not persisted
    let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM public.tx_rollback_test")
        .fetch_one(pool)
        .await?;
    assert_eq!(count.0, 0, "transaction should have been rolled back");
    sqlx::query("DROP TABLE IF EXISTS public.tx_rollback_test")
        .execute(pool)
        .await?;

    Ok(())
}

// =============================================================================
// with_retry_transaction_idempotent — requires DB
// =============================================================================

#[sinex_test]
async fn retry_transaction_commits_on_first_success(ctx: TestContext) -> TestResult<()> {
    let pool = &ctx.pool;
    let config = RetryConfig::default();

    let result: i64 =
        with_retry_transaction_idempotent(pool, config, IdempotentTransaction::new(), |tx| {
            Box::pin(async move {
                let row: (i64,) = sqlx::query_as("SELECT 42::bigint")
                    .fetch_one(&mut **tx)
                    .await
                    .map_err(|e| sinex_db::db_error(e, "select"))?;
                Ok(row.0)
            })
        })
        .await?;

    assert_eq!(result, 42);
    Ok(())
}

#[sinex_test]
async fn retry_transaction_returns_error_on_non_retryable(ctx: TestContext) -> TestResult<()> {
    let pool = &ctx.pool;
    let config = RetryConfig::default();

    let result =
        with_retry_transaction_idempotent(pool, config, IdempotentTransaction::new(), |_tx| {
            Box::pin(async move { Err::<i64, _>(SinexError::validation("not retryable")) })
        })
        .await;

    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("not retryable"),
        "error should propagate the original message, got: {err_msg}"
    );
    Ok(())
}

#[sinex_test]
async fn retry_transaction_respects_max_attempts(ctx: TestContext) -> TestResult<()> {
    let pool = &ctx.pool;
    // Use the builder API — primitives RetryConfig uses bon::Builder (issue #746 A8).
    let config = RetryConfig::builder()
        .max_attempts(2)
        .initial_delay(std::time::Duration::from_millis(10))
        .max_delay(std::time::Duration::from_millis(50))
        .multiplier(2.0)
        .build();

    let result =
        with_retry_transaction_idempotent(pool, config, IdempotentTransaction::new(), |_tx| {
            Box::pin(async move {
                // Always fail with retryable error
                Err::<i64, _>(SinexError::database("deadlock detected"))
            })
        })
        .await;

    // Should fail after max_attempts
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("deadlock detected"),
        "error should contain the original retryable message, got: {err_msg}"
    );

    Ok(())
}

#[sinex_test]
async fn retry_transaction_commits_data_on_success(ctx: TestContext) -> TestResult<()> {
    let pool = &ctx.pool;
    let config = RetryConfig::default();

    // Create a regular (not temp) table — visible across all pool connections
    sqlx::query("DROP TABLE IF EXISTS public.retry_data")
        .execute(pool)
        .await?;
    sqlx::query("CREATE TABLE public.retry_data (value text)")
        .execute(pool)
        .await?;

    with_retry_transaction_idempotent(pool, config, IdempotentTransaction::new(), |tx| {
        Box::pin(async move {
            sqlx::query("INSERT INTO public.retry_data (value) VALUES ('committed')")
                .execute(&mut **tx)
                .await
                .map_err(|e| sinex_db::db_error(e, "insert"))?;
            Ok(())
        })
    })
    .await?;

    // Verify data was committed
    let row: (String,) = sqlx::query_as("SELECT value FROM public.retry_data LIMIT 1")
        .fetch_one(pool)
        .await?;
    assert_eq!(row.0, "committed");
    sqlx::query("DROP TABLE IF EXISTS public.retry_data")
        .execute(pool)
        .await?;

    Ok(())
}

#[sinex_test]
async fn retry_transaction_with_repeatable_read(ctx: TestContext) -> TestResult<()> {
    let pool = &ctx.pool;
    let config = RetryConfig::default();

    let isolation: String =
        with_retry_transaction_idempotent(pool, config, IdempotentTransaction::new(), |tx| {
            Box::pin(async move {
                set_repeatable_read(tx).await?;

                let row: (String,) =
                    sqlx::query_as("SELECT current_setting('transaction_isolation')")
                        .fetch_one(&mut **tx)
                        .await
                        .map_err(|e| sinex_db::db_error(e, "check isolation"))?;
                Ok(row.0)
            })
        })
        .await?;

    assert_eq!(isolation, "repeatable read");
    Ok(())
}

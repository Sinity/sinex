use crate::{DbResult, DbTransaction};
use futures::future::BoxFuture;
use sinex_primitives::SinexError;
use sqlx::PgPool;
use std::time::Duration;
use tokio::time::sleep;
use tracing::warn;

// Re-export db_error for consumers expecting it in query_helpers
pub use crate::db_error;

/// Re-export the canonical retry configuration from sinex-primitives.
///
/// The former hand-rolled DB copy (`exponential_base: f64`) was a duplicate of the
/// richer primitives type (`multiplier: f64`). Callers that previously used struct
/// literal syntax with `exponential_base` must use `multiplier` — see issue #746 (A8).
pub use sinex_primitives::utils::wait_helpers::RetryConfig;

fn rollback_failure(
    original_error: &SinexError,
    rollback_error: impl std::fmt::Display,
    operation: &'static str,
) -> SinexError {
    SinexError::database("Failed to rollback transaction after operation error")
        .with_source(rollback_error.to_string())
        .with_context("original_error", original_error.to_string())
        .with_operation(operation)
}

/// Marker indicating a retryable transaction is idempotent.
#[derive(Debug, Clone, Copy)]
pub struct IdempotentTransaction;

impl Default for IdempotentTransaction {
    fn default() -> Self {
        Self::new()
    }
}

impl IdempotentTransaction {
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

/// Set transaction isolation level to REPEATABLE READ
pub async fn set_repeatable_read(tx: &mut DbTransaction<'_>) -> DbResult<()> {
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .execute(&mut **tx)
        .await
        .map_err(|e| db_error(e, "set repeatable read isolation"))?;
    Ok(())
}

/// Execute a retryable transaction explicitly marked as idempotent.
pub async fn with_retry_transaction_idempotent<F, T>(
    pool: &PgPool,
    config: RetryConfig,
    _idempotent: IdempotentTransaction,
    mut f: F,
) -> DbResult<T>
where
    F: for<'borrow> FnMut(&'borrow mut DbTransaction<'_>) -> BoxFuture<'borrow, DbResult<T>>,
{
    let mut attempts = 0;
    let mut delay = config.initial_delay;

    loop {
        attempts += 1;

        let mut tx = pool
            .begin()
            .await
            .map_err(|e| db_error(e, "Failed to begin transaction"))?;

        let outcome = f(&mut tx).await;

        match outcome {
            Ok(result) => {
                tx.commit()
                    .await
                    .map_err(|e| db_error(e, "Failed to commit transaction"))?;
                return Ok(result);
            }
            Err(e) if is_retryable_db_error(&e) && attempts < config.max_attempts => {
                if let Err(rollback_err) = tx.rollback().await {
                    return Err(rollback_failure(
                        &e,
                        rollback_err,
                        "with_retry_transaction_idempotent",
                    ));
                }
                warn!(
                    "Retryable database error (attempt {}/{}): {}",
                    attempts, config.max_attempts, e
                );
                sleep(delay).await;
                delay = std::cmp::min(delay.mul_f64(config.multiplier), config.max_delay);
            }
            Err(e) => {
                return Err(match tx.rollback().await {
                    Ok(()) => e,
                    Err(rollback_err) => {
                        rollback_failure(&e, rollback_err, "with_retry_transaction_idempotent")
                    }
                });
            }
        }
    }
}

/// Check if a database error is retryable
#[must_use]
pub fn is_retryable_db_error(err: &SinexError) -> bool {
    // Prefer typed SQLSTATE classification from db_error() context.
    // Class 40 = transaction rollback (includes serialization/deadlock).
    if let Some(sqlstate) = err.context_map().get("sqlstate")
        && (sqlstate.starts_with("40") || sqlstate == "25P02")
    {
        return true;
    }

    // Preserve legacy message-based classification for plain Database errors
    // created without SQLSTATE context.
    let rendered = err.to_string().to_lowercase();
    rendered.contains("deadlock detected")
        || rendered.contains("could not serialize access")
        || rendered.contains("transaction rollback")
        || rendered.contains("current transaction is aborted")
}

/// Execute a single operation within a transaction.
pub async fn with_transaction<F, T>(pool: &PgPool, f: F) -> DbResult<T>
where
    F: for<'tx> AsyncFnOnce(&'tx mut DbTransaction<'_>) -> DbResult<T>,
{
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| db_error(e, "begin transaction"))?;

    match f(&mut tx).await {
        Ok(result) => {
            tx.commit()
                .await
                .map_err(|e| db_error(e, "commit transaction"))?;
            Ok(result)
        }
        Err(e) => Err(match tx.rollback().await {
            Ok(()) => e,
            Err(rollback_err) => rollback_failure(&e, rollback_err, "with_transaction"),
        }),
    }
}

/// Check if a row exists matching the query
pub async fn exists<'a, E>(
    executor: E,
    query: sqlx::query::Query<'a, sqlx::Postgres, sqlx::postgres::PgArguments>,
) -> DbResult<bool>
where
    E: sqlx::Executor<'a, Database = sqlx::Postgres>,
{
    query
        .fetch_optional(executor)
        .await
        .map(|row| row.is_some())
        .map_err(|e| db_error(e, "check exists"))
}

/// Count rows matching a query
pub async fn count<'a, E>(
    executor: E,
    query: sqlx::query::QueryScalar<'a, sqlx::Postgres, i64, sqlx::postgres::PgArguments>,
) -> DbResult<i64>
where
    E: sqlx::Executor<'a, Database = sqlx::Postgres>,
{
    query
        .fetch_one(executor)
        .await
        .map_err(|e| db_error(e, "count rows"))
}

#[cfg(test)]
mod tests {
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
}

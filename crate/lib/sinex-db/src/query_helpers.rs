use crate::{DbResult, DbTransaction};
use futures::future::BoxFuture;
use sinex_primitives::SinexError;
use sqlx::PgPool;
use std::time::Duration;
use tokio::time::sleep;
use tracing::warn;

// Local constants replacing sinex-primitives types dependencies
pub const MAX_RETRY_ATTEMPTS: u32 = 3;
pub const DEFAULT_INITIAL_DELAY: Duration = Duration::from_millis(100);
pub const MAX_DELAY: Duration = Duration::from_secs(5);
pub const EXPONENTIAL_BASE: f64 = 2.0;

// Re-export db_error for consumers expecting it in query_helpers
pub use crate::db_error;

/// Configuration for transaction retry behavior
#[derive(Debug, Clone, Copy)]
pub struct RetryConfig {
    pub max_attempts: u32,
    pub initial_delay: Duration,
    pub max_delay: Duration,
    pub exponential_base: f64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: MAX_RETRY_ATTEMPTS,
            initial_delay: DEFAULT_INITIAL_DELAY,
            max_delay: MAX_DELAY,
            exponential_base: EXPONENTIAL_BASE,
        }
    }
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
                    warn!(
                        "Failed to rollback transaction (attempt {}/{}): {}",
                        attempts, config.max_attempts, rollback_err
                    );
                }
                warn!(
                    "Retryable database error (attempt {}/{}): {}",
                    attempts, config.max_attempts, e
                );
                sleep(delay).await;
                delay = std::cmp::min(delay.mul_f64(config.exponential_base), config.max_delay);
            }
            Err(e) => {
                let _ = tx.rollback().await;
                return Err(e);
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

    // Fall back to variant-level retryability for non-SQL database wrappers.
    err.is_retryable()
}

/// Execute a closure within a transaction
pub async fn with_transaction<F, Fut, T>(pool: &PgPool, mut f: F) -> DbResult<T>
where
    F: FnMut(&mut DbTransaction<'_>) -> Fut,
    Fut: std::future::Future<Output = DbResult<T>>,
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
        Err(e) => {
            let _ = tx.rollback().await;
            Err(e)
        }
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

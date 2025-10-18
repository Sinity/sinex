#![doc = include_str!("../../doc/query_helpers.md")]

use crate::types::error::{Result as SinexResult, SinexError};
use crate::types::{retry, timeouts};
use crate::{DbPool, DbPoolRef};
use sea_query::{Alias, Expr, Func, PostgresQueryBuilder, Query};
use sqlx::{Error as SqlxError, Postgres, Transaction};
use std::future::Future;
use std::time::Duration;
use tokio::time::sleep;
use tracing::warn;

// Re-export ULID conversion utilities from sinex-schema for convenience
pub use sinex_schema::ulid_conversions::*;

/// Convert sqlx errors to SinexError with context
pub fn db_error(err: SqlxError, context: &str) -> SinexError {
    match err {
        SqlxError::RowNotFound => SinexError::not_found(context.to_string()),
        SqlxError::Database(db_err) => {
            if db_err.is_unique_violation() {
                SinexError::database(format!("{context}: unique constraint violation"))
            } else if db_err.is_foreign_key_violation() {
                SinexError::database(format!("{context}: foreign key violation"))
            } else {
                SinexError::database(format!("{context}: {db_err}"))
            }
        }
        _ => SinexError::database(format!("{context}: {err}")),
    }
}

// ===== Transaction Helpers =====

/// Configuration for transaction retry behavior
pub struct RetryConfig {
    pub max_attempts: u32,
    pub initial_delay: Duration,
    pub max_delay: Duration,
    pub exponential_base: f64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: retry::MAX_RETRY_ATTEMPTS,
            initial_delay: timeouts::DEFAULT_TERMINAL_POLL_INTERVAL,
            max_delay: timeouts::RETRY_MAX_DELAY,
            exponential_base: retry::BACKOFF_MULTIPLIER as f64,
        }
    }
}

/// Execute a function within a transaction with automatic rollback on error
pub async fn with_transaction<F, Fut, T>(pool: &DbPool, f: F) -> SinexResult<T>
where
    F: FnOnce(&mut Transaction<'static, Postgres>) -> Fut,
    Fut: Future<Output = SinexResult<T>>,
{
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| db_error(e, "Failed to begin transaction"))?;

    match f(&mut tx).await {
        Ok(result) => {
            tx.commit()
                .await
                .map_err(|e| db_error(e, "Failed to commit transaction"))?;
            Ok(result)
        }
        Err(e) => {
            // Transaction will be automatically rolled back on drop
            Err(e)
        }
    }
}

/// Execute a function within a transaction with retry logic for deadlocks
pub async fn with_retry_transaction<F, Fut, T>(
    pool: &DbPool,
    config: RetryConfig,
    f: F,
) -> SinexResult<T>
where
    F: Fn(&mut Transaction<'static, Postgres>) -> Fut,
    Fut: Future<Output = SinexResult<T>>,
{
    let mut attempts = 0;
    let mut delay = config.initial_delay;

    loop {
        attempts += 1;

        let mut tx = pool
            .begin()
            .await
            .map_err(|e| db_error(e, "Failed to begin transaction"))?;

        match f(&mut tx).await {
            Ok(result) => {
                tx.commit()
                    .await
                    .map_err(|e| db_error(e, "Failed to commit transaction"))?;
                return Ok(result);
            }
            Err(e) if is_retryable_db_error(&e) && attempts < config.max_attempts => {
                warn!(
                    "Retryable database error (attempt {}/{}): {}",
                    attempts, config.max_attempts, e
                );
                sleep(delay).await;
                delay = std::cmp::min(delay.mul_f64(config.exponential_base), config.max_delay);
                continue;
            }
            Err(e) => return Err(e),
        }
    }
}

/// Check if a database error is retryable (deadlock, serialization failure)
pub fn is_retryable_db_error(err: &SinexError) -> bool {
    let msg = err.to_string();
    msg.contains("deadlock detected")
        || msg.contains("could not serialize access")
        || msg.contains("transaction rollback")
}

/// Check if a record exists
pub async fn exists(
    pool: DbPoolRef<'_>,
    table: &str,
    where_clause: &str,
    context: &str,
) -> SinexResult<bool> {
    let subquery = Query::select()
        .expr(Expr::val(1))
        .from(Alias::new(table))
        .cond_where(Expr::cust(where_clause))
        .to_owned();

    let query = Query::select().expr(Expr::exists(subquery)).to_owned();

    let (sql, _values) = query.build(PostgresQueryBuilder);

    // Since we're using Expr::cust() for where_clause, no additional parameters to bind
    let result: (bool,) = sqlx::query_as(&sql)
        .fetch_one(pool)
        .await
        .map_err(|e| db_error(e, context))?;

    Ok(result.0)
}

/// Count records matching a condition
pub async fn count(
    pool: DbPoolRef<'_>,
    table: &str,
    where_clause: Option<&str>,
    context: &str,
) -> SinexResult<i64> {
    let mut query = Query::select()
        .expr(Func::count(Expr::cust("*")))
        .from(Alias::new(table))
        .to_owned();

    if let Some(clause) = where_clause {
        query = query.cond_where(Expr::cust(clause)).to_owned();
    }
    let (sql, _values) = query.build(PostgresQueryBuilder);

    // Since we're using Expr::cust() for dynamic parts, no additional parameters to bind
    let result: (i64,) = sqlx::query_as(&sql)
        .fetch_one(pool)
        .await
        .map_err(|e| db_error(e, context))?;

    Ok(result.0)
}

// ULID/UUID conversion utilities are now provided by sinex-schema::ulid_conversions
// and re-exported above for convenience

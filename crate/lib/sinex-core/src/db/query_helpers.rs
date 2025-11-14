#![doc = include_str!("../../doc/query_helpers.md")]

use crate::types::error::{Result as SinexResult, SinexError};
use crate::types::{retry, timeouts};
use crate::{DbPool, DbPoolRef, DbTransaction};
use futures::future::BoxFuture;
use sqlx::{Error as SqlxError, Postgres, QueryBuilder};
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
pub async fn with_transaction<F, T>(pool: &DbPool, f: F) -> SinexResult<T>
where
    F: for<'borrow> FnOnce(&'borrow mut DbTransaction<'_>) -> BoxFuture<'borrow, SinexResult<T>>,
{
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| db_error(e, "Failed to begin transaction"))?;

    let result = f(&mut tx).await?;
    tx.commit()
        .await
        .map_err(|e| db_error(e, "Failed to commit transaction"))?;
    Ok(result)
}

/// Execute a function within a transaction with retry logic for deadlocks
pub async fn with_retry_transaction<F, T>(
    pool: &DbPool,
    config: RetryConfig,
    mut f: F,
) -> SinexResult<T>
where
    F: for<'borrow> FnMut(&'borrow mut DbTransaction<'_>) -> BoxFuture<'borrow, SinexResult<T>>,
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
    let mut builder = QueryBuilder::<Postgres>::new("SELECT EXISTS(SELECT 1 FROM ");
    push_identifier(&mut builder, table);
    if !where_clause.trim().is_empty() {
        builder.push(" WHERE ");
        builder.push(where_clause);
    }
    builder.push(")");

    let exists = builder
        .build_query_scalar::<bool>()
        .fetch_one(pool)
        .await
        .map_err(|e| db_error(e, context))?;

    Ok(exists)
}

/// Count records matching a condition
pub async fn count(
    pool: DbPoolRef<'_>,
    table: &str,
    where_clause: Option<&str>,
    context: &str,
) -> SinexResult<i64> {
    let mut builder = QueryBuilder::<Postgres>::new("SELECT COUNT(*) FROM ");
    push_identifier(&mut builder, table);

    if let Some(clause) = where_clause {
        if !clause.trim().is_empty() {
            builder.push(" WHERE ");
            builder.push(clause);
        }
    }

    let count = builder
        .build_query_scalar::<i64>()
        .fetch_one(pool)
        .await
        .map_err(|e| db_error(e, context))?;

    Ok(count)
}

fn push_identifier(builder: &mut QueryBuilder<Postgres>, ident: &str) {
    let escaped = ident.replace('"', "\"\"");
    builder.push(format_args!("\"{}\"", escaped));
}

// ULID/UUID conversion utilities are now provided by sinex-schema::ulid_conversions
// and re-exported above for convenience

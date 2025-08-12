//! Query execution helpers for reducing boilerplate and providing consistent error handling
//!
//! This module provides helper functions for common database operations with
//! transaction support and retry logic.
//!
//! For ULID/UUID conversions, use the functions from `sinex_schema::ulid_conversions`.
//!
//! # Examples
//!
//! ## Using function helpers:
//!
//! ```rust,no_run
//! use crate::prelude::*;
//! use sinex_core::RawEvent;
//!
//! # async fn example(pool: &DbPool) -> SinexResult<()> {
//! // Simple query with automatic error context
//! let event_id = Ulid::new();
//! let event: Event = query_one(pool, "SELECT * FROM core.events WHERE id = $1", ulid_to_uuid(event_id), "get event by id").await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Transaction helpers:
//!
//! ```ignore
//! use crate::prelude::*;
//!
//! # async fn example(pool: &DbPool) -> SinexResult<()> {
//! // Simple transaction with automatic rollback on error
//! let result = with_transaction(pool, |tx| async move {
//!     // Your transactional operations here
//!     let rows = sqlx::query("INSERT INTO table VALUES ($1)")
//!         .bind("value")
//!         .execute(&mut **tx)
//!         .await
//!         .map_err(|e| db_error(e, "insert operation"))?;
//!     Ok(rows.rows_affected())
//! }).await?;
//!
//! // Transaction with retry logic for deadlocks
//! let retry_config = RetryConfig::default();
//! let result = with_retry_transaction(pool, retry_config, |tx| async move {
//!     // Operations that might encounter deadlocks  
//!     let rows = sqlx::query("UPDATE table SET value = $1 WHERE id = $2")
//!         .bind("new_value")
//!         .bind(123)
//!         .execute(&mut **tx)
//!         .await
//!         .map_err(|e| db_error(e, "update operation"))?;
//!     Ok(rows.rows_affected())
//! }).await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## ULID conversion helpers:
//!
//! ```rust,no_run
//! use crate::prelude::*;
//!
//! # fn example() {
//! let ulid = Ulid::new();
//! let uuid = ulid_to_uuid(ulid);  // For database storage
//! let ulid_back = uuid_to_ulid(uuid);  // From database
//!
//! let ulids = vec![Ulid::new(), Ulid::new()];
//! let uuids = ulids.to_uuid_vec();  // Convert arrays efficiently
//! # }
//! ```

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
                SinexError::database(format!("{}: unique constraint violation", context))
            } else if db_err.is_foreign_key_violation() {
                SinexError::database(format!("{}: foreign key violation", context))
            } else {
                SinexError::database(format!("{}: {}", context, db_err))
            }
        }
        _ => SinexError::database(format!("{}: {}", context, err)),
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

#[cfg(test)]
mod tests {
    use super::*;
    use sinex_test_utils::{sinex_test, TestContext};

    use color_eyre::eyre::Result;

    use serde_json::json;

    #[sinex_test]
    async fn test_retry_config_default(ctx: TestContext) -> color_eyre::eyre::Result<()> {
        let config = RetryConfig::default();
        assert_eq!(config.max_attempts, retry::MAX_RETRY_ATTEMPTS);
        assert_eq!(
            config.initial_delay,
            timeouts::DEFAULT_TERMINAL_POLL_INTERVAL
        );
        assert_eq!(config.max_delay, timeouts::RETRY_MAX_DELAY);
        assert_eq!(config.exponential_base, retry::BACKOFF_MULTIPLIER as f64);
        Ok(())
    }

    #[sinex_test]
    async fn test_is_retryable_db_error_function_exists(
        ctx: TestContext,
    ) -> color_eyre::eyre::Result<()> {
        // Test that timeout errors are not retryable
        let timeout_err = SinexError::timeout("test timeout");
        assert!(!is_retryable_db_error(&timeout_err));

        // Test that general database errors are not retryable by default
        let db_err = SinexError::database("test database error");
        assert!(!is_retryable_db_error(&db_err));

        // Note: To properly test retryable errors, we'd need to create a SinexError
        // with a message containing the specific strings we check for
        Ok(())
    }

    // =============================================================================
    // ULID Parsing Error Tests
    // =============================================================================

    // ULID parsing and conversion tests have been moved to sinex-schema::ulid_conversions
}

//! Query execution helpers for reducing boilerplate and providing consistent error handling
//!
//! This module provides helper functions for common database operations with automatic
//! ULID<->UUID conversion, transaction support, and retry logic.
//!
//! # ULID/UUID Conversion Convention
//!
//! When working with database queries in sinex-db, always use the conversion
//! functions from this module:
//!
//! - `ulid_to_uuid()` for ULID → UUID conversion before database operations
//! - `uuid_to_ulid()` for UUID → ULID conversion after database fetches
//! - `UlidArrayExt` trait for batch conversions
//!
//! **DO NOT** use `.to_uuid()` method directly on ULID types. This ensures
//! consistency and makes conversions explicit at database boundaries.
//!
//! # Examples
//!
//! ## Using function helpers:
//!
//! ```rust,no_run
//! use crate::prelude::*;
//! use sinex_core::db::models::Event;
//!
//! # async fn example(pool: &DbPool) -> SinexResult<()> {
//! // Simple query with automatic error context
//! let event_id = Ulid::new();
//! let event: Event = query_one(pool, "SELECT * FROM core.events WHERE event_id = $1", ulid_to_uuid(event_id), "get event by id").await?;
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

use crate::{DbPool, DbPoolRef};
use sea_query::{Alias, Expr, Func, PostgresQueryBuilder, Query};
use sinex_core::types::error::{Result as SinexResult, SinexError};
use sinex_core::types::ulid::Ulid;
use sinex_core::types::{retry, timeouts};
use sqlx::{Error as SqlxError, Postgres, Transaction};
use std::future::Future;
use std::time::Duration;
use tokio::time::sleep;
use tracing::warn;

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

// ===== Unified ULID/UUID Database Integration =====
//
// This section provides a comprehensive API for ULID/UUID conversions at database boundaries.
// PostgreSQL stores ULIDs as UUIDs for efficiency, so we need seamless conversion.

use sqlx::types::Uuid as SqlxUuid;

/// Convert ULID to PostgreSQL UUID type (primary conversion function)
#[inline]
pub fn ulid_to_uuid(ulid: Ulid) -> SqlxUuid {
    SqlxUuid::from_bytes(*ulid.to_uuid().as_bytes())
}

/// Convert PostgreSQL UUID to ULID (primary conversion function)
#[inline]
pub fn uuid_to_ulid(uuid: SqlxUuid) -> Ulid {
    Ulid::from_uuid(uuid::Uuid::from_bytes(*uuid.as_bytes()))
}

// Shorter aliases for common use
pub use ulid_to_uuid as to_db;
pub use uuid_to_ulid as from_db;

/// Extension trait for ULID types to provide database conversions
pub trait UlidExt: Sized {
    /// Convert to database UUID representation
    fn to_db(&self) -> SqlxUuid;

    /// Convert an optional ULID to optional database UUID
    fn to_db_opt(opt: Option<Self>) -> Option<SqlxUuid>;
}

impl UlidExt for Ulid {
    #[inline]
    fn to_db(&self) -> SqlxUuid {
        ulid_to_uuid(*self)
    }

    #[inline]
    fn to_db_opt(opt: Option<Self>) -> Option<SqlxUuid> {
        opt.map(|ulid| ulid.to_db())
    }
}

/// Extension trait for database UUID types to provide ULID conversions
pub trait DbUuidExt {
    /// Convert from database UUID to ULID
    fn to_ulid(self) -> Ulid;
}

impl DbUuidExt for SqlxUuid {
    #[inline]
    fn to_ulid(self) -> Ulid {
        uuid_to_ulid(self)
    }
}

/// Helper trait for ULID collections
pub trait UlidArrayExt {
    fn to_uuid_vec(&self) -> Vec<SqlxUuid>;
    fn to_db_vec(&self) -> Vec<SqlxUuid> {
        self.to_uuid_vec()
    }
}

impl<T: AsRef<[Ulid]>> UlidArrayExt for T {
    fn to_uuid_vec(&self) -> Vec<SqlxUuid> {
        self.as_ref().iter().map(|&id| ulid_to_uuid(id)).collect()
    }
}

/// Extension trait for collections of database UUIDs
pub trait DbUuidCollectionExt {
    /// Convert collection of database UUIDs to ULIDs
    fn to_ulid_vec(self) -> Vec<Ulid>;
}

impl DbUuidCollectionExt for Vec<SqlxUuid> {
    fn to_ulid_vec(self) -> Vec<Ulid> {
        self.into_iter().map(uuid_to_ulid).collect()
    }
}

impl DbUuidCollectionExt for Option<Vec<SqlxUuid>> {
    fn to_ulid_vec(self) -> Vec<Ulid> {
        self.map(|v| v.to_ulid_vec()).unwrap_or_default()
    }
}

/// Convenience functions for common optional patterns
#[inline]
pub fn opt_to_db(ulid: Option<Ulid>) -> Option<SqlxUuid> {
    ulid.map(ulid_to_uuid)
}

#[inline]
pub fn opt_from_db(uuid: Option<SqlxUuid>) -> Option<Ulid> {
    uuid.map(uuid_to_ulid)
}

#[inline]
pub fn opt_vec_to_db(ulids: Option<Vec<Ulid>>) -> Option<Vec<SqlxUuid>> {
    ulids.map(|v| v.to_uuid_vec())
}

#[inline]
pub fn opt_vec_from_db(uuids: Option<Vec<SqlxUuid>>) -> Option<Vec<Ulid>> {
    uuids.map(|v| v.to_ulid_vec())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinex_test_utils::prelude::*;

    #[sinex_test]
    async fn test_ulid_conversion(ctx: TestContext) -> color_eyre::eyre::Result<()> {
        let ulid = Ulid::new();
        let uuid = ulid_to_uuid(ulid);
        let converted_back = uuid_to_ulid(uuid);
        assert_eq!(ulid, converted_back);
        Ok(())
    }

    #[sinex_test]
    async fn test_ulid_array_conversion(ctx: TestContext) -> color_eyre::eyre::Result<()> {
        let ulids = vec![Ulid::new(), Ulid::new(), Ulid::new()];
        let uuids = ulids.to_uuid_vec();
        assert_eq!(ulids.len(), uuids.len());

        for (ulid, uuid) in ulids.iter().zip(uuids.iter()) {
            assert_eq!(*ulid, uuid_to_ulid(*uuid));
        }
        Ok(())
    }

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
}

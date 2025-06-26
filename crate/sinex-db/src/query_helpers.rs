//! Query execution helpers for reducing boilerplate and providing consistent error handling
//!
//! This module provides helper functions for common database operations with automatic
//! ULID<->UUID conversion, transaction support, and retry logic.
//!
//! # Examples
//!
//! ## Using function helpers:
//!
//! ```rust,no_run
//! use sinex_db::prelude::*;
//!
//! # async fn example(pool: &DbPool) -> DbResult<()> {
//! // Simple query with automatic error context
//! let event: RawEvent = query_one(pool, "SELECT * FROM raw.events WHERE id = $1::uuid::ulid", "get event by id").await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Transaction helpers:
//!
//! ```ignore
//! use sinex_db::prelude::*;
//!
//! # async fn example(pool: &DbPool) -> DbResult<()> {
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
//! use sinex_db::prelude::*;
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
use sinex_ulid::Ulid;
use sqlx::{postgres::PgRow, Error as SqlxError, FromRow, Postgres, Transaction};
use std::future::Future;
use std::time::Duration;
use tokio::time::sleep;
use tracing::warn;
use thiserror::Error;

/// Database operation error type
#[derive(Error, Debug)]
pub enum DbError {
    #[error("Database error: {context}: {source}")]
    Query { context: String, source: SqlxError },
    
    #[error("Database timeout: {context}")]
    Timeout { context: String },
    
    #[error("Transaction error: {0}")]
    Transaction(String),
}

/// Convert sqlx errors to DbError with context
pub fn db_error(err: SqlxError, context: &str) -> DbError {
    DbError::Query {
        context: context.to_string(),
        source: err,
    }
}

/// Result type using DbError
pub type DbResult<T> = std::result::Result<T, DbError>;

// ===== Removed Query Execution Helpers =====
// These were unused abstractions that added complexity without value.
// Use sqlx::query! macros directly instead.


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
            max_attempts: 3,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(5),
            exponential_base: 2.0,
        }
    }
}

/// Execute a function within a transaction with automatic rollback on error
pub async fn with_transaction<F, Fut, T>(
    pool: &DbPool,
    f: F,
) -> DbResult<T>
where
    F: FnOnce(&mut Transaction<'static, Postgres>) -> Fut,
    Fut: Future<Output = DbResult<T>>,
{
    let mut tx = pool.begin()
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
) -> DbResult<T>
where
    F: Fn(&mut Transaction<'static, Postgres>) -> Fut,
    Fut: Future<Output = DbResult<T>>,
{
    let mut attempts = 0;
    let mut delay = config.initial_delay;

    loop {
        attempts += 1;
        
        let mut tx = pool.begin()
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
                delay = std::cmp::min(
                    delay.mul_f64(config.exponential_base),
                    config.max_delay,
                );
                continue;
            }
            Err(e) => return Err(e),
        }
    }
}

/// Check if a database error is retryable (deadlock, serialization failure)
pub fn is_retryable_db_error(err: &DbError) -> bool {
    match err {
        DbError::Query { source, .. } => {
            let msg = source.to_string();
            msg.contains("deadlock detected") || 
            msg.contains("could not serialize access") ||
            msg.contains("transaction rollback")
        }
        _ => false,
    }
}

// ===== Removed Common Query Patterns =====
// Removed insert_and_return, update_where, delete_where - barely used abstractions.
// Use sqlx::query! macros directly for better type safety and clarity.

/// Check if a record exists
pub async fn exists(
    pool: DbPoolRef<'_>,
    table: &str,
    where_clause: &str,
    context: &str,
) -> DbResult<bool> {
    let query = format!(
        "SELECT EXISTS(SELECT 1 FROM {} WHERE {})",
        table, where_clause
    );
    
    let result: (bool,) = sqlx::query_as(&query)
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
) -> DbResult<i64> {
    let query = match where_clause {
        Some(clause) => format!("SELECT COUNT(*) FROM {} WHERE {}", table, clause),
        None => format!("SELECT COUNT(*) FROM {}", table),
    };
    
    let result: (i64,) = sqlx::query_as(&query)
        .fetch_one(pool)
        .await
        .map_err(|e| db_error(e, context))?;
    
    Ok(result.0)
}

// ===== ULID Conversion Helpers =====

/// Convert ULID to UUID for database storage
pub fn ulid_to_uuid(ulid: Ulid) -> sqlx::types::Uuid {
    sqlx::types::Uuid::from_bytes(*ulid.to_uuid().as_bytes())
}

/// Convert UUID from database to ULID
pub fn uuid_to_ulid(uuid: sqlx::types::Uuid) -> Ulid {
    Ulid::from_uuid(uuid::Uuid::from_bytes(*uuid.as_bytes()))
}

/// Helper trait for ULID arrays
pub trait UlidArrayExt {
    fn to_uuid_vec(&self) -> Vec<sqlx::types::Uuid>;
}

impl UlidArrayExt for &[Ulid] {
    fn to_uuid_vec(&self) -> Vec<sqlx::types::Uuid> {
        self.iter().map(|&id| ulid_to_uuid(id)).collect()
    }
}

impl UlidArrayExt for Vec<Ulid> {
    fn to_uuid_vec(&self) -> Vec<sqlx::types::Uuid> {
        self.iter().map(|&id| ulid_to_uuid(id)).collect()
    }
}

// ===== Removed Macro Helpers =====
// Removed bind_ulid! and bind_optional_ulid! macros - never used.
// Use ulid_to_uuid() function directly for explicit, readable code.


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ulid_conversion() {
        let ulid = Ulid::new();
        let uuid = ulid_to_uuid(ulid);
        let converted_back = uuid_to_ulid(uuid);
        assert_eq!(ulid, converted_back);
    }

    #[test]
    fn test_ulid_array_conversion() {
        let ulids = vec![Ulid::new(), Ulid::new(), Ulid::new()];
        let uuids = ulids.to_uuid_vec();
        assert_eq!(ulids.len(), uuids.len());
        
        for (ulid, uuid) in ulids.iter().zip(uuids.iter()) {
            assert_eq!(*ulid, uuid_to_ulid(*uuid));
        }
    }

    #[test]
    fn test_retry_config_default() {
        let config = RetryConfig::default();
        assert_eq!(config.max_attempts, 3);
        assert_eq!(config.initial_delay, Duration::from_millis(100));
        assert_eq!(config.max_delay, Duration::from_secs(5));
        assert_eq!(config.exponential_base, 2.0);
    }

    #[test]
    fn test_is_retryable_db_error_function_exists() {
        // Test that timeout errors are not retryable
        let timeout_err = DbError::Timeout {
            context: "test timeout".to_string(),
        };
        assert!(!is_retryable_db_error(&timeout_err));
        
        // Test that transaction errors are not retryable by default
        let tx_err = DbError::Transaction("test".to_string());
        assert!(!is_retryable_db_error(&tx_err));
    }
}
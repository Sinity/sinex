//! Query execution helpers for reducing boilerplate and providing consistent error handling
//!
//! This module provides a fluent API for common database operations with automatic
//! ULID<->UUID conversion, transaction support, and retry logic.
//!
//! # Examples
//!
//! ## Using the fluent QueryBuilder API:
//!
//! ```rust,no_run
//! use sinex_db::prelude::*;
//! use std::time::Duration;
//!
//! # async fn example(pool: &DbPool) -> DbResult<()> {
//! // Simple query with automatic error context
//! let events: Vec<RawEvent> = QueryBuilder::new(pool)
//!     .sql("SELECT * FROM raw.events WHERE source = $1 ORDER BY ts_ingest DESC LIMIT $2")
//!     .context("Fetching recent events by source")
//!     .timeout(Duration::from_secs(5))
//!     .fetch_all()
//!     .await?;
//!
//! // Using macros for even more concise queries
//! let event: RawEvent = query_one!(pool, "SELECT * FROM raw.events WHERE id = $1::uuid::ulid").await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Transaction helpers:
//!
//! ```rust,no_run
//! use sinex_db::prelude::*;
//!
//! # async fn example(pool: &DbPool) -> DbResult<()> {
//! // Simple transaction with automatic rollback on error
//! let result = with_transaction(pool, |tx| async move {
//!     // Your transactional operations here
//!     query_one!(tx, "INSERT INTO table VALUES ($1) RETURNING id").await
//! }).await?;
//!
//! // Transaction with retry logic for deadlocks
//! let retry_config = RetryConfig::default();
//! let result = with_retry_transaction(pool, retry_config, |tx| async move {
//!     // Operations that might encounter deadlocks
//!     query_one!(tx, "UPDATE table SET value = $1 WHERE id = $2").await
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

// ===== Query Execution Helpers =====

/// Execute a query that returns a single row
pub async fn query_one<T>(
    pool: DbPoolRef<'_>,
    query: &str,
    context: &str,
) -> DbResult<T>
where
    T: for<'r> FromRow<'r, PgRow> + Send + Unpin,
{
    sqlx::query_as::<_, T>(query)
        .fetch_one(pool)
        .await
        .map_err(|e| db_error(e, context))
}

/// Execute a query that returns multiple rows
pub async fn query_many<T>(
    pool: DbPoolRef<'_>,
    query: &str,
    context: &str,
) -> DbResult<Vec<T>>
where
    T: for<'r> FromRow<'r, PgRow> + Send + Unpin,
{
    sqlx::query_as::<_, T>(query)
        .fetch_all(pool)
        .await
        .map_err(|e| db_error(e, context))
}

/// Execute a query that might return a row
pub async fn query_optional<T>(
    pool: DbPoolRef<'_>,
    query: &str,
    context: &str,
) -> DbResult<Option<T>>
where
    T: for<'r> FromRow<'r, PgRow> + Send + Unpin,
{
    sqlx::query_as::<_, T>(query)
        .fetch_optional(pool)
        .await
        .map_err(|e| db_error(e, context))
}

/// Execute a query without returning results
pub async fn execute(
    pool: DbPoolRef<'_>,
    query: &str,
    context: &str,
) -> DbResult<u64> {
    sqlx::query(query)
        .execute(pool)
        .await
        .map(|r| r.rows_affected())
        .map_err(|e| db_error(e, context))
}

// ===== Query Builder Pattern =====

/// Fluent query builder for common patterns
pub struct QueryBuilder<'a> {
    pool: DbPoolRef<'a>,
    query: String,
    context: String,
    timeout: Option<Duration>,
}

impl<'a> QueryBuilder<'a> {
    /// Create a new query builder
    pub fn new(pool: DbPoolRef<'a>) -> Self {
        Self {
            pool,
            query: String::new(),
            context: "Query execution".to_string(),
        timeout: None,
        }
    }

    /// Set the query SQL
    pub fn sql(mut self, query: impl Into<String>) -> Self {
        self.query = query.into();
        self
    }

    /// Set error context for better error messages
    pub fn context(mut self, context: impl Into<String>) -> Self {
        self.context = context.into();
        self
    }

    /// Set query timeout
    pub fn timeout(mut self, duration: Duration) -> Self {
        self.timeout = Some(duration);
        self
    }

    /// Execute and return one row
    pub async fn fetch_one<T>(self) -> DbResult<T>
    where
        T: for<'r> FromRow<'r, PgRow> + Send + Unpin,
    {
        let query = sqlx::query_as::<_, T>(&self.query);
        
        if let Some(timeout) = self.timeout {
            tokio::time::timeout(timeout, query.fetch_one(self.pool))
                .await
                .map_err(|_| DbError::Timeout { context: self.context.clone() })?
                .map_err(|e| db_error(e, &self.context))
        } else {
            query.fetch_one(self.pool)
                .await
                .map_err(|e| db_error(e, &self.context))
        }
    }

    /// Execute and return multiple rows
    pub async fn fetch_all<T>(self) -> DbResult<Vec<T>>
    where
        T: for<'r> FromRow<'r, PgRow> + Send + Unpin,
    {
        let query = sqlx::query_as::<_, T>(&self.query);
        
        if let Some(timeout) = self.timeout {
            tokio::time::timeout(timeout, query.fetch_all(self.pool))
                .await
                .map_err(|_| DbError::Timeout { context: self.context.clone() })?
                .map_err(|e| db_error(e, &self.context))
        } else {
            query.fetch_all(self.pool)
                .await
                .map_err(|e| db_error(e, &self.context))
        }
    }

    /// Execute and return optional row
    pub async fn fetch_optional<T>(self) -> DbResult<Option<T>>
    where
        T: for<'r> FromRow<'r, PgRow> + Send + Unpin,
    {
        let query = sqlx::query_as::<_, T>(&self.query);
        
        if let Some(timeout) = self.timeout {
            tokio::time::timeout(timeout, query.fetch_optional(self.pool))
                .await
                .map_err(|_| DbError::Timeout { context: self.context.clone() })?
                .map_err(|e| db_error(e, &self.context))
        } else {
            query.fetch_optional(self.pool)
                .await
                .map_err(|e| db_error(e, &self.context))
        }
    }

    /// Execute without returning results
    pub async fn execute(self) -> DbResult<u64> {
        let query = sqlx::query(&self.query);
        
        if let Some(timeout) = self.timeout {
            tokio::time::timeout(timeout, query.execute(self.pool))
                .await
                .map_err(|_| DbError::Timeout { context: self.context.clone() })?
                .map(|r| r.rows_affected())
                .map_err(|e| db_error(e, &self.context))
        } else {
            query.execute(self.pool)
                .await
                .map(|r| r.rows_affected())
                .map_err(|e| db_error(e, &self.context))
        }
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
fn is_retryable_db_error(err: &DbError) -> bool {
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

// ===== Common Query Patterns =====

/// Insert a record and return it with ULID support
pub async fn insert_and_return<T, R>(
    pool: DbPoolRef<'_>,
    table: &str,
    columns: &[&str],
    values: &[&str],
    context: &str,
) -> DbResult<R>
where
    R: for<'r> FromRow<'r, PgRow> + Send + Unpin,
{
    let columns_str = columns.join(", ");
    let placeholders: Vec<String> = (1..=values.len())
        .map(|i| format!("${}", i))
        .collect();
    let placeholders_str = placeholders.join(", ");
    
    let query = format!(
        "INSERT INTO {} ({}) VALUES ({}) RETURNING *",
        table, columns_str, placeholders_str
    );
    
    let mut q = sqlx::query_as::<_, R>(&query);
    for value in values {
        q = q.bind(value);
    }
    
    q.fetch_one(pool)
        .await
        .map_err(|e| db_error(e, context))
}

/// Update records with a WHERE clause
pub async fn update_where(
    pool: DbPoolRef<'_>,
    table: &str,
    set_clause: &str,
    where_clause: &str,
    context: &str,
) -> DbResult<u64> {
    let query = format!(
        "UPDATE {} SET {} WHERE {}",
        table, set_clause, where_clause
    );
    
    execute(pool, &query, context).await
}

/// Delete records with a WHERE clause
pub async fn delete_where(
    pool: DbPoolRef<'_>,
    table: &str,
    where_clause: &str,
    context: &str,
) -> DbResult<u64> {
    let query = format!(
        "DELETE FROM {} WHERE {}",
        table, where_clause
    );
    
    execute(pool, &query, context).await
}

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

// ===== Macro Helpers =====

/// Helper macro for creating parameterized queries with ULID support
#[macro_export]
macro_rules! bind_ulid {
    ($query:expr, $ulid:expr) => {
        $query.bind($crate::query_helpers::ulid_to_uuid($ulid))
    };
}

/// Helper macro for binding optional ULIDs
#[macro_export]
macro_rules! bind_optional_ulid {
    ($query:expr, $ulid:expr) => {
        $query.bind($ulid.map($crate::query_helpers::ulid_to_uuid))
    };
}

// ===== Query Execution Macros =====

/// Execute a query returning one row with automatic error context
#[macro_export]
macro_rules! query_one {
    ($pool:expr, $query:expr) => {
        $crate::query_helpers::QueryBuilder::new($pool)
            .sql($query)
            .context(concat!("query_one! at ", file!(), ":", line!()))
            .fetch_one()
            .await
    };
    ($pool:expr, $query:expr, $context:expr) => {
        $crate::query_helpers::QueryBuilder::new($pool)
            .sql($query)
            .context($context)
            .fetch_one()
            .await
    };
}

/// Execute a query returning multiple rows with automatic error context
#[macro_export]
macro_rules! query_many {
    ($pool:expr, $query:expr) => {
        $crate::query_helpers::QueryBuilder::new($pool)
            .sql($query)
            .context(concat!("query_many! at ", file!(), ":", line!()))
            .fetch_all()
            .await
    };
    ($pool:expr, $query:expr, $context:expr) => {
        $crate::query_helpers::QueryBuilder::new($pool)
            .sql($query)
            .context($context)
            .fetch_all()
            .await
    };
}

/// Execute a query returning an optional row with automatic error context
#[macro_export]
macro_rules! query_optional {
    ($pool:expr, $query:expr) => {
        $crate::query_helpers::QueryBuilder::new($pool)
            .sql($query)
            .context(concat!("query_optional! at ", file!(), ":", line!()))
            .fetch_optional()
            .await
    };
    ($pool:expr, $query:expr, $context:expr) => {
        $crate::query_helpers::QueryBuilder::new($pool)
            .sql($query)
            .context($context)
            .fetch_optional()
            .await
    };
}

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
#![doc = include_str!("../../docs/query_helpers.md")]

use crate::types::error::{Result as SinexResult, SinexError};
use crate::types::{retry, timeouts};
use crate::{DbPool, DbPoolRef, DbTransaction};
use futures::future::BoxFuture;
use sqlx::{Encode, Error as SqlxError, Postgres, QueryBuilder, Type};
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

/// Set transaction isolation level to REPEATABLE READ
pub async fn set_repeatable_read(tx: &mut DbTransaction<'_>) -> SinexResult<()> {
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .execute(&mut **tx)
        .await
        .map_err(|e| db_error(e, "set repeatable read isolation"))?;
    Ok(())
}

/// Configuration for transaction retry behavior
pub struct RetryConfig {
    pub max_attempts: u32,
    pub initial_delay: Duration,
    pub max_delay: Duration,
    pub exponential_base: f64,
}

/// Marker indicating a retryable transaction is idempotent.
#[derive(Debug, Clone, Copy)]
pub struct IdempotentTransaction;

impl IdempotentTransaction {
    pub fn new() -> Self {
        Self
    }
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

/// Execute a retryable transaction explicitly marked as idempotent.
pub async fn with_retry_transaction_idempotent<F, T>(
    pool: &DbPool,
    config: RetryConfig,
    _idempotent: IdempotentTransaction,
    mut f: F,
) -> SinexResult<T>
where
    F: for<'borrow> FnMut(&'borrow mut DbTransaction<'_>) -> BoxFuture<'borrow, SinexResult<T>>,
{
    with_retry_transaction_inner(pool, config, &mut f).await
}

async fn with_retry_transaction_inner<F, T>(
    pool: &DbPool,
    config: RetryConfig,
    f: &mut F,
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
                // Explicitly rollback to ensure the connection is clean before returning to pool
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
                continue;
            }
            Err(e) => {
                // Explicitly rollback to ensure the connection is clean before returning to pool
                let _ = tx.rollback().await;
                return Err(e);
            }
        }
    }
}

/// Check if a database error is retryable (deadlock, serialization failure, aborted transaction)
pub fn is_retryable_db_error(err: &SinexError) -> bool {
    let msg = err.to_string();
    msg.contains("deadlock detected")
        || msg.contains("could not serialize access")
        || msg.contains("transaction rollback")
        || msg.contains("current transaction is aborted")
}

/// Check if a record exists
pub async fn exists<F>(
    pool: DbPoolRef<'_>,
    table: &str,
    context: &str,
    build_where: F,
) -> SinexResult<bool>
where
    F: FnOnce(&mut WhereBuilder<'_>),
{
    let mut builder: QueryBuilder<'static, Postgres> =
        QueryBuilder::new("SELECT EXISTS(SELECT 1 FROM ");
    push_identifier(&mut builder, table);
    {
        let mut where_builder = WhereBuilder::new(&mut builder);
        build_where(&mut where_builder);
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
pub async fn count<F>(
    pool: DbPoolRef<'_>,
    table: &str,
    context: &str,
    build_where: F,
) -> SinexResult<i64>
where
    F: FnOnce(&mut WhereBuilder<'_>),
{
    let mut builder: QueryBuilder<'static, Postgres> = QueryBuilder::new("SELECT COUNT(*) FROM ");
    push_identifier(&mut builder, table);
    {
        let mut where_builder = WhereBuilder::new(&mut builder);
        build_where(&mut where_builder);
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

#[derive(Debug, Clone, Copy)]
pub enum Comparison {
    Eq,
    NotEq,
    Lt,
    Lte,
    Gt,
    Gte,
    Like,
    ILike,
}

impl Comparison {
    fn as_sql(self) -> &'static str {
        match self {
            Comparison::Eq => "=",
            Comparison::NotEq => "!=",
            Comparison::Lt => "<",
            Comparison::Lte => "<=",
            Comparison::Gt => ">",
            Comparison::Gte => ">=",
            Comparison::Like => "LIKE",
            Comparison::ILike => "ILIKE",
        }
    }
}

pub struct WhereBuilder<'a> {
    builder: &'a mut QueryBuilder<'static, Postgres>,
    has_conditions: bool,
}

impl<'a> WhereBuilder<'a> {
    fn new(builder: &'a mut QueryBuilder<'static, Postgres>) -> Self {
        Self {
            builder,
            has_conditions: false,
        }
    }

    pub fn and<T>(&mut self, column: &str, op: Comparison, value: T)
    where
        T: Encode<'static, Postgres> + Type<Postgres> + 'static,
    {
        self.push_joiner("AND");
        push_identifier(self.builder, column);
        self.builder.push(" ");
        self.builder.push(op.as_sql());
        self.builder.push(" ");
        self.builder.push_bind(value);
    }

    pub fn or<T>(&mut self, column: &str, op: Comparison, value: T)
    where
        T: Encode<'static, Postgres> + Type<Postgres> + 'static,
    {
        self.push_joiner("OR");
        push_identifier(self.builder, column);
        self.builder.push(" ");
        self.builder.push(op.as_sql());
        self.builder.push(" ");
        self.builder.push_bind(value);
    }

    pub fn and_is_null(&mut self, column: &str) {
        self.push_joiner("AND");
        push_identifier(self.builder, column);
        self.builder.push(" IS NULL");
    }

    pub fn and_is_not_null(&mut self, column: &str) {
        self.push_joiner("AND");
        push_identifier(self.builder, column);
        self.builder.push(" IS NOT NULL");
    }

    fn push_joiner(&mut self, joiner: &str) {
        if self.has_conditions {
            self.builder.push(" ");
            self.builder.push(joiner);
            self.builder.push(" ");
        } else {
            self.builder.push(" WHERE ");
            self.has_conditions = true;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::Execute;

    #[test]
    fn where_builder_escapes_identifiers_and_binds() {
        let mut builder: QueryBuilder<'static, Postgres> = QueryBuilder::new("SELECT 1");
        let mut where_builder = WhereBuilder::new(&mut builder);
        where_builder.and(
            "name\"; DROP TABLE users; --",
            Comparison::Eq,
            "value".to_string(),
        );

        let query = builder.build();
        let sql = query.sql();
        assert!(
            sql.contains("\"name\"\"; DROP TABLE users; --\""),
            "identifier should be escaped in SQL"
        );
        assert!(
            sql.contains("$1"),
            "value should be bound via placeholder instead of literal"
        );
        assert!(!sql.contains("value"), "value should not be in SQL text");
    }
}

// ULID/UUID conversion utilities are now provided by sinex-schema::ulid_conversions
// and re-exported above for convenience

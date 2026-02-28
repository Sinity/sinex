use crate::{DbTransaction, Ulid};
use sinex_primitives::error::{Result as SinexResult, SinexError};
use sinex_schema::primitives::conversions::{
    ulid_to_uuid as ulid_to_uuid_util, uuid_to_ulid as uuid_to_ulid_util,
};
use sqlx::PgPool;
use uuid::Uuid;

/// Convert ULID to UUID for database storage (adapter for reference-based usage)
pub fn ulid_to_uuid(ulid: &Ulid) -> Uuid {
    ulid_to_uuid_util(*ulid)
}

/// Convert UUID back to ULID (adapter for reference-based usage)
pub fn uuid_to_ulid(uuid: &Uuid) -> Ulid {
    uuid_to_ulid_util(*uuid)
}

/// Helper to convert database errors to SinexError
///
/// Converts sqlx errors to appropriate SinexError variants with context.
/// Preserves constraint violation details for debugging and analysis.
pub fn db_error(e: sqlx::Error, operation: &str) -> SinexError {
    match e {
        sqlx::Error::RowNotFound => {
            SinexError::not_found("Record not found").with_operation(operation)
        }
        sqlx::Error::Database(db_err) => {
            let mut error = if db_err.is_unique_violation() {
                SinexError::database("Unique constraint violation")
                    .with_context("constraint_type", "unique")
            } else if db_err.is_foreign_key_violation() {
                SinexError::database("Foreign key constraint violation")
                    .with_context("constraint_type", "foreign_key")
            } else {
                SinexError::database("Database error")
                    .with_context(
                        "error_code",
                        db_err
                            .code()
                            .map_or_else(|| "unknown".to_string(), |c| c.to_string()),
                    )
                    .with_source(db_err.to_string())
            };
            error = error.with_operation(operation);
            error
        }
        sqlx::Error::PoolTimedOut => SinexError::timeout("Database connection pool timeout")
            .with_operation(operation)
            .with_context("timeout_reason", "pool_exhausted"),
        _ => SinexError::database("Database error")
            .with_source(e.to_string())
            .with_operation(operation),
    }
}

/// Set statement timeout for long-running queries
///
/// # Query Timeout Protection
/// Long-running queries can block connection pool resources and cause cascading failures.
/// To prevent this:
///
/// 1. **Connection-level timeout**: Set at pool configuration (recommended for all connections)
///    ```rust
///    // In pool setup:
///    PgPoolOptions::new()
///        .after_connect(|conn, _meta| Box::pin(async move {
///            conn.execute("SET statement_timeout = '30s'").await?;
///            Ok(())
///        }))
///    ```
///
/// 2. **Per-query timeout**: Use this function for specific slow queries
///    ```rust
///    set_statement_timeout(executor, 60_000).await?; // 60 seconds
///    ```
///
/// 3. **Reset timeout**: Always reset after slow query completes
///    ```rust
///    set_statement_timeout(executor, 0).await?; // 0 = no timeout
///    ```
///
/// Without timeouts, slow queries (full table scans, complex joins) can hold connections
/// indefinitely and exhaust the pool.
pub async fn set_statement_timeout<'e, E>(executor: E, timeout_ms: i32) -> DbResult<()>
where
    E: sqlx::Executor<'e, Database = sqlx::Postgres>,
{
    sqlx::query(&format!("SET LOCAL statement_timeout = {timeout_ms}"))
        .execute(executor)
        .await
        .map_err(|e| db_error(e, "set statement timeout"))?;
    Ok(())
}

/// Common result type for database operations
pub type DbResult<T> = SinexResult<T>;

/// Base repository trait that all repositories should implement
pub trait Repository<'a> {
    /// Get a reference to the database pool
    fn pool(&self) -> &'a PgPool;

    /// Create a new instance with the given pool
    fn new(pool: &'a PgPool) -> Self;
}

/// Extension trait for transaction support
pub trait TransactionSupport {
    type Item;

    /// Execute the operation within a transaction
    fn with_tx(self, tx: &mut DbTransaction<'_>) -> Self::Item;
}

// Re-export TableDef from schema crate
pub use sinex_schema::schema::TableDef;

/// Enhanced repository trait with generic operations
pub trait EnhancedRepository<'a>: Repository<'a> {
    /// Associated table definition
    type Table: TableDef;

    /// Count all records in the table
    async fn count_all(&self) -> DbResult<i64> {
        // SAFETY: format! usage for query building
        //
        // This use of format! is safe because:
        // 1. schema_name() and table_name() return &'static str constants from trait implementations
        // 2. These are compile-time constants determined by the trait implementation, never user input
        // 3. The TableDef trait contract guarantees these return valid SQL identifiers
        //
        // However, this pattern should NOT be used with runtime values or user input.
        // For dynamic queries, always use QueryBuilder or properly parameterized queries.
        //
        // This is an intentional use of format! with compile-time constants. While format! with
        // user input would be a SQL injection risk, this specific usage is safe because all values
        // are &'static str from trait bounds. DO NOT copy this pattern for runtime string building.
        let query = format!(
            "SELECT COUNT(*) FROM {}.{}",
            Self::Table::schema_name(),
            Self::Table::table_name()
        );

        let result: (i64,) = sqlx::query_as(&query)
            .fetch_one(self.pool())
            .await
            .map_err(|e| db_error(e, "Failed to count records"))?;

        Ok(result.0)
    }

    /// Check if a record exists by primary key
    async fn exists_by_id(&self, id: &Ulid) -> DbResult<bool> {
        // SAFETY: format! usage for query building
        //
        // schema_name(), table_name(), and primary_key() return &'static str constants
        // from trait implementations. User input is properly parameterized via $1::uuid.
        // This is safe for the same reasons as count_all above - all format arguments are
        // compile-time constants, never user input.
        let sql = format!(
            "SELECT 1 FROM {}.{} WHERE {}::uuid = $1::uuid LIMIT 1",
            Self::Table::schema_name(),
            Self::Table::table_name(),
            Self::Table::primary_key()
        );

        let uuid = ulid_to_uuid(id);
        let result: Option<(i32,)> = sqlx::query_as(&sql)
            .bind(uuid)
            .fetch_optional(self.pool())
            .await
            .map_err(|e| db_error(e, "Failed to check existence"))?;

        Ok(result.is_some())
    }
}

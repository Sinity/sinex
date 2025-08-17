use crate::types::domain::{EventSource, EventType, HostName};
use crate::types::error::{Result as SinexResult, SinexError};
use crate::Ulid;
use chrono::{DateTime, Utc};
use sea_query::{Expr, PostgresQueryBuilder, Query};
use serde_json::Value as JsonValue;
use sqlx::{FromRow, PgPool, Postgres, Transaction};
use uuid::Uuid;

/// Convert ULID to UUID for database storage
pub fn ulid_to_uuid(ulid: &Ulid) -> Uuid {
    let bytes = ulid.to_bytes();
    Uuid::from_bytes(bytes)
}

/// Convert UUID back to ULID
pub fn uuid_to_ulid(uuid: &Uuid) -> Ulid {
    Ulid::from_bytes(*uuid.as_bytes()).expect("Valid ULID bytes from UUID")
}

/// Helper to convert database errors to SinexError
pub fn db_error(e: sqlx::Error, context: &str) -> SinexError {
    match e {
        sqlx::Error::RowNotFound => SinexError::not_found(context.to_string()),
        sqlx::Error::Database(db_err) => {
            if db_err.is_unique_violation() {
                SinexError::database(format!("{}: unique constraint violation", context))
            } else if db_err.is_foreign_key_violation() {
                SinexError::database(format!("{}: foreign key violation", context))
            } else {
                SinexError::database(format!("{}: {}", context, db_err))
            }
        }
        _ => SinexError::database(format!("{}: {}", context, e)),
    }
}

/// Common result type for database operations
pub type DbResult<T> = SinexResult<T>;

/// Time bucket result for aggregations
#[derive(Debug, FromRow)]
pub struct TimeBucketResult {
    pub bucket: DateTime<Utc>,
    pub count: i64,
}

/// Event search filters
#[derive(Debug, Default)]
pub struct EventSearchFilters {
    pub source: Option<EventSource>,
    pub event_type: Option<EventType>,
    pub after: Option<DateTime<Utc>>,
    pub before: Option<DateTime<Utc>>,
    pub host: Option<HostName>,
    pub payload_contains: Option<JsonValue>,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
}

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
    fn with_tx<'a>(self, tx: &'a mut Transaction<'_, Postgres>) -> Self::Item;
}

// Re-export TableDef from schema crate
pub use sinex_schema::schema::TableDef;

/// Enhanced repository trait with generic operations
pub trait EnhancedRepository<'a>: Repository<'a> {
    /// Associated table definition
    type Table: TableDef;

    /// Count all records in the table
    async fn count_all(&self) -> DbResult<i64> {
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
        // Use a parameterized query with explicit ULID cast
        let sql = format!(
            "SELECT 1 FROM {}.{} WHERE {} = $1::ulid LIMIT 1",
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

/// Batch operations for repositories
#[async_trait::async_trait]
pub trait BatchRepository<'a, T>: Repository<'a>
where
    T: FromRow<'a, sqlx::postgres::PgRow> + Send + Unpin,
{
    /// Insert multiple records in a single transaction
    async fn insert_batch(&self, records: Vec<T>) -> DbResult<Vec<Ulid>>;

    /// Update multiple records in a single transaction
    async fn update_batch(&self, records: Vec<(Ulid, T)>) -> DbResult<u64>;

    /// Delete multiple records by IDs
    async fn delete_batch(&self, ids: Vec<Ulid>) -> DbResult<u64>;
}

/// Transactional operations for repositories
#[async_trait::async_trait]
pub trait TransactionalRepository<'a>: Repository<'a> {
    /// Execute a closure within a transaction
    async fn with_transaction<F, R>(&self, f: F) -> DbResult<R>
    where
        F: for<'t> FnOnce(
                &'t mut Transaction<'_, Postgres>,
            ) -> futures::future::BoxFuture<'t, DbResult<R>>
            + Send,
        R: Send,
    {
        let mut tx = self
            .pool()
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
                let _ = tx.rollback().await;
                Err(e)
            }
        }
    }
}

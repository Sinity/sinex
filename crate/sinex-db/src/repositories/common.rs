use chrono::{DateTime, Utc};
use serde_json::Value as JsonValue;
use sinex_core_types::domain::{EventSource, EventType, HostName};
use sinex_error::{Result as SinexResult, SinexError};
use sinex_ulid::Ulid;
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

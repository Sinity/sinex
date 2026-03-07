//! Database persistence layer for Sinex
//!
//! This crate handles all database interactions, including:
//! - `EventRecord` (DTO) <-> Event (Domain) conversions
//! - Repositories for data access
//! - Connection pool management

pub mod advisory_lock;
pub mod error;
pub mod integrity;
pub mod models;
pub mod pool;
pub mod query_helpers;
pub mod replay;
pub mod repositories;
pub mod schema_apply;
pub mod security;
pub mod validation;

pub use error::{DbResult, db_error};
pub use models::*;
pub use pool::{
    DbPool, PoolConfig, acquire_with_timeout, create_database_if_not_exists, create_pool,
    create_pool_strict, create_pool_with_config, create_pool_with_config_strict, create_test_pool,
    get_database_url,
};
pub use query_helpers::{IdempotentTransaction, RetryConfig, with_retry_transaction_idempotent};
pub use repositories::DbPoolExt;
pub use repositories::events::{CascadeSource, EventRepository};
pub use repositories::events::{EventRecordExt, records_to_events};
pub use schema_apply::{apply_schema, apply_schema_for_url};
pub use sinex_primitives::SinexError;
pub use sinex_primitives::ids::Id;
pub use sinex_primitives::primitives::Timestamp;
pub use sinex_schema::schema;
pub use sinex_schema::schema::records::{BlobRecord, EventRecord, SourceMaterialRecord};
pub type JsonValue = serde_json::Value;

/// Database transaction type alias
pub type DbTransaction<'a> = sqlx::Transaction<'a, sqlx::Postgres>;

pub mod postgres_copy;

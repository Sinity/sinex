//! Database persistence layer for Sinex
//!
//! This crate handles all database interactions, including:
//! - EventRecord (DTO) <-> Event (Domain) conversions
//! - Repositories for data access
//! - Connection pool management

// Allow async fn in traits - we use trait_variant for Send bounds where needed
#![allow(async_fn_in_trait)]
// TODO: Enable strict clippy after cleanup
// sinex-db has accumulated lint issues that need dedicated cleanup
#![allow(clippy::all)]
#![allow(clippy::pedantic)]
#![allow(clippy::nursery)]
#![allow(unused_imports)]

pub mod advisory_lock;
pub mod error;
pub mod events;
pub mod integrity;
pub mod migration;
pub mod models;
pub mod pool;
pub mod query_helpers;
pub mod replay;
pub mod repositories;
pub mod sanitization;
pub mod security;
pub mod validation;

pub use error::{db_error, DbResult};
pub use events::conversions::{records_to_events, EventRecordExt};
pub use migration::{run_migrations, run_migrations_for_url};
pub use models::*;
pub use pool::{
    acquire_with_timeout, create_database_if_not_exists, create_pool, create_pool_strict,
    create_pool_with_config, create_pool_with_config_strict, create_test_pool, get_database_url,
    DbPool, DbPoolRef, PoolConfig,
};
pub use query_helpers::{with_retry_transaction_idempotent, IdempotentTransaction, RetryConfig};
pub use repositories::events::EventRepository;
pub use repositories::DbPoolExt;
pub use sinex_primitives::ids::Id;
pub use sinex_primitives::SinexError;
pub use sinex_schema::schema;
pub use sinex_schema::schema::records::{BlobRecord, EventRecord, SourceMaterialRecord};
pub use sinex_schema::ulid::{Timestamp, Ulid};
pub type JsonValue = serde_json::Value;
pub type OptionalTimestamp = Option<Timestamp>;
pub type SqlxPgPool = sqlx::PgPool;

/// Database transaction type alias
pub type DbTransaction<'a> = sqlx::Transaction<'a, sqlx::Postgres>;

pub mod postgres_copy;

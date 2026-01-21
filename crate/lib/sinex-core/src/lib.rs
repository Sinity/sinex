#![doc = include_str!("../docs/overview.md")]
#![doc = include_str!("../../../../docs/current/architecture/Core_Architecture.md")]
#![doc = include_str!("../../sinex-schema/docs/ulid.md")]

//! Core Sinex abstractions for types, persistence, and environment wiring.
#![allow(async_fn_in_trait)]
// Types module - unified types system
pub mod types {
    // Re-export the entire types module structure
    pub use crate::types_impl::*;
    // Re-export ulid from sinex_schema
    pub use sinex_schema::ulid;
}

// Database module - database access and models
pub mod db {
    // Re-export the entire db module structure
    pub use crate::db_impl::*;
}

// Environment namespacing module
pub mod environment;

// Filesystem helpers
pub mod fs;

// NATS configuration module (only with nats feature)
#[cfg(feature = "nats")]
pub mod nats;

// Coordination module (only with nats feature - uses NATS KV)
#[cfg(feature = "nats")]
pub mod coordination;

// Internal implementation modules (not directly exposed)
#[path = "types/mod.rs"]
mod types_impl;

#[path = "db/mod.rs"]
mod db_impl;

// Re-export database macros at crate level
#[cfg(feature = "macros")]
pub use sinex_macros::{db_query, db_transaction};

// Re-export commonly used types and functions at crate level for convenience
pub use types::{
    domain, error, events, utils, validation, HealthCheck, HealthStatus, OptionalTimestamp,
    Result as SinexResult, ServiceInfo, ServiceKind, SinexError,
};

// Re-export ID types - Ulid from sinex-schema, Id from local types
pub use sinex_schema::ulid::{Timestamp, Ulid};
pub use types::ids::Id;

// Re-export environment functionality at crate level
pub use environment::{environment, SinexEnvironment};

// Re-export event system at crate root for short imports
pub use types::events::EventPayload;

// Create facade for event payloads to flatten hierarchy
pub mod payloads {
    //! Flattened event payloads for easier imports
    //! Instead of `sinex_core::types::events::payloads::filesystem::FileCreatedPayload`
    //! use `sinex_core::payloads::FileCreatedPayload`
    pub use crate::types::events::payloads::*;
}

// Result type aliases for common operations
pub type EventResult<T = ()> = std::result::Result<T, SinexError>;

// Database transaction type alias (only available with sqlx feature)
#[cfg(feature = "sqlx")]
pub type DbTransaction<'a> = sqlx::Transaction<'a, sqlx::Postgres>;

// Note: DbResult is already re-exported from repositories

#[cfg(feature = "sqlx")]
pub use db::{
    acquire_with_timeout, create_database_if_not_exists, create_pool, create_pool_strict,
    create_pool_with_config, create_pool_with_config_strict, create_test_pool, get_database_url,
    models, pool, query_helpers, repositories, sanitization, security, DbPool, DbPoolRef,
    PoolConfig,
};

#[cfg(feature = "migrations")]
pub use db::run_migrations;

// Re-export the most commonly used database models at crate root
pub use db::models::{
    Blob, Entity, EntityRelation, Event, EventBuilder, EventId, HasProvenance, JsonValue,
    NoProvenance, Provenance, SourceMaterial,
};

// Re-export the unified Event type helpers
pub use db::models::event::OffsetKind;

// Re-export records from sinex-schema
pub use sinex_schema::schema::records::{BlobRecord, EventRecord, SourceMaterialRecord};

// Re-export all repository traits and types at crate root for short imports
#[cfg(feature = "sqlx")]
pub use db::repositories::{
    BlobRepository, CommandCount, CreateEntity, CreateEntityRelation, DbPoolExt, DbResult,
    EnhancedRepository, EntityRecord, EntityRelationRecord, EntityType, EventAnnotation,
    EventPayloadSchema, EventRepository, EventRepositoryTx, EventSearchFilters, EventTypeCount,
    KnowledgeGraphRepository, NewSchema, Operation, OperationRecord, OperationStatistics,
    Repository, SourceActivity, SourceMaterialExt, SourceMaterialRepository, StateRepository,
    StorageStats, SystemHealthReport, TableDef, TransactionSupport,
};

// Re-export all domain types at crate root for short imports
pub use types::domain::{
    ConsumerGroup, ConsumerName, EventSource, EventType, HostName, ProcessorName, SanitizedPath,
    SchemaName, SchemaVersion,
};

// Re-export migration functionality
#[cfg(feature = "migrations")]
pub use db::migration;

// Telemetry system has been removed - keeping this comment for historical context

// Re-export query helpers for easier access
#[cfg(feature = "sqlx")]
pub use query_helpers::{
    count, db_error, exists, from_db, is_retryable_db_error, opt_from_db, opt_to_db,
    opt_vec_from_db, opt_vec_to_db, to_db, ulid_to_uuid, uuid_to_ulid,
    with_retry_transaction_idempotent, with_transaction, DbUuidCollectionExt, DbUuidExt,
    IdempotentTransaction, RetryConfig, UlidArrayExt, UlidExt,
};

// Re-export repository pattern (DbPoolExt already re-exported above)
#[cfg(feature = "sqlx")]
pub use repositories::DbResult as RepoResult;

/// Prelude module for commonly used types and functions
///
/// Import this module to get access to the most frequently used types and functions:
/// ```rust
/// use sinex_core::prelude::*;
/// ```
pub mod prelude {
    // Core types always available (no sqlx required)
    pub use crate::{
        BlobRecord, EventRecord, EventSource, EventType, HostName, Id, OptionalTimestamp,
        ProcessorName, SourceMaterialRecord, Timestamp, Ulid,
    };

    // All commonly used nested types flattened for convenience
    #[cfg(feature = "sqlx")]
    pub use crate::validation::{validate_json, validate_path};
    pub use crate::{
        // Domain types
        ConsumerGroup,
        ConsumerName,
        // Event types
        Event,
        EventId,
        EventPayload,
        HasProvenance,
        JsonValue,
        Provenance,
        SchemaName,
        SchemaVersion,
        // Error types
        SinexError,
        SourceMaterial,
    };

    // Common external crates that are used throughout the codebase
    pub use color_eyre::eyre::{eyre, Result};

    // Database types and functionality (only with sqlx feature)
    #[cfg(feature = "sqlx")]
    pub use crate::{
        create_pool, create_pool_strict, create_test_pool, db_error, from_db, opt_from_db,
        opt_to_db, opt_vec_from_db, opt_vec_to_db, to_db, ulid_to_uuid, uuid_to_ulid,
        with_transaction, BlobRepository, DbPool, DbPoolExt, DbPoolRef, DbUuidCollectionExt,
        DbUuidExt, EventRepository, EventSearchFilters, KnowledgeGraphRepository, NewSchema,
        PoolConfig, Repository, RetryConfig, SanitizedPath, SourceMaterialRepository,
        StateRepository, UlidArrayExt, UlidExt,
    };

    #[cfg(feature = "sqlx")]
    pub use sqlx::{FromRow, Postgres, Transaction};
}

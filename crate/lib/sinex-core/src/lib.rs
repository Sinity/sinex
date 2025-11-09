#![doc = include_str!("../doc/overview.md")]
#![doc = include_str!("../../../../docs/architecture/Core_Architecture.md")]
#![doc = include_str!("../../../../docs/architecture/event-taxonomy.md")]
#![doc = include_str!("../../sinex-schema/doc/ulid.md")]

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
    domain,
    error,
    events,
    utils,
    // Export validation functions for backward compatibility
    validate_json,
    validate_path,
    validation,
    HealthCheck,
    HealthStatus,
    MetricsEntry,
    OptionalTimestamp,
    Result as SinexResult,
    ServiceInfo,
    ServiceKind,
    SinexError,
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

// Database transaction type alias
pub type DbTransaction<'a> = sqlx::Transaction<'a, sqlx::Postgres>;

// Result type aliases for common operations
pub type EventResult<T = ()> = std::result::Result<T, SinexError>;
// Note: DbResult is already re-exported from repositories

pub use db::{
    create_database_if_not_exists, create_pool, create_pool_strict, create_pool_with_config,
    create_pool_with_config_strict, create_test_pool, distributed_locking, get_database_url,
    models, pool, query_helpers, repositories, run_migrations, sanitization, seaquery_helpers,
    security, DbPool, DbPoolRef, PoolConfig,
};

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
pub use db::repositories::{
    BatchRepository, BlobRepository, Checkpoint, CheckpointExt, CheckpointRecord,
    CheckpointRepository, CommandCount, CreateEntity, CreateEntityRelation, DbPoolExt, DbResult,
    EnhancedRepository, EntityRecord, EntityRelationRecord, EntityType, EventAnnotation,
    EventPayloadSchema, EventRepository, EventRepositoryTx, EventSearchFilters, EventTypeCount,
    KnowledgeGraphRepository, NewSchema, Operation, OperationRecord, OperationStatistics,
    Repository, SourceActivity, SourceMaterialExt, SourceMaterialRepository, StateRepository,
    StorageStats, SystemHealthReport, TableDef, TransactionSupport, TransactionalRepository,
};

// Re-export all domain types at crate root for short imports
pub use types::domain::{
    ConsumerGroup, ConsumerName, EventSource, EventType, HostName, ProcessorName, SanitizedPath,
    SchemaName, SchemaVersion,
};

// Re-export migration functionality
#[cfg(feature = "migration")]
pub use db::migration;

// Telemetry system has been removed - keeping this comment for historical context

// Re-export query helpers for easier access
pub use query_helpers::{
    count, db_error, exists, from_db, is_retryable_db_error, opt_from_db, opt_to_db,
    opt_vec_from_db, opt_vec_to_db, to_db, ulid_to_uuid, uuid_to_ulid, with_retry_transaction,
    with_transaction, DbUuidCollectionExt, DbUuidExt, RetryConfig, UlidArrayExt, UlidExt,
};

// Re-export SeaQuery ULID helpers
pub use seaquery_helpers::SeaQueryUlidExt;

// Re-export repository pattern (DbPoolExt already re-exported above)
pub use repositories::DbResult as RepoResult;

/// Prelude module for commonly used types and functions
///
/// Import this module to get access to the most frequently used types and functions:
/// ```rust
/// use sinex_core::prelude::*;
/// ```
pub mod prelude {
    // Core data types - all available at crate root for convenience
    pub use crate::{
        BlobRecord, CheckpointRepository, DbPoolExt, Entity, EntityRelation, Event, EventId,
        EventRecord, EventRepository, EventSource, EventType, HostName, Id, JsonValue,
        OptionalTimestamp, ProcessorName, Provenance, Repository, SourceMaterial,
        SourceMaterialRecord, Timestamp, Ulid,
    };

    // All commonly used nested types flattened for convenience
    pub use crate::{
        validate_json,
        validate_path,
        // Domain types
        ConsumerGroup,
        ConsumerName,
        // Event types (Event already imported above, so just EventPayload)
        EventPayload,
        // Utils
        SanitizedPath,
        SchemaName,
        SchemaVersion,
        // Error types
        SinexError,
    };

    // Database functionality - all commonly used functions and types
    pub use crate::{
        create_pool,
        create_pool_strict,
        create_test_pool,
        // Query helpers
        db_error,
        from_db,
        opt_from_db,
        opt_to_db,
        opt_vec_from_db,
        opt_vec_to_db,
        to_db,
        ulid_to_uuid,
        uuid_to_ulid,
        with_retry_transaction,
        with_transaction,
        // All repository types for convenience
        BlobRepository,
        // Repository types
        Checkpoint,
        // Connection management
        DbPool,
        DbPoolRef,
        DbUuidCollectionExt,
        DbUuidExt,
        EventSearchFilters,
        KnowledgeGraphRepository,
        NewSchema,
        PoolConfig,
        RetryConfig,
        // SeaQuery helpers
        SeaQueryUlidExt,
        SourceMaterialRepository,
        StateRepository,
        UlidArrayExt,
        UlidExt,
    };

    // Common external crates that are used throughout the codebase
    pub use color_eyre::eyre::{eyre, Result};
    pub use sqlx::{FromRow, Postgres, Transaction};
}

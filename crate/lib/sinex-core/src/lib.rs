//! # Sinex Core
//!
//! Unified core types and database layer for the Sinex event-driven data capture system.
//! This crate combines the functionality that was previously split across sinex-types and sinex-db.
//!
//! ## Architecture Overview
//!
//! The Sinex data model implements key architectural principles through its table design:
//!
//! ### Core Tables
//!
//! #### `raw.source_material_registry`
//! The manifest of all external data - the "birth certificates" for all data entering the system:
//! - Immutable storage via git-annex integration
//! - Rich metadata including timing, source, and user context
//! - Supports the Stage-as-You-Go pattern for real-time provenance
//!
//! #### `core.events`
//! The unified interpretation log implementing Deep Oneness:
//! ```sql
//! CREATE TABLE core.events (
//!     event_id ULID PRIMARY KEY,              -- Time-ordered, globally unique
//!     ts_ingest TIMESTAMPTZ,                  -- System time (from ULID)
//!     ts_orig TIMESTAMPTZ,                    -- Semantic time
//!     source TEXT NOT NULL,                   -- Who created this
//!     event_type TEXT NOT NULL,               -- What happened
//!     payload JSONB NOT NULL,                 -- The details
//!     
//!     -- Provenance tracking
//!     source_event_ids ULID[],                -- NULL=raw, populated=synthesis
//!     source_material_id ULID,                -- External data reference
//!     anchor_byte BIGINT,                     -- Immutable location
//!     
//!     -- Schema evolution support
//!     payload_schema_id ULID,
//!     payload_schema_name TEXT,
//!     payload_schema_version TEXT
//! );
//! ```

// Types module - unified types system
pub mod types {
    // Re-export the entire types module structure
    pub use crate::types_impl::*;
    // Re-export ids and ulid from sinex_schema
    pub use sinex_schema::{ids, ulid};
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
    JsonValue,
    MetricsEntry,
    OptionalTimestamp,
    Result as SinexResult,
    ServiceInfo,
    ServiceKind,
    SinexError,
};

// Re-export ID types from sinex-schema
pub use sinex_schema::{
    ids::Id,
    ulid::{Timestamp, Ulid},
};

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

// Type aliases for complex generic types to reduce verbosity
pub type EventId = Id<RawEvent>;
// Create a placeholder Blob type for now
pub struct Blob;
pub type BlobId = Id<Blob>;
pub type EntityId = Id<Entity>;
pub type SourceMaterialId = Id<SourceMaterial>;
pub type CheckpointId = Id<CheckpointRecord>;
pub type OperationId = Id<Operation>;

// Database transaction type alias
pub type DbTransaction<'a> = sqlx::Transaction<'a, sqlx::Postgres>;

// Result type aliases for common operations
pub type EventResult<T = ()> = std::result::Result<T, SinexError>;
// Note: DbResult is already re-exported from repositories

pub use db::{
    constants, create_database_if_not_exists, create_pool, create_pool_strict,
    create_pool_with_config, create_pool_with_config_strict, create_test_pool, distributed_locking,
    get_database_url, models, pool, query_helpers, repositories, run_migrations, sanitization,
    schema_migrations, seaquery_helpers, security, DbPool, DbPoolRef, PoolConfig,
};

// Re-export the most commonly used database models at crate root
pub use db::models::{Entity, EntityRelation, Provenance, RawEvent, SourceMaterial};

// Re-export the unified Event type (EventId is already defined above as type alias)
pub use db::models::event::Event;

// Re-export records from sinex-schema
pub use sinex_schema::schema::records::{BlobRecord, EventRecord, SourceMaterialRecord};

// Re-export all repository traits and types at crate root for short imports
pub use db::repositories::{
    BatchRepository, BlobRepository, Checkpoint, CheckpointExt, CheckpointRecord,
    CheckpointRepository, CommandCount, CreateEntity, CreateEntityRelation, DbPoolExt, DbResult,
    EnhancedRepository, EntityRecord, EntityRelationRecord, EntityType, EventAnnotation,
    EventPayloadSchema, EventRepository, EventSearchFilters, EventTypeCount,
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

// Re-export telemetry if enabled
#[cfg(feature = "telemetry")]
pub use db::telemetry;

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
        OptionalTimestamp, ProcessorName, Provenance, RawEvent, Repository, SourceMaterial,
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

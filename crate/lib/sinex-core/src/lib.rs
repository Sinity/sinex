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
}

// Database module - database access and models
pub mod db {
    // Re-export the entire db module structure
    pub use crate::db_impl::*;
}

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
    ids,
    ulid,
    utils,
    // Export validation functions for backward compatibility
    validate_json,
    validate_path,
    validation,
    HealthCheck,
    HealthStatus,
    Id,
    JsonValue,
    MetricsEntry,
    OptionalTimestamp,
    Result as SinexResult,
    ServiceInfo,
    ServiceKind,
    SinexError,
    Timestamp,
    Ulid,
};

// Re-export commonly used event payloads at crate root
pub use types::events::payloads;

pub use db::{
    constants, create_database_if_not_exists, create_pool, create_pool_strict,
    create_pool_with_config, create_pool_with_config_strict, create_test_pool, distributed_locking,
    get_database_url, models, pool, query_helpers, repositories, run_migrations, sanitization,
    schema_migrations, seaquery_helpers, security, DbPool, DbPoolRef, PoolConfig,
};

// Re-export the most commonly used database models at crate root
pub use db::models::{
    Blob, BlobRecord, Entity, EntityRelation, Provenance, RawEvent, SourceMaterial,
};

// Re-export the most commonly used repository traits at crate root
pub use db::repositories::{CheckpointRepository, DbPoolExt, EventRepository, Repository};

// Re-export the most commonly used domain types at crate root
pub use types::domain::{
    EventSource, EventType, HostName, ProcessorName, SchemaName, SchemaVersion,
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
pub use repositories::{
    Checkpoint, DbResult as RepoResult, EventPayloadSchema, EventSearchFilters, NewSchema,
};

/// Prelude module for commonly used types and functions
///
/// Import this module to get access to the most frequently used types and functions:
/// ```rust
/// use sinex_core::prelude::*;
/// ```
pub mod prelude {
    // Core data types - now available at crate root for convenience
    pub use crate::{
        Blob, CheckpointRepository, DbPoolExt, EventRepository, EventSource, EventType, HostName,
        Id, JsonValue, OptionalTimestamp, ProcessorName, RawEvent, Repository, Timestamp, Ulid,
    };

    // Types from nested modules that are commonly used together
    pub use crate::types::{
        error::{Result as SinexResult, SinexError},
        events::EventPayload,
        utils::ResourceGuard,
        validation::{validate_json, validate_path, ValidationError},
    };

    // Database functionality that's frequently used together
    pub use crate::db::{
        query_helpers::{
            db_error, from_db, opt_from_db, opt_to_db, opt_vec_from_db, opt_vec_to_db, to_db,
            ulid_to_uuid, uuid_to_ulid, with_retry_transaction, with_transaction,
            DbUuidCollectionExt, DbUuidExt, RetryConfig, UlidArrayExt, UlidExt,
        },
        repositories::{Checkpoint, EventSearchFilters, NewSchema},
        seaquery_helpers::SeaQueryUlidExt,
        DbPool, DbPoolRef, PoolConfig,
    };

    // Common external crates that are used throughout the codebase
    pub use color_eyre::eyre::{eyre, Result};
    pub use sqlx::{FromRow, Postgres, Transaction};
}

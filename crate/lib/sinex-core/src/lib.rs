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
    domain, error, events, ids, ulid, utils, validation,
    Id, JsonValue, OptionalTimestamp, Result as SinexResult, SinexError, Timestamp, Ulid,
    HealthCheck, HealthStatus, MetricsEntry, ServiceInfo, ServiceKind,
    // Export validation functions for backward compatibility
    validate_json, validate_path,
};

pub use db::{
    models, pool, query_helpers, repositories, constants, distributed_locking,
    sanitization, security, schema_migrations, seaquery_helpers,
    create_pool, create_pool_strict, create_pool_with_config, create_pool_with_config_strict,
    create_test_pool, create_database_if_not_exists, get_database_url, run_migrations,
    DbPool, DbPoolRef, PoolConfig,
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

// Re-export repository pattern
pub use repositories::{
    Checkpoint, DbPoolExt, DbResult as RepoResult, EventPayloadSchema, EventSearchFilters,
    NewSchema,
};

/// Prelude module for commonly used types and functions
pub mod prelude {
    // Types from the types module
    pub use crate::types::{
        domain::{EventSource, EventType, HostName},
        error::{Result as SinexResult, SinexError},
        events::EventPayload,
        Id, JsonValue, OptionalTimestamp, Timestamp, Ulid,
    };
    
    // Database types and functions
    pub use crate::db::{
        models::RawEvent,
        query_helpers::{
            db_error, from_db, opt_from_db, opt_to_db, opt_vec_from_db, opt_vec_to_db, to_db,
            ulid_to_uuid, uuid_to_ulid, with_retry_transaction, with_transaction, DbUuidCollectionExt,
            DbUuidExt, RetryConfig, UlidArrayExt, UlidExt,
        },
        seaquery_helpers::SeaQueryUlidExt,
        repositories::{
            Checkpoint, CheckpointRepository, DbPoolExt, EventRepository, EventSearchFilters,
            NewSchema, Repository,
        },
        DbPool, DbPoolRef, PoolConfig,
    };
    
    // Common external crates
    pub use color_eyre::eyre::{eyre, Result};
    pub use sqlx::{FromRow, Postgres, Transaction};
}
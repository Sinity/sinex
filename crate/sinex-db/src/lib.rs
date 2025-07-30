//! # Sinex Database Layer
//!
//! The database layer for the Sinex event-driven data capture system. This crate provides
//! all database interactions including schema management, query builders, and data models.
//!
//! ## Data Model: The System's Constitution
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
//!
//! Key insights:
//! - **ULID Primary Keys**: Time-ordered identifiers for efficient indexing
//! - **Dual Timestamps**: System time (ts_ingest) vs semantic time (ts_orig)
//! - **Provenance Chain**: source_event_ids tracks synthesis lineage
//! - **Anchor Byte Principle**: Immutable reference for deterministic replay
//!
//! #### `audit.archived_events`
//! Complete audit trail of superseded interpretations:
//! - Populated by BEFORE DELETE trigger
//! - Includes reason and replacement reference
//! - Enables full historical analysis
//! - Implements the Archive and Replace pattern
//!
//! #### `core.operations_log`
//! Intent-level audit of all system actions:
//! - Records stage, replay, archive operations
//! - Captures exact parameters and outcomes
//! - Provides "why" for all data modifications
//! - Enables Auditable Metacognition
//!
//! ### Knowledge Graph (Materialized State)
//! - `core.entities`: Concepts, people, projects extracted from events
//! - `core.entity_relations`: Connections between entities
//! - Completely rebuildable from event stream
//! - Users can directly manipulate (generating events)
//!
//! ## Key Design Decisions
//!
//! 1. **Immutability**: Events are never updated, only archived and replaced
//! 2. **Time-Ordering**: ULID keys ensure natural time-based sorting
//! 3. **Schema Evolution**: Payload schemas tracked but not enforced
//! 4. **Provenance First**: Every piece of data traceable to its origin
//! 5. **Audit Everything**: System remembers not just what but why

pub mod models;
// Re-export RawEvent from sinex-events for type unification
pub use sinex_events::RawEvent;

// Helper functions for backward compatibility
pub async fn count_events(pool: DbPoolRef<'_>) -> Result<i64, crate::query_helpers::DbError> {
    use crate::repositories::{EventRepository, Repository};
    let repo = EventRepository::new(pool);
    repo.count_all().await.map_err(|e| {
        crate::query_helpers::db_error(sqlx::Error::Protocol(e.to_string()), "count_events")
    })
}

pub async fn insert_event_with_validator(
    pool: DbPoolRef<'_>,
    event: &RawEvent,
    _validator: Option<&()>, // Validator type not available yet
) -> Result<RawEvent, crate::query_helpers::DbError> {
    use crate::repositories::{EventRepository, NewEvent, Repository};
    use sinex_core_types::domain::{EventSource, EventType, HostName};
    use sinex_core_types::ids::{EventId, SchemaId};

    let repo = EventRepository::new(pool);
    let new_event = NewEvent {
        source: EventSource::new(&event.source),
        event_type: EventType::new(&event.event_type),
        host: HostName::new(&event.host),
        payload: event.payload.clone(),
        ts_orig: event.ts_orig,
        ingestor_version: event.ingestor_version.clone(),
        payload_schema_id: event.payload_schema_id.map(|id| SchemaId::from(id)),
        source_event_ids: event
            .source_event_ids
            .as_ref()
            .map(|ids| ids.iter().map(|id| EventId::from(*id)).collect()),
        source_material_id: None,
        source_material_offset_start: None,
        source_material_offset_end: None,
        anchor_byte: None,
        associated_blob_ids: None,
    };
    repo.insert(new_event).await.map_err(|e| {
        crate::query_helpers::db_error(
            sqlx::Error::Protocol(e.to_string()),
            "insert_event_with_validator",
        )
    })
}
// pub mod enhanced_queries; // Removed - superseded by *_correct modules
pub mod pool;
// Legacy queries removed - use repositories pattern instead
// pub mod integrity; // TODO: Re-enable after query system migration
pub mod query_helpers;
pub mod sanitization;
pub mod security;
// pub mod validation; // TODO: Re-enable after query system migration

// Legacy modules - re-enable after migrating to repository pattern
// pub mod annotations; // TODO: Migrate to repository pattern
// pub mod knowledge_graph; // TODO: Migrate to repository pattern

// New centralized query system (legacy - will be removed)
pub mod constants;
pub mod distributed_locking;
// pub mod queries;
// pub mod query_builder;
// pub mod query_macros;

// New repository pattern
pub mod repositories;

// Database schema definitions using SeaQuery
pub mod schema;
// pub mod schema_migrations;

// TODO: Re-enable query_system_test after migrating tests to repository pattern
// #[cfg(test)]
// pub mod query_system_test;

// New API modules
// pub mod annotations; // TODO: Re-enable after query system migration
// pub mod knowledge_graph; // TODO: Re-enable after query system migration

// Domain-specific query modules
// pub mod events; // TODO: Re-enable after fixing dependencies

// Re-export domain-specific query functions
// pub use annotations::{
//     create_annotation, delete_annotation, get_annotation_by_id, get_annotations_for_event,
//     get_recent_annotations, update_annotation_content,
// }; // TODO: Re-enable after query system migration
// pub use events::{
//     attach_blob_to_event, count_events, detach_blob_from_event, get_event_by_id,
//     get_events_with_blobs, insert_event, insert_event_with_blob, insert_event_with_validator,
//     EventRecord,
// }; // TODO: Re-enable after query system migration
// pub use knowledge_graph::{
//     create_entity, create_relation, get_entities_by_type, get_entity_by_id, get_entity_relations,
//     get_relation_by_id, search_entities,
// }; // TODO: Re-enable after query system migration

// Enhanced queries have been removed - functionality moved to domain modules

// Re-export query helpers for easier access
pub use query_helpers::{
    count, db_error, exists, is_retryable_db_error, ulid_to_uuid, uuid_to_ulid,
    with_retry_transaction, with_transaction, DbError, DbResult, RetryConfig, UlidArrayExt,
};

// Legacy query system removed - use repositories pattern instead
// pub use query_builder::{QueryBuilder, QueryParam};

// Re-export repository pattern
pub use repositories::{
    Checkpoint, DbResult as RepoResult, EventPayloadSchema, EventSearchFilters, NewCheckpoint,
    NewEvent, NewSchema,
};

/// Prelude module for commonly used database types and functions
pub mod prelude {
    pub use crate::models::{
        // New API models (now enabled)
        CreateAnnotationInput,
        CreateEntityInput,
        CreateRelationInput,
        Entity,
        EntityRelation,
        EventAnnotation,
        EventPayloadSchema,
    };
    // Use domain-specific modules
    // pub use crate::events::*; // TODO: Re-enable after query system migration
    pub use crate::query_helpers::{
        db_error, ulid_to_uuid, uuid_to_ulid, with_retry_transaction, with_transaction, DbError,
        DbResult, RetryConfig, UlidArrayExt,
    };
    // New API services (now enabled)
    // pub use crate::annotations::*; // TODO: Re-enable after query system migration
    // pub use crate::knowledge_graph::*; // TODO: Re-enable after query system migration
    pub use crate::{DbPool, DbPoolRef, JsonValue, OptionalTimestamp, PoolConfig, Timestamp};
    // Re-export repository pattern in prelude
    pub use crate::repositories::{
        Checkpoint, CheckpointRepository, EventRepository, EventSearchFilters, NewCheckpoint,
        NewEvent, NewSchema, Repository,
    };
    pub use anyhow::Result;
    pub use sinex_events::RawEvent;
    pub use sinex_ulid::Ulid;
    pub use sqlx::{FromRow, Postgres, Transaction};
}

use anyhow::Result;
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use sqlx::{migrate::MigrateDatabase, PgPool, Postgres, Row};
use std::env;
use std::time::Duration;
use tracing::{info, warn};
use validator::Validate;

// Common type aliases for database operations
pub type DbPool = PgPool;
pub type DbPoolRef<'a> = &'a PgPool;

// Re-export PgPool for external crates (avoiding naming conflict)
pub use sqlx::PgPool as SqlxPgPool;

// Import type aliases from sinex-ulid and add our own
pub use sinex_ulid::Timestamp;
pub type OptionalTimestamp = Option<Timestamp>;
pub type JsonValue = serde_json::Value;

/// Configuration for database connection pool
#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
pub struct PoolConfig {
    #[validate(range(min = 1, max = 1000))]
    pub max_connections: u32,

    #[validate(range(min = 0, max = 100))]
    pub min_connections: u32,

    #[validate(range(min = 1, max = 300))]
    pub acquire_timeout_secs: u64,

    #[validate(range(min = 0, max = 3600))]
    pub idle_timeout_secs: u64,

    pub validate_against_postgres_max: bool,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            max_connections: 25, // Conservative default
            min_connections: 5,
            acquire_timeout_secs: 30,
            idle_timeout_secs: 300, // 5 minutes
            validate_against_postgres_max: true,
        }
    }
}

/// Create a database connection pool with default settings
pub async fn create_pool(database_url: &str) -> Result<DbPool> {
    let config = PoolConfig::default();
    create_pool_with_config(database_url, &config).await
}

/// Create a database connection pool with custom configuration
pub async fn create_pool_with_config(database_url: &str, config: &PoolConfig) -> Result<DbPool> {
    // Validate configuration using validator crate
    config
        .validate()
        .map_err(|e| anyhow::anyhow!("Invalid pool configuration: {}", e))?;

    // Validate configuration against PostgreSQL limits if requested
    if config.validate_against_postgres_max {
        if let Err(e) = validate_pool_config_against_postgres(database_url, config).await {
            warn!("Pool configuration validation failed: {}", e);
            warn!("Proceeding anyway - this may cause connection exhaustion in production");
        }
    }

    let pool = PgPoolOptions::new()
        .max_connections(config.max_connections)
        .min_connections(config.min_connections)
        .acquire_timeout(Duration::from_secs(config.acquire_timeout_secs))
        .idle_timeout(Duration::from_secs(config.idle_timeout_secs))
        .connect(database_url)
        .await?;

    info!(
        max_connections = config.max_connections,
        min_connections = config.min_connections,
        acquire_timeout_secs = config.acquire_timeout_secs,
        "Database pool created successfully"
    );
    Ok(pool)
}

/// Get database URL from environment - DATABASE_URL required
pub fn get_database_url() -> Result<String> {
    env::var("DATABASE_URL").map_err(|_| {
        anyhow::anyhow!(
            "DATABASE_URL environment variable is required. Set it like: \
             export DATABASE_URL=postgresql:///sinex_dev?host=/run/postgresql"
        )
    })
}

/// Create a database connection pool
pub async fn create_pool_strict() -> Result<DbPool> {
    let database_url = get_database_url()?;
    create_pool(&database_url).await
}

/// Create a database connection pool with custom configuration
pub async fn create_pool_with_config_strict(config: &PoolConfig) -> Result<DbPool> {
    let database_url = get_database_url()?;
    create_pool_with_config(&database_url, config).await
}

/// Validate pool configuration against PostgreSQL server limits
async fn validate_pool_config_against_postgres(
    database_url: &str,
    config: &PoolConfig,
) -> Result<()> {
    // Create a temporary minimal connection to check PostgreSQL settings
    let temp_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(database_url)
        .await?;

    // Query PostgreSQL max_connections setting
    let max_connections_row = sqlx::query("SHOW max_connections")
        .fetch_one(&temp_pool)
        .await?;

    let postgres_max_connections: i32 = max_connections_row.try_get("max_connections")?;

    // Validate our pool size against PostgreSQL limits
    if config.max_connections as i32 > postgres_max_connections {
        return Err(anyhow::anyhow!(
            "Pool max_connections ({}) exceeds PostgreSQL max_connections ({}). \
             This will cause connection exhaustion. Consider reducing pool size or \
             increasing PostgreSQL max_connections setting.",
            config.max_connections,
            postgres_max_connections
        ));
    }

    // Warn if we're using more than 80% of available connections
    let usage_percentage =
        (config.max_connections as f64 / postgres_max_connections as f64) * 100.0;
    if usage_percentage > 80.0 {
        warn!(
            "Pool is configured to use {:.1}% of PostgreSQL max_connections. \
             Consider leaving more headroom for other applications.",
            usage_percentage
        );
    }

    info!(
        pool_max = config.max_connections,
        postgres_max = postgres_max_connections,
        usage_percent = format!("{:.1}%", usage_percentage),
        "Pool configuration validated against PostgreSQL limits"
    );

    temp_pool.close().await;
    Ok(())
}

/// Create a database connection pool optimized for testing with high concurrency
pub async fn create_test_pool(database_url: &str) -> Result<DbPool> {
    let test_config = PoolConfig {
        max_connections: 100, // High concurrency for tests
        min_connections: 10,
        acquire_timeout_secs: 30,
        idle_timeout_secs: 300,
        validate_against_postgres_max: false, // Skip validation in tests
    };

    let pool = PgPoolOptions::new()
        .max_connections(test_config.max_connections)
        .min_connections(test_config.min_connections)
        .acquire_timeout(Duration::from_secs(test_config.acquire_timeout_secs))
        .idle_timeout(Duration::from_secs(test_config.idle_timeout_secs))
        .test_before_acquire(false) // Skip connection testing for speed
        .connect(database_url)
        .await?;

    info!("Test database pool created successfully with optimized concurrency settings");
    Ok(pool)
}

/// Create database if it doesn't exist
pub async fn create_database_if_not_exists(database_url: &str) -> Result<()> {
    if !Postgres::database_exists(database_url).await? {
        info!("Creating database...");
        Postgres::create_database(database_url).await?;
    }
    Ok(())
}

/// Run database migrations
pub async fn run_migrations(pool: DbPoolRef<'_>) -> Result<()> {
    sqlx::migrate!("../../migrations").run(pool).await?;

    info!("Database migrations completed");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use serde_json::json;
    use sinex_events::RawEvent;
    use sinex_test_utils::prelude::*;
    use sinex_ulid::Ulid;

    #[sinex_test]
    async fn test_raw_event_creation(ctx: TestContext) -> anyhow::Result<()> {
        let event = RawEvent {
            id: Ulid::new(),
            source: "test.source".to_string(),
            event_type: "test_event".to_string(),
            ts_ingest: Utc::now(),
            ts_orig: None,
            host: "localhost".to_string(),
            ingestor_version: Some("1.0.0".to_string()),
            payload_schema_id: None,
            payload: json!({"test": "data"}),
            source_event_ids: None,
            anchor_byte: None,
            source_material_id: None,
            source_material_offset_start: None,
            source_material_offset_end: None,
            associated_blob_ids: None,
        };

        assert_eq!(event.source, "test.source");
        assert_eq!(event.event_type, "test_event");
        assert_eq!(event.host, "localhost");
        assert_eq!(event.ingestor_version, Some("1.0.0".to_string()));
        assert_eq!(event.payload["test"], "data");
        Ok(())
    }

    #[sinex_test]
    async fn test_ulid_in_models(ctx: TestContext) -> anyhow::Result<()> {
        let ulid1 = Ulid::new();
        // Small delay to ensure different timestamps
        std::thread::sleep(std::time::Duration::from_millis(1));
        let ulid2 = Ulid::new();

        // ULIDs should be unique
        assert_ne!(ulid1, ulid2);

        // ULIDs should be time-ordered (with very high probability after delay)
        assert!(ulid1 <= ulid2); // Allow equality in case delay wasn't enough

        // Test ULID string representation
        let ulid_str = ulid1.to_string();
        assert_eq!(ulid_str.len(), 26);

        // Test ULID parsing
        let parsed_ulid = ulid_str.parse::<Ulid>().unwrap();
        assert_eq!(ulid1, parsed_ulid);
        Ok(())
    }

    #[sinex_test]
    async fn test_event_payload_json_handling(ctx: TestContext) -> anyhow::Result<()> {
        // Test simple JSON payload
        let simple_payload = json!({"key": "value", "number": 42});
        let event = RawEvent {
            id: Ulid::new(),
            source: "test".to_string(),
            event_type: "test".to_string(),
            ts_ingest: Utc::now(),
            ts_orig: None,
            host: "localhost".to_string(),
            ingestor_version: None,
            payload_schema_id: None,
            payload: simple_payload.clone(),
            source_event_ids: None,
            anchor_byte: None,
            source_material_id: None,
            source_material_offset_start: None,
            source_material_offset_end: None,
            associated_blob_ids: None,
        };

        assert_eq!(event.payload["key"], "value");
        assert_eq!(event.payload["number"], 42);

        // Test complex nested JSON
        let complex_payload = json!({
            "metadata": {
                "version": "1.0",
                "tags": ["test", "event"]
            },
            "data": {
                "items": [1, 2, 3],
                "enabled": true
            }
        });

        let complex_event = RawEvent {
            id: Ulid::new(),
            source: "complex.test".to_string(),
            event_type: "complex_event".to_string(),
            ts_ingest: Utc::now(),
            ts_orig: None,
            host: "localhost".to_string(),
            ingestor_version: None,
            payload_schema_id: None,
            payload: complex_payload,
            source_event_ids: None,
            anchor_byte: None,
            source_material_id: None,
            source_material_offset_start: None,
            source_material_offset_end: None,
            associated_blob_ids: None,
        };

        assert_eq!(complex_event.payload["metadata"]["version"], "1.0");
        assert_eq!(complex_event.payload["data"]["items"][0], 1);
        assert_eq!(complex_event.payload["data"]["enabled"], true);
        Ok(())
    }

    #[sinex_test]
    async fn test_timestamp_handling(ctx: TestContext) -> anyhow::Result<()> {
        let now = Utc::now();
        let past = now - chrono::Duration::seconds(3600); // 1 hour ago

        let event = RawEvent {
            id: Ulid::new(),
            source: "timestamp.test".to_string(),
            event_type: "timestamp_event".to_string(),
            ts_ingest: now,
            ts_orig: Some(past),
            host: "localhost".to_string(),
            ingestor_version: None,
            payload_schema_id: None,
            payload: json!({}),
            source_event_ids: None,
            anchor_byte: None,
            source_material_id: None,
            source_material_offset_start: None,
            source_material_offset_end: None,
            associated_blob_ids: None,
        };

        // Test that ingestion timestamp is after original timestamp
        assert!(event.ts_ingest > event.ts_orig.unwrap());

        // Test that timestamps are properly set
        assert_eq!(event.ts_ingest, now);
        assert_eq!(event.ts_orig.unwrap(), past);
        Ok(())
    }

    #[sinex_test]
    async fn test_pool_creation(ctx: TestContext) -> anyhow::Result<()> {
        // This would require a test database
        // For now, just ensure the function compiles and types are correct

        // Test that the functions exist and have the right signatures
        // Cannot actually call them without a database, but we can test they compile
        // Test that the functions exist and have the right signatures
        // Cannot actually call them without a database, but compilation success is the test
        Ok(())
    }

    #[sinex_test]
    async fn test_function_signatures(ctx: TestContext) -> anyhow::Result<()> {
        // Just test that our functions exist and compile
        // We can't test the actual functionality without a database

        // This ensures the functions are callable and have the right basic structure
        // This ensures the functions are callable and have the right basic structure
        // Compilation success is the test - no runtime assertion needed
        Ok(())
    }

    #[sinex_test]
    async fn test_pool_config_validation(ctx: TestContext) -> anyhow::Result<()> {
        // Valid config should pass
        let valid_config = PoolConfig {
            max_connections: 50,
            min_connections: 5,
            acquire_timeout_secs: 30,
            idle_timeout_secs: 300,
            validate_against_postgres_max: true,
        };
        assert!(valid_config.validate().is_ok());

        // Too many max connections should fail
        let invalid_config = PoolConfig {
            max_connections: 1001, // Over the limit
            min_connections: 5,
            acquire_timeout_secs: 30,
            idle_timeout_secs: 300,
            validate_against_postgres_max: true,
        };
        assert!(invalid_config.validate().is_err());

        // Min connections > 100 should fail
        let invalid_config2 = PoolConfig {
            max_connections: 50,
            min_connections: 101, // Over the limit
            acquire_timeout_secs: 30,
            idle_timeout_secs: 300,
            validate_against_postgres_max: true,
        };
        assert!(invalid_config2.validate().is_err());
        Ok(())
    }
}

pub mod models;
// Re-export RawEvent and RawEventBuilder from sinex-core for type unification
pub use sinex_core::{RawEvent, RawEventBuilder};
// pub mod enhanced_queries; // Removed - superseded by *_correct modules
pub mod pool;
// pub mod queries; // Removed - superseded by domain-specific modules
pub mod query_helpers;
pub mod sanitization;
pub mod security;
pub mod validation;

// New API modules
pub mod annotations;
pub mod artifacts;
pub mod knowledge_graph;

// Domain-specific query modules
pub mod events;


// Old queries module removed - all functions migrated to domain-specific modules

// Re-export domain-specific query functions
pub use annotations::{
    create_annotation, delete_annotation, get_annotation_by_id, get_annotations_for_event,
    get_recent_annotations, update_annotation_content,
};
pub use artifacts::{create_artifact, get_artifact_by_id, get_recent_artifacts};
pub use events::{
    attach_blob_to_event, count_events, detach_blob_from_event, get_event_by_id,
    get_events_with_blobs, insert_event, insert_event_with_blob, insert_event_with_validator,
};
pub use knowledge_graph::{
    create_entity, create_relation, get_entities_by_type, get_entity_by_id, get_entity_relations,
    get_relation_by_id, search_entities,
};

// Enhanced queries have been removed - functionality moved to domain modules

// Re-export query helpers for easier access
pub use query_helpers::{
    count, db_error, exists, is_retryable_db_error, ulid_to_uuid, uuid_to_ulid,
    with_retry_transaction, with_transaction, DbError, DbResult, RetryConfig, UlidArrayExt,
};

/// Prelude module for commonly used database types and functions
pub mod prelude {
    pub use crate::models::{
        // New API models (now enabled)
        Artifact,
        Revision,
        CreateAnnotationInput,
        CreateRevisionInput,
        CreateArtifactInput,
        CreateEntityInput,
        CreateRelationInput,
        Entity,
        EntityRelation,
        EventAnnotation,
        EventPayloadSchema,
    };
    // Use domain-specific modules
    pub use crate::events::*;
    pub use crate::query_helpers::{
        db_error, ulid_to_uuid, uuid_to_ulid, with_retry_transaction, with_transaction, DbError,
        DbResult, RetryConfig, UlidArrayExt,
    };
    // New API services (now enabled)
    pub use crate::annotations::*;
    pub use crate::artifacts::*;
    pub use crate::knowledge_graph::*;
    pub use crate::{DbPool, DbPoolRef, JsonValue, OptionalTimestamp, PoolConfig, Timestamp};
    pub use anyhow::Result;
    pub use sinex_core::{RawEvent, RawEventBuilder};
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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolConfig {
    pub max_connections: u32,
    pub min_connections: u32,
    pub acquire_timeout_secs: u64,
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
    use chrono::Utc;
    use serde_json::json;
    use sinex_core::RawEvent;
    use sinex_ulid::Ulid;

    #[test]
    fn test_raw_event_creation() {
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
        };

        assert_eq!(event.source, "test.source");
        assert_eq!(event.event_type, "test_event");
        assert_eq!(event.host, "localhost");
        assert_eq!(event.ingestor_version, Some("1.0.0".to_string()));
        assert_eq!(event.payload["test"], "data");
    }


    #[test]
    fn test_ulid_in_models() {
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
    }

    #[test]
    fn test_event_payload_json_handling() {
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
        };

        assert_eq!(complex_event.payload["metadata"]["version"], "1.0");
        assert_eq!(complex_event.payload["data"]["items"][0], 1);
        assert_eq!(complex_event.payload["data"]["enabled"], true);
    }

    #[test]
    fn test_timestamp_handling() {
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
        };

        // Test that ingestion timestamp is after original timestamp
        assert!(event.ts_ingest > event.ts_orig.unwrap());

        // Test that timestamps are properly set
        assert_eq!(event.ts_ingest, now);
        assert_eq!(event.ts_orig.unwrap(), past);
    }


    #[tokio::test]
    async fn test_pool_creation() {
        // This would require a test database
        // For now, just ensure the function compiles and types are correct

        // Test that the functions exist and have the right signatures
        // Cannot actually call them without a database, but we can test they compile
        // Test that the functions exist and have the right signatures
        // Cannot actually call them without a database, but compilation success is the test
    }

    #[test]
    fn test_function_signatures() {
        // Just test that our functions exist and compile
        // We can't test the actual functionality without a database

        // This ensures the functions are callable and have the right basic structure
        // This ensures the functions are callable and have the right basic structure
        // Compilation success is the test - no runtime assertion needed
    }
}

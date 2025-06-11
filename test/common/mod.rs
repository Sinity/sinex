use anyhow::Result;
use serde_json::{json, Value};
use sinex_core::{RawEventBuilder, sources, event_type_constants};
use sinex_db::{create_test_pool, queries};
use sinex_ulid::Ulid;
use sqlx::PgPool;

/// Get test database URL with fallback
pub fn test_database_url() -> String {
    std::env::var("TEST_DATABASE_URL")
        .unwrap_or_else(|_| "postgres://sinex_test:testpass@localhost:5433/sinex_test".to_string())
}

/// Create a test database pool with high concurrency settings
pub async fn create_test_db_pool() -> Result<PgPool> {
    let db_url = test_database_url();
    create_test_pool(&db_url).await
}

/// Helper for inserting test events directly via queries
pub async fn insert_test_event(pool: &PgPool, event: &sinex_db::models::RawEvent) -> Result<Ulid> {
    let inserted = queries::insert_event(pool, event).await?;
    Ok(inserted.id)
}

/// Event builder utilities for testing
pub mod events {
    use super::*;

    /// Create a test filesystem event
    pub fn filesystem_event(event_type: &str, path: &str) -> sinex_db::models::RawEvent {
        RawEventBuilder::new(
            sources::FILESYSTEM,
            event_type,
            json!({
                "path": path,
                "size": 1024,
                "modified_time": "2025-01-01T00:00:00Z"
            })
        ).build()
    }

    /// Create a test kitty terminal event  
    pub fn kitty_event(command: &str) -> sinex_db::models::RawEvent {
        RawEventBuilder::new(
            sources::TERMINAL_KITTY,
            event_type_constants::terminal::COMMAND_EXECUTED,
            json!({
                "command": command,
                "exit_code": 0,
                "duration_ms": 100
            })
        ).build()
    }

    /// Create a test hyprland event
    pub fn hyprland_event(event_type: &str, data: Value) -> sinex_db::models::RawEvent {
        RawEventBuilder::new(
            sources::HYPRLAND,
            event_type,
            data
        ).build()
    }

    /// Create a test sinex agent event
    pub fn agent_event(event_type: &str, agent_name: &str) -> sinex_db::models::RawEvent {
        RawEventBuilder::new(
            sources::SINEX,
            event_type,
            json!({
                "agent_name": agent_name,
                "status": "running",
                "version": "1.0.0",
                "timestamp": "2025-01-01T00:00:00Z",
                "uptime_seconds": 3600,
                "events_processed_session": 42,
                "dlq_size": 0
            })
        ).build()
    }

    /// Create an invalid event for error testing
    pub fn invalid_event() -> sinex_db::models::RawEvent {
        RawEventBuilder::new(
            "", // Invalid empty source
            "",
            json!(null)
        ).build()
    }

    /// Create a test file created event
    pub fn file_created_event(path: &str) -> sinex_db::models::RawEvent {
        filesystem_event(event_type_constants::filesystem::FILE_CREATED, path)
    }

    /// Create a test file modified event
    pub fn file_modified_event(path: &str) -> sinex_db::models::RawEvent {
        filesystem_event(event_type_constants::filesystem::FILE_MODIFIED, path)
    }

    /// Create a test agent heartbeat event
    pub fn agent_heartbeat_event(agent_name: &str) -> sinex_db::models::RawEvent {
        agent_event(event_type_constants::sinex::AGENT_HEARTBEAT, agent_name)
    }
}

/// Assertion helpers for common test patterns
pub mod assertions {
    use super::*;
    use sinex_db::models::{RawEvent, AgentManifest};

    /// Assert that two events are equivalent (ignoring IDs and timestamps)
    pub fn assert_events_equivalent(actual: &RawEvent, expected: &RawEvent) {
        assert_eq!(actual.source, expected.source);
        assert_eq!(actual.event_type, expected.event_type);
        assert_eq!(actual.payload, expected.payload);
        assert_eq!(actual.host, expected.host);
        // Note: Don't compare IDs or timestamps as they're generated
    }

    /// Assert that an event was inserted successfully
    pub async fn assert_event_inserted(
        pool: &PgPool,
        event: &RawEvent
    ) -> Result<Ulid> {
        let inserted = queries::insert_event(pool, event).await?;
        assert!(!inserted.id.to_string().is_empty());
        Ok(inserted.id)
    }

    /// Assert that an event insertion fails with validation error
    pub async fn assert_event_insertion_fails(
        pool: &PgPool,
        event: &RawEvent
    ) -> Result<()> {
        let result = queries::insert_event(pool, event).await;
        assert!(result.is_err(), "Expected event insertion to fail, but it succeeded");
        Ok(())
    }

    /// Assert that manifest was registered successfully
    pub async fn assert_manifest_registered(
        pool: &PgPool,
        manifest: &AgentManifest
    ) -> Result<()> {
        let result = queries::upsert_agent_manifest(
            pool,
            &manifest.agent_name,
            &manifest.description.as_deref().unwrap_or(""),
            &manifest.version,
            &manifest.status,
            Some(&manifest.agent_type),
            manifest.config_template_json.clone(),
            manifest.produces_event_types.clone(),
        ).await;
        assert!(result.is_ok(), "Expected manifest registration to succeed");
        assert!(!manifest.agent_name.is_empty());
        assert!(!manifest.version.is_empty());
        Ok(())
    }
}

/// Test data generation utilities
pub mod generators {
    use super::*;

    /// Generate test file path
    pub fn test_file_path(name: &str) -> String {
        format!("/test/path/{}.txt", name)
    }

    /// Get test commands for terminal testing
    pub fn test_commands() -> Vec<&'static str> {
        vec!["ls -la", "cd /home", "git status", "cargo build", "vim file.rs"]
    }

    /// Generate test event with predictable data
    pub fn test_event(index: usize) -> sinex_db::models::RawEvent {
        match index % 3 {
            0 => events::filesystem_event(
                event_type_constants::filesystem::FILE_CREATED,
                &test_file_path(&format!("file_{}", index))
            ),
            1 => events::kitty_event(&test_commands()[index % test_commands().len()]),
            _ => events::hyprland_event("workspace", json!({"id": index})),
        }
    }

    /// Generate multiple test events
    pub fn test_events(count: usize) -> Vec<sinex_db::models::RawEvent> {
        (0..count).map(test_event).collect()
    }

    /// Generate test agent manifest
    pub fn test_agent_manifest(name: &str) -> AgentManifest {
        use sinex_db::models::AgentManifest;
        use chrono::Utc;
        
        AgentManifest {
            agent_name: name.to_string(),
            description: Some(format!("Test agent {}", name)),
            version: "1.0.0".to_string(),
            status: "development".to_string(),
            agent_type: "test".to_string(),
            config_template_json: Some(json!({"test": true})),
            produces_event_types: Some(json!(["test.event"])),
            subscribes_to_event_types: None,
            required_capabilities: None,
            llm_dependencies: None,
            repo_url: Some("https://github.com/test/test".to_string()),
            last_heartbeat_ts: None,
            last_error_ts: None,
            last_error_summary: None,
            registered_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }
}

/// Helper for querying events by ULID
pub async fn get_event_by_id(pool: &PgPool, event_id: Ulid) -> Result<sinex_db::models::RawEvent> {
    let record = sqlx::query!(
        r#"
        SELECT 
            id::uuid as "id!", 
            source, 
            event_type, 
            ts_ingest,
            ts_orig, 
            host, 
            ingestor_version, 
            payload_schema_id::uuid as payload_schema_id, 
            payload
        FROM raw.events
        WHERE id = $1::uuid::ulid
        "#,
        event_id.to_uuid()
    )
    .fetch_one(pool)
    .await?;
    
    Ok(sinex_db::models::RawEvent {
        id: record.id.into(),
        source: record.source,
        event_type: record.event_type,
        ts_ingest: record.ts_ingest.unwrap_or_else(|| chrono::Utc::now()),
        ts_orig: record.ts_orig,
        host: record.host,
        ingestor_version: record.ingestor_version,
        payload_schema_id: record.payload_schema_id.map(Into::into),
        payload: record.payload,
    })
}

/// Helper for getting event count from database
pub async fn get_event_count(pool: &PgPool) -> Result<i64> {
    let record = sqlx::query!("SELECT COUNT(*) as count FROM raw.events")
        .fetch_one(pool)
        .await?;
    Ok(record.count.unwrap_or(0))
}

/// Helper for checking if an event exists by ULID
pub async fn event_exists(pool: &PgPool, event_id: Ulid) -> Result<bool> {
    let exists = sqlx::query!(
        r#"
        SELECT EXISTS(
            SELECT 1 FROM raw.events WHERE id = $1::uuid::ulid
        ) as "exists!"
        "#,
        event_id.to_uuid()
    )
    .fetch_one(pool)
    .await?;
    
    Ok(exists.exists)
}

/// Macros for common test patterns
#[macro_export]
macro_rules! test_event_insertion {
    ($test_name:ident, $event_builder:expr) => {
        #[sqlx::test]
        async fn $test_name(pool: sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
            let event = $event_builder;
            crate::common::assertions::assert_event_inserted(&pool, &event).await?;
            Ok(())
        }
    };
}

#[macro_export]
macro_rules! test_invalid_event_insertion {
    ($test_name:ident, $event_builder:expr) => {
        #[sqlx::test]
        async fn $test_name(pool: sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
            let event = $event_builder;
            crate::common::assertions::assert_event_insertion_fails(&pool, &event).await?;
            Ok(())
        }
    };
}

/// Test environment utilities
pub mod env {
    /// Check if we're running in a test environment
    pub fn is_test_env() -> bool {
        std::env::var("TEST_DATABASE_URL").is_ok() || 
        std::env::var("CARGO_TEST").is_ok()
    }

    /// Setup test environment variables
    pub fn setup_test_env() {
        if std::env::var("RUST_LOG").is_err() {
            std::env::set_var("RUST_LOG", "debug");
        }
        if std::env::var("DATABASE_URL").is_err() {
            std::env::set_var("DATABASE_URL", super::test_database_url());
        }
    }
    
    /// Initialize test logging
    pub fn init_test_logging() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter("debug")
            .try_init();
    }
}

// Re-export commonly used items for convenience
pub use sinex_db::models::AgentManifest;
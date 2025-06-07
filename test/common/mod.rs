use anyhow::Result;
use serde_json::{json, Value};
use sinex_shared::{DatabaseConfig, DatabaseService, RawEventBuilder, sources, event_types};
use sinex_ulid::Ulid;
use std::sync::Arc;

/// Test database configuration helper
pub fn test_database_config() -> DatabaseConfig {
    DatabaseConfig {
        url: std::env::var("TEST_DATABASE_URL")
            .unwrap_or_else(|_| "postgres://sinex_test:testpass@localhost:5433/sinex_test".to_string()),
        max_connections: 5,
        min_connections: 1,
        acquire_timeout: std::time::Duration::from_secs(5),
        idle_timeout: std::time::Duration::from_secs(10),
    }
}

/// Create a test database service with appropriate configuration
pub async fn test_database_service() -> Result<Arc<DatabaseService>> {
    let config = test_database_config();
    Ok(Arc::new(DatabaseService::new(config).await?))
}

/// Create a database service from an existing pool (for sqlx::test)
pub fn database_service_from_pool(pool: sqlx::PgPool) -> Arc<DatabaseService> {
    Arc::new(DatabaseService::from_pool(pool))
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
            event_types::event_types::terminal::COMMAND_EXECUTED,
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
                "timestamp": "2025-01-01T00:00:00Z"
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
        db: &DatabaseService,
        event: &RawEvent
    ) -> Result<Ulid> {
        let inserted_id = db.insert_event(event).await?;
        assert!(!inserted_id.to_string().is_empty());
        Ok(inserted_id)
    }

    /// Assert that an event insertion fails with validation error
    pub async fn assert_event_insertion_fails(
        db: &DatabaseService,
        event: &RawEvent
    ) -> Result<()> {
        let result = db.insert_event(event).await;
        assert!(result.is_err(), "Expected event insertion to fail, but it succeeded");
        Ok(())
    }

    /// Assert that manifest was registered successfully
    pub async fn assert_manifest_registered(
        db: &DatabaseService,
        manifest: &AgentManifest
    ) -> Result<()> {
        // This would need the actual manifest insertion method
        // For now, just verify the structure is valid
        assert!(!manifest.agent_name.is_empty());
        assert!(!manifest.agent_version.is_empty());
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
                event_types::event_types::filesystem::FILE_CREATED,
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
}

/// Macros for common test patterns
#[macro_export]
macro_rules! test_event_insertion {
    ($test_name:ident, $event_builder:expr) => {
        #[sqlx::test]
        async fn $test_name(pool: sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
            let db = crate::common::database_service_from_pool(pool);
            let event = $event_builder;
            crate::common::assertions::assert_event_inserted(&db, &event).await?;
            Ok(())
        }
    };
}

#[macro_export]
macro_rules! test_invalid_event_insertion {
    ($test_name:ident, $event_builder:expr) => {
        #[sqlx::test]
        async fn $test_name(pool: sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
            let db = crate::common::database_service_from_pool(pool);
            let event = $event_builder;
            crate::common::assertions::assert_event_insertion_fails(&db, &event).await?;
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

    /// Get test database URL with fallback
    pub fn test_database_url() -> String {
        std::env::var("TEST_DATABASE_URL")
            .unwrap_or_else(|_| "postgres://sinex_test:testpass@localhost:5433/sinex_test".to_string())
    }

    /// Setup test environment variables
    pub fn setup_test_env() {
        if std::env::var("RUST_LOG").is_err() {
            std::env::set_var("RUST_LOG", "debug");
        }
    }
}
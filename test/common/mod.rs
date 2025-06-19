//! Common test utilities and helpers

#![allow(dead_code)] // Test utilities may not all be used
#![allow(unused_variables)] // Test patterns

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
#[allow(dead_code)]
pub async fn insert_test_event(pool: &PgPool, event: &sinex_db::models::RawEvent) -> Result<Ulid> {
    let inserted = queries::insert_event(pool, event).await?;
    Ok(inserted.id)
}

/// Event builder utilities for testing
#[allow(dead_code)]
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
        use chrono::Utc;
        sinex_db::models::RawEvent {
            id: sinex_ulid::Ulid::new(),
            source: "".to_string(), // Invalid empty source
            event_type: "".to_string(), // Invalid empty event_type
            ts_ingest: Utc::now(),
            ts_orig: None,
            host: "".to_string(), // Invalid empty host
            ingestor_version: None,
            payload_schema_id: None,
            payload: json!(null),
        }
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
#[allow(dead_code)]
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
#[allow(dead_code)]
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

/// Helper for querying events by ULID (delegates to sinex_db::queries)
pub async fn get_event_by_id(pool: &PgPool, event_id: Ulid) -> Result<sinex_db::models::RawEvent> {
    queries::get_event_by_id(pool, event_id).await
}

/// Helper for getting recent events
pub async fn get_recent_events(pool: &PgPool, limit: i64) -> Result<Vec<sinex_db::models::RawEvent>> {
    queries::get_recent_events(pool, limit).await
}

/// Helper for getting events by source
pub async fn get_events_by_source(pool: &PgPool, source: &str, limit: i64) -> Result<Vec<sinex_db::models::RawEvent>> {
    queries::get_events_by_source(pool, source, limit).await
}

/// Helper for getting events by type
pub async fn get_events_by_type(pool: &PgPool, event_type: &str, limit: i64) -> Result<Vec<sinex_db::models::RawEvent>> {
    queries::get_events_by_type(pool, event_type, limit).await
}

/// Helper for getting events in time range
pub async fn get_events_in_time_range(pool: &PgPool, start_time: chrono::DateTime<chrono::Utc>, end_time: chrono::DateTime<chrono::Utc>) -> Result<Vec<sinex_db::models::RawEvent>> {
    queries::get_events_in_time_range(pool, start_time, end_time).await
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
#[allow(dead_code)]
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

/// Helper for creating a simple test event with custom source and type
pub fn create_test_event(source: &str, event_type: &str) -> sinex_db::models::RawEvent {
    RawEventBuilder::new(
        source,
        event_type,
        json!({
            "test": true,
            "timestamp": chrono::Utc::now().to_rfc3339()
        })
    ).build()
}

/// Helper for creating test events with specific payload
pub fn create_test_event_with_payload(source: &str, event_type: &str, payload: Value) -> sinex_db::models::RawEvent {
    RawEventBuilder::new(source, event_type, payload).build()
}

/// Health check utilities for integration tests
#[allow(dead_code)]
pub mod health {
    use super::*;
    
    /// Check if database is healthy
    pub async fn check_database_health(pool: &PgPool) -> Result<bool> {
        match sqlx::query("SELECT 1").fetch_one(pool).await {
            Ok(_) => Ok(true),
            Err(_) => Ok(false),
        }
    }
    
    /// Check if git-annex is available
    pub async fn check_git_annex_available() -> bool {
        use std::process::Command;
        
        match Command::new("git").args(["annex", "version"]).output() {
            Ok(output) => output.status.success(),
            Err(_) => false,
        }
    }
    
    /// Check if required system tools are available
    pub async fn check_system_tools() -> Vec<(String, bool)> {
        let tools = vec!["git", "kitty", "hyprctl", "wl-paste", "xclip"];
        let mut results = Vec::new();
        
        for tool in tools {
            let available = std::process::Command::new(tool)
                .arg("--version")
                .output()
                .map(|o| o.status.success())
                .unwrap_or(false);
            results.push((tool.to_string(), available));
        }
        
        results
    }
}

/// Cleanup utilities for integration tests
#[allow(dead_code)]
pub mod cleanup {
    use super::*;
    
    /// Truncate all test tables
    pub async fn truncate_all_tables(pool: &PgPool) -> Result<()> {
        // Use the existing cleanup function from test_setup
        crate::test_setup::cleanup_test_data(pool).await
            .map_err(|e| anyhow::anyhow!("Failed to cleanup test data: {}", e))
    }
    
    /// Clean up test files and directories
    pub async fn cleanup_test_files(paths: &[&str]) -> Result<()> {
        for path in paths {
            if std::path::Path::new(path).exists() {
                if std::path::Path::new(path).is_dir() {
                    let _ = std::fs::remove_dir_all(path);
                } else {
                    let _ = std::fs::remove_file(path);
                }
            }
        }
        Ok(())
    }
}

/// Configuration utilities for integration tests
#[allow(dead_code)]
pub mod config {
    use super::*;
    use tempfile::NamedTempFile;
    use tokio::fs;
    
    /// Create a temporary configuration file
    pub async fn create_temp_config(content: &str) -> Result<NamedTempFile> {
        let temp_file = NamedTempFile::new()?;
        fs::write(temp_file.path(), content).await?;
        Ok(temp_file)
    }
    
    /// Create a minimal valid configuration
    pub fn minimal_valid_config() -> String {
        r#"
enabled_events = ["filesystem.file.created"]

[monitoring]
health_check_interval_secs = 30
metrics_enabled = true

[database]
max_connections = 10
"#.to_string()
    }
    
    /// Create a comprehensive test configuration
    pub fn comprehensive_test_config() -> String {
        r#"
enabled_events = [
    "filesystem.file.created",
    "filesystem.file.modified", 
    "terminal.command.executed",
    "hyprland.window.focus",
    "clipboard.content.changed"
]

[monitoring]
health_check_interval_secs = 30
metrics_enabled = true
failure_threshold = 3
recovery_timeout_secs = 60

[database]
max_connections = 50
connection_timeout_secs = 30
health_check_enabled = true

[git_annex]
enabled = true
repository_path = "/tmp/test-annex"
size_threshold_bytes = 1024000
"#.to_string()
    }
}

// Re-export commonly used items for convenience
pub use sinex_db::models::AgentManifest;
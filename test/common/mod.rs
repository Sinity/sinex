//! Common test utilities and helpers

#![allow(dead_code)] // Test utilities may not all be used
#![allow(unused_variables)] // Test patterns

// Test prelude for standardized imports
pub mod prelude;

// Database helper functions and macros
pub mod database_helpers;

// NEW: Unified database access
pub mod database;

// Test database isolation
pub mod test_database;

// Unified test context for all tests
pub mod test_context;

// Unified event builder hierarchy
pub mod event_builders;

// Re-export the procedural macros from sinex-test-macros crate
use crate::common::prelude::*;
use sinex_core::{RawEventBuilder, sources, event_type_constants};

/// Get test database URL with fallback
pub fn test_database_url() -> String {
    std::env::var("TEST_DATABASE_URL")
        .unwrap_or_else(|_| "postgres://sinex_test:testpass@localhost:5433/sinex_test".to_string())
}

/// Create a test database pool with high concurrency settings
pub async fn create_test_db_pool() -> Result<PgPool> {
    let test_pool = database::TestPool::with_strategy(database::CleanupStrategy::None).await?;
    Ok(test_pool.pool().clone())
}

/// Insert any event into database (renamed for clarity)
#[allow(dead_code)]
pub async fn insert_event(pool: &PgPool, event: &sinex_db::models::RawEvent) -> Result<Ulid> {
    let inserted = queries::insert_event(&pool, event).await?;
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

    /// Create a test event for race condition testing
    pub fn race_test_event(target: &str) -> sinex_db::models::RawEvent {
        RawEventBuilder::new(
            "test",
            "race.test",
            json!({"target": target})
        ).build()
    }

    /// Create a test event with minimal fields for adversarial testing
    pub fn adversarial_test_event(event_type: &str, payload: serde_json::Value) -> sinex_db::models::RawEvent {
        RawEventBuilder::new("test", event_type, payload).build()
    }

    /// Create a batch of test events efficiently
    pub fn test_event_batch(source: &str, event_type: &str, count: usize) -> Vec<sinex_db::models::RawEvent> {
        (0..count).map(|i| {
            RawEventBuilder::new(
                source,
                event_type,
                json!({"sequence": i, "batch": true, "timestamp": chrono::Utc::now()})
            ).build()
        }).collect()
    }

    /// Create a quick filesystem event with default payload
    pub fn quick_filesystem_event(path: &str) -> sinex_db::models::RawEvent {
        filesystem_event(event_type_constants::filesystem::FILE_CREATED, path)
    }

    /// Create a quick agent heartbeat with default payload
    pub fn quick_agent_heartbeat(agent_name: &str) -> sinex_db::models::RawEvent {
        agent_event(event_type_constants::sinex::AGENT_HEARTBEAT, agent_name)
    }

    /// Create test events for timing and ordering tests
    pub fn timing_test_event(sequence: u32, delay_ms: u64) -> sinex_db::models::RawEvent {
        RawEventBuilder::new(
            "timing_test",
            "sequence.event",
            json!({
                "sequence": sequence,
                "delay_ms": delay_ms,
                "created_at": chrono::Utc::now()
            })
        ).build()
    }

    /// Create test events for performance testing
    pub fn performance_test_event(payload_size_kb: usize) -> sinex_db::models::RawEvent {
        let large_data = "x".repeat(payload_size_kb * 1024);
        RawEventBuilder::new(
            "performance_test",
            "large.payload",
            json!({
                "size_kb": payload_size_kb,
                "data": large_data,
                "metadata": {
                    "test_type": "performance",
                    "created_at": chrono::Utc::now()
                }
            })
        ).build()
    }

    /// Create agent heartbeat event for chaos testing
    pub fn agent_heartbeat_chaos_event(agent_name: &str, version: Option<&str>) -> sinex_db::models::RawEvent {
        let mut builder = RawEventBuilder::new(
            "agent",
            "agent.heartbeat",
            json!({
                "agent_name": agent_name,
                "status": "alive",
                "version": version.unwrap_or("1.0.0")
            })
        );
        
        if let Some(v) = version {
            builder = builder.with_ingestor_version(v);
        }
        
        builder.build()
    }

    /// Create filesystem event for chaos testing
    pub fn filesystem_chaos_event(event_type: &str, path: &str, version: Option<&str>) -> sinex_db::models::RawEvent {
        let mut builder = RawEventBuilder::new(
            "filesystem",
            event_type,
            json!({
                "path": path,
                "chaos_test": true
            })
        );
        
        if let Some(v) = version {
            builder = builder.with_ingestor_version(v);
        }
        
        builder.build()
    }

    /// Create large payload event for boundary testing
    pub fn large_payload_test_event(data_size: usize) -> sinex_db::models::RawEvent {
        let large_data = "x".repeat(data_size);
        RawEventBuilder::new(
            "test",
            "large.payload",
            json!({
                "data": large_data,
                "size": data_size,
                "test_type": "boundary"
            })
        ).build()
    }

    /// Create indexed test event for database boundary testing
    pub fn indexed_test_event(index: i64, event_time: chrono::DateTime<chrono::Utc>) -> sinex_db::models::RawEvent {
        RawEventBuilder::new(
            "btree_test",
            "index.split",
            json!({
                "index": index,
                "timestamp": event_time,
                "test_type": "btree_boundary"
            })
        ).with_orig_timestamp(event_time).build()
    }

    /// Generic adversarial event with customizable source and type  
    pub fn generic_adversarial_event(source: &str, event_type: &str, payload: serde_json::Value, version: Option<&str>) -> sinex_db::models::RawEvent {
        let mut builder = RawEventBuilder::new(source, event_type, payload);
        
        if let Some(v) = version {
            builder = builder.with_ingestor_version(v);
        }
        
        builder.build()
    }

    /// Create a raw event with specified timestamp (for comprehensive tests)
    pub fn create_raw_event(source: &str, event_type: &str, payload: serde_json::Value, timestamp: chrono::DateTime<chrono::Utc>) -> sinex_db::models::RawEvent {
        RawEventBuilder::new(source, event_type, payload)
            .with_orig_timestamp(timestamp)
            .build()
    }
}

/// Assertion helpers for common test patterns
#[allow(dead_code)]
pub mod assertions {
    use super::*;
    use sinex_db::models::{RawEvent, AgentManifest};

    /// Assert that two events are equivalent (ignoring IDs and timestamps)
    pub fn assert_events_equivalent(actual: &RawEvent, expected: &RawEvent) {
        pretty_assertions::assert_eq!(actual.source, expected.source);
        pretty_assertions::assert_eq!(actual.event_type, expected.event_type);
        pretty_assertions::assert_eq!(actual.payload, expected.payload);
        pretty_assertions::assert_eq!(actual.host, expected.host);
        // Note: Don't compare IDs or timestamps as they're generated
    }

    /// Assert that an event was inserted successfully
    pub async fn assert_event_inserted(
        pool: &PgPool,
        event: &RawEvent
    ) -> Result<Ulid> {
        let inserted = queries::insert_event(&pool, event).await?;
        assert!(!inserted.id.to_string().is_empty());
        Ok(inserted.id)
    }

    /// Assert that an event insertion fails with validation error
    pub async fn assert_event_insertion_fails(
        pool: &PgPool,
        event: &RawEvent
    ) -> Result<(), anyhow::Error> {
        let result = queries::insert_event(&pool, event).await;
        assert!(result.is_err(), "Expected event insertion to fail, but it succeeded");
        Ok(())
    }

    /// Assert that manifest was registered successfully
    pub async fn assert_manifest_registered(
        pool: &PgPool,
        manifest: &AgentManifest
    ) -> Result<(), anyhow::Error> {
        let result = queries::upsert_agent_manifest(&pool,
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

/// Enhanced test data generation with realistic patterns
#[allow(dead_code)]
pub mod generators {
    use super::*;

    /// Generate test file path
    pub fn file_path(name: &str) -> String {
        format!("/test/path/{}.txt", name)
    }

    /// Common terminal commands for testing
    pub fn common_commands() -> Vec<&'static str> {
        vec!["ls -la", "cd /home", "git status", "cargo build", "vim file.rs"]
    }

    /// Generate test event with predictable data based on index
    pub fn indexed_event(index: usize) -> sinex_db::models::RawEvent {
        match index % 3 {
            0 => events::filesystem_event(
                event_type_constants::filesystem::FILE_CREATED,
                &file_path(&format!("file_{}", index))
            ),
            1 => events::kitty_event(&common_commands()[index % common_commands().len()]),
            _ => events::hyprland_event("workspace", json!({"id": index})),
        }
    }

    /// Generate multiple test events
    pub fn test_events(count: usize) -> Vec<sinex_db::models::RawEvent> {
        (0..count).map(indexed_event).collect()
    }

    /// Generate realistic filesystem events with proper paths
    pub fn realistic_filesystem_events(count: usize) -> Vec<sinex_db::models::RawEvent> {
        let realistic_paths = vec![
            "/home/user/Documents/report.pdf",
            "/home/user/Code/project/src/main.rs",
            "/tmp/cache/session_data.json",
            "/var/log/system.log",
            "/home/user/.config/app/settings.toml",
            "/home/user/Downloads/image.png",
        ];
        
        let event_types = vec![
            event_type_constants::filesystem::FILE_CREATED,
            event_type_constants::filesystem::FILE_MODIFIED,
            event_type_constants::filesystem::FILE_DELETED,
        ];
        
        (0..count).map(|i| {
            let path = realistic_paths[i % realistic_paths.len()];
            let event_type = event_types[i % event_types.len()];
            events::filesystem_event(event_type, path)
        }).collect()
    }

    /// Generate realistic terminal command events
    pub fn realistic_terminal_events(count: usize) -> Vec<sinex_db::models::RawEvent> {
        let realistic_commands = vec![
            "git status",
            "cargo build --release",
            "ls -la /home/user",
            "cd ~/Projects/sinex",
            "vim src/main.rs",
            "grep -r 'TODO' .",
            "find . -name '*.rs' -exec wc -l {} +",
            "docker ps -a",
            "systemctl status postgresql",
            "nix develop",
        ];
        
        (0..count).map(|i| {
            let command = realistic_commands[i % realistic_commands.len()];
            events::kitty_event(command)
        }).collect()
    }

    /// Generate events with realistic time distribution
    pub fn time_distributed_events(
        count: usize, 
        start_time: chrono::DateTime<chrono::Utc>,
        interval_secs: i64
    ) -> Vec<sinex_db::models::RawEvent> {
        (0..count).map(|i| {
            let mut event = indexed_event(i);
            event.ts_orig = Some(start_time + chrono::Duration::seconds(interval_secs * i as i64));
            event
        }).collect()
    }

    /// Generate events simulating burst patterns
    pub fn burst_pattern_events(burst_count: usize, burst_size: usize) -> Vec<sinex_db::models::RawEvent> {
        let mut events = Vec::new();
        let base_time = chrono::Utc::now();
        
        for burst in 0..burst_count {
            let burst_start = base_time + chrono::Duration::minutes(burst as i64 * 10);
            
            for i in 0..burst_size {
                let mut event = indexed_event(burst * burst_size + i);
                event.ts_orig = Some(burst_start + chrono::Duration::milliseconds(i as i64 * 100));
                events.push(event);
            }
        }
        
        events
    }

    /// Generate events with realistic payload sizes
    pub fn variable_payload_events(count: usize) -> Vec<sinex_db::models::RawEvent> {
        (0..count).map(|i| {
            let payload = match i % 4 {
                0 => json!({"small": "data"}), // Small payload
                1 => json!({"medium": "data", "details": vec![1, 2, 3, 4, 5]}), // Medium payload
                2 => json!({"large": "data".repeat(100), "metadata": {"tags": vec!["tag1", "tag2", "tag3"]}}), // Large payload
                _ => json!({"binary_data": "a".repeat(1000)}), // Very large payload
            };
            
            test_event_with_payload(
                sources::FILESYSTEM,
                event_type_constants::filesystem::FILE_MODIFIED,
                payload
            )
        }).collect()
    }

    /// Generate test agent manifest
    pub fn test_agent_manifest(name: &str) -> AgentManifest {
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

// Query functions are available directly from sinex_db::queries - no need for wrappers

/// Helper for getting event count from database
pub async fn get_event_count(pool: &PgPool) -> Result<i64> {
    let record = sqlx::query!("SELECT COUNT(*) as count FROM raw.events")
        .fetch_one(pool)
        .await?;
    Ok(record.count.unwrap_or(0i64))
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
        #[sinex_test]
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
        #[sinex_test]
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

/// Create test event with custom payload
pub fn test_event_with_payload(source: &str, event_type: &str, payload: Value) -> sinex_db::models::RawEvent {
    RawEventBuilder::new(source, event_type, payload).build()
}

/// Legacy compatibility alias
pub fn create_test_event_with_payload(source: &str, event_type: &str, payload: Value) -> sinex_db::models::RawEvent {
    test_event_with_payload(source, event_type, payload)
}

/// Create a simple test event with source and type (legacy compatibility)
pub fn create_test_event(source: &str, event_type: &str) -> sinex_db::models::RawEvent {
    RawEventBuilder::new(source, event_type, json!({"test": true})).build()
}

/// Helper for creating a test agent with default settings
pub async fn create_test_agent(pool: &PgPool, agent_name: &str) -> Result<(), anyhow::Error> {
    let manifest = generators::test_agent_manifest(agent_name);
    queries::upsert_agent_manifest(&pool,
        &manifest.agent_name,
        manifest.description.as_deref().unwrap_or(""),
        &manifest.version,
        &manifest.status,
        Some(&manifest.agent_type),
        manifest.config_template_json.clone(),
        manifest.produces_event_types.clone(),
    ).await?;
    Ok(())
}

/// Quick test event insertion - creates minimal event
#[allow(dead_code)]
pub async fn insert_test_event(pool: &PgPool, source: &str, event_type: &str) -> Result<Ulid> {
    let event = RawEventBuilder::new(source, event_type, json!({"test": true})).build();
    insert_event(&pool, &event).await
}

/// Helper for creating agent with specific subscriptions
pub async fn create_agent_with_subscriptions(
    pool: &PgPool, 
    agent_name: &str, 
    subscriptions: &serde_json::Value
) -> Result<(), anyhow::Error> {
    // Create a test agent manifest and add subscriptions
    let mut manifest = generators::test_agent_manifest(agent_name);
    manifest.subscribes_to_event_types = Some(subscriptions.clone());
    
    queries::upsert_agent_manifest(&pool,
        &manifest.agent_name,
        manifest.description.as_deref().unwrap_or(""),
        &manifest.version,
        &manifest.status,
        Some(&manifest.agent_type),
        manifest.config_template_json.clone(),
        manifest.produces_event_types.clone(),
    ).await?;
    
    // The agent is registered via upsert_agent_manifest above
    
    Ok(())
}

/// Simple database test utilities
#[allow(dead_code)]
pub mod db_utils {
    use super::*;
    
    /// Insert multiple test events quickly
    pub async fn insert_test_events(pool: &PgPool, count: usize) -> Result<Vec<Ulid>> {
        let mut ids = Vec::new();
        for i in 0..count {
            let event = generators::indexed_event(i);
            let id = insert_event(&pool, &event).await?;
            ids.push(id);
        }
        Ok(ids)
    }
}

/// Essential assertion helpers
#[allow(dead_code)]
pub mod assertions_extra {
    
    use sinex_db::models::RawEvent;

    /// Assert events are in chronological order
    pub fn assert_events_in_order(events: &[RawEvent]) {
        for window in events.windows(2) {
            let (prev, curr) = (&window[0], &window[1]);
            assert!(
                prev.ts_ingest <= curr.ts_ingest,
                "Events not in chronological order"
            );
        }
    }

    /// Assert no duplicate events (by ULID)
    pub fn assert_no_duplicate_events(events: &[RawEvent]) {
        let mut seen_ids = std::collections::HashSet::new();
        for event in events {
            assert!(seen_ids.insert(event.id), "Duplicate event found: {}", event.id);
        }
    }
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
    pub async fn truncate_all_tables(pool: &PgPool) -> Result<(), anyhow::Error> {
        // Clean up test data manually
        sqlx::query!("DELETE FROM sinex_schemas.work_queue WHERE target_agent_name LIKE 'test_%'")
            .execute(pool).await?;
        sqlx::query!("DELETE FROM sinex_schemas.agent_manifests WHERE agent_name LIKE 'test_%'")
            .execute(pool).await?;
        sqlx::query!("DELETE FROM raw.events WHERE source LIKE 'test_%'")
            .execute(pool).await?;
        Ok(())
    }
    
    /// Clean up test files and directories
    pub async fn cleanup_test_files(paths: &[&str]) -> Result<(), anyhow::Error> {
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

/// Test resource management utilities
#[allow(dead_code)]
pub mod resources {
    use super::*;
    use tempfile::{TempDir, NamedTempFile};
    use tokio::fs;
    use std::path::{Path, PathBuf};
    
    /// Create temporary directory with standard test structure
    pub fn temp_dir() -> Result<TempDir> {
        TempDir::new().map_err(|e| anyhow::anyhow!("Failed to create temp dir: {}", e))
    }
    
    /// Create temp directory with specific subdirectories
    pub fn temp_dir_with_structure(subdirs: &[&str]) -> Result<TempDir> {
        let temp = temp_dir()?;
        for subdir in subdirs {
            std::fs::create_dir_all(temp.path().join(subdir))?;
        }
        Ok(temp)
    }
    
    /// Create a temporary configuration file
    pub async fn temp_config_file(content: &str) -> Result<NamedTempFile> {
        let temp_file = NamedTempFile::new()?;
        fs::write(temp_file.path(), content).await?;
        Ok(temp_file)
    }
    
    /// Create test file with content
    pub fn create_test_file(dir: &Path, name: &str, content: &str) -> Result<PathBuf> {
        let file_path = dir.join(name);
        std::fs::write(&file_path, content)?;
        Ok(file_path)
    }
}

// Re-export commonly used items for convenience
pub use sinex_db::models::AgentManifest;
pub use sinex_db::queries::{get_event_by_id, get_events_by_source, get_recent_events, get_events_by_type};
/// Timing optimization utilities to reduce test flakiness
pub mod timing_optimization;

/// Validation test utilities
pub mod validation_test_utils;

/// Schema test utilities 
pub mod schema_test_utils;

/// Worker test utilities
pub mod worker_test_utils;

/// Event source testing utilities
#[allow(dead_code)]
pub mod event_sources {
    use super::*;
    use sinex_core::{EventSource, EventSourceContext, RawEvent};
    use tokio::time::{timeout, Duration};
    
    /// Create EventSourceContext with test configuration
    pub fn test_context(config: Value) -> EventSourceContext {
        EventSourceContext::new(config)
    }
    
    /// Create EventSourceContext with database pool
    pub fn test_context_with_db(config: Value, pool: sqlx::PgPool) -> EventSourceContext {
        EventSourceContext::new(config).with_db_pool(pool)
    }
    
    /// Standard filesystem event source config
    pub fn filesystem_config(watch_path: &str) -> Value {
        serde_json::json!({
            "watch_patterns": [format!("{}/**/*", watch_path)],
            "ignore_patterns": ["*.tmp", "*.log"],
            "debounce_ms": 50
        })
    }
    
    /// Standard terminal event source config  
    pub fn terminal_config(socket_path: &str) -> Value {
        serde_json::json!({
            "socket_path": socket_path,
            "polling_interval_secs": 1
        })
    }
    
    /// Standard clipboard event source config
    pub fn clipboard_config() -> Value {
        serde_json::json!({
            "monitor_clipboard": true,
            "monitor_primary": false,
            "poll_interval_ms": 100,
            "max_content_size": 1024
        })
    }
    
    /// Test event source until it produces events or times out
    pub async fn test_event_production<T: EventSource>(
        mut source: T,
        timeout_secs: u64,
        min_events: usize,
    ) -> Result<Vec<RawEvent>> {
        let (tx, mut rx) = mpsc::channel(100);
        let timeout_duration = Duration::from_secs(timeout_secs);
        
        let source_handle = tokio::spawn(async move {
            source.stream_events(tx).await
        });
        
        let mut events = Vec::new();
        let start = std::time::Instant::now();
        
        while events.len() < min_events && start.elapsed() < timeout_duration {
            match timeout(Duration::from_millis(100), rx.recv()).await {
                Ok(Some(event)) => events.push(event),
                Ok(None) => break, // Channel closed
                Err(_) => continue, // Timeout, keep waiting
            }
        }
        
        source_handle.abort();
        Ok(events)
    }
}

/// Test parallelization utilities
#[allow(dead_code)]
pub mod parallelization {
    use super::*;
    use tokio::task::JoinSet;

    /// Parallel test executor for independent test operations
    pub struct ParallelTestExecutor {
        max_concurrent: usize,
    }

    impl ParallelTestExecutor {
        /// Create a new parallel executor with concurrency limit
        pub fn new(max_concurrent: usize) -> Self {
            Self { max_concurrent }
        }

        /// Execute database operations in parallel with shared pool
        pub async fn execute_db_parallel<F, T, Fut>(
            &self,
            pool: Arc<PgPool>,
            operations: Vec<F>,
        ) -> Vec<Result<T, Box<dyn std::error::Error + Send + Sync>>>
        where
            F: FnOnce(Arc<PgPool>) -> Fut + Send + 'static,
            Fut: std::future::Future<Output = Result<T, Box<dyn std::error::Error + Send + Sync>>> + Send,
            T: Send + 'static,
        {
            let mut join_set = JoinSet::new();
            let mut results = Vec::new();
            let mut pending = operations.into_iter();

            // Start initial batch
            for _ in 0..self.max_concurrent {
                if let Some(op) = pending.next() {
                    let pool_clone = pool.clone();
                    join_set.spawn(async move { op(pool_clone).await });
                }
            }

            // Process results and start new tasks
            while let Some(result) = join_set.join_next().await {
                match result {
                    Ok(op_result) => results.push(op_result),
                    Err(join_error) => results.push(Err(Box::new(join_error) as Box<dyn std::error::Error + Send + Sync>)),
                }

                // Start next operation if available
                if let Some(op) = pending.next() {
                    let pool_clone = pool.clone();
                    join_set.spawn(async move { op(pool_clone).await });
                }
            }

            results
        }
    }

    /// Utility for running tests with shared resources safely
    pub async fn run_tests_with_shared_pool<F, T, Fut>(
        pool: Arc<PgPool>,
        operations: Vec<F>,
        max_concurrent: usize,
    ) -> Vec<Result<T, Box<dyn std::error::Error + Send + Sync>>>
    where
        F: FnOnce(Arc<PgPool>) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<T, Box<dyn std::error::Error + Send + Sync>>> + Send,
        T: Send + 'static,
    {
        ParallelTestExecutor::new(max_concurrent)
            .execute_db_parallel(pool, operations)
            .await
    }
}

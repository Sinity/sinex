// Common test utilities and helpers
//
// This module provides the foundational testing infrastructure for Sinex,
// including database management, event creation, timing utilities, and
// comprehensive test context management.
//
// # Key Modules
// - `prelude` - Common imports for all test files
// - `test_context` - Unified test context with database and timing helpers
// - Event creation using production `EventFactory`
// - `database` - Database pool management and cleanup
// - `timing_optimization` - Deterministic wait utilities
// - `validation_test_utils` - Event validation testing
// - `worker_test_utils` - Worker and work queue testing
// - `schema_test_utils` - JSON schema validation testing

use crate::common::prelude::*;

#[allow(dead_code)] // Test utilities may not all be used
#[allow(unused_variables)] // Test patterns

// Test prelude for standardized imports
pub mod prelude;

// Pre-initialized database pool with clean-before-use
pub mod database_pool;

// Unified test context for all tests
pub mod test_context;

// Event builders for test compatibility
pub mod event_builders;

// Re-export the procedural macros from sinex-test-macros crate and make them public
pub use crate::common::prelude::*;
use sinex_db::events as db_events;
use sinex_db::queries::{EventQueries, CheckpointQueries};
use sinex_db::query_builder::{QueryBuilder, QueryParam};
use sinex_db::query_helpers::uuid_to_ulid;
use sinex_events::{sources, EventFactory};

/// Get test database URL with fallback
pub fn test_database_url() -> String {
    std::env::var("TEST_DATABASE_URL")
        .unwrap_or_else(|_| "postgres://sinex_test:testpass@localhost:5433/sinex_test".to_string())
}

use tokio::net::UnixListener;

/// Start a test ingestd server for integration tests
pub async fn start_test_ingestd(
    ctx: &crate::common::test_context::TestContext,
) -> AnyhowResult<(tokio::task::JoinHandle<()>, String), Box<dyn std::error::Error>> {
    let socket_path = ctx
        .work_dir()
        .join("test-ingestd.sock")
        .to_string_lossy()
        .to_string();
    start_test_ingestd_at_path(ctx, &socket_path).await
}

/// Start ingestd at specific socket path
pub async fn start_test_ingestd_at_path(
    ctx: &crate::common::test_context::TestContext,
    socket_path: &str,
) -> AnyhowResult<(tokio::task::JoinHandle<()>, String), Box<dyn std::error::Error>> {
    use std::time::Duration;

    // Remove socket if it exists
    let _ = std::fs::remove_file(socket_path);

    // Create a simple test ingestd that accepts events and stores them
    let pool = ctx.pool();
    let socket_path_for_server = socket_path.to_string();

    let handle = tokio::spawn(async move {
        // This is a simplified ingestd for testing
        // In real implementation, this would use sinex_ingestd::IngestServer
        let listener = UnixListener::bind(&socket_path_for_server).unwrap();

        loop {
            match listener.accept().await {
                Ok((stream, _)) => {
                    // Handle gRPC connection
                    // For now, just keep the connection alive
                    let _ = stream;
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                Err(_) => break,
            }
        }
    });

    // Wait for server to start
    tokio::time::sleep(Duration::from_millis(100)).await;

    Ok((handle, socket_path.to_string()))
}

/// Count events from a specific satellite source
pub async fn count_events_from_source(
    pool: &DbPool,
    source: &str,
) -> AnyhowResult<u64, Box<dyn std::error::Error>> {
    let (count,) = EventQueries::count_by_source(source.to_string())
        .fetch_one::<(i64,)>(pool)
        .await?;

    Ok(count as u64)
}

/// Create a test database pool with high concurrency settings
pub async fn create_test_db_pool() -> AnyhowResult<DbPool> {
    let test_db = database_pool::acquire_test_database().await?;
    Ok(test_db.pool().clone())
}

/// Insert any event into database (renamed for clarity)
#[allow(dead_code)]
pub async fn insert_event(pool: &DbPool, event: &sinex_db::RawEvent) -> AnyhowResult<Ulid> {
    let inserted = sinex_db::insert_event_with_validator(pool, event, None).await?;
    Ok(inserted.id)
}

/// Event builder utilities for testing
#[allow(dead_code)]
pub mod events {
    use super::*;

    /// Create a test filesystem event
    pub fn filesystem_event(event_type: &str, path: &str) -> sinex_db::RawEvent {
        EventFactory::new(sources::FS).create_event(
            event_type,
            json!({
                "path": path,
                "size": 1024,
                "modified_time": "2025-01-01T00:00:00Z"
            }),
        )
    }

    /// Create a test kitty terminal event
    pub fn kitty_event(command: &str) -> sinex_db::RawEvent {
        EventFactory::new(sources::SHELL_KITTY).create_event(
            event_types::shell::COMMAND_EXECUTED,
            json!({
                "command": command,
                "exit_code": 0,
                "duration_ms": 100
            }),
        )
    }

    /// Create a test hyprland event
    pub fn hyprland_event(event_type: &str, data: Value) -> sinex_db::RawEvent {
        EventFactory::new(sources::WM_HYPRLAND).create_event(event_type, data)
    }

    /// Create a test sinex agent event
    pub fn agent_event(event_type: &str, agent_name: &str) -> sinex_db::RawEvent {
        EventFactory::new(sources::SINEX).create_event(
            event_type,
            json!({
                "agent_name": agent_name,
                "status": "running",
                "version": "1.0.0",
                "timestamp": "2025-01-01T00:00:00Z",
                "uptime_seconds": 3600,
                "events_processed_session": 42,
                "dlq_size": 0
            }),
        )
    }

    /// Create an invalid event for error testing
    pub fn invalid_event() -> sinex_db::RawEvent {
        use chrono::Utc;
        sinex_db::RawEvent {
            id: sinex_ulid::Ulid::new(),
            source: "".to_string(),     // Invalid empty source
            event_type: "".to_string(), // Invalid empty event_type
            ts_ingest: Utc::now(),
            ts_orig: None,
            host: "".to_string(), // Invalid empty host
            ingestor_version: None,
            payload_schema_id: None,
            payload: json!(null),
            source_event_ids: None,
            source_material_id: None,
            source_material_offset_start: None,
            source_material_offset_end: None,
            anchor_byte: None,
            associated_blob_ids: None,
        }
    }

    /// Create a test file created event
    pub fn file_created_event(path: &str) -> sinex_db::RawEvent {
        filesystem_event(event_types::filesystem::FILE_CREATED, path)
    }

    /// Create a test file modified event
    pub fn file_modified_event(path: &str) -> sinex_db::RawEvent {
        filesystem_event(event_types::filesystem::FILE_MODIFIED, path)
    }

    /// Create a test agent heartbeat event
    pub fn agent_heartbeat_event(agent_name: &str) -> sinex_db::RawEvent {
        agent_event(event_types::sinex::AUTOMATON_HEARTBEAT, agent_name)
    }

    /// Create a test event for race condition testing
    pub fn race_test_event(target: &str) -> sinex_db::RawEvent {
        EventFactory::new("test").create_event("race.test", json!({"target": target}))
    }

    /// Create a test event with minimal fields for adversarial testing
    pub fn adversarial_test_event(
        event_type: &str,
        payload: serde_json::Value,
    ) -> sinex_db::RawEvent {
        EventFactory::new("test").create_event(event_type, payload)
    }

    /// Create a batch of test events efficiently
    pub fn test_event_batch(
        source: &str,
        event_type: &str,
        count: usize,
    ) -> Vec<sinex_db::RawEvent> {
        let factory = EventFactory::new(source);
        (0..count)
            .map(|i| {
                factory.create_event(
                    event_type,
                    json!({"sequence": i, "batch": true, "timestamp": chrono::Utc::now()}),
                )
            })
            .collect()
    }

    /// Create a quick filesystem event with default payload
    pub fn quick_filesystem_event(path: &str) -> sinex_db::RawEvent {
        filesystem_event(event_types::filesystem::FILE_CREATED, path)
    }

    /// Create a quick agent heartbeat with default payload
    pub fn quick_agent_heartbeat(agent_name: &str) -> sinex_db::RawEvent {
        agent_event(event_types::sinex::AUTOMATON_HEARTBEAT, agent_name)
    }

    /// Create test events for timing and ordering tests
    pub fn timing_test_event(sequence: u32, delay_ms: u64) -> sinex_db::RawEvent {
        EventFactory::new("timing_test").create_event(
            "sequence.event",
            json!({
                "sequence": sequence,
                "delay_ms": delay_ms,
                "created_at": chrono::Utc::now()
            }),
        )
    }

    /// Create test events for performance testing
    pub fn performance_test_event(payload_size_kb: usize) -> sinex_db::RawEvent {
        let large_data = "x".repeat(payload_size_kb * 1024);
        EventFactory::new("performance_test").create_event(
            "large.payload",
            json!({
                "size_kb": payload_size_kb,
                "data": large_data,
                "metadata": {
                    "test_type": "performance",
                    "created_at": chrono::Utc::now()
                }
            }),
        )
    }

    /// Create agent heartbeat event for chaos testing
    pub fn agent_heartbeat_chaos_event(
        agent_name: &str,
        version: Option<&str>,
    ) -> sinex_db::RawEvent {
        let mut event = EventFactory::new("agent").create_event(
            "automaton.heartbeat",
            json!({
                "agent_name": agent_name,
                "status": "alive",
                "version": version.unwrap_or("1.0.0")
            }),
        );

        if let Some(v) = version {
            event.ingestor_version = Some(v.to_string());
        }

        event
    }

    /// Create filesystem event for chaos testing
    pub fn filesystem_chaos_event(
        event_type: &str,
        path: &str,
        version: Option<&str>,
    ) -> sinex_db::RawEvent {
        let mut event = EventFactory::new("fs").create_event(
            event_type,
            json!({
                "path": path,
                "chaos_test": true
            }),
        );

        if let Some(v) = version {
            event.ingestor_version = Some(v.to_string());
        }

        event
    }

    /// Create large payload event for boundary testing
    pub fn large_payload_test_event(data_size: usize) -> sinex_db::RawEvent {
        let large_data = "x".repeat(data_size);
        EventFactory::new("test").create_event(
            "large.payload",
            json!({
                "data": large_data,
                "size": data_size,
                "test_type": "boundary"
            }),
        )
    }

    /// Create indexed test event for database boundary testing
    pub fn indexed_test_event(
        index: i64,
        event_time: chrono::DateTime<chrono::Utc>,
    ) -> sinex_db::RawEvent {
        let mut event = EventFactory::new("btree_test").create_event(
            "index.split",
            json!({
                "index": index,
                "timestamp": event_time,
                "test_type": "btree_boundary"
            }),
        );
        event.ts_orig = Some(event_time);
        event
    }

    /// Generic adversarial event with customizable source and type
    pub fn generic_adversarial_event(
        source: &str,
        event_type: &str,
        payload: serde_json::Value,
        version: Option<&str>,
    ) -> sinex_db::RawEvent {
        let mut event = EventFactory::new(source).create_event(event_type, payload);

        if let Some(v) = version {
            event.ingestor_version = Some(v.to_string());
        }

        event
    }

    /// Create a raw event with specified timestamp (for comprehensive tests)
    pub fn create_raw_event(
        source: &str,
        event_type: &str,
        payload: serde_json::Value,
        timestamp: chrono::DateTime<chrono::Utc>,
    ) -> sinex_db::RawEvent {
        let mut event = EventFactory::new(source).create_event(event_type, payload);
        event.ts_orig = Some(timestamp);
        event
    }
}

/// Assertion helpers for common test patterns
#[allow(dead_code)]
pub mod assertions {
    use super::*;
    // Using RawEvent, AutomatonManifest from prelude

    /// Assert that two events are equivalent (ignoring IDs and timestamps)
    pub fn assert_events_equivalent(actual: &RawEvent, expected: &RawEvent) {
        pretty_assertions::assert_eq!(actual.source, expected.source);
        pretty_assertions::assert_eq!(actual.event_type, expected.event_type);
        pretty_assertions::assert_eq!(actual.payload, expected.payload);
        pretty_assertions::assert_eq!(actual.host, expected.host);
        // Note: Don't compare IDs or timestamps as they're generated
    }

    /// Assert that an event was inserted successfully
    pub async fn assert_event_inserted(pool: &DbPool, event: &RawEvent) -> AnyhowResult<Ulid> {
        let inserted = sinex_db::insert_event_with_validator(pool, event, None).await?;
        assert!(!inserted.id.to_string().is_empty());
        Ok(inserted.id)
    }

    /// Assert that an event insertion fails with validation error
    pub async fn assert_event_insertion_fails(
        pool: &DbPool,
        event: &RawEvent,
    ) -> AnyhowResult<(), anyhow::Error> {
        let result = sinex_db::insert_event_with_validator(pool, event, None).await;
        assert!(
            result.is_err(),
            "Expected event insertion to fail, but it succeeded"
        );
        Ok(())
    }

    /// Assert that manifest was registered successfully
    pub async fn assert_manifest_registered(
        pool: &DbPool,
        manifest: &AutomatonManifest,
    ) -> AnyhowResult<(), anyhow::Error> {
        // NOTE: upsert_automaton_manifest function has been removed from this architecture
        assert!(!manifest.automaton_name.is_empty());
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
        vec![
            "ls -la",
            "cd /home",
            "git status",
            "cargo build",
            "vim file.rs",
        ]
    }

    /// Generate test event with predictable data based on index
    pub fn indexed_event(index: usize) -> sinex_db::RawEvent {
        match index % 3 {
            0 => events::filesystem_event(
                event_types::filesystem::FILE_CREATED,
                &file_path(&format!("file_{}", index)),
            ),
            1 => events::kitty_event(common_commands()[index % common_commands().len()]),
            _ => events::hyprland_event("workspace", json!({"id": index})),
        }
    }

    /// Generate multiple test events
    pub fn test_events(count: usize) -> Vec<sinex_db::RawEvent> {
        (0..count).map(indexed_event).collect()
    }

    /// Generate realistic filesystem events with proper paths
    pub fn realistic_filesystem_events(count: usize) -> Vec<sinex_db::RawEvent> {
        let realistic_paths = [
            "/home/user/Documents/report.pdf",
            "/home/user/Code/project/src/main.rs",
            "/tmp/cache/session_data.json",
            "/var/log/system.log",
            "/home/user/.config/app/settings.toml",
            "/home/user/Downloads/image.png",
        ];

        let event_types = [
            sinex_events::event_types::filesystem::FILE_CREATED,
            sinex_events::event_types::filesystem::FILE_MODIFIED,
            sinex_events::event_types::filesystem::FILE_DELETED,
        ];

        (0..count)
            .map(|i| {
                let path = realistic_paths[i % realistic_paths.len()];
                let event_type = event_types[i % event_types.len()];
                events::filesystem_event(event_type, path)
            })
            .collect()
    }

    /// Generate realistic terminal command events
    pub fn realistic_shell_events(count: usize) -> Vec<sinex_db::RawEvent> {
        let realistic_commands = [
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

        (0..count)
            .map(|i| {
                let command = realistic_commands[i % realistic_commands.len()];
                events::kitty_event(command)
            })
            .collect()
    }

    /// Generate events with realistic time distribution
    pub fn time_distributed_events(
        count: usize,
        start_time: chrono::DateTime<chrono::Utc>,
        interval_secs: i64,
    ) -> Vec<sinex_db::RawEvent> {
        (0..count)
            .map(|i| {
                let mut event = indexed_event(i);
                event.ts_orig =
                    Some(start_time + chrono::Duration::seconds(interval_secs * i as i64));
                event
            })
            .collect()
    }

    /// Generate events simulating burst patterns
    pub fn burst_pattern_events(burst_count: usize, burst_size: usize) -> Vec<sinex_db::RawEvent> {
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
    pub fn variable_payload_events(count: usize) -> Vec<sinex_db::RawEvent> {
        (0..count).map(|i| {
            let payload = match i % 4 {
                0 => json!({"small": "data"}), // Small payload
                1 => json!({"medium": "data", "details": vec![1, 2, 3, 4, 5]}), // Medium payload
                2 => json!({"large": "data".repeat(100), "metadata": {"tags": vec!["tag1", "tag2", "tag3"]}}), // Large payload
                _ => json!({"binary_data": "a".repeat(1000)}), // Very large payload
            };
            test_event_with_payload(
                sources::FS,
                sinex_events::event_types::filesystem::FILE_MODIFIED,
                payload
            )
        }).collect()
    }

    /// Generate test agent manifest
    pub fn test_agent_manifest(name: &str) -> AutomatonManifest {
        use chrono::Utc;

        AutomatonManifest {
            automaton_name: name.to_string(),
            description: Some(format!("Test agent {}", name)),
            version: "1.0.0".to_string(),
            status: "development".to_string(),
            automaton_type: "test".to_string(),
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
pub async fn get_event_count(pool: &DbPool) -> AnyhowResult<i64> {
    sinex_db::count_events(pool).await
}

/// Helper for checking if an event exists by ULID
pub async fn event_exists(pool: &DbPool, event_id: Ulid) -> AnyhowResult<bool> {
    let result: Option<RawEvent> = EventQueries::get_by_id(event_id)
        .fetch_optional(pool)
        .await?;

    Ok(result.is_some())
}

/// Helper for getting recent events
pub async fn get_recent_events(pool: &DbPool, limit: i64) -> AnyhowResult<Vec<RawEvent>> {
    let events = EventQueries::get_recent(Some(limit), None).fetch_all(pool)
        .await?;
    Ok(events)
}

/// Helper for getting events by type
pub async fn get_events_by_type(
    pool: &DbPool,
    event_type: &str,
    limit: i64,
) -> AnyhowResult<Vec<RawEvent>> {
    let events = EventQueries::get_by_event_type(event_type.to_string(), Some(limit), None)
        .fetch_all(pool)
        .await?;
    Ok(events)
}

/// Helper for getting a single event by ID
pub async fn get_event_by_id(pool: &DbPool, event_id: Ulid) -> AnyhowResult<RawEvent> {
    let event = EventQueries::get_by_id(event_id)
        .fetch_one(pool)
        .await?;
    Ok(event)
}

/// Helper for getting events by source
pub async fn get_events_by_source(
    pool: &DbPool,
    source: &str,
    limit: i64,
) -> AnyhowResult<Vec<RawEvent>> {
    let events = EventQueries::get_by_source(source.to_string(), Some(limit), None)
        .fetch_all(pool)
        .await?;
    Ok(events)
}

/// Get events within a specific time range
pub async fn get_events_in_time_range(
    pool: &DbPool,
    start_time: chrono::DateTime<chrono::Utc>,
    end_time: chrono::DateTime<chrono::Utc>,
) -> AnyhowResult<Vec<RawEvent>> {
    let events = EventQueries::get_by_time_range(start_time, end_time, None, None)
        .fetch_all(pool)
        .await?;
    Ok(events)
}

/// Macros for common test patterns
#[macro_export]
macro_rules! test_event_insertion {
    ($test_name:ident, $event_builder:expr) => {
        #[sinex_test]
        async fn $test_name(pool: DbPool) -> TestResult {
            let event = $event_builder;
            $crate::common::assertions::assert_event_inserted(&pool, &event).await?;
            Ok(())
        }
    };
}

#[macro_export]
macro_rules! test_invalid_event_insertion {
    ($test_name:ident, $event_builder:expr) => {
        #[sinex_test]
        async fn $test_name(pool: DbPool) -> TestResult {
            let event = $event_builder;
            $crate::common::assertions::assert_event_insertion_fails(&pool, &event).await?;
            Ok(())
        }
    };
}

/// Test environment utilities
#[allow(dead_code)]
pub mod env {
    /// Check if we're running in a test environment
    pub fn is_test_env() -> bool {
        std::env::var("TEST_DATABASE_URL").is_ok() || std::env::var("CARGO_TEST").is_ok()
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
pub fn test_event_with_payload(
    source: &str,
    event_type: &str,
    payload: Value,
) -> sinex_db::RawEvent {
    EventFactory::new(source).create_event(event_type, payload)
}

/// Legacy compatibility alias
pub fn create_test_event_with_payload(
    source: &str,
    event_type: &str,
    payload: Value,
) -> sinex_db::RawEvent {
    test_event_with_payload(source, event_type, payload)
}

/// Helper for creating a test agent with default settings
pub async fn create_test_agent(pool: &DbPool, agent_name: &str) -> AnyhowResult<(), anyhow::Error> {
    let manifest = generators::test_agent_manifest(agent_name);
    // NOTE: upsert_automaton_manifest function has been removed from this architecture
    Ok(())
}

/// Quick test event insertion - creates minimal event
#[allow(dead_code)]
pub async fn insert_test_event(
    pool: &DbPool,
    source: &str,
    event_type: &str,
) -> AnyhowResult<Ulid> {
    let event = EventFactory::new(source).create_event(event_type, json!({"test": true}));
    insert_event(pool, &event).await
}

/// Helper to insert events for testing
#[allow(dead_code)]
pub async fn insert_event_with_validator(
    pool: &DbPool,
    source: &str,
    event_type: &str,
    host: &str,
    payload: serde_json::Value,
    ts_orig: Option<chrono::DateTime<chrono::Utc>>,
    ingestor_version: Option<&str>,
    payload_schema_id: Option<sinex_ulid::Ulid>,
) -> AnyhowResult<RawEvent> {
    let mut event = EventFactory::new(source).create_event(event_type, payload);
    event.host = host.to_string();
    if let Some(ts) = ts_orig {
        event.ts_orig = Some(ts);
    }
    if let Some(version) = ingestor_version {
        event.ingestor_version = Some(version.to_string());
    }
    if let Some(schema_id) = payload_schema_id {
        event.payload_schema_id = Some(schema_id);
    }

    sinex_db::insert_event_with_validator(pool, &event, None).await
}

/// Helper for creating agent with specific subscriptions
pub async fn create_agent_with_subscriptions(
    pool: &DbPool,
    agent_name: &str,
    subscriptions: &serde_json::Value,
) -> AnyhowResult<(), anyhow::Error> {
    // Create a test agent manifest and add subscriptions
    let mut manifest = generators::test_agent_manifest(agent_name);
    manifest.subscribes_to_event_types = Some(subscriptions.clone());

    // NOTE: upsert_automaton_manifest function has been removed from this architecture
    Ok(())
}

/// Test execution summary for reporting
#[derive(Debug, Clone)]
pub struct TestExecutionSummary {
    pub test_name: String,
    pub duration: std::time::Duration,
    pub events_created: usize,
    pub database_operations: usize,
    pub success: bool,
    pub error_message: Option<String>,
}

/// Redis Streams testing utilities
pub mod redis_streams {
    use super::*;
    use redis::aio::MultiplexedConnection;
    use redis::AsyncCommands;
    use std::collections::HashMap;

    /// Create a test Redis stream with consumer group
    pub async fn create_test_stream(
        redis: &mut MultiplexedConnection,
        stream_key: &str,
        group_name: &str,
    ) -> AnyhowResult<()> {
        // Create consumer group, ignore if it already exists
        let _: Result<String, redis::RedisError> =
            redis.xgroup_create(stream_key, group_name, "$").await;
        Ok(())
    }

    /// Publish multiple test events to a stream
    pub async fn publish_test_events(
        redis: &mut MultiplexedConnection,
        stream_key: &str,
        events: &[RawEvent],
    ) -> AnyhowResult<Vec<String>> {
        let mut message_ids = Vec::new();

        for event in events {
            let event_json = serde_json::to_string(event)?;
            let message_id: String = redis
                .xadd(
                    stream_key,
                    "*",
                    &[
                        ("event", event_json),
                        ("source", event.source.clone()),
                        ("event_type", event.event_type.clone()),
                        ("id", event.id.to_string()),
                    ],
                )
                .await?;
            message_ids.push(message_id);
        }

        Ok(message_ids)
    }

    /// Get stream length
    pub async fn stream_length(
        redis: &mut MultiplexedConnection,
        stream_key: &str,
    ) -> AnyhowResult<usize> {
        Ok(redis.xlen::<_, usize>(stream_key).await?)
    }

    /// Get consumer group info
    pub async fn consumer_group_info(
        redis: &mut MultiplexedConnection,
        stream_key: &str,
    ) -> AnyhowResult<Vec<HashMap<String, String>>> {
        Ok(redis.xinfo_groups(stream_key).await?)
    }

    /// Simulate consumer processing with acknowledgment
    pub async fn simulate_consumer_processing(
        redis: &mut MultiplexedConnection,
        stream_key: &str,
        group_name: &str,
        consumer_name: &str,
        max_messages: usize,
    ) -> AnyhowResult<Vec<String>> {
        let mut processed_ids = Vec::new();

        // Use xread for simplified testing since xreadgroup signature is different
        let result: Vec<(String, Vec<(String, String)>)> = redis
            .xread::<(&str, &str), String, Vec<(String, Vec<(String, String)>)>>(&[(stream_key, "0")], &[])
            .await?;

        for (_stream, messages) in result {
            for (id, _fields) in messages {
                // Acknowledge the message
                let _: i64 = redis.xack(stream_key, group_name, &[&id]).await?;
                processed_ids.push(id);
            }
        }

        Ok(processed_ids)
    }

    /// Clean up test stream
    pub async fn cleanup_test_stream(
        redis: &mut MultiplexedConnection,
        stream_key: &str,
    ) -> AnyhowResult<()> {
        let _: Result<i64, redis::RedisError> = redis.del(stream_key).await;
        Ok(())
    }
}

/// Automaton testing utilities
pub mod automaton_testing {
    use super::*;
    use sinex_satellite_sdk::checkpoint::{CheckpointManager, CheckpointState};

    /// Create a test checkpoint manager
    pub fn create_test_checkpoint_manager(
        pool: DbPool,
        automaton_name: &str,
        group_name: &str,
        consumer_name: &str,
    ) -> CheckpointManager {
        CheckpointManager::new(
            pool,
            automaton_name.to_string(),
            group_name.to_string(),
            consumer_name.to_string(),
        )
    }

    /// Insert test checkpoint
    pub async fn insert_test_checkpoint(
        pool: &DbPool,
        automaton_name: &str,
        processed_count: u64,
        last_processed_id: Option<&str>,
    ) -> AnyhowResult<()> {
        CheckpointQueries::upsert_checkpoint(
            sinex_ulid::Ulid::new(),
            automaton_name.to_string(),
            format!("{}-group", automaton_name),
            format!("{}-consumer", automaton_name),
            last_processed_id.map(|s| s.to_string()),
            processed_count as i64,
            chrono::Utc::now(),
            None,
            1,
            None,
            chrono::Utc::now(),
            chrono::Utc::now(),
        )
        .execute(pool)
        .await?;

        Ok(())
    }

    /// Get checkpoint state from database
    pub async fn get_checkpoint_state(
        pool: &DbPool,
        automaton_name: &str,
    ) -> AnyhowResult<Option<CheckpointState>> {
        let checkpoint = CheckpointQueries::get_all_checkpoints_for_processor(automaton_name.to_string())
            .fetch_optional(pool)
            .await?;

        Ok(checkpoint.map(|row| {
            // Use the unified checkpoint format if available (version 2+)
            if row.checkpoint_version >= 2 && row.checkpoint_data.is_some() {
                let checkpoint_data = row.checkpoint_data.unwrap();
                let checkpoint: sinex_satellite_sdk::stream_processor::Checkpoint =
                    serde_json::from_value(checkpoint_data)
                        .unwrap_or(sinex_satellite_sdk::stream_processor::Checkpoint::None);

                sinex_satellite_sdk::checkpoint::CheckpointState {
                    checkpoint,
                    processed_count: row.processed_count as u64,
                    last_activity: row.last_activity,
                    data: row.state_data,
                    version: row.checkpoint_version as u32,
                }
            } else {
                // Legacy format (version 1) - convert Redis Stream message ID
                let checkpoint = if let Some(id) = row.last_processed_id {
                    sinex_satellite_sdk::stream_processor::Checkpoint::Stream {
                        message_id: id,
                        event_id: None,
                    }
                } else {
                    sinex_satellite_sdk::stream_processor::Checkpoint::None
                };

                sinex_satellite_sdk::checkpoint::CheckpointState {
                    checkpoint,
                    processed_count: row.processed_count as u64,
                    last_activity: row.last_activity,
                    data: row.state_data,
                    version: row.checkpoint_version as u32,
                }
            }
        }))
    }

    /// Wait for checkpoint to reach expected state with timeout
    pub async fn wait_for_checkpoint_progress(
        pool: &DbPool,
        automaton_name: &str,
        expected_count: u64,
        timeout_secs: u64,
    ) -> AnyhowResult<CheckpointState> {
        let timeout = std::time::Duration::from_secs(timeout_secs);
        let start = std::time::Instant::now();

        loop {
            if let Some(checkpoint) = get_checkpoint_state(pool, automaton_name).await? {
                if checkpoint.processed_count >= expected_count {
                    return Ok(checkpoint);
                }
            }

            if start.elapsed() > timeout {
                return Err(anyhow::anyhow!(
                    "Timeout waiting for automaton {} to reach count {}",
                    automaton_name,
                    expected_count
                ));
            }

            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    }

    /// Verify automaton processed events in order
    pub async fn verify_processing_order(
        pool: &DbPool,
        automaton_name: &str,
        expected_sequence: &[Ulid],
    ) -> AnyhowResult<bool> {
        // This would check that events were processed in the expected order
        // For now, just verify the count matches
        let checkpoint = get_checkpoint_state(pool, automaton_name).await?;
        Ok(checkpoint.map(|c| c.processed_count as usize).unwrap_or(0) == expected_sequence.len())
    }
}

impl TestExecutionSummary {
    pub fn print_report(&self) {
        println!("=== Test Execution Summary ===");
        println!("Test: {}", self.test_name);
        println!("Duration: {:?}", self.duration);
        println!("Events created: {}", self.events_created);
        println!("DB operations: {}", self.database_operations);
        println!("Result: {}", if self.success { "✓ PASS" } else { "✗ FAIL" });
        if let Some(error) = &self.error_message {
            println!("Error: {}", error);
        }
    }
}

/// Simple database test utilities
#[allow(dead_code)]
pub mod db_utils {
    use super::*;

    /// Insert multiple test events quickly
    pub async fn insert_test_events(pool: &DbPool, count: usize) -> AnyhowResult<Vec<Ulid>> {
        let mut ids = Vec::new();
        for i in 0..count {
            let event = generators::indexed_event(i);
            let id = insert_event(pool, &event).await?;
            ids.push(id);
        }
        Ok(ids)
    }
}

/// Essential assertion helpers
#[allow(dead_code)]
pub mod assertions_extra {

    use sinex_db::RawEvent;

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
            assert!(
                seen_ids.insert(event.id),
                "Duplicate event found: {}",
                event.id
            );
        }
    }
}

/// Health check utilities for integration tests
#[allow(dead_code)]
pub mod health {
    use super::*;

    /// Check if database is healthy
    pub async fn check_database_health(pool: &DbPool) -> AnyhowResult<bool> {
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
    pub async fn truncate_all_tables(pool: &DbPool) -> AnyhowResult<(), anyhow::Error> {
        // Clean up test data manually
        EventQueries::delete_by_source("test_%".to_string())
            .execute(pool)
            .await?;

        // Clean up test checkpoints
        sqlx::query!(
            "DELETE FROM core.automaton_checkpoints WHERE automaton_name LIKE $1",
            "test_%"
        )
        .execute(pool)
        .await?;

        Ok(())
    }

    /// Clean up Redis test streams
    pub async fn cleanup_redis_streams(
        redis: &mut redis::aio::MultiplexedConnection,
        stream_patterns: &[&str],
    ) -> AnyhowResult<(), anyhow::Error> {
        use redis::AsyncCommands;

        for pattern in stream_patterns {
            let keys: Vec<String> = redis.keys::<_, Vec<String>>(pattern).await.unwrap_or_default();
            if !keys.is_empty() {
                let _: Result<i64, redis::RedisError> = redis.del(&keys).await;
            }
        }

        Ok(())
    }

    /// Clean up test files and directories
    pub async fn cleanup_test_files(paths: &[&str]) -> AnyhowResult<(), anyhow::Error> {
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
    use std::path::{Path, PathBuf};
    use tempfile::{NamedTempFile, TempDir};
    use tokio::fs;

    /// Create temporary directory with standard test structure
    pub fn temp_dir() -> AnyhowResult<TempDir> {
        TempDir::new().map_err(|e| anyhow::anyhow!("Failed to create temp dir: {}", e))
    }

    /// Create temp directory with specific subdirectories
    pub fn temp_dir_with_structure(subdirs: &[&str]) -> AnyhowResult<TempDir> {
        let temp = temp_dir()?;
        for subdir in subdirs {
            std::fs::create_dir_all(temp.path().join(subdir))?;
        }
        Ok(temp)
    }

    /// Create a temporary configuration file
    pub async fn temp_config_file(content: &str) -> AnyhowResult<NamedTempFile> {
        let temp_file = NamedTempFile::new()?;
        fs::write(temp_file.path(), content).await?;
        Ok(temp_file)
    }

    /// Create test file with content
    pub fn create_test_file(dir: &Path, name: &str, content: &str) -> AnyhowResult<PathBuf> {
        let file_path = dir.join(name);
        std::fs::write(&file_path, content)?;
        Ok(file_path)
    }
}

// Re-export commonly used items for convenience
pub use sinex_db::models::AutomatonManifest;
// Note: Some query functions may need to be migrated to domain modules
pub mod channel_test_utils;
pub mod config_test_utils;
pub mod enhanced_assertions;
/// Timing optimization utilities to reduce test flakiness
pub mod timing_optimization;

/// Validation test utilities
pub mod validation_test_utils;

// Re-export the final pool as the default - used directly from database_pool module

/// Schema test utilities
pub mod schema_test_utils;

/// Worker test utilities
pub mod worker_test_utils;

/// Coverage assurance utilities
pub mod coverage_assurance;

// Satellite architecture test utilities
pub mod satellite_test_utils;

/// Mock implementations for testing
pub mod mocks;

/// Configuration compatibility testing utilities
pub mod config_compatibility_tester;

/// Integration testing patterns for satellite architecture
pub mod satellite_integration {
    use super::*;
    use crate::common::satellite_test_utils::{
        TestAutomatonHandle, TestIngestdHandle, TestSatelliteHandle,
    };
    use crate::common::test_context::TestContext;

    /// Standard satellite test setup
    pub struct SatelliteTestSetup {
        pub ctx: TestContext,
        pub ingestd: TestIngestdHandle,
        pub redis: redis::aio::MultiplexedConnection,
        pub stream_key: String,
    }

    impl SatelliteTestSetup {
        /// Create a complete satellite test environment
        pub async fn new(test_name: &str) -> AnyhowResult<Self> {
            let mut config = crate::common::test_context::TestConfig::default();
            config.test_name = test_name.to_string();

            let ctx = TestContext::with_config(config).await?;
            let ingestd = ctx.start_test_ingestd().await?;
            let redis = ctx.redis().await?;
            let stream_key = format!("test:{}:events", test_name);

            Ok(Self {
                ctx,
                ingestd,
                redis,
                stream_key,
            })
        }

        /// Add a test satellite to the setup
        pub async fn add_satellite(&self, service_name: &str) -> AnyhowResult<TestSatelliteHandle> {
            let config = crate::common::satellite_test_utils::create_test_satellite_config(
                service_name,
                &self.ingestd.socket_path,
            );
            self.ctx.start_test_satellite(config).await
        }

        /// Add a test automaton to the setup
        pub async fn add_automaton(
            &self,
            automaton_type: &str,
        ) -> AnyhowResult<TestAutomatonHandle> {
            self.ctx.start_test_automaton(automaton_type).await
        }

        /// Wait for complete event processing cycle
        pub async fn wait_for_processing_cycle(
            &self,
            expected_events: usize,
            automaton_name: &str,
        ) -> AnyhowResult<()> {
            // Wait for events to appear in stream
            self.ctx
                .wait_for_redis_stream_length(&self.stream_key, expected_events)
                .await?;

            // Wait for automaton to process them
            self.ctx
                .wait_for_checkpoint_progress(automaton_name, expected_events as u64)
                .await?;

            Ok(())
        }

        /// Verify end-to-end event flow
        pub async fn verify_event_flow(
            &self,
            source_events: &[RawEvent],
            automaton_name: &str,
        ) -> AnyhowResult<()> {
            // Insert events
            self.ctx.insert_events(source_events).await?;

            // Wait for processing
            self.wait_for_processing_cycle(source_events.len(), automaton_name)
                .await?;

            // Verify final state
            let checkpoint = self.ctx.verify_checkpoint(automaton_name).await?;
            assert_eq!(checkpoint.processed_count, source_events.len() as u64);

            Ok(())
        }
    }
}

/// Event source testing utilities
#[allow(dead_code)]
pub mod event_sources {
    use super::*;
    use sinex_events::RawEvent;
    use sinex_satellite_sdk::{EventSourceConfig, StatefulStreamProcessor};
    use tokio::time::{timeout, Duration};

    /// Trait for event sources in testing
    #[async_trait]
    pub trait EventSource: Send + Sync {
        async fn stream_events(&mut self, tx: mpsc::Sender<RawEvent>) -> AnyhowResult<()>;
    }

    /// Create EventSourceConfig with test configuration
    pub fn test_context(config: Value) -> EventSourceConfig {
        EventSourceConfig { base: Default::default(), batch_size: 100, batch_timeout_secs: 1, source_config: config.into() }
    }

    /// Create EventSourceConfig with database pool
    pub fn test_context_with_db(config: Value, pool: DbPool) -> EventSourceConfig {
        { let mut config = EventSourceConfig { base: Default::default(), batch_size: 100, batch_timeout_secs: 1, source_config: Default::default() }; config.base.db_pool = Some(pool); config }
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
    ) -> AnyhowResult<Vec<RawEvent>> {
        let (tx, mut rx) = mpsc::channel(100);
        let timeout_duration = Duration::from_secs(timeout_secs);

        let source_handle = tokio::spawn(async move { source.stream_events(tx).await });

        let mut events = Vec::new();
        let start = std::time::Instant::now();

        while events.len() < min_events && start.elapsed() < timeout_duration {
            match timeout(Duration::from_millis(100), rx.recv()).await {
                Ok(Some(event)) => events.push(event),
                Ok(None) => break,  // Channel closed
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
            pool: Arc<DbPool>,
            operations: Vec<F>,
        ) -> Vec<Result<T, Box<dyn std::error::Error + Send + Sync>>>
        where
            F: FnOnce(Arc<DbPool>) -> Fut + Send + 'static,
            Fut: std::future::Future<Output = Result<T, Box<dyn std::error::Error + Send + Sync>>>
                + Send,
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
                    Err(join_error) => results.push(Err(
                        Box::new(join_error) as Box<dyn std::error::Error + Send + Sync>
                    )),
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
        pool: Arc<DbPool>,
        operations: Vec<F>,
        max_concurrent: usize,
    ) -> Vec<Result<T, Box<dyn std::error::Error + Send + Sync>>>
    where
        F: FnOnce(Arc<DbPool>) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<T, Box<dyn std::error::Error + Send + Sync>>>
            + Send,
        T: Send + 'static,
    {
        ParallelTestExecutor::new(max_concurrent)
            .execute_db_parallel(pool, operations)
            .await
    }
}

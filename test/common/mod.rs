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

/// Enhanced test data generation with realistic patterns
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
            let mut event = test_event(i);
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
                let mut event = test_event(burst * burst_size + i);
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
            
            create_test_event_with_payload(
                sources::FILESYSTEM,
                event_type_constants::filesystem::FILE_MODIFIED,
                payload
            )
        }).collect()
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

/// EventSource testing harness for consistent testing patterns
#[allow(dead_code)]
pub mod event_source_harness {
    use super::*;
    use sinex_core::{EventSource, EventSourceContext};
    use tokio::sync::mpsc;
    use tokio::time::{timeout, Duration};
    use std::collections::VecDeque;

    /// Test harness for any EventSource implementation
    pub struct EventSourceTestHarness<T: EventSource> {
        source: Option<T>,
        events_received: VecDeque<sinex_core::RawEvent>,
        config: T::Config,
        context: EventSourceContext,
    }

    impl<T: EventSource> EventSourceTestHarness<T> {
        /// Create a new test harness with given config
        pub async fn new(config: T::Config) -> Result<Self> {
            let context = EventSourceContext {
                config: serde_json::to_value(&config)?,
                shutdown: tokio_util::sync::CancellationToken::new(),
            };

            Ok(Self {
                source: None,
                events_received: VecDeque::new(),
                config,
                context,
            })
        }

        /// Initialize the EventSource
        pub async fn initialize(&mut self) -> Result<()> {
            self.source = Some(T::initialize(self.context.clone()).await?);
            Ok(())
        }

        /// Collect events for a specified duration
        pub async fn collect_events(&mut self, duration: Duration) -> Result<Vec<sinex_core::RawEvent>> {
            let source = self.source.as_mut().ok_or_else(|| anyhow::anyhow!("EventSource not initialized"))?;
            
            let (tx, mut rx) = mpsc::channel(1000);
            
            let stream_task = {
                let mut source_clone = std::mem::replace(source, unsafe { std::mem::zeroed() });
                tokio::spawn(async move {
                    source_clone.stream_events(tx).await
                })
            };

            let collect_task = tokio::spawn(async move {
                let mut events = Vec::new();
                while let Some(event) = rx.recv().await {
                    events.push(event);
                }
                events
            });

            tokio::time::sleep(duration).await;
            self.context.shutdown.cancel();
            
            let _ = stream_task.await;
            timeout(Duration::from_secs(1), collect_task).await??
        }

        /// Wait for a specific number of events with timeout
        pub async fn wait_for_events(&mut self, count: usize, timeout_duration: Duration) -> Result<Vec<sinex_core::RawEvent>> {
            let source = self.source.as_mut().ok_or_else(|| anyhow::anyhow!("EventSource not initialized"))?;
            
            let (tx, mut rx) = mpsc::channel(1000);
            
            let stream_task = {
                tokio::spawn(async move {
                    if let Some(mut src) = self.source.take() {
                        let _ = src.stream_events(tx).await;
                    }
                })
            };

            let result = timeout(timeout_duration, async {
                let mut events = Vec::new();
                while events.len() < count {
                    if let Some(event) = rx.recv().await {
                        events.push(event);
                    } else {
                        break;
                    }
                }
                events
            }).await?;

            self.context.shutdown.cancel();
            let _ = stream_task.await;
            
            Ok(result)
        }

        /// Shutdown the EventSource
        pub async fn shutdown(&mut self) {
            self.context.shutdown.cancel();
        }
    }
}

/// Database state builder for complex test scenarios
#[allow(dead_code)]
pub mod database_builder {
    use super::*;
    use sinex_db::models::{RawEvent, AgentManifest};
    use chrono::{DateTime, Utc};

    /// Builder for setting up complex database states in tests
    pub struct DatabaseStateBuilder {
        pool: PgPool,
        events: Vec<RawEvent>,
        manifests: Vec<AgentManifest>,
        schemas: Vec<(String, serde_json::Value)>,
    }

    impl DatabaseStateBuilder {
        /// Create a new database state builder
        pub fn new(pool: PgPool) -> Self {
            Self {
                pool,
                events: Vec::new(),
                manifests: Vec::new(),
                schemas: Vec::new(),
            }
        }

        /// Add events to be inserted
        pub fn with_events(mut self, events: Vec<RawEvent>) -> Self {
            self.events.extend(events);
            self
        }

        /// Add a single event
        pub fn with_event(mut self, event: RawEvent) -> Self {
            self.events.push(event);
            self
        }

        /// Add events from a generator function
        pub fn with_generated_events<F>(mut self, count: usize, generator: F) -> Self 
        where
            F: Fn(usize) -> RawEvent,
        {
            for i in 0..count {
                self.events.push(generator(i));
            }
            self
        }

        /// Add events with time distribution
        pub fn with_time_distributed_events(
            mut self, 
            count: usize, 
            start_time: DateTime<Utc>, 
            interval: chrono::Duration
        ) -> Self {
            for i in 0..count {
                let mut event = generators::test_event(i);
                event.ts_orig = Some(start_time + interval * i as i32);
                self.events.push(event);
            }
            self
        }

        /// Add agent manifests
        pub fn with_manifests(mut self, manifests: Vec<AgentManifest>) -> Self {
            self.manifests.extend(manifests);
            self
        }

        /// Add JSON schemas
        pub fn with_schema(mut self, name: String, schema: serde_json::Value) -> Self {
            self.schemas.push((name, schema));
            self
        }

        /// Build the database state
        pub async fn build(self) -> Result<DatabaseState> {
            // Insert all events
            let mut event_ids = Vec::new();
            for event in &self.events {
                let inserted = queries::insert_event(&self.pool, event).await?;
                event_ids.push(inserted.id);
            }

            // Insert all manifests
            for manifest in &self.manifests {
                queries::upsert_agent_manifest(
                    &self.pool,
                    &manifest.agent_name,
                    manifest.description.as_deref().unwrap_or(""),
                    &manifest.version,
                    &manifest.status,
                    Some(&manifest.agent_type),
                    manifest.config_template_json.clone(),
                    manifest.produces_event_types.clone(),
                ).await?;
            }

            // Insert schemas if any
            for (name, schema) in &self.schemas {
                // Insert schema logic here when available
                // For now, just validate the schema
                let _ = serde_json::to_string(schema)?;
            }

            Ok(DatabaseState {
                pool: self.pool,
                event_ids,
                manifest_names: self.manifests.into_iter().map(|m| m.agent_name).collect(),
                schema_names: self.schemas.into_iter().map(|(name, _)| name).collect(),
            })
        }
    }

    /// Represents a built database state for testing
    pub struct DatabaseState {
        pub pool: PgPool,
        pub event_ids: Vec<Ulid>,
        pub manifest_names: Vec<String>,
        pub schema_names: Vec<String>,
    }

    impl DatabaseState {
        /// Verify all events were inserted correctly
        pub async fn verify_events(&self) -> Result<()> {
            for event_id in &self.event_ids {
                let exists = super::event_exists(&self.pool, *event_id).await?;
                assert!(exists, "Event {} was not found in database", event_id);
            }
            Ok(())
        }

        /// Get event count in database
        pub async fn event_count(&self) -> Result<i64> {
            super::get_event_count(&self.pool).await
        }

        /// Clean up all inserted data
        pub async fn cleanup(&self) -> Result<()> {
            super::cleanup::truncate_all_tables(&self.pool).await
        }
    }
}

/// Enhanced assertion helpers
#[allow(dead_code)]
pub mod enhanced_assertions {
    use super::*;
    use sinex_db::models::RawEvent;
    use chrono::{DateTime, Utc};

    /// Assert events are in chronological order
    pub fn assert_events_in_order(events: &[RawEvent]) {
        for window in events.windows(2) {
            let (prev, curr) = (&window[0], &window[1]);
            assert!(
                prev.ts_ingest <= curr.ts_ingest,
                "Events not in chronological order: {} > {}",
                prev.ts_ingest,
                curr.ts_ingest
            );
        }
    }

    /// Assert events are in ULID order (which implies time order)
    pub fn assert_events_in_ulid_order(events: &[RawEvent]) {
        for window in events.windows(2) {
            let (prev, curr) = (&window[0], &window[1]);
            assert!(
                prev.id.timestamp() <= curr.id.timestamp(),
                "Events not in ULID time order: {} > {}",
                prev.id,
                curr.id
            );
        }
    }

    /// Assert that worker processed expected number of events
    pub async fn assert_worker_processed(
        pool: &PgPool,
        worker_name: &str,
        expected_count: i64,
        timeout_secs: u64,
    ) -> Result<()> {
        let start = std::time::Instant::now();
        let timeout_duration = std::time::Duration::from_secs(timeout_secs);

        loop {
            // Check if worker has processed expected events
            let processed_count = sqlx::query_scalar!(
                "SELECT COUNT(*) FROM raw.events WHERE payload->>'processed_by' = $1",
                worker_name
            )
            .fetch_one(pool)
            .await?
            .unwrap_or(0);

            if processed_count >= expected_count {
                return Ok(());
            }

            if start.elapsed() > timeout_duration {
                anyhow::bail!(
                    "Worker {} processed {} events, expected {}, after {} seconds",
                    worker_name,
                    processed_count,
                    expected_count,
                    timeout_secs
                );
            }

            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }
    }

    /// Assert events match expected pattern
    pub fn assert_events_match_pattern<F>(events: &[RawEvent], pattern: F) 
    where
        F: Fn(&RawEvent) -> bool,
    {
        for (i, event) in events.iter().enumerate() {
            assert!(
                pattern(event),
                "Event at index {} does not match expected pattern: {:?}",
                i,
                event
            );
        }
    }

    /// Assert events are from expected sources
    pub fn assert_events_from_sources(events: &[RawEvent], expected_sources: &[&str]) {
        let unique_sources: std::collections::HashSet<_> = events.iter().map(|e| e.source.as_str()).collect();
        let expected_set: std::collections::HashSet<_> = expected_sources.iter().copied().collect();
        
        assert_eq!(
            unique_sources, expected_set,
            "Events from unexpected sources. Expected: {:?}, Found: {:?}",
            expected_sources,
            unique_sources
        );
    }

    /// Assert no duplicate events (by ULID)
    pub fn assert_no_duplicate_events(events: &[RawEvent]) {
        let mut seen_ids = std::collections::HashSet::new();
        for (i, event) in events.iter().enumerate() {
            assert!(
                seen_ids.insert(event.id),
                "Duplicate event ULID found at index {}: {}",
                i,
                event.id
            );
        }
    }

    /// Assert events contain expected payload fields
    pub fn assert_events_have_fields(events: &[RawEvent], required_fields: &[&str]) {
        for (i, event) in events.iter().enumerate() {
            if let serde_json::Value::Object(payload) = &event.payload {
                for field in required_fields {
                    assert!(
                        payload.contains_key(*field),
                        "Event at index {} missing required field '{}': {:?}",
                        i,
                        field,
                        event.payload
                    );
                }
            } else {
                panic!("Event at index {} has non-object payload: {:?}", i, event.payload);
            }
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
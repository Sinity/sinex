//! Test Context - Unified Interface for All Testing Operations
//!
//! The `TestContext` is the central abstraction for Sinex testing, providing isolated database
//! access, fluent builders, rich assertions, and comprehensive test utilities through a single
//! unified interface.
//!
//! # Architecture
//!
//! TestContext manages:
//! - **Database Isolation**: Each test gets its own database from the pool
//! - **Event Lifecycle**: Creation, validation, and querying of events
//! - **Test Coordination**: Timing, synchronization, and fixtures
//! - **Assertions**: Rich error messages with context
//! - **Mocking**: Access to comprehensive mock infrastructure
//!
//! # Core Components
//!
//! ## Event Creation
//! Events are created through domain-specific builders:
//!
//! ```rust
//! // Filesystem events
//! let event = ctx.event()
//!     .filesystem()
//!     .path("/data/report.pdf")
//!     .size(2048576)  // 2MB
//!     .permissions(0o644)
//!     .modified()
//!     .insert()
//!     .await?;
//!
//! // Terminal commands
//! let cmd = ctx.event()
//!     .terminal()
//!     .command("cargo test")
//!     .working_dir("/project")
//!     .duration_ms(1500)
//!     .success()
//!     .insert()
//!     .await?;
//!
//! // Custom events with incremental field building
//! let custom = ctx.event()
//!     .source("analytics")
//!     .type_("user.behavior")
//!     .field("action", "page_view")
//!     .field("duration_ms", 450)
//!     .fields(vec![
//!         ("browser", json!("Firefox")),
//!         ("viewport", json!({"width": 1920, "height": 1080}))
//!     ])
//!     .insert()
//!     .await?;
//! ```
//!
//! ## Event Querying
//! Type-safe query builders with chainable methods:
//!
//! ```rust
//! // Basic queries
//! let all_events = ctx.events().fetch().await?;
//! let recent = ctx.events().limit(20).fetch().await?;
//! let fs_events = ctx.events().by_source("fs").fetch().await?;
//!
//! // Complex queries
//! let terminal_errors = ctx.events()
//!     .by_source("shell-kitty")
//!     .by_type("shell.command.failed")
//!     .limit(10)
//!     .fetch()
//!     .await?;
//!
//! // Aggregations
//! let total_count = ctx.events().count().await?;
//! let fs_count = ctx.events().by_source("fs").count().await?;
//!
//! // Single event lookup
//! let event = ctx.events().by_id(event_id).fetch_one().await?;
//! ```
//!
//! ## Rich Assertions
//! Contextual assertions with detailed error messages:
//!
//! ```rust
//! // Basic assertions
//! ctx.assert("user data validation")
//!     .eq(&user.name, "Alice")?
//!     .that(user.age >= 18, "user must be adult")?
//!     .not_empty(&user.permissions)?;
//!
//! // Event-specific assertions
//! ctx.assert("event processing")
//!     .event_eq(&actual, &expected)?
//!     .completes_within(
//!         async { process_event(&event).await },
//!         Duration::from_secs(5),
//!         "event processing"
//!     ).await?;
//!
//! // Error assertions
//! ctx.assert("validation failure")
//!     .error_contains(&result, "invalid format")?;
//! ```
//!
//! ## Schema Validation
//! JSON Schema integration for event validation:
//!
//! ```rust
//! // Register schema
//! let schema_id = ctx.schema().register("fs", "file.created",
//!     json!({
//!         "type": "object",
//!         "properties": {
//!             "path": {"type": "string", "minLength": 1},
//!             "size": {"type": "integer", "minimum": 0},
//!             "hash": {"type": "string", "pattern": "^[a-f0-9]{64}$"}
//!         },
//!         "required": ["path", "size"]
//!     })
//! ).await?;
//!
//! // Validate existing events
//! ctx.schema().validate(&event, schema_id).await?;
//!
//! // Create pre-validated events
//! let event = ctx.validated_event(schema_id)
//!     .field("path", "/data/file.txt")
//!     .field("size", 1024)
//!     .field("hash", "a".repeat(64))
//!     .insert()
//!     .await?;
//! ```
//!
//! ## Timing and Synchronization
//! Utilities for coordinating async operations:
//!
//! ```rust
//! // Wait for conditions
//! ctx.wait_for_event_count(10).await?;
//! ctx.timing().wait_for_events_from("fs", 5).await?;
//!
//! // Synchronization primitives
//! let barrier = ctx.timing().barrier(3);
//! let sync = ctx.timing().synchronizer(Duration::from_secs(10));
//!
//! // Measure operations
//! let (result, duration) = ctx.measure(async {
//!     expensive_operation().await
//! }).await?;
//! ```
//!
//! ## Fixtures and Scenarios
//! Pre-built test data with lifecycle management:
//!
//! ```rust
//! // Standard scenarios
//! let session = ctx.scenarios().user_session().await?;
//! let checkpoints = ctx.scenarios().populated_checkpoints().await?;
//!
//! // Performance testing
//! let large_dataset = ctx.performance()
//!     .large_dataset_with(100_000)
//!     .await?;
//!
//! // Error scenarios
//! let errors = ctx.errors().validation_failures().await?;
//! ```
//!
//! # Design Principles
//!
//! 1. **Single Entry Point**: Everything through `ctx` parameter
//! 2. **Fluent Interfaces**: Chainable methods for intuitive API
//! 3. **Type Safety**: Compile-time guarantees where possible
//! 4. **Rich Context**: Detailed error messages for debugging
//! 5. **Performance**: Optimized for parallel test execution

use crate::database_pool::TestDatabase;
use crate::redis_pool::{acquire_test_redis, TestRedis};
use crate::Result;
use chrono::{DateTime, Utc};
use serde_json::{json, Value};
use sinex_core_types::RawEvent;
use sinex_db::queries::{CheckpointQueries, EventQueries};
use sinex_db::query_builder::QueryBuilder;
use sinex_error::SinexError;
use sinex_events::{event_types, sources, EventFactory};
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

// Default timeout for test operations
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(3);

/// Unified test context - single entry point for all test operations
#[derive(Debug)]
pub struct TestContext {
    db: TestDatabase,
    test_name: String,
    start_time: Instant,
    created_events: Arc<Mutex<Vec<sinex_ulid::Ulid>>>,
    redis_cleanup_keys: Arc<std::sync::Mutex<Vec<String>>>,
}

impl TestContext {
    /// Create new test context
    pub async fn new() -> Result<Self> {
        Self::with_name("unnamed_test").await
    }

    /// Create test context with custom name
    pub async fn with_name(test_name: &str) -> Result<Self> {
        let db = crate::database_pool::acquire_test_database().await?;

        Ok(Self {
            db,
            test_name: test_name.to_string(),
            start_time: Instant::now(),
            created_events: Arc::new(Mutex::new(Vec::new())),
            redis_cleanup_keys: Arc::new(std::sync::Mutex::new(Vec::new())),
        })
    }

    /// Get test name for fixture scoping
    pub fn test_name(&self) -> &str {
        &self.test_name
    }

    /// Get database pool (for fixture creation)
    pub fn pool(&self) -> &sinex_core_types::DbPool {
        self.db.pool()
    }

    /// Get a Redis instance for this test
    pub async fn redis(&self) -> Result<TestRedis> {
        acquire_test_redis().await.map_err(Into::into)
    }

    /// Track a Redis key for cleanup when the test context is dropped
    pub fn track_redis_key(&self, key: String) {
        if let Ok(mut keys) = self.redis_cleanup_keys.lock() {
            keys.push(key);
        }
    }

    /// Get elapsed time since test start
    pub fn elapsed(&self) -> Duration {
        self.start_time.elapsed()
    }

    // ===== SINGLE EVENT CREATION API =====

    /// Create an event builder - single entry point for all event creation
    pub fn event(&self) -> EventBuilder<'_> {
        EventBuilder::new(self)
    }

    /// Create checkpoint builder for tests
    pub fn checkpoint(&self) -> CheckpointBuilder<'_> {
        CheckpointBuilder::new(self)
    }

    /// Insert event directly (internal use)
    pub(crate) async fn insert_event_internal(&self, event: &RawEvent) -> Result<RawEvent> {
        let inserted = sinex_db::insert_event_with_validator(self.pool(), event, None)
            .await
            .map_err(|e| {
                SinexError::database("Failed to insert event")
                    .with_source(e)
                    .with_context("event_type", &event.event_type)
                    .with_context("source", &event.source)
            })?;
        self.created_events.lock().await.push(inserted.id);
        Ok(inserted)
    }

    // ===== QUERY ABSTRACTION API =====

    /// Query events using abstracted interface
    pub fn events(&self) -> EventQuery<'_> {
        EventQuery::new(self)
    }

    /// Query checkpoints using abstracted interface  
    pub fn checkpoints(&self) -> CheckpointQuery<'_> {
        CheckpointQuery::new(self)
    }

    // ===== TIMING HELPERS =====

    /// Wait for specific number of events using production wait helpers
    pub async fn wait_for_event_count(&self, expected: usize) -> Result<()> {
        let timeout_secs = DEFAULT_TIMEOUT.as_secs();

        sinex_core_utils::wait_for_condition_adaptive(
            || async {
                let count =
                    self.events().count().await.map_err(|e| {
                        SinexError::database("Failed to count events").with_source(e)
                    })? as usize;
                Ok(count >= expected)
            },
            timeout_secs,
            &format!("event count >= {}", expected),
        )
        .await
        .map_err(|e| SinexError::timeout(format!("Wait condition failed: {}", e)))
    }

    /// Wait for condition to become true using production wait helpers
    pub async fn wait_for_condition<F, Fut>(&self, condition: F) -> Result<()>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<bool>>,
    {
        let timeout_secs = DEFAULT_TIMEOUT.as_secs();

        sinex_core_utils::wait_for_condition_adaptive(
            || async {
                match condition().await {
                    Ok(result) => Ok(result),
                    Err(e) => Err(SinexError::unknown(e.to_string())),
                }
            },
            timeout_secs,
            "custom test condition",
        )
        .await
        .map_err(|e| SinexError::timeout(format!("Wait condition failed: {}", e)))
    }

    // ===== ASSERTION HELPERS =====

    /// Assert specific event count using production error context
    pub async fn assert_event_count(&self, expected: usize) -> Result<()> {
        let actual = self.events().count().await? as usize;
        if actual != expected {
            return Err(SinexError::validation(format!(
                "Event count assertion failed: expected {}, got {} (test: {})",
                expected, actual, self.test_name
            )));
        }
        Ok(())
    }

    /// Assert no events exist
    pub async fn assert_no_events(&self) -> Result<()> {
        self.assert_event_count(0).await
    }

    /// Assert event with ID exists
    pub async fn assert_event_exists(&self, id: sinex_ulid::Ulid) -> Result<()> {
        let event = self.events().by_id(id).fetch_one().await?;
        if event.is_none() {
            return Err(SinexError::not_found(format!("Event not found: {}", id)));
        }
        Ok(())
    }

    // ===== UTILITY METHODS =====

    /// Create batch of test events
    pub fn create_event_batch(&self, source: &str, count: usize) -> Vec<EventBuilder<'_>> {
        (0..count)
            .map(|i| {
                self.event()
                    .source(source)
                    .type_("test.batch")
                    .payload(json!({"index": i, "batch": true}))
            })
            .collect()
    }

    /// Get events created in this test
    pub async fn test_event_count(&self) -> usize {
        self.created_events.lock().await.len()
    }

    /// Insert a pre-built event
    pub async fn insert_event(&self, event: &RawEvent) -> Result<RawEvent> {
        self.insert_event_internal(event).await
    }

    /// Insert multiple pre-built events
    pub async fn insert_events(&self, events: &[RawEvent]) -> Result<Vec<RawEvent>> {
        let mut inserted = Vec::with_capacity(events.len());
        for event in events {
            inserted.push(self.insert_event_internal(event).await?);
        }
        Ok(inserted)
    }

    // ===== FIXTURE BUILDERS =====

    /// Access all fixtures through unified interface
    pub fn fixtures(&self) -> FixtureManager<'_> {
        FixtureManager { ctx: self }
    }

    // ===== INTEGRATION TEST UTILITIES =====

    /// Access channel behavior testing utilities
    pub fn channels(&self) -> ChannelTestUtils<'_> {
        ChannelTestUtils { ctx: self }
    }

    /// Access process management utilities (satellites, ingestd, automata)
    pub fn processes(&self) -> ProcessTestUtils<'_> {
        ProcessTestUtils { ctx: self }
    }

    /// Access deployment scenario testing utilities
    pub fn deployment(&self) -> DeploymentTestUtils<'_> {
        DeploymentTestUtils { ctx: self }
    }

    // ===== SCHEMA TESTING API =====

    /// Schema testing utilities
    pub fn schema(&self) -> SchemaTestUtils<'_> {
        SchemaTestUtils::new(self)
    }

    /// Create schema-validated event builder
    pub fn validated_event(&self, schema_id: sinex_ulid::Ulid) -> ValidatedEventBuilder<'_> {
        ValidatedEventBuilder::new(self, schema_id)
    }

    // ===== CONTEXTUAL ASSERTION API =====

    /// Create contextual assertions with rich error messages
    pub fn assert(&self, context: &str) -> ContextualAssert<'_> {
        ContextualAssert::new(self, context)
    }

    /// Assert that a value matches a stored snapshot
    pub async fn assert_snapshot(&self, name: &str, value: &impl serde::Serialize) -> Result<()> {
        let snapshot_path = self.snapshot_path(name);
        let json_value = serde_json::to_value(value).map_err(|e| {
            SinexError::serialization(format!("Failed to serialize value for snapshot: {}", e))
        })?;

        // If snapshot exists, compare
        if tokio::fs::metadata(&snapshot_path).await.is_ok() {
            let existing = tokio::fs::read_to_string(&snapshot_path)
                .await
                .map_err(|e| SinexError::io(format!("Failed to read snapshot {}: {}", name, e)))?;
            let existing_value: serde_json::Value =
                serde_json::from_str(&existing).map_err(|e| {
                    SinexError::parse(format!("Failed to parse snapshot {}: {}", name, e))
                })?;

            if existing_value != json_value {
                return Err(SinexError::validation(format!(
                    "Snapshot '{}' mismatch:\nExpected:\n{}\nActual:\n{}",
                    name,
                    serde_json::to_string_pretty(&existing_value).map_err(|e| {
                        SinexError::serialization("Failed to serialize existing value")
                            .with_source(e)
                    })?,
                    serde_json::to_string_pretty(&json_value).map_err(|e| {
                        SinexError::serialization("Failed to serialize expected value")
                            .with_source(e)
                    })?
                )));
            }
        } else {
            // Create new snapshot
            if let Some(parent) = std::path::Path::new(&snapshot_path).parent() {
                tokio::fs::create_dir_all(parent).await.map_err(|e| {
                    SinexError::io(format!("Failed to create snapshot directory: {}", e))
                })?;
            }

            let content = serde_json::to_string_pretty(&json_value)?;
            tokio::fs::write(&snapshot_path, content)
                .await
                .map_err(|e| SinexError::io(format!("Failed to write snapshot {}: {}", name, e)))?;

            eprintln!("Created new snapshot: {}", snapshot_path);
        }

        Ok(())
    }

    /// Get the path for a snapshot file
    fn snapshot_path(&self, name: &str) -> String {
        format!("test/snapshots/{}/{}.json", self.test_name, name)
    }

    // ===== TIMING UTILITIES API =====

    /// Access timing utilities for coordination and waiting
    pub fn timing(&self) -> crate::timing_utils::TimingUtils<'_> {
        crate::timing_utils::TimingUtils::new(self)
    }

    // ===== CONVERTED MACRO FUNCTIONALITY =====

    /// Wait for a condition to become true (replaces eventually! macro)
    pub async fn wait_until<F, Fut>(&self, condition: F, timeout: Duration) -> Result<()>
    where
        F: Fn() -> Fut + Send + Sync,
        Fut: std::future::Future<Output = bool> + Send,
    {
        // Use the existing wait_for_condition with adaptive timeout
        let timeout_secs = timeout.as_secs().max(1);
        sinex_core_utils::wait_for_condition_adaptive(
            || async { Ok(condition().await) },
            timeout_secs,
            "wait_until condition",
        )
        .await
        .map_err(|e| SinexError::timeout(format!("Wait condition failed: {}", e)))
    }

    /// Assert two events are equivalent (replaces assert_event_eq! macro)
    pub fn assert_event_eq(&self, actual: &RawEvent, expected: &RawEvent) -> Result<()> {
        if actual.source != expected.source {
            return Err(SinexError::validation(format!(
                "Event sources differ: expected '{}', got '{}'",
                expected.source, actual.source
            )));
        }

        if actual.event_type != expected.event_type {
            return Err(SinexError::validation(format!(
                "Event types differ: expected '{}', got '{}'",
                expected.event_type, actual.event_type
            )));
        }

        if actual.payload != expected.payload {
            return Err(SinexError::validation(format!(
                "Event payloads differ:\nExpected: {}\nActual: {}",
                serde_json::to_string_pretty(&expected.payload)
                    .unwrap_or_else(|e| format!("<JSON serialization failed: {}>", e)),
                serde_json::to_string_pretty(&actual.payload)
                    .unwrap_or_else(|e| format!("<JSON serialization failed: {}>", e))
            )));
        }

        Ok(())
    }

    /// Assert events match patterns (replaces assert_events_match! macro)
    pub fn assert_events_match(
        &self,
        events: &[RawEvent],
        patterns: &[(String, String)],
    ) -> Result<()> {
        if events.len() != patterns.len() {
            return Err(SinexError::validation(format!(
                "Event count mismatch: expected {}, got {}",
                patterns.len(),
                events.len()
            )));
        }

        for (i, (event, pattern)) in events.iter().zip(patterns.iter()).enumerate() {
            if event.source != pattern.0 {
                return Err(SinexError::validation(format!(
                    "Event {} source mismatch: expected '{}', got '{}'",
                    i, pattern.0, event.source
                )));
            }

            if event.event_type != pattern.1 {
                return Err(SinexError::validation(format!(
                    "Event {} type mismatch: expected '{}', got '{}'",
                    i, pattern.1, event.event_type
                )));
            }
        }

        Ok(())
    }

    /// Run concurrent test tasks (replaces concurrent_test! macro)
    pub async fn run_concurrent<F, T, Fut>(&self, count: usize, f: F) -> Result<Vec<T>>
    where
        F: Fn(TestContext, usize) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<T>> + Send + 'static,
        T: Send + 'static,
    {
        use tokio::task::JoinSet;

        let f = Arc::new(f);
        let mut join_set = JoinSet::new();

        for i in 0..count {
            let test_name = format!("{}_concurrent_{}", self.test_name, i);
            let f = f.clone();
            join_set.spawn(async move {
                // Add timeout to detect potential deadlocks
                let task_timeout = Duration::from_secs(30);
                match tokio::time::timeout(task_timeout, async {
                    // Each concurrent task gets its own test database
                    let ctx = TestContext::with_name(&test_name).await?;
                    f(ctx, i).await
                })
                .await
                {
                    Ok(result) => result,
                    Err(_) => Err(SinexError::validation(format!(
                        "Task {} timed out after {:?} - possible deadlock",
                        i, task_timeout
                    ))),
                }
            });
        }

        let mut results = Vec::new();
        let mut errors = Vec::new();

        while let Some(result) = join_set.join_next().await {
            match result {
                Ok(Ok(value)) => results.push(value),
                Ok(Err(e)) => errors.push(e),
                Err(join_err) => errors.push(SinexError::unknown(format!(
                    "Task join failed: {}",
                    join_err
                ))),
            }
        }

        if !errors.is_empty() {
            // Aggregate errors with task indices for better debugging
            let error_details = errors
                .iter()
                .enumerate()
                .map(|(i, e)| format!("  Task {}: {}", i, e))
                .collect::<Vec<_>>()
                .join("\n");

            return Err(SinexError::validation(format!(
                "Concurrent test had {} failures out of {} tasks:\n{}",
                errors.len(),
                count,
                error_details
            )));
        }

        // Ensure we got all results
        if results.len() != count {
            return Err(SinexError::validation(format!(
                "Expected {} results but got {}. This may indicate tasks that panicked or deadlocked.",
                count,
                results.len()
            )));
        }

        Ok(results)
    }

    /// Measure execution time (replaces measure_time! macro)
    pub async fn measure<F, T>(&self, operation: F) -> Result<(T, Duration)>
    where
        F: std::future::Future<Output = T>,
    {
        let start = std::time::Instant::now();
        let result = operation.await;
        let duration = start.elapsed();
        Ok((result, duration))
    }

    /// Assert error contains specific text (replaces assert_error_contains! macro)
    pub fn assert_error_contains<T, E>(
        &self,
        result: &std::result::Result<T, E>,
        expected_text: &str,
    ) -> Result<()>
    where
        E: std::fmt::Display,
    {
        match result {
            Ok(_) => Err(SinexError::validation(format!(
                "Expected error containing '{}', but got Ok",
                expected_text,
            ))),
            Err(err) => {
                let err_string = err.to_string();
                if err_string.contains(expected_text) {
                    Ok(())
                } else {
                    Err(SinexError::validation(format!(
                        "Error '{}' does not contain '{}'",
                        err_string, expected_text,
                    )))
                }
            }
        }
    }

    /// Access property testing functionality
    pub fn property_tester(&self) -> crate::property_testing::PropertyTester<'_> {
        crate::property_testing::PropertyTester::new(self)
    }
}

// ===== FIXTURE BUILDERS =====

/// Unified fixture manager providing access to all fixture types
pub struct FixtureManager<'ctx> {
    ctx: &'ctx TestContext,
}

impl<'ctx> FixtureManager<'ctx> {
    /// Access scenario-based fixtures
    pub fn scenarios(&self) -> ScenarioFixtures<'ctx> {
        ScenarioFixtures { ctx: self.ctx }
    }

    /// Access performance testing fixtures
    pub fn performance(&self) -> PerformanceFixtures<'ctx> {
        PerformanceFixtures { ctx: self.ctx }
    }

    /// Access error testing fixtures
    pub fn errors(&self) -> ErrorFixtures<'ctx> {
        ErrorFixtures { ctx: self.ctx }
    }

    // Direct access methods for common fixtures
    pub async fn user_session(&self) -> Result<crate::fixtures::UserSessionFixture> {
        let fixture = crate::fixtures::standard_user_session(self.ctx)
            .await
            .map_err(|e| {
                SinexError::unknown("Failed to generate standard user session fixture")
                    .with_source(e)
            })?;
        Ok((*fixture).clone())
    }

    pub async fn large_dataset(&self) -> Result<crate::fixtures::LargeDatasetFixture> {
        crate::fixtures::large_event_dataset(self.ctx, 10000)
            .await
            .map_err(|e| {
                SinexError::unknown("Failed to generate large event dataset").with_source(e)
            })
    }

    pub async fn validation_failures(&self) -> Result<crate::fixtures::ValidationErrorsFixture> {
        crate::fixtures::validation_failures(self.ctx)
            .await
            .map_err(|e| {
                SinexError::unknown("Failed to generate validation failures fixture").with_source(e)
            })
    }
}

/// Scenario-based fixtures for testing common user workflows
pub struct ScenarioFixtures<'ctx> {
    ctx: &'ctx TestContext,
}

impl<'ctx> ScenarioFixtures<'ctx> {
    pub async fn user_session(&self) -> Result<crate::fixtures::UserSessionFixture> {
        let fixture = crate::fixtures::standard_user_session(self.ctx)
            .await
            .map_err(|e| {
                SinexError::unknown("Failed to generate user session fixture").with_source(e)
            })?;
        Ok((*fixture).clone())
    }

    pub async fn populated_checkpoints(&self) -> Result<crate::fixtures::CheckpointFixture> {
        let fixture = crate::fixtures::populated_checkpoints(self.ctx)
            .await
            .map_err(|e| {
                SinexError::unknown("Failed to generate populated checkpoints fixture")
                    .with_source(e)
            })?;
        Ok((*fixture).clone())
    }

    pub async fn terminal_session(&self) -> Result<crate::fixtures::TerminalSessionFixture> {
        crate::fixtures::terminal_session(self.ctx)
            .await
            .map_err(|e| {
                SinexError::unknown("Failed to generate terminal session fixture").with_source(e)
            })
    }

    pub async fn concurrent_operations(
        &self,
    ) -> Result<crate::fixtures::ConcurrentOperationsFixture> {
        crate::fixtures::concurrent_operations(self.ctx)
            .await
            .map_err(|e| {
                SinexError::unknown("Failed to generate concurrent operations fixture")
                    .with_source(e)
            })
    }
}

/// Performance testing fixtures for benchmarking and load testing
pub struct PerformanceFixtures<'ctx> {
    ctx: &'ctx TestContext,
}

impl<'ctx> PerformanceFixtures<'ctx> {
    pub async fn small_dataset(&self) -> Result<crate::fixtures::LargeDatasetFixture> {
        let config = crate::fixture_config::FIXTURE_CONFIG.clone();
        crate::fixtures::large_event_dataset(self.ctx, config.small_dataset_size)
            .await
            .map_err(|e| SinexError::unknown("Failed to generate small dataset").with_source(e))
    }

    pub async fn medium_dataset(&self) -> Result<crate::fixtures::LargeDatasetFixture> {
        let config = crate::fixture_config::FIXTURE_CONFIG.clone();
        crate::fixtures::large_event_dataset(self.ctx, config.medium_dataset_size)
            .await
            .map_err(|e| SinexError::unknown("Failed to generate medium dataset").with_source(e))
    }

    pub async fn large_dataset(&self) -> Result<crate::fixtures::LargeDatasetFixture> {
        let config = crate::fixture_config::FIXTURE_CONFIG.clone();
        crate::fixtures::large_event_dataset(self.ctx, config.large_dataset_size)
            .await
            .map_err(|e| SinexError::unknown("Failed to generate large dataset").with_source(e))
    }

    pub async fn dataset_with_size(
        &self,
        count: usize,
    ) -> Result<crate::fixtures::LargeDatasetFixture> {
        crate::fixtures::large_event_dataset(self.ctx, count)
            .await
            .map_err(|e| {
                SinexError::unknown("Failed to generate dataset with custom size")
                    .with_source(e)
                    .with_context("count", count)
            })
    }

    pub async fn large_dataset_with(
        &self,
        count: usize,
    ) -> Result<crate::fixtures::LargeDatasetFixture> {
        crate::fixtures::large_event_dataset(self.ctx, count)
            .await
            .map_err(|e| {
                SinexError::unknown("Failed to generate large dataset with custom count")
                    .with_source(e)
            })
    }

    pub async fn event_storm(&self) -> Result<crate::fixtures::EventStormFixture> {
        crate::fixtures::event_storm(self.ctx).await.map_err(|e| {
            SinexError::unknown("Failed to generate event storm fixture").with_source(e)
        })
    }

    pub async fn high_volume_checkpoints(
        &self,
    ) -> Result<crate::fixtures::HighVolumeCheckpointsFixture> {
        crate::fixtures::high_volume_checkpoints(self.ctx)
            .await
            .map_err(|e| {
                SinexError::unknown("Failed to generate high volume checkpoints fixture")
                    .with_source(e)
            })
    }
}

/// Error testing fixtures for validating error handling
pub struct ErrorFixtures<'ctx> {
    ctx: &'ctx TestContext,
}

impl<'ctx> ErrorFixtures<'ctx> {
    pub async fn validation_failures(&self) -> Result<crate::fixtures::ValidationErrorsFixture> {
        crate::fixtures::validation_failures(self.ctx)
            .await
            .map_err(|e| {
                SinexError::unknown("Failed to generate large event dataset").with_source(e)
            })
    }

    pub async fn schema_violations(&self) -> Result<crate::fixtures::SchemaViolationsFixture> {
        crate::fixtures::schema_violations(self.ctx)
            .await
            .map_err(|e| {
                SinexError::unknown("Failed to generate large event dataset").with_source(e)
            })
    }

    pub async fn malformed_events(&self) -> Result<crate::fixtures::MalformedEventsFixture> {
        crate::fixtures::malformed_events(self.ctx)
            .await
            .map_err(|e| {
                SinexError::unknown("Failed to generate large event dataset").with_source(e)
            })
    }
}

// ===== INTEGRATION TEST UTILITIES =====

/// Channel behavior testing utilities
pub struct ChannelTestUtils<'ctx> {
    ctx: &'ctx TestContext,
}

impl<'ctx> ChannelTestUtils<'ctx> {
    /// Test basic send/receive functionality with error context
    pub async fn test_basic_send_receive<T>(
        &self,
        sender: &impl sinex_channel::ChannelSenderExt<T>,
        receiver: &mut impl sinex_channel::ChannelReceiverExt<T>,
        test_value: T,
        test_name: &str,
    ) -> Result<()>
    where
        T: Send + PartialEq + std::fmt::Debug + Clone,
    {
        crate::channel_behavior_utils::behavior::test_basic_send_receive(
            sender, receiver, test_value, test_name,
        )
        .await
        .map_err(Into::into)
    }

    /// Test channel backpressure management
    pub async fn test_backpressure_management<T>(
        &self,
        sender: &impl sinex_channel::ChannelSenderExt<T>,
        test_items: Vec<T>,
        expected_timeout: std::time::Duration,
    ) -> Result<()>
    where
        T: Send + Clone,
    {
        crate::channel_behavior_utils::backpressure::test_backpressure_management(
            sender,
            test_items,
            expected_timeout,
        )
        .await
        .map_err(Into::into)
    }

    /// Create test channel setup with monitoring
    pub fn setup<T>(&self) -> crate::channel_behavior_utils::TestChannelSetup<T> {
        crate::channel_behavior_utils::TestChannelSetup::new(100)
    }
}

/// Process management testing utilities  
pub struct ProcessTestUtils<'ctx> {
    ctx: &'ctx TestContext,
}

impl<'ctx> ProcessTestUtils<'ctx> {
    /// Start test ingestd with default configuration
    pub async fn start_test_ingestd(
        &self,
    ) -> Result<crate::satellite_management_utils::TestIngestdHandle> {
        let config = crate::satellite_management_utils::TestIngestdConfig::default();
        crate::satellite_management_utils::start_test_ingestd_with_config(config)
            .await
            .map_err(|e| SinexError::unknown(format!("Failed to start test ingestd: {}", e)))
    }

    /// Start test satellite with configuration
    pub async fn start_test_satellite(
        &self,
        config: serde_json::Value,
    ) -> Result<crate::satellite_management_utils::TestSatelliteHandle> {
        crate::satellite_management_utils::TestSatelliteHandle::start(
            config,
            self.ctx.pool().clone(),
        )
        .await
        .map_err(|e| SinexError::unknown(format!("Failed to start test satellite: {}", e)))
    }

    /// Create satellite configuration
    pub fn satellite_config(&self, service_name: &str, socket_path: &str) -> serde_json::Value {
        crate::satellite_management_utils::build_test_satellite_config(service_name, socket_path)
    }
}

/// Deployment scenario testing utilities
pub struct DeploymentTestUtils<'ctx> {
    ctx: &'ctx TestContext,
}

impl<'ctx> DeploymentTestUtils<'ctx> {
    /// Create deployment scenario tester
    pub async fn create_tester(
        &self,
    ) -> Result<crate::deployment_scenario_utils::ConfigCompatibilityTester> {
        crate::deployment_scenario_utils::ConfigCompatibilityTester::new()
            .await
            .map_err(|e| SinexError::unknown(format!("Failed to create deployment tester: {}", e)))
    }

    // Removed unimplemented test_environment_compatibility method.
    // This functionality should be implemented when deployment scenario testing is actually needed.
}

/// Fluent event builder with schema validation
pub struct EventBuilder<'ctx> {
    ctx: &'ctx TestContext,
    source: Option<String>,
    event_type: Option<String>,
    payload: Value,
    timestamp: Option<DateTime<Utc>>,
}

impl<'ctx> EventBuilder<'ctx> {
    fn new(ctx: &'ctx TestContext) -> Self {
        Self {
            ctx,
            source: None,
            event_type: None,
            payload: json!({}),
            timestamp: None,
        }
    }

    /// Set event source
    pub fn source(mut self, source: impl Into<String>) -> Self {
        self.source = Some(source.into());
        self
    }

    /// Set event type
    pub fn type_(mut self, event_type: impl Into<String>) -> Self {
        self.event_type = Some(event_type.into());
        self
    }

    /// Set payload
    pub fn payload(mut self, payload: Value) -> Self {
        self.payload = payload;
        self
    }

    /// Add individual field (incremental payload building)
    pub fn field(mut self, key: &str, value: impl Into<Value>) -> Self {
        self.payload[key] = value.into();
        self
    }

    /// Add multiple fields at once  
    pub fn fields<I, K, V>(mut self, fields: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str>,
        V: Into<Value>,
    {
        for (key, value) in fields {
            self.payload[key.as_ref()] = value.into();
        }
        self
    }

    /// Set timestamp
    pub fn timestamp(mut self, ts: DateTime<Utc>) -> Self {
        self.timestamp = Some(ts);
        self
    }

    /// Domain-specific builder for filesystem events
    pub fn filesystem(mut self) -> FilesystemEventBuilder<'ctx> {
        self.source = Some(sources::FS.to_string());
        FilesystemEventBuilder { inner: self }
    }

    /// Domain-specific builder for terminal events  
    pub fn terminal(mut self) -> TerminalEventBuilder<'ctx> {
        self.source = Some(sources::SHELL_KITTY.to_string());
        TerminalEventBuilder { inner: self }
    }

    /// Domain-specific builder for agent events
    pub fn agent(mut self) -> AgentEventBuilder<'ctx> {
        self.source = Some(sources::SINEX.to_string());
        AgentEventBuilder { inner: self }
    }

    /// Domain-specific builder for clipboard events
    pub fn clipboard(mut self) -> ClipboardEventBuilder<'ctx> {
        self.source = Some(sources::CLIPBOARD.to_string());
        ClipboardEventBuilder { inner: self }
    }

    /// Domain-specific builder for window manager events
    pub fn window(mut self) -> WindowEventBuilder<'ctx> {
        self.source = Some(sources::WM_HYPRLAND.to_string());
        WindowEventBuilder { inner: self }
    }

    /// Domain-specific builder for system events
    pub fn system(mut self) -> SystemEventBuilder<'ctx> {
        self.source = Some(sources::SYSTEMD.to_string());
        SystemEventBuilder { inner: self }
    }

    /// Build event without inserting (like old TestEventBuilder)
    pub fn build(self) -> Result<RawEvent> {
        let source = self
            .source
            .ok_or_else(|| SinexError::validation("Source required"))?;
        let event_type = self
            .event_type
            .ok_or_else(|| SinexError::validation("Event type required"))?;

        let factory = EventFactory::new(&source);
        let mut event = factory.create_event(&event_type, self.payload);

        if let Some(ts) = self.timestamp {
            event.ts_orig = Some(ts);
        }

        Ok(event)
    }

    /// Build and insert event with validation (most common case)
    pub async fn insert(self) -> Result<RawEvent> {
        let ctx = self.ctx;
        let event = self.build()?;
        ctx.insert_event_internal(&event).await
    }

    /// Build and insert event without validation (for error testing)
    pub async fn insert_direct(self) -> Result<RawEvent> {
        // Use direct query path (bypasses validation like TestQueries)
        let host = gethostname::gethostname().to_string_lossy().to_string();
        use sinex_db::queries::EventQueries;

        use sinex_db::events::EventRecord;

        let record: EventRecord = EventQueries::insert_event(
            self.source.unwrap_or_else(|| "test".to_string()),
            self.event_type.unwrap_or_else(|| "test.event".to_string()),
            host,
            self.payload,
            self.timestamp,
            None, // ingestor_version
            None, // payload_schema_id
            None, // source_event_ids
        )
        .fetch_one(self.ctx.pool())
        .await?;

        Ok(record.into())
    }

    /// Build and insert multiple copies of this event
    pub async fn insert_batch(self, count: usize) -> Result<Vec<RawEvent>> {
        let ctx = self.ctx;
        let base_event = self.build()?;
        let mut events = Vec::with_capacity(count);

        for i in 0..count {
            let mut event = base_event.clone();
            // Add batch index to make each event unique
            if let Value::Object(ref mut map) = event.payload {
                map.insert("batch_index".to_string(), json!(i));
            }
            // Generate new ULID for each event
            event.id = sinex_ulid::Ulid::new();

            let inserted = ctx.insert_event_internal(&event).await?;
            events.push(inserted);
        }

        Ok(events)
    }
}

/// Filesystem-specific event builder
pub struct FilesystemEventBuilder<'ctx> {
    inner: EventBuilder<'ctx>,
}

impl<'ctx> FilesystemEventBuilder<'ctx> {
    /// Set file path
    pub fn path(mut self, path: impl Into<String>) -> Self {
        self.inner.payload["path"] = json!(path.into());
        self
    }

    /// Set file size
    pub fn size(mut self, size: u64) -> Self {
        self.inner.payload["size"] = json!(size);
        self
    }

    /// Set file permissions (unix style)
    pub fn permissions(mut self, perms: u32) -> Self {
        self.inner.payload["permissions"] = json!(perms);
        self
    }

    /// Add custom field (for extended file attributes)
    pub fn field(mut self, key: &str, value: impl Into<Value>) -> Self {
        self.inner.payload[key] = value.into();
        self
    }

    /// File created event (uses standard filesystem.file.created event type)
    pub fn created(mut self) -> Self {
        self.inner.event_type = Some(event_types::filesystem::FILE_CREATED.to_string());
        self
    }

    /// File modified event (uses standard filesystem.file.modified event type)
    pub fn modified(mut self) -> Self {
        self.inner.event_type = Some(event_types::filesystem::FILE_MODIFIED.to_string());
        self
    }

    /// File deleted event (uses standard filesystem.file.deleted event type)
    pub fn deleted(mut self) -> Self {
        self.inner.event_type = Some(event_types::filesystem::FILE_DELETED.to_string());
        self
    }

    /// Build event
    pub fn build(self) -> Result<RawEvent> {
        self.inner.build()
    }

    /// Build and insert
    pub async fn insert(self) -> Result<RawEvent> {
        self.inner.insert().await
    }
}

/// Terminal-specific event builder
pub struct TerminalEventBuilder<'ctx> {
    inner: EventBuilder<'ctx>,
}

impl<'ctx> TerminalEventBuilder<'ctx> {
    /// Set command (uses standard shell.command.executed event type)
    pub fn command(mut self, cmd: impl Into<String>) -> Self {
        self.inner.payload["command"] = json!(cmd.into());
        self.inner.event_type = Some(event_types::shell::COMMAND_EXECUTED.to_string());
        self
    }

    /// Set exit code directly
    pub fn exit_code(mut self, code: i32) -> Self {
        self.inner.payload["exit_code"] = json!(code);
        self
    }

    /// Set success status (exit_code = 0)
    pub fn success(mut self) -> Self {
        self.inner.payload["exit_code"] = json!(0);
        self
    }

    /// Set failure status (exit_code = 1)
    pub fn failed(mut self) -> Self {
        self.inner.payload["exit_code"] = json!(1);
        self
    }

    /// Set execution duration
    pub fn duration_ms(mut self, ms: u64) -> Self {
        self.inner.payload["duration_ms"] = json!(ms);
        self
    }

    /// Set working directory
    pub fn working_dir(mut self, dir: impl Into<String>) -> Self {
        self.inner.payload["working_directory"] = json!(dir.into());
        self
    }

    /// Add custom field
    pub fn field(mut self, key: &str, value: impl Into<Value>) -> Self {
        self.inner.payload[key] = value.into();
        self
    }

    /// Build event
    pub fn build(self) -> Result<RawEvent> {
        self.inner.build()
    }

    /// Build and insert  
    pub async fn insert(self) -> Result<RawEvent> {
        self.inner.insert().await
    }
}

/// Agent-specific event builder
pub struct AgentEventBuilder<'ctx> {
    inner: EventBuilder<'ctx>,
}

impl<'ctx> AgentEventBuilder<'ctx> {
    /// Set agent name
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.inner.payload["agent_name"] = json!(name.into());
        self
    }

    /// Set agent version
    pub fn version(mut self, version: impl Into<String>) -> Self {
        self.inner.payload["version"] = json!(version.into());
        self
    }

    /// Set uptime in seconds
    pub fn uptime_seconds(mut self, seconds: u64) -> Self {
        self.inner.payload["uptime_seconds"] = json!(seconds);
        self
    }

    /// Set events processed count
    pub fn events_processed(mut self, count: u64) -> Self {
        self.inner.payload["events_processed_session"] = json!(count);
        self
    }

    /// Set DLQ size
    pub fn dlq_size(mut self, size: u64) -> Self {
        self.inner.payload["dlq_size"] = json!(size);
        self
    }

    /// Heartbeat event
    pub fn heartbeat(mut self) -> Self {
        self.inner.event_type = Some("processor.heartbeat".to_string());
        self.inner.payload["status"] = json!("running");
        self
    }

    /// Startup event
    pub fn startup(mut self) -> Self {
        self.inner.event_type = Some("processor.startup".to_string());
        self.inner.payload["status"] = json!("starting");
        self
    }

    /// Error event
    pub fn error(mut self, error_msg: impl Into<String>) -> Self {
        self.inner.event_type = Some("processor.error".to_string());
        self.inner.payload["status"] = json!("error");
        self.inner.payload["error_message"] = json!(error_msg.into());
        self
    }

    /// Add custom field
    pub fn field(mut self, key: &str, value: impl Into<Value>) -> Self {
        self.inner.payload[key] = value.into();
        self
    }

    /// Build event
    pub fn build(self) -> Result<RawEvent> {
        self.inner.build()
    }

    /// Build and insert
    pub async fn insert(self) -> Result<RawEvent> {
        self.inner.insert().await
    }
}

/// Event query builder - abstracts all database operations
pub struct EventQuery<'ctx> {
    ctx: &'ctx TestContext,
    source_filter: Option<String>,
    type_filter: Option<String>,
    id_filter: Option<sinex_ulid::Ulid>,
    ids_filter: Option<Vec<sinex_ulid::Ulid>>,
    after_filter: Option<DateTime<Utc>>,
    limit_value: Option<i64>,
    offset_value: Option<i64>,
}

impl<'ctx> EventQuery<'ctx> {
    fn new(ctx: &'ctx TestContext) -> Self {
        Self {
            ctx,
            source_filter: None,
            type_filter: None,
            id_filter: None,
            ids_filter: None,
            after_filter: None,
            limit_value: None,
            offset_value: None,
        }
    }

    /// Filter by source
    pub fn by_source(mut self, source: impl Into<String>) -> Self {
        self.source_filter = Some(source.into());
        self
    }

    /// Filter by event type
    pub fn by_type(mut self, event_type: impl Into<String>) -> Self {
        self.type_filter = Some(event_type.into());
        self
    }

    /// Filter by ID
    pub fn by_id(mut self, id: sinex_ulid::Ulid) -> Self {
        self.id_filter = Some(id);
        self
    }

    /// Filter by multiple IDs
    pub fn by_ids(mut self, ids: Vec<sinex_ulid::Ulid>) -> Self {
        self.ids_filter = Some(ids);
        self
    }

    /// Filter events after timestamp
    pub fn after(mut self, timestamp: DateTime<Utc>) -> Self {
        self.after_filter = Some(timestamp);
        self
    }

    /// Limit results
    pub fn limit(mut self, limit: i64) -> Self {
        self.limit_value = Some(limit);
        self
    }

    /// Offset results
    pub fn offset(mut self, offset: i64) -> Self {
        self.offset_value = Some(offset);
        self
    }

    /// Fetch all matching events
    pub async fn fetch(self) -> Result<Vec<RawEvent>> {
        // Handle single ID filter
        if let Some(id) = self.id_filter {
            let event = EventQueries::get_by_id(id)
                .fetch_optional(self.ctx.pool())
                .await?;
            return Ok(event.into_iter().collect());
        }

        // Handle multiple IDs filter
        if let Some(ids) = self.ids_filter {
            let mut events = Vec::new();
            for id in ids {
                if let Some(event) = EventQueries::get_by_id(id)
                    .fetch_optional(self.ctx.pool())
                    .await?
                {
                    events.push(event);
                }
            }
            return Ok(events);
        }

        // Handle other filters
        if let Some(source) = self.source_filter {
            EventQueries::get_by_source(source, self.limit_value, self.offset_value)
                .fetch_all(self.ctx.pool())
                .await
                .map_err(Into::into)
        } else if let Some(event_type) = self.type_filter {
            EventQueries::get_by_event_type(event_type, self.limit_value, self.offset_value)
                .fetch_all(self.ctx.pool())
                .await
                .map_err(Into::into)
        } else {
            EventQueries::get_recent(self.limit_value, self.offset_value)
                .fetch_all(self.ctx.pool())
                .await
                .map_err(Into::into)
        }
    }

    /// Fetch single event
    pub async fn fetch_one(self) -> Result<Option<RawEvent>> {
        if let Some(id) = self.id_filter {
            EventQueries::get_by_id(id)
                .fetch_optional(self.ctx.pool())
                .await
                .map_err(Into::into)
        } else {
            let mut results = self.limit(1).fetch().await?;
            Ok(results.pop())
        }
    }

    /// Count matching events
    pub async fn count(self) -> Result<i64> {
        if let Some(source) = self.source_filter {
            let (count,) = EventQueries::count_by_source(source)
                .fetch_one::<(i64,)>(self.ctx.pool())
                .await?;
            Ok(count)
        } else if let Some(event_type) = self.type_filter {
            let (count,) = EventQueries::count_by_event_type(event_type)
                .fetch_one::<(i64,)>(self.ctx.pool())
                .await?;
            Ok(count)
        } else {
            sinex_db::count_events(self.ctx.pool())
                .await
                .map_err(Into::into)
        }
    }
}

/// Checkpoint builder for creating test checkpoints
pub struct CheckpointBuilder<'ctx> {
    ctx: &'ctx TestContext,
    processor_name: Option<String>,
    processed_count: Option<i64>,
    last_event_id: Option<sinex_ulid::Ulid>,
    status: Option<String>,
    metadata: Option<serde_json::Value>,
}

impl<'ctx> CheckpointBuilder<'ctx> {
    fn new(ctx: &'ctx TestContext) -> Self {
        Self {
            ctx,
            processor_name: None,
            processed_count: None,
            last_event_id: None,
            status: None,
            metadata: None,
        }
    }

    /// Set processor name
    pub fn processor(mut self, name: impl Into<String>) -> Self {
        self.processor_name = Some(name.into());
        self
    }

    /// Set processed event count
    pub fn processed_count(mut self, count: i64) -> Self {
        self.processed_count = Some(count);
        self
    }

    /// Set last processed event ID
    pub fn last_event_id(mut self, id: sinex_ulid::Ulid) -> Self {
        self.last_event_id = Some(id);
        self
    }

    /// Set checkpoint status
    pub fn status(mut self, status: impl Into<String>) -> Self {
        self.status = Some(status.into());
        self
    }

    /// Set checkpoint metadata
    pub fn metadata(mut self, metadata: serde_json::Value) -> Self {
        self.metadata = Some(metadata);
        self
    }

    /// Insert checkpoint
    pub async fn insert(self) -> Result<sinex_ulid::Ulid> {
        let processor_name = self
            .processor_name
            .unwrap_or_else(|| "test-processor".to_string());
        let checkpoint_id = sinex_ulid::Ulid::new();

        sqlx::query!(
            r#"
            INSERT INTO core.processor_checkpoints 
                (id, processor_name, consumer_group, consumer_name, last_processed_id, processed_count, created_at, updated_at)
            VALUES ($1, $2, $3, $4, $5, $6, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            "#,
            checkpoint_id as sinex_ulid::Ulid,
            processor_name,
            "default",
            "default",
            self.last_event_id as Option<sinex_ulid::Ulid>,
            self.processed_count.unwrap_or(0)
        )
        .execute(self.ctx.pool())
        .await
        .map_err(|e| SinexError::database("Failed to insert checkpoint")
            .with_source(e)
            .with_context("processor", &processor_name))?;

        Ok(checkpoint_id)
    }
}

/// Simple checkpoint record for tests
#[derive(sqlx::FromRow)]
pub struct CheckpointRecord {
    pub id: uuid::Uuid,
    pub processor_name: String,
    pub consumer_group: String,
    pub consumer_name: String,
    pub last_processed_id: Option<uuid::Uuid>,
    pub processed_count: i64,
}

impl CheckpointRecord {
    /// Get ID as ULID
    pub fn ulid_id(&self) -> sinex_ulid::Ulid {
        sinex_ulid::Ulid::from(self.id)
    }

    /// Get last processed ID as ULID
    pub fn ulid_last_processed_id(&self) -> Option<sinex_ulid::Ulid> {
        self.last_processed_id.map(sinex_ulid::Ulid::from)
    }
}

/// Checkpoint query builder
pub struct CheckpointQuery<'ctx> {
    ctx: &'ctx TestContext,
    processor_filter: Option<String>,
}

impl<'ctx> CheckpointQuery<'ctx> {
    fn new(ctx: &'ctx TestContext) -> Self {
        Self {
            ctx,
            processor_filter: None,
        }
    }

    /// Filter by processor name
    pub fn by_processor(mut self, processor: impl Into<String>) -> Self {
        self.processor_filter = Some(processor.into());
        self
    }

    /// Fetch all matching checkpoints
    pub async fn fetch(self) -> Result<Vec<CheckpointRecord>> {
        if let Some(processor) = self.processor_filter {
            sqlx::query_as!(
                CheckpointRecord,
                r#"
                SELECT 
                    id::uuid as "id!",
                    processor_name as "processor_name!",
                    consumer_group as "consumer_group!",
                    consumer_name as "consumer_name!",
                    last_processed_id::uuid as "last_processed_id",
                    processed_count as "processed_count!"
                FROM core.processor_checkpoints 
                WHERE processor_name = $1
                "#,
                &processor
            )
            .fetch_all(self.ctx.pool())
            .await
            .map_err(Into::into)
        } else {
            // Fetch all checkpoints
            sqlx::query_as!(
                CheckpointRecord,
                r#"
                SELECT 
                    id::uuid as "id!",
                    processor_name as "processor_name!",
                    consumer_group as "consumer_group!",
                    consumer_name as "consumer_name!",
                    last_processed_id::uuid as "last_processed_id",
                    processed_count as "processed_count!"
                FROM core.processor_checkpoints
                "#
            )
            .fetch_all(self.ctx.pool())
            .await
            .map_err(Into::into)
        }
    }

    /// Get checkpoint count for processor
    pub async fn count(self) -> Result<i64> {
        if let Some(processor) = self.processor_filter {
            let (count,) = CheckpointQueries::count_checkpoints_by_processor(processor)
                .fetch_one::<(i64,)>(self.ctx.pool())
                .await?;
            Ok(count)
        } else {
            // Count all checkpoints
            let (count,) = QueryBuilder::select("core.processor_checkpoints")
                .columns(&["COUNT(*) as count"])
                .fetch_one::<(i64,)>(self.ctx.pool())
                .await?;
            Ok(count)
        }
    }
}

/// Schema testing utilities
pub struct SchemaTestUtils<'ctx> {
    ctx: &'ctx TestContext,
}

impl<'ctx> SchemaTestUtils<'ctx> {
    fn new(ctx: &'ctx TestContext) -> Self {
        Self { ctx }
    }

    /// Register a test schema with automatic versioning
    pub async fn register(
        &self,
        source: &str,
        event_type: &str,
        schema: Value,
    ) -> Result<sinex_ulid::Ulid> {
        // Store schema in database using sinex_schemas.event_payload_schemas table
        let schema_name = format!("test_{}_{}", source, event_type.replace(".", "_"));
        let schema_version = "1.0.0";

        let result = sqlx::query!(
            r#"
            INSERT INTO sinex_schemas.event_payload_schemas 
                (schema_name, schema_version, schema_content, event_types, description, is_active)
            VALUES 
                ($1, $2, $3, $4, $5, true)
            ON CONFLICT (schema_name, schema_version) 
            DO UPDATE SET 
                schema_content = EXCLUDED.schema_content,
                updated_at = NOW()
            RETURNING id::text as "id!: String"
            "#,
            schema_name,
            schema_version,
            schema,
            &vec![event_type.to_string()],
            format!("Test schema for {} events", event_type),
        )
        .fetch_one(self.ctx.pool())
        .await
        .map_err(|e| SinexError::database(format!("Failed to register schema: {}", e)))?;

        // Parse the returned ID back to ULID
        let returned_id = sinex_ulid::Ulid::from_str(&result.id)
            .map_err(|e| SinexError::database(format!("Invalid ULID returned: {}", e)))?;

        Ok(returned_id)
    }

    /// Create a simple filesystem event schema
    pub fn filesystem_schema() -> Value {
        json!({
            "type": "object",
            "properties": {
                "path": {"type": "string"},
                "size": {"type": "number", "minimum": 0},
                "permissions": {"type": "string", "pattern": "^[0-7]{3,4}$"}
            },
            "required": ["path"]
        })
    }

    /// Create a simple terminal event schema
    pub fn terminal_schema() -> Value {
        json!({
            "type": "object",
            "properties": {
                "command": {"type": "string"},
                "exit_code": {"type": "number"},
                "duration_ms": {"type": "number", "minimum": 0}
            },
            "required": ["command"]
        })
    }

    /// Validate event and return detailed error if invalid
    pub async fn validate(&self, event: &RawEvent, schema_id: sinex_ulid::Ulid) -> Result<()> {
        // Fetch the schema directly and validate using json_matches_schema
        let schema_result = sqlx::query!(
            r#"
            SELECT schema_content, schema_name, schema_version
            FROM sinex_schemas.event_payload_schemas
            WHERE id::uuid = $1::uuid
            "#,
            schema_id.to_uuid(),
        )
        .fetch_optional(self.ctx.pool())
        .await
        .map_err(|e| SinexError::database(format!("Failed to fetch schema: {}", e)))?;

        match schema_result {
            Some(schema_row) => {
                // Perform direct validation using json_matches_schema
                let is_valid = sqlx::query_scalar!(
                    r#"
                    SELECT json_matches_schema($1::json, $2::json) as "is_valid!"
                    "#,
                    schema_row.schema_content,
                    &event.payload,
                )
                .fetch_one(self.ctx.pool())
                .await
                .map_err(|e| {
                    SinexError::validation(format!("Failed to validate against schema: {}", e))
                })?;

                if is_valid {
                    Ok(())
                } else {
                    // Try to get more detailed error by using jsonschema validation
                    Err(SinexError::validation(format!(
                        "Event payload does not match schema {} v{}",
                        schema_row.schema_name, schema_row.schema_version
                    )))
                }
            }
            None => Err(SinexError::not_found(format!(
                "Schema with ID {} not found",
                schema_id
            ))),
        }
    }

    /// Assert that event validates successfully
    pub async fn assert_valid(&self, event: &RawEvent, schema_id: sinex_ulid::Ulid) -> Result<()> {
        self.validate(event, schema_id).await.map_err(|e| {
            SinexError::validation(format!(
                "Expected event to be valid but validation failed: {}",
                e
            ))
        })
    }

    /// Assert that event validation fails
    pub async fn assert_invalid(
        &self,
        event: &RawEvent,
        schema_id: sinex_ulid::Ulid,
    ) -> Result<()> {
        match self.validate(event, schema_id).await {
            Ok(()) => Err(SinexError::validation(
                "Expected event to be invalid but validation passed",
            )),
            Err(_) => Ok(()), // Validation failed as expected
        }
    }
}

/// Validated event builder - ensures schema validation before insertion
pub struct ValidatedEventBuilder<'ctx> {
    ctx: &'ctx TestContext,
    schema_id: sinex_ulid::Ulid,
    event_builder: EventBuilder<'ctx>,
}

impl<'ctx> ValidatedEventBuilder<'ctx> {
    fn new(ctx: &'ctx TestContext, schema_id: sinex_ulid::Ulid) -> Self {
        Self {
            ctx,
            schema_id,
            event_builder: EventBuilder::new(ctx),
        }
    }

    /// Set event source
    pub fn source(mut self, source: impl Into<String>) -> Self {
        self.event_builder = self.event_builder.source(source);
        self
    }

    /// Set event type
    pub fn type_(mut self, event_type: impl Into<String>) -> Self {
        self.event_builder = self.event_builder.type_(event_type);
        self
    }

    /// Add field to payload
    pub fn field(mut self, key: &str, value: impl Into<Value>) -> Self {
        self.event_builder = self.event_builder.field(key, value);
        self
    }

    /// Build event with validation
    pub async fn build(self) -> Result<RawEvent> {
        let event = self.event_builder.build()?;
        self.ctx.schema().validate(&event, self.schema_id).await?;
        Ok(event)
    }

    /// Build and insert event with validation
    pub async fn insert(self) -> Result<RawEvent> {
        let ctx = self.ctx;
        let event = self.build().await?;
        ctx.insert_event_internal(&event).await
    }

    /// Filesystem-specific builder (uses production constants)
    pub fn filesystem(mut self) -> Self {
        self.event_builder = self
            .event_builder
            .source(sources::FS)
            .type_(event_types::filesystem::FILE_CREATED);
        self
    }

    /// Terminal-specific builder (uses production constants)  
    pub fn terminal(mut self) -> Self {
        self.event_builder = self
            .event_builder
            .source(sources::SHELL_KITTY)
            .type_(event_types::shell::COMMAND_EXECUTED);
        self
    }
}

/// Contextual assertion builder - provides rich error context for all assertions
#[derive(Debug)]
pub struct ContextualAssert<'ctx> {
    ctx: &'ctx TestContext,
    context: String,
}

impl<'ctx> ContextualAssert<'ctx> {
    fn new(ctx: &'ctx TestContext, context: &str) -> Self {
        Self {
            ctx,
            context: context.to_string(),
        }
    }

    /// Generic equality assertion with rich context
    pub fn eq<T: std::fmt::Debug + PartialEq>(self, actual: &T, expected: &T) -> Result<Self> {
        if actual != expected {
            return Err(SinexError::validation(format!(
                "Assertion failed in '{}': expected {:?}, got {:?}",
                self.context, expected, actual
            )));
        }
        Ok(self)
    }

    /// Generic inequality assertion with rich context
    pub fn not_eq<T: std::fmt::Debug + PartialEq>(self, actual: &T, expected: &T) -> Result<Self> {
        if actual == expected {
            return Err(SinexError::validation(format!(
                "Assertion failed in '{}': expected value to not equal {:?}, but it did",
                self.context, expected
            )));
        }
        Ok(self)
    }

    /// Boolean condition assertion with context
    pub fn that(self, condition: bool, message: &str) -> Result<Self> {
        if !condition {
            return Err(SinexError::validation(format!(
                "Assertion failed in '{}': {}",
                self.context, message
            )));
        }
        Ok(self)
    }

    /// Event-specific equality with field-by-field comparison
    pub fn event_eq(self, actual: &RawEvent, expected: &RawEvent) -> Result<Self> {
        // Check each field individually for better error messages
        if actual.source != expected.source {
            return Err(SinexError::validation(format!(
                "Event source mismatch in '{}': expected '{}', got '{}'",
                self.context, expected.source, actual.source
            )));
        }

        if actual.event_type != expected.event_type {
            return Err(SinexError::validation(format!(
                "Event type mismatch in '{}': expected '{}', got '{}'",
                self.context, expected.event_type, actual.event_type
            )));
        }

        if actual.payload != expected.payload {
            return Err(SinexError::validation(format!(
                "Event payload mismatch in '{}':\nExpected: {}\nActual: {}",
                self.context,
                serde_json::to_string_pretty(&expected.payload)
                    .unwrap_or_else(|e| format!("<JSON serialization failed: {}>", e)),
                serde_json::to_string_pretty(&actual.payload)
                    .unwrap_or_else(|e| format!("<JSON serialization failed: {}>", e))
            )));
        }

        Ok(self)
    }

    /// Assert that event insertion succeeds
    pub async fn event_inserts(self, event: &RawEvent) -> Result<sinex_ulid::Ulid> {
        match self.ctx.insert_event_internal(event).await {
            Ok(inserted) => Ok(inserted.id),
            Err(e) => Err(SinexError::validation(format!(
                "Event insertion failed in '{}': {} (source: {}, type: {})",
                self.context, e, event.source, event.event_type,
            ))),
        }
    }

    /// Assert that operation completes within timeout
    pub async fn completes_within<F, T>(
        self,
        operation: F,
        timeout: Duration,
        operation_name: &str,
    ) -> Result<T>
    where
        F: std::future::Future<Output = Result<T>>,
    {
        match tokio::time::timeout(timeout, operation).await {
            Ok(result) => result,
            Err(_) => Err(SinexError::validation(format!(
                "Operation '{}' timed out after {:?} in context '{}'",
                operation_name, timeout, self.context,
            ))),
        }
    }

    /// Assert error contains specific message
    pub fn error_contains<T>(self, result: &Result<T>, expected_message: &str) -> Result<Self> {
        match result {
            Ok(_) => Err(SinexError::validation(format!(
                "Expected error in '{}' but operation succeeded",
                self.context,
            ))),
            Err(e) => {
                let error_message = e.to_string();
                if error_message.contains(expected_message) {
                    Ok(self)
                } else {
                    Err(SinexError::validation(format!(
                        "Error in '{}' did not contain expected message '{}'. Actual error: {}",
                        self.context, expected_message, error_message
                    )))
                }
            }
        }
    }

    /// Assert collection has specific size
    pub fn has_size<T>(self, collection: &[T], expected_size: usize) -> Result<Self> {
        let actual_size = collection.len();
        if actual_size != expected_size {
            return Err(SinexError::validation(format!(
                "Collection size mismatch in '{}': expected {}, got {}",
                self.context, expected_size, actual_size
            )));
        }
        Ok(self)
    }

    /// Assert collection is not empty
    pub fn not_empty<T>(self, collection: &[T]) -> Result<Self> {
        if collection.is_empty() {
            return Err(SinexError::validation(format!(
                "Expected non-empty collection in '{}' but got empty collection",
                self.context
            )));
        }
        Ok(self)
    }

    /// Assert option contains a value
    pub fn some<T>(self, option: &Option<T>) -> Result<Self> {
        if option.is_none() {
            return Err(SinexError::validation(format!(
                "Expected Some(_) in '{}' but got None",
                self.context
            )));
        }
        Ok(self)
    }

    /// Assert option is None
    pub fn none<T>(self, option: &Option<T>) -> Result<Self> {
        if option.is_some() {
            return Err(SinexError::validation(format!(
                "Expected None in '{}' but got Some(_)",
                self.context
            )));
        }
        Ok(self)
    }
}

/// Clipboard-specific event builder
pub struct ClipboardEventBuilder<'ctx> {
    inner: EventBuilder<'ctx>,
}

impl<'ctx> ClipboardEventBuilder<'ctx> {
    /// Set clipboard content
    pub fn content(mut self, content: impl Into<String>) -> Self {
        self.inner.payload["content"] = json!(content.into());
        self
    }

    /// Set clipboard format (text, image, etc)
    pub fn format(mut self, format: impl Into<String>) -> Self {
        self.inner.payload["format"] = json!(format.into());
        self
    }

    /// Add custom field (for extended clipboard attributes)
    pub fn field(mut self, key: &str, value: impl Into<Value>) -> Self {
        self.inner.payload[key] = value.into();
        self
    }

    /// Clipboard copy event
    pub fn copied(mut self) -> Self {
        self.inner.event_type = Some("clipboard.copied".to_string());
        self
    }

    /// Clipboard paste event
    pub fn pasted(mut self) -> Self {
        self.inner.event_type = Some("clipboard.pasted".to_string());
        self
    }

    /// Build event
    pub fn build(self) -> Result<RawEvent> {
        self.inner.build()
    }

    /// Build and insert
    pub async fn insert(self) -> Result<RawEvent> {
        self.inner.insert().await
    }
}

/// Window manager event builder
pub struct WindowEventBuilder<'ctx> {
    inner: EventBuilder<'ctx>,
}

impl<'ctx> WindowEventBuilder<'ctx> {
    /// Set window ID
    pub fn window_id(mut self, id: impl Into<Value>) -> Self {
        self.inner.payload["window_id"] = id.into();
        self
    }

    /// Set window title
    pub fn title(mut self, title: impl Into<String>) -> Self {
        self.inner.payload["title"] = json!(title.into());
        self
    }

    /// Set window class
    pub fn class(mut self, class: impl Into<String>) -> Self {
        self.inner.payload["class"] = json!(class.into());
        self
    }

    /// Add custom field (for extended window attributes)
    pub fn field(mut self, key: &str, value: impl Into<Value>) -> Self {
        self.inner.payload[key] = value.into();
        self
    }

    /// Window focused event
    pub fn focused(mut self) -> Self {
        self.inner.event_type = Some("window.focused".to_string());
        self
    }

    /// Window created event
    pub fn created(mut self) -> Self {
        self.inner.event_type = Some("window.created".to_string());
        self
    }

    /// Window closed event
    pub fn closed(mut self) -> Self {
        self.inner.event_type = Some("window.closed".to_string());
        self
    }

    /// Build event
    pub fn build(self) -> Result<RawEvent> {
        self.inner.build()
    }

    /// Build and insert
    pub async fn insert(self) -> Result<RawEvent> {
        self.inner.insert().await
    }
}

/// System event builder
pub struct SystemEventBuilder<'ctx> {
    inner: EventBuilder<'ctx>,
}

impl<'ctx> SystemEventBuilder<'ctx> {
    /// Set service name
    pub fn service(mut self, name: impl Into<String>) -> Self {
        self.inner.payload["service"] = json!(name.into());
        self
    }

    /// Set unit type (service, timer, etc)
    pub fn unit_type(mut self, unit_type: impl Into<String>) -> Self {
        self.inner.payload["unit_type"] = json!(unit_type.into());
        self
    }

    /// Add custom field (for extended system attributes)
    pub fn field(mut self, key: &str, value: impl Into<Value>) -> Self {
        self.inner.payload[key] = value.into();
        self
    }

    /// Service started event
    pub fn started(mut self) -> Self {
        self.inner.event_type = Some("service.started".to_string());
        self
    }

    /// Service stopped event
    pub fn stopped(mut self) -> Self {
        self.inner.event_type = Some("service.stopped".to_string());
        self
    }

    /// Service failed event
    pub fn failed(mut self) -> Self {
        self.inner.event_type = Some("service.failed".to_string());
        self
    }

    /// System boot event
    pub fn boot(mut self) -> Self {
        self.inner.event_type = Some("system.boot".to_string());
        self
    }

    /// Build event
    pub fn build(self) -> Result<RawEvent> {
        self.inner.build()
    }

    /// Build and insert
    pub async fn insert(self) -> Result<RawEvent> {
        self.inner.insert().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sinex_test;
    use serde_json::json;

    #[sinex_test]
    async fn test_contexts_are_isolated(ctx: TestContext) -> Result<()> {
        // Create another context
        let ctx2 = TestContext::with_name("isolation_test").await?;

        // Insert event in first context
        let event1 = ctx
            .event()
            .source("ctx1")
            .type_("isolation.test")
            .field("context", "first")
            .insert()
            .await?;

        // Insert event in second context
        let event2 = ctx2
            .event()
            .source("ctx2")
            .type_("isolation.test")
            .field("context", "second")
            .insert()
            .await?;

        // Each context should only see its own event
        ctx.assert_event_exists(event1.id).await?;
        ctx2.assert_event_exists(event2.id).await?;

        // First context should not see second context's event
        let ctx1_events = ctx.events().fetch().await?;
        assert_eq!(ctx1_events.len(), 1);
        assert_eq!(ctx1_events[0].id, event1.id);

        // Second context should not see first context's event
        let ctx2_events = ctx2.events().fetch().await?;
        assert_eq!(ctx2_events.len(), 1);
        assert_eq!(ctx2_events[0].id, event2.id);

        Ok(())
    }

    #[sinex_test]
    async fn test_event_builder_fluent_api(ctx: TestContext) -> Result<()> {
        // Test all builder methods work correctly
        let event = ctx
            .event()
            .source("test_source")
            .type_("test.type")
            .field("string", "value")
            .field("number", 42)
            .field("boolean", true)
            .fields(vec![
                ("array", json!([1, 2, 3])),
                ("object", json!({"nested": "value"})),
            ])
            .timestamp(chrono::Utc::now())
            .insert()
            .await?;

        // Verify all fields
        assert_eq!(event.source, "test_source");
        assert_eq!(event.event_type, "test.type");
        assert_eq!(event.payload["string"], json!("value"));
        assert_eq!(event.payload["number"], json!(42));
        assert_eq!(event.payload["boolean"], json!(true));
        assert_eq!(event.payload["array"], json!([1, 2, 3]));
        assert_eq!(event.payload["object"], json!({"nested": "value"}));

        Ok(())
    }

    #[sinex_test]
    async fn test_domain_specific_builders(ctx: TestContext) -> Result<()> {
        // Test filesystem builder
        let fs_event = ctx
            .event()
            .filesystem()
            .path("/test/file.txt")
            .size(1024)
            .permissions(0o644)
            .created()
            .insert()
            .await?;

        assert_eq!(fs_event.source, sources::FS);
        assert_eq!(fs_event.event_type, event_types::filesystem::FILE_CREATED);
        assert_eq!(fs_event.payload["path"], json!("/test/file.txt"));
        assert_eq!(fs_event.payload["size"], json!(1024));

        // Test terminal builder
        let term_event = ctx
            .event()
            .terminal()
            .command("ls -la")
            .exit_code(0)
            .duration_ms(100)
            .working_dir("/home/user")
            .insert()
            .await?;

        assert_eq!(term_event.source, sources::SHELL_KITTY);
        assert_eq!(term_event.event_type, event_types::shell::COMMAND_EXECUTED);
        assert_eq!(term_event.payload["command"], json!("ls -la"));
        assert_eq!(term_event.payload["exit_code"], json!(0));

        // Test agent builder
        let agent_event = ctx
            .event()
            .agent()
            .name("test-processor")
            .version("1.0.0")
            .uptime_seconds(3600)
            .events_processed(1000)
            .heartbeat()
            .insert()
            .await?;

        assert_eq!(agent_event.source, sources::SINEX);
        assert_eq!(agent_event.event_type, "processor.heartbeat");
        assert_eq!(agent_event.payload["agent_name"], json!("test-processor"));
        assert_eq!(agent_event.payload["version"], json!("1.0.0"));

        Ok(())
    }

    #[sinex_test]
    async fn test_query_builder_chains(ctx: TestContext) -> Result<()> {
        // Insert various events
        for i in 0..10 {
            ctx.event()
                .source(if i % 2 == 0 { "even" } else { "odd" })
                .type_(if i < 5 { "type.a" } else { "type.b" })
                .field("index", i)
                .field("value", i * 10)
                .insert()
                .await?;
        }

        // Test by_source
        let even_events = ctx.events().by_source("even").fetch().await?;
        assert_eq!(even_events.len(), 5);

        // Test by_type
        let type_a_events = ctx.events().by_type("type.a").fetch().await?;
        assert_eq!(type_a_events.len(), 5);

        // Test limit
        let limited = ctx.events().limit(3).fetch().await?;
        assert_eq!(limited.len(), 3);

        // Test combined filters
        let even_type_a = ctx
            .events()
            .by_source("even")
            .by_type("type.a")
            .fetch()
            .await?;
        assert_eq!(even_type_a.len(), 3); // 0, 2, 4

        // Test count
        let total_count = ctx.events().count().await?;
        assert_eq!(total_count, 10);

        let even_count = ctx.events().by_source("even").count().await?;
        assert_eq!(even_count, 5);

        Ok(())
    }

    #[sinex_test]
    async fn test_assertion_api(ctx: TestContext) -> Result<()> {
        // Test successful assertions
        ctx.assert("basic equality").eq(&5, &5)?;
        ctx.assert("condition")
            .that(10 > 5, "10 should be greater than 5")?;

        // Test collection assertions
        let items = vec!["a", "b", "c"];
        ctx.assert("collection size").has_size(&items, 3)?;
        ctx.assert("not empty").not_empty(&items)?;

        // Test option assertions
        let some_val = Some("value");
        let none_val: Option<&str> = None;
        ctx.assert("some value").some(&some_val)?;
        ctx.assert("none value").none(&none_val)?;

        // Test event assertions
        let event1 = ctx.event().source("test").type_("assert").build()?;
        let event2 = ctx.event().source("test").type_("assert").build()?;
        ctx.assert("events equal").event_eq(&event1, &event2)?;

        // Test that assertions fail when they should
        let fail_result = ctx.assert("should fail").eq(&5, &10);
        assert!(fail_result.is_err());
        assert!(fail_result
            .unwrap_err()
            .to_string()
            .contains("expected 10, got 5"));

        Ok(())
    }

    #[sinex_test]
    async fn test_wait_helpers(ctx: TestContext) -> Result<()> {
        // Test wait_for_event_count
        // First create the events directly
        for i in 0..5 {
            ctx.event()
                .source("async")
                .type_("test")
                .field("index", i)
                .insert()
                .await?;
        }

        // Wait for events to appear
        ctx.wait_for_event_count(5).await?;

        // Verify they're there
        let count = ctx.events().count().await?;
        assert_eq!(count, 5);

        Ok(())
    }

    #[sinex_test]
    async fn test_batch_operations(ctx: TestContext) -> Result<()> {
        // Test create_event_batch
        let batch = ctx.create_event_batch("batch_test", 5);
        assert_eq!(batch.len(), 5);

        // Insert them all
        let mut inserted = Vec::new();
        for builder in batch {
            let event = builder.insert().await?;
            inserted.push(event);
        }

        // Verify all were inserted
        let events = ctx.events().by_source("batch_test").fetch().await?;
        assert_eq!(events.len(), 5);

        // Test insert_events with pre-built events
        let pre_built: Vec<RawEvent> = (0..3)
            .map(|i| {
                ctx.event()
                    .source("pre_built")
                    .type_("test")
                    .field("index", i)
                    .build()
            })
            .collect::<Result<Vec<_>>>()?;

        let inserted = ctx.insert_events(&pre_built).await?;
        assert_eq!(inserted.len(), 3);

        Ok(())
    }

    #[sinex_test]
    async fn test_schema_validation(ctx: TestContext) -> Result<()> {
        // Register a test schema
        let schema = json!({
            "type": "object",
            "properties": {
                "required_field": {"type": "string"},
                "optional_field": {"type": "number"}
            },
            "required": ["required_field"]
        });

        let schema_id = ctx
            .schema()
            .register("test", "validated.event", schema)
            .await?;

        // Create valid event
        let valid_event = ctx
            .event()
            .source("test")
            .type_("validated.event")
            .field("required_field", "present")
            .field("optional_field", 42)
            .build()?;

        // Should validate successfully
        ctx.schema().validate(&valid_event, schema_id).await?;

        // Create invalid event (missing required field)
        let invalid_event = ctx
            .event()
            .source("test")
            .type_("validated.event")
            .field("optional_field", 42)
            .build()?;

        // Should fail validation
        let result = ctx.schema().validate(&invalid_event, schema_id).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("required"));

        // Test validated event builder
        let validated = ctx
            .validated_event(schema_id)
            .source("test")
            .type_("validated.event")
            .field("required_field", "valid")
            .insert()
            .await?;

        assert_eq!(validated.payload["required_field"], json!("valid"));

        Ok(())
    }

    #[sinex_test]
    async fn test_timing_utilities(ctx: TestContext) -> Result<()> {
        // Test synchronizer
        let sync = ctx.timing().synchronizer(Duration::from_secs(1));

        // Spawn task to signal after delay
        let sync_clone = Arc::new(sync);
        let sync_for_task = sync_clone.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            sync_for_task.signal();
        });

        // Wait should succeed
        sync_clone
            .wait()
            .await
            .map_err(|_| SinexError::timeout("Sync wait failed"))?;

        // Test event counter
        let counter = ctx.timing().event_counter(3);
        counter.increment();
        counter.increment();
        counter.increment();
        assert_eq!(counter.get(), 3);

        // Test barrier
        let barrier = ctx.timing().barrier(2);
        let barrier_clone = Arc::new(barrier);

        let b1 = barrier_clone.clone();
        let h1 = tokio::spawn(async move { b1.wait(Duration::from_secs(1)).await });

        let b2 = barrier_clone.clone();
        let h2 = tokio::spawn(async move { b2.wait(Duration::from_secs(1)).await });

        // Both should complete successfully
        h1.await
            .map_err(|e| SinexError::service(format!("Task 1 failed: {}", e)))?
            .map_err(|e| SinexError::timeout(format!("Barrier wait failed: {:?}", e)))?;
        h2.await
            .map_err(|e| SinexError::service(format!("Task 2 failed: {}", e)))?
            .map_err(|e| SinexError::timeout(format!("Barrier wait failed: {:?}", e)))?;

        Ok(())
    }

    #[sinex_test]
    async fn test_error_handling_in_builders(ctx: TestContext) -> Result<()> {
        // Test empty source validation
        let result = ctx.event().source("").type_("test").insert().await;
        assert!(result.is_err());

        // Test empty type validation
        let result = ctx.event().source("test").type_("").insert().await;
        assert!(result.is_err());

        // Test insert_direct bypasses validation
        let event = ctx
            .event()
            .source("") // Would normally fail
            .type_("test")
            .insert_direct()
            .await;
        // This might succeed or fail depending on database constraints
        // The point is it bypasses our validation layer

        Ok(())
    }

    #[sinex_test]
    async fn test_concurrent_helpers(ctx: TestContext) -> Result<()> {
        // Test run_concurrent
        let results = ctx
            .run_concurrent(3, |ctx, i| async move {
                // Each task inserts an event
                let event = ctx
                    .event()
                    .source("concurrent")
                    .type_("test")
                    .field("task_id", i)
                    .insert()
                    .await?;
                Ok(event.id)
            })
            .await?;

        assert_eq!(results.len(), 3);

        // All events should be in original context
        let events = ctx.events().by_source("concurrent").fetch().await?;
        assert_eq!(events.len(), 3);

        Ok(())
    }

    #[sinex_test]
    async fn test_measure_helper(ctx: TestContext) -> Result<()> {
        let (result, duration) = ctx
            .measure(async {
                tokio::time::sleep(Duration::from_millis(50)).await;
                ctx.event().source("measured").type_("test").insert().await
            })
            .await?;

        assert!(duration >= Duration::from_millis(50));
        assert!(duration < Duration::from_millis(200)); // Reasonable upper bound
        assert!(result.is_ok());

        Ok(())
    }

    // Removed test_context_provides_isolation - duplicate of test_contexts_are_isolated
    // Removed test_context_tracks_event_count - functionality tested in other tests
    // Removed test_context_timing_measurement - covered by test_timing_utilities

    // Merged test_assertion_helpers and test_assertion_api into comprehensive test above

    // Merged test_query_builder_chaining and test_query_builder_flexibility
    // into test_query_builder_chains above

    #[sinex_test]
    async fn test_multiple_schemas(ctx: TestContext) -> Result<()> {
        // Test managing multiple schemas for different event types
        let user_schema = ctx
            .schema()
            .register(
                "user",
                "user.created",
                json!({
                    "type": "object",
                    "properties": {
                        "username": {"type": "string", "pattern": "^[a-z0-9_]+$"},
                        "email": {"type": "string", "format": "email"},
                        "age": {"type": "integer", "minimum": 18}
                    },
                    "required": ["username", "email"]
                }),
            )
            .await?;

        let product_schema = ctx
            .schema()
            .register(
                "product",
                "product.added",
                json!({
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"},
                        "price": {"type": "number", "minimum": 0},
                        "quantity": {"type": "integer", "minimum": 0}
                    },
                    "required": ["name", "price", "quantity"]
                }),
            )
            .await?;

        // Create events with appropriate schemas
        let user_event = ctx
            .validated_event(user_schema)
            .source("user")
            .type_("user.created")
            .field("username", "test_user")
            .field("email", "test@example.com")
            .field("age", 25)
            .insert()
            .await?;

        let product_event = ctx
            .validated_event(product_schema)
            .source("product")
            .type_("product.added")
            .field("name", "Test Product")
            .field("price", 19.99)
            .field("quantity", 100)
            .insert()
            .await?;

        // Verify correct schema validation
        ctx.schema().assert_valid(&user_event, user_schema).await?;
        ctx.schema()
            .assert_valid(&product_event, product_schema)
            .await?;

        // Cross-validation should fail
        ctx.schema()
            .assert_invalid(&user_event, product_schema)
            .await?;
        ctx.schema()
            .assert_invalid(&product_event, user_schema)
            .await?;

        Ok(())
    }

    #[sinex_test]
    async fn test_schema_evolution(ctx: TestContext) -> Result<()> {
        // Test schema versioning and evolution patterns
        let v1_schema = ctx
            .schema()
            .register(
                "api",
                "api.request.v1",
                json!({
                    "type": "object",
                    "properties": {
                        "method": {"type": "string"},
                        "path": {"type": "string"}
                    },
                    "required": ["method", "path"]
                }),
            )
            .await?;

        // Create v1 event
        let v1_event = ctx
            .validated_event(v1_schema)
            .source("api")
            .type_("api.request.v1")
            .field("method", "GET")
            .field("path", "/users")
            .insert()
            .await?;

        // Register evolved schema with additional fields
        let v2_schema = ctx
            .schema()
            .register(
                "api",
                "api.request.v2",
                json!({
                    "type": "object",
                    "properties": {
                        "method": {"type": "string"},
                        "path": {"type": "string"},
                        "headers": {"type": "object"},
                        "timestamp": {"type": "string", "format": "date-time"}
                    },
                    "required": ["method", "path", "timestamp"]
                }),
            )
            .await?;

        // Create v2 event
        let v2_event = ctx
            .validated_event(v2_schema)
            .source("api")
            .type_("api.request.v2")
            .field("method", "POST")
            .field("path", "/users")
            .field("headers", json!({"content-type": "application/json"}))
            .field("timestamp", chrono::Utc::now().to_rfc3339())
            .insert()
            .await?;

        // Both should validate against their respective schemas
        ctx.schema().assert_valid(&v1_event, v1_schema).await?;
        ctx.schema().assert_valid(&v2_event, v2_schema).await?;

        Ok(())
    }
}

#[cfg(all(test, feature = "bench"))]
mod benches {
    use super::*;
    use crate::sinex_bench;
    use divan::black_box;

    #[sinex_bench]
    async fn bench_context_creation() -> anyhow::Result<()> {
        black_box(TestContext::new().await?);
        Ok(())
    }

    #[sinex_bench]
    async fn bench_context_with_name() -> anyhow::Result<()> {
        black_box(TestContext::with_name("bench_test").await?);
        Ok(())
    }

    #[sinex_bench]
    async fn bench_single_event_creation() -> anyhow::Result<()> {
        let ctx = TestContext::new().await?;
        black_box(
            ctx.event()
                .source("bench")
                .type_("test.event")
                .field("index", 1)
                .insert()
                .await?,
        );
        Ok(())
    }

    #[sinex_bench]
    async fn bench_batch_event_creation_small() -> anyhow::Result<()> {
        let ctx = TestContext::new().await?;
        let batch = ctx.create_event_batch("bench", 10);
        for builder in batch {
            black_box(builder.insert().await?);
        }
        Ok(())
    }

    #[sinex_bench]
    async fn bench_batch_event_creation_medium() -> anyhow::Result<()> {
        let ctx = TestContext::new().await?;
        let batch = ctx.create_event_batch("bench", 100);
        for builder in batch {
            black_box(builder.insert().await?);
        }
        Ok(())
    }

    // For benchmarks that need persistent data, we use BenchContext
    use crate::bench::BenchContext;

    #[sinex_bench]
    async fn bench_query_count_all(ctx: &BenchContext) -> anyhow::Result<()> {
        // Load standard query benchmark fixture
        ctx.query_bench(crate::static_fixtures::DatasetSize::Small)
            .await?;

        // Measure the count query
        use sinex_db::queries::EventQueries;
        let (count,) = EventQueries::count_all()
            .fetch_one::<(i64,)>(ctx.pool())
            .await?;
        black_box(count);
        Ok(())
    }

    #[sinex_bench]
    async fn bench_query_fetch_limited(ctx: &BenchContext) -> anyhow::Result<()> {
        ctx.query_bench(crate::static_fixtures::DatasetSize::Small)
            .await?;

        use sinex_db::queries::EventQueries;
        let events = EventQueries::get_recent(Some(10), None)
            .fetch_all::<sinex_db::events::EventRecord>(ctx.pool())
            .await?;
        black_box(events);
        Ok(())
    }

    #[sinex_bench]
    async fn bench_query_filtered(ctx: &BenchContext) -> anyhow::Result<()> {
        ctx.query_bench(crate::static_fixtures::DatasetSize::Small)
            .await?;

        use sinex_db::queries::EventQueries;
        let events = EventQueries::get_by_event_type("file.created".to_string(), Some(100), None)
            .fetch_all::<sinex_db::events::EventRecord>(ctx.pool())
            .await?;
        black_box(events);
        Ok(())
    }

    #[sinex_bench]
    async fn bench_concurrent_operations() -> anyhow::Result<()> {
        let ctx = TestContext::new().await?;
        let results = ctx
            .run_concurrent(4, |ctx, i| async move {
                ctx.event()
                    .source("concurrent")
                    .type_("task")
                    .field("worker", i)
                    .insert()
                    .await
            })
            .await?;
        black_box(results);
        Ok(())
    }

    #[sinex_bench]
    async fn bench_simple_assertions() -> anyhow::Result<()> {
        let ctx = TestContext::new().await?;
        ctx.assert("test1").eq(&5, &5)?;
        ctx.assert("test2").that(true, "should be true")?;
        ctx.assert("test3").not_empty(&vec![1, 2, 3])?;
        black_box(());
        Ok(())
    }
}

// Cleanup implementation for TestContext
impl Drop for TestContext {
    fn drop(&mut self) {
        // Clean up Redis keys if any were tracked
        if let Ok(keys) = self.redis_cleanup_keys.lock() {
            if !keys.is_empty() {
                // Spawn a task to clean up Redis keys
                let keys_to_clean = keys.clone();
                let test_name = self.test_name.clone();

                // We can't use async in Drop, so spawn a detached task
                std::thread::spawn(move || {
                    if let Ok(runtime) = tokio::runtime::Runtime::new() {
                        runtime.block_on(async {
                            match crate::redis_pool::acquire_test_redis().await {
                                Ok(mut test_redis) => {
                                    let conn = test_redis.conn();
                                    for key in keys_to_clean {
                                        if let Err(e) = redis::cmd("DEL")
                                            .arg(&key)
                                            .query::<()>(conn)
                                        {
                                            eprintln!(
                                                "Failed to clean up Redis key '{}' for test '{}': {}",
                                                key, test_name, e
                                            );
                                        }
                                    }
                                }
                                Err(e) => {
                                    eprintln!(
                                        "Failed to get Redis connection for cleanup in test '{}': {}",
                                        test_name, e
                                    );
                                }
                            }
                        });
                    }
                });
            }
        }

        // Log test completion
        let duration = self.start_time.elapsed();
        if duration > Duration::from_secs(5) {
            eprintln!(
                "Test '{}' took {:?} to complete (including cleanup)",
                self.test_name, duration
            );
        }
    }
}

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
use sinex_core_types::RawEvent;
use sinex_db::queries::{EventQueries, CheckpointQueries};
use sinex_db::query_builder::QueryBuilder;
use sinex_events::{EventFactory, sources, event_types};
use sinex_error::CoreError;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use std::sync::Arc;
use sinex_core_types::Result as TestResult;
use serde_json::{Value, json};
use chrono::{DateTime, Utc};


// Default timeout for test operations
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(3);

/// Unified test context - single entry point for all test operations
#[derive(Debug)]
pub struct TestContext {
    db: TestDatabase,
    test_name: String,
    start_time: Instant,
    created_events: Arc<Mutex<Vec<sinex_ulid::Ulid>>>,
}

impl TestContext {
    /// Create new test context
    pub async fn new() -> TestResult<Self> {
        Self::with_name("unnamed_test").await
    }
    
    /// Create test context with custom name
    pub async fn with_name(test_name: &str) -> TestResult<Self> {
        let db = crate::database_pool::acquire_test_database().await?;
        
        Ok(Self {
            db,
            test_name: test_name.to_string(),
            start_time: Instant::now(),
            created_events: Arc::new(Mutex::new(Vec::new())),
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
    
    /// Get elapsed time since test start
    pub fn elapsed(&self) -> Duration {
        self.start_time.elapsed()
    }
    
    // ===== SINGLE EVENT CREATION API =====
    
    /// Create an event builder - single entry point for all event creation
    pub fn event(&self) -> EventBuilder<'_> {
        EventBuilder::new(self)
    }
    
    /// Insert event directly (internal use)
    pub(crate) async fn insert_event_internal(&self, event: &RawEvent) -> TestResult<RawEvent> {
        let inserted = sinex_db::insert_event_with_validator(self.pool(), event, None).await
            .map_err(|e| CoreError::Database(e.to_string()))?;
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
    pub async fn wait_for_event_count(&self, expected: usize) -> TestResult<()> {
        let timeout_secs = DEFAULT_TIMEOUT.as_secs();
        
        sinex_core_utils::wait_for_condition_adaptive(
            || async {
                let count = self.events().count().await.map_err(|e| CoreError::Database(e.to_string()))? as usize;
                Ok(count >= expected)
            },
            timeout_secs,
            &format!("event count >= {}", expected)
        ).await
        .map_err(|e| CoreError::Timeout(format!("Wait condition failed: {}", e)))
    }
    
    /// Wait for condition to become true using production wait helpers
    pub async fn wait_for_condition<F, Fut>(&self, condition: F) -> TestResult<()>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = TestResult<bool>>,
    {
        let timeout_secs = DEFAULT_TIMEOUT.as_secs();
        
        sinex_core_utils::wait_for_condition_adaptive(
            || async {
                match condition().await {
                    Ok(result) => Ok(result),
                    Err(e) => Err(CoreError::Unknown(e.to_string())),
                }
            },
            timeout_secs,
            "custom test condition"
        ).await
        .map_err(|e| CoreError::Timeout(format!("Wait condition failed: {}", e)))
    }
    
    // ===== ASSERTION HELPERS =====
    
    /// Assert specific event count using production error context
    pub async fn assert_event_count(&self, expected: usize) -> TestResult<()> {
        let actual = self.events().count().await? as usize;
        if actual != expected {
            return Err(
                CoreError::validation("Event count assertion failed")
                    .with_context("expected_count", expected)
                    .with_context("actual_count", actual)
                    .with_context("test_name", &self.test_name)
                    .with_operation("assert_event_count")
                    .build()
                    .into()
            );
        }
        Ok(())
    }
    
    /// Assert no events exist
    pub async fn assert_no_events(&self) -> TestResult<()> {
        self.assert_event_count(0).await
    }
    
    /// Assert event with ID exists
    pub async fn assert_event_exists(&self, id: sinex_ulid::Ulid) -> TestResult<()> {
        let event = self.events().by_id(id).fetch_one().await?;
        if event.is_none() {
            return Err(CoreError::NotFound(format!("Event {}", id)));
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
    pub async fn insert_event(&self, event: &RawEvent) -> TestResult<RawEvent> {
        self.insert_event_internal(event).await
    }
    
    /// Insert multiple pre-built events
    pub async fn insert_events(&self, events: &[RawEvent]) -> TestResult<Vec<RawEvent>> {
        let mut inserted = Vec::with_capacity(events.len());
        for event in events {
            inserted.push(self.insert_event_internal(event).await?);
        }
        Ok(inserted)
    }
    
    // ===== FIXTURE BUILDERS =====
    
    /// Access scenario fixtures (user sessions, checkpoints, etc.)
    pub fn scenarios(&self) -> ScenarioFixtures<'_> {
        ScenarioFixtures { ctx: self }
    }
    
    /// Access performance fixtures (large datasets, pre-warmed data, etc.)
    pub fn performance(&self) -> PerformanceFixtures<'_> {
        PerformanceFixtures { ctx: self }
    }
    
    /// Access error testing fixtures (validation failures, empty database, etc.)
    pub fn errors(&self) -> ErrorFixtures<'_> {
        ErrorFixtures { ctx: self }
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
    
    /// Register test schema for validation
    pub async fn register_schema(&self, source: &str, event_type: &str, _version: &str, schema: Value) -> TestResult<sinex_ulid::Ulid> {
        use sinex_db::queries::SchemaQueries;
        let schema_id = sinex_ulid::Ulid::new();
        
        SchemaQueries::insert_schema(
            event_type.to_string(),
            1, // schema_version as i32
            schema
        ).execute(self.pool()).await.map_err(|e| {
            CoreError::database(format!("Failed to register test schema for {}/{}: {}", source, event_type, e)).build()
        })?;
        
        Ok(schema_id)
    }
    
    /// Validate event against registered schema
    pub async fn validate_against_schema(&self, event: &RawEvent, schema_id: sinex_ulid::Ulid) -> TestResult<()> {
        // Get schema from database using production query
        let schema_record: sinex_db::models::EventPayloadSchema = sinex_db::queries::SchemaQueries::get_by_id(schema_id)
            .fetch_optional(self.pool())
            .await?
            .ok_or_else(|| CoreError::Validation(format!("Schema not found: {}", schema_id)))?;
        
        // Validate using jsonschema crate
        let schema = jsonschema::JSONSchema::compile(&schema_record.json_schema_definition)
            .map_err(|e| CoreError::Validation(format!("Invalid JSON schema {}: {}", schema_id, e)))?;
        
        let validation_result = schema.validate(&event.payload);
        if let Err(errors) = validation_result {
            let error_messages: Vec<String> = errors
                .map(|e| format!("  - {}: {}", e.instance_path, e))
                .collect();
            return Err(CoreError::Validation(format!("Schema validation failed for event {}\nSchema ID: {}\nErrors:\n{}", event.id, schema_id, error_messages.join("\n"))));
        }
        
        Ok(())
    }
    
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
    
    // ===== TIMING UTILITIES API =====
    
    /// Access timing utilities for coordination and waiting
    pub fn timing(&self) -> crate::timing_utils::TimingUtils<'_> {
        crate::timing_utils::TimingUtils::new(self)
    }
    
    // ===== CONVERTED MACRO FUNCTIONALITY =====
    
    /// Wait for a condition to become true (replaces eventually! macro)
    pub async fn wait_until<F, Fut>(&self, condition: F, timeout: Duration) -> TestResult<()>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = bool>,
    {
        let start = std::time::Instant::now();
        
        loop {
            if condition().await {
                return Ok(());
            }
            
            if start.elapsed() > timeout {
                return Err(CoreError::Timeout(format!(
                    "Condition not met within {:?}", timeout
                )));
            }
            
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
    
    /// Assert two events are equivalent (replaces assert_event_eq! macro)
    pub fn assert_event_eq(&self, actual: &RawEvent, expected: &RawEvent) -> TestResult<()> {
        if actual.source != expected.source {
            return Err(CoreError::Validation(format!(
                "Event sources differ: expected '{}', got '{}'", 
                expected.source, actual.source
            )));
        }
        
        if actual.event_type != expected.event_type {
            return Err(CoreError::Validation(format!(
                "Event types differ: expected '{}', got '{}'", 
                expected.event_type, actual.event_type
            )));
        }
        
        if actual.payload != expected.payload {
            return Err(CoreError::Validation(format!(
                "Event payloads differ:\nExpected: {}\nActual: {}", 
                serde_json::to_string_pretty(&expected.payload).unwrap_or_else(|_| "invalid JSON".to_string()),
                serde_json::to_string_pretty(&actual.payload).unwrap_or_else(|_| "invalid JSON".to_string())
            )));
        }
        
        Ok(())
    }
    
    /// Assert events match patterns (replaces assert_events_match! macro)
    pub fn assert_events_match(&self, events: &[RawEvent], patterns: &[(String, String)]) -> TestResult<()> {
        if events.len() != patterns.len() {
            return Err(CoreError::Validation(format!(
                "Event count mismatch: expected {}, got {}", 
                patterns.len(), events.len()
            )));
        }
        
        for (i, (event, pattern)) in events.iter().zip(patterns.iter()).enumerate() {
            if event.source != pattern.0 {
                return Err(CoreError::Validation(format!(
                    "Event {} source mismatch: expected '{}', got '{}'", 
                    i, pattern.0, event.source
                )));
            }
            
            if event.event_type != pattern.1 {
                return Err(CoreError::Validation(format!(
                    "Event {} type mismatch: expected '{}', got '{}'", 
                    i, pattern.1, event.event_type
                )));
            }
        }
        
        Ok(())
    }
    
    /// Run concurrent test tasks (replaces concurrent_test! macro)
    pub async fn run_concurrent<F, T, Fut>(&self, count: usize, f: F) -> TestResult<Vec<T>>
    where
        F: Fn(TestContext, usize) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = TestResult<T>> + Send + 'static,
        T: Send + 'static,
    {
        use tokio::task::JoinSet;
        
        let f = Arc::new(f);
        let mut join_set = JoinSet::new();
        
        for i in 0..count {
            let test_name = format!("{}_concurrent_{}", self.test_name, i);
            let f = f.clone();
            join_set.spawn(async move {
                // Each concurrent task gets its own test database
                let ctx = TestContext::with_name(&test_name).await?;
                f(ctx, i).await
            });
        }
        
        let mut results = Vec::new();
        let mut errors = Vec::new();
        
        while let Some(result) = join_set.join_next().await {
            match result {
                Ok(Ok(value)) => results.push(value),
                Ok(Err(e)) => errors.push(e),
                Err(join_err) => errors.push(CoreError::Service(format!("Task join failed: {}", join_err))),
            }
        }
        
        if !errors.is_empty() {
            return Err(CoreError::Unknown(format!(
                "Concurrent test had {} failures: {:?}", 
                errors.len(), errors
            )));
        }
        
        Ok(results)
    }
    
    /// Measure execution time (replaces measure_time! macro)
    pub async fn measure<F, T>(&self, operation: F) -> TestResult<(T, Duration)>
    where
        F: std::future::Future<Output = T>,
    {
        let start = std::time::Instant::now();
        let result = operation.await;
        let duration = start.elapsed();
        Ok((result, duration))
    }
    
    /// Assert error contains specific text (replaces assert_error_contains! macro)
    pub fn assert_error_contains<T, E>(&self, result: &Result<T, E>, expected_text: &str) -> TestResult<()>
    where
        E: std::fmt::Display,
    {
        match result {
            Ok(_) => Err(CoreError::Validation(format!(
                "Expected error containing '{}', but got Ok", expected_text
            ))),
            Err(err) => {
                let err_string = err.to_string();
                if err_string.contains(expected_text) {
                    Ok(())
                } else {
                    Err(CoreError::Validation(format!(
                        "Error '{}' does not contain '{}'", 
                        err_string, expected_text
                    )))
                }
            }
        }
    }
    
    /// Access mock objects
    pub fn mocks(&self) -> crate::mocks::MockBuilder<'_> {
        crate::mocks::MockBuilder::new(self)
    }
    
    /// Access property testing functionality
    pub fn property_tester(&self) -> crate::property_testing::PropertyTester<'_> {
        crate::property_testing::PropertyTester::new(self)
    }
}

// ===== FIXTURE BUILDERS =====

/// Scenario fixtures for common test patterns
pub struct ScenarioFixtures<'ctx> {
    ctx: &'ctx TestContext,
}

impl<'ctx> ScenarioFixtures<'ctx> {
    /// Standard user session with filesystem, terminal, and clipboard events
    pub async fn user_session(&self) -> TestResult<crate::fixtures::FixtureHandle<crate::fixtures::UserSessionFixture>> {
        crate::fixtures::standard_user_session(self.ctx).await
    }
    
    /// User session with custom event count and checkpoint intervals
    pub async fn user_session_with(&self, event_count: usize, checkpoint_interval: usize) -> TestResult<crate::fixtures::FixtureHandle<crate::fixtures::UserSessionFixture>> {
        crate::fixtures::user_session_with_params(self.ctx, event_count, checkpoint_interval).await
    }
    
    /// Pre-populated automaton checkpoints
    pub async fn populated_checkpoints(&self) -> TestResult<crate::fixtures::FixtureHandle<crate::fixtures::PopulatedCheckpointsFixture>> {
        crate::fixtures::populated_checkpoints(self.ctx).await
    }
    
    /// Complex multi-event scenario builder
    pub fn multi_event(&self) -> crate::builders::TestScenarioBuilder {
        crate::builders::TestScenarioBuilder::new()
    }
}

/// Performance fixtures for testing at scale
pub struct PerformanceFixtures<'ctx> {
    ctx: &'ctx TestContext,
}

impl<'ctx> PerformanceFixtures<'ctx> {
    /// Large dataset for performance testing (default 10k events)
    pub async fn large_dataset(&self) -> TestResult<crate::fixtures::FixtureHandle<crate::fixtures::PerformanceDatasetFixture>> {
        crate::fixtures::performance_dataset(self.ctx).await
    }
    
    /// Large dataset with custom size
    pub async fn large_dataset_with(&self, event_count: usize) -> TestResult<crate::fixtures::FixtureHandle<crate::fixtures::PerformanceDatasetFixture>> {
        crate::fixtures::performance_dataset_with_size(self.ctx, event_count).await
    }
    
    /// Pre-warmed database with mixed data types
    pub async fn pre_warmed_db(&self) -> TestResult<crate::fixtures::FixtureHandle<crate::fixtures::PreWarmedFixture>> {
        crate::fixtures::pre_warmed_database(self.ctx).await
    }
}

/// Error testing fixtures for validation and edge cases
pub struct ErrorFixtures<'ctx> {
    ctx: &'ctx TestContext,
}

impl<'ctx> ErrorFixtures<'ctx> {
    /// Invalid events and failed operations for error testing
    pub async fn validation_failures(&self) -> TestResult<crate::fixtures::FixtureHandle<crate::fixtures::ErrorScenariosFixture>> {
        crate::fixtures::error_scenarios(self.ctx).await
    }
    
    /// Empty database for isolation testing
    pub async fn empty_database(&self) -> TestResult<crate::fixtures::FixtureHandle<()>> {
        crate::fixtures::empty_database(self.ctx).await
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
    ) -> TestResult<()>
    where
        T: Send + PartialEq + std::fmt::Debug + Clone,
    {
        crate::channel_behavior_utils::behavior::test_basic_send_receive(
            sender, receiver, test_value, test_name
        ).await
    }
    
    /// Test channel backpressure management
    pub async fn test_backpressure_management<T>(
        &self,
        sender: &impl sinex_channel::ChannelSenderExt<T>,
        test_items: Vec<T>,
        expected_timeout: std::time::Duration,
    ) -> TestResult<()>
    where
        T: Send + Clone,
    {
        crate::channel_behavior_utils::backpressure::test_backpressure_management(
            sender, test_items, expected_timeout
        ).await
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
    pub async fn start_test_ingestd(&self) -> TestResult<crate::satellite_management_utils::TestIngestdHandle> {
        let config = crate::satellite_management_utils::TestIngestdConfig::default();
        crate::satellite_management_utils::start_test_ingestd_with_config(config).await.map_err(Into::into)
    }
    
    /// Start test satellite with configuration
    pub async fn start_test_satellite(
        &self, 
        config: serde_json::Value
    ) -> TestResult<crate::satellite_management_utils::TestSatelliteHandle> {
        crate::satellite_management_utils::TestSatelliteHandle::start(config, self.ctx.pool().clone()).await.map_err(Into::into)
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
    pub async fn create_tester(&self) -> TestResult<crate::deployment_scenario_utils::ConfigCompatibilityTester> {
        crate::deployment_scenario_utils::ConfigCompatibilityTester::new().await.map_err(Into::into)
    }
    
    /// Test environment compatibility
    pub async fn test_environment_compatibility(
        &self,
        _env_type: crate::deployment_scenario_utils::EnvironmentType,
    ) -> TestResult<crate::deployment_scenario_utils::CompatibilityTestResult> {
        let _tester = self.create_tester().await?;
        // This would need implementation in the deployment utils
        todo!("Implementation depends on deployment_scenario_utils having test execution methods")
    }
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
    pub fn build(self) -> TestResult<RawEvent> {
        let source = self.source.ok_or_else(|| CoreError::Validation("Source required".to_string()))?;
        let event_type = self.event_type.ok_or_else(|| CoreError::Validation("Event type required".to_string()))?;
        
        let factory = EventFactory::new(&source);
        let mut event = factory.create_event(&event_type, self.payload);
        
        if let Some(ts) = self.timestamp {
            event.ts_orig = Some(ts);
        }
        
        Ok(event)
    }
    
    /// Build and insert event with validation (most common case)
    pub async fn insert(self) -> TestResult<RawEvent> {
        let ctx = self.ctx;
        let event = self.build()?;
        ctx.insert_event_internal(&event).await
    }
    
    /// Build and insert event without validation (for error testing)
    pub async fn insert_direct(self) -> TestResult<RawEvent> {
        // Use direct query path (bypasses validation like TestQueries)
        let host = gethostname::gethostname().to_string_lossy().to_string();
        use sinex_db::queries::EventQueries;
        
        EventQueries::insert_event(
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
        .await
        .map_err(Into::into)
    }
    
    /// Build and insert multiple copies of this event
    pub async fn insert_batch(self, count: usize) -> TestResult<Vec<RawEvent>> {
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
    pub fn build(self) -> TestResult<RawEvent> {
        self.inner.build()
    }
    
    /// Build and insert
    pub async fn insert(self) -> TestResult<RawEvent> {
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
    pub fn build(self) -> TestResult<RawEvent> {
        self.inner.build()
    }
    
    /// Build and insert  
    pub async fn insert(self) -> TestResult<RawEvent> {
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
        self.inner.event_type = Some("automaton.heartbeat".to_string());
        self.inner.payload["status"] = json!("running");
        self
    }
    
    /// Startup event
    pub fn startup(mut self) -> Self {
        self.inner.event_type = Some("automaton.startup".to_string());
        self.inner.payload["status"] = json!("starting");
        self
    }
    
    /// Error event
    pub fn error(mut self, error_msg: impl Into<String>) -> Self {
        self.inner.event_type = Some("automaton.error".to_string());
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
    pub fn build(self) -> TestResult<RawEvent> {
        self.inner.build()
    }
    
    /// Build and insert
    pub async fn insert(self) -> TestResult<RawEvent> {
        self.inner.insert().await
    }
}

/// Event query builder - abstracts all database operations
pub struct EventQuery<'ctx> {
    ctx: &'ctx TestContext,
    source_filter: Option<String>,
    type_filter: Option<String>,
    id_filter: Option<sinex_ulid::Ulid>,
    after_filter: Option<DateTime<Utc>>,
    limit_value: Option<i64>,
}

impl<'ctx> EventQuery<'ctx> {
    fn new(ctx: &'ctx TestContext) -> Self {
        Self {
            ctx,
            source_filter: None,
            type_filter: None,
            id_filter: None,
            after_filter: None,
            limit_value: None,
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
    
    /// Fetch all matching events
    pub async fn fetch(self) -> TestResult<Vec<RawEvent>> {
        match self.id_filter {
            Some(id) => {
                let event = EventQueries::get_by_id(id)
                    .fetch_optional(self.ctx.pool())
                    .await?;
                Ok(event.into_iter().collect())
            }
            None => {
                if let Some(source) = self.source_filter {
                    EventQueries::get_by_source(source, self.limit_value, None)
                        .fetch_all(self.ctx.pool())
                        .await
                        .map_err(Into::into)
                } else if let Some(event_type) = self.type_filter {
                    EventQueries::get_by_event_type(event_type, self.limit_value, None)
                        .fetch_all(self.ctx.pool())
                        .await
                        .map_err(Into::into)
                } else {
                    EventQueries::get_recent(self.limit_value, None)
                        .fetch_all(self.ctx.pool())
                        .await
                        .map_err(Into::into)
                }
            }
        }
    }
    
    /// Fetch single event
    pub async fn fetch_one(self) -> TestResult<Option<RawEvent>> {
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
    pub async fn count(self) -> TestResult<i64> {
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
            sinex_db::count_events(self.ctx.pool()).await.map_err(Into::into)
        }
    }
}

/// Checkpoint query builder
pub struct CheckpointQuery<'ctx> {
    ctx: &'ctx TestContext,
    automaton_filter: Option<String>,
}

impl<'ctx> CheckpointQuery<'ctx> {
    fn new(ctx: &'ctx TestContext) -> Self {
        Self {
            ctx,
            automaton_filter: None,
        }
    }
    
    /// Filter by automaton name
    pub fn by_automaton(mut self, automaton: impl Into<String>) -> Self {
        self.automaton_filter = Some(automaton.into());
        self
    }
    
    /// Get checkpoint count for automaton
    pub async fn count(self) -> TestResult<i64> {
        if let Some(automaton) = self.automaton_filter {
            let (count,) = CheckpointQueries::count_checkpoints_by_processor(automaton)
                .fetch_one::<(i64,)>(self.ctx.pool())
                .await?;
            Ok(count)
        } else {
            // Count all checkpoints
            let (count,) = QueryBuilder::select("core.automaton_checkpoints")
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
    pub async fn register(&self, source: &str, event_type: &str, schema: Value) -> TestResult<sinex_ulid::Ulid> {
        self.ctx.register_schema(source, event_type, "test.1.0", schema).await
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
    pub async fn validate(&self, event: &RawEvent, schema_id: sinex_ulid::Ulid) -> TestResult<()> {
        self.ctx.validate_against_schema(event, schema_id).await
    }
    
    /// Assert that event validates successfully
    pub async fn assert_valid(&self, event: &RawEvent, schema_id: sinex_ulid::Ulid) -> TestResult<()> {
        self.validate(event, schema_id).await.map_err(|e| {
            CoreError::Validation(format!("Expected event to be valid but validation failed: {}", e))
        })
    }
    
    /// Assert that event validation fails
    pub async fn assert_invalid(&self, event: &RawEvent, schema_id: sinex_ulid::Ulid) -> TestResult<()> {
        match self.validate(event, schema_id).await {
            Ok(()) => Err(CoreError::Validation(format!("Expected event to be invalid but validation passed"))),
            Err(_) => Ok(()) // Validation failed as expected
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
    pub async fn build(self) -> TestResult<RawEvent> {
        let event = self.event_builder.build()?;
        self.ctx.validate_against_schema(&event, self.schema_id).await?;
        Ok(event)
    }
    
    /// Build and insert event with validation
    pub async fn insert(self) -> TestResult<RawEvent> {
        let ctx = self.ctx;
        let event = self.build().await?;
        ctx.insert_event_internal(&event).await
    }
    
    /// Filesystem-specific builder (uses production constants)
    pub fn filesystem(mut self) -> Self {
        self.event_builder = self.event_builder.source(sources::FS).type_(event_types::filesystem::FILE_CREATED);
        self
    }
    
    /// Terminal-specific builder (uses production constants)  
    pub fn terminal(mut self) -> Self {
        self.event_builder = self.event_builder.source(sources::SHELL_KITTY).type_(event_types::shell::COMMAND_EXECUTED);
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
    pub fn eq<T: std::fmt::Debug + PartialEq>(self, actual: &T, expected: &T) -> TestResult<Self> {
        if actual != expected {
            return Err(CoreError::Validation(format!("Assertion failed in '{}': expected {:?}, got {:?}", self.context, expected, actual)));
        }
        Ok(self)
    }
    
    /// Boolean condition assertion with context
    pub fn that(self, condition: bool, message: &str) -> TestResult<Self> {
        if !condition {
            return Err(CoreError::Validation(format!("Assertion failed in '{}': {}", self.context, message)));
        }
        Ok(self)
    }
    
    /// Event-specific equality with field-by-field comparison
    pub fn event_eq(self, actual: &RawEvent, expected: &RawEvent) -> TestResult<Self> {
        // Check each field individually for better error messages
        if actual.source != expected.source {
            return Err(CoreError::Validation(format!("Event source mismatch in '{}': expected '{}', got '{}'", self.context, expected.source, actual.source)));
        }
        
        if actual.event_type != expected.event_type {
            return Err(CoreError::Validation(format!("Event type mismatch in '{}': expected '{}', got '{}'", self.context, expected.event_type, actual.event_type)));
        }
        
        if actual.payload != expected.payload {
            return Err(CoreError::Validation(format!("Event payload mismatch in '{}':\nExpected: {}\nActual: {}", self.context, serde_json::to_string_pretty(&expected.payload).unwrap_or_else(|_| "invalid JSON".to_string()), serde_json::to_string_pretty(&actual.payload).unwrap_or_else(|_| "invalid JSON".to_string()))));
        }
        
        Ok(self)
    }
    
    /// Assert that event insertion succeeds
    pub async fn event_inserts(self, event: &RawEvent) -> TestResult<sinex_ulid::Ulid> {
        match self.ctx.insert_event_internal(event).await {
            Ok(inserted) => Ok(inserted.id),
            Err(e) => Err(CoreError::Validation(format!("Event insertion failed in '{}': {} (source: {}, type: {})", self.context, e, event.source, event.event_type)))
        }
    }
    
    /// Assert that operation completes within timeout
    pub async fn completes_within<F, T>(self, operation: F, timeout: Duration, operation_name: &str) -> TestResult<T>
    where
        F: std::future::Future<Output = TestResult<T>>,
    {
        match tokio::time::timeout(timeout, operation).await {
            Ok(result) => result,
            Err(_) => Err(CoreError::Timeout(format!("Operation '{}' timed out after {:?} in context '{}'", operation_name, timeout, self.context)))
        }
    }
    
    /// Assert error contains specific message
    pub fn error_contains<T>(self, result: &Result<T, CoreError>, expected_message: &str) -> TestResult<Self> {
        match result {
            Ok(_) => Err(CoreError::Validation(format!("Expected error in '{}' but operation succeeded", self.context))),
            Err(e) => {
                let error_message = e.to_string();
                if error_message.contains(expected_message) {
                    Ok(self)
                } else {
                    Err(CoreError::Validation(format!("Error in '{}' did not contain expected message '{}'. Actual error: {}", self.context, expected_message, error_message)))
                }
            }
        }
    }
    
    /// Assert collection has specific size
    pub fn has_size<T>(self, collection: &[T], expected_size: usize) -> TestResult<Self> {
        let actual_size = collection.len();
        if actual_size != expected_size {
            return Err(CoreError::Validation(format!("Collection size mismatch in '{}': expected {}, got {}", self.context, expected_size, actual_size)));
        }
        Ok(self)
    }
    
    /// Assert collection is not empty
    pub fn not_empty<T>(self, collection: &[T]) -> TestResult<Self> {
        if collection.is_empty() {
            return Err(CoreError::Validation(format!("Expected non-empty collection in '{}' but got empty collection", self.context)));
        }
        Ok(self)
    }
    
    /// Assert option contains a value
    pub fn some<T>(self, option: &Option<T>) -> TestResult<Self> {
        if option.is_none() {
            return Err(CoreError::Validation(format!("Expected Some(_) in '{}' but got None", self.context)));
        }
        Ok(self)
    }
    
    /// Assert option is None
    pub fn none<T>(self, option: &Option<T>) -> TestResult<Self> {
        if option.is_some() {
            return Err(CoreError::Validation(format!("Expected None in '{}' but got Some(_)", self.context)));
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
    pub fn build(self) -> TestResult<RawEvent> {
        self.inner.build()
    }
    
    /// Build and insert
    pub async fn insert(self) -> TestResult<RawEvent> {
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
    pub fn build(self) -> TestResult<RawEvent> {
        self.inner.build()
    }
    
    /// Build and insert
    pub async fn insert(self) -> TestResult<RawEvent> {
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
    pub fn build(self) -> TestResult<RawEvent> {
        self.inner.build()
    }
    
    /// Build and insert
    pub async fn insert(self) -> TestResult<RawEvent> {
        self.inner.insert().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sinex_test;
    use serde_json::json;
    
    #[sinex_test]
    async fn test_contexts_are_isolated(ctx: TestContext) -> TestResult<()> {
        // Create another context
        let ctx2 = TestContext::with_name("isolation_test").await?;
        
        // Insert event in first context
        let event1 = ctx.event()
            .source("ctx1")
            .type_("isolation.test")
            .field("context", "first")
            .insert()
            .await?;
        
        // Insert event in second context
        let event2 = ctx2.event()
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
    async fn test_event_builder_fluent_api(ctx: TestContext) -> TestResult<()> {
        // Test all builder methods work correctly
        let event = ctx.event()
            .source("test_source")
            .type_("test.type")
            .field("string", "value")
            .field("number", 42)
            .field("boolean", true)
            .fields(vec![
                ("array", json!([1, 2, 3])),
                ("object", json!({"nested": "value"}))
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
    async fn test_domain_specific_builders(ctx: TestContext) -> TestResult<()> {
        // Test filesystem builder
        let fs_event = ctx.event()
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
        let term_event = ctx.event()
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
        let agent_event = ctx.event()
            .agent()
            .name("test-automaton")
            .version("1.0.0")
            .uptime_seconds(3600)
            .events_processed(1000)
            .heartbeat()
            .insert()
            .await?;
        
        assert_eq!(agent_event.source, sources::SINEX);
        assert_eq!(agent_event.event_type, "automaton.heartbeat");
        assert_eq!(agent_event.payload["agent_name"], json!("test-automaton"));
        assert_eq!(agent_event.payload["version"], json!("1.0.0"));
        
        Ok(())
    }
    
    #[sinex_test]
    async fn test_query_builder_chains(ctx: TestContext) -> TestResult<()> {
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
        let even_type_a = ctx.events()
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
    async fn test_assertion_api(ctx: TestContext) -> TestResult<()> {
        // Test successful assertions
        ctx.assert("basic equality").eq(&5, &5)?;
        ctx.assert("condition").that(10 > 5, "10 should be greater than 5")?;
        
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
        assert!(fail_result.unwrap_err().to_string().contains("expected 10, got 5"));
        
        Ok(())
    }
    
    #[sinex_test]
    async fn test_wait_helpers(ctx: TestContext) -> TestResult<()> {
        // Test wait_for_event_count
        let handle = tokio::spawn(async move {
            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
            for i in 0..5 {
                ctx.event()
                    .source("async")
                    .type_("test")
                    .field("index", i)
                    .insert()
                    .await
                    .unwrap();
            }
        });
        
        // Wait for events to appear
        ctx.wait_for_event_count(5).await?;
        
        // Verify they're there
        let count = ctx.events().count().await?;
        assert_eq!(count, 5);
        
        handle.await.map_err(|e| CoreError::Service(format!("Task failed: {}", e)))?;
        
        Ok(())
    }
    
    #[sinex_test]
    async fn test_batch_operations(ctx: TestContext) -> TestResult<()> {
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
            .map(|i| ctx.event()
                .source("pre_built")
                .type_("test")
                .field("index", i)
                .build()
                .unwrap())
            .collect();
        
        let inserted = ctx.insert_events(&pre_built).await?;
        assert_eq!(inserted.len(), 3);
        
        Ok(())
    }
    
    #[sinex_test]
    async fn test_schema_validation(ctx: TestContext) -> TestResult<()> {
        // Register a test schema
        let schema = json!({
            "type": "object",
            "properties": {
                "required_field": {"type": "string"},
                "optional_field": {"type": "number"}
            },
            "required": ["required_field"]
        });
        
        let schema_id = ctx.register_schema("test", "validated.event", "1.0", schema).await?;
        
        // Create valid event
        let valid_event = ctx.event()
            .source("test")
            .type_("validated.event")
            .field("required_field", "present")
            .field("optional_field", 42)
            .build()?;
        
        // Should validate successfully
        ctx.validate_against_schema(&valid_event, schema_id).await?;
        
        // Create invalid event (missing required field)
        let invalid_event = ctx.event()
            .source("test")
            .type_("validated.event")
            .field("optional_field", 42)
            .build()?;
        
        // Should fail validation
        let result = ctx.validate_against_schema(&invalid_event, schema_id).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("required"));
        
        // Test validated event builder
        let validated = ctx.validated_event(schema_id)
            .source("test")
            .type_("validated.event")
            .field("required_field", "valid")
            .insert()
            .await?;
        
        assert_eq!(validated.payload["required_field"], json!("valid"));
        
        Ok(())
    }
    
    #[sinex_test]
    async fn test_timing_utilities(ctx: TestContext) -> TestResult<()> {
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
        sync_clone.wait().await.map_err(|_| CoreError::Timeout("Sync wait failed".to_string()))?;
        
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
        let h1 = tokio::spawn(async move {
            b1.wait(Duration::from_secs(1)).await
        });
        
        let b2 = barrier_clone.clone();
        let h2 = tokio::spawn(async move {
            b2.wait(Duration::from_secs(1)).await
        });
        
        // Both should complete successfully
        h1.await.map_err(|e| CoreError::Service(format!("Task 1 failed: {}", e)))??;
        h2.await.map_err(|e| CoreError::Service(format!("Task 2 failed: {}", e)))??;
        
        Ok(())
    }
    
    #[sinex_test]
    async fn test_error_handling_in_builders(ctx: TestContext) -> TestResult<()> {
        // Test empty source validation
        let result = ctx.event()
            .source("")
            .type_("test")
            .insert()
            .await;
        assert!(result.is_err());
        
        // Test empty type validation
        let result = ctx.event()
            .source("test")
            .type_("")
            .insert()
            .await;
        assert!(result.is_err());
        
        // Test insert_direct bypasses validation
        let event = ctx.event()
            .source("") // Would normally fail
            .type_("test")
            .insert_direct()
            .await;
        // This might succeed or fail depending on database constraints
        // The point is it bypasses our validation layer
        
        Ok(())
    }
    
    #[sinex_test]
    async fn test_concurrent_helpers(ctx: TestContext) -> TestResult<()> {
        // Test run_concurrent
        let results = ctx.run_concurrent(3, |ctx, i| async move {
            // Each task inserts an event
            let event = ctx.event()
                .source("concurrent")
                .type_("test")
                .field("task_id", i)
                .insert()
                .await?;
            Ok(event.id)
        }).await?;
        
        assert_eq!(results.len(), 3);
        
        // All events should be in original context
        let events = ctx.events().by_source("concurrent").fetch().await?;
        assert_eq!(events.len(), 3);
        
        Ok(())
    }
    
    #[sinex_test]
    async fn test_measure_helper(ctx: TestContext) -> TestResult<()> {
        let (result, duration) = ctx.measure(async {
            tokio::time::sleep(Duration::from_millis(50)).await;
            ctx.event().source("measured").type_("test").insert().await
        }).await?;
        
        assert!(duration >= Duration::from_millis(50));
        assert!(duration < Duration::from_millis(200)); // Reasonable upper bound
        assert!(result.is_ok());
        
        Ok(())
    }
}
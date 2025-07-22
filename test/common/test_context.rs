// Unified Test Context - Single Entry Point for All Test Operations
//
// QUICK START - Everything you need for testing:
//
// 1. CREATE EVENTS:
// ```rust
// // Filesystem events with fluent API:
// let event = ctx.event().filesystem()
//     .path("/tmp/test.txt")
//     .size(1024)
//     .created()                      // or .modified(), .deleted()
//     .insert().await?;
//
// // Terminal/shell commands:
// let cmd = ctx.event().terminal()
//     .command("ls -la")
//     .success()                      // or .failed(), .exit_code(1)
//     .insert().await?;
//
// // Custom events (build fields incrementally):
// let custom = ctx.event()
//     .source("my_service")
//     .type_("service.started")
//     .field("version", "1.0.0")
//     .field("port", 8080)
//     .insert().await?;
// ```
//
// 2. QUERY EVENTS:
// ```rust
// // Find events by various criteria:
// let recent = ctx.events().limit(10).fetch().await?;
// let fs_events = ctx.events().by_source(sources::FS).fetch().await?;
// let count = ctx.events().by_type(event_types::filesystem::FILE_CREATED).count().await?;
//
// // Time-based queries:
// let since_noon = ctx.events().after(timestamp).fetch().await?;
// ```
//
// 3. RICH ASSERTIONS (get clear error messages):
// ```rust
// // Basic assertions with context:
// ctx.assert("file creation test")
//     .eq(&actual.path, "/expected/path")?
//     .that(event.size > 0, "file should have content")?;
//
// // Event-specific assertions:
// ctx.assert("event comparison").event_eq(&actual, &expected)?;
//
// // Database assertions:
// let id = ctx.assert("insertion test").event_inserts(&event).await?;
// ```
//
// 4. SCHEMA VALIDATION:
// ```rust
// // Register and use schemas:
// let schema_id = ctx.register_schema(
//     sources::FS, 
//     event_types::filesystem::FILE_CREATED, 
//     "1.0", 
//     schema
// ).await?;
// ctx.validate_against_schema(&event, schema_id).await?;
//
// // Create validated events (validates before insertion):
// let validated = ctx.validated_event(schema_id)
//     .filesystem().path("/test").insert().await?;
// ```
//
// 5. TIMING & COORDINATION:
// ```rust
// // Wait for events to appear:
// ctx.timing().wait_for_event_count(5).await?;
// ctx.timing().wait_for_source_events(sources::FS, 3).await?;
//
// // Coordinate multiple operations:
// let barrier = ctx.timing().barrier(3);  // Wait for 3 participants
// let counter = ctx.timing().event_counter(100);  // Count to 100
// ```
//
// 6. TEST FIXTURES (automatic setup/cleanup):
// ```rust
// // Pre-built test data (cached across tests):
// let session = ctx.standard_user_session().await?;
// let dataset = ctx.performance_dataset().await?;
//
// // Custom fixtures:
// let checkpoint = ctx.checkpoint("test-automaton")
//     .with_processed_count(100)
//     .insert(pool).await?;
// ```

use crate::common::database_pool::TestDatabase;
use sinex_core_types::{DbPoolRef, RawEvent};
use sinex_db::queries::{EventQueries, CheckpointQueries};
use sinex_db::query_builder::{QueryBuilder, QueryParam};
use sinex_events::{EventFactory, sources, event_types};
use sinex_error::{CoreError, ErrorContext, ResultExt};
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use std::sync::Arc;
use anyhow::Result as AnyhowResult;
use serde_json::{Value, json};
use chrono::{DateTime, Utc};

pub type TestResult<T = ()> = AnyhowResult<T>;

/// Configuration for test behavior
#[derive(Debug, Clone)]
pub struct TestConfig {
    pub test_name: String,
    pub default_timeout: Duration,
    pub verbose: bool,
}

impl Default for TestConfig {
    fn default() -> Self {
        Self {
            test_name: "unnamed_test".to_string(),
            default_timeout: Duration::from_secs(3),
            verbose: false,
        }
    }
}

/// Unified test context - single entry point for all test operations
pub struct TestContext {
    db: TestDatabase,
    config: TestConfig,
    start_time: Instant,
    created_events: Arc<Mutex<Vec<sinex_ulid::Ulid>>>,
}

impl TestContext {
    /// Create new test context with default config
    pub async fn new() -> TestResult<Self> {
        Self::with_config(TestConfig::default()).await
    }
    
    /// Create test context with custom config
    pub async fn with_config(config: TestConfig) -> TestResult<Self> {
        let db = crate::common::database_pool::acquire_test_database().await?;
        
        Ok(Self {
            db,
            config,
            start_time: Instant::now(),
            created_events: Arc::new(Mutex::new(Vec::new())),
        })
    }
    
    /// Get pool reference (internal use only)
    fn pool(&self) -> DbPoolRef<'_> {
        self.db.pool()
    }
    
    /// Get test configuration
    pub fn config(&self) -> &TestConfig {
        &self.config
    }
    
    /// Get test name for fixture scoping
    pub fn test_name(&self) -> &str {
        &self.config.test_name
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
        let inserted = sinex_db::insert_event_with_validator(self.pool(), event, None).await?;
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
    pub async fn wait_for_event_count(&self, expected: usize) -> TestResult {
        let timeout_secs = self.config.default_timeout.as_secs();
        
        sinex_core_utils::wait_for_condition_adaptive(
            || async {
                let count = self.events().count().await? as usize;
                Ok(count >= expected)
            },
            timeout_secs,
            &format!("event count >= {}", expected)
        ).await
        .map_err(|e| 
            CoreError::timeout(&format!("Wait for {} events failed", expected))
                .with_context("timeout_duration", format!("{}s", timeout_secs))
                .with_source(e)
                .with_operation("wait_for_event_count")
                .build()
                .into()
        )
    }
    
    /// Wait for condition to become true using production wait helpers
    pub async fn wait_for_condition<F, Fut>(&self, condition: F) -> TestResult
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = TestResult<bool>>,
    {
        let timeout_secs = self.config.default_timeout.as_secs();
        
        sinex_core_utils::wait_for_condition_adaptive(
            || async {
                match condition().await {
                    Ok(result) => Ok(result),
                    Err(e) => Err(sinex_core_types::CoreError::Unknown(e.to_string())),
                }
            },
            timeout_secs,
            "custom test condition"
        ).await
        .map_err(|e|
            CoreError::timeout("Wait for test condition failed")
                .with_context("timeout_duration", format!("{}s", timeout_secs))
                .with_source(e)
                .with_operation("wait_for_condition")
                .build()
                .into()
        )
    }
    
    // ===== ASSERTION HELPERS =====
    
    /// Assert specific event count using production error context
    pub async fn assert_event_count(&self, expected: usize) -> TestResult {
        let actual = self.events().count().await? as usize;
        if actual != expected {
            return Err(
                CoreError::validation("Event count assertion failed")
                    .with_context("expected_count", expected)
                    .with_context("actual_count", actual)
                    .with_context("test_name", &self.config.test_name)
                    .with_operation("assert_event_count")
                    .build()
                    .into()
            );
        }
        Ok(())
    }
    
    /// Assert no events exist
    pub async fn assert_no_events(&self) -> TestResult {
        self.assert_event_count(0).await
    }
    
    /// Assert event with ID exists
    pub async fn assert_event_exists(&self, id: sinex_ulid::Ulid) -> TestResult {
        let event = self.events().by_id(id).fetch_one().await?;
        if event.is_none() {
            return Err(anyhow::anyhow!("Event {} does not exist", id));
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
    pub async fn register_schema(&self, source: &str, event_type: &str, version: &str, schema: Value) -> TestResult<sinex_ulid::Ulid> {
        use sinex_db::queries::SchemaQueries;
        let schema_id = sinex_ulid::Ulid::new();
        
        SchemaQueries::insert_schema(
            schema_id,
            source.to_string(),
            event_type.to_string(),
            version.to_string(),
            schema,
            chrono::Utc::now()
        ).execute(self.pool()).await.map_err(|e| {
            anyhow::anyhow!("Failed to register test schema for {}/{}: {}", source, event_type, e)
        })?;
        
        Ok(schema_id)
    }
    
    /// Validate event against registered schema
    pub async fn validate_against_schema(&self, event: &RawEvent, schema_id: sinex_ulid::Ulid) -> TestResult<()> {
        // Get schema from database using production query
        let schema_record = sinex_db::queries::SchemaQueries::get_by_id(schema_id)
            .fetch_optional(self.pool())
            .await.map_err(|e| {
                anyhow::anyhow!("Failed to fetch schema {}: {}", schema_id, e)
            })?
            .ok_or_else(|| anyhow::anyhow!("Schema not found: {}", schema_id))?;
        
        // Validate using jsonschema crate
        let schema = jsonschema::JSONSchema::compile(&schema_record.definition)
            .map_err(|e| anyhow::anyhow!("Invalid JSON schema {}: {}", schema_id, e))?;
        
        let validation_result = schema.validate(&event.payload);
        if let Err(errors) = validation_result {
            let error_messages: Vec<String> = errors
                .map(|e| format!("  - {}: {}", e.instance_path, e))
                .collect();
            return Err(anyhow::anyhow!(
                "Schema validation failed for event {}\nSchema ID: {}\nErrors:\n{}",
                event.id,
                schema_id,
                error_messages.join("\n")
            ));
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
    pub fn timing(&self) -> crate::common::timing_utils::TimingUtils<'_> {
        crate::common::timing_utils::TimingUtils::new(self)
    }
}

// ===== FIXTURE BUILDERS =====

/// Scenario fixtures for common test patterns
pub struct ScenarioFixtures<'ctx> {
    ctx: &'ctx TestContext,
}

impl<'ctx> ScenarioFixtures<'ctx> {
    /// Standard user session with filesystem, terminal, and clipboard events
    pub async fn user_session(&self) -> TestResult<crate::common::fixtures::FixtureHandle<crate::common::fixtures::UserSessionFixture>> {
        crate::common::fixtures::standard_user_session(self.ctx).await
    }
    
    /// User session with custom event count and checkpoint intervals
    pub async fn user_session_with(&self, event_count: usize, checkpoint_interval: usize) -> TestResult<crate::common::fixtures::FixtureHandle<crate::common::fixtures::UserSessionFixture>> {
        crate::common::fixtures::user_session_with_params(self.ctx, event_count, checkpoint_interval).await
    }
    
    /// Pre-populated automaton checkpoints
    pub async fn populated_checkpoints(&self) -> TestResult<crate::common::fixtures::FixtureHandle<crate::common::fixtures::PopulatedCheckpointsFixture>> {
        crate::common::fixtures::populated_checkpoints(self.ctx).await
    }
    
    /// Complex multi-event scenario builder
    pub fn multi_event(&self) -> crate::common::builders::TestScenarioBuilder {
        crate::common::builders::TestScenarioBuilder::new()
    }
}

/// Performance fixtures for testing at scale
pub struct PerformanceFixtures<'ctx> {
    ctx: &'ctx TestContext,
}

impl<'ctx> PerformanceFixtures<'ctx> {
    /// Large dataset for performance testing (default 10k events)
    pub async fn large_dataset(&self) -> TestResult<crate::common::fixtures::FixtureHandle<crate::common::fixtures::PerformanceDatasetFixture>> {
        crate::common::fixtures::performance_dataset(self.ctx).await
    }
    
    /// Large dataset with custom size
    pub async fn large_dataset_with(&self, event_count: usize) -> TestResult<crate::common::fixtures::FixtureHandle<crate::common::fixtures::PerformanceDatasetFixture>> {
        crate::common::fixtures::performance_dataset_with_size(self.ctx, event_count).await
    }
    
    /// Pre-warmed database with mixed data types
    pub async fn pre_warmed_db(&self) -> TestResult<crate::common::fixtures::FixtureHandle<crate::common::fixtures::PreWarmedFixture>> {
        crate::common::fixtures::pre_warmed_database(self.ctx).await
    }
}

/// Error testing fixtures for validation and edge cases
pub struct ErrorFixtures<'ctx> {
    ctx: &'ctx TestContext,
}

impl<'ctx> ErrorFixtures<'ctx> {
    /// Invalid events and failed operations for error testing
    pub async fn validation_failures(&self) -> TestResult<crate::common::fixtures::FixtureHandle<crate::common::fixtures::ErrorScenariosFixture>> {
        crate::common::fixtures::error_scenarios(self.ctx).await
    }
    
    /// Empty database for isolation testing
    pub async fn empty_database(&self) -> TestResult<crate::common::fixtures::FixtureHandle<()>> {
        crate::common::fixtures::empty_database(self.ctx).await
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
    ) -> TestResult
    where
        T: Send + PartialEq + std::fmt::Debug,
    {
        crate::common::channel_behavior_utils::behavior::test_basic_send_receive(
            sender, receiver, test_value, test_name
        ).await
    }
    
    /// Test channel backpressure management
    pub async fn test_backpressure_management<T>(
        &self,
        sender: &impl sinex_channel::ChannelSenderExt<T>,
        test_items: Vec<T>,
        expected_timeout: std::time::Duration,
    ) -> TestResult
    where
        T: Send + Clone,
    {
        crate::common::channel_behavior_utils::backpressure::test_backpressure_management(
            sender, test_items, expected_timeout
        ).await
    }
    
    /// Create test channel setup with monitoring
    pub fn setup<T>(&self) -> crate::common::channel_behavior_utils::TestChannelSetup<T> {
        crate::common::channel_behavior_utils::TestChannelSetup::new(100)
    }
}

/// Process management testing utilities  
pub struct ProcessTestUtils<'ctx> {
    ctx: &'ctx TestContext,
}

impl<'ctx> ProcessTestUtils<'ctx> {
    /// Start test ingestd with default configuration
    pub async fn start_test_ingestd(&self) -> TestResult<crate::common::satellite_management_utils::TestIngestdHandle> {
        let config = crate::common::satellite_management_utils::TestIngestdConfig::default();
        crate::common::satellite_management_utils::start_test_ingestd_with_config(config).await.map_err(Into::into)
    }
    
    /// Start test satellite with configuration
    pub async fn start_test_satellite(
        &self, 
        config: serde_json::Value
    ) -> TestResult<crate::common::satellite_management_utils::TestSatelliteHandle> {
        crate::common::satellite_management_utils::TestSatelliteHandle::start(config, self.ctx.pool().clone()).await.map_err(Into::into)
    }
    
    /// Create satellite configuration
    pub fn satellite_config(&self, service_name: &str, socket_path: &str) -> serde_json::Value {
        crate::common::satellite_management_utils::create_test_satellite_config(service_name, socket_path)
    }
}

/// Deployment scenario testing utilities
pub struct DeploymentTestUtils<'ctx> {
    ctx: &'ctx TestContext,
}

impl<'ctx> DeploymentTestUtils<'ctx> {
    /// Create deployment scenario tester
    pub async fn create_tester(&self) -> TestResult<crate::common::deployment_scenario_utils::ConfigCompatibilityTester> {
        crate::common::deployment_scenario_utils::ConfigCompatibilityTester::new().await.map_err(Into::into)
    }
    
    /// Test environment compatibility
    pub async fn test_environment_compatibility(
        &self,
        env_type: crate::common::deployment_scenario_utils::EnvironmentType,
    ) -> TestResult<crate::common::deployment_scenario_utils::CompatibilityTestResult> {
        let mut tester = self.create_tester().await?;
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
    
    /// Build event without inserting (like old TestEventBuilder)
    pub fn build(self) -> TestResult<RawEvent> {
        let source = self.source.ok_or_else(|| anyhow::anyhow!("Source required"))?;
        let event_type = self.event_type.ok_or_else(|| anyhow::anyhow!("Event type required"))?;
        
        let factory = EventFactory::new(&source);
        let mut event = factory.create_event(&event_type, self.payload);
        
        if let Some(ts) = self.timestamp {
            event.ts_orig = Some(ts);
        }
        
        Ok(event)
    }
    
    /// Build and insert event with validation (most common case)
    pub async fn insert(self) -> TestResult<RawEvent> {
        let event = self.build()?;
        self.ctx.insert_event_internal(&event).await
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
            self.payload.unwrap_or_else(|| json!({})),
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
            
            let inserted = self.ctx.insert_event_internal(&event).await?;
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
            anyhow::anyhow!("Expected event to be valid but validation failed: {}", e)
        })
    }
    
    /// Assert that event validation fails
    pub async fn assert_invalid(&self, event: &RawEvent, schema_id: sinex_ulid::Ulid) -> TestResult<()> {
        match self.validate(event, schema_id).await {
            Ok(()) => Err(anyhow::anyhow!("Expected event to be invalid but validation passed")),
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
        let event = self.build().await?;
        self.ctx.insert_event_internal(&event).await
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
            return Err(anyhow::anyhow!(
                "Assertion failed in '{}': expected {:?}, got {:?}", 
                self.context, expected, actual
            ));
        }
        Ok(self)
    }
    
    /// Boolean condition assertion with context
    pub fn that(self, condition: bool, message: &str) -> TestResult<Self> {
        if !condition {
            return Err(anyhow::anyhow!(
                "Assertion failed in '{}': {}", 
                self.context, message
            ));
        }
        Ok(self)
    }
    
    /// Event-specific equality with field-by-field comparison
    pub fn event_eq(self, actual: &RawEvent, expected: &RawEvent) -> TestResult<Self> {
        // Check each field individually for better error messages
        if actual.source != expected.source {
            return Err(anyhow::anyhow!(
                "Event source mismatch in '{}': expected '{}', got '{}'",
                self.context, expected.source, actual.source
            ));
        }
        
        if actual.event_type != expected.event_type {
            return Err(anyhow::anyhow!(
                "Event type mismatch in '{}': expected '{}', got '{}'",
                self.context, expected.event_type, actual.event_type
            ));
        }
        
        if actual.payload != expected.payload {
            return Err(anyhow::anyhow!(
                "Event payload mismatch in '{}':\nExpected: {}\nActual: {}",
                self.context, 
                serde_json::to_string_pretty(&expected.payload).unwrap_or_else(|_| "invalid JSON".to_string()),
                serde_json::to_string_pretty(&actual.payload).unwrap_or_else(|_| "invalid JSON".to_string())
            ));
        }
        
        Ok(self)
    }
    
    /// Assert that event insertion succeeds
    pub async fn event_inserts(self, event: &RawEvent) -> TestResult<sinex_ulid::Ulid> {
        match self.ctx.insert_event_internal(event).await {
            Ok(inserted) => Ok(inserted.id),
            Err(e) => Err(anyhow::anyhow!(
                "Event insertion failed in '{}': {} (source: {}, type: {})",
                self.context, e, event.source, event.event_type
            ))
        }
    }
    
    /// Assert that operation completes within timeout
    pub async fn completes_within<F, T>(self, operation: F, timeout: Duration, operation_name: &str) -> TestResult<T>
    where
        F: std::future::Future<Output = TestResult<T>>,
    {
        match tokio::time::timeout(timeout, operation).await {
            Ok(result) => result,
            Err(_) => Err(anyhow::anyhow!(
                "Operation '{}' timed out after {:?} in context '{}'", 
                operation_name, timeout, self.context
            ))
        }
    }
    
    /// Assert error contains specific message
    pub fn error_contains<T>(self, result: &Result<T, anyhow::Error>, expected_message: &str) -> TestResult<Self> {
        match result {
            Ok(_) => Err(anyhow::anyhow!(
                "Expected error containing '{}' in '{}' but operation succeeded",
                expected_message, self.context
            )),
            Err(e) => {
                let error_message = e.to_string();
                if error_message.contains(expected_message) {
                    Ok(self)
                } else {
                    Err(anyhow::anyhow!(
                        "Error message mismatch in '{}': expected to contain '{}', got '{}'",
                        self.context, expected_message, error_message
                    ))
                }
            }
        }
    }
    
    /// Assert collection has specific size
    pub fn has_size<T>(self, collection: &[T], expected_size: usize) -> TestResult<Self> {
        let actual_size = collection.len();
        if actual_size != expected_size {
            return Err(anyhow::anyhow!(
                "Collection size mismatch in '{}': expected {}, got {}",
                self.context, expected_size, actual_size
            ));
        }
        Ok(self)
    }
    
    /// Assert collection is not empty
    pub fn not_empty<T>(self, collection: &[T]) -> TestResult<Self> {
        if collection.is_empty() {
            return Err(anyhow::anyhow!(
                "Expected non-empty collection in '{}' but got empty collection",
                self.context
            ));
        }
        Ok(self)
    }
    
    /// Assert option contains a value
    pub fn some<T>(self, option: &Option<T>) -> TestResult<Self> {
        if option.is_none() {
            return Err(anyhow::anyhow!(
                "Expected Some value in '{}' but got None",
                self.context
            ));
        }
        Ok(self)
    }
    
    /// Assert option is None
    pub fn none<T>(self, option: &Option<T>) -> TestResult<Self> {
        if option.is_some() {
            return Err(anyhow::anyhow!(
                "Expected None in '{}' but got Some value",
                self.context
            ));
        }
        Ok(self)
    }
}
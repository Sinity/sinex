//! Unified test context for Sinex tests
//!
//! Provides a comprehensive testing context that encapsulates:
//! - Database connection using the universal pool system
//! - Event builder factories for consistent event creation
//! - Timing helpers to eliminate flaky sleeps
//! - Common test utilities in one ergonomic interface
//!
//! # Usage
//! ```rust
//! use crate::common::test_context::TestContext;
//!
//! #[sinex_test]
//! async fn my_test(ctx: TestContext) -> TestResult {
//!     let event = ctx.filesystem_event("/test/file");
//!     ctx.insert_event(&event).await?;
//!     ctx.wait_for_event_count(1).await?;
//!     Ok(())
//! }
//! ```

use crate::common::db_pool_final::TestDatabase;
use crate::common::event_builders::{EventBuilder, GenericEventBuilder};
use crate::common::prelude::*;
use crate::common::timing_optimization::wait_helpers::{
    wait_for_condition_or_timeout, wait_for_event_count, wait_for_filtered_event_count,
    wait_for_work_queue_count,
};
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

/// Configuration for test context behavior
#[derive(Debug, Clone)]
pub struct TestConfig {
    /// Maximum time to wait for conditions
    pub default_timeout: Duration,
    /// Pool size for database connections
    pub pool_size: u32,
    /// Enable verbose logging for debugging
    pub verbose: bool,
    /// Test name for identification
    pub test_name: String,
}

impl Default for TestConfig {
    fn default() -> Self {
        Self {
            default_timeout: Duration::from_secs(3), // Reduced from 5s for faster tests
            pool_size: 5,
            verbose: false,
            test_name: "unnamed_test".to_string(),
        }
    }
}

/// Unified test context providing all common test functionality
pub struct TestContext {
    /// Database from the managed pool
    db: TestDatabase,
    /// Test configuration
    config: TestConfig,
    /// Test start time for diagnostics
    start_time: Instant,
    /// Track events created in this test
    created_events: Arc<Mutex<Vec<Ulid>>>,
}

impl TestContext {
    /// Create a new test context with default configuration
    pub async fn new() -> Result<Self> {
        Self::with_config(TestConfig::default()).await
    }

    /// Create a new test context with custom configuration
    pub async fn with_config(config: TestConfig) -> Result<Self> {
        let db = crate::common::db_pool_final::acquire_test_database().await?;

        Ok(Self {
            db,
            config,
            start_time: Instant::now(),
            created_events: Arc::new(Mutex::new(Vec::new())),
        })
    }

    /// Create a test context with a managed database (used by #[sinex_test])
    pub async fn with_managed_database(db: TestDatabase, config: TestConfig) -> Result<Self> {
        Ok(Self {
            db,
            config,
            start_time: Instant::now(),
            created_events: Arc::new(Mutex::new(Vec::new())),
        })
    }

    /// Get the database pool
    pub fn pool(&self) -> &DbPool {
        self.db.pool()
    }

    /// Get the test name
    pub fn test_name(&self) -> &str {
        &self.config.test_name
    }

    /// Get elapsed time since test start
    pub fn elapsed(&self) -> Duration {
        self.start_time.elapsed()
    }

    /// Get the default timeout for operations
    pub fn default_timeout(&self) -> Duration {
        self.config.default_timeout
    }

    /// Check if verbose logging is enabled
    pub fn is_verbose(&self) -> bool {
        self.config.verbose
    }

    /// Get the entire test configuration (for cloning)
    pub fn config(&self) -> &TestConfig {
        &self.config
    }

    // ===== Database Operations =====

    /// Insert an event into the database
    pub async fn insert_event(&self, event: &RawEvent) -> TestResult {
        queries::insert_event(self.pool(), event).await?;
        self.created_events.lock().await.push(event.id);
        Ok(())
    }

    /// Insert multiple events
    pub async fn insert_events(&self, events: &[RawEvent]) -> TestResult {
        for event in events {
            self.insert_event(event).await?;
        }
        Ok(())
    }

    /// Query recent events
    pub async fn query_events(&self) -> Result<Vec<DbRawEvent>> {
        queries::get_recent_events(self.pool(), 1000).await
    }

    /// Query events by source
    pub async fn query_events_by_source(&self, source: &str) -> Result<Vec<DbRawEvent>> {
        queries::get_events_by_source(self.pool(), source, 1000).await
    }

    /// Get count of events
    pub async fn event_count(&self) -> Result<i64> {
        let count = sqlx::query_scalar!("SELECT COUNT(*) FROM raw.events")
            .fetch_one(self.pool())
            .await?;
        Ok(count.unwrap_or(0))
    }

    /// Get count of events created in this test
    pub async fn test_event_count(&self) -> usize {
        self.created_events.lock().await.len()
    }

    // ===== Event Building =====

    /// Create a generic event builder with source and type
    pub fn event_builder(&self, source: &str, event_type: &str) -> GenericEventBuilder {
        EventBuilder::generic(source, event_type)
    }

    /// Create a filesystem event
    pub fn filesystem_event(&self, path: &str) -> RawEvent {
        EventBuilder::filesystem().path(path).created().build()
    }

    /// Create a terminal event
    pub fn terminal_event(&self, command: &str) -> RawEvent {
        EventBuilder::terminal().command(command).success().build()
    }

    /// Create a clipboard event
    pub fn clipboard_event(&self, content: &str) -> RawEvent {
        EventBuilder::clipboard().text(content).build()
    }

    /// Create a window manager event
    pub fn hyprland_event(&self, event_type: &str, data: Value) -> RawEvent {
        let builder = EventBuilder::hyprland();

        // Map common event types to builder methods
        let builder = match event_type {
            "window.created" => builder.window_created(),
            "window.destroyed" => builder.window_destroyed(),
            "window.focused" => builder.window_focused(),
            _ => builder.event_type(crate::common::event_builders::HyprlandEventType::Custom(
                event_type.to_string(),
            )),
        };

        builder.custom_data(data).build()
    }

    // ===== Timing Helpers =====

    /// Wait for a specific number of events to exist
    pub async fn wait_for_event_count(&self, expected: usize) -> TestResult {
        wait_for_event_count(
            self.pool(),
            expected as i64,
            self.config.default_timeout.as_secs(),
        )
        .await?;
        Ok(())
    }

    /// Wait for events from a specific source
    pub async fn wait_for_source_events(&self, source: &str, count: usize) -> TestResult {
        wait_for_filtered_event_count(
            self.pool(),
            "source = $1",
            &[source],
            count as i64,
            self.config.default_timeout.as_secs(),
        )
        .await?;
        Ok(())
    }

    /// Wait for a condition to become true
    pub async fn wait_for_condition<F, Fut>(&self, condition: F) -> Result<()>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<bool>>,
    {
        wait_for_condition_or_timeout(condition, self.config.default_timeout.as_secs()).await?;
        Ok(())
    }

    /// Wait a short time for processing (replaces arbitrary sleeps)
    pub async fn wait_for_processing(&self) -> TestResult {
        // Smart wait that checks for activity with faster polling
        let initial_count = self.event_count().await?;
        tokio::time::sleep(Duration::from_millis(5)).await; // Reduced from 10ms

        // If events are still being created, wait a bit more
        let new_count = self.event_count().await?;
        if new_count > initial_count {
            // Use a more robust approach for closure capture
            let pool = self.pool().clone();
            let final_count = new_count;

            self.wait_for_condition(move || {
                let pool = pool.clone();
                async move {
                    tokio::time::sleep(Duration::from_millis(5)).await; // Reduced from 10ms
                    let count = sqlx::query_scalar!("SELECT COUNT(*) FROM raw.events")
                        .fetch_one(&pool)
                        .await
                        .map(|c| c.unwrap_or(0))
                        .unwrap_or(0);
                    Ok(count == final_count)
                }
            })
            .await?;
        }

        Ok(())
    }

    /// Wait for work queue to reach expected count
    pub async fn wait_for_work_queue(&self, expected: usize) -> TestResult {
        wait_for_work_queue_count(
            self.pool(),
            expected as i64,
            self.config.default_timeout.as_secs(),
        )
        .await?;
        Ok(())
    }

    // ===== Test Helpers =====

    /// Run a test step with timing and logging
    pub async fn run_step<F, Fut, T>(&self, step_name: &str, f: F) -> Result<T>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        if self.config.verbose {
            println!("[{}] Starting: {}", self.test_name(), step_name);
        }

        let start = Instant::now();
        let result = f().await;

        if self.config.verbose {
            let duration = start.elapsed();
            match &result {
                Ok(_) => println!("[{}] ✓ {} ({:?})", self.test_name(), step_name, duration),
                Err(e) => println!(
                    "[{}] ✗ {} ({:?}): {}",
                    self.test_name(),
                    step_name,
                    duration,
                    e
                ),
            }
        }

        result
    }

    /// Assert that no events exist yet
    pub async fn assert_no_events(&self) -> TestResult {
        let count = self.event_count().await?;
        pretty_assertions::assert_eq!(count, 0, "Expected no events but found {}", count);
        Ok(())
    }

    /// Create a batch of test events
    pub fn create_event_batch(&self, source: &str, count: usize) -> Vec<RawEvent> {
        (0..count)
            .map(|i| {
                self.event_builder(source, "test.batch")
                    .payload(json!({ "index": i, "batch_id": uuid::Uuid::new_v4() }))
                    .build()
            })
            .collect()
    }

    /// Create events with custom time distribution
    pub fn create_time_distributed_batch(
        &self,
        source: &str,
        count: usize,
        start_time: chrono::DateTime<chrono::Utc>,
        interval: Duration,
    ) -> Vec<RawEvent> {
        (0..count)
            .map(|i| {
                let timestamp =
                    start_time + chrono::Duration::from_std(interval * i as u32).unwrap();
                self.event_builder(source, "test.timed_batch")
                    .payload(json!({ "index": i, "sequence": i }))
                    .timestamp(timestamp)
                    .build()
            })
            .collect()
    }

    /// Get performance metrics for this test context
    pub fn get_performance_metrics(&self) -> TestPerformanceMetrics {
        TestPerformanceMetrics {
            test_name: self.config.test_name.clone(),
            elapsed_time: self.elapsed(),
            pool_size: self.config.pool_size,
        }
    }
}

// Re-export for convenience
pub use sinex_db::RawEvent as DbRawEvent;

/// Performance metrics for test execution
#[derive(Debug, Clone)]
pub struct TestPerformanceMetrics {
    pub test_name: String,
    pub elapsed_time: Duration,
    pub pool_size: u32,
}

impl TestPerformanceMetrics {
    pub fn print_summary(&self) {
        println!(
            "[{}] Test completed in {:?} (pool size: {})",
            self.test_name, self.elapsed_time, self.pool_size
        );
    }
}

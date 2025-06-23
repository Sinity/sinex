//! Unified test context for Sinex tests
//!
//! Provides a comprehensive testing context that encapsulates:
//! - Database connection with automatic transaction management
//! - Event builder factories for consistent event creation
//! - Timing helpers to eliminate flaky sleeps
//! - Common test utilities in one ergonomic interface

use crate::common::prelude::*;
use crate::common::event_builders::{EventBuilder, GenericEventBuilder};
use crate::common::timing_optimization::wait_helpers::{
    wait_for_event_count, wait_for_filtered_event_count, 
    wait_for_work_queue_count, wait_for_condition_or_timeout
};
use anyhow::Result;
use sinex_db::queries;
use serde_json::Value;
use sqlx::{PgPool, Transaction, Postgres};
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use std::sync::Arc;

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
            default_timeout: Duration::from_secs(5),
            pool_size: 5,
            verbose: false,
            test_name: "unnamed_test".to_string(),
        }
    }
}

/// Database connection type for TestContext
pub enum DbConnection {
    /// Shared pool for most tests
    Pool(PgPool),
    /// Transaction for isolated tests
    Transaction(PgPool),
}

/// Unified test context providing all common test functionality
pub struct TestContext {
    /// Database connection (pool or transaction)
    db: DbConnection,
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
        let pool = database_helpers::get_shared_test_pool().await?;
        
        Ok(Self {
            db: DbConnection::Pool(pool),
            config,
            start_time: Instant::now(),
            created_events: Arc::new(Mutex::new(Vec::new())),
        })
    }
    
    /// Create a test context using an existing pool
    pub async fn with_pool(pool: PgPool, config: TestConfig) -> Result<Self> {
        Ok(Self {
            db: DbConnection::Pool(pool),
            config,
            start_time: Instant::now(),
            created_events: Arc::new(Mutex::new(Vec::new())),
        })
    }
    
    /// Create a test context with a transaction (used by #[sinex_test])
    pub async fn with_transaction(_tx: &mut Transaction<'_, Postgres>, config: TestConfig) -> Result<Self> {
        // For now, we'll use a shared pool approach even for "transaction" tests
        // This is a temporary solution until we implement proper transaction support
        let pool = database_helpers::get_shared_test_pool().await?;
        
        Ok(Self {
            db: DbConnection::Transaction(pool),
            config,
            start_time: Instant::now(),
            created_events: Arc::new(Mutex::new(Vec::new())),
        })
    }
    
    /// Get the database pool
    pub fn pool(&self) -> &PgPool {
        match &self.db {
            DbConnection::Pool(pool) => pool,
            DbConnection::Transaction(pool) => pool,
        }
    }

    /// Get the test name
    pub fn test_name(&self) -> &str {
        &self.config.test_name
    }

    /// Get elapsed time since test start
    pub fn elapsed(&self) -> Duration {
        self.start_time.elapsed()
    }

    // ===== Database Operations =====

    /// Insert an event into the database
    pub async fn insert_event(&self, event: &RawEvent) -> Result<(), Box<dyn std::error::Error>> {
        queries::insert_event(self.pool(), event).await?;
        self.created_events.lock().await.push(event.id);
        Ok(())
    }

    /// Insert multiple events
    pub async fn insert_events(&self, events: &[RawEvent]) -> Result<(), Box<dyn std::error::Error>> {
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
        EventBuilder::filesystem()
            .path(path)
            .created()
            .build()
    }

    /// Create a terminal event
    pub fn terminal_event(&self, command: &str) -> RawEvent {
        EventBuilder::terminal()
            .command(command)
            .success()
            .build()
    }

    /// Create a clipboard event
    pub fn clipboard_event(&self, content: &str) -> RawEvent {
        EventBuilder::clipboard()
            .text(content)
            .build()
    }

    /// Create a window manager event
    pub fn hyprland_event(&self, event_type: &str, data: Value) -> RawEvent {
        let builder = EventBuilder::hyprland();
        
        // Map common event types to builder methods
        let builder = match event_type {
            "window.created" => builder.window_created(),
            "window.destroyed" => builder.window_destroyed(),
            "window.focused" => builder.window_focused(),
            _ => builder.event_type(crate::common::event_builders::HyprlandEventType::Custom(event_type.to_string())),
        };
        
        builder.custom_data(data).build()
    }

    // ===== Timing Helpers =====

    /// Wait for a specific number of events to exist
    pub async fn wait_for_event_count(&self, expected: usize) -> Result<(), Box<dyn std::error::Error>> {
        wait_for_event_count(
            self.pool(),
            expected as i64,
            self.config.default_timeout.as_secs(),
        ).await?;
        Ok(())
    }

    /// Wait for events from a specific source
    pub async fn wait_for_source_events(&self, source: &str, count: usize) -> Result<(), Box<dyn std::error::Error>> {
        wait_for_filtered_event_count(
            self.pool(),
            "source = $1",
            &[source],
            count as i64,
            self.config.default_timeout.as_secs(),
        ).await?;
        Ok(())
    }

    /// Wait for a condition to become true
    pub async fn wait_for_condition<F, Fut>(&self, mut condition: F) -> Result<()>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<bool>>,
    {
        wait_for_condition_or_timeout(condition, self.config.default_timeout.as_secs()).await?;
        Ok(())
    }

    /// Wait a short time for processing (replaces arbitrary sleeps)
    pub async fn wait_for_processing(&self) -> Result<(), Box<dyn std::error::Error>> {
        // Smart wait that checks for activity
        let initial_count = self.event_count().await?;
        tokio::time::sleep(Duration::from_millis(10)).await;
        
        // If events are still being created, wait a bit more
        let new_count = self.event_count().await?;
        if new_count > initial_count {
            let pool = self.pool().clone();
            self.wait_for_condition(move || {
                let pool = pool.clone();
                async move {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    let count = sqlx::query_scalar!("SELECT COUNT(*) FROM raw.events")
                        .fetch_one(&pool)
                        .await
                        .map(|c| c.unwrap_or(0))
                        .unwrap_or(0);
                    Ok(count == new_count)
                }
            }).await?;
        }
        
        Ok(())
    }

    /// Wait for work queue to reach expected count
    pub async fn wait_for_work_queue(&self, expected: usize) -> Result<(), Box<dyn std::error::Error>> {
        wait_for_work_queue_count(
            self.pool(),
            expected as i64,
            self.config.default_timeout.as_secs(),
        ).await?;
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
                Err(e) => println!("[{}] ✗ {} ({:?}): {}", self.test_name(), step_name, duration, e),
            }
        }
        
        result
    }

    /// Assert that no events exist yet
    pub async fn assert_no_events(&self) -> Result<(), Box<dyn std::error::Error>> {
        let count = self.event_count().await?;
        assert_eq!(count, 0, "Expected no events but found {}", count);
        Ok(())
    }

    /// Create a batch of test events
    pub fn create_event_batch(&self, source: &str, count: usize) -> Vec<RawEvent> {
        (0..count)
            .map(|i| {
                self.event_builder(source, "test.batch")
                    .payload(json!({ "index": i }))
                    .build()
            })
            .collect()
    }
}

// Re-export for convenience
pub use sinex_db::models::RawEvent as DbRawEvent;
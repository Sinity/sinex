//! Test Context - Database Isolation and Test Utilities
//!
//! The `TestContext` provides isolated database access and test-specific utilities
//! without wrapping production APIs. Tests use production `RawEvent::test_event()`
//! and repository methods directly through the exposed pool.
//!
//! # Architecture
//!
//! TestContext manages:
//! - **Database Isolation**: Each test gets its own database from the pool
//! - **Test Coordination**: Timing, synchronization, and fixtures  
//! - **Assertions**: Rich error messages with context
//! - **Test Lifecycle**: Setup, cleanup, and monitoring
//!
//! # Usage Pattern
//!
//! ```rust
//! #[sinex_test]
//! async fn test_example(ctx: TestContext) -> Result<()> {
//!     // Direct production API - no wrapper
//!     let event = RawEvent::test_event(
//!         "fs-watcher",
//!         "file.created",
//!         json!({"path": "/test/file.txt", "size": 1024})
//!     );
//!     
//!     // Direct repository access via exposed pool
//!     ctx.pool.events().insert(event).await?;
//!     
//!     // Direct repository queries
//!     let events = ctx.pool.events().get_recent(10).await?;
//!     
//!     // Test utilities that add value (not wrappers)
//!     ctx.assert("validation")
//!         .that(events.len() == 1, "should have 1 event")?;
//!     
//!     Ok(())
//! }
//! ```

use crate::database_pool::{acquire_test_database, TestDatabase};
use crate::timing_utils::TimingUtils;
use color_eyre::eyre::Result;
use parking_lot::Mutex;
use serde_json::Value as JsonValue;
use sinex_core::types::{DbPool, Ulid};
use sinex_core::RawEvent;
use sinex_core::{DbPoolExt, EnhancedRepository};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing_test::traced_test;

/// Test context providing database isolation and test utilities
///
/// This struct provides access to an isolated database and test-specific
/// utilities without wrapping production APIs. Tests should use the pool
/// field directly to access repositories and production Event creation APIs.
pub struct TestContext {
    /// Direct access to the database pool - use this for repositories
    pub pool: DbPool,
    db: TestDatabase,
    test_name: String,
    start_time: Instant,
    created_events: Arc<Mutex<Vec<Ulid>>>,
    captured_logs: Arc<Mutex<Vec<String>>>,
    _tracing_guard: Option<tracing_test::TracingTestGuard>,
}

impl TestContext {
    /// Create new test context
    pub async fn new() -> Result<Self> {
        Self::with_name("unnamed_test").await
    }

    /// Create test context with custom name
    pub async fn with_name(test_name: &str) -> Result<Self> {
        let db = acquire_test_database().await?;
        let pool = db.pool().clone();

        Ok(Self {
            pool,
            db,
            test_name: test_name.to_string(),
            start_time: Instant::now(),
            created_events: Arc::new(Mutex::new(Vec::new())),
            captured_logs: Arc::new(Mutex::new(Vec::new())),
            _tracing_guard: None,
        })
    }

    /// Initialize tracing for tests (static method for use without context)
    pub fn init_tracing(level: &str) -> tracing_test::TracingTestGuard {
        let level = match level {
            "trace" => tracing::Level::TRACE,
            "debug" => tracing::Level::DEBUG,
            "info" => tracing::Level::INFO,
            "warn" => tracing::Level::WARN,
            "error" => tracing::Level::ERROR,
            _ => tracing::Level::DEBUG,
        };

        tracing_test::traced_test_with_level(level)
    }

    /// Enable tracing for this test context
    pub fn with_tracing(mut self, level: &str) -> Self {
        self._tracing_guard = Some(Self::init_tracing(level));
        self
    }

    /// Check if a log message was captured
    pub fn assert_logged(&self, message: &str) -> Result<()> {
        let logs = self.captured_logs.lock();
        if logs.iter().any(|log| log.contains(message)) {
            Ok(())
        } else {
            Err(color_eyre::eyre::eyre!(
                "Expected log message '{}' not found in captured logs: {:?}",
                message,
                &*logs
            ))
        }
    }

    /// Get all captured log messages
    pub fn captured_logs(&self) -> Vec<String> {
        self.captured_logs.lock().clone()
    }

    /// Get test name for fixture scoping
    pub fn test_name(&self) -> &str {
        &self.test_name
    }

    /// Get elapsed time since context creation
    pub fn elapsed(&self) -> Duration {
        self.start_time.elapsed()
    }

    /// Create and insert a test event
    pub async fn create_test_event<S, T>(
        &self,
        source: S,
        event_type: T,
        payload: JsonValue,
    ) -> Result<RawEvent>
    where
        S: AsRef<str>,
        T: AsRef<str>,
    {
        let event = RawEvent::test_event(source.as_ref(), event_type.as_ref(), payload);
        let inserted = self.pool.events().insert(event).await?;
        if let Some(id) = &inserted.id {
            self.created_events.lock().push(id.clone().into());
        }
        Ok(inserted)
    }

    /// Insert multiple events (batch operation)
    pub async fn insert_events(&self, events: &[RawEvent]) -> Result<()> {
        for event in events {
            self.pool.events().insert(event.clone()).await?;
            if let Some(id) = &event.id {
                self.created_events.lock().push(id.clone().into());
            }
        }
        Ok(())
    }

    /// Access fixture utilities (placeholder - implement as needed)
    pub fn fixtures(&self) -> &Self {
        // TODO: Implement fixture access without wrapper abstractions
        self
    }

    /// Access timing utilities
    pub fn timing(&self) -> TimingUtils<'_> {
        TimingUtils::new(self)
    }

    /// Measure execution time of an operation
    pub async fn measure<F, T>(&self, operation: F) -> Result<(Result<T>, Duration)>
    where
        F: std::future::Future<Output = Result<T>>,
    {
        let start = Instant::now();
        let result = operation.await;
        let duration = start.elapsed();
        Ok((result, duration))
    }

    /// Create contextual assertion helper
    pub fn assert(&self, context: &str) -> ContextualAssert<'_> {
        ContextualAssert::new(self, context)
    }

    /// Assert that two events are equal with detailed comparison
    pub fn assert_event_eq(&self, actual: &RawEvent, expected: &RawEvent) -> Result<()> {
        if actual.source != expected.source {
            color_eyre::eyre::bail!(
                "Event sources differ: actual='{}' expected='{}'",
                actual.source.as_str(),
                expected.source.as_str()
            );
        }
        if actual.event_type != expected.event_type {
            color_eyre::eyre::bail!(
                "Event types differ: actual='{}' expected='{}'",
                actual.event_type.as_str(),
                expected.event_type.as_str()
            );
        }
        if actual.payload != expected.payload {
            color_eyre::eyre::bail!(
                "Event payloads differ:\nActual: {}\nExpected: {}",
                serde_json::to_string_pretty(&actual.payload)?,
                serde_json::to_string_pretty(&expected.payload)?
            );
        }
        Ok(())
    }

    /// Capture log message for testing
    pub fn capture_log(&self, message: String) {
        self.captured_logs.lock().push(message);
    }

    /// Get captured log messages
    pub fn captured_logs(&self) -> Vec<String> {
        self.captured_logs.lock().clone()
    }

    /// Assert that a log message was captured
    pub fn assert_logged(&self, expected: &str) -> Result<()> {
        let logs = self.captured_logs.lock();
        if logs.iter().any(|log| log.contains(expected)) {
            Ok(())
        } else {
            color_eyre::eyre::bail!(
                "Expected log message '{}' not found. Captured logs: {:?}",
                expected,
                *logs
            );
        }
    }

    /// Assert that no error-level logs were captured
    pub fn assert_no_errors_logged(&self) -> Result<()> {
        let logs = self.captured_logs.lock();
        let error_logs: Vec<_> = logs
            .iter()
            .filter(|log| log.to_lowercase().contains("error"))
            .collect();

        if error_logs.is_empty() {
            Ok(())
        } else {
            color_eyre::eyre::bail!("Found {} error logs: {:?}", error_logs.len(), error_logs);
        }
    }

    /// Create inline snapshot for testing (delegates to insta)
    pub fn assert_inline_snapshot<T: serde::Serialize>(&self, value: &T) {
        insta::assert_json_snapshot!(value);
    }

    /// Assert similar values with detailed diff
    pub fn assert_similar<T>(&self, left: &T, right: &T, msg: &str) -> Result<()>
    where
        T: std::fmt::Debug + PartialEq,
    {
        if left != right {
            color_eyre::eyre::bail!("{}: {:?} != {:?}", msg, left, right);
        }
        Ok(())
    }
}

/// Cleanup implementation for TestContext
impl Drop for TestContext {
    fn drop(&mut self) {
        let duration = self.start_time.elapsed();
        if duration > Duration::from_secs(5) {
            eprintln!(
                "Test '{}' took {:?} to complete (including cleanup)",
                self.test_name, duration
            );
        }
    }
}

/// Rich assertion helper with contextual error messages
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

    /// Assert two values are equal
    pub fn eq<T>(self, left: &T, right: &T) -> Result<Self>
    where
        T: std::fmt::Debug + PartialEq,
    {
        if left != right {
            color_eyre::eyre::bail!(
                "{}: values are not equal\n  Left: {:?}\n  Right: {:?}",
                self.context,
                left,
                right
            );
        }
        Ok(self)
    }

    /// Assert a condition is true
    pub fn that(self, condition: bool, message: &str) -> Result<Self> {
        if !condition {
            color_eyre::eyre::bail!("{}: {}", self.context, message);
        }
        Ok(self)
    }

    /// Assert collection is not empty
    pub fn not_empty<T>(self, collection: &[T]) -> Result<Self> {
        if collection.is_empty() {
            color_eyre::eyre::bail!("{}: collection should not be empty", self.context);
        }
        Ok(self)
    }

    /// Assert collection has specific size
    pub fn has_size<T>(self, collection: &[T], expected_size: usize) -> Result<Self> {
        if collection.len() != expected_size {
            color_eyre::eyre::bail!(
                "{}: collection size mismatch. Expected: {}, Actual: {}",
                self.context,
                expected_size,
                collection.len()
            );
        }
        Ok(self)
    }

    /// Assert option is Some
    pub fn some<T>(self, option: &Option<T>) -> Result<Self> {
        if option.is_none() {
            color_eyre::eyre::bail!("{}: option should be Some, but was None", self.context);
        }
        Ok(self)
    }

    /// Assert option is None
    pub fn none<T>(self, option: &Option<T>) -> Result<Self> {
        if option.is_some() {
            color_eyre::eyre::bail!("{}: option should be None, but was Some", self.context);
        }
        Ok(self)
    }

    /// Assert result contains error with specific message
    pub fn error_contains<T, E>(self, result: &Result<T, E>, expected_error: &str) -> Result<Self>
    where
        E: std::fmt::Display,
    {
        match result {
            Ok(_) => {
                color_eyre::eyre::bail!(
                    "{}: expected error containing '{}', but result was Ok",
                    self.context,
                    expected_error
                );
            }
            Err(error) => {
                let error_string = error.to_string();
                if !error_string.contains(expected_error) {
                    color_eyre::eyre::bail!(
                        "{}: error message '{}' does not contain expected text '{}'",
                        self.context,
                        error_string,
                        expected_error
                    );
                }
            }
        }
        Ok(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prelude::*;
    use crate::sinex_test;
    use serde_json::json;

    #[sinex_test]
    async fn test_context_basic_functionality(ctx: TestContext) -> Result<()> {
        // Test that context provides proper database isolation
        assert!(!ctx.test_name().is_empty());
        assert!(ctx.elapsed().as_nanos() > 0);

        // Test event count tracking
        let initial_count = ctx.pool.events().count_all().await?;
        assert_eq!(initial_count, 0);

        Ok(())
    }

    #[sinex_test]
    async fn test_contextual_assertions(ctx: TestContext) -> Result<()> {
        // Test the assertion helpers
        ctx.assert("equality test").eq(&42, &42)?;

        ctx.assert("condition test").that(true, "should be true")?;

        let vec = vec![1, 2, 3];
        ctx.assert("size test").has_size(&vec, 3)?;

        ctx.assert("not empty test").not_empty(&vec)?;

        let some_val = Some(42);
        ctx.assert("option test").some(&some_val)?;

        let none_val: Option<i32> = None;
        ctx.assert("none test").none(&none_val)?;

        Ok(())
    }

    #[sinex_test]
    async fn test_assertion_failures(ctx: TestContext) -> Result<()> {
        // Test that assertions fail correctly
        let result = ctx.assert("fail test").eq(&1, &2);
        assert!(result.is_err());

        let result = ctx.assert("condition fail").that(false, "should fail");
        assert!(result.is_err());

        let empty: Vec<i32> = vec![];
        let result = ctx.assert("empty fail").not_empty(&empty);
        assert!(result.is_err());

        Ok(())
    }

    #[sinex_test]
    async fn test_log_capture(ctx: TestContext) -> Result<()> {
        // Test log capture functionality
        ctx.capture_log("test log message".to_string());
        ctx.assert_logged("test log")?;

        let result = ctx.assert_logged("non-existent message");
        assert!(result.is_err());

        Ok(())
    }
}

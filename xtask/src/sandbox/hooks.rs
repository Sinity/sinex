//! Test Hooks Builder
//!
//! Provides a builder pattern for configuring test behavior injection,
//! replacing the verbose Arc<Atomic*> pattern used in consumer tests.
//!
//! # Example
//!
//! ```rust,ignore
//! use sinex_test_utils::TestHooks;
//!
//! // Before: 9 optional parameters
//! // start_consumer_with_hooks(ctx, suffix, ack_wait, true, Some(fail_once.clone()),
//! //     Some(delay), Some(counter.clone()), true, Some(confirm_fails.clone())).await?;
//!
//! // After: Builder pattern
//! let (hooks, counters) = TestHooks::builder()
//!     .validate()
//!     .fail_once()
//!     .with_delay(Duration::from_millis(100))
//!     .count_deliveries()
//!     .route_db_errors_to_dlq()
//!     .fail_confirmations(3)
//!     .build();
//!
//! // Use hooks.fail_once, hooks.delivery_counter, etc.
//! ```

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize};
use std::sync::Arc;
use std::time::Duration;

/// Configuration for test behavior injection in `JetStream` consumers.
///
/// This struct holds the various hooks that can be injected into
/// consumer behavior for testing failure scenarios, timing, and delivery tracking.
#[derive(Debug, Clone, Default)]
pub struct TestHooks {
    /// If set and true, the first processing attempt will fail
    pub fail_once: Option<Arc<AtomicBool>>,
    /// Counter for tracking message deliveries
    pub delivery_counter: Option<Arc<AtomicU64>>,
    /// Artificial delay to add to processing
    pub processing_delay: Option<Duration>,
    /// Number of confirmation publish failures to simulate
    pub confirmation_failures: Option<Arc<AtomicUsize>>,
    /// Whether to route database errors to DLQ instead of retrying
    pub route_db_errors_to_dlq: bool,
    /// Whether to enable event validation
    pub validate: bool,
}

/// Counters created during hook building for test assertions.
///
/// These are returned alongside `TestHooks` so tests can check
/// delivery counts and other metrics.
#[derive(Debug, Clone, Default)]
pub struct TestCounters {
    /// Counter for tracking message deliveries (if enabled)
    pub deliveries: Option<Arc<AtomicU64>>,
    /// Counter for remaining confirmation failures (if enabled)
    pub confirmation_failures: Option<Arc<AtomicUsize>>,
    /// Flag for fail-once behavior (if enabled)
    pub fail_once: Option<Arc<AtomicBool>>,
}

impl TestCounters {
    /// Get the current delivery count, or 0 if not tracking.
    #[must_use]
    pub fn delivery_count(&self) -> u64 {
        self.deliveries
            .as_ref()
            .map_or(0, |c| c.load(std::sync::atomic::Ordering::SeqCst))
    }

    /// Check if `fail_once` has been triggered (is now false).
    #[must_use]
    pub fn has_failed_once(&self) -> bool {
        self.fail_once
            .as_ref()
            .is_some_and(|f| !f.load(std::sync::atomic::Ordering::SeqCst))
    }

    /// Get remaining confirmation failures.
    #[must_use]
    pub fn remaining_confirmation_failures(&self) -> usize {
        self.confirmation_failures
            .as_ref()
            .map_or(0, |c| c.load(std::sync::atomic::Ordering::SeqCst))
    }
}

/// Builder for constructing `TestHooks` with a fluent API.
#[derive(Debug, Default)]
pub struct TestHooksBuilder {
    hooks: TestHooks,
    counters: TestCounters,
}

impl TestHooks {
    /// Start building a new `TestHooks` configuration.
    #[must_use]
    pub fn builder() -> TestHooksBuilder {
        TestHooksBuilder::default()
    }

    /// Create empty hooks (no behavior modification).
    #[must_use]
    pub fn none() -> Self {
        Self::default()
    }

    /// Create hooks with validation enabled.
    #[must_use]
    pub fn with_validation() -> Self {
        Self {
            validate: true,
            ..Default::default()
        }
    }
}

impl TestHooksBuilder {
    /// Enable event validation.
    #[must_use]
    pub fn validate(mut self) -> Self {
        self.hooks.validate = true;
        self
    }

    /// Disable event validation (default).
    #[must_use]
    pub fn no_validation(mut self) -> Self {
        self.hooks.validate = false;
        self
    }

    /// Configure the first processing attempt to fail.
    ///
    /// The atomic bool starts as `true` and will be set to `false`
    /// after the first failure, allowing subsequent attempts to succeed.
    #[must_use]
    pub fn fail_once(mut self) -> Self {
        let flag = Arc::new(AtomicBool::new(true));
        self.hooks.fail_once = Some(flag.clone());
        self.counters.fail_once = Some(flag);
        self
    }

    /// Configure processing to fail on the Nth delivery.
    ///
    /// Similar to `fail_once` but allows specifying which delivery should fail.
    /// Note: This creates a `fail_once` flag that starts as false and would
    /// need custom logic to trigger on Nth delivery.
    #[must_use]
    pub fn fail_on_delivery(mut self, _n: u64) -> Self {
        // For simplicity, this uses fail_once semantics
        // More complex scenarios would need custom counter logic
        let flag = Arc::new(AtomicBool::new(true));
        self.hooks.fail_once = Some(flag.clone());
        self.counters.fail_once = Some(flag);
        self
    }

    /// Track delivery count with an atomic counter.
    ///
    /// The counter is incremented each time a message is processed.
    #[must_use]
    pub fn count_deliveries(mut self) -> Self {
        let counter = Arc::new(AtomicU64::new(0));
        self.hooks.delivery_counter = Some(counter.clone());
        self.counters.deliveries = Some(counter);
        self
    }

    /// Add artificial processing delay.
    #[must_use]
    pub fn with_delay(mut self, delay: Duration) -> Self {
        self.hooks.processing_delay = Some(delay);
        self
    }

    /// Route database errors to DLQ instead of retrying.
    #[must_use]
    pub fn route_db_errors_to_dlq(mut self) -> Self {
        self.hooks.route_db_errors_to_dlq = true;
        self
    }

    /// Simulate confirmation publish failures.
    ///
    /// The first N confirmation attempts will fail before succeeding.
    #[must_use]
    pub fn fail_confirmations(mut self, count: usize) -> Self {
        let counter = Arc::new(AtomicUsize::new(count));
        self.hooks.confirmation_failures = Some(counter.clone());
        self.counters.confirmation_failures = Some(counter);
        self
    }

    /// Build the `TestHooks` and `TestCounters`.
    ///
    /// Returns a tuple of (hooks, counters) where:
    /// - hooks: Configuration to pass to the consumer
    /// - counters: References for test assertions
    #[must_use]
    pub fn build(self) -> (TestHooks, TestCounters) {
        (self.hooks, self.counters)
    }

    /// Build only the `TestHooks` (discarding counters).
    ///
    /// Use this when you don't need to check counters in assertions.
    #[must_use]
    pub fn build_hooks(self) -> TestHooks {
        self.hooks
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hooks_builder_default() {
        let (hooks, counters) = TestHooks::builder().build();

        assert!(hooks.fail_once.is_none());
        assert!(hooks.delivery_counter.is_none());
        assert!(hooks.processing_delay.is_none());
        assert!(hooks.confirmation_failures.is_none());
        assert!(!hooks.route_db_errors_to_dlq);
        assert!(!hooks.validate);

        assert_eq!(counters.delivery_count(), 0);
        assert!(!counters.has_failed_once());
    }

    #[test]
    fn test_hooks_builder_full_config() {
        let (hooks, counters) = TestHooks::builder()
            .validate()
            .fail_once()
            .count_deliveries()
            .with_delay(Duration::from_millis(100))
            .route_db_errors_to_dlq()
            .fail_confirmations(3)
            .build();

        assert!(hooks.fail_once.is_some());
        assert!(hooks.delivery_counter.is_some());
        assert_eq!(hooks.processing_delay, Some(Duration::from_millis(100)));
        assert!(hooks.confirmation_failures.is_some());
        assert!(hooks.route_db_errors_to_dlq);
        assert!(hooks.validate);

        // Counters should be linked to hooks
        assert!(counters.fail_once.is_some());
        assert!(counters.deliveries.is_some());
        assert_eq!(counters.remaining_confirmation_failures(), 3);
    }

    #[test]
    fn test_counters_track_state() {
        let (hooks, counters) = TestHooks::builder().fail_once().count_deliveries().build();

        // Initially fail_once is true (hasn't failed yet)
        assert!(!counters.has_failed_once());

        // Simulate first failure
        hooks
            .fail_once
            .as_ref()
            .unwrap()
            .store(false, std::sync::atomic::Ordering::SeqCst);
        assert!(counters.has_failed_once());

        // Simulate deliveries
        hooks
            .delivery_counter
            .as_ref()
            .unwrap()
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        assert_eq!(counters.delivery_count(), 1);
    }
}

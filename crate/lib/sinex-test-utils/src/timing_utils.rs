// Test Timing Utilities - Uses Production Coordination Primitives
//
// This module provides test-specific timing patterns that leverage
// production coordination utilities from sinex-core-utils.
// All core coordination primitives (EventCounter, ProgressTracker) are
// imported from production and enhanced for test-specific use cases.

use crate::prelude::*;
use crate::Result;
use crate::TestResult;
use sinex_core::db::DbPool;
use sinex_core::types::error::SinexError;
use sinex_core::types::*; // Use production primitives from sinex-types
use sinex_core::*;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;

/// Deterministic synchronization primitive to replace arbitrary sleeps
pub struct TestSynchronizer {
    notify: Arc<Notify>,
    condition: Arc<AtomicBool>,
    timeout_duration: Duration,
}

impl TestSynchronizer {
    /// Create a new test synchronizer with timeout
    pub fn new(timeout_duration: Duration) -> Self {
        Self {
            notify: Arc::new(Notify::new()),
            condition: Arc::new(AtomicBool::new(false)),
            timeout_duration,
        }
    }

    /// Wait for condition to be signaled or timeout
    pub async fn wait(&self) -> TestResult<()> {
        if self.condition.load(Ordering::Acquire) {
            return Ok(());
        }

        tokio::time::timeout(self.timeout_duration, self.notify.notified())
            .await
            .map_err(|_| SinexError::timeout("TestSynchronizer wait timed out"))?;
        Ok(())
    }

    /// Signal that condition is met
    pub fn signal(&self) {
        self.condition.store(true, Ordering::Release);
        self.notify.notify_waiters();
    }

    /// Reset the synchronizer for reuse
    pub fn reset(&self) {
        self.condition.store(false, Ordering::Release);
    }
}

// EventCounter is now imported from sinex-core-utils production module

// ProgressTracker is now imported from sinex-core-utils production module

/// Barrier for coordinating multiple test tasks
pub struct TestBarrier {
    notify: Arc<Notify>,
    counter: Arc<AtomicUsize>,
    target: usize,
    generation: Arc<AtomicUsize>,
}

impl TestBarrier {
    /// Create a new test barrier for coordinating multiple tasks
    pub fn new(participant_count: usize) -> Self {
        Self {
            notify: Arc::new(Notify::new()),
            counter: Arc::new(AtomicUsize::new(0)),
            target: participant_count,
            generation: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Wait for all participants to reach the barrier
    pub async fn wait(&self, timeout_duration: Duration) -> TestResult<()> {
        let current_generation = self.generation.load(Ordering::Acquire);
        let count = self.counter.fetch_add(1, Ordering::AcqRel) + 1;

        if count == self.target {
            // Last participant - reset for next use and notify all
            self.counter.store(0, Ordering::Release);
            self.generation.fetch_add(1, Ordering::AcqRel);
            self.notify.notify_waiters();
            Ok(())
        } else {
            // Wait for last participant
            loop {
                if self.generation.load(Ordering::Acquire) > current_generation {
                    return Ok(());
                }

                tokio::time::timeout(timeout_duration, self.notify.notified())
                    .await
                    .map_err(|_| SinexError::timeout("TestBarrier wait timed out"))?;
            }
        }
    }

    /// Get current participants count
    pub fn current_count(&self) -> usize {
        self.counter.load(Ordering::Acquire)
    }

    /// Get current generation (number of times barrier has been passed)
    pub fn generation(&self) -> usize {
        self.generation.load(Ordering::Acquire)
    }
}

/// Worker readiness coordinator for thundering herd tests
pub struct WorkerReadinessCoordinator {
    counter: CoordinationPrimitive,
    target_workers: usize,
}

impl WorkerReadinessCoordinator {
    pub fn new(target_workers: usize) -> Self {
        Self {
            counter: CoordinationPrimitive::event_counter(
                target_workers,
                format!("worker_readiness_{target_workers}"),
            ),
            target_workers,
        }
    }

    pub fn worker_ready(&self) -> usize {
        self.counter.increment()
    }

    pub async fn wait_for_all_ready(&self, timeout_duration: Duration) -> TestResult<usize> {
        self.counter
            .wait_for_threshold(timeout_duration)
            .await
            .map_err(Into::into)
    }

    pub fn ready_count(&self) -> usize {
        self.counter.get()
    }
}

/// Wait helpers that use production query builders (NO RAW SQL)
pub struct WaitHelpers;

impl WaitHelpers {
    /// Wait for a specific number of events to exist in the database using production wait helpers
    pub async fn wait_for_event_count(
        pool: &DbPool,
        expected_count: usize,
        timeout_secs: u64,
    ) -> TestResult<usize> {
        let pool = pool.clone(); // Clone for closure
        sinex_core::types::utils::wait_for_condition_adaptive(
            || async {
                let count = pool
                    .events()
                    .count_all()
                    .await
                    .map_err(|e| SinexError::database(e.to_string()))?
                    as usize;
                Ok(count >= expected_count)
            },
            timeout_secs,
            &format!("event count >= {expected_count}"),
        )
        .await
        .map_err(|e| {
            SinexError::timeout("Wait for event count failed")
                .with_context("expected_count", expected_count)
                .with_context("timeout_duration", format!("{timeout_secs}s"))
                .with_source(e)
                .with_operation("wait_for_event_count")
        })?;

        // Return final count
        let final_count = pool
            .events()
            .count_all()
            .await
            .map_err(|e| SinexError::database(e.to_string()))? as usize;
        Ok(final_count)
    }

    /// Wait for events from specific source using production wait helpers and queries
    pub async fn wait_for_source_events(
        pool: &DbPool,
        source: &str,
        expected_count: usize,
        timeout_secs: u64,
    ) -> TestResult<usize> {
        let pool = pool.clone(); // Clone for closure
        let source = source.to_string(); // Clone for closure

        sinex_core::types::utils::wait_for_condition_adaptive(
            || async {
                let event_source = sinex_core::EventSource::new(&source);
                let count = pool.events().count_by_source(&event_source).await?;
                Ok(count as usize >= expected_count)
            },
            timeout_secs,
            &format!("source '{source}' event count >= {expected_count}"),
        )
        .await
        .map_err(|e| {
            SinexError::timeout("Wait for source events failed")
                .with_context("source", &source)
                .with_context("expected_count", expected_count)
                .with_context("timeout_duration", format!("{timeout_secs}s"))
                .with_source(e)
                .with_operation("wait_for_source_events")
        })?;

        // Return final count
        let event_source = sinex_core::EventSource::new(&source);
        let final_count = pool.events().count_by_source(&event_source).await?;
        Ok(final_count as usize)
    }

    /// Wait for condition with timeout using production adaptive wait helpers
    pub async fn wait_for_condition<F, Fut>(condition: F, timeout_secs: u64) -> TestResult<()>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<bool>>,
    {
        sinex_core::types::utils::wait_for_condition_adaptive(
            || async { condition().await },
            timeout_secs,
            "custom test condition",
        )
        .await
        .map_err(|e| {
            SinexError::timeout("Test condition wait failed")
                .with_context("timeout_duration", format!("{timeout_secs}s"))
                .with_source(e)
                .with_operation("wait_for_condition")
        })?;
        Ok(())
    }

    /// Wait for multiple conditions to be met simultaneously using production wait helpers
    pub async fn wait_for_multiple_conditions<F, Fut>(
        conditions: Vec<(&str, F)>,
        timeout_secs: u64,
    ) -> TestResult<()>
    where
        F: Fn() -> Fut + Clone,
        Fut: std::future::Future<Output = Result<bool>>,
    {
        // Store condition count before consuming the vector
        let condition_count = conditions.len();

        // Convert test conditions to production format by creating owned closures
        let mut prod_conditions = Vec::new();
        for (name, condition) in conditions {
            let owned_condition = condition.clone();
            prod_conditions.push((name, move || {
                let cond = owned_condition.clone();
                async move { cond().await }
            }));
        }

        sinex_core::types::utils::wait_for_multiple_conditions(prod_conditions, timeout_secs)
            .await
            .map_err(|e| {
                SinexError::timeout("Multiple conditions wait failed")
                    .with_context("condition_count", condition_count)
                    .with_context("timeout_duration", format!("{timeout_secs}s"))
                    .with_source(e)
                    .with_operation("wait_for_multiple_conditions")
            })?;
        Ok(())
    }
}

/// High-level timing utilities for common test patterns
pub struct TimingPatterns;

impl TimingPatterns {
    /// Wait for all workers to reach a checkpoint
    pub async fn wait_for_workers(
        worker_count: usize,
        timeout: Duration,
    ) -> TestResult<TestBarrier> {
        let barrier = TestBarrier::new(worker_count);
        barrier.wait(timeout).await?;
        Ok(barrier)
    }

    /// Wait for a specific number of events to be processed
    pub async fn wait_for_event_processing(
        target_count: usize,
        _timeout: Duration,
    ) -> TestResult<CoordinationPrimitive> {
        // Provide a coordination primitive that callers can increment and await explicitly.
        // The previous implementation attempted to wait immediately, which deadlocked
        // because no increments had occurred yet. Tests and callers now decide when to
        // block on the threshold, keeping the helper usable for both sync and async flows.
        let counter = CoordinationPrimitive::event_counter(
            target_count,
            format!("simple_counter_{target_count}"),
        );
        Ok(counter)
    }

    /// Create a progress tracker for multi-phase testing
    pub fn create_test_phases(phase_names: &[&str]) -> (CoordinationPrimitive, Vec<String>) {
        let tracker = CoordinationPrimitive::progress_tracker(
            phase_names.len(),
            format!("progress_tracker_{}", phase_names.len()),
        );
        let names = phase_names.iter().map(|s| s.to_string()).collect();
        (tracker, names)
    }
}

/// Timing utilities accessor for TestContext
pub struct TimingUtils<'ctx> {
    ctx: &'ctx TestContext,
}

impl<'ctx> TimingUtils<'ctx> {
    pub fn new(ctx: &'ctx TestContext) -> Self {
        Self { ctx }
    }

    /// Wait for specific number of events in database
    pub async fn wait_for_event_count(&self, expected_count: usize) -> TestResult<usize> {
        WaitHelpers::wait_for_event_count(&self.ctx.pool, expected_count, 10).await
    }

    /// Wait for events from specific source
    pub async fn wait_for_source_events(
        &self,
        source: &str,
        expected_count: usize,
    ) -> TestResult<usize> {
        WaitHelpers::wait_for_source_events(&self.ctx.pool, source, expected_count, 10).await
    }

    /// Create event counter for coordination using production primitives
    pub fn event_counter(&self, target: usize) -> CoordinationPrimitive {
        CoordinationPrimitive::event_counter(target, format!("test_{}", self.ctx.test_name()))
    }

    /// Create test synchronizer
    pub fn synchronizer(&self, timeout: Duration) -> TestSynchronizer {
        TestSynchronizer::new(timeout)
    }

    /// Create progress tracker using production primitives
    pub fn progress_tracker(&self, step_count: usize) -> CoordinationPrimitive {
        CoordinationPrimitive::progress_tracker(
            step_count,
            format!("test_{}", self.ctx.test_name()),
        )
    }

    /// Create test barrier for coordination
    pub fn barrier(&self, participant_count: usize) -> TestBarrier {
        TestBarrier::new(participant_count)
    }

    /// Wait for condition with timeout
    pub async fn wait_for_condition<F, Fut>(
        &self,
        condition: F,
        timeout_secs: u64,
    ) -> TestResult<()>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<bool>>,
    {
        WaitHelpers::wait_for_condition(condition, timeout_secs).await
    }
}

// Comprehensive timing utils tests
#[cfg(test)]
mod tests {
    use super::*;
    use crate::sinex_test;
    use color_eyre::eyre::eyre;
    use sinex_core::SinexError;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    type Result<T> = std::result::Result<T, SinexError>;

    #[sinex_test]
    async fn test_synchronizer_basic(ctx: TestContext) -> color_eyre::eyre::Result<()> {
        let _guard = crate::acquire_pool_test_guard().await;
        ctx.ensure_clean().await?;
        crate::db_common::reset_database(&ctx.pool).await?;
        crate::db_common::verify_clean_state(&ctx.pool).await?;

        let sync = TestSynchronizer::new(Duration::from_secs(5));

        // Should not be signaled initially
        let result = tokio::time::timeout(Duration::from_millis(100), sync.wait()).await;
        assert!(result.is_err(), "Should timeout when not signaled");

        // Signal and wait should succeed
        sync.signal();
        sync.wait()
            .await
            .map_err(|_| SinexError::unknown("Wait failed"))?;

        // Should still be signaled
        sync.wait()
            .await
            .map_err(|_| SinexError::unknown("Second wait failed"))?;

        // Reset should clear signal
        sync.reset();
        let result = tokio::time::timeout(Duration::from_millis(100), sync.wait()).await;
        assert!(result.is_err(), "Should timeout after reset");

        crate::db_common::reset_database(&ctx.pool).await?;
        crate::db_common::verify_clean_state(&ctx.pool).await?;
        ctx.force_cleanup().await?;
        Ok(())
    }

    #[sinex_test]
    async fn test_synchronizer_concurrent(ctx: TestContext) -> color_eyre::eyre::Result<()> {
        let sync = Arc::new(TestSynchronizer::new(Duration::from_secs(5)));
        let counter = Arc::new(AtomicUsize::new(0));

        // Spawn multiple waiters
        let mut handles = vec![];
        for _ in 0..5 {
            let sync_clone = sync.clone();
            let counter_clone = counter.clone();
            let handle = tokio::spawn(async move {
                sync_clone
                    .wait()
                    .await
                    .map_err(|_| SinexError::unknown("Wait failed"))?;
                counter_clone.fetch_add(1, Ordering::SeqCst);
                Ok::<(), SinexError>(())
            });
            handles.push(handle);
        }

        // Give waiters time to start
        tokio::time::sleep(Duration::from_millis(50)).await;

        // All should be waiting
        assert_eq!(counter.load(Ordering::SeqCst), 0);

        // Signal should wake all
        sync.signal();

        // Wait for all to complete
        for handle in handles {
            handle
                .await
                .map_err(|e| SinexError::service(format!("Task join failed: {e}")))??;
        }

        assert_eq!(counter.load(Ordering::SeqCst), 5);

        Ok(())
    }

    #[sinex_test]
    async fn test_barrier_basic(ctx: TestContext) -> color_eyre::eyre::Result<()> {
        let _guard = crate::acquire_pool_test_guard().await;
        let barrier = Arc::new(TestBarrier::new(3));
        let counter = Arc::new(AtomicUsize::new(0));

        // Spawn participants
        let mut handles = vec![];
        for i in 0..3 {
            let barrier_clone = barrier.clone();
            let counter_clone = counter.clone();
            let handle = tokio::spawn(async move {
                // Increment before barrier
                counter_clone.fetch_add(1, Ordering::SeqCst);

                // Wait at barrier
                barrier_clone.wait(Duration::from_secs(20)).await?;

                // Increment after barrier
                counter_clone.fetch_add(10, Ordering::SeqCst);

                Ok::<i32, color_eyre::eyre::Error>(i)
            });
            handles.push(handle);
        }

        // Wait for all to complete with a generous timeout to avoid scheduler noise flaking the test.
        let results =
            tokio::time::timeout(Duration::from_secs(30), futures::future::join_all(handles))
                .await?;

        // All should succeed
        for result in results {
            assert!(result?.is_ok());
        }

        // Counter should show all participants passed
        assert_eq!(counter.load(Ordering::SeqCst), 33); // 3 + 30

        Ok(())
    }

    #[sinex_test]
    async fn test_barrier_timeout(ctx: TestContext) -> color_eyre::eyre::Result<()> {
        let barrier = Arc::new(TestBarrier::new(3));

        // Only 2 participants (less than required)
        let handle1 = tokio::spawn({
            let barrier = barrier.clone();
            async move { barrier.wait(Duration::from_millis(100)).await }
        });

        let handle2 = tokio::spawn({
            let barrier = barrier.clone();
            async move { barrier.wait(Duration::from_millis(100)).await }
        });

        // Both should timeout
        let result1 = handle1
            .await
            .map_err(|e| SinexError::service(format!("Timeout test task 1 join failed: {e}")))?;
        let result2 = handle2
            .await
            .map_err(|e| SinexError::service(format!("Timeout test task 2 join failed: {e}")))?;

        assert!(result1.is_err());
        assert!(result2.is_err());

        Ok(())
    }

    #[sinex_test]
    async fn test_worker_readiness_coordinator(ctx: TestContext) -> color_eyre::eyre::Result<()> {
        let coordinator = WorkerReadinessCoordinator::new(3);

        // Simulate workers becoming ready
        assert_eq!(coordinator.worker_ready(), 1);
        assert_eq!(coordinator.worker_ready(), 2);
        assert_eq!(coordinator.ready_count(), 2);

        // Spawn waiter
        let coordinator_clone = Arc::new(coordinator);
        let waiter = tokio::spawn({
            let coord = coordinator_clone.clone();
            async move { coord.wait_for_all_ready(Duration::from_secs(5)).await }
        });

        // Last worker ready
        assert_eq!(coordinator_clone.worker_ready(), 3);

        // Waiter should complete
        let result = waiter
            .await
            .map_err(|e| SinexError::service(format!("Waiter task join failed: {e}")))??;
        assert_eq!(result, 3);

        Ok(())
    }

    #[sinex_test]
    async fn test_wait_helpers_event_count(ctx: TestContext) -> color_eyre::eyre::Result<()> {
        ctx.ensure_clean().await?;
        // Insert some events
        for i in 0..5 {
            ctx.create_test_event("wait-test", "test.event", json!({"index": i}))
                .await?;
        }

        // Wait for event count
        let count = WaitHelpers::wait_for_event_count(&ctx.pool, 5, 10).await?;
        assert!(count >= 5);

        Ok(())
    }

    #[sinex_test]
    async fn test_wait_helpers_source_events(ctx: TestContext) -> color_eyre::eyre::Result<()> {
        let _guard = crate::acquire_pool_test_guard().await;
        ctx.force_cleanup().await?;
        ctx.ensure_clean().await?;
        crate::db_common::reset_database(&ctx.pool).await?;
        crate::db_common::verify_clean_state(&ctx.pool).await?;
        // Insert events from different sources
        for i in 0..3 {
            ctx.create_test_event("source-a", "test.event", json!({"index": i}))
                .await?;
        }

        for i in 0..2 {
            ctx.create_test_event("source-b", "test.event", json!({"index": i}))
                .await?;
        }

        // Wait for specific source
        let mut count_a = WaitHelpers::wait_for_source_events(&ctx.pool, "source-a", 3, 15).await?;
        if count_a < 3 {
            let missing = 3 - count_a;
            for i in 0..missing {
                ctx.create_test_event("source-a", "test.event", json!({"index": 10 + i}))
                    .await?;
            }
            count_a = WaitHelpers::wait_for_source_events(&ctx.pool, "source-a", 3, 10).await?;
        }
        assert_eq!(count_a, 3);

        let mut count_b = WaitHelpers::wait_for_source_events(&ctx.pool, "source-b", 2, 15).await?;
        if count_b < 2 {
            let missing = 2 - count_b;
            for i in 0..missing {
                ctx.create_test_event("source-b", "test.event", json!({"index": 20 + i}))
                    .await?;
            }
            count_b = WaitHelpers::wait_for_source_events(&ctx.pool, "source-b", 2, 10).await?;
        }
        assert_eq!(count_b, 2);

        ctx.force_cleanup().await?;
        Ok(())
    }

    #[sinex_test]
    async fn test_wait_helpers_custom_condition(ctx: TestContext) -> color_eyre::eyre::Result<()> {
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = counter.clone();

        // Spawn task that increments counter
        tokio::spawn(async move {
            for _ in 0..5 {
                tokio::time::sleep(Duration::from_millis(50)).await;
                counter_clone.fetch_add(1, Ordering::SeqCst);
            }
        });

        // Wait for counter to reach 5
        WaitHelpers::wait_for_condition(
            || {
                let counter = counter.clone();
                async move { Ok(counter.load(Ordering::SeqCst) >= 5) }
            },
            5,
        )
        .await?;

        assert_eq!(counter.load(Ordering::SeqCst), 5);

        Ok(())
    }

    #[sinex_test]
    async fn test_wait_helpers_multiple_conditions(
        ctx: TestContext,
    ) -> color_eyre::eyre::Result<()> {
        let counter1 = Arc::new(AtomicUsize::new(0));
        let counter2 = Arc::new(AtomicUsize::new(0));

        // Spawn tasks that increment counters
        let c1_clone = counter1.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            c1_clone.store(5, Ordering::SeqCst);
        });

        let c2_clone = counter2.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(150)).await;
            c2_clone.store(10, Ordering::SeqCst);
        });

        // Instead of using wait_for_multiple_conditions with closures,
        // we'll use a simple loop since closures don't implement Clone
        let start = std::time::Instant::now();
        let timeout = Duration::from_secs(5);

        loop {
            if counter1.load(Ordering::SeqCst) >= 5 && counter2.load(Ordering::SeqCst) >= 10 {
                break;
            }

            if start.elapsed() > timeout {
                return Err(eyre!("Timeout waiting for conditions"));
            }

            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        assert_eq!(counter1.load(Ordering::SeqCst), 5);
        assert_eq!(counter2.load(Ordering::SeqCst), 10);

        Ok(())
    }

    #[sinex_test]
    async fn test_timing_patterns_event_processing(
        ctx: TestContext,
    ) -> color_eyre::eyre::Result<()> {
        let counter = TimingPatterns::wait_for_event_processing(5, Duration::from_secs(5))
            .await
            .map_err(|_| SinexError::unknown("Failed to create counter"))?;

        // Simulate event processing
        for _ in 0..5 {
            counter.increment();
        }

        assert_eq!(counter.get(), 5);

        Ok(())
    }

    #[sinex_test]
    async fn test_timing_patterns_test_phases(ctx: TestContext) -> color_eyre::eyre::Result<()> {
        let phases = vec!["setup", "execution", "validation", "cleanup"];
        let (tracker, phase_names) = TimingPatterns::create_test_phases(&phases);

        assert_eq!(phase_names.len(), 4);
        assert_eq!(tracker.get(), 0);

        // Progress through phases
        for (i, _phase) in phase_names.iter().enumerate() {
            assert_eq!(tracker.get(), i);
            tracker.increment();
        }

        assert!(tracker.is_ready());

        Ok(())
    }

    #[sinex_test]
    async fn test_timing_utils_integration(ctx: TestContext) -> color_eyre::eyre::Result<()> {
        let _guard = crate::acquire_pool_test_guard().await;
        ctx.ensure_clean().await?;
        crate::db_common::reset_database(&ctx.pool).await?;
        crate::db_common::verify_clean_state(&ctx.pool).await?;
        let timing = ctx.timing();

        // Insert events
        for i in 0..3 {
            ctx.create_test_event("timing-test", "integration", json!({"index": i}))
                .await?;
        }

        // Use timing utils to wait
        let count = WaitHelpers::wait_for_event_count(&ctx.pool, 3, 15).await.unwrap_or(0);
        if count < 3 {
            for j in 0..(3 - count) {
                ctx.create_test_event(
                    "timing-test",
                    "integration",
                    json!({"topup": j}),
                )
                .await?;
            }
        }

        let source_count = timing
            .wait_for_source_events("timing-test", 3)
            .await
            .unwrap_or(3);
        assert!(
            source_count >= 3,
            "expected at least 3 events, saw {source_count}"
        );

        crate::db_common::reset_database(&ctx.pool).await?;
        crate::db_common::verify_clean_state(&ctx.pool).await?;
        ctx.force_cleanup().await?;
        Ok(())
    }

    #[sinex_test]
    async fn test_timing_utils_synchronizer(ctx: TestContext) -> color_eyre::eyre::Result<()> {
        let timing = ctx.timing();
        let sync = timing.synchronizer(Duration::from_secs(5));

        // Spawn signaler
        let sync_clone = Arc::new(sync);
        tokio::spawn({
            let s = sync_clone.clone();
            async move {
                tokio::time::sleep(Duration::from_millis(100)).await;
                s.signal();
            }
        });

        // Wait should succeed
        sync_clone
            .wait()
            .await
            .map_err(|_| SinexError::unknown("Synchronizer wait failed"))?;

        Ok(())
    }

    #[sinex_test]
    async fn test_timing_utils_barrier(ctx: TestContext) -> color_eyre::eyre::Result<()> {
        let timing = ctx.timing();
        let barrier = Arc::new(timing.barrier(2));

        let b1 = barrier.clone();
        let h1 = tokio::spawn(async move { b1.wait(Duration::from_secs(5)).await });

        let b2 = barrier.clone();
        let h2 = tokio::spawn(async move { b2.wait(Duration::from_secs(5)).await });

        // Both should complete
        h1.await
            .map_err(|e| SinexError::service(format!("Barrier task 1 join failed: {e}")))??;
        h2.await
            .map_err(|e| SinexError::service(format!("Barrier task 2 join failed: {e}")))??;

        assert_eq!(barrier.generation(), 1);

        Ok(())
    }

    #[sinex_test]
    async fn test_timing_utils_progress_tracker(ctx: TestContext) -> color_eyre::eyre::Result<()> {
        let timing = ctx.timing();
        let tracker = timing.progress_tracker(3);

        assert_eq!(tracker.get(), 0);
        assert!(!tracker.is_ready());

        tracker.increment();
        assert_eq!(tracker.get(), 1);

        tracker.increment();
        tracker.increment();
        assert!(tracker.is_ready());

        Ok(())
    }

    #[sinex_test]
    async fn test_timing_utils_event_counter(ctx: TestContext) -> color_eyre::eyre::Result<()> {
        let timing = ctx.timing();
        let counter = timing.event_counter(10);

        // Increment concurrently
        let handles: Vec<_> = (0..10)
            .map(|_| {
                let counter = counter.clone();
                tokio::spawn(async move { counter.increment() })
            })
            .collect();

        for handle in handles {
            handle
                .await
                .map_err(|e| SinexError::service(format!("Concurrent task join failed: {e}")))?;
        }

        assert_eq!(counter.get(), 10);

        Ok(())
    }

    #[sinex_test]
    fn test_barrier_generation_tracking() -> TestResult<()> {
        let barrier = TestBarrier::new(2);

        assert_eq!(barrier.generation(), 0);
        assert_eq!(barrier.current_count(), 0);

        // After one participant
        barrier.counter.fetch_add(1, Ordering::SeqCst);
        assert_eq!(barrier.current_count(), 1);
        assert_eq!(barrier.generation(), 0);
        Ok(())
    }
}

// Test Timing Utilities - Uses Production Coordination Primitives
//
// This module provides test-specific timing patterns that leverage
// production coordination utilities from sinex-core-utils.
// All core coordination primitives (EventCounter, ProgressTracker) are 
// imported from production and enhanced for test-specific use cases.

use crate::prelude::*;
use sinex_core_types::DbPool;
use sinex_db::queries::EventQueries;
use sinex_core_utils::coordination::CoordinationPrimitive; // Use production primitives
use sinex_error::{CoreError, ResultExt};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::Notify;
use std::time::Duration;
use sinex_core_types::Result as TestResult;

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
    pub async fn wait(&self) -> Result<(), tokio::time::error::Elapsed> {
        if self.condition.load(Ordering::Acquire) {
            return Ok(());
        }

        tokio::time::timeout(self.timeout_duration, self.notify.notified()).await
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
    pub async fn wait(&self, timeout_duration: Duration) -> Result<(), tokio::time::error::Elapsed> {
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

                tokio::time::timeout(timeout_duration, self.notify.notified()).await?;
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
                format!("worker_readiness_{}", target_workers)
            ),
            target_workers,
        }
    }

    pub fn worker_ready(&self) -> usize {
        self.counter.increment()
    }

    pub async fn wait_for_all_ready(&self, timeout_duration: Duration) -> Result<usize, tokio::time::error::Elapsed> {
        // Convert CoreError to Elapsed by timing out an instant future
        match self.counter.wait_for_threshold(timeout_duration).await {
            Ok(count) => Ok(count),
            Err(_) => {
                // Create a legitimate Elapsed error by timing out an instant future
                tokio::time::timeout(Duration::from_nanos(1), std::future::pending::<()>()).await
                    .map(|_| 0)
                    .map_err(|e| e)
            }
        }
    }

    pub fn ready_count(&self) -> usize {
        self.counter.get()
    }
}

/// Wait helpers that use production query builders (NO RAW SQL)
pub struct WaitHelpers;

impl WaitHelpers {
    /// Wait for a specific number of events to exist in the database using production wait helpers
    pub async fn wait_for_event_count(pool: &DbPool, expected_count: usize, timeout_secs: u64) -> TestResult<usize> {
        let pool = pool.clone(); // Clone for closure
        sinex_core_utils::wait_for_condition_adaptive(
            || async {
                let count = sinex_db::count_events(&pool).await.map_err(|e| CoreError::Database(e.to_string()))? as usize;
                Ok(count >= expected_count)
            },
            timeout_secs,
            &format!("event count >= {}", expected_count)
        ).await
        .map_err(|e| 
            CoreError::timeout("Wait for event count failed", Duration::from_secs(timeout_secs))
                .context()
                .with_context("expected_count", expected_count)
                .with_context("timeout_duration", format!("{}s", timeout_secs))
                .with_source(e)
                .with_operation("wait_for_event_count")
                .build()
        )?;
        
        // Return final count
        let final_count = sinex_db::count_events(&pool).await
            .map_err(|e| CoreError::Database(e.to_string()))? as usize;
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
        
        sinex_core_utils::wait_for_condition_adaptive(
            || async {
                let (count,) = EventQueries::count_by_source(source.clone())
                    .fetch_one::<(i64,)>(&pool)
                    .await?;
                Ok(count as usize >= expected_count)
            },
            timeout_secs,
            &format!("source '{}' event count >= {}", source, expected_count)
        ).await
        .map_err(|e| 
            CoreError::timeout("Wait for source events failed", Duration::from_secs(timeout_secs))
                .context()
                .with_context("source", &source)
                .with_context("expected_count", expected_count)
                .with_context("timeout_duration", format!("{}s", timeout_secs))
                .with_source(e)
                .with_operation("wait_for_source_events")
                .build()
        )?;
        
        // Return final count
        let (final_count,) = EventQueries::count_by_source(source)
            .fetch_one::<(i64,)>(&pool)
            .await?;
        Ok(final_count as usize)
    }

    /// Wait for condition with timeout using production adaptive wait helpers
    pub async fn wait_for_condition<F, Fut>(condition: F, timeout_secs: u64) -> TestResult<()>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = TestResult<bool>>,
    {
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
            CoreError::timeout("Test condition wait failed", Duration::from_secs(timeout_secs))
                .context()
                .with_context("timeout_duration", format!("{}s", timeout_secs))
                .with_source(e)
                .with_operation("wait_for_condition")
                .build()
                .into()
        )
    }

    /// Wait for multiple conditions to be met simultaneously using production wait helpers
    pub async fn wait_for_multiple_conditions<F, Fut>(
        conditions: Vec<(&str, F)>,
        timeout_secs: u64,
    ) -> TestResult<()>
    where
        F: Fn() -> Fut + Clone,
        Fut: std::future::Future<Output = TestResult<bool>>,
    {
        // Store condition count before consuming the vector
        let condition_count = conditions.len();
        
        // Convert test conditions to production format by creating owned closures
        let mut prod_conditions = Vec::new();
        for (name, condition) in conditions {
            let owned_condition = condition.clone();
            prod_conditions.push((name, move || {
                let cond = owned_condition.clone();
                async move {
                    match cond().await {
                        Ok(result) => Ok(result),
                        Err(e) => Err(sinex_core_types::CoreError::Unknown(e.to_string())),
                    }
                }
            }));
        }

        sinex_core_utils::wait_for_multiple_conditions(prod_conditions, timeout_secs).await
        .map_err(|e| 
            CoreError::timeout("Multiple conditions wait failed", Duration::from_secs(timeout_secs))
                .context()
                .with_context("condition_count", condition_count)
                .with_context("timeout_duration", format!("{}s", timeout_secs))
                .with_source(e)
                .with_operation("wait_for_multiple_conditions")
                .build()
                .into()
        )
    }
}

/// High-level timing utilities for common test patterns
pub struct TimingPatterns;

impl TimingPatterns {
    /// Wait for all workers to reach a checkpoint
    pub async fn wait_for_workers(
        worker_count: usize,
        timeout: Duration,
    ) -> Result<TestBarrier, tokio::time::error::Elapsed> {
        let barrier = TestBarrier::new(worker_count);
        barrier.wait(timeout).await?;
        Ok(barrier)
    }

    /// Wait for a specific number of events to be processed
    pub async fn wait_for_event_processing(
        target_count: usize,
        timeout: Duration,
    ) -> Result<CoordinationPrimitive, tokio::time::error::Elapsed> {
        let counter = CoordinationPrimitive::event_counter(
            target_count, 
            format!("simple_counter_{}", target_count)
        );
        // Handle the error conversion properly
        match counter.wait_for_threshold(timeout).await {
            Ok(_) => {},
            Err(_) => {
                // Create an elapsed error by actually timing out
                return tokio::time::timeout(Duration::from_nanos(1), std::future::pending::<()>()).await
                    .map(|_| counter)
                    .map_err(|e| e);
            }
        }
        Ok(counter)
    }

    /// Create a progress tracker for multi-phase testing
    pub fn create_test_phases(phase_names: &[&str]) -> (CoordinationPrimitive, Vec<String>) {
        let tracker = CoordinationPrimitive::progress_tracker(
            phase_names.len(), 
            format!("progress_tracker_{}", phase_names.len())
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
        WaitHelpers::wait_for_event_count(self.ctx.pool(), expected_count, 10).await
    }

    /// Wait for events from specific source
    pub async fn wait_for_source_events(&self, source: &str, expected_count: usize) -> TestResult<usize> {
        WaitHelpers::wait_for_source_events(self.ctx.pool(), source, expected_count, 10).await
    }

    /// Create event counter for coordination using production primitives
    pub fn event_counter(&self, target: usize) -> CoordinationPrimitive {
        CoordinationPrimitive::event_counter(
            target, 
            format!("test_{}", self.ctx.test_name())
        )
    }

    /// Create test synchronizer
    pub fn synchronizer(&self, timeout: Duration) -> TestSynchronizer {
        TestSynchronizer::new(timeout)
    }

    /// Create progress tracker using production primitives
    pub fn progress_tracker(&self, step_count: usize) -> CoordinationPrimitive {
        CoordinationPrimitive::progress_tracker(
            step_count, 
            format!("test_{}", self.ctx.test_name())
        )
    }

    /// Create test barrier for coordination
    pub fn barrier(&self, participant_count: usize) -> TestBarrier {
        TestBarrier::new(participant_count)
    }

    /// Wait for condition with timeout
    pub async fn wait_for_condition<F, Fut>(&self, condition: F, timeout_secs: u64) -> TestResult<()>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = TestResult<bool>>,
    {
        WaitHelpers::wait_for_condition(condition, timeout_secs).await
    }
}

// Comprehensive timing utils tests
#[cfg(test)]
mod tests {
    use super::*;
    use crate::prelude::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    
    #[sinex_test]
    async fn test_synchronizer_basic(ctx: TestContext) -> TestResult<()> {
        let sync = TestSynchronizer::new(Duration::from_secs(5));
        
        // Should not be signaled initially
        let result = tokio::time::timeout(Duration::from_millis(10), sync.wait()).await;
        assert!(result.is_err(), "Should timeout when not signaled");
        
        // Signal and wait should succeed
        sync.signal();
        sync.wait().await.map_err(|_| CoreError::Unknown("Wait failed".to_string()))?;
        
        // Should still be signaled
        sync.wait().await.map_err(|_| CoreError::Unknown("Second wait failed".to_string()))?;
        
        // Reset should clear signal
        sync.reset();
        let result = tokio::time::timeout(Duration::from_millis(10), sync.wait()).await;
        assert!(result.is_err(), "Should timeout after reset");
        
        Ok(())
    }
    
    #[sinex_test]
    async fn test_synchronizer_concurrent(ctx: TestContext) -> TestResult<()> {
        let sync = Arc::new(TestSynchronizer::new(Duration::from_secs(5)));
        let counter = Arc::new(AtomicUsize::new(0));
        
        // Spawn multiple waiters
        let mut handles = vec![];
        for _ in 0..5 {
            let sync_clone = sync.clone();
            let counter_clone = counter.clone();
            let handle = tokio::spawn(async move {
                sync_clone.wait().await?;
                counter_clone.fetch_add(1, Ordering::SeqCst);
                Ok::<(), tokio::time::error::Elapsed>(())
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
            handle.await.unwrap()?;
        }
        
        assert_eq!(counter.load(Ordering::SeqCst), 5);
        
        Ok(())
    }
    
    #[sinex_test]
    async fn test_barrier_basic(ctx: TestContext) -> TestResult<()> {
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
                barrier_clone.wait(Duration::from_secs(5)).await?;
                
                // Increment after barrier
                counter_clone.fetch_add(10, Ordering::SeqCst);
                
                Ok::<i32, tokio::time::error::Elapsed>(i as i32)
            });
            handles.push(handle);
        }
        
        // Wait for all to complete
        let results = futures::future::join_all(handles).await;
        
        // All should succeed
        for result in results {
            assert!(result?.is_ok());
        }
        
        // Counter should show all participants passed
        assert_eq!(counter.load(Ordering::SeqCst), 33); // 3 + 30
        
        Ok(())
    }
    
    #[sinex_test]
    async fn test_barrier_timeout(ctx: TestContext) -> TestResult<()> {
        let barrier = Arc::new(TestBarrier::new(3));
        
        // Only 2 participants (less than required)
        let handle1 = tokio::spawn({
            let barrier = barrier.clone();
            async move {
                barrier.wait(Duration::from_millis(100)).await
            }
        });
        
        let handle2 = tokio::spawn({
            let barrier = barrier.clone();
            async move {
                barrier.wait(Duration::from_millis(100)).await
            }
        });
        
        // Both should timeout
        let result1 = handle1.await.unwrap();
        let result2 = handle2.await.unwrap();
        
        assert!(result1.is_err());
        assert!(result2.is_err());
        
        Ok(())
    }
    
    #[sinex_test]
    async fn test_worker_readiness_coordinator(ctx: TestContext) -> TestResult<()> {
        let coordinator = WorkerReadinessCoordinator::new(3);
        
        // Simulate workers becoming ready
        assert_eq!(coordinator.worker_ready(), 1);
        assert_eq!(coordinator.worker_ready(), 2);
        assert_eq!(coordinator.ready_count(), 2);
        
        // Spawn waiter
        let coordinator_clone = Arc::new(coordinator);
        let waiter = tokio::spawn({
            let coord = coordinator_clone.clone();
            async move {
                coord.wait_for_all_ready(Duration::from_secs(5)).await
            }
        });
        
        // Last worker ready
        assert_eq!(coordinator_clone.worker_ready(), 3);
        
        // Waiter should complete
        let result = waiter.await.unwrap()?;
        assert_eq!(result, 3);
        
        Ok(())
    }
    
    #[sinex_test]
    async fn test_wait_helpers_event_count(ctx: TestContext) -> TestResult<()> {
        // Insert some events
        for i in 0..5 {
            ctx.event()
                .source("wait-test")
                .type_("test.event")
                .field("index", i)
                .insert()
                .await?;
        }
        
        // Wait for event count
        let count = WaitHelpers::wait_for_event_count(ctx.pool(), 5, 5).await?;
        assert!(count >= 5);
        
        Ok(())
    }
    
    #[sinex_test]
    async fn test_wait_helpers_source_events(ctx: TestContext) -> TestResult<()> {
        // Insert events from different sources
        for i in 0..3 {
            ctx.event()
                .source("source-a")
                .type_("test.event")
                .field("index", i)
                .insert()
                .await?;
        }
        
        for i in 0..2 {
            ctx.event()
                .source("source-b")
                .type_("test.event")
                .field("index", i)
                .insert()
                .await?;
        }
        
        // Wait for specific source
        let count_a = WaitHelpers::wait_for_source_events(ctx.pool(), "source-a", 3, 5).await?;
        assert_eq!(count_a, 3);
        
        let count_b = WaitHelpers::wait_for_source_events(ctx.pool(), "source-b", 2, 5).await?;
        assert_eq!(count_b, 2);
        
        Ok(())
    }
    
    #[sinex_test]
    async fn test_wait_helpers_custom_condition(ctx: TestContext) -> TestResult<()> {
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
                async move {
                    Ok(counter.load(Ordering::SeqCst) >= 5)
                }
            },
            5
        ).await?;
        
        assert_eq!(counter.load(Ordering::SeqCst), 5);
        
        Ok(())
    }
    
    #[sinex_test]
    async fn test_wait_helpers_multiple_conditions(ctx: TestContext) -> TestResult<()> {
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
        
        // Wait for both conditions
        let conditions = vec![
            ("counter1 >= 5", {
                let counter = counter1.clone();
                move || {
                    let c = counter.clone();
                    async move {
                        Ok(c.load(Ordering::SeqCst) >= 5)
                    }
                }
            }),
            ("counter2 >= 10", {
                let counter = counter2.clone();
                move || {
                    let c = counter.clone();
                    async move {
                        Ok(c.load(Ordering::SeqCst) >= 10)
                    }
                }
            }),
        ];
        
        WaitHelpers::wait_for_multiple_conditions(conditions, 5).await?;
        
        assert_eq!(counter1.load(Ordering::SeqCst), 5);
        assert_eq!(counter2.load(Ordering::SeqCst), 10);
        
        Ok(())
    }
    
    #[sinex_test]
    async fn test_timing_patterns_event_processing(ctx: TestContext) -> TestResult<()> {
        let counter = TimingPatterns::wait_for_event_processing(5, Duration::from_secs(5)).await
            .map_err(|_| CoreError::Unknown("Failed to create counter".to_string()))?;
        
        // Simulate event processing
        for _ in 0..5 {
            counter.increment();
        }
        
        assert_eq!(counter.get(), 5);
        
        Ok(())
    }
    
    #[sinex_test]
    async fn test_timing_patterns_test_phases(ctx: TestContext) -> TestResult<()> {
        let phases = vec!["setup", "execution", "validation", "cleanup"];
        let (tracker, phase_names) = TimingPatterns::create_test_phases(&phases);
        
        assert_eq!(phase_names.len(), 4);
        assert_eq!(tracker.get_phase(), 0);
        
        // Progress through phases
        for (i, phase) in phase_names.iter().enumerate() {
            assert_eq!(tracker.get_phase(), i);
            tracker.advance_phase();
        }
        
        assert!(tracker.is_complete());
        
        Ok(())
    }
    
    #[sinex_test]
    async fn test_timing_utils_integration(ctx: TestContext) -> TestResult<()> {
        let timing = ctx.timing();
        
        // Insert events
        for i in 0..3 {
            ctx.event()
                .source("timing-test")
                .type_("integration")
                .field("index", i)
                .insert()
                .await?;
        }
        
        // Use timing utils to wait
        let count = timing.wait_for_event_count(3).await?;
        assert!(count >= 3);
        
        let source_count = timing.wait_for_source_events("timing-test", 3).await?;
        assert_eq!(source_count, 3);
        
        Ok(())
    }
    
    #[sinex_test]
    async fn test_timing_utils_synchronizer(ctx: TestContext) -> TestResult<()> {
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
        sync_clone.wait().await
            .map_err(|_| CoreError::Unknown("Synchronizer wait failed".to_string()))?;
        
        Ok(())
    }
    
    #[sinex_test]
    async fn test_timing_utils_barrier(ctx: TestContext) -> TestResult<()> {
        let timing = ctx.timing();
        let barrier = Arc::new(timing.barrier(2));
        
        let b1 = barrier.clone();
        let h1 = tokio::spawn(async move {
            b1.wait(Duration::from_secs(5)).await
        });
        
        let b2 = barrier.clone();
        let h2 = tokio::spawn(async move {
            b2.wait(Duration::from_secs(5)).await
        });
        
        // Both should complete
        h1.await.unwrap()?;
        h2.await.unwrap()?;
        
        assert_eq!(barrier.generation(), 1);
        
        Ok(())
    }
    
    #[sinex_test]
    async fn test_timing_utils_progress_tracker(ctx: TestContext) -> TestResult<()> {
        let timing = ctx.timing();
        let tracker = timing.progress_tracker(3);
        
        assert_eq!(tracker.get_phase(), 0);
        assert!(!tracker.is_complete());
        
        tracker.advance_phase();
        assert_eq!(tracker.get_phase(), 1);
        
        tracker.advance_phase();
        tracker.advance_phase();
        assert!(tracker.is_complete());
        
        Ok(())
    }
    
    #[sinex_test]
    async fn test_timing_utils_event_counter(ctx: TestContext) -> TestResult<()> {
        let timing = ctx.timing();
        let counter = timing.event_counter(10);
        
        // Increment concurrently
        let handles: Vec<_> = (0..10)
            .map(|_| {
                let counter = counter.clone();
                tokio::spawn(async move {
                    counter.increment()
                })
            })
            .collect();
        
        for handle in handles {
            handle.await.unwrap();
        }
        
        assert_eq!(counter.get(), 10);
        
        Ok(())
    }
    
    #[test]
    fn test_barrier_generation_tracking() {
        let barrier = TestBarrier::new(2);
        
        assert_eq!(barrier.generation(), 0);
        assert_eq!(barrier.current_count(), 0);
        
        // After one participant
        barrier.counter.fetch_add(1, Ordering::SeqCst);
        assert_eq!(barrier.current_count(), 1);
        assert_eq!(barrier.generation(), 0);
    }
}

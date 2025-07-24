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
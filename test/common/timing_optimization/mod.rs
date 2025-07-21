// Timing optimization utilities to replace sleep-based synchronization

use crate::common::prelude::*;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio::sync::Notify;

/// Deterministic wait utilities for database conditions
// pub mod wait_helpers;

// Compatibility re-export for old import paths
// pub mod replacements {
//     pub use super::wait_helpers::*;
// }

// Re-export everything for convenience
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
    pub async fn wait(&self) -> AnyhowResult<(), tokio::time::error::Elapsed> {
        if self.condition.load(Ordering::Acquire) {
            return Ok(());
        }

        timeout(self.timeout_duration, self.notify.notified()).await
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

/// Counter-based synchronization for waiting on specific event counts
#[derive(Clone)]
pub struct EventCounter {
    count: Arc<AtomicUsize>,
    target: usize,
    notify: Arc<Notify>,
}

impl EventCounter {
    /// Create a new event counter that triggers at target count
    pub fn new(target: usize) -> Self {
        Self {
            count: Arc::new(AtomicUsize::new(0)),
            target,
            notify: Arc::new(Notify::new()),
        }
    }

    /// Increment the counter and notify if target reached
    pub fn increment(&self) -> usize {
        let new_count = self.count.fetch_add(1, Ordering::AcqRel) + 1;
        if new_count >= self.target {
            self.notify.notify_waiters();
        }
        new_count
    }

    /// Wait for the target count to be reached
    pub async fn wait_for_target(
        &self,
        timeout_duration: Duration,
    ) -> AnyhowResult<usize, tokio::time::error::Elapsed> {
        loop {
            let current = self.count.load(Ordering::Acquire);
            if current >= self.target {
                return Ok(current);
            }

            timeout(timeout_duration, self.notify.notified()).await?;
        }
    }

    /// Get current count
    pub fn get(&self) -> usize {
        self.count.load(Ordering::Acquire)
    }

    /// Reset counter
    pub fn reset(&self) {
        self.count.store(0, Ordering::Release);
    }
}

/// Progress tracker for multi-step operations
pub struct ProgressTracker {
    steps: Vec<Arc<AtomicBool>>,
    notify: Arc<Notify>,
}

impl ProgressTracker {
    /// Create a new progress tracker with specified number of steps
    pub fn new(step_count: usize) -> Self {
        let steps = (0..step_count)
            .map(|_| Arc::new(AtomicBool::new(false)))
            .collect();

        Self {
            steps,
            notify: Arc::new(Notify::new()),
        }
    }

    /// Mark a step as complete
    pub fn complete_step(&self, step_index: usize) -> bool {
        if step_index < self.steps.len() {
            let was_completed = self.steps[step_index].swap(true, Ordering::AcqRel);
            if !was_completed {
                self.notify.notify_waiters();
            }
            !was_completed // Return true if this was the first completion
        } else {
            false
        }
    }

    /// Wait for all steps to complete
    pub async fn wait_for_completion(
        &self,
        timeout_duration: Duration,
    ) -> AnyhowResult<(), tokio::time::error::Elapsed> {
        loop {
            if self.is_complete() {
                return Ok(());
            }

            timeout(timeout_duration, self.notify.notified()).await?;
        }
    }

    /// Check if all steps are complete
    pub fn is_complete(&self) -> bool {
        self.steps.iter().all(|step| step.load(Ordering::Acquire))
    }

    /// Get completion status of each step
    pub fn get_completion_status(&self) -> Vec<bool> {
        self.steps
            .iter()
            .map(|step| step.load(Ordering::Acquire))
            .collect()
    }

    /// Reset all steps
    pub fn reset(&self) {
        for step in &self.steps {
            step.store(false, Ordering::Release);
        }
    }
}

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
    pub async fn wait(
        &self,
        timeout_duration: Duration,
    ) -> AnyhowResult<(), tokio::time::error::Elapsed> {
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

                timeout(timeout_duration, self.notify.notified()).await?;
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

/// High-level timing utilities for common test patterns
pub mod patterns {
    use super::*;

    /// Wait for all workers to reach a checkpoint
    pub async fn wait_for_workers(
        worker_count: usize,
        timeout: Duration,
    ) -> AnyhowResult<TestBarrier, tokio::time::error::Elapsed> {
        let barrier = TestBarrier::new(worker_count);
        barrier.wait(timeout).await?;
        Ok(barrier)
    }

    /// Wait for a specific number of events to be processed
    pub async fn wait_for_event_processing(
        target_count: usize,
        timeout: Duration,
    ) -> AnyhowResult<EventCounter, tokio::time::error::Elapsed> {
        let counter = EventCounter::new(target_count);
        counter.wait_for_target(timeout).await?;
        Ok(counter)
    }

    /// Create a progress tracker for multi-phase testing
    pub fn create_test_phases(phase_names: &[&str]) -> (ProgressTracker, Vec<String>) {
        let tracker = ProgressTracker::new(phase_names.len());
        let names = phase_names.iter().map(|s| s.to_string()).collect();
        (tracker, names)
    }
}

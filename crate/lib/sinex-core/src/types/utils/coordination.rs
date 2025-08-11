//! Unified coordination primitive replacing EventCounter, ProgressTracker, and Barrier patterns
//!
//! This module provides a single, flexible coordination primitive that can handle:
//! - Event counting (like EventCounter)
//! - Boolean signaling (like Synchronizer)
//! - Multi-participant barriers (like TestBarrier)
//! - Progress tracking (like ProgressTracker)

use crate::error::{Result, SinexError};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Notify;

/// Reset behavior determines what happens when threshold is reached
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResetBehavior {
    /// Never reset - count only grows (EventCounter pattern)
    Never,
    /// Manual reset via reset() call (Synchronizer pattern)
    Manual,
    /// Automatic reset when threshold reached (Barrier pattern)
    Automatic,
}

/// Unified coordination primitive with threshold-based signaling
pub struct CoordinationPrimitive {
    state: Arc<AtomicUsize>,
    threshold: usize,
    reset_behavior: ResetBehavior,
    generation: Arc<AtomicUsize>, // Track reset cycles for barrier reuse
    notify: Arc<Notify>,
    name: String,
}

impl CoordinationPrimitive {
    /// Create a new coordination primitive
    pub fn new(threshold: usize, reset_behavior: ResetBehavior, name: impl Into<String>) -> Self {
        Self {
            state: Arc::new(AtomicUsize::new(0)),
            threshold,
            reset_behavior,
            generation: Arc::new(AtomicUsize::new(0)),
            notify: Arc::new(Notify::new()),
            name: name.into(),
        }
    }

    // ===== FACTORY METHODS FOR COMMON PATTERNS =====

    /// Create an event counter (never resets, counts toward target)
    pub fn event_counter(target: usize, name: impl Into<String>) -> Self {
        Self::new(target, ResetBehavior::Never, name)
    }

    /// Create a synchronizer (threshold=1, manual reset for boolean signaling)
    pub fn synchronizer(name: impl Into<String>) -> Self {
        Self::new(1, ResetBehavior::Manual, name)
    }

    /// Create a barrier (auto-reset when all participants arrive)
    pub fn barrier(participant_count: usize, name: impl Into<String>) -> Self {
        Self::new(participant_count, ResetBehavior::Automatic, name)
    }

    /// Create a progress tracker (like barrier but for step tracking)
    pub fn progress_tracker(step_count: usize, name: impl Into<String>) -> Self {
        Self::new(step_count, ResetBehavior::Manual, name)
    }

    // ===== CORE OPERATIONS =====

    /// Increment the state and notify waiters if threshold reached
    pub fn increment(&self) -> usize {
        let new_state = self.state.fetch_add(1, Ordering::AcqRel) + 1;
        self.check_threshold_and_notify(new_state);
        new_state
    }

    /// Add multiple to the state atomically
    pub fn add(&self, amount: usize) -> usize {
        let new_state = self.state.fetch_add(amount, Ordering::AcqRel) + amount;
        self.check_threshold_and_notify(new_state);
        new_state
    }

    /// Set state to specific value
    pub fn set(&self, value: usize) -> usize {
        let old_state = self.state.swap(value, Ordering::AcqRel);
        self.check_threshold_and_notify(value);
        old_state
    }

    /// Wait for the threshold to be reached
    pub async fn wait_for_threshold(&self, timeout_duration: Duration) -> Result<usize> {
        let start = Instant::now();
        let initial_generation = self.generation.load(Ordering::Acquire);

        loop {
            let current = self.state.load(Ordering::Acquire);
            let current_generation = self.generation.load(Ordering::Acquire);

            // Check if threshold reached or generation changed (barrier reset)
            if current >= self.threshold || current_generation > initial_generation {
                return Ok(current);
            }

            if start.elapsed() >= timeout_duration {
                return Err(SinexError::timeout(format!(
                    "CoordinationPrimitive '{}' did not reach threshold {} within {:?} (current: {})",
                    self.name, self.threshold, timeout_duration, current
                )));
            }

            let remaining = timeout_duration.saturating_sub(start.elapsed());
            if remaining.is_zero() {
                break;
            }

            tokio::time::timeout(remaining, self.notify.notified())
                .await
                .map_err(|_| {
                    SinexError::timeout(format!(
                        "CoordinationPrimitive '{}' timeout waiting for threshold {}",
                        self.name, self.threshold
                    ))
                })?;
        }

        let final_state = self.state.load(Ordering::Acquire);
        Err(SinexError::timeout(format!(
            "CoordinationPrimitive '{}' did not reach threshold {} (final: {})",
            self.name, self.threshold, final_state
        )))
    }

    // ===== CONVENIENCE METHODS FOR SPECIFIC PATTERNS =====

    /// Signal condition met (Synchronizer pattern - sets to threshold)
    pub fn signal(&self) -> usize {
        self.set(self.threshold)
    }

    /// Wait with barrier semantics (blocks until all participants arrive)
    pub async fn wait(&self, timeout_duration: Duration) -> Result<()> {
        // For barriers, we increment and wait for threshold
        if self.reset_behavior == ResetBehavior::Automatic {
            let current_generation = self.generation.load(Ordering::Acquire);
            let count = self.increment();

            if count == self.threshold {
                // We were the last participant - barrier opens immediately
                Ok(())
            } else {
                // Wait for last participant or generation change
                loop {
                    let new_generation = self.generation.load(Ordering::Acquire);
                    if new_generation > current_generation {
                        return Ok(());
                    }

                    tokio::time::timeout(timeout_duration / 10, self.notify.notified())
                        .await
                        .map_err(|_| {
                            SinexError::timeout(format!(
                                "Barrier '{}' timeout waiting for participants",
                                self.name
                            ))
                        })?;
                }
            }
        } else {
            // For non-barriers, just wait for threshold
            self.wait_for_threshold(timeout_duration).await.map(|_| ())
        }
    }

    /// Reset state to zero (Manual reset for Synchronizer)
    pub fn reset(&self) {
        let old_state = self.state.swap(0, Ordering::AcqRel);
        tracing::debug!(
            "CoordinationPrimitive '{}' reset from {} to 0",
            self.name,
            old_state
        );
    }

    /// Reset to specific value
    pub fn reset_to(&self, value: usize) {
        let old_state = self.state.swap(value, Ordering::AcqRel);
        tracing::debug!(
            "CoordinationPrimitive '{}' reset from {} to {}",
            self.name,
            old_state,
            value
        );
    }

    // ===== STATE INSPECTION =====

    /// Get current state without blocking
    pub fn get(&self) -> usize {
        self.state.load(Ordering::Acquire)
    }

    /// Get threshold value
    pub fn threshold(&self) -> usize {
        self.threshold
    }

    /// Check if threshold has been reached
    pub fn is_ready(&self) -> bool {
        self.get() >= self.threshold
    }

    /// Get current generation (for barrier reuse tracking)
    pub fn generation(&self) -> usize {
        self.generation.load(Ordering::Acquire)
    }

    /// Get primitive name for logging/debugging
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get reset behavior
    pub fn reset_behavior(&self) -> ResetBehavior {
        self.reset_behavior
    }

    // ===== INTERNAL METHODS =====

    fn check_threshold_and_notify(&self, new_state: usize) {
        if new_state >= self.threshold {
            match self.reset_behavior {
                ResetBehavior::Automatic => {
                    // Barrier pattern - reset and increment generation
                    self.state.store(0, Ordering::Release);
                    self.generation.fetch_add(1, Ordering::AcqRel);
                    tracing::debug!(
                        "Barrier '{}' reached threshold {} - auto-reset and generation increment",
                        self.name,
                        self.threshold
                    );
                }
                _ => {
                    tracing::debug!(
                        "CoordinationPrimitive '{}' reached threshold: {}",
                        self.name,
                        new_state
                    );
                }
            }
            self.notify.notify_waiters();
        }
    }
}

impl Clone for CoordinationPrimitive {
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
            threshold: self.threshold,
            reset_behavior: self.reset_behavior,
            generation: self.generation.clone(),
            notify: self.notify.clone(),
            name: self.name.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinex_test_utils::sinex_test;
    use tokio::time::sleep;

    #[sinex_test]
    async fn test_event_counter_pattern() -> color_eyre::eyre::Result<()> {
        let counter = CoordinationPrimitive::event_counter(3, "test_counter");

        assert_eq!(counter.get(), 0);
        assert!(!counter.is_ready());

        assert_eq!(counter.increment(), 1);
        assert_eq!(counter.increment(), 2);
        assert!(!counter.is_ready());

        assert_eq!(counter.increment(), 3);
        assert!(counter.is_ready());

        // Should be able to wait immediately since threshold is reached
        let result = counter.wait_for_threshold(Duration::from_millis(10)).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 3);
        Ok(())
    }

    #[sinex_test]
    async fn test_synchronizer_pattern() -> color_eyre::eyre::Result<()> {
        let sync = CoordinationPrimitive::synchronizer("test_sync");

        assert!(!sync.is_ready());

        sync.signal();
        assert!(sync.is_ready());

        // Should complete immediately
        let result = sync.wait(Duration::from_millis(10)).await;
        assert!(result.is_ok());

        // Reset and test again
        sync.reset();
        assert!(!sync.is_ready());
        Ok(())
    }

    #[sinex_test]
    async fn test_barrier_pattern() -> color_eyre::eyre::Result<()> {
        let barrier = CoordinationPrimitive::barrier(3, "test_barrier");
        let barrier_clone = barrier.clone();

        let initial_generation = barrier.generation();

        // Simulate 3 participants arriving at barrier
        let handles = (0..3)
            .map(|i| {
                let barrier = barrier.clone();
                tokio::spawn(async move {
                    sleep(Duration::from_millis(i * 10)).await;
                    barrier.wait(Duration::from_secs(1)).await
                })
            })
            .collect::<Vec<_>>();

        // All should complete
        for handle in handles {
            assert!(handle.await.unwrap().is_ok());
        }

        // Generation should have incremented (barrier reset)
        assert!(barrier_clone.generation() > initial_generation);
        assert_eq!(barrier_clone.get(), 0); // Should be reset
        Ok(())
    }

    #[sinex_test]
    async fn test_timeout() -> color_eyre::eyre::Result<()> {
        let counter = CoordinationPrimitive::event_counter(5, "timeout_test");

        // Should timeout since target is never reached
        let result = counter.wait_for_threshold(Duration::from_millis(100)).await;
        assert!(result.is_err());
        Ok(())
    }

    #[sinex_test]
    async fn test_event_counter_factory_method() -> color_eyre::eyre::Result<()> {
        let counter = CoordinationPrimitive::event_counter(100, "test_events");

        // Initial state
        assert_eq!(counter.get(), 0);
        assert!(!counter.is_ready());
        assert_eq!(counter.name(), "test_events");
        assert_eq!(counter.threshold(), 100);

        // Increment operations
        counter.add(50);
        assert_eq!(counter.get(), 50);
        assert!(!counter.is_ready());

        counter.add(30);
        assert_eq!(counter.get(), 80);
        assert!(!counter.is_ready());

        // Reach threshold
        counter.add(20);
        assert_eq!(counter.get(), 100);
        assert!(counter.is_ready());

        // Event counter never resets automatically
        counter.add(10);
        assert_eq!(counter.get(), 110);
        assert!(counter.is_ready());

        Ok(())
    }

    #[sinex_test]
    async fn test_concurrent_operations() -> color_eyre::eyre::Result<()> {
        let counter = Arc::new(CoordinationPrimitive::event_counter(
            1000,
            "concurrent_test",
        ));
        let mut handles = vec![];

        // Spawn 10 tasks adding 100 each = 1000 total
        for _ in 0..10 {
            let counter_clone = counter.clone();
            let handle = tokio::spawn(async move {
                for _ in 0..100 {
                    counter_clone.add(1);
                }
            });
            handles.push(handle);
        }

        // Wait for all tasks
        for handle in handles {
            handle.await.unwrap();
        }

        assert_eq!(counter.get(), 1000);
        assert!(counter.is_ready());

        Ok(())
    }

    #[sinex_test]
    async fn test_edge_cases() -> color_eyre::eyre::Result<()> {
        // Zero threshold
        let zero_barrier = CoordinationPrimitive::barrier(0, "zero_test");
        assert!(zero_barrier.is_ready()); // Should be immediately complete

        // Large threshold
        let large_counter = CoordinationPrimitive::event_counter(usize::MAX, "large_test");
        large_counter.add(1000);
        assert!(!large_counter.is_ready());

        // Empty name
        let unnamed = CoordinationPrimitive::synchronizer("");
        assert_eq!(unnamed.name(), "");
        unnamed.signal();
        assert!(unnamed.is_ready());

        Ok(())
    }
}

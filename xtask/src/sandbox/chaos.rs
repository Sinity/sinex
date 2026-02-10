//! Chaos testing utilities for resilience verification.
//!
//! This module provides comprehensive chaos injection capabilities for testing
//! system behavior under adverse conditions:
//!
//! - **ChaosConfig/ChaosInjector**: Low-level latency and failure injection
//! - **`ChaosTestBuilder`**: Builder for constructing chaos test scenarios
//! - **`ChaosScenarios`**: High-level scenario orchestration (partitions, message loss, etc.)
//!
//! # Example
//!
//! ```rust,ignore
//! use xtask::sandbox::chaos::{ChaosTestBuilder, ChaosScenarios};
//!
//! // Build a chaos scenario with message corruption and reordering
//! let chaos = ChaosTestBuilder::new()
//!     .with_message_corruption(0.1)   // 10% corruption rate
//!     .with_reordering(0.2)           // 20% reordering rate
//!     .with_latency(Duration::from_millis(50))
//!     .build();
//!
//! // Run network partition scenario
//! let scenarios = ChaosScenarios::new();
//! scenarios.network_partition(Duration::from_secs(2)).await?;
//! ```

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use color_eyre::eyre::{eyre, Result};
use parking_lot::Mutex;
use rand::seq::SliceRandom;
use rand::Rng;
use serde_json::Value as JsonValue;
use sinex_primitives::Event;
use tokio::time::sleep;

// ============================================================================
// Core Chaos Configuration
// ============================================================================

/// Chaos injection settings.
#[derive(Clone, Copy, Debug)]
pub struct ChaosConfig {
    pub latency: Duration,
    pub failure_rate: f64,
}

impl ChaosConfig {
    #[must_use]
    pub fn new(latency: Duration, failure_rate: f64) -> Self {
        Self {
            latency,
            failure_rate: failure_rate.clamp(0.0, 1.0),
        }
    }

    /// Apply latency then randomly fail based on `failure_rate`.
    pub async fn inject<T, F, Fut>(&self, op: F) -> Result<T>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        if !self.latency.is_zero() {
            sleep(self.latency).await;
        }
        if self.failure_rate > 0.0 {
            let mut rng = rand::thread_rng();
            if rng.gen_bool(self.failure_rate) {
                return Err(eyre!("chaos: induced failure"));
            }
        }
        op().await
    }

    /// Simulate a transient partition by sleeping for the given duration.
    pub async fn partition(&self, duration: Duration) {
        let delay = if duration.is_zero() {
            self.latency
        } else {
            duration
        };
        if !delay.is_zero() {
            sleep(delay).await;
        }
    }
}

/// High-level chaos helper used by integration/chaos suites.
#[derive(Clone, Debug)]
pub struct ChaosInjector {
    config: ChaosConfig,
}

/// Backwards-compatible alias for `ChaosInjector`.
pub type ChaosInjestor = ChaosInjector;

impl ChaosInjector {
    #[must_use]
    pub fn new(latency: Duration, failure_rate: f64) -> Self {
        Self {
            config: ChaosConfig::new(latency, failure_rate),
        }
    }

    /// Execute an async operation with optional simulated failures/latency.
    pub async fn with_simulated_failures<F, Fut, T>(&self, op: F) -> Result<T>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        self.config.inject(op).await
    }

    /// Simulate a temporary network partition.
    pub async fn simulate_network_partition(&self) -> Result<()> {
        self.config.partition(Duration::ZERO).await;
        Ok(())
    }

    /// Simulate a database crash for callers that expect a failure.
    pub async fn simulate_database_crash(&self) -> Result<()> {
        Err(eyre!("simulated database crash"))
    }
}

// ============================================================================
// Chaos Metrics
// ============================================================================

/// Metrics collected during chaos testing.
#[derive(Debug, Default)]
pub struct ChaosMetrics {
    /// Messages successfully processed
    pub processed: AtomicU64,
    /// Messages that failed processing
    pub failed: AtomicU64,
    /// Messages corrupted by chaos
    pub corrupted: AtomicU64,
    /// Messages reordered by chaos
    pub reordered: AtomicU64,
    /// Messages dropped by chaos
    pub dropped: AtomicU64,
    /// Messages delayed by chaos
    pub delayed: AtomicU64,
    /// Network partitions simulated
    pub partitions: AtomicU64,
    /// Recovery attempts made
    pub recovery_attempts: AtomicU64,
    /// Successful recoveries
    pub successful_recoveries: AtomicU64,
}

impl ChaosMetrics {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_processed(&self) {
        self.processed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_failed(&self) {
        self.failed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_corrupted(&self) {
        self.corrupted.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_reordered(&self) {
        self.reordered.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_dropped(&self) {
        self.dropped.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_delayed(&self) {
        self.delayed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_partition(&self) {
        self.partitions.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_recovery_attempt(&self) {
        self.recovery_attempts.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_successful_recovery(&self) {
        self.successful_recoveries.fetch_add(1, Ordering::Relaxed);
    }

    /// Get a summary snapshot of all metrics.
    pub fn snapshot(&self) -> ChaosMetricsSnapshot {
        ChaosMetricsSnapshot {
            processed: self.processed.load(Ordering::Relaxed),
            failed: self.failed.load(Ordering::Relaxed),
            corrupted: self.corrupted.load(Ordering::Relaxed),
            reordered: self.reordered.load(Ordering::Relaxed),
            dropped: self.dropped.load(Ordering::Relaxed),
            delayed: self.delayed.load(Ordering::Relaxed),
            partitions: self.partitions.load(Ordering::Relaxed),
            recovery_attempts: self.recovery_attempts.load(Ordering::Relaxed),
            successful_recoveries: self.successful_recoveries.load(Ordering::Relaxed),
        }
    }
}

/// Snapshot of chaos metrics at a point in time.
#[derive(Debug, Clone, Default)]
pub struct ChaosMetricsSnapshot {
    pub processed: u64,
    pub failed: u64,
    pub corrupted: u64,
    pub reordered: u64,
    pub dropped: u64,
    pub delayed: u64,
    pub partitions: u64,
    pub recovery_attempts: u64,
    pub successful_recoveries: u64,
}

impl std::fmt::Display for ChaosMetricsSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ChaosMetrics {{ processed: {}, failed: {}, corrupted: {}, reordered: {}, \
             dropped: {}, delayed: {}, partitions: {}, recovery_attempts: {}, \
             successful_recoveries: {} }}",
            self.processed,
            self.failed,
            self.corrupted,
            self.reordered,
            self.dropped,
            self.delayed,
            self.partitions,
            self.recovery_attempts,
            self.successful_recoveries
        )
    }
}

// ============================================================================
// Chaos Test Builder
// ============================================================================

/// Builder for constructing chaos test configurations.
///
/// Provides fine-grained control over what types of chaos to inject:
/// - Message corruption (garbled payloads)
/// - Message reordering (out-of-sequence delivery)
/// - Message dropping (simulated packet loss)
/// - Latency injection (slow processing)
/// - Slow consumer simulation
#[derive(Clone, Debug)]
pub struct ChaosTestBuilder {
    corruption_rate: f64,
    reorder_rate: f64,
    drop_rate: f64,
    latency: Duration,
    latency_jitter: Duration,
    slow_consumer_delay: Option<Duration>,
    failure_rate: f64,
    metrics: Arc<ChaosMetrics>,
}

impl Default for ChaosTestBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl ChaosTestBuilder {
    /// Create a new chaos test builder with no chaos enabled.
    #[must_use]
    pub fn new() -> Self {
        Self {
            corruption_rate: 0.0,
            reorder_rate: 0.0,
            drop_rate: 0.0,
            latency: Duration::ZERO,
            latency_jitter: Duration::ZERO,
            slow_consumer_delay: None,
            failure_rate: 0.0,
            metrics: Arc::new(ChaosMetrics::new()),
        }
    }

    /// Set message corruption rate (0.0 to 1.0).
    /// Corrupted messages will have their payloads garbled.
    #[must_use]
    pub fn with_message_corruption(mut self, rate: f64) -> Self {
        self.corruption_rate = rate.clamp(0.0, 1.0);
        self
    }

    /// Set message reordering rate (0.0 to 1.0).
    /// Reordered messages may be delivered out of sequence.
    #[must_use]
    pub fn with_reordering(mut self, rate: f64) -> Self {
        self.reorder_rate = rate.clamp(0.0, 1.0);
        self
    }

    /// Set message drop rate (0.0 to 1.0).
    /// Dropped messages will be silently discarded.
    #[must_use]
    pub fn with_drop_rate(mut self, rate: f64) -> Self {
        self.drop_rate = rate.clamp(0.0, 1.0);
        self
    }

    /// Set base latency to inject on each message.
    #[must_use]
    pub fn with_latency(mut self, latency: Duration) -> Self {
        self.latency = latency;
        self
    }

    /// Set latency jitter (random additional delay up to this amount).
    #[must_use]
    pub fn with_latency_jitter(mut self, jitter: Duration) -> Self {
        self.latency_jitter = jitter;
        self
    }

    /// Enable slow consumer simulation with the given delay per message.
    #[must_use]
    pub fn with_slow_consumer(mut self, delay: Duration) -> Self {
        self.slow_consumer_delay = Some(delay);
        self
    }

    /// Set general failure rate (0.0 to 1.0).
    /// Operations will randomly fail with this probability.
    #[must_use]
    pub fn with_failure_rate(mut self, rate: f64) -> Self {
        self.failure_rate = rate.clamp(0.0, 1.0);
        self
    }

    /// Use existing metrics instance for tracking.
    pub fn with_metrics(mut self, metrics: Arc<ChaosMetrics>) -> Self {
        self.metrics = metrics;
        self
    }

    /// Build the chaos context.
    #[must_use]
    pub fn build(self) -> ChaosContext {
        ChaosContext {
            corruption_rate: self.corruption_rate,
            reorder_rate: self.reorder_rate,
            drop_rate: self.drop_rate,
            latency: self.latency,
            latency_jitter: self.latency_jitter,
            slow_consumer_delay: self.slow_consumer_delay,
            failure_rate: self.failure_rate,
            metrics: self.metrics,
            reorder_buffer: Arc::new(Mutex::new(VecDeque::new())),
        }
    }
}

/// Active chaos context for injecting failures into message processing.
#[derive(Clone)]
pub struct ChaosContext {
    corruption_rate: f64,
    reorder_rate: f64,
    drop_rate: f64,
    latency: Duration,
    latency_jitter: Duration,
    slow_consumer_delay: Option<Duration>,
    failure_rate: f64,
    metrics: Arc<ChaosMetrics>,
    reorder_buffer: Arc<Mutex<VecDeque<Event<JsonValue>>>>,
}

impl ChaosContext {
    /// Get access to the metrics tracker.
    #[must_use]
    pub fn metrics(&self) -> &ChaosMetrics {
        &self.metrics
    }

    /// Check if a message should be dropped.
    #[must_use]
    pub fn should_drop(&self) -> bool {
        if self.drop_rate > 0.0 {
            let mut rng = rand::thread_rng();
            if rng.gen_bool(self.drop_rate) {
                self.metrics.record_dropped();
                return true;
            }
        }
        false
    }

    /// Apply chaos to an event, potentially corrupting it.
    #[must_use]
    pub fn maybe_corrupt(&self, mut event: Event<JsonValue>) -> Event<JsonValue> {
        if self.corruption_rate > 0.0 {
            let mut rng = rand::thread_rng();
            if rng.gen_bool(self.corruption_rate) {
                self.metrics.record_corrupted();
                // Corrupt the payload by replacing with garbage
                event.payload = JsonValue::String(format!("CORRUPTED_{}", rng.gen::<u64>()));
            }
        }
        event
    }

    /// Buffer event for potential reordering.
    /// Returns events that should be processed (potentially reordered).
    #[must_use]
    pub fn buffer_for_reorder(&self, event: Event<JsonValue>) -> Vec<Event<JsonValue>> {
        if self.reorder_rate > 0.0 {
            let mut rng = rand::thread_rng();
            let mut buffer = self.reorder_buffer.lock();
            buffer.push_back(event);

            // Randomly decide to flush some events in random order
            if rng.gen_bool(self.reorder_rate) && buffer.len() >= 2 {
                self.metrics.record_reordered();
                let mut events: Vec<_> = buffer.drain(..).collect();
                events.shuffle(&mut rng);
                return events;
            }

            // Or just return oldest if buffer is getting large
            if buffer.len() > 10 {
                return buffer.drain(..5).collect();
            }

            Vec::new()
        } else {
            vec![event]
        }
    }

    /// Flush any remaining buffered events.
    #[must_use]
    pub fn flush_reorder_buffer(&self) -> Vec<Event<JsonValue>> {
        self.reorder_buffer.lock().drain(..).collect()
    }

    /// Apply latency injection.
    pub async fn apply_latency(&self) {
        if !self.latency.is_zero() || !self.latency_jitter.is_zero() {
            let mut rng = rand::thread_rng();
            let jitter = if self.latency_jitter.is_zero() {
                Duration::ZERO
            } else {
                Duration::from_millis(rng.gen_range(0..self.latency_jitter.as_millis() as u64))
            };
            let total_delay = self.latency + jitter;
            if !total_delay.is_zero() {
                self.metrics.record_delayed();
                sleep(total_delay).await;
            }
        }
    }

    /// Apply slow consumer delay if configured.
    pub async fn apply_slow_consumer_delay(&self) {
        if let Some(delay) = self.slow_consumer_delay {
            sleep(delay).await;
        }
    }

    /// Check if operation should fail randomly.
    #[must_use]
    pub fn should_fail(&self) -> bool {
        if self.failure_rate > 0.0 {
            let mut rng = rand::thread_rng();
            if rng.gen_bool(self.failure_rate) {
                self.metrics.record_failed();
                return true;
            }
        }
        false
    }

    /// Process an event through the chaos pipeline.
    /// Returns None if the event should be dropped.
    pub async fn process_event(&self, event: Event<JsonValue>) -> Option<Vec<Event<JsonValue>>> {
        // Check for drop
        if self.should_drop() {
            return None;
        }

        // Apply latency
        self.apply_latency().await;

        // Apply slow consumer delay
        self.apply_slow_consumer_delay().await;

        // Maybe corrupt
        let event = self.maybe_corrupt(event);

        // Buffer for reordering.
        // Returns Some(events) even if empty (event buffered for later),
        // None only means "dropped" (decided above).
        let events = self.buffer_for_reorder(event);
        Some(events)
    }
}

// ============================================================================
// Chaos Scenarios
// ============================================================================

/// High-level chaos scenario orchestrator.
///
/// Provides pre-built chaos scenarios for common resilience testing patterns:
/// - Network partitions (connection loss/recovery)
/// - Message loss bursts
/// - Checkpoint survival testing
/// - Worst-case combined chaos
#[derive(Clone)]
pub struct ChaosScenarios {
    metrics: Arc<ChaosMetrics>,
    partition_active: Arc<std::sync::atomic::AtomicBool>,
}

impl Default for ChaosScenarios {
    fn default() -> Self {
        Self::new()
    }
}

impl ChaosScenarios {
    /// Create a new chaos scenarios orchestrator.
    #[must_use]
    pub fn new() -> Self {
        Self {
            metrics: Arc::new(ChaosMetrics::new()),
            partition_active: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    /// Create with existing metrics tracker.
    pub fn with_metrics(metrics: Arc<ChaosMetrics>) -> Self {
        Self {
            metrics,
            partition_active: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    /// Get access to the metrics tracker.
    #[must_use]
    pub fn metrics(&self) -> &ChaosMetrics {
        &self.metrics
    }

    /// Check if a network partition is currently active.
    #[must_use]
    pub fn is_partition_active(&self) -> bool {
        self.partition_active.load(Ordering::SeqCst)
    }

    /// Simulate a network partition for the given duration.
    ///
    /// During the partition:
    /// - New connections should fail
    /// - Existing operations should timeout
    /// - The system should detect and handle the partition
    ///
    /// After the partition:
    /// - Connections should recover
    /// - Pending work should be retried
    pub async fn network_partition(&self, duration: Duration) -> Result<()> {
        self.metrics.record_partition();
        self.partition_active.store(true, Ordering::SeqCst);

        sleep(duration).await;

        self.partition_active.store(false, Ordering::SeqCst);
        Ok(())
    }

    /// Simulate a partition with a recovery callback.
    ///
    /// The callback is invoked after the partition ends to verify recovery.
    pub async fn network_partition_with_recovery<F, Fut>(
        &self,
        duration: Duration,
        recovery_check: F,
    ) -> Result<bool>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = bool>,
    {
        self.network_partition(duration).await?;

        // Give system time to detect recovery
        sleep(Duration::from_millis(100)).await;

        self.metrics.record_recovery_attempt();
        let recovered = recovery_check().await;

        if recovered {
            self.metrics.record_successful_recovery();
        }

        Ok(recovered)
    }

    /// Simulate message loss burst (drop N consecutive messages).
    pub async fn message_loss_burst(&self, count: u64) {
        for _ in 0..count {
            self.metrics.record_dropped();
        }
    }

    /// Create a chaos context configured for checkpoint survival testing.
    ///
    /// This scenario:
    /// - Drops some messages to test checkpoint recovery
    /// - Injects latency to simulate slow processing
    /// - Does NOT corrupt messages (to verify checkpoint integrity)
    #[must_use]
    pub fn checkpoint_survival_context(&self) -> ChaosContext {
        ChaosTestBuilder::new()
            .with_drop_rate(0.2) // 20% message loss
            .with_latency(Duration::from_millis(10))
            .with_metrics(self.metrics.clone())
            .build()
    }

    /// Create a chaos context configured for worst-case testing.
    ///
    /// This scenario combines:
    /// - Message corruption
    /// - Message reordering
    /// - Message drops
    /// - High latency with jitter
    /// - Random failures
    #[must_use]
    pub fn worst_case_context(&self) -> ChaosContext {
        ChaosTestBuilder::new()
            .with_message_corruption(0.05) // 5% corruption
            .with_reordering(0.1) // 10% reordering
            .with_drop_rate(0.1) // 10% drops
            .with_latency(Duration::from_millis(50))
            .with_latency_jitter(Duration::from_millis(100))
            .with_failure_rate(0.05) // 5% random failures
            .with_metrics(self.metrics.clone())
            .build()
    }

    /// Create a chaos context for slow consumer testing.
    ///
    /// This scenario:
    /// - Simulates a slow consumer that can't keep up
    /// - Tests backpressure handling
    #[must_use]
    pub fn slow_consumer_context(&self, delay_per_message: Duration) -> ChaosContext {
        ChaosTestBuilder::new()
            .with_slow_consumer(delay_per_message)
            .with_metrics(self.metrics.clone())
            .build()
    }

    /// Run a chaos scenario that alternates between partitions and recovery.
    ///
    /// Useful for testing sustained resilience over time.
    pub async fn intermittent_partitions(
        &self,
        partition_duration: Duration,
        recovery_duration: Duration,
        cycles: u32,
    ) -> Result<()> {
        for _ in 0..cycles {
            self.network_partition(partition_duration).await?;
            sleep(recovery_duration).await;
        }
        Ok(())
    }
}

// ============================================================================
// Chaos Event Wrapper
// ============================================================================

/// Result of processing an event through chaos.
#[derive(Debug)]
pub enum ChaosEventResult {
    /// Event was processed normally
    Processed(Event<JsonValue>),
    /// Event was dropped
    Dropped,
    /// Event was corrupted (original preserved for comparison)
    Corrupted {
        original: Event<JsonValue>,
        corrupted: Event<JsonValue>,
    },
    /// Processing failed
    Failed(String),
}

/// Wrapper for processing events through a chaos context with detailed results.
pub struct ChaosEventProcessor {
    context: ChaosContext,
}

impl ChaosEventProcessor {
    #[must_use]
    pub fn new(context: ChaosContext) -> Self {
        Self { context }
    }

    /// Process an event and return detailed result.
    pub async fn process(&self, event: Event<JsonValue>) -> ChaosEventResult {
        // Check for drop
        if self.context.should_drop() {
            return ChaosEventResult::Dropped;
        }

        // Check for random failure
        if self.context.should_fail() {
            return ChaosEventResult::Failed("chaos: random failure".to_string());
        }

        // Apply latency
        self.context.apply_latency().await;

        // Apply slow consumer delay
        self.context.apply_slow_consumer_delay().await;

        // Check for corruption
        let original = event.clone();
        let processed = self.context.maybe_corrupt(event);

        if original.payload == processed.payload {
            ChaosEventResult::Processed(processed)
        } else {
            ChaosEventResult::Corrupted {
                original,
                corrupted: processed,
            }
        }
    }

    /// Get access to the underlying context.
    #[must_use]
    pub fn context(&self) -> &ChaosContext {
        &self.context
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_chaos_config_clamps_failure_rate() {
        let config = ChaosConfig::new(Duration::ZERO, 1.5);
        assert_eq!(config.failure_rate, 1.0);

        let config = ChaosConfig::new(Duration::ZERO, -0.5);
        assert_eq!(config.failure_rate, 0.0);
    }

    #[test]
    fn test_chaos_test_builder_defaults() {
        let ctx = ChaosTestBuilder::new().build();
        assert!(!ctx.should_drop());
        assert!(!ctx.should_fail());
    }

    #[test]
    fn test_chaos_metrics_snapshot() {
        let metrics = ChaosMetrics::new();
        metrics.record_processed();
        metrics.record_processed();
        metrics.record_failed();

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.processed, 2);
        assert_eq!(snapshot.failed, 1);
    }

    #[test]
    fn test_chaos_scenarios_partition_flag() {
        let scenarios = ChaosScenarios::new();
        assert!(!scenarios.is_partition_active());

        scenarios.partition_active.store(true, Ordering::SeqCst);
        assert!(scenarios.is_partition_active());
    }
}

//! Enhanced channel operations with performance monitoring and error reporting
//!
//! This module provides production-ready enhancements to the existing channel helpers,
//! including performance measurement, health monitoring, and advanced error reporting.

use crate::{CoreError, Result, EventSender, RawEvent};
use crate::channel_helpers::{ChannelMonitor, ChannelStats};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::{Instant, timeout};

/// Enhanced event sender with comprehensive monitoring and error reporting
pub struct EnhancedEventSender {
    inner: EventSender,
    monitor: Arc<ChannelMonitor>,
    source_name: String,
    performance_tracker: Arc<PerformanceTracker>,
}

impl EnhancedEventSender {
    /// Create a new enhanced event sender
    pub fn new(sender: EventSender, source_name: String) -> Self {
        Self {
            inner: sender,
            monitor: Arc::new(ChannelMonitor::new()),
            source_name,
            performance_tracker: Arc::new(PerformanceTracker::new()),
        }
    }

    /// Send an event with enhanced monitoring and error reporting
    pub async fn send_event(&self, event: RawEvent, context: &str) -> Result<()> {
        let start_time = Instant::now();
        let event_type = event.event_type.clone();
        
        self.performance_tracker.record_send_attempt();

        match self.inner.send(event).await {
            Ok(()) => {
                self.monitor.record_send();
                self.performance_tracker.record_send_success(start_time.elapsed());
                tracing::trace!("[{}] Sent {} event: {}", self.source_name, event_type, context);
                Ok(())
            }
            Err(e) => {
                let error_msg = format!("Failed to send {} event: {}", event_type, e);
                self.monitor.record_error(error_msg.clone());
                self.performance_tracker.record_send_failure(start_time.elapsed());
                
                tracing::error!("[{}] {}: {}", self.source_name, error_msg, context);
                
                Err(CoreError::Other(format!(
                    "{} (source: {}, event_type: {}, context: {}, duration_ms: {})",
                    error_msg, self.source_name, event_type, context, start_time.elapsed().as_millis()
                )))
            }
        }
    }

    /// Send event with timeout and enhanced error reporting
    pub async fn send_event_timeout(
        &self,
        event: RawEvent,
        timeout_duration: Duration,
        context: &str,
    ) -> Result<()> {
        let start_time = Instant::now();
        let event_type = event.event_type.clone();

        match timeout(timeout_duration, self.send_event(event, context)).await {
            Ok(result) => result,
            Err(_) => {
                let error_msg = format!("Send timeout for {} event after {:?}", event_type, timeout_duration);
                self.monitor.record_error(error_msg.clone());
                self.performance_tracker.record_send_failure(start_time.elapsed());
                
                Err(CoreError::Other(format!(
                    "{} (source: {}, event_type: {}, timeout: {:?}, context: {})",
                    error_msg, self.source_name, event_type, timeout_duration, context
                )))
            }
        }
    }

    /// Send events in batch with performance monitoring
    pub async fn send_batch(&self, events: Vec<RawEvent>, context: &str) -> Result<BatchSendResult> {
        let start_time = Instant::now();
        let total_events = events.len();
        let mut successful = 0;
        let mut failed = 0;
        let mut errors = Vec::new();

        for (i, event) in events.into_iter().enumerate() {
            let item_context = format!("{}_batch_item_{}", context, i);
            match self.send_event(event, &item_context).await {
                Ok(()) => successful += 1,
                Err(e) => {
                    failed += 1;
                    errors.push(e);
                }
            }
        }

        let duration = start_time.elapsed();
        let result = BatchSendResult {
            total_events,
            successful,
            failed,
            duration,
            errors,
        };

        tracing::info!(
            "[{}] Batch send completed: {}/{} successful in {:?}",
            self.source_name, successful, total_events, duration
        );

        Ok(result)
    }

    /// Get current channel statistics
    pub fn stats(&self) -> ChannelStats {
        self.monitor.stats()
    }

    /// Get performance metrics
    pub fn performance_metrics(&self) -> PerformanceMetrics {
        self.performance_tracker.get_metrics()
    }

    /// Get comprehensive health report
    pub fn health_report(&self) -> ChannelHealthReport {
        let stats = self.stats();
        let perf = self.performance_metrics();
        
        ChannelHealthReport {
            source_name: self.source_name.clone(),
            sent_count: stats.sent,
            error_count: stats.errors,
            queue_depth: stats.queue_depth,
            average_send_latency: perf.average_send_latency,
            success_rate: perf.success_rate,
            throughput_per_second: perf.throughput_per_second,
            last_error: stats.last_error,
            is_healthy: self.is_healthy(),
        }
    }

    /// Check if the channel is healthy
    pub fn is_healthy(&self) -> bool {
        let stats = self.stats();
        let perf = self.performance_metrics();
        
        // Healthy if:
        // - Error rate is low (< 5%)
        // - Queue depth is reasonable (< 1000)
        // - Average latency is acceptable (< 100ms)
        perf.success_rate > 0.95 &&
        stats.queue_depth < 1000 &&
        perf.average_send_latency < Duration::from_millis(100)
    }

    /// Reset monitoring counters (useful for testing or periodic resets)
    pub fn reset_monitoring(&self) {
        self.performance_tracker.reset();
        // Note: ChannelMonitor doesn't have reset, but we could add it if needed
    }
}

/// Performance tracking for channel operations
#[derive(Debug)]
pub struct PerformanceTracker {
    send_attempts: AtomicU64,
    send_successes: AtomicU64,
    send_failures: AtomicU64,
    total_send_duration: AtomicU64, // in nanoseconds
    start_time: Instant,
}

impl PerformanceTracker {
    pub fn new() -> Self {
        Self {
            send_attempts: AtomicU64::new(0),
            send_successes: AtomicU64::new(0),
            send_failures: AtomicU64::new(0),
            total_send_duration: AtomicU64::new(0),
            start_time: Instant::now(),
        }
    }

    pub fn record_send_attempt(&self) {
        self.send_attempts.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_send_success(&self, duration: Duration) {
        self.send_successes.fetch_add(1, Ordering::Relaxed);
        self.total_send_duration.fetch_add(duration.as_nanos() as u64, Ordering::Relaxed);
    }

    pub fn record_send_failure(&self, duration: Duration) {
        self.send_failures.fetch_add(1, Ordering::Relaxed);
        self.total_send_duration.fetch_add(duration.as_nanos() as u64, Ordering::Relaxed);
    }

    pub fn get_metrics(&self) -> PerformanceMetrics {
        let attempts = self.send_attempts.load(Ordering::Relaxed);
        let successes = self.send_successes.load(Ordering::Relaxed);
        let failures = self.send_failures.load(Ordering::Relaxed);
        let total_duration_ns = self.total_send_duration.load(Ordering::Relaxed);
        let elapsed = self.start_time.elapsed();

        let success_rate = if attempts > 0 {
            successes as f64 / attempts as f64
        } else {
            1.0
        };

        let average_send_latency = if attempts > 0 {
            Duration::from_nanos(total_duration_ns / attempts)
        } else {
            Duration::from_nanos(0)
        };

        let throughput_per_second = if elapsed.as_secs_f64() > 0.0 {
            successes as f64 / elapsed.as_secs_f64()
        } else {
            0.0
        };

        PerformanceMetrics {
            attempts,
            successes,
            failures,
            success_rate,
            average_send_latency,
            throughput_per_second,
            total_elapsed: elapsed,
        }
    }

    pub fn reset(&self) {
        self.send_attempts.store(0, Ordering::Relaxed);
        self.send_successes.store(0, Ordering::Relaxed);
        self.send_failures.store(0, Ordering::Relaxed);
        self.total_send_duration.store(0, Ordering::Relaxed);
    }
}

impl Default for PerformanceTracker {
    fn default() -> Self {
        Self::new()
    }
}

/// Performance metrics for channel operations
#[derive(Debug, Clone)]
pub struct PerformanceMetrics {
    pub attempts: u64,
    pub successes: u64,
    pub failures: u64,
    pub success_rate: f64,
    pub average_send_latency: Duration,
    pub throughput_per_second: f64,
    pub total_elapsed: Duration,
}

/// Result of batch send operation
#[derive(Debug)]
pub struct BatchSendResult {
    pub total_events: usize,
    pub successful: usize,
    pub failed: usize,
    pub duration: Duration,
    pub errors: Vec<CoreError>,
}

impl BatchSendResult {
    pub fn success_rate(&self) -> f64 {
        if self.total_events > 0 {
            self.successful as f64 / self.total_events as f64
        } else {
            1.0
        }
    }

    pub fn throughput_per_second(&self) -> f64 {
        if self.duration.as_secs_f64() > 0.0 {
            self.successful as f64 / self.duration.as_secs_f64()
        } else {
            0.0
        }
    }

    pub fn print_summary(&self) {
        println!("=== Batch Send Results ===");
        println!("Total events: {}", self.total_events);
        println!("Successful: {}", self.successful);
        println!("Failed: {}", self.failed);
        println!("Success rate: {:.2}%", self.success_rate() * 100.0);
        println!("Duration: {:?}", self.duration);
        println!("Throughput: {:.2} events/sec", self.throughput_per_second());
        if !self.errors.is_empty() {
            println!("First error: {}", self.errors[0]);
        }
    }
}

/// Comprehensive health report for channel operations
#[derive(Debug, Clone)]
pub struct ChannelHealthReport {
    pub source_name: String,
    pub sent_count: u64,
    pub error_count: u64,
    pub queue_depth: i64,
    pub average_send_latency: Duration,
    pub success_rate: f64,
    pub throughput_per_second: f64,
    pub last_error: Option<String>,
    pub is_healthy: bool,
}

impl ChannelHealthReport {
    pub fn print_summary(&self) {
        println!("=== Channel Health Report: {} ===", self.source_name);
        println!("Health status: {}", if self.is_healthy { "✓ HEALTHY" } else { "✗ UNHEALTHY" });
        println!("Events sent: {}", self.sent_count);
        println!("Errors: {}", self.error_count);
        println!("Queue depth: {}", self.queue_depth);
        println!("Success rate: {:.2}%", self.success_rate * 100.0);
        println!("Average latency: {:?}", self.average_send_latency);
        println!("Throughput: {:.2} events/sec", self.throughput_per_second);
        if let Some(ref error) = self.last_error {
            println!("Last error: {}", error);
        }
    }

    /// Get health score (0.0 to 1.0, where 1.0 is perfect health)
    pub fn health_score(&self) -> f64 {
        let mut score = 1.0;

        // Penalize low success rate
        score *= self.success_rate;

        // Penalize high latency (exponential penalty after 50ms)
        let latency_ms = self.average_send_latency.as_millis() as f64;
        if latency_ms > 50.0 {
            score *= 0.5_f64.powf((latency_ms - 50.0) / 50.0);
        }

        // Penalize large queue depth (exponential penalty after 100)
        if self.queue_depth > 100 {
            score *= 0.5_f64.powf((self.queue_depth - 100) as f64 / 100.0);
        }

        score.clamp(0.0, 1.0)
    }
}

/// Helper function to create an enhanced event sender
pub fn create_enhanced_event_sender(
    buffer_size: usize,
    source_name: String,
) -> (EnhancedEventSender, mpsc::Receiver<RawEvent>) {
    let (tx, rx) = mpsc::channel(buffer_size);
    (EnhancedEventSender::new(tx, source_name), rx)
}

/// Channel diagnostics utilities
pub struct ChannelDiagnostics;

impl ChannelDiagnostics {
    /// Run comprehensive channel diagnostics
    pub async fn run_diagnostics(
        sender: &EnhancedEventSender,
        test_event: RawEvent,
    ) -> Result<DiagnosticsReport> {
        let start_time = Instant::now();
        
        // Test basic send
        let basic_send_result = sender.send_event(test_event.clone(), "diagnostics_basic").await;
        
        // Test timeout send
        let timeout_result = sender
            .send_event_timeout(test_event.clone(), Duration::from_millis(100), "diagnostics_timeout")
            .await;
        
        // Test batch send
        let batch_events = vec![test_event.clone(), test_event.clone(), test_event];
        let batch_result = sender.send_batch(batch_events, "diagnostics_batch").await;
        
        let total_duration = start_time.elapsed();
        let health_report = sender.health_report();
        
        Ok(DiagnosticsReport {
            basic_send_success: basic_send_result.is_ok(),
            timeout_send_success: timeout_result.is_ok(),
            batch_send_success: batch_result.is_ok(),
            batch_success_rate: batch_result.map(|r| r.success_rate()).unwrap_or(0.0),
            health_report,
            total_duration,
        })
    }
}

/// Diagnostics report
#[derive(Debug)]
pub struct DiagnosticsReport {
    pub basic_send_success: bool,
    pub timeout_send_success: bool,
    pub batch_send_success: bool,
    pub batch_success_rate: f64,
    pub health_report: ChannelHealthReport,
    pub total_duration: Duration,
}

impl DiagnosticsReport {
    pub fn print_summary(&self) {
        println!("=== Channel Diagnostics Report ===");
        println!("Basic send: {}", if self.basic_send_success { "✓ PASS" } else { "✗ FAIL" });
        println!("Timeout send: {}", if self.timeout_send_success { "✓ PASS" } else { "✗ FAIL" });
        println!("Batch send: {}", if self.batch_send_success { "✓ PASS" } else { "✗ FAIL" });
        println!("Batch success rate: {:.2}%", self.batch_success_rate * 100.0);
        println!("Total diagnostics time: {:?}", self.total_duration);
        println!("Overall health score: {:.2}", self.health_report.health_score());
    }

    pub fn is_passing(&self) -> bool {
        self.basic_send_success &&
        self.timeout_send_success &&
        self.batch_send_success &&
        self.batch_success_rate > 0.9 &&
        self.health_report.is_healthy
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RawEventBuilder;
    use serde_json::json;

    #[tokio::test]
    async fn test_enhanced_event_sender() {
        let (enhanced_sender, mut rx) = create_enhanced_event_sender(10, "test_source".to_string());

        let test_event = RawEventBuilder::new("test", "test.event", json!({"data": "test"}))
            .build();

        // Test basic send
        assert!(enhanced_sender.send_event(test_event.clone(), "test_context").await.is_ok());

        // Verify event was received
        let received = rx.recv().await.unwrap();
        assert_eq!(received.source, "test");

        // Check stats
        let stats = enhanced_sender.stats();
        assert_eq!(stats.sent, 1);
        assert_eq!(stats.errors, 0);
    }

    #[tokio::test]
    async fn test_batch_send() {
        let (enhanced_sender, mut rx) = create_enhanced_event_sender(10, "test_source".to_string());

        let test_events = vec![
            RawEventBuilder::new("test", "test.event1", json!({"data": "test1"})).build(),
            RawEventBuilder::new("test", "test.event2", json!({"data": "test2"})).build(),
            RawEventBuilder::new("test", "test.event3", json!({"data": "test3"})).build(),
        ];

        let result = enhanced_sender.send_batch(test_events, "batch_test").await.unwrap();
        
        assert_eq!(result.total_events, 3);
        assert_eq!(result.successful, 3);
        assert_eq!(result.failed, 0);
        assert_eq!(result.success_rate(), 1.0);

        // Verify all events were received
        for i in 1..=3 {
            let received = rx.recv().await.unwrap();
            assert!(received.event_type.contains(&format!("event{}", i)));
        }
    }

    #[tokio::test]
    async fn test_performance_tracking() {
        let (enhanced_sender, _rx) = create_enhanced_event_sender(10, "test_source".to_string());

        let test_event = RawEventBuilder::new("test", "test.event", json!({"data": "test"}))
            .build();

        // Send a few events
        for i in 0..5 {
            let _ = enhanced_sender.send_event(test_event.clone(), &format!("test_{}", i)).await;
        }

        let metrics = enhanced_sender.performance_metrics();
        assert_eq!(metrics.attempts, 5);
        assert!(metrics.average_send_latency > Duration::from_nanos(0));
        assert!(metrics.throughput_per_second >= 0.0);
    }

    #[tokio::test]
    async fn test_health_report() {
        let (enhanced_sender, _rx) = create_enhanced_event_sender(10, "test_source".to_string());

        let test_event = RawEventBuilder::new("test", "test.event", json!({"data": "test"}))
            .build();

        // Send some events
        for _ in 0..3 {
            let _ = enhanced_sender.send_event(test_event.clone(), "health_test").await;
        }

        let health_report = enhanced_sender.health_report();
        assert_eq!(health_report.source_name, "test_source");
        assert!(health_report.is_healthy);
        assert!(health_report.success_rate >= 0.0);
        assert!(health_report.health_score() > 0.0);
    }
}
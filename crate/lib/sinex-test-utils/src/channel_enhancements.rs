//! Enhanced channel operations with performance monitoring and error reporting
//!
//! This module provides production-ready enhancements to the existing channel helpers,
//! including performance measurement, health monitoring, and advanced error reporting.

use crate::channel_helpers::{ChannelMonitor, ChannelStats};
use crate::Result;
use sinex_core::db::models::RawEvent;
use sinex_core::types::error::SinexError;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::{timeout, Instant};

/// Performance tracking for channel operations
#[derive(Debug, Default)]
pub struct PerformanceTracker {
    send_attempts: AtomicU64,
    send_successes: AtomicU64,
    send_failures: AtomicU64,
    total_send_duration_ms: AtomicU64,
}

impl PerformanceTracker {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_send_attempt(&self) {
        self.send_attempts.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_send_success(&self, duration: Duration) {
        self.send_successes.fetch_add(1, Ordering::Relaxed);
        self.total_send_duration_ms
            .fetch_add(duration.as_millis() as u64, Ordering::Relaxed);
    }

    pub fn record_send_failure(&self, duration: Duration) {
        self.send_failures.fetch_add(1, Ordering::Relaxed);
        self.total_send_duration_ms
            .fetch_add(duration.as_millis() as u64, Ordering::Relaxed);
    }

    pub fn get_metrics(&self) -> PerformanceMetrics {
        let attempts = self.send_attempts.load(Ordering::Relaxed);
        let successes = self.send_successes.load(Ordering::Relaxed);
        let failures = self.send_failures.load(Ordering::Relaxed);
        let total_duration = self.total_send_duration_ms.load(Ordering::Relaxed);

        PerformanceMetrics {
            send_attempts: attempts,
            send_successes: successes,
            send_failures: failures,
            success_rate: if attempts > 0 {
                (successes as f64) / (attempts as f64)
            } else {
                0.0
            },
            avg_send_duration_ms: if attempts > 0 {
                total_duration as f64 / attempts as f64
            } else {
                0.0
            },
        }
    }
}

/// Performance metrics snapshot
#[derive(Debug, Clone)]
pub struct PerformanceMetrics {
    pub send_attempts: u64,
    pub send_successes: u64,
    pub send_failures: u64,
    pub success_rate: f64,
    pub avg_send_duration_ms: f64,
}

/// Enhanced event sender with comprehensive monitoring and error reporting
pub struct EnhancedEventSender {
    inner: mpsc::Sender<RawEvent>,
    monitor: Arc<ChannelMonitor>,
    source_name: String,
    performance_tracker: Arc<PerformanceTracker>,
}

impl EnhancedEventSender {
    /// Create a new enhanced event sender
    pub fn new(sender: mpsc::Sender<RawEvent>, source_name: String) -> Self {
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
                self.performance_tracker
                    .record_send_success(start_time.elapsed());
                tracing::trace!(
                    "[{}] Sent {} event: {}",
                    self.source_name,
                    event_type,
                    context
                );
                Ok(())
            }
            Err(e) => {
                let error_msg = format!("Failed to send {} event: {}", event_type, e);
                self.monitor.record_error(error_msg.clone());
                self.performance_tracker
                    .record_send_failure(start_time.elapsed());

                tracing::error!("[{}] {}: {}", self.source_name, error_msg, context);

                Err(SinexError::unknown(format!(
                    "{} (source: {}, event_type: {}, context: {}, duration_ms: {})",
                    error_msg,
                    self.source_name,
                    event_type,
                    context,
                    start_time.elapsed().as_millis()
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
                let error_msg = format!(
                    "Send timeout for {} event after {:?}",
                    event_type, timeout_duration
                );
                self.monitor.record_error(error_msg.clone());
                self.performance_tracker
                    .record_send_failure(start_time.elapsed());

                Err(SinexError::unknown(format!(
                    "{} (source: {}, event_type: {}, timeout: {:?}, context: {})",
                    error_msg, self.source_name, event_type, timeout_duration, context
                )))
            }
        }
    }

    /// Get channel statistics
    pub fn get_stats(&self) -> ChannelStats {
        self.monitor.stats()
    }

    /// Get performance metrics
    pub fn get_performance_metrics(&self) -> PerformanceMetrics {
        self.performance_tracker.get_metrics()
    }

    /// Get source name
    pub fn source_name(&self) -> &str {
        &self.source_name
    }
}

/// Create an enhanced event sender
pub fn create_enhanced_event_sender(
    sender: mpsc::Sender<RawEvent>,
    source_name: String,
) -> EnhancedEventSender {
    EnhancedEventSender::new(sender, source_name)
}

/// Results from batch send operations
#[derive(Debug)]
pub struct BatchSendResult {
    pub successful: usize,
    pub failed: usize,
    pub errors: Vec<String>,
}

/// Channel diagnostics information
#[derive(Debug, Clone)]
pub struct ChannelDiagnostics {
    pub source_name: String,
    pub channel_stats: ChannelStats,
    pub performance_metrics: PerformanceMetrics,
}

/// Health report for channel operations
#[derive(Debug, Clone)]
pub struct ChannelHealthReport {
    pub is_healthy: bool,
    pub issues: Vec<String>,
    pub recommendations: Vec<String>,
}

/// Diagnostics report combining multiple sources
#[derive(Debug)]
pub struct DiagnosticsReport {
    pub channels: Vec<ChannelDiagnostics>,
    pub overall_health: ChannelHealthReport,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sinex_test;
    use sinex_core::db::models::RawEvent;

    #[sinex_test]
    async fn test_enhanced_event_sender() -> color_eyre::eyre::Result<()> {
        let (tx, mut rx) = mpsc::channel::<RawEvent>(10);
        let sender = create_enhanced_event_sender(tx, "test_source".to_string());

        let event = RawEvent::schemaless(
            sinex_core::types::domain::EventSource::new("test_source"),
            sinex_core::types::domain::EventType::new("test_event"),
            serde_json::json!({}),
        );

        // Test successful send
        assert!(sender.send_event(event, "test context").await.is_ok());

        // Verify event was received
        let received = rx.recv().await.unwrap();
        assert_eq!(received.event_type.as_str(), "test_event");

        // Check stats
        let stats = sender.get_stats();
        assert_eq!(stats.sent, 1);
        assert_eq!(stats.errors, 0);

        // Check performance metrics
        let metrics = sender.get_performance_metrics();
        assert_eq!(metrics.send_attempts, 1);
        assert_eq!(metrics.send_successes, 1);
        assert_eq!(metrics.send_failures, 0);
        assert_eq!(metrics.success_rate, 1.0);
        Ok(())
    }

    #[sinex_test]
    async fn test_enhanced_sender_timeout() -> color_eyre::eyre::Result<()> {
        let (tx, _rx) = mpsc::channel::<RawEvent>(1);
        let sender = create_enhanced_event_sender(tx, "test_source".to_string());

        // Fill the channel
        let event1 = RawEvent::schemaless(
            sinex_core::types::domain::EventSource::new("test_source"),
            sinex_core::types::domain::EventType::new("test_event"),
            serde_json::json!({}),
        );
        let _ = sender.send_event(event1, "fill channel").await;

        // This should timeout
        let event2 = RawEvent::schemaless(
            sinex_core::types::domain::EventSource::new("test_source"),
            sinex_core::types::domain::EventType::new("test_event"),
            serde_json::json!({}),
        );

        let result = sender
            .send_event_timeout(event2, Duration::from_millis(10), "timeout test")
            .await;

        assert!(result.is_err());

        // Check that failure was recorded
        let metrics = sender.get_performance_metrics();
        assert!(metrics.send_failures > 0);
        Ok(())
    }
}

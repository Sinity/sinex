//! Channel operation helpers for consistent patterns across the codebase
//!
//! This module provides extension traits and utilities for working with channels,
//! particularly focused on event streaming operations. It includes:
//!
//! - Extension traits for senders and receivers with common patterns
//! - Backpressure handling and monitoring
//! - Error handling with context
//! - Batch operations for efficiency

use crate::{CoreError, Result};
use async_trait::async_trait;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::{sleep, timeout};

/// Extension trait for channel senders with common patterns
#[async_trait]
pub trait ChannelSenderExt<T> {
    /// Send a value with automatic error logging and context
    async fn send_or_log(&self, value: T, context: &str) -> Result<()>;

    /// Send with a timeout, returning error if channel is full for too long
    async fn send_timeout(&self, value: T, timeout_duration: Duration) -> Result<()>;

    /// Try to send immediately, queueing if channel is full (up to max_queue items)
    async fn try_send_or_queue(&self, value: T, queue: &mut Vec<T>, max_queue: usize) -> Result<()>
    where
        T: Clone;
}

/// Extension trait for channel receivers with batch and timeout operations
#[async_trait]
pub trait ChannelReceiverExt<T> {
    /// Receive with a timeout, returning None if no items arrive in time
    async fn recv_timeout(&mut self, timeout_duration: Duration) -> Result<Option<T>>;

    /// Receive up to max_items within the timeout window
    async fn recv_batch(&mut self, max_items: usize, timeout_duration: Duration) -> Vec<T>;

    /// Drain all currently available items without blocking
    async fn drain_all(&mut self) -> Vec<T>;
}

/// Channel health monitoring
#[derive(Debug, Default)]
pub struct ChannelMonitor {
    /// Total items sent through the channel
    pub sent: AtomicU64,
    /// Total items received from the channel
    pub received: AtomicU64,
    /// Total send errors encountered
    pub errors: AtomicU64,
    /// Last error message (if any)
    pub last_error: RwLock<Option<String>>,
}

impl ChannelMonitor {
    /// Create a new channel monitor
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a successful send
    pub fn record_send(&self) {
        self.sent.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a successful receive
    pub fn record_receive(&self) {
        self.received.fetch_add(1, Ordering::Relaxed);
    }

    /// Record an error with context
    pub fn record_error(&self, error: String) {
        self.errors.fetch_add(1, Ordering::Relaxed);
        if let Ok(mut last_error) = self.last_error.write() {
            *last_error = Some(error);
        }
    }

    /// Get current queue depth estimate (sent - received)
    pub fn queue_depth(&self) -> i64 {
        let sent = self.sent.load(Ordering::Relaxed) as i64;
        let received = self.received.load(Ordering::Relaxed) as i64;
        sent - received
    }

    /// Get current statistics
    pub fn stats(&self) -> ChannelStats {
        ChannelStats {
            sent: self.sent.load(Ordering::Relaxed),
            received: self.received.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
            queue_depth: self.queue_depth(),
            last_error: self.last_error.read().ok().and_then(|e| e.clone()),
        }
    }
}

/// Channel statistics snapshot
#[derive(Debug, Clone)]
pub struct ChannelStats {
    pub sent: u64,
    pub received: u64,
    pub errors: u64,
    pub queue_depth: i64,
    pub last_error: Option<String>,
}

// Implementation for generic mpsc::Sender
#[async_trait]
impl<T: Send> ChannelSenderExt<T> for mpsc::Sender<T> {
    async fn send_or_log(&self, value: T, context: &str) -> Result<()> {
        match self.send(value).await {
            Ok(()) => Ok(()),
            Err(e) => {
                tracing::error!("Failed to send on channel ({}): {}", context, e);
                Err(CoreError::Other(format!(
                    "Channel send failed ({}): {}",
                    context, e
                )))
            }
        }
    }

    async fn send_timeout(&self, value: T, timeout_duration: Duration) -> Result<()> {
        match timeout(timeout_duration, self.send(value)).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => Err(CoreError::Other(format!("Channel send failed: {}", e))),
            Err(_) => Err(CoreError::Other("Channel send timed out".to_string())),
        }
    }

    async fn try_send_or_queue(&self, value: T, queue: &mut Vec<T>, max_queue: usize) -> Result<()>
    where
        T: Clone,
    {
        // First try to drain the queue
        while !queue.is_empty() {
            match self.try_send(queue[0].clone()) {
                Ok(()) => {
                    queue.remove(0);
                }
                Err(mpsc::error::TrySendError::Full(_)) => {
                    break; // Channel still full, keep items in queue
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    return Err(CoreError::Other("Channel closed".to_string()));
                }
            }
        }

        // Now try to send the new value
        match self.try_send(value) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(value)) => {
                if queue.len() < max_queue {
                    queue.push(value);
                    Ok(())
                } else {
                    Err(CoreError::Other(format!(
                        "Channel full and queue at limit ({})",
                        max_queue
                    )))
                }
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                Err(CoreError::Other("Channel closed".to_string()))
            }
        }
    }
}

// Implementation for generic mpsc::Receiver
#[async_trait]
impl<T: Send> ChannelReceiverExt<T> for mpsc::Receiver<T> {
    async fn recv_timeout(&mut self, timeout_duration: Duration) -> Result<Option<T>> {
        match timeout(timeout_duration, self.recv()).await {
            Ok(Some(value)) => Ok(Some(value)),
            Ok(None) => Ok(None), // Channel closed
            Err(_) => Ok(None),   // Timeout
        }
    }

    async fn recv_batch(&mut self, max_items: usize, timeout_duration: Duration) -> Vec<T> {
        let mut items = Vec::with_capacity(max_items.min(100)); // Cap pre-allocation

        // First item waits for timeout
        match self.recv_timeout(timeout_duration).await {
            Ok(Some(item)) => items.push(item),
            _ => return items,
        }

        // Subsequent items are collected without waiting
        while items.len() < max_items {
            match self.try_recv() {
                Ok(item) => items.push(item),
                _ => break,
            }
        }

        items
    }

    async fn drain_all(&mut self) -> Vec<T> {
        let mut items = Vec::new();

        while let Ok(item) = self.try_recv() {
            items.push(item);
            // Prevent unbounded growth
            if items.len() >= 10000 {
                tracing::warn!("Channel drain limited to 10000 items");
                break;
            }
        }

        items
    }
}

/*
/// Specialized implementation for RawEvent channels with metrics
/// Note: Temporarily disabled due to RawEvent being moved to sinex-db
pub struct MonitoredEventSender {
    inner: mpsc::Sender<RawEvent>,
    monitor: ChannelMonitor,
    source_name: String,
}*/

/*
/// MonitoredEventSender implementation temporarily commented out due to RawEvent move
impl MonitoredEventSender {
    /// Create a new monitored event sender
    pub fn new(sender: mpsc::Sender<RawEvent>, source_name: String) -> Self {
        Self {
            inner: sender,
            monitor: ChannelMonitor::new(),
            source_name,
        }
    }

    /// Send an event with monitoring
    pub async fn send(&self, event: RawEvent) -> Result<()> {
        let event_type = event.event_type.clone();

        match self.inner.send(event).await {
            Ok(()) => {
                self.monitor.record_send();
                Ok(())
            }
            Err(e) => {
                let error_msg = format!("Failed to send {} event: {}", event_type, e);
                self.monitor.record_error(error_msg.clone());
                tracing::error!("[{}] {}", self.source_name, error_msg);
                Err(CoreError::Other(error_msg))
            }
        }
    }

    /// Get current channel statistics
    pub fn stats(&self) -> ChannelStats {
        self.monitor.stats()
    }

    /// Get reference to the inner sender
    pub fn inner(&self) -> &mpsc::Sender<RawEvent> {
        &self.inner
    }
}
*/

/// Backpressure handling utilities
pub struct BackpressureManager {
    high_watermark: usize,
    low_watermark: usize,
    current_delay: Duration,
    max_delay: Duration,
}

impl BackpressureManager {
    /// Create a new backpressure manager
    pub fn new(high_watermark: usize, low_watermark: usize) -> Self {
        Self {
            high_watermark,
            low_watermark,
            current_delay: Duration::from_millis(0),
            max_delay: Duration::from_secs(1),
        }
    }

    /// Check queue depth and apply backpressure if needed
    pub async fn check_and_wait(&mut self, queue_depth: usize) {
        if queue_depth > self.high_watermark {
            // Increase delay exponentially up to max, starting with minimum delay
            if self.current_delay == Duration::from_millis(0) {
                self.current_delay = Duration::from_millis(10); // Start with 10ms
            } else {
                self.current_delay = (self.current_delay * 2).min(self.max_delay);
            }
            tracing::debug!(
                "Applying backpressure: {:?} delay for queue depth {}",
                self.current_delay,
                queue_depth
            );
            sleep(self.current_delay).await;
        } else if queue_depth < self.low_watermark {
            // Decrease delay when queue is draining
            self.current_delay /= 2;
        }
    }

    /// Reset backpressure state
    pub fn reset(&mut self) {
        self.current_delay = Duration::from_millis(0);
    }
}

/* Helper function temporarily commented out due to RawEvent move
/// Helper function to create a monitored channel pair
pub fn monitored_channel(
    buffer: usize,
    source_name: String,
) -> (MonitoredEventSender, mpsc::Receiver<RawEvent>) {
    let (tx, rx) = mpsc::channel(buffer);
    (MonitoredEventSender::new(tx, source_name), rx)
}
*/

#[cfg(test)]
mod tests {
    use super::*;
    // use crate::RawEventBuilder; // Commented out due to RawEvent move
    use serde_json::json;

    #[tokio::test]
    async fn test_channel_sender_ext() {
        let (tx, mut rx) = mpsc::channel::<String>(2);

        // Test send_or_log
        assert!(tx
            .send_or_log("test1".to_string(), "test context")
            .await
            .is_ok());
        assert_eq!(rx.recv().await, Some("test1".to_string()));

        // Test send_timeout
        assert!(tx
            .send_timeout("test2".to_string(), Duration::from_secs(1))
            .await
            .is_ok());
        assert_eq!(rx.recv().await, Some("test2".to_string()));
    }

    #[tokio::test]
    async fn test_channel_receiver_ext() {
        let (tx, mut rx) = mpsc::channel::<i32>(10);

        // Send some test data
        for i in 0..5 {
            tx.send(i).await.unwrap();
        }
        drop(tx); // Close sender

        // Test recv_batch
        let batch = rx.recv_batch(3, Duration::from_millis(100)).await;
        assert_eq!(batch, vec![0, 1, 2]);

        // Test drain_all
        let remaining = rx.drain_all().await;
        assert_eq!(remaining, vec![3, 4]);
    }

    /* Test temporarily commented out due to RawEvent move
    #[tokio::test]
    async fn test_monitored_event_sender() {
        let (monitored_tx, mut rx) = monitored_channel(10, "test_source".to_string());

        let event = RawEventBuilder::new("test", "test.event", json!({"data": "test"}))
            .build();

        // Send event
        assert!(monitored_tx.send(event.clone()).await.is_ok());

        // Check stats
        let stats = monitored_tx.stats();
        assert_eq!(stats.sent, 1);
        assert_eq!(stats.errors, 0);

        // Receive event
        let received = rx.recv().await.unwrap();
        assert_eq!(received.source, "test");
    }
    */

    #[tokio::test]
    async fn test_backpressure_manager() {
        let mut manager = BackpressureManager::new(100, 50);

        // No delay when under low watermark
        let start = tokio::time::Instant::now();
        manager.check_and_wait(25).await;
        assert!(start.elapsed() < Duration::from_millis(10));

        // Delay increases when over high watermark
        manager.check_and_wait(150).await;
        assert!(manager.current_delay > Duration::from_millis(0));
    }
}

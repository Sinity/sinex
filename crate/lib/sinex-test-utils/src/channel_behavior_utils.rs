// Channel testing utilities using ChannelSenderExt and ChannelReceiverExt abstractions
//
// This module provides testing utilities for async channel patterns using the new
// channel extension traits, backpressure management, and monitoring capabilities.

use crate::channel_helpers::{
    BackpressureManager, ChannelMonitor, ChannelReceiverExt, ChannelSenderExt,
};
use crate::prelude::*;
use crate::Result;
use std::fmt::Debug;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

/// Test channel setup with monitoring capabilities
pub struct TestChannelSetup<T> {
    pub sender: mpsc::Sender<T>,
    pub receiver: mpsc::Receiver<T>,
    pub monitor: Arc<ChannelMonitor>,
}

impl<T> TestChannelSetup<T> {
    /// Create a new test channel with specified buffer size
    pub fn new(buffer_size: usize) -> Self {
        let (sender, receiver) = mpsc::channel(buffer_size);
        let monitor = Arc::new(ChannelMonitor::new());

        Self {
            sender,
            receiver,
            monitor,
        }
    }

    /// Create a minimal capacity channel for immediate backpressure testing
    /// Note: Tokio mpsc channels require buffer > 0, so we use 1
    pub fn zero_capacity() -> Self {
        Self::new(1)
    }

    /// Create a small capacity channel for backpressure testing
    pub fn small_capacity() -> Self {
        Self::new(1)
    }

    /// Create a large capacity channel for throughput testing
    pub fn large_capacity() -> Self {
        Self::new(1000)
    }
}

/// Channel behavior testing utilities
pub mod behavior {
    use super::*;

    /// Test basic send/receive functionality with error context
    pub async fn test_basic_send_receive<T>(
        sender: &impl ChannelSenderExt<T>,
        receiver: &mut impl ChannelReceiverExt<T>,
        test_value: T,
        test_name: &str,
    ) -> crate::Result<()>
    where
        T: Send + PartialEq + Debug + Clone,
    {
        let expected_value = test_value.clone();

        // Send the value with context using the trait method
        sender.send_or_log(test_value, test_name).await?;

        // Receive with timeout
        let received = receiver
            .recv_timeout(Duration::from_secs(1))
            .await
            .map_err(|e| {
                SinexError::unknown(format!(
                    "Failed to receive from channel in test {}: {}",
                    test_name, e
                ))
            })?
            .ok_or_else(|| {
                SinexError::unknown(format!("Channel closed unexpectedly in test {}", test_name))
            })?;

        // Verify the received value matches what was sent
        if received != expected_value {
            return Err(SinexError::validation(format!(
                "Channel test '{}' failed: received {:?}, expected {:?}",
                test_name, received, expected_value
            ))
            .into());
        }

        Ok(())
    }

    /// Test channel timeout behavior
    pub async fn test_channel_timeout<T>(
        receiver: &mut impl ChannelReceiverExt<T>,
        timeout: Duration,
        should_timeout: bool,
    ) -> crate::Result<()>
    where
        T: Send,
    {
        let result = receiver.recv_timeout(timeout).await;

        match (
            result.is_err() || result.as_ref().unwrap().is_none(),
            should_timeout,
        ) {
            (true, true) => Ok(()),   // Expected timeout
            (false, false) => Ok(()), // Expected receive
            (false, true) => Err(
                SinexError::validation("Expected timeout but received value")
                    .wrap_err_with("timeout_duration", format!("{:?}", timeout))
                    .into(),
            ),
            (true, false) => Err(
                SinexError::validation("Expected receive but got timeout/close")
                    .wrap_err_with("timeout_duration", format!("{:?}", timeout))
                    .into(),
            ),
        }
    }

    /// Test batch receive functionality
    pub async fn test_batch_receive<T>(
        sender: &impl ChannelSenderExt<T>,
        receiver: &mut impl ChannelReceiverExt<T>,
        items: Vec<T>,
        max_batch_size: usize,
        batch_timeout: Duration,
    ) -> crate::Result<()>
    where
        T: Send + Clone + Debug,
    {
        // Send all items quickly
        for item in &items {
            sender.send_or_log(item.clone(), "batch_test").await?;
        }

        // Receive in batches
        let mut total_received = 0;
        let mut batch_count = 0;

        while total_received < items.len() {
            let batch = receiver.recv_batch(max_batch_size, batch_timeout).await;

            if batch.is_empty() {
                return Err(SinexError::validation(
                    "Received empty batch before all items collected",
                )
                .wrap_err_with("total_received", total_received)
                .wrap_err_with("expected_total", items.len()));
            }

            total_received += batch.len();
            batch_count += 1;

            // Prevent infinite loops
            if batch_count > 100 {
                return Err(
                    SinexError::validation("Too many batches - possible infinite loop")
                        .wrap_err_with("batch_count", batch_count),
                );
            }
        }

        if total_received != items.len() {
            return Err(SinexError::unknown(format!(
                "Batch receive count mismatch: {} != {}",
                total_received,
                items.len()
            )));
        }
        Ok(())
    }

    /// Test channel drain functionality
    pub async fn test_channel_drain<T>(
        sender: &impl ChannelSenderExt<T>,
        receiver: &mut impl ChannelReceiverExt<T>,
        items: Vec<T>,
    ) -> crate::Result<()>
    where
        T: Send + Clone + Debug,
    {
        // Send all items
        for item in &items {
            sender.send_or_log(item.clone(), "drain_test").await?;
        }

        // Give a moment for items to be queued
        tokio::task::yield_now().await;

        // Drain all items
        let drained = receiver.drain_all().await;

        if drained.len() != items.len() {
            return Err(SinexError::unknown(format!(
                "Drain count mismatch: {} != {}",
                drained.len(),
                items.len()
            )));
        }
        Ok(())
    }
}

/// Backpressure testing utilities
pub mod backpressure {
    use super::*;

    /// Test backpressure behavior with full channel
    pub async fn test_backpressure_management<T>(
        sender: &impl ChannelSenderExt<T>,
        test_items: Vec<T>,
        expected_timeout: Duration,
    ) -> crate::Result<()>
    where
        T: Send + Clone,
    {
        let mut backpressure_manager = BackpressureManager::new(10, 5);
        let mut _successful_sends = 0;
        let mut timeouts = 0;

        for item in test_items {
            match sender.send_timeout(item, expected_timeout).await {
                Ok(()) => {
                    _successful_sends += 1;
                    backpressure_manager.check_and_wait(0).await; // Low queue depth
                }
                Err(_) => {
                    timeouts += 1;
                    backpressure_manager.check_and_wait(20).await; // High queue depth
                }
            }
        }

        // Validate that we got some timeouts (indicating backpressure)
        if timeouts == 0 {
            return Err(SinexError::unknown(
                "Expected some timeouts due to backpressure".to_string(),
            ));
        }

        Ok(())
    }

    /// Test queue management with try_send_or_queue
    pub async fn test_queue_management<T>(
        sender: &impl ChannelSenderExt<T>,
        items: Vec<T>,
        max_queue_size: usize,
    ) -> crate::Result<()>
    where
        T: Send + Clone,
    {
        let mut queue = Vec::new();
        let mut _successful_immediate = 0;
        let mut _queued = 0;
        let mut _rejected = 0;

        for item in items {
            match sender
                .try_send_or_queue(item, &mut queue, max_queue_size)
                .await
            {
                Ok(()) => {
                    if queue.is_empty() {
                        _successful_immediate += 1;
                    } else {
                        _queued += 1;
                    }
                }
                Err(_) => {
                    _rejected += 1;
                }
            }
        }

        // Validate queue behavior
        if queue.len() > max_queue_size {
            return Err(SinexError::unknown(format!(
                "Queue size {} exceeds maximum {}",
                queue.len(),
                max_queue_size
            )));
        }

        Ok(())
    }

    /// Test adaptive backpressure with varying load
    pub async fn test_adaptive_backpressure() -> crate::Result<()> {
        let mut manager = BackpressureManager::new(50, 20);
        let start_time = tokio::time::Instant::now();

        // Simulate varying queue depths
        let queue_depths = vec![10, 30, 60, 80, 40, 15, 5];

        for depth in queue_depths {
            manager.check_and_wait(depth).await;
        }

        let total_time = start_time.elapsed();

        // Should have some delay due to high queue depths
        if total_time <= Duration::from_millis(10) {
            return Err(SinexError::unknown(format!(
                "Adaptive backpressure should introduce some delay, but total time was {:?}",
                total_time
            ))
            .into());
        }

        Ok(())
    }
}

/// Performance testing utilities for channels
pub mod performance {
    use super::*;

    /// Measure channel throughput
    pub async fn measure_channel_throughput<T>(
        sender: impl ChannelSenderExt<T> + Clone + Send + 'static,
        mut receiver: impl ChannelReceiverExt<T> + Send + 'static,
        item_count: usize,
        test_item: T,
    ) -> std::result::Result<ChannelPerformanceReport, SinexError>
    where
        T: Send + Clone + 'static,
    {
        let start_time = tokio::time::Instant::now();
        let sent_counter = Arc::new(AtomicU64::new(0));
        let received_counter = Arc::new(AtomicU64::new(0));

        // Spawn sender task
        let sender_counter = sent_counter.clone();
        let sender_item = test_item.clone();
        let sender_task = tokio::spawn(async move {
            for i in 0..item_count {
                match sender
                    .send_or_log(sender_item.clone(), "throughput_test")
                    .await
                {
                    Ok(()) => {
                        sender_counter.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(_) => break,
                }

                // Occasional yield to prevent starvation
                if i % 100 == 0 {
                    tokio::task::yield_now().await;
                }
            }
        });

        // Spawn receiver task
        let receiver_counter = received_counter.clone();
        let receiver_task = tokio::spawn(async move {
            let mut received = 0;
            while received < item_count {
                match receiver.recv_timeout(Duration::from_secs(5)).await {
                    Ok(Some(_)) => {
                        received += 1;
                        receiver_counter.fetch_add(1, Ordering::Relaxed);
                    }
                    Ok(None) => break, // Channel closed
                    Err(_) => break,   // Timeout
                }
            }
        });

        // Wait for completion
        let _ = tokio::try_join!(sender_task, receiver_task)
            .map_err(|e| SinexError::service(format!("Task join failed: {}", e)))?;

        let total_time = start_time.elapsed();
        let sent = sent_counter.load(Ordering::Relaxed);
        let received = received_counter.load(Ordering::Relaxed);

        Ok(ChannelPerformanceReport {
            total_time,
            items_sent: sent,
            items_received: received,
            send_rate: sent as f64 / total_time.as_secs_f64(),
            receive_rate: received as f64 / total_time.as_secs_f64(),
        })
    }

    /// Test concurrent channel access
    pub async fn test_concurrent_channel_access<T>(
        sender: impl ChannelSenderExt<T> + Clone + Send + 'static,
        concurrent_senders: usize,
        items_per_sender: usize,
        test_item: T,
    ) -> crate::Result<()>
    where
        T: Send + Clone + 'static,
    {
        let total_expected = concurrent_senders * items_per_sender;
        let success_counter = Arc::new(AtomicU64::new(0));
        let error_counter = Arc::new(AtomicU64::new(0));

        // Spawn concurrent sender tasks
        let mut handles = Vec::new();

        for sender_id in 0..concurrent_senders {
            let sender_clone = sender.clone();
            let item_clone = test_item.clone();
            let success_counter_clone = success_counter.clone();
            let error_counter_clone = error_counter.clone();

            let handle = tokio::spawn(async move {
                for i in 0..items_per_sender {
                    let context = format!("sender_{}_item_{}", sender_id, i);
                    match sender_clone.send_or_log(item_clone.clone(), &context).await {
                        Ok(()) => {
                            success_counter_clone.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(_) => {
                            error_counter_clone.fetch_add(1, Ordering::Relaxed);
                        }
                    }

                    // Small random delay to increase contention
                    if i % 10 == 0 {
                        tokio::task::yield_now().await;
                    }
                }
            });

            handles.push(handle);
        }

        // Wait for all senders to complete
        for handle in handles {
            handle.await.map_err(|e| {
                SinexError::unknown(format!("Concurrent sender task failed: {}", e))
            })?;
        }

        let successes = success_counter.load(Ordering::Relaxed);
        let errors = error_counter.load(Ordering::Relaxed);

        // Validate results
        if successes + errors != total_expected as u64 {
            return Err(SinexError::unknown(format!(
                "Operation count mismatch: successes: {}, errors: {}, expected: {}",
                successes, errors, total_expected
            ))
            .into());
        }

        Ok(())
    }
}

/// Channel monitoring and metrics utilities
pub mod monitoring {
    use super::*;

    /// Test channel monitoring functionality
    pub async fn test_channel_monitoring<T>(
        sender: &impl ChannelSenderExt<T>,
        monitor: &ChannelMonitor,
        test_items: Vec<T>,
    ) -> crate::Result<()>
    where
        T: Send + Clone,
    {
        let initial_stats = monitor.stats();

        // Send items and manually record metrics
        for (i, item) in test_items.iter().enumerate() {
            match sender.send_or_log(item.clone(), "monitoring_test").await {
                Ok(()) => {
                    monitor.record_send();
                }
                Err(e) => {
                    monitor.record_error(format!("Send failed at item {}: {}", i, e));
                }
            }
        }

        let final_stats = monitor.stats();

        // Validate monitoring data
        if final_stats.sent <= initial_stats.sent {
            return Err(SinexError::unknown(format!(
                "Send count should have increased: initial: {}, final: {}",
                initial_stats.sent, final_stats.sent
            ))
            .into());
        }

        if final_stats.sent - initial_stats.sent != test_items.len() as u64 {
            return Err(SinexError::unknown(format!(
                "Send count mismatch: delta: {}, items: {}",
                final_stats.sent - initial_stats.sent,
                test_items.len()
            ))
            .into());
        }

        Ok(())
    }

    /// Test channel health metrics collection
    pub async fn collect_channel_health_metrics(
        monitor: &ChannelMonitor,
        duration: Duration,
    ) -> ChannelHealthReport {
        let start_stats = monitor.stats();
        let start_time = tokio::time::Instant::now();

        // Wait for the specified duration
        tokio::time::sleep(duration).await;

        let end_stats = monitor.stats();
        let actual_duration = start_time.elapsed();

        ChannelHealthReport {
            duration: actual_duration,
            send_rate: (end_stats.sent - start_stats.sent) as f64 / actual_duration.as_secs_f64(),
            receive_rate: (end_stats.received - start_stats.received) as f64
                / actual_duration.as_secs_f64(),
            error_rate: (end_stats.errors - start_stats.errors) as f64
                / actual_duration.as_secs_f64(),
            average_queue_depth: end_stats.queue_depth as f64,
            last_error: end_stats.last_error,
        }
    }
}

/// Test data structures for channel performance and health
#[derive(Debug, Clone)]
pub struct ChannelPerformanceReport {
    pub total_time: Duration,
    pub items_sent: u64,
    pub items_received: u64,
    pub send_rate: f64,
    pub receive_rate: f64,
}

impl ChannelPerformanceReport {
    pub fn print_summary(&self) {
        println!("=== Channel Performance Report ===");
        println!("Total time: {:?}", self.total_time);
        println!("Items sent: {}", self.items_sent);
        println!("Items received: {}", self.items_received);
        println!("Send rate: {:.2} items/sec", self.send_rate);
        println!("Receive rate: {:.2} items/sec", self.receive_rate);
        println!(
            "Success rate: {:.2}%",
            (self.items_received as f64 / self.items_sent as f64) * 100.0
        );
    }
}

#[derive(Debug, Clone)]
pub struct ChannelHealthReport {
    pub duration: Duration,
    pub send_rate: f64,
    pub receive_rate: f64,
    pub error_rate: f64,
    pub average_queue_depth: f64,
    pub last_error: Option<String>,
}

impl ChannelHealthReport {
    pub fn print_summary(&self) {
        println!("=== Channel Health Report ===");
        println!("Monitoring duration: {:?}", self.duration);
        println!("Send rate: {:.2} ops/sec", self.send_rate);
        println!("Receive rate: {:.2} ops/sec", self.receive_rate);
        println!("Error rate: {:.2} errors/sec", self.error_rate);
        println!("Average queue depth: {:.2}", self.average_queue_depth);
        if let Some(ref error) = self.last_error {
            println!("Last error: {}", error);
        }
    }

    pub fn is_healthy(&self) -> bool {
        self.error_rate < 0.1 && // Less than 0.1 errors per second
        self.average_queue_depth < 100.0 && // Queue not too deep
        self.send_rate > 0.0 // Some activity
    }
}

/// Channel test scenario builders
pub mod scenarios {
    use super::*;

    /// Build a comprehensive channel test scenario
    pub async fn run_comprehensive_channel_test<T>(
        test_name: &str,
        test_items: Vec<T>,
        buffer_size: usize,
    ) -> crate::Result<()>
    where
        T: Send + Clone + Debug + PartialEq + 'static,
    {
        let mut test_setup = TestChannelSetup::new(buffer_size);

        println!("Running comprehensive channel test: {}", test_name);

        // Test basic functionality
        if !test_items.is_empty() {
            behavior::test_basic_send_receive(
                &test_setup.sender,
                &mut test_setup.receiver,
                test_items[0].clone(),
                &format!("{}_basic", test_name),
            )
            .await?;
        }

        // Test batch operations
        if test_items.len() > 1 {
            behavior::test_batch_receive(
                &test_setup.sender,
                &mut test_setup.receiver,
                test_items.clone(),
                10,
                Duration::from_millis(100),
            )
            .await?;
        }

        // Test monitoring
        monitoring::test_channel_monitoring(&test_setup.sender, &test_setup.monitor, test_items)
            .await?;

        println!("✓ Comprehensive channel test '{}' passed", test_name);
        Ok(())
    }

    /// Run backpressure-focused test scenario
    pub async fn run_backpressure_test_scenario<T>(
        test_name: &str,
        test_items: Vec<T>,
    ) -> crate::Result<()>
    where
        T: Send + Clone + 'static,
    {
        println!("Running backpressure test: {}", test_name);

        // Use zero-capacity channel for immediate backpressure
        let test_setup = TestChannelSetup::zero_capacity();

        // Test backpressure management
        backpressure::test_backpressure_management(
            &test_setup.sender,
            test_items.clone(),
            Duration::from_millis(10),
        )
        .await?;

        // Test queue management
        backpressure::test_queue_management(&test_setup.sender, test_items, 5).await?;

        // Test adaptive backpressure
        backpressure::test_adaptive_backpressure().await?;

        println!("✓ Backpressure test '{}' passed", test_name);
        Ok(())
    }
}

// All assertions should be done through TestContext.assert() API
// No custom helper functions needed here

// concurrent_test macro is defined in test_macros.rs

// Helper functions should use TestContext mechanisms

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_channel_test_utilities() {
        let test_items = vec!["item1", "item2", "item3"];

        // Test comprehensive scenario
        scenarios::run_comprehensive_channel_test("string_channel_test", test_items.clone(), 10)
            .await
            .unwrap();

        // Test backpressure scenario
        scenarios::run_backpressure_test_scenario("string_backpressure_test", test_items)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_channel_performance_measurement() {
        let (tx, rx) = tokio::sync::mpsc::channel(100);

        let report = performance::measure_channel_throughput(tx, rx, 1000, "test_item")
            .await
            .unwrap();

        assert!(report.items_sent > 0);
        assert!(report.send_rate > 0.0);
        assert_eq!(report.items_sent, report.items_received);
    }

    #[tokio::test]
    async fn test_channel_monitoring() {
        let (tx, _rx) = tokio::sync::mpsc::channel(10);
        let monitor = ChannelMonitor::new();

        let test_items = vec![1, 2, 3, 4, 5];

        monitoring::test_channel_monitoring(&tx, &monitor, test_items)
            .await
            .unwrap();

        let stats = monitor.stats();
        assert_eq!(stats.sent, 5);
    }
}

// Comprehensive channel behavior tests
#[cfg(test)]
mod comprehensive_tests {
    use super::*;
    use tokio::sync::mpsc;

    #[sinex_test]
    async fn test_channel_setup_creation(_ctx: TestContext) -> crate::Result<()> {
        // Test different channel setup methods
        let zero_cap = TestChannelSetup::<i32>::zero_capacity();
        assert_eq!(zero_cap.monitor.stats().sent, 0);
        assert_eq!(zero_cap.monitor.stats().received, 0);

        let small_cap = TestChannelSetup::<String>::small_capacity();
        assert_eq!(small_cap.monitor.stats().errors, 0);

        let large_cap = TestChannelSetup::<Vec<u8>>::large_capacity();
        assert_eq!(large_cap.monitor.queue_depth(), 0);

        Ok(())
    }

    #[sinex_test]
    async fn test_basic_send_receive_behavior(_ctx: TestContext) -> crate::Result<()> {
        let mut setup = TestChannelSetup::<String>::new(10);

        // Test successful send/receive
        behavior::test_basic_send_receive(
            &setup.sender,
            &mut setup.receiver,
            "test_message".to_string(),
            "basic_test",
        )
        .await?;

        // Monitor should track the operation
        assert_eq!(setup.monitor.stats().sent, 0); // Not tracked by test function

        Ok(())
    }

    #[sinex_test]
    async fn test_channel_timeout_behavior(_ctx: TestContext) -> crate::Result<()> {
        let mut setup = TestChannelSetup::<i32>::new(1);

        // Test timeout when no data
        behavior::test_channel_timeout(&mut setup.receiver, Duration::from_millis(100), true)
            .await?;

        // Send data and test no timeout
        setup.sender.send(42).await?;
        behavior::test_channel_timeout(&mut setup.receiver, Duration::from_millis(100), false)
            .await?;

        Ok(())
    }

    #[sinex_test]
    async fn test_backpressure_handling(_ctx: TestContext) -> crate::Result<()> {
        let setup = TestChannelSetup::<i32>::zero_capacity();
        let _backpressure = BackpressureManager::new(
            10, // high watermark
            5,  // low watermark
        );

        // Test backpressure detection
        let result = backpressure::test_backpressure_management(
            &setup.sender,
            vec![1, 2, 3, 4, 5],
            Duration::from_millis(10),
        )
        .await;

        // Should handle backpressure appropriately
        assert!(result.is_ok());

        Ok(())
    }

    #[sinex_test]
    async fn test_batch_receive_operations(_ctx: TestContext) -> crate::Result<()> {
        let mut setup = TestChannelSetup::<String>::new(20);

        // Send multiple items
        let items = vec![
            "item1".to_string(),
            "item2".to_string(),
            "item3".to_string(),
            "item4".to_string(),
            "item5".to_string(),
        ];

        for item in &items {
            setup.sender.send(item.clone()).await?;
        }

        // Test batch receive
        behavior::test_batch_receive(
            &setup.sender,
            &mut setup.receiver,
            items,
            5, // max_batch_size
            Duration::from_secs(1),
        )
        .await?;

        Ok(())
    }

    #[sinex_test]
    async fn test_channel_monitoring(_ctx: TestContext) -> crate::Result<()> {
        let monitor = Arc::new(ChannelMonitor::new());

        // Record various operations
        monitor.record_send();
        monitor.record_send();
        monitor.record_receive();
        monitor.record_error("test error".to_string());

        let stats = monitor.stats();
        assert_eq!(stats.sent, 2);
        assert_eq!(stats.received, 1);
        assert_eq!(stats.errors, 1);
        assert_eq!(stats.queue_depth, 1); // 2 sent - 1 received
        assert_eq!(stats.last_error, Some("test error".to_string()));

        Ok(())
    }

    #[sinex_test]
    async fn test_channel_extension_traits(_ctx: TestContext) -> crate::Result<()> {
        let (sender, mut receiver) = mpsc::channel::<i32>(5);

        // Test send_or_log
        sender.send_or_log(42, "test_context").await?;

        // Test send_timeout
        sender.send_timeout(43, Duration::from_secs(1)).await?;

        // Test try_send_or_queue
        let mut queue = Vec::new();
        sender.try_send_or_queue(44, &mut queue, 10).await?;

        // Test recv_timeout
        let received = receiver.recv_timeout(Duration::from_secs(1)).await?;
        assert_eq!(received, Some(42));

        // Test recv_batch
        let batch = receiver.recv_batch(5, Duration::from_millis(100)).await;
        assert!(batch.len() >= 2); // Should have at least 43 and 44

        Ok(())
    }

    #[sinex_test]
    async fn test_channel_error_handling(_ctx: TestContext) -> crate::Result<()> {
        let (sender, receiver) = mpsc::channel::<String>(1);

        // Drop receiver to cause send errors
        drop(receiver);

        // send_or_log should handle the error gracefully
        let result = sender.send_or_log("test".to_string(), "error_test").await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Channel send failed"));

        Ok(())
    }

    #[sinex_test]
    async fn test_channel_drain_operations(_ctx: TestContext) -> crate::Result<()> {
        let (sender, mut receiver) = mpsc::channel::<i32>(10);

        // Send multiple items
        for i in 0..5 {
            sender.send(i).await?;
        }

        // Test drain_all
        let drained = receiver.drain_all().await;
        assert_eq!(drained.len(), 5);
        assert_eq!(drained, vec![0, 1, 2, 3, 4]);

        // Channel should be empty now
        let empty = receiver.drain_all().await;
        assert!(empty.is_empty());

        Ok(())
    }

    #[sinex_test]
    async fn test_concurrent_channel_operations(_ctx: TestContext) -> crate::Result<()> {
        let setup = TestChannelSetup::<i32>::new(100);
        let sender = setup.sender.clone();
        let monitor = setup.monitor.clone();

        // Spawn multiple senders
        let mut handles = vec![];
        for i in 0..10 {
            let sender_clone = sender.clone();
            let monitor_clone = monitor.clone();
            let handle = tokio::spawn(async move {
                for j in 0..10 {
                    sender_clone.send(i * 10 + j).await?;
                    monitor_clone.record_send();
                }
                Ok::<_, SinexError>(())
            });
            handles.push(handle);
        }

        // Wait for all senders
        for handle in handles {
            handle.await??;
        }

        // Verify monitoring
        assert_eq!(monitor.stats().sent, 100);

        Ok(())
    }

    #[sinex_test]
    async fn test_channel_queue_management(_ctx: TestContext) -> crate::Result<()> {
        let (sender, mut receiver) = mpsc::channel::<String>(2);
        let mut queue = Vec::new();

        // Fill channel
        sender.send("first".to_string()).await?;
        sender.send("second".to_string()).await?;

        // These should go to queue
        sender
            .try_send_or_queue("third".to_string(), &mut queue, 5)
            .await?;
        sender
            .try_send_or_queue("fourth".to_string(), &mut queue, 5)
            .await?;

        assert_eq!(queue.len(), 2);

        // Drain one item
        let _ = receiver.recv().await;

        // Try again - should move one from queue
        sender
            .try_send_or_queue("fifth".to_string(), &mut queue, 5)
            .await?;

        // Queue should have fewer items now
        assert!(queue.len() < 2);

        Ok(())
    }

    #[test]
    fn test_channel_monitor_thread_safety() {
        use std::thread;

        let monitor = Arc::new(ChannelMonitor::new());
        let mut handles = vec![];

        // Spawn threads that increment counters
        for _ in 0..10 {
            let monitor_clone = monitor.clone();
            let handle = thread::spawn(move || {
                for _ in 0..100 {
                    monitor_clone.record_send();
                    monitor_clone.record_receive();
                }
            });
            handles.push(handle);
        }

        // Wait for all threads
        for handle in handles {
            handle.join().unwrap();
        }

        // Verify counts
        let stats = monitor.stats();
        assert_eq!(stats.sent, 1000);
        assert_eq!(stats.received, 1000);
        assert_eq!(stats.queue_depth, 0);
    }
}

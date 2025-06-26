//! Channel testing utilities using ChannelSenderExt and ChannelReceiverExt abstractions
//!
//! This module provides testing utilities for async channel patterns using the new
//! channel extension traits, backpressure management, and monitoring capabilities.

use crate::common::prelude::*;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

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
    
    /// Create a zero-capacity channel for immediate backpressure testing
    pub fn zero_capacity() -> Self {
        Self::new(0)
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
    ) -> TestResult
    where
        T: Send + PartialEq + Debug,
    {
        // Send the value with context
        assert_channel_send_success(sender, test_value, test_name).await?;
        
        // Receive with timeout
        let received = receiver.recv_timeout(Duration::from_secs(1)).await
            .map_err(|e| {
                CoreError::other("Failed to receive from channel")
                    .with_context("test_name", test_name)
                    .with_source(e)
                    .build()
            })?
            .ok_or_else(|| {
                CoreError::other("Channel closed unexpectedly")
                    .with_context("test_name", test_name)
                    .build()
            })?;
        
        Ok(())
    }
    
    /// Test channel timeout behavior
    pub async fn test_channel_timeout<T>(
        receiver: &mut impl ChannelReceiverExt<T>,
        timeout: Duration,
        should_timeout: bool,
    ) -> TestResult
    where
        T: Send,
    {
        let result = receiver.recv_timeout(timeout).await;
        
        match (result.is_err() || result.as_ref().unwrap().is_none(), should_timeout) {
            (true, true) => Ok(()), // Expected timeout
            (false, false) => Ok(()), // Expected receive
            (false, true) => {
                Err(Box::new(CoreError::validation("Expected timeout but received value")
                    .with_context("timeout_duration", format!("{:?}", timeout))
                    .build()))
            }
            (true, false) => {
                Err(Box::new(CoreError::validation("Expected receive but got timeout/close")
                    .with_context("timeout_duration", format!("{:?}", timeout))
                    .build()))
            }
        }
    }
    
    /// Test batch receive functionality
    pub async fn test_batch_receive<T>(
        sender: &impl ChannelSenderExt<T>,
        receiver: &mut impl ChannelReceiverExt<T>,
        items: Vec<T>,
        max_batch_size: usize,
        batch_timeout: Duration,
    ) -> TestResult
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
                return Err(Box::new(
                    CoreError::validation("Received empty batch before all items collected")
                        .with_context("total_received", total_received)
                        .with_context("expected_total", items.len())
                        .build()
                ));
            }
            
            total_received += batch.len();
            batch_count += 1;
            
            // Prevent infinite loops
            if batch_count > 100 {
                return Err(Box::new(
                    CoreError::validation("Too many batches - possible infinite loop")
                        .with_context("batch_count", batch_count)
                        .build()
                ));
            }
        }
        
        assert_eq_with_context(&total_received, &items.len(), "batch receive count")?;
        Ok(())
    }
    
    /// Test channel drain functionality
    pub async fn test_channel_drain<T>(
        sender: &impl ChannelSenderExt<T>,
        receiver: &mut impl ChannelReceiverExt<T>,
        items: Vec<T>,
    ) -> TestResult
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
        
        assert_eq_with_context(&drained.len(), &items.len(), "drain count")?;
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
    ) -> TestResult
    where
        T: Send + Clone,
    {
        let mut backpressure_manager = BackpressureManager::new(10, 5);
        let mut successful_sends = 0;
        let mut timeouts = 0;
        
        for item in test_items {
            match sender.send_timeout(item, expected_timeout).await {
                Ok(()) => {
                    successful_sends += 1;
                    backpressure_manager.check_and_wait(0).await; // Low queue depth
                }
                Err(_) => {
                    timeouts += 1;
                    backpressure_manager.check_and_wait(20).await; // High queue depth
                }
            }
        }
        
        // Validate that we got some timeouts (indicating backpressure)
        assert_with_context(
            timeouts > 0,
            "Expected some timeouts due to backpressure",
            "backpressure test"
        )?;
        
        Ok(())
    }
    
    /// Test queue management with try_send_or_queue
    pub async fn test_queue_management<T>(
        sender: &impl ChannelSenderExt<T>,
        items: Vec<T>,
        max_queue_size: usize,
    ) -> TestResult
    where
        T: Send + Clone,
    {
        let mut queue = Vec::new();
        let mut successful_immediate = 0;
        let mut queued = 0;
        let mut rejected = 0;
        
        for item in items {
            match sender.try_send_or_queue(item, &mut queue, max_queue_size).await {
                Ok(()) => {
                    if queue.is_empty() {
                        successful_immediate += 1;
                    } else {
                        queued += 1;
                    }
                }
                Err(_) => {
                    rejected += 1;
                }
            }
        }
        
        // Validate queue behavior
        assert_with_context(
            queue.len() <= max_queue_size,
            "Queue size should not exceed maximum",
            &format!("queue size: {} max: {}", queue.len(), max_queue_size)
        )?;
        
        Ok(())
    }
    
    /// Test adaptive backpressure with varying load
    pub async fn test_adaptive_backpressure() -> TestResult {
        let mut manager = BackpressureManager::new(50, 20);
        let start_time = tokio::time::Instant::now();
        
        // Simulate varying queue depths
        let queue_depths = vec![10, 30, 60, 80, 40, 15, 5];
        
        for depth in queue_depths {
            manager.check_and_wait(depth).await;
        }
        
        let total_time = start_time.elapsed();
        
        // Should have some delay due to high queue depths
        assert_with_context(
            total_time > Duration::from_millis(10),
            "Adaptive backpressure should introduce some delay",
            &format!("total time: {:?}", total_time)
        )?;
        
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
    ) -> Result<ChannelPerformanceReport, Box<dyn std::error::Error>>
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
                match sender.send_or_log(sender_item.clone(), "throughput_test").await {
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
        let _ = tokio::try_join!(sender_task, receiver_task)?;
        
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
    ) -> TestResult
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
                CoreError::other("Concurrent sender task failed")
                    .with_source(e)
                    .build()
            })?;
        }
        
        let successes = success_counter.load(Ordering::Relaxed);
        let errors = error_counter.load(Ordering::Relaxed);
        
        // Validate results
        assert_with_context(
            successes + errors == total_expected as u64,
            "All operations should be accounted for",
            &format!("successes: {}, errors: {}, expected: {}", successes, errors, total_expected)
        )?;
        
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
    ) -> TestResult
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
        assert_with_context(
            final_stats.sent > initial_stats.sent,
            "Send count should have increased",
            &format!("initial: {}, final: {}", initial_stats.sent, final_stats.sent)
        )?;
        
        assert_with_context(
            final_stats.sent - initial_stats.sent == test_items.len() as u64,
            "Send count should match number of items",
            &format!("delta: {}, items: {}", final_stats.sent - initial_stats.sent, test_items.len())
        )?;
        
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
            receive_rate: (end_stats.received - start_stats.received) as f64 / actual_duration.as_secs_f64(),
            error_rate: (end_stats.errors - start_stats.errors) as f64 / actual_duration.as_secs_f64(),
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
        println!("Success rate: {:.2}%", 
                 (self.items_received as f64 / self.items_sent as f64) * 100.0);
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
    ) -> TestResult
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
            ).await?;
        }
        
        // Test batch operations
        if test_items.len() > 1 {
            behavior::test_batch_receive(
                &test_setup.sender,
                &mut test_setup.receiver,
                test_items.clone(),
                10,
                Duration::from_millis(100),
            ).await?;
        }
        
        // Test monitoring
        monitoring::test_channel_monitoring(
            &test_setup.sender,
            &test_setup.monitor,
            test_items,
        ).await?;
        
        println!("✓ Comprehensive channel test '{}' passed", test_name);
        Ok(())
    }
    
    /// Run backpressure-focused test scenario
    pub async fn run_backpressure_test_scenario<T>(
        test_name: &str,
        test_items: Vec<T>,
    ) -> TestResult
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
        ).await?;
        
        // Test queue management
        backpressure::test_queue_management(
            &test_setup.sender,
            test_items,
            5,
        ).await?;
        
        // Test adaptive backpressure
        backpressure::test_adaptive_backpressure().await?;
        
        println!("✓ Backpressure test '{}' passed", test_name);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_channel_test_utilities() {
        let test_items = vec!["item1", "item2", "item3"];
        
        // Test comprehensive scenario
        scenarios::run_comprehensive_channel_test(
            "string_channel_test",
            test_items.clone(),
            10,
        ).await.unwrap();
        
        // Test backpressure scenario
        scenarios::run_backpressure_test_scenario(
            "string_backpressure_test",
            test_items,
        ).await.unwrap();
    }
    
    #[tokio::test]
    async fn test_channel_performance_measurement() {
        let (tx, rx) = tokio::sync::mpsc::channel(100);
        
        let report = performance::measure_channel_throughput(
            tx,
            rx,
            1000,
            "test_item",
        ).await.unwrap();
        
        assert!(report.items_sent > 0);
        assert!(report.send_rate > 0.0);
        assert_eq!(report.items_sent, report.items_received);
    }
    
    #[tokio::test]
    async fn test_channel_monitoring() {
        let (tx, _rx) = tokio::sync::mpsc::channel(10);
        let monitor = ChannelMonitor::new();
        
        let test_items = vec![1, 2, 3, 4, 5];
        
        monitoring::test_channel_monitoring(&tx, &monitor, test_items).await.unwrap();
        
        let stats = monitor.stats();
        assert_eq!(stats.sent, 5);
    }
}
//! Performance tests for satellite architecture
//!
//! These tests verify that the satellite system can handle high-throughput
//! scenarios similar to the old collector architecture.

use crate::common::prelude::*;
use sinex_satellite_sdk::{EventSource, EventSourceContext, IngestClient, EventSourceRunner};
use std::time::{Duration, Instant};
use tokio::time::{sleep, timeout};

// =============================================================================
// High-Performance Test Satellites
// =============================================================================

/// High-frequency event satellite for performance testing
struct HighFrequencyEventSatellite {
    events_per_second: usize,
    total_events: usize,
    events_sent: usize,
    start_time: Option<Instant>,
    context: Option<EventSourceContext>,
}

impl HighFrequencyEventSatellite {
    fn new(events_per_second: usize, total_events: usize) -> Self {
        Self {
            events_per_second,
            total_events,
            events_sent: 0,
            start_time: None,
            context: None,
        }
    }
}

#[async_trait::async_trait]
impl EventSource for HighFrequencyEventSatellite {
    async fn initialize(&mut self, ctx: EventSourceContext) -> sinex_satellite_sdk::SatelliteResult<()> {
        self.context = Some(ctx);
        self.start_time = Some(Instant::now());
        Ok(())
    }

    async fn start_streaming(&mut self) -> sinex_satellite_sdk::SatelliteResult<()> {
        let ctx = self.context.as_ref().unwrap();
        let interval_micros = 1_000_000 / self.events_per_second as u64;
        let mut interval = tokio::time::interval(Duration::from_micros(interval_micros));
        
        while self.events_sent < self.total_events {
            interval.tick().await;
            
            let event = sinex_events::RawEventBuilder::new(
                "perf-test",
                "high_frequency.event",
                serde_json::json!({
                    "sequence": self.events_sent,
                    "timestamp": chrono::Utc::now().to_rfc3339(),
                    "batch_id": self.events_sent / 100,
                    "payload_size": "medium",
                })
            )
            .with_host(&ctx.host)
            .build();
            
            // Send event via context.event_sender
            if let Err(_) = ctx.event_sender.send(event) {
                break; // Channel closed
            }
            
            self.events_sent += 1;
            
            // Log progress occasionally
            if self.events_sent % 1000 == 0 {
                let elapsed = self.start_time.unwrap().elapsed();
                let rate = self.events_sent as f64 / elapsed.as_secs_f64();
                println!("Sent {} events at {:.2} events/sec", self.events_sent, rate);
            }
        }
        
        Ok(())
    }

    fn source_name(&self) -> &str {
        "perf-test"
    }
}

/// Bursty event satellite that sends events in bursts
struct BurstyEventSatellite {
    burst_size: usize,
    burst_interval: Duration,
    total_bursts: usize,
    bursts_sent: usize,
    events_in_burst: usize,
    context: Option<EventSourceContext>,
}

impl BurstyEventSatellite {
    fn new(burst_size: usize, burst_interval: Duration, total_bursts: usize) -> Self {
        Self {
            burst_size,
            burst_interval,
            total_bursts,
            bursts_sent: 0,
            events_in_burst: 0,
            context: None,
        }
    }
}

#[async_trait::async_trait]
impl EventSource for BurstyEventSatellite {
    async fn initialize(&mut self, ctx: EventSourceContext) -> sinex_satellite_sdk::SatelliteResult<()> {
        self.context = Some(ctx);
        Ok(())
    }

    async fn start_streaming(&mut self) -> sinex_satellite_sdk::SatelliteResult<()> {
        let ctx = self.context.as_ref().unwrap();
        
        while self.bursts_sent < self.total_bursts {
            // Send a burst of events
            for i in 0..self.burst_size {
                let event = sinex_events::RawEventBuilder::new(
                    "burst-test",
                    "burst.event",
                    serde_json::json!({
                        "burst_id": self.bursts_sent,
                        "event_in_burst": i,
                        "timestamp": chrono::Utc::now().to_rfc3339(),
                        "burst_size": self.burst_size,
                    })
                )
                .with_host(&ctx.host)
                .build();
                
                // Send event via context.event_sender
                if let Err(_) = ctx.event_sender.send(event) {
                    return Ok(()); // Channel closed
                }
                
                self.events_in_burst += 1;
            }
            
            self.bursts_sent += 1;
            
            // Wait before next burst
            if self.bursts_sent < self.total_bursts {
                tokio::time::sleep(self.burst_interval).await;
            }
        }
        
        Ok(())
    }

    fn source_name(&self) -> &str {
        "burst-test"
    }
}

// =============================================================================
// Performance Tests
// =============================================================================

/// Test high-frequency event processing
#[sinex_test]
async fn test_high_frequency_event_processing(ctx: TestContext) -> TestResult {
    // Create high-frequency satellite
    let target_rate = 1000; // 1000 events/second
    let test_duration = 2;   // 2 seconds (reduced for testing)
    let total_events = target_rate * test_duration;
    
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    
    let satellite_ctx = EventSourceContext {
        service_name: "perf-test".to_string(),
        host: "perf-host".to_string(),
        work_dir: ctx.work_dir(),
        dry_run: false,
        config: std::collections::HashMap::new(),
        event_sender: tx,
    };
    
    let mut satellite = HighFrequencyEventSatellite::new(target_rate, total_events);
    satellite.initialize(satellite_ctx).await?;
    
    // Measure performance
    let start_time = Instant::now();
    
    // Run the satellite with a timeout
    let satellite_handle = tokio::spawn(async move {
        satellite.start_streaming().await
    });
    
    // Collect events
    let mut events = Vec::new();
    let mut timeout_count = 0;
    
    while events.len() < total_events && timeout_count < 50 {
        match tokio::time::timeout(Duration::from_millis(100), rx.recv()).await {
            Ok(Some(event)) => {
                events.push(event);
            }
            Ok(None) => break, // Channel closed
            Err(_) => {
                timeout_count += 1;
                if satellite_handle.is_finished() {
                    break;
                }
            }
        }
    }
    
    // Wait for completion or timeout
    let result = timeout(Duration::from_secs(10), satellite_handle).await;
    
    let elapsed = start_time.elapsed();
    
    // Check if completed successfully
    assert!(result.is_ok(), "Satellite performance test timed out");
    
    assert_eq!(events.len(), total_events, "Expected {} events, got {}", total_events, events.len());
    
    // Calculate actual throughput
    let actual_rate = events.len() as f64 / elapsed.as_secs_f64();
    
    println!("Performance test results:");
    println!("  Target rate: {} events/sec", target_rate);
    println!("  Actual rate: {:.2} events/sec", actual_rate);
    println!("  Duration: {:.2} seconds", elapsed.as_secs_f64());
    
    // Verify performance is reasonable (allow 40% variance for test environment)
    let expected_min_rate = (target_rate as f64) * 0.6;
    assert!(actual_rate >= expected_min_rate, 
            "Performance too low: {:.2} < {:.2} events/sec", 
            actual_rate, expected_min_rate);
    
    // Verify all events are properly formed
    for event in &events {
        assert_eq!(event.source, "perf-test");
        assert_eq!(event.event_type, "high_frequency.event");
        assert_eq!(event.host, "perf-host");
    }
    
    Ok(())
}

/// Test bursty event processing
#[sinex_test]
async fn test_bursty_event_processing(ctx: TestContext) -> TestResult {
    // Create bursty satellite
    let burst_size = 50;
    let burst_interval = Duration::from_millis(200);
    let total_bursts = 5;
    let expected_total = burst_size * total_bursts;
    
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    
    let satellite_ctx = EventSourceContext {
        service_name: "burst-test".to_string(),
        host: "burst-host".to_string(),
        work_dir: ctx.work_dir(),
        dry_run: false,
        config: std::collections::HashMap::new(),
        event_sender: tx,
    };
    
    let mut satellite = BurstyEventSatellite::new(burst_size, burst_interval, total_bursts);
    satellite.initialize(satellite_ctx).await?;
    
    // Run the satellite
    let start_time = Instant::now();
    
    let satellite_handle = tokio::spawn(async move {
        satellite.start_streaming().await
    });
    
    // Collect events
    let mut events = Vec::new();
    let mut timeout_count = 0;
    
    while events.len() < expected_total && timeout_count < 50 {
        match tokio::time::timeout(Duration::from_millis(100), rx.recv()).await {
            Ok(Some(event)) => {
                events.push(event);
            }
            Ok(None) => break, // Channel closed
            Err(_) => {
                timeout_count += 1;
                if satellite_handle.is_finished() {
                    break;
                }
            }
        }
    }
    
    // Wait for completion
    let result = timeout(Duration::from_secs(10), satellite_handle).await;
    let elapsed = start_time.elapsed();
    
    assert!(result.is_ok(), "Bursty satellite test timed out");
    
    assert_eq!(events.len(), expected_total, "Expected {} events, got {}", expected_total, events.len());
    
    // Verify burst structure
    let mut burst_counts = std::collections::HashMap::new();
    for event in &events {
        let burst_id = event.payload.get("burst_id").unwrap().as_u64().unwrap();
        *burst_counts.entry(burst_id).or_insert(0) += 1;
    }
    
    assert_eq!(burst_counts.len(), total_bursts, "Expected {} bursts, got {}", total_bursts, burst_counts.len());
    
    for (burst_id, count) in burst_counts {
        assert_eq!(count, burst_size, "Burst {} has {} events, expected {}", burst_id, count, burst_size);
    }
    
    // Verify all events are properly formed
    for event in &events {
        assert_eq!(event.source, "burst-test");
        assert_eq!(event.event_type, "burst.event");
        assert_eq!(event.host, "burst-host");
    }
    
    println!("Bursty test results:");
    println!("  Total bursts: {}", total_bursts);
    println!("  Burst size: {}", burst_size);
    println!("  Total events: {}", expected_total);
    println!("  Duration: {:.2} seconds", elapsed.as_secs_f64());
    
    Ok(())
}

/// Test multi-satellite performance under load
#[sinex_test]
async fn test_multi_satellite_performance(ctx: TestContext) -> TestResult {
    // Create multiple satellites
    let satellite_count = 3;
    let events_per_satellite = 200;
    let total_expected = satellite_count * events_per_satellite;
    
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    
    let mut satellite_handles = Vec::new();
    
    for i in 0..satellite_count {
        let satellite_ctx = EventSourceContext {
            service_name: format!("perf-test-{}", i),
            host: "multi-perf-host".to_string(),
            work_dir: ctx.work_dir(),
            dry_run: false,
            config: std::collections::HashMap::new(),
            event_sender: tx.clone(),
        };
        
        let mut satellite = HighFrequencyEventSatellite::new(500, events_per_satellite);
        satellite.initialize(satellite_ctx).await?;
        
        let handle = tokio::spawn(async move {
            satellite.start_streaming().await
        });
        
        satellite_handles.push(handle);
    }
    
    // Drop the sender to allow proper channel closing
    drop(tx);
    
    // Wait for all satellites to complete
    let start_time = Instant::now();
    
    // Collect events from all satellites
    let mut events = Vec::new();
    let mut timeout_count = 0;
    
    while events.len() < total_expected && timeout_count < 100 {
        match tokio::time::timeout(Duration::from_millis(100), rx.recv()).await {
            Ok(Some(event)) => {
                events.push(event);
            }
            Ok(None) => break, // Channel closed
            Err(_) => {
                timeout_count += 1;
                // Check if all satellites are finished
                let all_finished = satellite_handles.iter().all(|h| h.is_finished());
                if all_finished {
                    break;
                }
            }
        }
    }
    
    // Clean up satellite handles
    for handle in satellite_handles {
        handle.abort();
    }
    
    let elapsed = start_time.elapsed();
    
    assert_eq!(events.len(), total_expected, "Expected {} events, got {}", total_expected, events.len());
    
    // Verify events from all satellites
    let mut source_counts = std::collections::HashMap::new();
    for event in &events {
        *source_counts.entry(event.source.clone()).or_insert(0) += 1;
    }
    
    assert_eq!(source_counts.len(), satellite_count, "Expected events from {} satellites", satellite_count);
    
    for (source, count) in &source_counts {
        assert_eq!(*count, events_per_satellite, "Source {} has {} events, expected {}", source, count, events_per_satellite);
    }
    
    let total_rate = events.len() as f64 / elapsed.as_secs_f64();
    
    println!("Multi-satellite performance results:");
    println!("  Satellites: {}", satellite_count);
    println!("  Events per satellite: {}", events_per_satellite);
    println!("  Total events: {}", total_expected);
    println!("  Duration: {:.2} seconds", elapsed.as_secs_f64());
    println!("  Combined rate: {:.2} events/sec", total_rate);
    
    Ok(())
}

/// Test satellite backpressure handling
#[sinex_test]
async fn test_satellite_backpressure(ctx: TestContext) -> TestResult {
    // Create high-frequency satellite that should trigger backpressure
    let total_events = 200;
    let target_rate = 500; // 500 events/sec
    
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    
    let satellite_ctx = EventSourceContext {
        service_name: "backpressure-test".to_string(),
        host: "backpressure-host".to_string(),
        work_dir: ctx.work_dir(),
        dry_run: false,
        config: std::collections::HashMap::new(),
        event_sender: tx,
    };
    
    let mut satellite = HighFrequencyEventSatellite::new(target_rate, total_events);
    satellite.initialize(satellite_ctx).await?;
    
    // Run the satellite
    let start_time = Instant::now();
    
    let satellite_handle = tokio::spawn(async move {
        satellite.start_streaming().await
    });
    
    // Simulate backpressure by consuming events slowly
    let mut events = Vec::new();
    let mut timeout_count = 0;
    
    while events.len() < total_events && timeout_count < 100 {
        match tokio::time::timeout(Duration::from_millis(200), rx.recv()).await {
            Ok(Some(event)) => {
                events.push(event);
                // Simulate processing delay to create backpressure
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            Ok(None) => break, // Channel closed
            Err(_) => {
                timeout_count += 1;
                if satellite_handle.is_finished() {
                    break;
                }
            }
        }
    }
    
    // Wait for completion
    let result = timeout(Duration::from_secs(30), satellite_handle).await;
    let elapsed = start_time.elapsed();
    
    assert!(result.is_ok(), "Backpressure test timed out");
    
    assert_eq!(events.len(), total_events, "Expected {} events, got {}", total_events, events.len());
    
    // Calculate the effective rate after backpressure
    let effective_rate = events.len() as f64 / elapsed.as_secs_f64();
    
    // Should be significantly slower than target rate due to backpressure
    assert!(effective_rate < (target_rate as f64) * 0.8, 
            "Backpressure didn't slow down processing: {:.2} events/sec", effective_rate);
    
    // Verify all events are properly formed
    for event in &events {
        assert_eq!(event.source, "backpressure-test");
        assert_eq!(event.event_type, "high_frequency.event");
        assert_eq!(event.host, "backpressure-host");
    }
    
    println!("Backpressure test results:");
    println!("  Events processed: {}", events.len());
    println!("  Duration: {:.2} seconds", elapsed.as_secs_f64());
    println!("  Target rate: {} events/sec", target_rate);
    println!("  Effective rate: {:.2} events/sec", effective_rate);
    println!("  Backpressure factor: {:.2}x", target_rate as f64 / effective_rate);
    
    Ok(())
}
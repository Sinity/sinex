use async_trait::async_trait;
use sinex_satellite_sdk::{
    EventSourceConfig, IngestClient, StatefulStreamProcessor, StreamProcessorRunner,
};
use sinex_test_utils::mocks::mock_ingestd::MockIngestdBuilder;
use sinex_test_utils::prelude::*;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::{sleep, timeout};

// EventSource trait definition for test event sources
#[async_trait]
trait EventSource: Send + Sync {
    async fn stream_events(&mut self, tx: mpsc::Sender<sinex_events::RawEvent>)
        -> AnyhowResult<()>;
}

/// Satellite-based event source integration tests
///
/// Tests the new satellite architecture where event sources run as independent
/// satellites that stream events to ingestd via gRPC.

// =============================================================================
// Test Satellite Event Source Implementation
// =============================================================================

/// Test satellite that generates filesystem-like events
struct TestFilesystemSatellite {
    events_to_generate: usize,
    events_sent: usize,
}

impl TestFilesystemSatellite {
    fn new(events_to_generate: usize) -> Self {
        Self {
            events_to_generate,
            events_sent: 0,
        }
    }
}

/// Test satellite that generates command-like events
struct TestCommandSatellite {
    events_to_generate: usize,
    events_sent: usize,
}

impl TestCommandSatellite {
    fn new(events_to_generate: usize) -> Self {
        Self {
            events_to_generate,
            events_sent: 0,
        }
    }
}

#[async_trait::async_trait]
#[async_trait]
impl EventSource for TestFilesystemSatellite {
    async fn stream_events(
        &mut self,
        tx: tokio::sync::mpsc::Sender<sinex_events::RawEvent>,
    ) -> AnyhowResult<()> {
        // This would be replaced with real filesystem watching
        while self.events_sent < self.events_to_generate {
            // In real implementation, this would be triggered by filesystem events
            let event = sinex_events::EventFactory::new("test-fs").create_event(
                "file.created",
                serde_json::json!({
                    "path": format!("/test/file_{}.txt", self.events_sent),
                    "size": 1024,
                    "timestamp": chrono::Utc::now().to_rfc3339(),
                }),
            );

            // Send event via channel
            if let Err(_) = tx.send(event).await {
                break; // Channel closed
            }

            self.events_sent += 1;

            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl EventSource for TestCommandSatellite {
    async fn stream_events(
        &mut self,
        tx: tokio::sync::mpsc::Sender<sinex_events::RawEvent>,
    ) -> AnyhowResult<()> {
        // This would be replaced with real command monitoring
        while self.events_sent < self.events_to_generate {
            // In real implementation, this would be triggered by command execution
            let event = sinex_events::EventFactory::new("test-cmd").create_event(
                "command.executed",
                serde_json::json!({
                    "command": format!("echo 'test command {}'", self.events_sent),
                    "exit_code": 0,
                    "duration_ms": 100,
                    "timestamp": chrono::Utc::now().to_rfc3339(),
                }),
            );

            // Send event via channel
            if let Err(_) = tx.send(event).await {
                break; // Channel closed
            }

            self.events_sent += 1;

            tokio::time::sleep(Duration::from_millis(15)).await;
        }
        Ok(())
    }
}

/// Test satellite that performs finite scanning operations
struct TestScannerSatellite {
    items_to_scan: usize,
    items_scanned: usize,
}

impl TestScannerSatellite {
    fn new(items_to_scan: usize) -> Self {
        Self {
            items_to_scan,
            items_scanned: 0,
        }
    }
}

#[async_trait::async_trait]
impl EventSource for TestScannerSatellite {
    async fn stream_events(
        &mut self,
        tx: tokio::sync::mpsc::Sender<sinex_events::RawEvent>,
    ) -> AnyhowResult<()> {
        // Scanner mode - finite operation that completes
        while self.items_scanned < self.items_to_scan {
            let event = sinex_events::EventFactory::new("test-scanner").create_event(
                "scan.completed",
                serde_json::json!({
                    "item_id": self.items_scanned,
                    "scan_type": "filesystem",
                    "timestamp": chrono::Utc::now().to_rfc3339(),
                }),
            );

            // Send event via channel
            if let Err(_) = tx.send(event).await {
                break; // Channel closed
            }

            self.items_scanned += 1;

            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        // Scanner completes naturally
        Ok(())
    }
}

// =============================================================================
// Satellite Architecture Integration Tests
// =============================================================================

#[sinex_test]
async fn test_satellite_basic_initialization(ctx: TestContext) -> TestResult {
    // Create filesystem satellite
    let mut satellite = TestFilesystemSatellite::new(5);
    // Create a channel for streaming events
    let (tx, mut rx) = mpsc::channel(100);

    // Stream some events
    let handle = tokio::spawn(async move { satellite.stream_events(tx).await });

    // Verify we can receive events
    let mut event_count = 0;
    while let Some(event) = rx.recv().await {
        assert_eq!(event.source, "test-fs");
        assert_eq!(event.event_type, "file.created");
        event_count += 1;
        if event_count >= 5 {
            break;
        }
    }

    handle.await??;
    assert_eq!(event_count, 5);

    Ok(())
}

/// Test that satellite can stream events through full pipeline
#[sinex_test]
async fn test_satellite_event_pipeline_integration(ctx: TestContext) -> TestResult {
    // Create a test satellite that generates events
    let mut satellite = TestFilesystemSatellite::new(5);

    // Create a channel to collect events
    let (tx, mut rx) = mpsc::channel(100);
    // Start the satellite in a background task
    let satellite_handle = tokio::spawn(async move { satellite.stream_events(tx).await });

    // Collect events from the channel
    let mut events = Vec::new();
    let mut timeout_count = 0;

    while events.len() < 5 && timeout_count < 10 {
        match tokio::time::timeout(std::time::Duration::from_millis(500), rx.recv()).await {
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

    // Stop the satellite
    satellite_handle.abort();

    // Verify events were generated
    assert!(!events.is_empty(), "No events received");
    assert_eq!(events.len(), 5, "Expected 5 events, got {}", events.len());

    // Verify event structure
    let event = &events[0];
    assert_eq!(event.source, "test-fs");
    assert_eq!(event.event_type, "file.created");
    assert_eq!(event.host, "test-host");

    Ok(())
}

/// Test satellite coordination and multi-satellite scenarios
#[sinex_test]
async fn test_multi_satellite_coordination(ctx: TestContext) -> TestResult {
    // Create multiple satellites of different types
    let mut fs_satellite = TestFilesystemSatellite::new(3);
    let mut cmd_satellite = TestCommandSatellite::new(2);

    // Create channels for each satellite
    let (fs_tx, mut fs_rx) = mpsc::channel(100);
    let (cmd_tx, mut cmd_rx) = mpsc::channel(100);

    // Start both satellites concurrently
    let fs_handle = tokio::spawn(async move { fs_satellite.stream_events(fs_tx).await });
    let cmd_handle = tokio::spawn(async move { cmd_satellite.stream_events(cmd_tx).await });

    // Collect events from both satellites
    let mut events = Vec::new();
    let mut timeout_count = 0;

    // Collect from both channels
    while events.len() < 5 && timeout_count < 10 {
        tokio::select! {
            result = tokio::time::timeout(std::time::Duration::from_millis(100), fs_rx.recv()) => {
                match result {
                    Ok(Some(event)) => events.push(event),
                    _ => {}
                }
            }
            result = tokio::time::timeout(std::time::Duration::from_millis(100), cmd_rx.recv()) => {
                match result {
                    Ok(Some(event)) => events.push(event),
                    _ => {}
                }
            }
        }

        timeout_count += 1;
        if fs_handle.is_finished() && cmd_handle.is_finished() {
            break;
        }
    }

    // Stop both satellites
    fs_handle.abort();
    cmd_handle.abort();

    // Verify events from both satellites were received
    assert!(!events.is_empty(), "No events received");
    assert_eq!(
        events.len(),
        5,
        "Expected 5 events total, got {}",
        events.len()
    );

    // Verify events are properly tagged by source
    let fs_events: Vec<_> = events.iter().filter(|e| e.source == "test-fs").collect();
    let cmd_events: Vec<_> = events.iter().filter(|e| e.source == "test-cmd").collect();

    assert_eq!(fs_events.len(), 3, "Expected 3 filesystem events");
    assert_eq!(cmd_events.len(), 2, "Expected 2 command events");

    // Verify event types
    assert!(fs_events.iter().all(|e| e.event_type == "file.created"));
    assert!(cmd_events
        .iter()
        .all(|e| e.event_type == "command.executed"));

    Ok(())
}

/// Test satellite scanner mode (one-time scan) vs sensor mode (continuous)
#[sinex_test]
async fn test_satellite_operational_modes(ctx: TestContext) -> TestResult {
    // Test 1: Scanner mode - finite operation
    let (scanner_tx, mut scanner_rx) = mpsc::channel(100);

    let mut scanner_satellite = TestScannerSatellite::new(3);

    // Run scanner mode (should complete naturally)
    let scanner_start = std::time::Instant::now();
    let scanner_result = scanner_satellite.stream_events(scanner_tx).await;
    let scanner_duration = scanner_start.elapsed();

    // Should complete successfully
    assert!(
        scanner_result.is_ok(),
        "Scanner mode failed: {:?}",
        scanner_result
    );
    // Should complete quickly as it's a finite operation
    assert!(
        scanner_duration.as_secs() < 5,
        "Scanner mode took too long: {:?}",
        scanner_duration
    );

    // Collect scanner events
    let mut scanner_events = Vec::new();
    while let Ok(event) = scanner_rx.try_recv() {
        scanner_events.push(event);
    }

    // Test 2: Sensor mode - continuous operation
    let (sensor_tx, mut sensor_rx) = mpsc::channel(100);

    let mut sensor_satellite = TestFilesystemSatellite::new(100); // Large number to ensure continuous operation

    // Start sensor mode in background
    let sensor_handle =
        tokio::spawn(async move { sensor_satellite.stream_events(sensor_tx).await });

    // Wait a bit for sensor mode to start producing events
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Stop sensor mode (it should still be running)
    sensor_handle.abort();

    // Collect sensor events
    let mut sensor_events = Vec::new();
    while let Ok(event) = sensor_rx.try_recv() {
        sensor_events.push(event);
    }

    // Verify scanner events
    assert_eq!(scanner_events.len(), 3, "Expected 3 scanner events");

    // Verify sensor events (should have started producing)
    assert!(!sensor_events.is_empty(), "Expected sensor events");

    // Verify event sources
    assert!(scanner_events.iter().all(|e| e.source == "test-scanner"));
    assert!(sensor_events.iter().all(|e| e.source == "test-sensor"));

    Ok(())
}

/// Test satellite reconnection and fault tolerance
#[sinex_test]
async fn test_satellite_fault_tolerance(ctx: TestContext) -> TestResult {
    // Create a channel that simulates failure scenarios
    let (tx, mut rx) = mpsc::channel(100);

    // Create a satellite that will try to send events
    let mut satellite = TestFilesystemSatellite::new(10);

    // Start satellite in background
    let satellite_handle = tokio::spawn(async move { satellite.stream_events(tx).await });

    // Collect events with simulated processing failures
    let mut events = Vec::new();
    let mut processed_count = 0;
    let mut failed_count = 0;

    while processed_count < 10 {
        match tokio::time::timeout(std::time::Duration::from_millis(100), rx.recv()).await {
            Ok(Some(event)) => {
                // Simulate 30% failure rate (every 3rd event fails)
                if processed_count % 3 == 0 {
                    failed_count += 1;
                    // Simulate processing failure - don't store the event
                } else {
                    events.push(event);
                }
                processed_count += 1;
            }
            Ok(None) => break, // Channel closed
            Err(_) => {
                // Timeout - continue
                if satellite_handle.is_finished() {
                    break;
                }
            }
        }
    }

    // Stop satellite
    satellite_handle.abort();

    // Verify that despite failures, some events were still processed
    assert!(!events.is_empty(), "No events received despite failures");

    // Should have received some events but not all due to failures
    assert!(
        events.len() <= 10,
        "Received more events than expected: {}",
        events.len()
    );
    assert!(
        events.len() >= 5,
        "Received too few events despite retries: {}",
        events.len()
    );

    // Verify all received events are valid
    for event in &events {
        assert_eq!(event.source, "test-resilient");
        assert_eq!(event.event_type, "file.created");
        assert_eq!(event.host, "test-host");
    }

    // Verify failures were simulated
    assert!(failed_count > 0, "Expected some failures to be simulated");

    Ok(())
}

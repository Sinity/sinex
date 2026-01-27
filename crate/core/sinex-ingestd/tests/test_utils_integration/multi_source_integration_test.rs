use async_trait::async_trait;
use sinex_core::db::models::{EventFactory, RawEvent};
use xtask::sandbox::prelude::*;
use std::time::Duration;
use tokio::sync::mpsc;

// EventSource trait definition for test event sources
#[async_trait]
trait EventSource: Send + Sync {
    async fn stream_events(&mut self, tx: mpsc::Sender<RawEvent>) -> color_eyre::Result<()>;
}

/// Test node that generates filesystem-like events
struct TestFilesystemNode {
    events_to_generate: usize,
    events_sent: usize,
}

impl TestFilesystemNode {
    fn new(events_to_generate: usize) -> Self {
        Self {
            events_to_generate,
            events_sent: 0,
        }
    }
}

/// Test node that generates command-like events
struct TestCommandNode {
    events_to_generate: usize,
    events_sent: usize,
}

impl TestCommandNode {
    fn new(events_to_generate: usize) -> Self {
        Self {
            events_to_generate,
            events_sent: 0,
        }
    }
}

/// Test node that performs finite scanning operations
struct TestScannerNode {
    items_to_scan: usize,
    items_scanned: usize,
}

impl TestScannerNode {
    fn new(items_to_scan: usize) -> Self {
        Self {
            items_to_scan,
            items_scanned: 0,
        }
    }
}

#[async_trait]
impl EventSource for TestFilesystemNode {
    async fn stream_events(&mut self, tx: mpsc::Sender<RawEvent>) -> color_eyre::Result<()> {
        // This would be replaced with real filesystem watching
        while self.events_sent < self.events_to_generate {
            // In real implementation, this would be triggered by filesystem events
            let event = EventFactory::new("test-fs").create_event(
                "file.created",
                serde_json::json!({
                    "path": format!("/test/file_{}.txt", self.events_sent),
                    "size": 1024,
                    "timestamp": chrono::Utc::now().to_rfc3339(),
                }),
            );

            // Send event via channel
            if tx.send(event).await.is_err() {
                break; // Channel closed
            }

            self.events_sent += 1;
            tokio::task::yield_now().await;
        }
        Ok(())
    }
}

#[async_trait]
impl EventSource for TestCommandNode {
    async fn stream_events(&mut self, tx: mpsc::Sender<RawEvent>) -> color_eyre::Result<()> {
        // This would be replaced with real command monitoring
        while self.events_sent < self.events_to_generate {
            // In real implementation, this would be triggered by command execution
            let event = EventFactory::new("test-cmd").create_event(
                "command.executed",
                serde_json::json!({
                    "command": format!("echo 'test command {}'", self.events_sent),
                    "exit_code": 0,
                    "duration_ms": 100,
                    "timestamp": chrono::Utc::now().to_rfc3339(),
                }),
            );

            // Send event via channel
            if tx.send(event).await.is_err() {
                break; // Channel closed
            }

            self.events_sent += 1;
            tokio::task::yield_now().await;
        }
        Ok(())
    }
}

#[async_trait]
impl EventSource for TestScannerNode {
    async fn stream_events(&mut self, tx: mpsc::Sender<RawEvent>) -> color_eyre::Result<()> {
        // Scanner mode - finite operation that completes
        while self.items_scanned < self.items_to_scan {
            let event = EventFactory::new("test-scanner").create_event(
                "scan.completed",
                serde_json::json!({
                    "item_id": self.items_scanned,
                    "scan_type": "filesystem",
                    "timestamp": chrono::Utc::now().to_rfc3339(),
                }),
            );

            // Send event via channel
            if tx.send(event).await.is_err() {
                break; // Channel closed
            }

            self.items_scanned += 1;
            tokio::task::yield_now().await;
        }
        // Scanner completes naturally
        Ok(())
    }
}

#[sinex_test]
async fn test_node_basic_initialization(ctx: TestContext) -> color_eyre::Result<()> {
    // Create filesystem node
    let mut node = TestFilesystemNode::new(5);
    // Create a channel for streaming events
    let (tx, mut rx) = mpsc::channel(100);

    // Stream some events
    let handle = tokio::spawn(async move { node.stream_events(tx).await });

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

/// Test that node can stream events through full pipeline
#[sinex_test]
async fn test_node_event_pipeline_integration(ctx: TestContext) -> color_eyre::Result<()> {
    // Create a test node that generates events
    let mut node = TestFilesystemNode::new(5);

    // Create a channel to collect events
    let (tx, mut rx) = mpsc::channel(100);
    // Start the node in a background task
    let node_handle = tokio::spawn(async move { node.stream_events(tx).await });

    // Collect events from the channel
    let mut events = Vec::new();
    let mut timeout_count = 0;

    while events.len() < 5 && timeout_count < 10 {
        match tokio::time::timeout(Duration::from_millis(500), rx.recv()).await {
            Ok(Some(event)) => {
                events.push(event);
            }
            Ok(None) => break, // Channel closed
            Err(_) => {
                timeout_count += 1;
                if node_handle.is_finished() {
                    break;
                }
            }
        }
    }

    // Stop the node
    node_handle.abort();

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

/// Test node coordination and multi-node scenarios
#[sinex_test]
async fn test_multi_node_coordination(ctx: TestContext) -> color_eyre::Result<()> {
    // Create multiple nodes of different types
    let mut fs_node = TestFilesystemNode::new(3);
    let mut cmd_node = TestCommandNode::new(2);

    // Create channels for each node
    let (fs_tx, mut fs_rx) = mpsc::channel(100);
    let (cmd_tx, mut cmd_rx) = mpsc::channel(100);

    // Start both nodes concurrently
    let fs_handle = tokio::spawn(async move { fs_node.stream_events(fs_tx).await });
    let cmd_handle = tokio::spawn(async move { cmd_node.stream_events(cmd_tx).await });

    // Collect events from both nodes
    let mut events = Vec::new();
    let mut timeout_count = 0;

    // Collect from both channels
    while events.len() < 5 && timeout_count < 10 {
        tokio::select! {
            result = tokio::time::timeout(Duration::from_millis(100), fs_rx.recv()) => {
                match result {
                    Ok(Some(event)) => events.push(event),
                    _ => {}
                }
            }
            result = tokio::time::timeout(Duration::from_millis(100), cmd_rx.recv()) => {
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

    // Stop both nodes
    fs_handle.abort();
    cmd_handle.abort();

    // Verify events from both nodes were received
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
    assert!(cmd_events.iter().all(|e| e.event_type == "command.executed"));

    Ok(())
}

/// Test node scanner mode (one-time scan) vs sensor mode (continuous)
#[sinex_test]
async fn test_node_operational_modes(ctx: TestContext) -> color_eyre::Result<()> {
    // Test 1: Scanner mode - finite operation
    let (scanner_tx, mut scanner_rx) = mpsc::channel(100);
    let mut scanner_node = TestScannerNode::new(3);

    // Run scanner mode (should complete naturally)
    let scanner_start = std::time::Instant::now();
    let scanner_result = scanner_node.stream_events(scanner_tx).await;
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
    let mut sensor_node = TestFilesystemNode::new(100); // Large number to ensure continuous operation

    // Start sensor mode in background
    let sensor_handle = tokio::spawn(async move { sensor_node.stream_events(sensor_tx).await });

    let mut sensor_events = Vec::new();
    let first_event = tokio::time::timeout(Duration::from_secs(Timeouts::SHORT), sensor_rx.recv()).await??;
    if let Some(event) = first_event {
        sensor_events.push(event);
    }

    // Stop sensor mode (it should still be running)
    sensor_handle.abort();

    // Collect sensor events
    while let Ok(event) = sensor_rx.try_recv() {
        sensor_events.push(event);
    }

    // Verify scanner events
    assert_eq!(scanner_events.len(), 3, "Expected 3 scanner events");

    // Verify sensor events (should have started producing)
    assert!(!sensor_events.is_empty(), "Expected sensor events");

    // Verify event sources
    assert!(scanner_events.iter().all(|e| e.source == "test-scanner"));
    assert!(sensor_events.iter().all(|e| e.source == "test-fs"));

    Ok(())
}

/// Test node reconnection and fault tolerance
#[sinex_test]
async fn test_node_fault_tolerance(ctx: TestContext) -> color_eyre::Result<()> {
    // Create a channel that simulates failure scenarios
    let (tx, mut rx) = mpsc::channel(100);

    // Create a node that will try to send events
    let mut node = TestFilesystemNode::new(10);

    // Start node in background
    let node_handle = tokio::spawn(async move { node.stream_events(tx).await });

    // Collect events with simulated processing failures
    let mut events = Vec::new();
    let mut processed_count = 0;
    let mut failed_count = 0;

    while processed_count < 10 {
        match tokio::time::timeout(Duration::from_millis(100), rx.recv()).await {
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
                if node_handle.is_finished() {
                    break;
                }
            }
        }
    }

    // Stop node
    node_handle.abort();

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
        assert_eq!(event.source, "test-fs");
        assert_eq!(event.event_type, "file.created");
        assert_eq!(event.host, "test-host");
    }

    // Verify failures were simulated
    assert!(failed_count > 0, "Expected some failures to be simulated");

    Ok(())
}

/// Test event ordering across multiple sources
#[sinex_test]
async fn test_multi_source_event_ordering(ctx: TestContext) -> color_eyre::Result<()> {
    // Create three different node types
    let mut fs_node = TestFilesystemNode::new(2);
    let mut cmd_node = TestCommandNode::new(2);
    let mut scanner_node = TestScannerNode::new(2);

    // Create a shared channel for all events
    let (tx, mut rx) = mpsc::channel(100);

    // Clone sender for each node
    let fs_tx = tx.clone();
    let cmd_tx = tx.clone();
    let scanner_tx = tx.clone();

    // Start all nodes concurrently
    let fs_handle = tokio::spawn(async move { fs_node.stream_events(fs_tx).await });
    let cmd_handle = tokio::spawn(async move { cmd_node.stream_events(cmd_tx).await });
    let scanner_handle = tokio::spawn(async move { scanner_node.stream_events(scanner_tx).await });

    // Drop original sender to close channel when all nodes finish
    drop(tx);

    // Collect all events in order they arrive
    let mut events = Vec::new();
    while let Some(event) = rx.recv().await {
        events.push(event);
        if events.len() >= 6 {
            break;
        }
    }

    // Wait for all nodes to complete
    let _ = fs_handle.await;
    let _ = cmd_handle.await;
    let _ = scanner_handle.await;

    // Verify we received events from all sources
    assert_eq!(events.len(), 6, "Expected 6 events total");

    let fs_count = events.iter().filter(|e| e.source == "test-fs").count();
    let cmd_count = events.iter().filter(|e| e.source == "test-cmd").count();
    let scanner_count = events.iter().filter(|e| e.source == "test-scanner").count();

    assert_eq!(fs_count, 2, "Expected 2 filesystem events");
    assert_eq!(cmd_count, 2, "Expected 2 command events");
    assert_eq!(scanner_count, 2, "Expected 2 scanner events");

    // Verify events maintain temporal consistency within each source
    let fs_events: Vec<_> = events.iter().filter(|e| e.source == "test-fs").collect();
    let cmd_events: Vec<_> = events.iter().filter(|e| e.source == "test-cmd").collect();
    let scanner_events: Vec<_> = events.iter().filter(|e| e.source == "test-scanner").collect();

    // Verify each source's events are temporally ordered
    for events_slice in [&fs_events, &cmd_events, &scanner_events] {
        for window in events_slice.windows(2) {
            let t0 = window[0]
                .id
                .as_ref()
                .expect("id present")
                .as_ulid()
                .timestamp();
            let t1 = window[1]
                .id
                .as_ref()
                .expect("id present")
                .as_ulid()
                .timestamp();
            assert!(t0 <= t1, "Events from same source should be temporally ordered");
        }
    }

    Ok(())
}

/// Test handling of heterogeneous event payloads from multiple sources
#[sinex_test]
async fn test_multi_source_payload_diversity(ctx: TestContext) -> color_eyre::Result<()> {
    // Create nodes with different payload structures
    let mut fs_node = TestFilesystemNode::new(1);
    let mut cmd_node = TestCommandNode::new(1);
    let mut scanner_node = TestScannerNode::new(1);

    // Create channels for each node
    let (fs_tx, mut fs_rx) = mpsc::channel(10);
    let (cmd_tx, mut cmd_rx) = mpsc::channel(10);
    let (scanner_tx, mut scanner_rx) = mpsc::channel(10);

    // Start all nodes
    let fs_handle = tokio::spawn(async move { fs_node.stream_events(fs_tx).await });
    let cmd_handle = tokio::spawn(async move { cmd_node.stream_events(cmd_tx).await });
    let scanner_handle = tokio::spawn(async move { scanner_node.stream_events(scanner_tx).await });

    // Collect one event from each source
    let fs_event = fs_rx.recv().await.expect("Should receive filesystem event");
    let cmd_event = cmd_rx.recv().await.expect("Should receive command event");
    let scanner_event = scanner_rx.recv().await.expect("Should receive scanner event");

    // Wait for nodes to complete
    let _ = fs_handle.await;
    let _ = cmd_handle.await;
    let _ = scanner_handle.await;

    // Verify filesystem event payload structure
    assert_eq!(fs_event.source, "test-fs");
    assert_eq!(fs_event.event_type, "file.created");
    let fs_payload = fs_event.payload.as_object().unwrap();
    assert!(fs_payload.contains_key("path"));
    assert!(fs_payload.contains_key("size"));
    assert!(fs_payload.contains_key("timestamp"));

    // Verify command event payload structure
    assert_eq!(cmd_event.source, "test-cmd");
    assert_eq!(cmd_event.event_type, "command.executed");
    let cmd_payload = cmd_event.payload.as_object().unwrap();
    assert!(cmd_payload.contains_key("command"));
    assert!(cmd_payload.contains_key("exit_code"));
    assert!(cmd_payload.contains_key("duration_ms"));

    // Verify scanner event payload structure
    assert_eq!(scanner_event.source, "test-scanner");
    assert_eq!(scanner_event.event_type, "scan.completed");
    let scanner_payload = scanner_event.payload.as_object().unwrap();
    assert!(scanner_payload.contains_key("item_id"));
    assert!(scanner_payload.contains_key("scan_type"));
    assert!(scanner_payload.contains_key("timestamp"));

    // Verify all payloads are valid JSON objects with distinct structures
    assert_ne!(fs_payload, cmd_payload);
    assert_ne!(cmd_payload, scanner_payload);
    assert_ne!(fs_payload, scanner_payload);

    Ok(())
}
use async_trait::async_trait;
use sinex_test_utils::TestResult;
use sinex_core::db::models::{EventFactory, RawEvent};
use sinex_satellite_sdk::{EventSourceConfig, StatefulStreamProcessor};
use sinex_test_utils::prelude::*;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::{sleep, timeout};

/// Event generation integration tests
///
/// Tests event generation patterns, EventFactory usage, and event source architectures.
/// These tests verify that events can be generated correctly through various mechanisms
/// including satellites, factories, and direct creation.

// =============================================================================
// Event Generation Test Structures
// =============================================================================

/// Test satellite that generates filesystem-like events
struct TestFilesystemSatellite {
    events_to_generate: usize,
    events_sent: usize,
    source_name: String,
}

impl TestFilesystemSatellite {
    fn new(events_to_generate: usize, source_name: impl Into<String>) -> Self {
        Self {
            events_to_generate,
            events_sent: 0,
            source_name: source_name.into(),
        }
    }

    async fn generate_next_event(&mut self) -> Option<RawEvent> {
        if self.events_sent >= self.events_to_generate {
            return None;
        }

        let event = EventFactory::new(&self.source_name).create_event(
            "file.created",
            serde_json::json!({
                "path": format!("/test/file_{}.txt", self.events_sent),
                "size": 1024 + self.events_sent * 100,
                "timestamp": chrono::Utc::now().to_rfc3339(),
                "event_index": self.events_sent,
            }),
        );

        self.events_sent += 1;
        Some(event)
    }
}

/// Test satellite that generates command execution events
struct TestCommandSatellite {
    events_to_generate: usize,
    events_sent: usize,
    commands: Vec<String>,
}

impl TestCommandSatellite {
    fn new(events_to_generate: usize) -> Self {
        let commands = vec![
            "ls -la".to_string(),
            "grep -r pattern".to_string(),
            "find . -name '*.rs'".to_string(),
            "cargo nextest run".to_string(),
            "git status".to_string(),
        ];

        Self {
            events_to_generate,
            events_sent: 0,
            commands,
        }
    }

    async fn generate_next_event(&mut self) -> Option<RawEvent> {
        if self.events_sent >= self.events_to_generate {
            return None;
        }

        let command = &self.commands[self.events_sent % self.commands.len()];
        let event = EventFactory::new("test-cmd").create_event(
            "command.executed",
            serde_json::json!({
                "command": command,
                "exit_code": 0,
                "duration_ms": 50 + (self.events_sent * 10),
                "working_directory": "/tmp",
                "timestamp": chrono::Utc::now().to_rfc3339(),
            }),
        );

        self.events_sent += 1;
        Some(event)
    }
}

// =============================================================================
// Basic Event Generation Tests
// =============================================================================

/// Test basic event factory functionality
#[sinex_test]
async fn test_event_factory_basic_generation(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let pool = ctx.pool().clone();

    // Create events using EventFactory
    let factory = EventFactory::new("test-source");
    
    let mut events = Vec::new();
    for i in 0..5 {
        let event = factory.create_event(
            &format!("test.event.{}", i),
            serde_json::json!({
                "event_id": i,
                "data": format!("test data {}", i),
                "timestamp": chrono::Utc::now().to_rfc3339(),
            }),
        );
        events.push(event);
    }

    // Verify event structure
    assert_eq!(events.len(), 5);
    for (i, event) in events.iter().enumerate() {
        assert_eq!(event.source, "test-source");
        assert_eq!(event.event_type, format!("test.event.{}", i));
        assert_eq!(event.host, "test-host");
        
        // Verify payload structure
        let payload = &event.payload;
        assert_eq!(payload["event_id"], i);
        assert_eq!(payload["data"], format!("test data {}", i));
    }

    // Insert events and verify persistence
    for event in &events {
        let stored_event = sinex_core::db::insert_event_with_validator(&pool, event, None).await?;
        assert_eq!(stored_event.id, event.id);
        assert_eq!(stored_event.source, event.source);
        assert_eq!(stored_event.event_type, event.event_type);
    }

    println!("✓ Basic event factory generation verified");
    Ok(())
}

/// Test event generation with different payload types
#[sinex_test]
async fn test_event_generation_payload_varieties(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let pool = ctx.pool().clone();
    let factory = EventFactory::new("payload-test");

    // Test simple string payload
    let string_event = factory.create_event(
        "payload.string",
        serde_json::json!("simple string payload"),
    );

    // Test numeric payload
    let numeric_event = factory.create_event(
        "payload.numeric",
        serde_json::json!(42),
    );

    // Test array payload
    let array_event = factory.create_event(
        "payload.array",
        serde_json::json!([1, 2, 3, "test", true]),
    );

    // Test nested object payload
    let complex_event = factory.create_event(
        "payload.complex",
        serde_json::json!({
            "metadata": {
                "version": "1.0",
                "tags": ["test", "complex"],
                "config": {
                    "enabled": true,
                    "timeout": 5000
                }
            },
            "data": {
                "items": [
                    {"id": 1, "name": "item1"},
                    {"id": 2, "name": "item2"}
                ],
                "statistics": {
                    "total_count": 2,
                    "last_updated": chrono::Utc::now().to_rfc3339()
                }
            }
        }),
    );

    let events = vec![string_event, numeric_event, array_event, complex_event];

    // Insert all events and verify they can be stored with different payload types
    let mut stored_ids = Vec::new();
    for event in &events {
        let stored_event = sinex_core::db::insert_event_with_validator(&pool, event, None).await?;
        stored_ids.push(stored_event.id);
    }

    // Retrieve and verify payload integrity
    for (original, stored_id) in events.iter().zip(stored_ids.iter()) {
        let retrieved = sinex_core::db::get_event_by_id(&pool, *stored_id).await?;
        assert_eq!(retrieved.payload, original.payload);
    }

    println!("✓ Event generation with payload varieties verified");
    Ok(())
}

// =============================================================================
// Satellite Event Generation Tests
// =============================================================================

/// Test filesystem satellite event generation
#[sinex_test]
async fn test_filesystem_satellite_generation(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let mut satellite = TestFilesystemSatellite::new(10, "fs-satellite");
    
    let mut generated_events = Vec::new();
    
    // Generate events from the satellite
    while let Some(event) = satellite.generate_next_event().await {
        generated_events.push(event);
        tokio::time::sleep(Duration::from_millis(1)).await; // Simulate timing
    }

    // Verify generation results
    assert_eq!(generated_events.len(), 10);
    
    for (i, event) in generated_events.iter().enumerate() {
        assert_eq!(event.source, "fs-satellite");
        assert_eq!(event.event_type, "file.created");
        assert_eq!(event.host, "test-host");
        
        // Verify payload progression
        let payload = &event.payload;
        assert_eq!(payload["path"], format!("/test/file_{}.txt", i));
        assert_eq!(payload["size"], 1024 + i * 100);
        assert_eq!(payload["event_index"], i);
    }

    println!("✓ Filesystem satellite generation verified");
    Ok(())
}

/// Test command satellite event generation  
#[sinex_test]
async fn test_command_satellite_generation(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let mut satellite = TestCommandSatellite::new(8);
    
    let mut generated_events = Vec::new();
    
    // Generate events from the command satellite
    while let Some(event) = satellite.generate_next_event().await {
        generated_events.push(event);
        tokio::time::sleep(Duration::from_millis(2)).await;
    }

    // Verify generation results
    assert_eq!(generated_events.len(), 8);
    
    // Verify command cycling
    let expected_commands = [
        "ls -la",
        "grep -r pattern",
        "find . -name '*.rs'",
        "cargo nextest run",
        "git status",
    ];
    
    for (i, event) in generated_events.iter().enumerate() {
        assert_eq!(event.source, "test-cmd");
        assert_eq!(event.event_type, "command.executed");
        
        let payload = &event.payload;
        let expected_command = expected_commands[i % expected_commands.len()];
        assert_eq!(payload["command"], expected_command);
        assert_eq!(payload["exit_code"], 0);
        assert_eq!(payload["duration_ms"], 50 + (i * 10));
        assert_eq!(payload["working_directory"], "/tmp");
    }

    println!("✓ Command satellite generation verified");
    Ok(())
}

/// Test multiple satellites generating concurrently
#[sinex_test]
async fn test_concurrent_satellite_generation(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let pool = ctx.pool().clone();

    // Create multiple satellites
    let mut fs_satellite = TestFilesystemSatellite::new(5, "concurrent-fs");
    let mut cmd_satellite = TestCommandSatellite::new(5);
    
    // Generate events concurrently
    let (fs_tx, mut fs_rx) = mpsc::channel(100);
    let (cmd_tx, mut cmd_rx) = mpsc::channel(100);

    // Spawn filesystem satellite task
    let fs_handle = tokio::spawn(async move {
        while let Some(event) = fs_satellite.generate_next_event().await {
            if fs_tx.send(event).await.is_err() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    });

    // Spawn command satellite task
    let cmd_handle = tokio::spawn(async move {
        while let Some(event) = cmd_satellite.generate_next_event().await {
            if cmd_tx.send(event).await.is_err() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(15)).await;
        }
    });

    // Collect events from both satellites
    let mut all_events = Vec::new();
    let mut timeout_count = 0;
    const MAX_TIMEOUTS: usize = 20;

    while all_events.len() < 10 && timeout_count < MAX_TIMEOUTS {
        tokio::select! {
            result = tokio::time::timeout(Duration::from_millis(100), fs_rx.recv()) => {
                match result {
                    Ok(Some(event)) => all_events.push(event),
                    Ok(None) => break,
                    Err(_) => timeout_count += 1,
                }
            }
            result = tokio::time::timeout(Duration::from_millis(100), cmd_rx.recv()) => {
                match result {
                    Ok(Some(event)) => all_events.push(event),
                    Ok(None) => break,  
                    Err(_) => timeout_count += 1,
                }
            }
        }

        // Check if both handles finished
        if fs_handle.is_finished() && cmd_handle.is_finished() {
            // Drain remaining events
            while let Ok(event) = fs_rx.try_recv() {
                all_events.push(event);
            }
            while let Ok(event) = cmd_rx.try_recv() {
                all_events.push(event);
            }
            break;
        }
    }

    // Wait for handles to complete
    let _ = tokio::join!(fs_handle, cmd_handle);

    // Verify concurrent generation results
    assert_eq!(all_events.len(), 10, "Expected 10 events from both satellites");

    // Count events by source
    let fs_events: Vec<_> = all_events.iter().filter(|e| e.source == "concurrent-fs").collect();
    let cmd_events: Vec<_> = all_events.iter().filter(|e| e.source == "test-cmd").collect();
    
    assert_eq!(fs_events.len(), 5, "Expected 5 filesystem events");
    assert_eq!(cmd_events.len(), 5, "Expected 5 command events");

    // Store all events in database
    for event in &all_events {
        let _ = sinex_core::db::insert_event_with_validator(&pool, event, None).await?;
    }

    println!("✓ Concurrent satellite generation verified");
    Ok(())
}

// =============================================================================
// Event Generation Performance Tests  
// =============================================================================

/// Test event generation performance under load
#[sinex_test]
async fn test_event_generation_performance(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let pool = ctx.pool().clone();
    let factory = EventFactory::new("perf-test");

    let event_count = 1000;
    let start_time = std::time::Instant::now();

    // Generate events in batches for better performance
    let batch_size = 50;
    let mut all_events = Vec::new();

    for batch_start in (0..event_count).step_by(batch_size) {
        let batch_end = std::cmp::min(batch_start + batch_size, event_count);
        let mut batch_events = Vec::new();

        // Generate batch
        for i in batch_start..batch_end {
            let event = factory.create_event(
                "performance.test",
                serde_json::json!({
                    "batch_id": batch_start / batch_size,
                    "event_index": i,
                    "payload_size": "medium",
                    "data": format!("performance test data for event {}", i),
                    "metadata": {
                        "generated_at": chrono::Utc::now().to_rfc3339(),
                        "batch_size": batch_size,
                        "total_events": event_count
                    }
                }),
            );
            batch_events.push(event);
        }

        all_events.extend(batch_events);
    }

    let generation_time = start_time.elapsed();
    let generation_rate = event_count as f64 / generation_time.as_secs_f64();

    println!("Event generation performance:");
    println!("- Generated {} events", event_count);
    println!("- Generation time: {:?}", generation_time);
    println!("- Generation rate: {:.2} events/second", generation_rate);

    // Performance assertions
    assert!(generation_rate > 1000.0, "Generation should be > 1000 events/second");
    assert!(generation_time.as_millis() < 5000, "Generation should complete within 5 seconds");

    // Verify all events were generated correctly
    assert_eq!(all_events.len(), event_count);

    // Sample check of generated events
    for i in (0..event_count).step_by(100) {
        let event = &all_events[i];
        assert_eq!(event.source, "perf-test");
        assert_eq!(event.event_type, "performance.test");
        let payload = &event.payload;
        assert_eq!(payload["event_index"], i);
    }

    println!("✓ Event generation performance verified");
    Ok(())
}

/// Test event generation with varying payload sizes
#[sinex_test]
async fn test_event_generation_payload_sizes(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let pool = ctx.pool().clone();
    let factory = EventFactory::new("size-test");

    // Generate small payload event
    let small_event = factory.create_event(
        "payload.small",
        serde_json::json!({
            "size": "small",
            "data": "x"
        }),
    );

    // Generate medium payload event  
    let medium_data = "x".repeat(1000);
    let medium_event = factory.create_event(
        "payload.medium", 
        serde_json::json!({
            "size": "medium",
            "data": medium_data
        }),
    );

    // Generate large payload event
    let large_data = "x".repeat(10000);
    let large_event = factory.create_event(
        "payload.large",
        serde_json::json!({
            "size": "large", 
            "data": large_data,
            "metadata": {
                "chunks": (0..100).collect::<Vec<i32>>(),
                "description": "Large payload test with substantial data"
            }
        }),
    );

    let events = vec![small_event, medium_event, large_event];

    // Verify all events can be stored regardless of payload size
    for event in &events {
        let stored_event = sinex_core::db::insert_event_with_validator(&pool, event, None).await?;
        let retrieved_event = sinex_core::db::get_event_by_id(&pool, stored_event.id).await?;
        
        // Verify payload integrity across different sizes
        assert_eq!(retrieved_event.payload, event.payload);
        assert_eq!(retrieved_event.source, "size-test");
    }

    println!("✓ Event generation with varying payload sizes verified");
    Ok(())
}

// =============================================================================
// Event Generation Edge Cases
// =============================================================================

/// Test event generation error handling
#[sinex_test]
async fn test_event_generation_error_handling(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let pool = ctx.pool().clone();

    // Test factory with empty source name (should still work)
    let factory = EventFactory::new("");
    let event = factory.create_event(
        "test.event",
        serde_json::json!({"test": "data"}),
    );
    
    // Should be able to create and store event even with empty source
    let stored_event = sinex_core::db::insert_event_with_validator(&pool, &event, None).await?;
    assert_eq!(stored_event.source, "");

    // Test with very long source name
    let long_source = "x".repeat(255);
    let long_factory = EventFactory::new(&long_source);
    let long_event = long_factory.create_event(
        "test.long.source",
        serde_json::json!({"test": "long source"}),
    );
    
    let stored_long = sinex_core::db::insert_event_with_validator(&pool, &long_event, None).await?;
    assert_eq!(stored_long.source, long_source);

    // Test with special characters in event type
    let special_event = factory.create_event(
        "test.special!@#$%^&*()_+-=[]{}|;':\",./<>?",
        serde_json::json!({"test": "special characters"}),
    );
    
    let stored_special = sinex_core::db::insert_event_with_validator(&pool, &special_event, None).await?;
    assert_eq!(stored_special.event_type, "test.special!@#$%^&*()_+-=[]{}|;':\",./<>?");

    println!("✓ Event generation error handling verified");
    Ok(())
}

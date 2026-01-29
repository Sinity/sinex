//! Event generation integration tests
//!
//! Tests event generation patterns using TestContext's event publishing capabilities.
//! These tests verify that events can be generated correctly through various mechanisms.

use sinex_node_sdk::db::models::{Event, JsonValue};
use sinex_node_sdk::DynamicPayload;
use std::time::Duration;
use tokio::sync::mpsc;
use xtask::sandbox::prelude::*;

// =============================================================================
// Event Generation Test Structures
// =============================================================================

/// Simple event data for testing generation patterns
struct TestEventData {
    source: String,
    event_type: String,
    payload: JsonValue,
}

impl TestEventData {
    fn filesystem_event(index: usize, source_name: &str) -> Self {
        Self {
            source: source_name.to_string(),
            event_type: "file.created".to_string(),
            payload: serde_json::json!({
                "path": format!("/test/file_{}.txt", index),
                "size": 1024 + index * 100,
                "timestamp": OffsetDateTime::now_utc().format(&time::format_description::well_known::Rfc3339).unwrap(),
                "event_index": index,
            }),
        }
    }

    fn command_event(index: usize, command: &str) -> Self {
        Self {
            source: "test-cmd".to_string(),
            event_type: "command.executed".to_string(),
            payload: serde_json::json!({
                "command": command,
                "exit_code": 0,
                "duration_ms": 50 + (index * 10),
                "working_directory": "/tmp",
                "timestamp": OffsetDateTime::now_utc().format(&time::format_description::well_known::Rfc3339).unwrap(),
            }),
        }
    }
}

// =============================================================================
// Basic Event Generation Tests
// =============================================================================

/// Test basic event generation and publishing
#[sinex_test]
async fn test_event_basic_generation(ctx: TestContext) -> TestResult<()> {
    // Enable shared NATS for proper event pipeline
    let ctx = ctx.with_nats().shared().await?;

    // Create events using TestContext
    let mut events = Vec::new();
    for i in 0..5 {
        let event = ctx
            .publish(DynamicPayload::new(
                "test-source",
                format!("test.event.{}", i),
                serde_json::json!({
                    "event_id": i,
                    "data": format!("test data {}", i),
                    "timestamp": OffsetDateTime::now_utc().format(&time::format_description::well_known::Rfc3339).unwrap(),
                }),
            ))
            .await?;
        events.push(event);
    }

    // Verify event structure
    assert_eq!(events.len(), 5);
    for (i, event) in events.iter().enumerate() {
        assert_eq!(event.source.as_str(), "test-source");
        assert_eq!(event.event_type.as_str(), format!("test.event.{}", i));

        // Verify payload structure
        let payload = &event.payload;
        assert_eq!(payload["event_id"], i);
        assert_eq!(payload["data"], format!("test data {}", i));
    }

    println!("✓ Basic event generation verified");
    Ok(())
}

/// Test event generation with different payload types
#[sinex_test]
async fn test_event_generation_payload_varieties(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;

    // Test simple string payload
    let string_event = ctx
        .publish(DynamicPayload::new(
            "payload-test",
            "payload.string",
            serde_json::json!("simple string payload"),
        ))
        .await?;

    // Test numeric payload
    let numeric_event = ctx
        .publish(DynamicPayload::new(
            "payload-test",
            "payload.numeric",
            serde_json::json!(42),
        ))
        .await?;

    // Test array payload
    let array_event = ctx
        .publish(DynamicPayload::new(
            "payload-test",
            "payload.array",
            serde_json::json!([1, 2, 3, "test", true]),
        ))
        .await?;

    // Test nested object payload
    let complex_event = ctx
        .publish(DynamicPayload::new(
            "payload-test",
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
                        "last_updated": OffsetDateTime::now_utc().format(&time::format_description::well_known::Rfc3339).unwrap()
                    }
                }
            }),
        ))
        .await?;

    // Verify events were created
    assert_eq!(
        string_event.payload,
        serde_json::json!("simple string payload")
    );
    assert_eq!(numeric_event.payload, serde_json::json!(42));
    assert_eq!(
        array_event.payload,
        serde_json::json!([1, 2, 3, "test", true])
    );
    assert!(complex_event.payload["metadata"]["version"] == "1.0");

    println!("✓ Event generation with payload varieties verified");
    Ok(())
}

// =============================================================================
// Node Event Generation Tests
// =============================================================================

/// Test filesystem-style event generation
#[sinex_test]
async fn test_filesystem_event_generation(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;

    let mut generated_events = Vec::new();

    // Generate filesystem-like events
    for i in 0..10 {
        let data = TestEventData::filesystem_event(i, "fs-ingestor");
        let event = ctx
            .publish(DynamicPayload::new(
                data.source,
                data.event_type,
                data.payload,
            ))
            .await?;
        generated_events.push(event);
        tokio::time::sleep(Duration::from_millis(1)).await;
    }

    // Verify generation results
    assert_eq!(generated_events.len(), 10);

    for (i, event) in generated_events.iter().enumerate() {
        assert_eq!(event.source.as_str(), "fs-ingestor");
        assert_eq!(event.event_type.as_str(), "file.created");

        // Verify payload progression
        let payload = &event.payload;
        assert_eq!(payload["path"], format!("/test/file_{}.txt", i));
        assert_eq!(payload["size"], 1024 + i * 100);
        assert_eq!(payload["event_index"], i);
    }

    println!("✓ Filesystem event generation verified");
    Ok(())
}

/// Test command-style event generation
#[sinex_test]
async fn test_command_event_generation(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;

    let commands = [
        "ls -la",
        "grep -r pattern",
        "find . -name '*.rs'",
        "cargo nextest run",
        "git status",
    ];

    let mut generated_events = Vec::new();

    // Generate command execution events
    for i in 0..8 {
        let command = commands[i % commands.len()];
        let data = TestEventData::command_event(i, command);
        let event = ctx
            .publish(DynamicPayload::new(
                data.source,
                data.event_type,
                data.payload,
            ))
            .await?;
        generated_events.push(event);
        tokio::time::sleep(Duration::from_millis(2)).await;
    }

    // Verify generation results
    assert_eq!(generated_events.len(), 8);

    for (i, event) in generated_events.iter().enumerate() {
        assert_eq!(event.source.as_str(), "test-cmd");
        assert_eq!(event.event_type.as_str(), "command.executed");

        let payload = &event.payload;
        let expected_command = commands[i % commands.len()];
        assert_eq!(payload["command"], expected_command);
        assert_eq!(payload["exit_code"], 0);
        assert_eq!(payload["duration_ms"], 50 + (i * 10));
    }

    println!("✓ Command event generation verified");
    Ok(())
}

/// Test concurrent event generation from multiple sources
#[sinex_test]
async fn test_concurrent_event_generation(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let pool = ctx.pool().clone();

    // Generate events concurrently using channels
    let (tx, mut rx) = mpsc::channel::<Event<JsonValue>>(100);

    // Spawn filesystem events task
    let fs_tx = tx.clone();
    let fs_pool = pool.clone();
    let fs_handle = tokio::spawn(async move {
        // Use shared pool
        let repo = fs_pool.events();

        // Use the default test material ID constant directly or just a default
        // For integration tests, we can just use a random ULID if we don't care about material linkage,
        // or we can look up the default one.
        // Simpler: Just build the event without material or use a dummy ID.
        // `DynamicPayload` build() creates a valid event.

        for i in 0..5 {
            let data = TestEventData::filesystem_event(i, "concurrent-fs");
            let event_res = DynamicPayload::new(data.source, data.event_type, data.payload).build();

            if let Ok(event) = event_res {
                if let Ok(_) = repo.insert(event.clone()).await {
                    let _ = fs_tx.send(event).await;
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    });

    // Spawn command events task
    let cmd_tx = tx;
    let cmd_pool = pool.clone();
    let cmd_handle = tokio::spawn(async move {
        // Use shared pool
        let repo = cmd_pool.events();
        let commands = ["ls", "pwd", "date", "whoami", "uname"];
        for i in 0..5 {
            let data = TestEventData::command_event(i, commands[i]);
            let event_res = DynamicPayload::new(data.source, data.event_type, data.payload).build();

            if let Ok(event) = event_res {
                if let Ok(_) = repo.insert(event.clone()).await {
                    let _ = cmd_tx.send(event).await;
                } else {
                    eprintln!("Failed to insert command event {i}");
                }
            } else {
                eprintln!("Failed to build command event {i}");
            }
            tokio::time::sleep(Duration::from_millis(15)).await;
        }
    });

    // Create events from main context in parallel
    for i in 0..5 {
        let data = TestEventData::filesystem_event(i + 100, "main-source");
        ctx.publish(DynamicPayload::new(
            data.source,
            data.event_type,
            data.payload,
        ))
        .await?;
    }

    // Collect events from spawned tasks
    let mut all_events = Vec::new();
    let timeout = tokio::time::timeout(Duration::from_secs(30), async {
        while let Some(event) = rx.recv().await {
            all_events.push(event);
            if all_events.len() >= 10 {
                break;
            }
        }
    });

    let _ = timeout.await;
    let _ = tokio::join!(fs_handle, cmd_handle);

    // Verify concurrent generation results
    assert!(
        all_events.len() >= 5,
        "Expected at least 5 events from spawned tasks, got {}",
        all_events.len()
    );

    // Count events by source
    let fs_count = all_events
        .iter()
        .filter(|e| e.source.as_str() == "concurrent-fs")
        .count();
    let cmd_count = all_events
        .iter()
        .filter(|e| e.source.as_str() == "test-cmd")
        .count();

    assert!(fs_count >= 2, "Expected at least 2 filesystem events");
    assert!(cmd_count >= 2, "Expected at least 2 command events");

    println!("✓ Concurrent event generation verified");
    Ok(())
}

// =============================================================================
// Event Generation Performance Tests
// =============================================================================

/// Test event generation performance under load
#[sinex_test]
async fn test_event_generation_performance(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;

    let event_count = 100; // Reduced for faster test execution
    let start_time = std::time::Instant::now();

    let mut all_events = Vec::new();

    for i in 0..event_count {
        let event = ctx
            .publish(DynamicPayload::new(
                "perf-test",
                "performance.test",
                serde_json::json!({
                    "event_index": i,
                    "payload_size": "medium",
                    "data": format!("performance test data for event {}", i),
                    "metadata": {
                        "generated_at": OffsetDateTime::now_utc().format(&time::format_description::well_known::Rfc3339).unwrap(),
                        "total_events": event_count
                    }
                }),
            ))
            .await?;
        all_events.push(event);
    }

    let generation_time = start_time.elapsed();
    let generation_rate = event_count as f64 / generation_time.as_secs_f64();

    println!("Event generation performance:");
    println!("- Generated {} events", event_count);
    println!("- Generation time: {:?}", generation_time);
    println!("- Generation rate: {:.2} events/second", generation_rate);

    // Verify all events were generated correctly
    assert_eq!(all_events.len(), event_count);

    // Sample check of generated events
    for i in (0..event_count).step_by(10) {
        let event = &all_events[i];
        assert_eq!(event.source.as_str(), "perf-test");
        assert_eq!(event.event_type.as_str(), "performance.test");
        let payload = &event.payload;
        assert_eq!(payload["event_index"], i);
    }

    println!("✓ Event generation performance verified");
    Ok(())
}

/// Test event generation with varying payload sizes
#[sinex_test]
async fn test_event_generation_payload_sizes(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;

    // Generate small payload event
    let small_event = ctx
        .publish(DynamicPayload::new(
            "size-test",
            "payload.small",
            serde_json::json!({
                "size": "small",
                "data": "x"
            }),
        ))
        .await?;

    // Generate medium payload event
    let medium_data = "x".repeat(1000);
    let medium_event = ctx
        .publish(DynamicPayload::new(
            "size-test",
            "payload.medium",
            serde_json::json!({
                "size": "medium",
                "data": medium_data
            }),
        ))
        .await?;

    // Generate large payload event
    let large_data = "x".repeat(10000);
    let large_event = ctx
        .publish(DynamicPayload::new(
            "size-test",
            "payload.large",
            serde_json::json!({
                "size": "large",
                "data": large_data,
                "metadata": {
                    "chunks": (0..100).collect::<Vec<i32>>(),
                    "description": "Large payload test with substantial data"
                }
            }),
        ))
        .await?;

    // Verify events were created with correct sizes
    assert_eq!(small_event.payload["size"], "small");
    assert_eq!(medium_event.payload["size"], "medium");
    assert_eq!(large_event.payload["size"], "large");

    println!("✓ Event generation with varying payload sizes verified");
    Ok(())
}

// =============================================================================
// Event Generation Edge Cases
// =============================================================================

/// Test event generation with edge case inputs
#[sinex_test]
async fn test_event_generation_edge_cases(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;

    // Test with special characters in event type
    let special_event = ctx
        .publish(DynamicPayload::new(
            "edge-test",
            "test.special_chars",
            serde_json::json!({"test": "special characters"}),
        ))
        .await?;
    assert_eq!(special_event.event_type.as_str(), "test.special_chars");

    // Test with unicode in payload
    let unicode_event = ctx
        .publish(DynamicPayload::new(
            "edge-test",
            "test.unicode",
            serde_json::json!({
                "greeting": "Hello, 世界! 🌍",
                "description": "Unicode test with émojis and accénts"
            }),
        ))
        .await?;
    assert!(unicode_event.payload["greeting"]
        .as_str()
        .unwrap()
        .contains("世界"));

    // Test with null values in payload
    let null_event = ctx
        .publish(DynamicPayload::new(
            "edge-test",
            "test.nulls",
            serde_json::json!({
                "present": "value",
                "absent": null,
                "nested": {"also_null": null}
            }),
        ))
        .await?;
    assert!(null_event.payload["absent"].is_null());

    println!("✓ Event generation edge cases verified");
    Ok(())
}

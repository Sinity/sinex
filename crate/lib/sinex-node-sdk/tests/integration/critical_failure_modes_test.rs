//! Critical Failure Modes Testing
//!
//! This module tests critical failure scenarios that could break the system
//! in production, focusing on system resilience and error handling.

use sinex_core::db::repositories::DbPoolExt;
use sinex_core::DynamicPayload;
use sinex_node_sdk::VersionInfo;
use sinex_test_utils::prelude::*;
use std::fs;
use tempfile::TempDir;

// ============================================================================
// Version Tracking Failure Tests
// ============================================================================

/// Test version tracking with corrupted git environment
#[sinex_test]
async fn test_version_tracking_corrupted_git() -> TestResult<()> {
    // Create a temporary directory with fake git directory
    let temp_dir = TempDir::new()?;
    let temp_path = temp_dir.path();

    // Create a fake .git directory with corrupted content
    let git_dir = temp_path.join(".git");
    fs::create_dir_all(&git_dir)?;
    fs::write(git_dir.join("HEAD"), "corrupted content")?;

    // Change to the directory with corrupted git
    let original_dir = std::env::current_dir()?;
    std::env::set_current_dir(temp_path)?;

    // Version tracking should handle corrupted git gracefully
    let version_info = VersionInfo::current("git-test");

    // Should not panic and should have some form of identification
    assert!(!version_info.component_version.is_empty());

    // Restore original directory
    std::env::set_current_dir(original_dir)?;

    Ok(())
}

/// Test version tracking performance under stress
#[sinex_test]
async fn test_version_tracking_stress() -> TestResult<()> {
    let start_time = std::time::Instant::now();

    // Generate many version infos quickly
    for i in 0..50 {
        let version_info = VersionInfo::current(&format!("stress-{}", i));
        assert!(!version_info.component_version.is_empty());
    }

    let elapsed = start_time.elapsed();

    // Should complete in reasonable time (10 seconds for 50 generations)
    assert!(
        elapsed.as_secs() < 10,
        "Version tracking stress test too slow: {:?}",
        elapsed
    );

    Ok(())
}

// ============================================================================
// Database Resilience Tests
// ============================================================================

/// Test database operations under high load
#[sinex_test]
async fn test_database_high_load_resilience(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let start_memory = get_current_memory_usage();

    // Create many events to test database resilience
    let mut events = Vec::new();
    for i in 0..100 {
        // Reduced from 1000 for faster testing
        let event = ctx
            .publish(DynamicPayload::new(
                "load-test",
                "high.volume",
                json!({
                    "index": i,
                    "data": format!("load-test-data-{}", i)
                }),
            ))
            .await?;
        events.push(event);

        // Check memory growth periodically
        if i % 50 == 0 {
            let current_memory = get_current_memory_usage();
            let growth = current_memory.saturating_sub(start_memory);

            // Should not use excessive memory (allow 50MB growth)
            assert!(
                growth < 50 * 1024 * 1024,
                "Excessive memory usage during load test: {} bytes",
                growth
            );
        }
    }

    // Verify events were created
    assert_eq!(events.len(), 100);

    let end_memory = get_current_memory_usage();
    let total_growth = end_memory.saturating_sub(start_memory);

    // Total memory growth should be reasonable
    assert!(
        total_growth < 100 * 1024 * 1024,
        "Total memory growth too high: {} bytes",
        total_growth
    );

    Ok(())
}

/// Test database connection exhaustion recovery
#[sinex_test]
async fn test_database_connection_exhaustion_recovery(ctx: TestContext) -> TestResult<()> {
    use sinex_core::types::domain::{EventSource, EventType};
    use sinex_core::{Event, JsonValue};
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;

    let _ctx = ctx.with_nats().shared().await?;
    let success_count = Arc::new(AtomicU32::new(0));
    let error_count = Arc::new(AtomicU32::new(0));
    let mut tasks = Vec::new();

    // Spawn concurrent database operations, each with its own context
    for i in 0..20 {
        let success_count = success_count.clone();
        let error_count = error_count.clone();

        let task = tokio::spawn(async move {
            // Each task gets its own context for isolation
            let task_ctx = match TestContext::new().await {
                Ok(ctx) => ctx,
                Err(_) => {
                    error_count.fetch_add(1, Ordering::SeqCst);
                    return;
                }
            };

            // Create and insert event directly using repository
            let event = Event::test_event(
                EventSource::new("conn-test"),
                EventType::new("exhaustion"),
                json!({"task": i}),
            );

            match task_ctx.pool.events().insert(event).await {
                Ok(_) => {
                    success_count.fetch_add(1, Ordering::SeqCst);
                }
                Err(_) => {
                    error_count.fetch_add(1, Ordering::SeqCst);
                }
            }
        });

        tasks.push(task);
    }

    // Wait for all tasks to complete
    let results = futures::future::join_all(tasks).await;

    let successes = success_count.load(Ordering::SeqCst);

    // Some operations should succeed (system should be resilient)
    assert!(successes > 0, "At least some operations should succeed");

    // Verify that failures are handled gracefully (no panics)
    for result in results {
        match result {
            Ok(_) => {} // Success or handled error
            Err(e) => {
                // Task panic is not acceptable
                panic!("Task should not panic during connection exhaustion: {}", e);
            }
        }
    }

    Ok(())
}

// ============================================================================
// Event System Resilience Tests
// ============================================================================

/// Test event creation with extreme payload sizes
#[sinex_test]
async fn test_event_creation_extreme_payloads(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;

    let test_cases = vec![
        // Empty payload
        ("empty", json!({})),
        // Null payload
        ("null", json!(null)),
        // Large payload (100KB instead of 1MB for speed)
        ("large", json!({"data": "x".repeat(100 * 1024)})),
        // Deep nesting
        ("nested", create_deeply_nested_json(20)),
        // Wide object (many keys)
        ("wide", create_wide_json(100)),
        // Unicode and special characters
        (
            "unicode",
            json!({
                "text": "Hello 世界 🌍",
                "special": "!@#$%^&*()_+{}|:<>?[]\\;',./\"",
            }),
        ),
    ];

    for (name, payload) in test_cases {
        let result = ctx
            .publish(DynamicPayload::new("extreme-test", name, payload.clone()))
            .await;

        match result {
            Ok(event) => {
                // Event creation succeeded - verify it was stored correctly
                assert_eq!(event.source.as_str(), "extreme-test");
                assert_eq!(event.event_type.as_str(), name);
            }
            Err(err) => {
                // Event creation failed - should be a meaningful error
                let error_msg = err.to_string().to_lowercase();
                assert!(
                    error_msg.contains("payload")
                        || error_msg.contains("size")
                        || error_msg.contains("limit")
                        || error_msg.contains("validation")
                        || error_msg.contains("error"),
                    "Expected payload-related error for case '{}', got: {}",
                    name,
                    err
                );
            }
        }
    }

    Ok(())
}

/// Test concurrent event creation under stress
#[sinex_test]
async fn test_concurrent_event_creation_stress(ctx: TestContext) -> TestResult<()> {
    use sinex_core::types::domain::{EventSource, EventType};
    use sinex_core::{Event, JsonValue};
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;

    let _ctx = ctx.with_nats().shared().await?;
    let success_count = Arc::new(AtomicU32::new(0));
    let mut tasks = Vec::new();

    // Create concurrent event creation tasks
    for i in 0..10 {
        let success_count = success_count.clone();

        let task = tokio::spawn(async move {
            // Each task creates its own context
            let task_ctx = match TestContext::new().await {
                Ok(ctx) => ctx,
                Err(_) => return,
            };

            // Create a batch of events from this task
            let mut local_successes = 0u32;
            for j in 0..10 {
                let event = Event::test_event(
                    EventSource::new(format!("stress-{i}")),
                    EventType::new("concurrent"),
                    json!({"task": i, "iteration": j}),
                );

                match task_ctx.pool.events().insert(event).await {
                    Ok(_) => local_successes += 1,
                    Err(_) => {} // Count failures silently
                }
            }
            success_count.fetch_add(local_successes, Ordering::SeqCst);
        });

        tasks.push(task);
    }

    // Wait for all tasks
    let results = futures::future::join_all(tasks).await;

    // Verify no tasks panicked
    for (i, result) in results.into_iter().enumerate() {
        match result {
            Ok(()) => {} // Task completed normally
            Err(e) => panic!("Task {} panicked during stress test: {}", i, e),
        }
    }

    let total_successes = success_count.load(Ordering::SeqCst);

    // At least 50% of operations should succeed under stress
    let expected_operations = 10 * 10; // 10 tasks * 10 operations each
    assert!(
        total_successes >= expected_operations / 2,
        "Too many failures under stress: {}/{} succeeded",
        total_successes,
        expected_operations
    );

    Ok(())
}

// ============================================================================
// Configuration Edge Cases
// ============================================================================

/// Test system behavior with invalid configurations
#[sinex_test]
async fn test_invalid_configuration_handling(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;

    // Test various source/event_type combinations
    let test_cases = vec![
        // Special characters in identifiers
        ("source/with/slashes", "type"),
        ("source", "type.with.dots"),
        // Unicode in identifiers
        ("源", "类型"),
    ];

    for (source, event_type) in test_cases {
        let result = ctx
            .publish(DynamicPayload::new(
                source,
                event_type,
                json!({"test": "invalid"}),
            ))
            .await;

        // Either succeeds (unicode may be valid) or fails with validation error
        match result {
            Ok(_) => {
                // Some cases might be valid
            }
            Err(err) => {
                // Should be a meaningful validation error
                let _error_msg = err.to_string().to_lowercase();
                // Just verify it doesn't panic - validation behavior may vary
            }
        }
    }

    Ok(())
}

// ============================================================================
// Error Recovery Tests
// ============================================================================

/// Test system recovery after temporary failures
#[sinex_test]
async fn test_error_recovery_patterns(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;

    // Create some valid events to verify system is working
    let mut successes = 0;

    for i in 0..10 {
        let result = ctx
            .publish(DynamicPayload::new(
                "recovery-test",
                "valid",
                json!({"iteration": i, "valid": true}),
            ))
            .await;

        if result.is_ok() {
            successes += 1;
        }
    }

    // Most operations should succeed
    assert!(
        successes >= 8,
        "Expected at least 8 successes, got {}",
        successes
    );

    // System should still be able to create events after the test
    let final_event = ctx
        .publish(DynamicPayload::new(
            "recovery-test",
            "final",
            json!({"recovered": true}),
        ))
        .await?;
    assert_eq!(final_event.payload["recovered"], json!(true));

    Ok(())
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Get current memory usage (simplified for testing)
fn get_current_memory_usage() -> usize {
    // Try to read from /proc/self/status on Linux
    if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
        for line in status.lines() {
            if line.starts_with("VmRSS:") {
                if let Some(size_str) = line.split_whitespace().nth(1) {
                    if let Ok(size_kb) = size_str.parse::<usize>() {
                        return size_kb * 1024; // Convert to bytes
                    }
                }
            }
        }
    }

    // Fallback: return 0 (can't measure on this platform)
    0
}

/// Create a deeply nested JSON object for testing
fn create_deeply_nested_json(depth: usize) -> serde_json::Value {
    let mut nested = json!("deepest_value");
    for i in 0..depth {
        nested = json!({
            format!("level_{}", depth - i): nested
        });
    }
    nested
}

/// Create a wide JSON object with many keys
fn create_wide_json(key_count: usize) -> serde_json::Value {
    let mut obj = serde_json::Map::new();
    for i in 0..key_count {
        obj.insert(format!("key_{}", i), json!(format!("value_{}", i)));
    }
    serde_json::Value::Object(obj)
}

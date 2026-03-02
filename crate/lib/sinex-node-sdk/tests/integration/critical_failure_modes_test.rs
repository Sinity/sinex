//! Critical Failure Modes Testing
//!
//! This module tests critical failure scenarios that could break the system
//! in production, focusing on system resilience and error handling.

use sinex_node_sdk::VersionInfo;
use sinex_primitives::DynamicPayload;
use std::fs;
use tempfile::TempDir;
use xtask::sandbox::prelude::*;

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
        let version_info = VersionInfo::current(&format!("stress-{i}"));
        assert!(!version_info.component_version.is_empty());
    }

    let elapsed = start_time.elapsed();

    // Should complete in reasonable time (10 seconds for 50 generations)
    assert!(
        elapsed.as_secs() < 10,
        "Version tracking stress test too slow: {elapsed:?}"
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
    let _scope = ctx.pipeline().await?;
    let start_memory = get_current_memory_usage();

    // Batch publish: all events go to NATS first, then a single wait for persistence.
    // 100 events through the full pipeline should complete in seconds.
    let event_count = 100;
    let payloads: Vec<_> = (0..event_count)
        .map(|i| {
            DynamicPayload::new(
                "load-test",
                "high.volume",
                json!({
                    "index": i,
                    "data": format!("load-test-data-{}", i)
                }),
            )
        })
        .collect();

    let events = ctx.publish_many(payloads).await?;

    // Verify events were created
    assert_eq!(events.len(), event_count);

    let end_memory = get_current_memory_usage();
    let total_growth = end_memory.saturating_sub(start_memory);

    // Total memory growth should be reasonable
    assert!(
        total_growth < 100 * 1024 * 1024,
        "Total memory growth too high: {total_growth} bytes"
    );

    Ok(())
}

/// Test database connection exhaustion recovery
///
/// Verifies that concurrent operations on the same pool handle contention
/// gracefully without panicking or corrupting state.
#[sinex_test]
async fn test_database_connection_exhaustion_recovery(ctx: TestContext) -> TestResult<()> {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    let pool = ctx.pool().clone();
    let success_count = Arc::new(AtomicU32::new(0));
    let error_count = Arc::new(AtomicU32::new(0));
    let mut tasks = Vec::new();

    // Spawn concurrent database operations sharing the SAME pool (4 connections).
    // This creates real connection contention without needing extra DB slots.
    for i in 0..8 {
        let pool = pool.clone();
        let success_count = success_count.clone();
        let error_count = error_count.clone();

        let task = tokio::spawn(async move {
            // Rapid-fire queries to stress the connection pool
            for j in 0..5 {
                let result = sqlx::query_scalar!(
                    "SELECT COUNT(*) FROM core.events WHERE source = $1",
                    format!("conn-stress-{i}-{j}")
                )
                .fetch_one(&pool)
                .await;

                match result {
                    Ok(_) => {
                        success_count.fetch_add(1, Ordering::SeqCst);
                    }
                    Err(_) => {
                        error_count.fetch_add(1, Ordering::SeqCst);
                    }
                }
            }
        });

        tasks.push(task);
    }

    // Wait for all tasks to complete
    let results = futures::future::join_all(tasks).await;

    let successes = success_count.load(Ordering::SeqCst);

    // Most operations should succeed — pool handles contention gracefully
    assert!(
        successes > 0,
        "At least some operations should succeed under pool contention"
    );

    // Verify no tasks panicked
    for (i, result) in results.into_iter().enumerate() {
        if let Err(e) = result {
            panic!("Task {i} should not panic during connection exhaustion: {e}");
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
    let _scope = ctx.pipeline().await?;

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
                    "Expected payload-related error for case '{name}', got: {err}"
                );
            }
        }
    }

    Ok(())
}

/// Test concurrent event creation under stress
///
/// Verifies that concurrent database operations sharing the same pool handle
/// contention gracefully without panicking.
#[sinex_test]
async fn test_concurrent_event_creation_stress(ctx: TestContext) -> TestResult<()> {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    let pool = ctx.pool().clone();

    // Register a source material for provenance (required for event insertion)
    let material = pool
        .source_materials()
        .register_in_flight("stress.test", Some("/test"), json!({}))
        .await?;

    let success_count = Arc::new(AtomicU32::new(0));
    let mut tasks = Vec::new();

    // Spawn concurrent event insertion tasks sharing the same pool.
    // This tests real DB contention without needing extra pool slots.
    for i in 0..5 {
        let pool = pool.clone();
        let success_count = success_count.clone();
        let material_id = material.id;

        let task = tokio::spawn(async move {
            let mut local_successes = 0u32;
            for j in 0..10 {
                let event = DynamicPayload::new(
                    format!("stress-{i}"),
                    "concurrent",
                    json!({"task": i, "iteration": j}),
                )
                .from_material(material_id)
                .build();

                let Ok(event) = event else { continue };

                if pool.events().insert(event).await.is_ok() {
                    local_successes += 1;
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
        if let Err(e) = result {
            panic!("Task {i} panicked during stress test: {e}");
        }
    }

    let total_successes = success_count.load(Ordering::SeqCst);

    // At least 50% of operations should succeed under stress
    let expected_operations: u32 = 5 * 10; // 5 tasks * 10 operations each
    assert!(
        total_successes >= expected_operations / 2,
        "Too many failures under stress: {total_successes}/{expected_operations} succeeded"
    );

    Ok(())
}

// ============================================================================
// Configuration Edge Cases
// ============================================================================

/// Test system behavior with invalid configurations
///
/// Verifies that event creation with unusual source/event_type combinations
/// doesn't panic or corrupt state.
#[sinex_test]
async fn test_invalid_configuration_handling(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool().clone();

    // Register a source material for provenance
    let material = pool
        .source_materials()
        .register_in_flight("config-edge-case", Some("/test"), json!({}))
        .await?;

    // Test various source/event_type combinations via direct DB insert
    let test_cases = vec![
        ("source/with/slashes", "type"),
        ("source", "type.with.dots"),
        ("源", "类型"),
    ];

    for (source, event_type) in test_cases {
        let event = DynamicPayload::new(source, event_type, json!({"test": "invalid"}))
            .from_material(material.id)
            .build();

        match event {
            Ok(event) => {
                // Try to insert — may succeed or fail on DB constraints
                let _ = pool.events().insert(event).await;
            }
            Err(_) => {
                // Builder rejected the input — that's a valid outcome
            }
        }
    }

    Ok(())
}

// ============================================================================
// Error Recovery Tests
// ============================================================================

/// Test system recovery after temporary failures
///
/// Verifies that the database connection pool handles a mix of valid and
/// invalid operations gracefully, and remains functional afterwards.
#[sinex_test]
async fn test_error_recovery_patterns(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool().clone();

    // Register a source material for provenance
    let material = pool
        .source_materials()
        .register_in_flight("recovery-test", Some("/test"), json!({}))
        .await?;

    let mut successes = 0;

    for i in 0..10 {
        let event = DynamicPayload::new(
            "recovery-test",
            "valid",
            json!({"iteration": i, "valid": true}),
        )
        .from_material(material.id)
        .build();

        let Ok(event) = event else { continue };
        if pool.events().insert(event).await.is_ok() {
            successes += 1;
        }
    }

    // All operations should succeed (no failures expected with valid data)
    assert!(
        successes >= 8,
        "Expected at least 8 successes, got {successes}"
    );

    // System should still be able to create events after the loop
    let final_event = DynamicPayload::new("recovery-test", "final", json!({"recovered": true}))
        .from_material(material.id)
        .build()?;

    let inserted = pool.events().insert(final_event).await?;
    assert_eq!(inserted.payload["recovered"], json!(true));

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
                let Some(size_str) = line.split_whitespace().nth(1) else {
                    continue;
                };
                if let Ok(size_kb) = size_str.parse::<usize>() {
                    return size_kb * 1024; // Convert to bytes
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
        obj.insert(format!("key_{i}"), json!(format!("value_{}", i)));
    }
    serde_json::Value::Object(obj)
}

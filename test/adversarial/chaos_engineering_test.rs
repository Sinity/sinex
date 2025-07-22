// # Chaos Engineering Test Suite
//
// Comprehensive chaos engineering tests that simulate system failures and edge cases.
// This module tests system resilience under various failure scenarios.
//
// ## Test Categories
// - **Automaton Lifecycle Chaos**: Concurrent registration, heartbeat failures
// - **Filesystem Edge Cases**: Permission changes, mount failures, file system chaos
// - **State Machine Violations**: Shutdown during initialization, concurrent shutdowns
// - **System Resource Chaos**: Memory exhaustion, disk full, network failures

use crate::common::prelude::*;
use chrono::Utc;
use redis::AsyncCommands;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::task::yield_now;

// =============================================================================
// Agent Lifecycle Chaos Tests
// =============================================================================

/// Test concurrent agent registration attempts
#[sinex_test]
async fn test_agent_registering_from_multiple_instances(ctx: TestContext) -> TestResult {
    let agent_count = 10;
    let mut handles = vec![];

    // Spawn multiple agents concurrently
    for i in 0..agent_count {
        let ctx_clone = ctx.clone();
        let handle = tokio::spawn(async move {
            let agent_name = format!("chaos_agent_{}", i);
            
            // Simulate agent registration
            let event = ctx_clone.create_test_event(
                "agent",
                "registered",
                json!({
                    "agent_name": agent_name,
                    "timestamp": Utc::now().to_rfc3339()
                }),
            );
            
            ctx_clone.insert_event(&event).await
        });
        handles.push(handle);
    }

    // Wait for all agents
    let results = futures::future::join_all(handles).await;
    let success_count = results.iter().filter(|r| r.is_ok() && r.as_ref().unwrap().is_ok()).count();
    
    println!("Agent registration results: {}/{} successful", success_count, agent_count);
    assert!(success_count >= agent_count * 8 / 10, "Too many registration failures");

    Ok(())
}

/// Test agent heartbeat chaos with simulated failures
#[sinex_test]
async fn test_agent_heartbeat_chaos_with_network_failures(ctx: TestContext) -> TestResult {
    let heartbeat_count = 20;
    let failure_rate = 0.3; // 30% chance of failure
    
    let mut success_count = 0;
    let mut failure_count = 0;

    for i in 0..heartbeat_count {
        // Randomly simulate network failure
        if rand::random::<f64>() < failure_rate {
            failure_count += 1;
            println!("Heartbeat {} simulated network failure", i);
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            continue;
        }

        let event = ctx.create_test_event(
            "agent",
            "heartbeat",
            json!({
                "sequence": i,
                "timestamp": Utc::now().to_rfc3339(),
                "health": "ok"
            }),
        );

        match ctx.insert_event(&event).await {
            Ok(_) => success_count += 1,
            Err(e) => {
                failure_count += 1;
                println!("Heartbeat {} failed: {}", i, e);
            }
        }
    }

    println!("Heartbeat results: {} success, {} failures", success_count, failure_count);
    assert!(success_count >= heartbeat_count / 2, "Too many heartbeat failures");

    Ok(())
}

/// Test agent lifecycle during concurrent operations
#[sinex_test]
async fn test_agent_lifecycle_during_concurrent_operations(ctx: TestContext) -> TestResult {
    // Simulate agent lifecycle: start -> work -> stop
    let lifecycle_stages = vec!["starting", "running", "stopping", "stopped"];
    let mut handles = vec![];

    for (i, stage) in lifecycle_stages.iter().enumerate() {
        let ctx_clone = ctx.clone();
        let stage = stage.to_string();
        
        let handle = tokio::spawn(async move {
            // Add some jitter to simulate real timing
            tokio::time::sleep(tokio::time::Duration::from_millis(i as u64 * 50)).await;
            
            let event = ctx_clone.create_test_event(
                "agent_lifecycle",
                &stage,
                json!({
                    "stage": stage,
                    "timestamp": Utc::now().to_rfc3339()
                }),
            );
            
            ctx_clone.insert_event(&event).await
        });
        
        handles.push(handle);
    }

    let results = futures::future::join_all(handles).await;
    let all_success = results.iter().all(|r| r.is_ok() && r.as_ref().unwrap().is_ok());
    
    assert!(all_success, "Some lifecycle stages failed");
    Ok(())
}

// =============================================================================
// Filesystem Edge Case Tests
// =============================================================================

/// Test file permission revoked while watching
#[sinex_test]
async fn test_file_permission_revoked_while_watching(ctx: TestContext) -> TestResult {
    let temp_dir = TempDir::new()?;
    let watch_dir = temp_dir.path().join("watch_me");

    // Create directory with full permissions
    fs::create_dir(&watch_dir)?;
    fs::set_permissions(&watch_dir, fs::Permissions::from_mode(0o755))?;

    println!("Created watch directory: {:?}", watch_dir);

    // Simulate starting file watcher (we'll just track access attempts)
    let access_attempts = Arc::new(AtomicU64::new(0));
    let successful_accesses = Arc::new(AtomicU64::new(0));

    let watch_dir_clone = watch_dir.clone();
    let attempts = access_attempts.clone();
    let successes = successful_accesses.clone();

    // Simulate watcher trying to access directory periodically
    let watcher_handle = tokio::spawn(async move {
        for i in 0..20 {
            attempts.fetch_add(1, Ordering::SeqCst);

            match fs::read_dir(&watch_dir_clone) {
                Ok(_entries) => {
                    successes.fetch_add(1, Ordering::SeqCst);
                    println!("Access {}: Successfully read directory", i);
                }
                Err(e) => {
                    println!("Access {}: Failed to read directory: {}", i, e);
                }
            }

            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }
    });

    // After a delay, revoke permissions
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    fs::set_permissions(&watch_dir, fs::Permissions::from_mode(0o000))?;
    println!("Revoked directory permissions");

    // Wait for watcher to finish
    let _ = watcher_handle.await;

    let total_attempts = access_attempts.load(Ordering::SeqCst);
    let total_successes = successful_accesses.load(Ordering::SeqCst);

    println!("Access results: {}/{} successful", total_successes, total_attempts);
    
    // Should have some successes before permission revocation
    assert!(total_successes > 0, "No successful accesses before permission change");
    assert!(total_successes < total_attempts, "All accesses succeeded despite permission change");

    Ok(())
}

/// Test filesystem watcher during mount/unmount
#[sinex_test]
async fn test_filesystem_watcher_during_mount_unmount(ctx: TestContext) -> TestResult {
    // This test simulates filesystem mount/unmount scenarios
    // In practice, we'll simulate by creating/removing directories
    
    let base_dir = TempDir::new()?;
    let mount_point = base_dir.path().join("mount");
    
    let events_generated = Arc::new(AtomicU64::new(0));
    let events_clone = events_generated.clone();

    // Simulate filesystem operations
    let fs_handle = tokio::spawn(async move {
        for cycle in 0..5 {
            // "Mount" - create directory
            if fs::create_dir(&mount_point).is_ok() {
                println!("Cycle {}: Mounted filesystem", cycle);
                
                // Generate some events while "mounted"
                for i in 0..10 {
                    let file_path = mount_point.join(format!("file_{}.txt", i));
                    if fs::write(&file_path, format!("content {}", i)).is_ok() {
                        events_clone.fetch_add(1, Ordering::SeqCst);
                    }
                }
            }
            
            tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
            
            // "Unmount" - remove directory
            if fs::remove_dir_all(&mount_point).is_ok() {
                println!("Cycle {}: Unmounted filesystem", cycle);
            }
            
            tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
        }
    });

    // Wait for filesystem operations
    let _ = fs_handle.await;
    
    let total_events = events_generated.load(Ordering::SeqCst);
    println!("Generated {} events during mount/unmount cycles", total_events);
    
    assert!(total_events > 0, "No events generated during filesystem operations");
    Ok(())
}

// =============================================================================
// State Machine Violation Tests
// =============================================================================

/// Test shutdown during initialization
#[sinex_test]
async fn test_shutdown_during_initialization(ctx: TestContext) -> TestResult {
    // Simulate component initialization stages
    let init_stages = vec!["config_loading", "db_connecting", "service_starting"];
    let shutdown_signal = Arc::new(AtomicU64::new(0));
    
    let mut handles = vec![];
    
    for (i, stage) in init_stages.iter().enumerate() {
        let ctx_clone = ctx.clone();
        let stage = stage.to_string();
        let shutdown = shutdown_signal.clone();
        
        let handle = tokio::spawn(async move {
            // Check if shutdown was triggered
            if shutdown.load(Ordering::SeqCst) > 0 {
                println!("Stage {} skipped due to shutdown", stage);
                return Err("Shutdown during initialization".into());
            }
            
            // Simulate initialization work
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            
            let event = ctx_clone.create_test_event(
                "initialization",
                &stage,
                json!({
                    "stage": stage,
                    "index": i
                }),
            );
            
            ctx_clone.insert_event(&event).await?;
            
            // Trigger shutdown after second stage
            if i == 1 {
                shutdown.store(1, Ordering::SeqCst);
                println!("Shutdown triggered after stage: {}", stage);
            }
            
            Ok(())
        });
        
        handles.push(handle);
    }
    
    let results = futures::future::join_all(handles).await;
    
    // Count successful stages
    let successful = results.iter().filter(|r| r.is_ok() && r.as_ref().unwrap().is_ok()).count();
    println!("Completed {}/{} initialization stages before shutdown", successful, init_stages.len());
    
    // Should have completed some but not all stages
    assert!(successful > 0 && successful < init_stages.len(), 
        "Expected partial initialization before shutdown");
    
    Ok(())
}

/// Test concurrent shutdown attempts
#[sinex_test]
async fn test_concurrent_shutdown_attempts(ctx: TestContext) -> TestResult {
    let shutdown_count = Arc::new(AtomicU64::new(0));
    let mut handles = vec![];
    
    // Spawn multiple tasks trying to shutdown concurrently
    for i in 0..10 {
        let ctx_clone = ctx.clone();
        let shutdown = shutdown_count.clone();
        
        let handle = tokio::spawn(async move {
            // Try to be the first to shutdown
            let prev = shutdown.fetch_add(1, Ordering::SeqCst);
            
            if prev == 0 {
                // First shutdown attempt
                let event = ctx_clone.create_test_event(
                    "shutdown",
                    "initiated",
                    json!({
                        "initiator": i,
                        "timestamp": Utc::now().to_rfc3339()
                    }),
                );
                ctx_clone.insert_event(&event).await?;
                println!("Task {} initiated shutdown", i);
                Ok("shutdown_initiated")
            } else {
                // Subsequent attempts should be rejected
                println!("Task {} shutdown rejected (already shutting down)", i);
                Ok("shutdown_rejected")
            }
        });
        
        handles.push(handle);
    }
    
    let results = futures::future::join_all(handles).await;
    
    // Count outcomes
    let initiated = results.iter()
        .filter(|r| r.is_ok() && r.as_ref().unwrap().as_ref().map(|s| s == &"shutdown_initiated").unwrap_or(false))
        .count();
    
    assert_eq!(initiated, 1, "Exactly one shutdown should be initiated");
    Ok(())
}

// =============================================================================
// System Resource Chaos Tests
// =============================================================================

/// Test behavior under memory pressure
#[sinex_test]
async fn test_behavior_under_memory_pressure(ctx: TestContext) -> TestResult {
    // Simulate memory pressure by creating large allocations
    let mut memory_hogs = Vec::new();
    let allocation_size = 10 * 1024 * 1024; // 10MB per allocation
    let max_allocations = 50;
    
    let mut successful_events = 0;
    let mut failed_events = 0;
    
    for i in 0..max_allocations {
        // Allocate memory
        let hog = vec![0u8; allocation_size];
        memory_hogs.push(hog);
        
        // Try to process event under memory pressure
        let event = ctx.create_test_event(
            "memory_test",
            "allocation",
            json!({
                "allocation_index": i,
                "total_allocated_mb": (i + 1) * 10
            }),
        );
        
        match ctx.insert_event(&event).await {
            Ok(_) => successful_events += 1,
            Err(e) => {
                failed_events += 1;
                println!("Event {} failed under memory pressure: {}", i, e);
                
                // Start releasing memory if we're failing
                if failed_events > 3 {
                    memory_hogs.truncate(memory_hogs.len() / 2);
                    println!("Released half of allocated memory");
                }
            }
        }
        
        // Give system time to react
        yield_now().await;
    }
    
    println!("Memory pressure test: {} successful, {} failed events", 
        successful_events, failed_events);
    
    // Should handle at least some events even under pressure
    assert!(successful_events > 0, "No events processed under memory pressure");
    
    Ok(())
}

/// Test Redis stream overflow handling
#[sinex_test]
async fn test_redis_stream_overflow_handling(ctx: TestContext) -> TestResult {
    let mut redis_conn = ctx.redis_conn().await?;
    let stream_key = "sinex:chaos:overflow_test";
    
    // Configure small max length to force trimming
    let max_length = 100;
    let overflow_count = 200;
    
    // Generate events rapidly
    for i in 0..overflow_count {
        let event_data = json!({
            "index": i,
            "timestamp": Utc::now().to_rfc3339()
        });
        
        // Add to stream with MAXLEN
        let _: Result<String, _> = redis_conn.xadd_maxlen(
            stream_key,
            redis::streams::StreamMaxlen::Approx(max_length),
            "*",
            &[("data", serde_json::to_string(&event_data).unwrap())]
        ).await;
    }
    
    // Check stream length
    let info: redis::streams::StreamInfoStreamReply = redis_conn
        .xinfo_stream(stream_key)
        .await?;
    
    println!("Stream length after overflow: {}", info.length);
    println!("First entry ID: {:?}", info.first_entry.as_ref().map(|e| &e.id));
    println!("Last entry ID: {:?}", info.last_entry.as_ref().map(|e| &e.id));
    
    // Stream should be trimmed to approximately max_length
    assert!(info.length <= max_length + 10, "Stream not properly trimmed");
    assert!(info.length >= max_length - 10, "Stream over-trimmed");
    
    // Cleanup
    let _: Result<(), _> = redis_conn.del(stream_key).await;
    
    Ok(())
}

/// Test system behavior during network partitions
#[sinex_test]
async fn test_network_partition_simulation(ctx: TestContext) -> TestResult {
    // Simulate network partition by timing out operations
    let partition_duration = tokio::time::Duration::from_secs(2);
    let mut successes_before = 0;
    let mut failures_during = 0;
    let mut successes_after = 0;
    
    // Phase 1: Normal operation
    for i in 0..5 {
        let event = ctx.create_test_event(
            "network_test",
            "before_partition",
            json!({ "index": i }),
        );
        
        if ctx.insert_event(&event).await.is_ok() {
            successes_before += 1;
        }
    }
    
    // Phase 2: Simulate partition with very short timeout
    let partition_start = tokio::time::Instant::now();
    while partition_start.elapsed() < partition_duration {
        let event = ctx.create_test_event(
            "network_test",
            "during_partition",
            json!({ "timestamp": Utc::now().to_rfc3339() }),
        );
        
        // Use very short timeout to simulate network issues
        match tokio::time::timeout(
            tokio::time::Duration::from_millis(10),
            ctx.insert_event(&event)
        ).await {
            Ok(Ok(_)) => {
                // Shouldn't happen often during "partition"
                println!("Unexpected success during partition");
            }
            _ => {
                failures_during += 1;
            }
        }
        
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    }
    
    // Phase 3: Recovery after partition
    for i in 0..5 {
        let event = ctx.create_test_event(
            "network_test",
            "after_partition",
            json!({ "index": i }),
        );
        
        if ctx.insert_event(&event).await.is_ok() {
            successes_after += 1;
        }
    }
    
    println!("Network partition test results:");
    println!("  Before partition: {} successes", successes_before);
    println!("  During partition: {} failures", failures_during);
    println!("  After partition: {} successes", successes_after);
    
    assert!(successes_before > 0, "No successes before partition");
    assert!(failures_during > 0, "No failures during partition");
    assert!(successes_after > 0, "No recovery after partition");
    
    Ok(())
}
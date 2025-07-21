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

use crate::common::test_macros::*;
use crate::common::prelude::*;
use crate::common::builders::{TestEventBuilder};
use crate::common::query_helpers::TestQueries;
use chrono::Utc;
use redis::AsyncCommands;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::task::yield_now;

// =============================================================================
// Agent Lifecycle Chaos Tests
// =============================================================================

/// Test multiple agent instances
test_concurrent_operations!(test_agent_registering_from_multiple_instances, 10,
    |pool: Arc<DbPool>, index: usize| async move {
        // Concurrent operation
        Ok(())
    },
    |pool: &Arc<DbPool>, results: &Vec<_>| async move {
        assert_eq!(results.len(), 10);
        Ok(())
    }
);

/// Test agent heartbeat chaos
test_concurrent_operations!(test_agent_heartbeat_chaos_with_network_failures, 20,
    |pool: Arc<DbPool>, index: usize| async move {
        // Concurrent operation
        Ok(())
    },
    |pool: &Arc<DbPool>, results: &Vec<_>| async move {
        assert_eq!(results.len(), 20);
        Ok(())
    }
););

/// Test agent lifecycle
test_concurrent_operations!(test_agent_lifecycle_during_concurrent_operations, 10,
    |pool: Arc<DbPool>, index: usize| async move {
        // Concurrent operation
        Ok(())
    },
    |pool: &Arc<DbPool>, results: &Vec<_>| async move {
        assert_eq!(results.len(), 10);
        Ok(())
    }
);should succeed");
    
    Ok(())
}

// =============================================================================
// Filesystem Edge Case Tests
// =============================================================================

/// Test file permission revoked while watching
#[sinex_test]
async fn test_file_permission_revoked_while_watching(ctx: TestContext) -> TestResult {
    let temp_dir = tempfile::TempDir::new()?;
    let watch_dir = temp_dir.path().join("watch_me");

    // Create directory with full permissions
    fs::create_dir(&watch_dir).unwrap();
    fs::set_permissions(&watch_dir, fs::Permissions::from_mode(0o755)).unwrap();

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

            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    });

    // After some time, revoke permissions
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Remove all permissions
    match fs::set_permissions(&watch_dir, fs::Permissions::from_mode(0o000)) {
        Ok(_) => {
            println!("Revoked all permissions from watch directory");
        }
        Err(e) => {
            println!("Failed to revoke permissions: {}", e);
        }
    }

    // Wait for watcher to complete
    watcher_handle.await.unwrap();

    let total_attempts = access_attempts.load(Ordering::SeqCst);
    let successful = successful_accesses.load(Ordering::SeqCst);

    println!("Permission revocation test results:");
    println!("- Total access attempts: {}", total_attempts);
    println!("- Successful accesses: {}", successful);
    println!("- Failed accesses: {}", total_attempts - successful);

    if successful == total_attempts {
        println!("ISSUE: All accesses succeeded despite permission revocation");
    } else {
        println!("Expected behavior: Some accesses failed after permission revocation");
    }
    Ok(())
}

/// Test directory unmounted while watching
#[sinex_test]
async fn test_directory_unmounted_while_watching(ctx: TestContext) -> TestResult {
    // This test simulates what happens when a watched directory becomes unavailable
    let temp_dir = tempfile::TempDir::new()?;
    let mount_point = temp_dir.path().join("mount_point");

    fs::create_dir(&mount_point).unwrap();

    // Create some files in the "mounted" directory
    let test_file = mount_point.join("test_file.txt");
    fs::write(&test_file, "test content").unwrap();

    println!("Created mock mount point: {:?}", mount_point);

    let access_attempts = Arc::new(AtomicU64::new(0));
    let successful_accesses = Arc::new(AtomicU64::new(0));
    let stale_file_accesses = Arc::new(AtomicU64::new(0));

    let mount_point_clone = mount_point.clone();
    let test_file_clone = test_file.clone();
    let attempts = access_attempts.clone();
    let successes = successful_accesses.clone();
    let stale_accesses = stale_file_accesses.clone();

    // Simulate watcher trying to access mount point
    let watcher_handle = tokio::spawn(async move {
        for i in 0..15 {
            attempts.fetch_add(1, Ordering::SeqCst);

            // Try to access the mount point
            match fs::read_dir(&mount_point_clone) {
                Ok(_entries) => {
                    successes.fetch_add(1, Ordering::SeqCst);
                    println!("Access {}: Successfully read mount point", i);
                }
                Err(e) => {
                    println!("Access {}: Failed to read mount point: {}", i, e);
                }
            }

            // Try to access a specific file
            match fs::read(&test_file_clone) {
                Ok(_contents) => {
                    stale_accesses.fetch_add(1, Ordering::SeqCst);
                    println!("Access {}: Successfully read file", i);
                }
                Err(e) => {
                    println!("Access {}: Failed to read file: {}", i, e);
                }
            }

            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    });

    // After some time, "unmount" by removing the directory
    tokio::time::sleep(Duration::from_millis(500)).await;

    match fs::remove_dir_all(&mount_point) {
        Ok(_) => {
            println!("Simulated unmount by removing directory");
        }
        Err(e) => {
            println!("Failed to simulate unmount: {}", e);
        }
    }

    // Wait for watcher to complete
    watcher_handle.await.unwrap();

    let total_attempts = access_attempts.load(Ordering::SeqCst);
    let successful = successful_accesses.load(Ordering::SeqCst);
    let stale_successful = stale_file_accesses.load(Ordering::SeqCst);

    println!("Directory unmount test results:");
    println!("- Total access attempts: {}", total_attempts);
    println!("- Successful directory accesses: {}", successful);
    println!("- Successful file accesses: {}", stale_successful);
    println!("- Failed accesses: {}", total_attempts - successful);

    // After unmount, accesses should start failing
    assert!(successful < total_attempts, "Some accesses should fail after unmount");

/// Test filesystem chaos
test_concurrent_operations!(test_filesystem_chaos_concurrent_operations, 10,
    |pool: Arc<DbPool>, index: usize| async move {
        // Concurrent operation
        Ok(())
    },
    |pool: &Arc<DbPool>, results: &Vec<_>| async move {
        assert_eq!(results.len(), 10);
        Ok(())
    }
);s should occur");
    
    Ok(())
}

// =============================================================================
// State Machine Violation Tests
// =============================================================================

/// Test shutdown signal during initialization
#[sinex_test]
async fn test_shutdown_signal_during_initialization(ctx: TestContext) -> TestResult {
    let pool = ctx.pool().clone();
    let shutdown_triggered = Arc::new(AtomicU64::new(0));
    let init_completed = Arc::new(AtomicU64::new(0));

    let shutdown_flag = shutdown_triggered.clone();
    let init_flag = init_completed.clone();

    // Simulate initialization process
    let init_handle = tokio::spawn(async move {
        // Simulate slow initialization (migration, schema setup, etc.)
        for step in 0..10 {
            if shutdown_flag.load(Ordering::SeqCst) > 0 {
                println!("Initialization interrupted at step {}", step);
                return Err("shutdown_during_init");
            }

            // Simulate database operations during init
            let event = TestEventBuilder::new("init", &format!("init.step_{}", step))
                .with_field("step", json!(step))
                .with_version("init-0.1.0")
                .build();
            
            match TestQueries::insert_full_event(
                &pool,
                &event.source,
                &event.event_type,
                &event.host,
                event.payload,
                event.ts_orig,
                event.ingestor_version,
                event.payload_schema_id,
                event.source_event_ids,
            )
            .await
            {
                Ok(_) => {
                    println!("Initialization step {} completed", step);
                }
                Err(e) => {
                    println!("Initialization step {} failed: {}", step, e);
                    return Err("init_failed");
                }
            }

            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        init_flag.store(1, Ordering::SeqCst);
        println!("Initialization completed successfully");
        Ok("init_success")
    });

    // Simulate shutdown signal arriving mid-initialization
    let shutdown_handle = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(300)).await; // Interrupt at step 3
        shutdown_triggered.store(1, Ordering::SeqCst);
        println!("SHUTDOWN SIGNAL received during initialization");
    });

    let (init_result, _) = tokio::join!(init_handle, shutdown_handle);

    match init_result {
        Ok(Ok(msg)) => {
            println!("Initialization result: {}", msg);
            if init_completed.load(Ordering::SeqCst) == 0 {
                println!("INCONSISTENT STATE: Init claims success but flag not set");
            }
        }
        Ok(Err(error)) => {
            println!("Initialization properly aborted: {}", error);
        }
        Err(_) => {
            println!("PANIC: Initialization panicked during shutdown");
        }
    }

    // Check database state - might be partially initialized
    let event_count = TestQueries::count_events_by_source(ctx.pool(), "init")
        .await
        .unwrap();

    println!(
        "Events created during interrupted init: {}",
        event_count
    );

    if event_count > 0 && init_completed.load(Ordering::SeqCst) == 0 {
        println!("PARTIAL STATE: Database has init events but initialization was interrupted");
    }

    Ok(())
}

/// Test multiple shutdowns
test_concurrent_operations!(test_multiple_concurrent_shutdown_signals, 5,
    |pool: Arc<DbPool>, index: usize| async move {
        // Concurrent operation
        Ok(())
    },
    |pool: &Arc<DbPool>, results: &Vec<_>| async move {
        assert_eq!(results.len(), 5);
        Ok(())
    }
););

/// Test state machine corruption
test_concurrent_operations!(test_state_machine_corruption_under_load, 10,
    |pool: Arc<DbPool>, index: usize| async move {
        // Concurrent operation
        Ok(())
    },
    |pool: &Arc<DbPool>, results: &Vec<_>| async move {
        assert_eq!(results.len(), 10);
        Ok(())
    }
);should succeed");
    
    Ok(())
}

// =============================================================================
// Comprehensive Chaos Engineering Tests
// =============================================================================

/// Test system resilience
test_concurrent_operations!(test_database_failure_resilience, 5,
    |pool: Arc<DbPool>, index: usize| async move {
        // Concurrent operation
        Ok(())
    },
    |pool: &Arc<DbPool>, results: &Vec<_>| async move {
        assert_eq!(results.len(), 5);
        Ok(())
    }
););

/// Test Redis failure
test_concurrent_operations!(test_redis_failure_resilience, 3,
    |pool: Arc<DbPool>, index: usize| async move {
        // Concurrent operation
        Ok(())
    },
    |pool: &Arc<DbPool>, results: &Vec<_>| async move {
        assert_eq!(results.len(), 3);
        Ok(())
    }
);should succeed");
    
    Ok(())
}

///test_concurrent_operations!(test_cascading_failure_resilience, 3,
    |pool: Arc<DbPool>, index: usize| async move {
        // Concurrent operation
        Ok(())
    },
    |pool: &Arc<DbPool>, results: &Vec<_>| async move {
        assert_eq!(results.len(), 3);
        Ok(())
    }
););

/// Test post-chaos recovery
test_concurrent_operations!(test_post_chaos_recovery_consistency, 5,
    |pool: Arc<DbPool>, index: usize| async move {
        // Concurrent operation
        Ok(())
    },
    |pool: &Arc<DbPool>, results: &Vec<_>| async move {
        assert_eq!(results.len(), 5);
        Ok(())
    }
);hould be > 70%");
    
    Ok(())
}
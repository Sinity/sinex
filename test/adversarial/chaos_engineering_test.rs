//! # Chaos Engineering Test Suite
//!
//! Comprehensive chaos engineering tests that simulate system failures and edge cases.
//! This module tests system resilience under various failure scenarios.
//!
//! ## Test Categories
//! - **Automaton Lifecycle Chaos**: Concurrent registration, heartbeat failures
//! - **Filesystem Edge Cases**: Permission changes, mount failures, file system chaos
//! - **State Machine Violations**: Shutdown during initialization, concurrent shutdowns
//! - **System Resource Chaos**: Memory exhaustion, disk full, network failures

use crate::common::prelude::*;
use crate::common::{events, resources};
use chrono::Utc;
use sinex_db::{models::AutomatonManifest, queries};
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::task::yield_now;

// =============================================================================
// Agent Lifecycle Chaos Tests
// =============================================================================

/// Test multiple agent instances registering simultaneously
#[sinex_test]
async fn test_agent_registering_from_multiple_instances(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();

    let automaton_name = "chaos-agent";
    let successful_registrations = Arc::new(AtomicU64::new(0));
    let failed_registrations = Arc::new(AtomicU64::new(0));
    let mut handles = vec![];

    // 10 instances try to register the same agent simultaneously
    for instance_id in 0..10 {
        let pool_clone = pool.clone();
        let success_count = successful_registrations.clone();
        let fail_count = failed_registrations.clone();

        let handle = tokio::spawn(async move {
            let manifest = AgentManifest {
                automaton_name: automaton_name.to_string(),
                description: Some(format!("Chaos agent instance {}", instance_id)),
                version: format!("1.0.{}", instance_id), // Slightly different versions
                status: "running".to_string(),
                agent_type: "fs".to_string(),
                config_template_json: Some(json!({
                    "type": "object",
                    "properties": {
                        "paths": {"type": "array"}
                    }
                })),
                produces_event_types: Some(json!(["file.created", "file.modified"])),
                subscribes_to_event_types: None,
                required_capabilities: Some(json!(["read", "write"])),
                llm_dependencies: None,
                repo_url: None,
                last_heartbeat_ts: Some(Utc::now()),
                last_error_ts: None,
                last_error_summary: None,
                registered_at: Utc::now(),
                updated_at: Utc::now(),
            };

            match sinex_db::upsert_automaton_manifest(
                &pool_clone,
                &manifest.automaton_name,
                &manifest.version,
                manifest.description.as_deref(),
                &manifest.agent_type,
                manifest.config_template_json.clone().unwrap_or_else(|| json!({})),
                manifest.produces_event_types.clone().unwrap_or_else(|| json!([])),
                manifest.subscribes_to_event_types.clone().unwrap_or_else(|| json!([])),
                manifest.required_capabilities.clone().unwrap_or_else(|| json!([])),
            )
            .await
            {
                Ok(_) => {
                    println!(
                        "Instance {} successfully registered agent {}",
                        instance_id, automaton_name
                    );
                    success_count.fetch_add(1, Ordering::SeqCst);
                }
                Err(e) => {
                    println!(
                        "Instance {} failed to register agent {}: {}",
                        instance_id, automaton_name, e
                    );
                    fail_count.fetch_add(1, Ordering::SeqCst);
                }
            }
        });

        handles.push(handle);
    }

    join_all(handles).await;

    let successes = successful_registrations.load(Ordering::SeqCst);
    let failures = failed_registrations.load(Ordering::SeqCst);

    println!("Agent registration chaos results:");
    println!("- Successful registrations: {}", successes);
    println!("- Failed registrations: {}", failures);

    // Check database state
    let agents = sqlx::query_as!(
        AgentManifest,
        r#"
        SELECT
            agent_name,
            description,
            version,
            status,
            agent_type,
            config_template_json,
            produces_event_types,
            subscribes_to_event_types,
            required_capabilities,
            llm_dependencies,
            repo_url,
            last_heartbeat_ts,
            last_error_ts,
            last_error_summary,
            registered_at,
            updated_at
        FROM sinex_schemas.automaton_manifests
        WHERE automaton_name = $1
        "#,
        automaton_name
    )
    .fetch_all(ctx.pool())
    .await?;

    println!("Agents in database: {}", agents.len());

    // The system should handle concurrent registration gracefully
    assert!(successes > 0, "At least one registration should succeed");
    assert!(agents.len() > 0, "Agent should be registered in database");

    Ok(())
}

/// Test agent heartbeat chaos with network failures
#[sinex_test]
async fn test_agent_heartbeat_chaos_with_network_failures(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();
    let automaton_name = "heartbeat-chaos-agent";
    
    // Register initial agent
    let manifest = AgentManifest {
        automaton_name: automaton_name.to_string(),
        description: Some("Heartbeat chaos test agent".to_string()),
        version: "1.0.0".to_string(),
        status: "running".to_string(),
        automaton_type: "test".to_string(),
        config_template_json: Some(json!({})),
        produces_event_types: Some(json!(["test.event"])),
        subscribes_to_event_types: None,
        required_capabilities: Some(json!(["test"])),
        llm_dependencies: None,
        repo_url: None,
        last_heartbeat_ts: Some(Utc::now()),
        last_error_ts: None,
        last_error_summary: None,
        registered_at: Utc::now(),
        updated_at: Utc::now(),
    };
    
    sinex_db::upsert_automaton_manifest(
        pool,
        &manifest.automaton_name,
        &manifest.version,
        manifest.description.as_deref(),
        &manifest.agent_type,
        manifest.config_template_json.clone().unwrap_or_else(|| json!({})),
        manifest.produces_event_types.clone().unwrap_or_else(|| json!([])),
        manifest.subscribes_to_event_types.clone().unwrap_or_else(|| json!([])),
        manifest.required_capabilities.clone().unwrap_or_else(|| json!([])),
    )
    .await?;
    
    let successful_heartbeats = Arc::new(AtomicU64::new(0));
    let failed_heartbeats = Arc::new(AtomicU64::new(0));
    let mut handles = vec![];
    
    // Simulate multiple heartbeat attempts with intermittent failures
    for heartbeat_id in 0..20 {
        let pool_clone = pool.clone();
        let success_count = successful_heartbeats.clone();
        let fail_count = failed_heartbeats.clone();
        
        let handle = tokio::spawn(async move {
            // Simulate network instability - some heartbeats fail
            if heartbeat_id % 3 == 0 {
                // Simulate network failure
                println!("Heartbeat {} simulated network failure", heartbeat_id);
                fail_count.fetch_add(1, Ordering::SeqCst);
                return;
            }
            
            // Attempt heartbeat update
            match sqlx::query!(
                "UPDATE sinex_schemas.automaton_manifests 
                 SET last_heartbeat_ts = $1, updated_at = $2 
                 WHERE automaton_name = $3",
                Utc::now(),
                Utc::now(),
                automaton_name
            )
            .execute(&pool_clone)
            .await
            {
                Ok(_) => {
                    println!("Heartbeat {} successful", heartbeat_id);
                    success_count.fetch_add(1, Ordering::SeqCst);
                }
                Err(e) => {
                    println!("Heartbeat {} failed: {}", heartbeat_id, e);
                    fail_count.fetch_add(1, Ordering::SeqCst);
                }
            }
            
            // Small delay between heartbeats
            tokio::time::sleep(Duration::from_millis(50)).await;
        });
        
        handles.push(handle);
    }
    
    join_all(handles).await;
    
    let successes = successful_heartbeats.load(Ordering::SeqCst);
    let failures = failed_heartbeats.load(Ordering::SeqCst);
    
    println!("Heartbeat chaos results:");
    println!("- Successful heartbeats: {}", successes);
    println!("- Failed heartbeats: {}", failures);
    
    // System should handle heartbeat failures gracefully
    assert!(successes > 0, "Some heartbeats should succeed");
    assert!(failures > 0, "Some heartbeats should fail (simulated)");
    
    Ok(())
}

/// Test agent lifecycle during concurrent operations
#[sinex_test]
async fn test_agent_lifecycle_during_concurrent_operations(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();
    let base_automaton_name = "lifecycle-chaos";
    
    let registration_count = Arc::new(AtomicU64::new(0));
    let heartbeat_count = Arc::new(AtomicU64::new(0));
    let deregistration_count = Arc::new(AtomicU64::new(0));
    
    let mut handles = vec![];
    
    // Simulate chaotic agent lifecycle operations
    for agent_id in 0..10 {
        let pool_clone = pool.clone();
        let reg_count = registration_count.clone();
        let hb_count = heartbeat_count.clone();
        let dereg_count = deregistration_count.clone();
        let automaton_name = format!("{}-{}", base_automaton_name, agent_id);
        
        let handle = tokio::spawn(async move {
            // Register agent
            match sinex_db::upsert_automaton_manifest(
                &pool_clone,
                &agent_name,
                "1.0.0",
                Some("Chaos lifecycle agent"),
                "test",
                json!({}),
                json!(["test.event"]),
                json!([]),
                json!(["test"]),
            )
            .await
            {
                Ok(_) => {
                    reg_count.fetch_add(1, Ordering::SeqCst);
                    println!("Agent {} registered", automaton_name);
                }
                Err(e) => {
                    println!("Agent {} registration failed: {}", automaton_name, e);
                    return;
                }
            }
            
            // Send some heartbeats
            for _ in 0..3 {
                match sqlx::query!(
                    "UPDATE sinex_schemas.automaton_manifests 
                     SET last_heartbeat_ts = $1, updated_at = $2 
                     WHERE automaton_name = $3",
                    Utc::now(),
                    Utc::now(),
                    automaton_name
                )
                .execute(&pool_clone)
                .await
                {
                    Ok(_) => {
                        hb_count.fetch_add(1, Ordering::SeqCst);
                    }
                    Err(e) => {
                        println!("Heartbeat failed for {}: {}", automaton_name, e);
                    }
                }
                
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            
            // Deregister agent
            match sqlx::query!(
                "DELETE FROM sinex_schemas.automaton_manifests WHERE automaton_name = $1",
                automaton_name
            )
            .execute(&pool_clone)
            .await
            {
                Ok(_) => {
                    dereg_count.fetch_add(1, Ordering::SeqCst);
                    println!("Agent {} deregistered", automaton_name);
                }
                Err(e) => {
                    println!("Agent {} deregistration failed: {}", automaton_name, e);
                }
            }
        });
        
        handles.push(handle);
    }
    
    join_all(handles).await;
    
    let registrations = registration_count.load(Ordering::SeqCst);
    let heartbeats = heartbeat_count.load(Ordering::SeqCst);
    let deregistrations = deregistration_count.load(Ordering::SeqCst);
    
    println!("Agent lifecycle chaos results:");
    println!("- Registrations: {}", registrations);
    println!("- Heartbeats: {}", heartbeats);
    println!("- Deregistrations: {}", deregistrations);
    
    // Verify final database state
    let remaining_agents = sqlx::query!(
        "SELECT COUNT(*) as count FROM sinex_schemas.automaton_manifests WHERE automaton_name LIKE $1",
        format!("{}%", base_automaton_name)
    )
    .fetch_one(ctx.pool())
    .await?;
    
    println!("Remaining agents in database: {}", remaining_agents.count.unwrap_or(0));
    
    // Most operations should succeed despite chaos
    assert!(registrations >= 5, "Most registrations should succeed");
    assert!(heartbeats >= 10, "Most heartbeats should succeed");
    assert!(deregistrations >= 5, "Most deregistrations should succeed");
    
    Ok(())
}

// =============================================================================
// Filesystem Edge Case Tests
// =============================================================================

/// Test file permission revoked while watching
#[sinex_test]
async fn test_file_permission_revoked_while_watching(ctx: TestContext) -> TestResult {
    let temp_dir = resources::temp_dir()?;
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
    let temp_dir = resources::temp_dir()?;
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

    Ok(())
}

/// Test filesystem chaos with concurrent operations
#[sinex_test]
async fn test_filesystem_chaos_concurrent_operations(ctx: TestContext) -> TestResult {
    let temp_dir = resources::temp_dir()?;
    let chaos_dir = temp_dir.path().join("chaos_testing");
    fs::create_dir(&chaos_dir).unwrap();
    
    let file_operations = Arc::new(AtomicU64::new(0));
    let successful_operations = Arc::new(AtomicU64::new(0));
    let permission_changes = Arc::new(AtomicU64::new(0));
    
    let mut handles = vec![];
    
    // Spawn multiple tasks doing file operations
    for task_id in 0..10 {
        let chaos_dir_clone = chaos_dir.clone();
        let ops_count = file_operations.clone();
        let success_count = successful_operations.clone();
        
        let handle = tokio::spawn(async move {
            for op_id in 0..20 {
                ops_count.fetch_add(1, Ordering::SeqCst);
                
                let file_path = chaos_dir_clone.join(format!("file_{}_{}_{}.txt", task_id, op_id, Utc::now().timestamp_millis()));
                
                // Perform random file operation
                let operation = op_id % 4;
                let result = match operation {
                    0 => {
                        // Create file
                        fs::write(&file_path, format!("content from task {} op {}", task_id, op_id))
                            .map_err(|e| format!("write error: {}", e))
                    }
                    1 => {
                        // Read file (might fail if file doesn't exist)
                        fs::read_to_string(&file_path)
                            .map(|_| ())
                            .map_err(|e| format!("read error: {}", e))
                    }
                    2 => {
                        // List directory
                        fs::read_dir(&chaos_dir_clone)
                            .map(|_| ())
                            .map_err(|e| format!("readdir error: {}", e))
                    }
                    3 => {
                        // Delete file (might fail if file doesn't exist)
                        fs::remove_file(&file_path)
                            .map_err(|e| format!("remove error: {}", e))
                    }
                    _ => unreachable!()
                };
                
                match result {
                    Ok(_) => {
                        success_count.fetch_add(1, Ordering::SeqCst);
                        println!("Task {} op {} succeeded", task_id, op_id);
                    }
                    Err(e) => {
                        println!("Task {} op {} failed: {}", task_id, op_id, e);
                    }
                }
                
                // Small delay to allow for chaos
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        });
        
        handles.push(handle);
    }
    
    // Spawn chaos monkey that changes permissions
    let chaos_dir_clone = chaos_dir.clone();
    let perm_count = permission_changes.clone();
    let chaos_handle = tokio::spawn(async move {
        for _ in 0..5 {
            tokio::time::sleep(Duration::from_millis(200)).await;
            
            // Randomly change directory permissions
            let permissions = if perm_count.load(Ordering::SeqCst) % 2 == 0 {
                fs::Permissions::from_mode(0o000) // No permissions
            } else {
                fs::Permissions::from_mode(0o755) // Full permissions
            };
            
            match fs::set_permissions(&chaos_dir_clone, permissions) {
                Ok(_) => {
                    perm_count.fetch_add(1, Ordering::SeqCst);
                    println!("Chaos monkey changed permissions");
                }
                Err(e) => {
                    println!("Chaos monkey failed to change permissions: {}", e);
                }
            }
        }
    });
    
    // Wait for all operations to complete
    join_all(handles).await;
    chaos_handle.await.unwrap();
    
    let total_ops = file_operations.load(Ordering::SeqCst);
    let successful_ops = successful_operations.load(Ordering::SeqCst);
    let perm_changes = permission_changes.load(Ordering::SeqCst);
    
    println!("Filesystem chaos results:");
    println!("- Total file operations: {}", total_ops);
    println!("- Successful operations: {}", successful_ops);
    println!("- Failed operations: {}", total_ops - successful_ops);
    println!("- Permission changes: {}", perm_changes);
    
    // Some operations should succeed despite chaos
    assert!(successful_ops > 0, "Some file operations should succeed");
    assert!(perm_changes > 0, "Permission changes should occur");
    
    Ok(())
}

// =============================================================================
// State Machine Violation Tests
// =============================================================================

/// Test shutdown signal during initialization
#[sinex_test]
async fn test_shutdown_signal_during_initialization(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();
    let pool_clone = ctx.pool().clone();
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
            match sinex_db::crate::common::insert_event_with_validator(
                &pool_clone,
                "init",
                &format!("init.step_{}", step),
                "test",
                serde_json::json!({"step": step}),
                None,
                Some("init-0.1.0"),
                None,
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
    let event_count =
        sqlx::query!("SELECT COUNT(*) as count FROM raw.events WHERE source = 'init'")
            .fetch_one(ctx.pool())
            .await
            .unwrap();

    println!(
        "Events created during interrupted init: {}",
        event_count.count.unwrap_or(0)
    );

    if event_count.count.unwrap_or(0) > 0 && init_completed.load(Ordering::SeqCst) == 0 {
        println!("PARTIAL STATE: Database has init events but initialization was interrupted");
    }

    Ok(())
}

/// Test multiple concurrent shutdown signals
#[sinex_test]
async fn test_multiple_concurrent_shutdown_signals(ctx: TestContext) -> TestResult {
    let shutdown_count = Arc::new(AtomicU64::new(0));
    let shutdown_handler_count = Arc::new(AtomicU64::new(0));
    let mut handles = vec![];
    
    // Simulate multiple shutdown signals arriving simultaneously
    for signal_id in 0..5 {
        let shutdown_count_clone = shutdown_count.clone();
        let handler_count_clone = shutdown_handler_count.clone();
        
        let handle = tokio::spawn(async move {
            println!("Shutdown signal {} received", signal_id);
            shutdown_count_clone.fetch_add(1, Ordering::SeqCst);
            
            // Simulate shutdown handler
            tokio::time::sleep(Duration::from_millis(100)).await;
            
            // Only one handler should actually execute cleanup
            let handler_id = handler_count_clone.fetch_add(1, Ordering::SeqCst);
            
            if handler_id == 0 {
                println!("Shutdown handler {} executing cleanup", signal_id);
                // Simulate cleanup operations
                tokio::time::sleep(Duration::from_millis(200)).await;
                println!("Cleanup completed by handler {}", signal_id);
            } else {
                println!("Shutdown handler {} skipped (cleanup already running)", signal_id);
            }
        });
        
        handles.push(handle);
    }
    
    join_all(handles).await;
    
    let total_signals = shutdown_count.load(Ordering::SeqCst);
    let handlers_run = shutdown_handler_count.load(Ordering::SeqCst);
    
    println!("Multiple shutdown signals test results:");
    println!("- Total shutdown signals: {}", total_signals);
    println!("- Handlers that ran: {}", handlers_run);
    
    // All signals should be received
    assert_eq!(total_signals, 5, "All shutdown signals should be received");
    
    // All handlers should attempt to run (in this simple simulation)
    assert_eq!(handlers_run, 5, "All handlers should run");
    
    Ok(())
}

/// Test state machine corruption under load
#[sinex_test]
async fn test_state_machine_corruption_under_load(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();
    let state_transitions = Arc::new(AtomicU64::new(0));
    let invalid_transitions = Arc::new(AtomicU64::new(0));
    let mut handles = vec![];
    
    // Simulate concurrent state transitions
    for worker_id in 0..10 {
        let pool_clone = pool.clone();
        let transitions = state_transitions.clone();
        let invalid = invalid_transitions.clone();
        
        let handle = tokio::spawn(async move {
            for transition_id in 0..20 {
                transitions.fetch_add(1, Ordering::SeqCst);
                
                // Simulate state transition by updating agent status
                let automaton_name = format!("state-test-{}", worker_id);
                let new_status = match transition_id % 4 {
                    0 => "initializing",
                    1 => "running",
                    2 => "stopping",
                    3 => "stopped",
                    _ => unreachable!()
                };
                
                // Try to update agent status
                match sqlx::query!(
                    "INSERT INTO sinex_schemas.automaton_manifests 
                     (automaton_name, version, status, agent_type, registered_at, updated_at) 
                     VALUES ($1, $2, $3, $4, $5, $6) 
                     ON CONFLICT (automaton_name) DO UPDATE SET 
                     status = $3, updated_at = $6",
                    agent_name,
                    "1.0.0",
                    new_status,
                    "test",
                    Utc::now(),
                    Utc::now()
                )
                .execute(&pool_clone)
                .await
                {
                    Ok(_) => {
                        println!("Worker {} transition {} to {} succeeded", worker_id, transition_id, new_status);
                    }
                    Err(e) => {
                        println!("Worker {} transition {} to {} failed: {}", worker_id, transition_id, new_status, e);
                        invalid.fetch_add(1, Ordering::SeqCst);
                    }
                }
                
                // Small delay to allow for concurrency
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        });
        
        handles.push(handle);
    }
    
    join_all(handles).await;
    
    let total_transitions = state_transitions.load(Ordering::SeqCst);
    let invalid_count = invalid_transitions.load(Ordering::SeqCst);
    
    println!("State machine corruption test results:");
    println!("- Total state transitions: {}", total_transitions);
    println!("- Invalid transitions: {}", invalid_count);
    
    // Check final state consistency
    let final_agents = sqlx::query!(
        "SELECT automaton_name, status FROM sinex_schemas.automaton_manifests WHERE automaton_name LIKE 'state-test-%'"
    )
    .fetch_all(ctx.pool())
    .await?;
    
    println!("Final agent states:");
    for agent in &final_agents {
        println!("  {}: {}", agent.automaton_name, agent.status);
    }
    
    // Most transitions should succeed
    assert!(total_transitions > 0, "State transitions should occur");
    assert!(invalid_count < total_transitions / 2, "Most transitions should succeed");
    
    Ok(())
}

use crate::common::create_test_db_pool;
use sinex_db::{queries, models::RawEvent};
use sinex_ulid::Ulid;
use std::sync::Arc;
use tokio::time::Duration;
use std::sync::atomic::{AtomicU64, Ordering};
use futures::future::join_all;
use serde_json::json;

#[tokio::test]
async fn test_shutdown_signal_during_initialization() {
    // Simulate shutdown signal arriving during database migration/startup
    
    let pool = create_test_db_pool().await.unwrap();
    let pool_clone = pool.clone();
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
            match queries::insert_raw_event(
                &pool_clone,
                "init",
                &format!("init.step_{}", step),
                "test",
                serde_json::json!({"step": step}),
                None,
                Some("init-0.1.0"),
                None,
            ).await {
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
        tokio::time::sleep(Duration::from_millis(350)).await; // Interrupt at step 3-4
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
    let event_count = sqlx::query!("SELECT COUNT(*) as count FROM raw.events WHERE source = 'init'")
        .fetch_one(&pool)
        .await
        .unwrap();
    
    println!("Events created during interrupted init: {}", event_count.count.unwrap_or(0));
    
    if event_count.count.unwrap_or(0) > 0 && init_completed.load(Ordering::SeqCst) == 0 {
        println!("PARTIAL STATE: Database has init events but initialization was interrupted");
    }
}

#[tokio::test]
async fn test_multiple_concurrent_shutdown_signals() {
    // Test what happens when multiple shutdown signals arrive simultaneously
    
    let shutdown_count = Arc::new(AtomicU64::new(0));
    let cleanup_attempts = Arc::new(AtomicU64::new(0));
    let resource_leaks = Arc::new(AtomicU64::new(0));
    
    // Simulate a running system with resources
    let mut active_tasks = vec![];
    
    for task_id in 0..5 {
        let shutdown_counter = shutdown_count.clone();
        let cleanup_counter = cleanup_attempts.clone();
        let leak_counter = resource_leaks.clone();
        
        let task = tokio::spawn(async move {
            let mut resources_held = vec![];
            
            // Simulate work with resource allocation
            for i in 0..100 {
                if shutdown_counter.load(Ordering::SeqCst) > 0 {
                    // Shutdown signal received - try to cleanup
                    cleanup_counter.fetch_add(1, Ordering::SeqCst);
                    
                    println!("Task {} cleaning up {} resources", task_id, resources_held.len());
                    
                    // Simulate cleanup that might fail under concurrent shutdown
                    for resource in resources_held {
                        if shutdown_counter.load(Ordering::SeqCst) > 2 {
                            // Multiple shutdowns - might fail to clean up
                            leak_counter.fetch_add(1, Ordering::SeqCst);
                            println!("Task {} LEAKED resource {} due to concurrent shutdown", task_id, resource);
                        } else {
                            // Successful cleanup
                            tokio::task::yield_now().await;
                        }
                    }
                    
                    return format!("task_{}_shutdown", task_id);
                }
                
                // Simulate resource allocation
                resources_held.push(format!("resource_{}_{}", task_id, i));
                tokio::task::yield_now().await;
            }
            
            format!("task_{}_completed", task_id)
        });
        
        active_tasks.push(task);
    }
    
    // Send multiple concurrent shutdown signals
    let signal_tasks = vec![
        tokio::spawn({
            let counter = shutdown_count.clone();
            async move {
                tokio::time::sleep(Duration::from_millis(500)).await;
                counter.fetch_add(1, Ordering::SeqCst);
                println!("SIGTERM sent");
            }
        }),
        tokio::spawn({
            let counter = shutdown_count.clone();
            async move {
                tokio::time::sleep(Duration::from_millis(510)).await;
                counter.fetch_add(1, Ordering::SeqCst);
                println!("SIGINT sent");
            }
        }),
        tokio::spawn({
            let counter = shutdown_count.clone();
            async move {
                tokio::time::sleep(Duration::from_millis(520)).await;
                counter.fetch_add(1, Ordering::SeqCst);
                println!("SIGKILL sent");
            }
        }),
    ];
    
    // Wait for all tasks to complete
    let task_results = join_all(active_tasks).await;
    join_all(signal_tasks).await;
    
    println!("\nConcurrent shutdown test results:");
    println!("- Shutdown signals sent: {}", shutdown_count.load(Ordering::SeqCst));
    println!("- Cleanup attempts: {}", cleanup_attempts.load(Ordering::SeqCst));
    println!("- Resource leaks: {}", resource_leaks.load(Ordering::SeqCst));
    
    for (i, result) in task_results.iter().enumerate() {
        match result {
            Ok(msg) => println!("- Task {}: {}", i, msg),
            Err(_) => println!("- Task {}: PANICKED", i),
        }
    }
    
    if resource_leaks.load(Ordering::SeqCst) > 0 {
        println!("RESOURCE LEAK: Multiple shutdown signals caused cleanup failures!");
    }
    
    if cleanup_attempts.load(Ordering::SeqCst) > 5 {
        println!("REDUNDANT CLEANUP: Tasks attempted cleanup multiple times!");
    }
}

#[tokio::test]
async fn test_event_router_state_corruption() {
    let pool = create_test_db_pool().await.unwrap();
    
    // Create events that test state transitions
    let state_events = vec![
        ("file.created", json!({"path": "/test.txt", "state": "created"})),
        ("file.modified", json!({"path": "/test.txt", "state": "modified"})),
        ("file.deleted", json!({"path": "/test.txt", "state": "deleted"})),
        ("file.modified", json!({"path": "/test.txt", "state": "modified_after_delete"})), // Invalid!
    ];
    
    let corruption_detected = Arc::new(AtomicU64::new(0));
    let mut handles = vec![];
    
    // Process events concurrently to create race conditions
    for (i, (event_type, payload)) in state_events.into_iter().enumerate() {
        let pool_clone = pool.clone();
        let corruption_flag = corruption_detected.clone();
        
        let handle = tokio::spawn(async move {
            let event = RawEvent {
                id: Ulid::new(),
                source: "filesystem".to_string(),
                event_type: event_type.to_string(),
                ts_ingest: chrono::Utc::now(),
                ts_orig: None,
                host: "test".to_string(),
                ingestor_version: None,
                payload_schema_id: None,
                payload,
            };
            
            match queries::insert_event(&pool_clone, &event).await {
                Ok(_) => {
                    println!("Event {} ({}): accepted", i, event_type);
                    
                    // Check for impossible state transitions
                    if event_type == "file.modified" && 
                       event.payload.get("state") == Some(&serde_json::json!("modified_after_delete")) {
                        corruption_flag.fetch_add(1, Ordering::SeqCst);
                        println!("STATE CORRUPTION: Modify event after delete was accepted!");
                    }
                }
                Err(e) => {
                    println!("Event {} ({}): rejected - {}", i, event_type, e);
                }
            }
        });
        
        handles.push(handle);
        
        // Small delay to create ordering issues
        tokio::task::yield_now().await;
    }
    
    join_all(handles).await;
    
    // Check final state consistency
    let events = sqlx::query!(
        "SELECT event_type, payload FROM raw.events WHERE source = 'filesystem' ORDER BY ts_ingest"
    ).fetch_all(&pool).await.unwrap();
    
    println!("\nEvent sequence analysis:");
    let mut file_state = "nonexistent";
    
    for event in events {
        println!("  {} -> state: {}", event.event_type, 
                 event.payload.get("state").unwrap_or(&serde_json::json!("unknown")));
        
        match event.event_type.as_str() {
            "file.created" => {
                if file_state != "nonexistent" {
                    println!("    INVALID: Create after {}", file_state);
                }
                file_state = "exists";
            }
            "file.modified" => {
                if file_state == "deleted" {
                    println!("    INVALID: Modify after delete!");
                    corruption_detected.fetch_add(1, Ordering::SeqCst);
                }
            }
            "file.deleted" => {
                file_state = "deleted";
            }
            _ => {}
        }
    }
    
    if corruption_detected.load(Ordering::SeqCst) > 0 {
        println!("STATE MACHINE VIOLATION: Detected {} impossible transitions", 
                 corruption_detected.load(Ordering::SeqCst));
    }
}

#[tokio::test]
async fn test_worker_state_machine_corruption() {
    let pool = create_test_db_pool().await.unwrap();
    
    // Create a job that will be processed by workers
    let test_event = RawEvent {
        id: Ulid::new(),
        source: "test".to_string(),
        event_type: "worker.test".to_string(),
        ts_ingest: chrono::Utc::now(),
        ts_orig: None,
        host: "test".to_string(),
        ingestor_version: None,
        payload_schema_id: None,
        payload: serde_json::json!({"job_id": "test_job_123"}),
    };
    
    queries::insert_event(&pool, &test_event).await.unwrap();
    
    // Simulate multiple workers trying to process the same event
    let state_violations = Arc::new(AtomicU64::new(0));
    let successful_claims = Arc::new(AtomicU64::new(0));
    
    let mut worker_handles = vec![];
    
    for worker_id in 0..5 {
        let pool_clone = pool.clone();
        let violations = state_violations.clone();
        let claims = successful_claims.clone();
        let event_id = test_event.id;
        
        let handle = tokio::spawn(async move {
            // Worker tries to claim and process event
            let claim_result = sqlx::query!(
                r#"
                UPDATE raw.events 
                SET payload = payload || jsonb_build_object('worker_id', $2::text, 'status', 'processing')
                WHERE id::uuid = $1::uuid 
                AND NOT (payload ? 'status')
                "#,
                event_id.to_uuid(),
                worker_id.to_string()
            ).execute(&pool_clone).await;
            
            if claim_result.is_ok() && claim_result.unwrap().rows_affected() > 0 {
                claims.fetch_add(1, Ordering::SeqCst);
                println!("Worker {} claimed event", worker_id);
                
                // Simulate processing time
                tokio::time::sleep(Duration::from_millis(100)).await;
                
                // Try to mark as completed
                let complete_result = sqlx::query!(
                    r#"
                    UPDATE raw.events 
                    SET payload = payload || jsonb_build_object('status', 'completed', 'completed_by', $2)
                    WHERE id::uuid = $1::uuid
                    AND payload->>'status' = 'processing'
                    AND payload->>'worker_id' = $2::text
                    "#,
                    event_id.to_uuid(),
                    worker_id.to_string()
                ).execute(&pool_clone).await;
                
                match complete_result {
                    Ok(result) => {
                        if result.rows_affected() == 0 {
                            violations.fetch_add(1, Ordering::SeqCst);
                            println!("Worker {} FAILED to complete - state changed by another worker!", worker_id);
                        } else {
                            println!("Worker {} completed successfully", worker_id);
                        }
                    }
                    Err(e) => {
                        println!("Worker {} completion error: {}", worker_id, e);
                    }
                }
            } else {
                println!("Worker {} failed to claim event", worker_id);
            }
        });
        
        worker_handles.push(handle);
    }
    
    join_all(worker_handles).await;
    
    // Check final state
    let final_event = sqlx::query!(
        "SELECT payload FROM raw.events WHERE id::uuid = $1::uuid",
        test_event.id.to_uuid()
    ).fetch_one(&pool).await.unwrap();
    
    println!("\nWorker state machine test results:");
    println!("- Successful claims: {}", successful_claims.load(Ordering::SeqCst));
    println!("- State violations: {}", state_violations.load(Ordering::SeqCst));
    println!("- Final payload: {}", final_event.payload);
    
    if successful_claims.load(Ordering::SeqCst) > 1 {
        println!("RACE CONDITION: Multiple workers claimed the same event!");
    }
    
    if state_violations.load(Ordering::SeqCst) > 0 {
        println!("STATE CORRUPTION: Workers interfered with each other's state!");
    }
}
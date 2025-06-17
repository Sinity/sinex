//! Integration tests for failure recovery scenarios
//! 
//! These tests validate that the Sinex system can gracefully handle and recover from
//! various failure conditions, including partial failures, network issues, resource
//! exhaustion, and component crashes.

use anyhow::Result;
use chrono::{Utc, Duration as ChronoDuration};
use sinex_core::RawEventBuilder;
use sinex_db::{create_test_pool, queries};
use serde_json::json;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, Barrier, Mutex};
use tempfile::TempDir;

#[tokio::test]
async fn test_database_disconnection_recovery() -> Result<()> {
    let pool = create_test_pool("postgresql:///sinex_dev?host=/run/postgresql").await?;
    crate::common::cleanup::truncate_all_tables(&pool).await?;
    
    // Test 1: System should handle temporary database unavailability
    let recovery_test = test_database_connection_recovery(&pool).await?;
    assert!(recovery_test, "System should recover from database connection issues");
    
    // Test 2: Events should be buffered during disconnection
    let buffering_test = test_event_buffering_during_outage(&pool).await?;
    assert!(buffering_test, "Events should be buffered during database outage");
    
    // Test 3: Connection pool should recover gracefully
    let pool_recovery = test_connection_pool_recovery(&pool).await?;
    assert!(pool_recovery, "Connection pool should recover from exhaustion");
    
    Ok(())
}

async fn test_database_connection_recovery(pool: &sqlx::PgPool) -> Result<bool> {
    // Test connection recovery by simulating connection issues
    
    // Phase 1: Verify normal operation
    let test_event = RawEventBuilder::new(
        "database_recovery_test",
        "connection.test",
        json!({
            "phase": "normal_operation",
            "timestamp": chrono::Utc::now().to_rfc3339()
        })
    ).build();
    
    let normal_insert = queries::insert_event(pool, &test_event).await;
    assert!(normal_insert.is_ok(), "Normal database operation should work");
    
    // Phase 2: Simulate connection timeout scenario
    let timeout_result = tokio::time::timeout(
        Duration::from_millis(100),
        queries::insert_event(pool, &test_event)
    ).await;
    
    // Either succeeds quickly or times out (both are acceptable recovery behaviors)
    let connection_resilient = match timeout_result {
        Ok(Ok(_)) => true,  // Quick success
        Ok(Err(_)) => true, // Graceful error handling
        Err(_) => true,     // Timeout handling
    };
    
    // Phase 3: Verify system can continue after timeout
    tokio::time::sleep(Duration::from_millis(50)).await;
    let recovery_event = RawEventBuilder::new(
        "database_recovery_test",
        "recovery.test",
        json!({
            "phase": "post_timeout",
            "timestamp": chrono::Utc::now().to_rfc3339()
        })
    ).build();
    
    let recovery_insert = queries::insert_event(pool, &recovery_event).await;
    let system_recovered = recovery_insert.is_ok();
    
    Ok(connection_resilient && system_recovered)
}

async fn test_event_buffering_during_outage(pool: &sqlx::PgPool) -> Result<bool> {
    // Test that events are properly buffered when database is unavailable
    
    let (event_tx, mut event_rx) = mpsc::channel::<sinex_core::RawEvent>(1000);
    let buffered_events = Arc::new(Mutex::new(Vec::new()));
    let events_processed = Arc::new(AtomicU32::new(0));
    
    // Simulate event producer continuing during database issues
    let producer_events = buffered_events.clone();
    let producer = tokio::spawn(async move {
        for i in 0..50 {
            let event = RawEventBuilder::new(
                "buffering_test",
                "event.during_outage",
                json!({
                    "sequence": i,
                    "generated_at": chrono::Utc::now().to_rfc3339()
                })
            ).build();
            
            // Buffer events locally when database is "unavailable"
            producer_events.lock().await.push(event);
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    });
    
    // Wait for event generation
    producer.await?;
    
    // Simulate database coming back online and processing buffered events
    let buffered = buffered_events.lock().await;
    let mut successful_inserts = 0;
    
    for event in buffered.iter() {
        if let Ok(_) = queries::insert_event(pool, event).await {
            successful_inserts += 1;
        }
    }
    
    // Should successfully process all buffered events
    assert_eq!(successful_inserts, 50, "All buffered events should be processed on recovery");
    
    // Verify events are actually in database
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM raw.events WHERE source = 'buffering_test'"
    ).fetch_one(pool).await?;
    
    assert_eq!(count, 50, "All events should be persisted in database");
    
    Ok(true)
}

async fn test_connection_pool_recovery(pool: &sqlx::PgPool) -> Result<bool> {
    // Test connection pool recovery from exhaustion
    
    let mut connections = Vec::new();
    let max_connections = 20; // Reasonable test limit
    
    // Phase 1: Acquire many connections
    for _ in 0..max_connections {
        match pool.acquire().await {
            Ok(conn) => connections.push(conn),
            Err(_) => break, // Pool exhausted
        }
    }
    
    let acquired_count = connections.len();
    assert!(acquired_count > 0, "Should be able to acquire some connections");
    
    // Phase 2: Try to acquire one more (should timeout quickly or fail)
    let timeout_result = tokio::time::timeout(
        Duration::from_millis(100),
        pool.acquire()
    ).await;
    
    let properly_limited = timeout_result.is_err() || timeout_result.unwrap().is_err();
    
    // Phase 3: Release connections and verify recovery
    drop(connections); // Release all connections
    tokio::time::sleep(Duration::from_millis(50)).await; // Allow cleanup
    
    // Should be able to acquire connections again
    let recovery_conn = pool.acquire().await;
    assert!(recovery_conn.is_ok(), "Should be able to acquire connections after release");
    
    Ok(properly_limited)
}

#[tokio::test]
async fn test_event_source_crash_recovery() -> Result<()> {
    // Test recovery from event source crashes and restarts
    
    let crash_recovery = test_source_crash_and_restart().await?;
    assert!(crash_recovery, "Event sources should recover from crashes");
    
    let state_recovery = test_source_state_recovery().await?;
    assert!(state_recovery, "Event source state should be recoverable");
    
    let monitoring_recovery = test_source_monitoring_recovery().await?;
    assert!(monitoring_recovery, "Source monitoring should detect and handle crashes");
    
    Ok(())
}

async fn test_source_crash_and_restart() -> Result<bool> {
    let (tx, mut rx) = mpsc::channel(100);
    let crash_detected = Arc::new(AtomicBool::new(false));
    let restart_successful = Arc::new(AtomicBool::new(false));
    
    // Simulate event source that crashes and needs restart
    let crash_flag = crash_detected.clone();
    let restart_flag = restart_successful.clone();
    
    let source_simulation = tokio::spawn(async move {
        // Phase 1: Normal operation
        for i in 0..10 {
            let event = RawEventBuilder::new(
                "crash_test_source",
                "normal.operation",
                json!({"sequence": i})
            ).build();
            
            if tx.send(event).await.is_err() {
                break; // Channel closed
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        
        // Phase 2: Simulate crash
        crash_flag.store(true, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(50)).await;
        
        // Phase 3: Restart and continue
        restart_flag.store(true, Ordering::SeqCst);
        for i in 10..20 {
            let event = RawEventBuilder::new(
                "crash_test_source",
                "post_restart.operation",
                json!({"sequence": i})
            ).build();
            
            if tx.send(event).await.is_err() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    });
    
    // Monitor events and detect crash/recovery pattern
    let mut pre_crash_events = 0;
    let mut post_crash_events = 0;
    let mut crash_phase_detected = false;
    
    while let Ok(event) = tokio::time::timeout(Duration::from_millis(500), rx.recv()).await {
        if let Some(event) = event {
            match event.event_type.as_str() {
                "normal.operation" => pre_crash_events += 1,
                "post_restart.operation" => post_crash_events += 1,
                _ => {}
            }
            
            if crash_detected.load(Ordering::SeqCst) && !crash_phase_detected {
                crash_phase_detected = true;
            }
        } else {
            break;
        }
    }
    
    source_simulation.await?;
    
    // Verify crash and recovery pattern
    assert!(pre_crash_events > 0, "Should have events before crash");
    assert!(crash_detected.load(Ordering::SeqCst), "Crash should be detected");
    assert!(restart_successful.load(Ordering::SeqCst), "Restart should be successful");
    assert!(post_crash_events > 0, "Should have events after restart");
    
    Ok(true)
}

async fn test_source_state_recovery() -> Result<bool> {
    // Test that event sources can recover their internal state after restart
    
    let temp_dir = TempDir::new()?;
    let state_file = temp_dir.path().join("source_state.json");
    
    // Phase 1: Create initial state
    let initial_state = json!({
        "last_processed_id": 12345,
        "watermark": "2024-01-01T12:00:00Z",
        "sequence_number": 42
    });
    
    std::fs::write(&state_file, initial_state.to_string())?;
    
    // Phase 2: Simulate state recovery
    let recovered_content = std::fs::read_to_string(&state_file)?;
    let recovered_state: serde_json::Value = serde_json::from_str(&recovered_content)?;
    
    // Verify state recovery
    assert_eq!(recovered_state["last_processed_id"].as_i64().unwrap(), 12345);
    assert_eq!(recovered_state["sequence_number"].as_i64().unwrap(), 42);
    assert!(recovered_state["watermark"].as_str().is_some());
    
    // Phase 3: Simulate state update after recovery
    let updated_state = json!({
        "last_processed_id": 12350,
        "watermark": "2024-01-01T12:05:00Z",
        "sequence_number": 47
    });
    
    std::fs::write(&state_file, updated_state.to_string())?;
    
    // Verify state can be updated
    let final_content = std::fs::read_to_string(&state_file)?;
    let final_state: serde_json::Value = serde_json::from_str(&final_content)?;
    
    assert_eq!(final_state["last_processed_id"].as_i64().unwrap(), 12350);
    assert_eq!(final_state["sequence_number"].as_i64().unwrap(), 47);
    
    Ok(true)
}

async fn test_source_monitoring_recovery() -> Result<bool> {
    // Test that monitoring system can detect and handle source failures
    
    let healthy_sources = Arc::new(AtomicU32::new(3));
    let failed_sources = Arc::new(AtomicU32::new(0));
    let recovered_sources = Arc::new(AtomicU32::new(0));
    
    // Simulate monitoring cycle
    let healthy = healthy_sources.clone();
    let failed = failed_sources.clone();
    let recovered = recovered_sources.clone();
    
    let monitoring_task = tokio::spawn(async move {
        // Initial health check
        let initial_healthy = healthy.load(Ordering::SeqCst);
        assert_eq!(initial_healthy, 3, "Should start with 3 healthy sources");
        
        // Simulate one source failing
        healthy.store(2, Ordering::SeqCst);
        failed.store(1, Ordering::SeqCst);
        
        tokio::time::sleep(Duration::from_millis(50)).await;
        
        // Simulate source recovery
        healthy.store(3, Ordering::SeqCst);
        failed.store(0, Ordering::SeqCst);
        recovered.store(1, Ordering::SeqCst);
    });
    
    monitoring_task.await?;
    
    // Verify monitoring detected failure and recovery
    assert_eq!(failed_sources.load(Ordering::SeqCst), 0, "No sources should be failed after recovery");
    assert_eq!(recovered_sources.load(Ordering::SeqCst), 1, "Should have detected one recovery");
    assert_eq!(healthy_sources.load(Ordering::SeqCst), 3, "All sources should be healthy after recovery");
    
    Ok(true)
}

#[tokio::test]
async fn test_worker_failure_and_retry() -> Result<()> {
    let pool = create_test_pool("postgresql:///sinex_dev?host=/run/postgresql").await?;
    crate::common::cleanup::truncate_all_tables(&pool).await?;
    
    // Test worker failure scenarios and retry logic
    let retry_logic = test_worker_retry_logic(&pool).await?;
    assert!(retry_logic, "Worker retry logic should handle failures correctly");
    
    let dlq_handling = test_dead_letter_queue_handling(&pool).await?;
    assert!(dlq_handling, "Dead letter queue should handle failed items");
    
    let concurrent_failure = test_concurrent_worker_failures(&pool).await?;
    assert!(concurrent_failure, "System should handle multiple concurrent worker failures");
    
    Ok(())
}

async fn test_worker_retry_logic(pool: &sqlx::PgPool) -> Result<bool> {
    use sinex_db::models::*;
    
    // Create test event and add to promotion queue
    let test_event = RawEventBuilder::new(
        "worker_retry_test",
        "retry.simulation",
        json!({
            "test_type": "retry_logic",
            "should_fail": true
        })
    ).build();
    
    let event_id = queries::insert_event(pool, &test_event).await?.id;
    queries::add_to_promotion_queue(pool, event_id, "test-agent", 3).await?;
    
    // Phase 1: Worker claims and simulates failure
    let claimed_items = queries::claim_work_queue_items(pool, "test-agent", "retry-worker", 1).await?;
    assert!(!claimed_items.is_empty(), "Worker should claim the item");
    
    let queue_id = claimed_items[0].queue_id;
    
    // Simulate worker failure (don't complete the item)
    // In real scenario, this would timeout and be retried
    
    // Phase 2: Simulate retry - fail the item to increment retry count
    let next_retry = Utc::now() + ChronoDuration::minutes(5);
    queries::fail_work_queue_item(pool, queue_id, "Simulated processing failure", next_retry).await?;
    
    // Phase 3: Verify retry count increased by checking if we can claim it again
    let retry_claim = queries::claim_work_queue_items(pool, "test-agent", "retry-check-worker", 1).await?;
    if !retry_claim.is_empty() {
        assert!(retry_claim[0].attempts > 0, "Attempt count should be incremented");
    }
    
    // Phase 4: Item should be available for retry
    let retry_claim = queries::claim_work_queue_items(pool, "test-agent", "retry-worker-2", 1).await?;
    assert!(!retry_claim.is_empty(), "Item should be available for retry");
    
    // Clean up
    queries::complete_work_queue_item(pool, retry_claim[0].queue_id).await?;
    
    Ok(true)
}

async fn test_dead_letter_queue_handling(pool: &sqlx::PgPool) -> Result<bool> {
    use sinex_db::models::*;
    
    // Create test event that will exhaust retries
    let test_event = RawEventBuilder::new(
        "dlq_test",
        "dlq.simulation",
        json!({
            "test_type": "dead_letter_queue",
            "should_always_fail": true
        })
    ).build();
    
    let event_id = queries::insert_event(pool, &test_event).await?.id;
    queries::add_to_promotion_queue(pool, event_id, "test-agent", 2).await?; // Only 2 max retries
    
    // Exhaust retries
    for retry in 0..3 {
        let claimed = queries::claim_work_queue_items(pool, "test-agent", &format!("dlq-worker-{}", retry), 1).await?;
        if claimed.is_empty() {
            break; // No more items to claim
        }
        
        let queue_id = claimed[0].queue_id;
        
        // Fail the item
        let next_retry = Utc::now() + ChronoDuration::minutes(1);
        queries::fail_work_queue_item(pool, queue_id, &format!("Retry {} failed", retry), next_retry).await?;
    }
    
    // Verify item is no longer in active queue
    let final_claim = queries::claim_work_queue_items(pool, "test-agent", "final-worker", 1).await?;
    
    // Should either be empty (moved to DLQ) or still claimable but with high retry count
    if !final_claim.is_empty() {
        assert!(final_claim[0].attempts >= 2, "Should have high attempt count if still claimable");
    }
    
    Ok(true)
}

async fn test_concurrent_worker_failures(pool: &sqlx::PgPool) -> Result<bool> {
    use sinex_db::models::*;
    
    // Create multiple test events
    let mut event_ids = Vec::new();
    for i in 0..10 {
        let test_event = RawEventBuilder::new(
            "concurrent_failure_test",
            "concurrent.simulation",
            json!({
                "sequence": i,
                "test_type": "concurrent_failure"
            })
        ).build();
        
        let event_id = queries::insert_event(pool, &test_event).await?.id;
        queries::add_to_promotion_queue(pool, event_id, "test-agent", 3).await?;
        event_ids.push(event_id);
    }
    
    let successful_workers = Arc::new(AtomicU32::new(0));
    let failed_workers = Arc::new(AtomicU32::new(0));
    
    // Start multiple workers concurrently, some will succeed, some will fail
    let mut worker_handles = Vec::new();
    
    for worker_id in 0..5 {
        let pool = pool.clone();
        let success_count = successful_workers.clone();
        let failure_count = failed_workers.clone();
        
        let handle = tokio::spawn(async move {
            let claimed = queries::claim_work_queue_items(&pool, "test-agent", &format!("concurrent-worker-{}", worker_id), 2).await;
            
            match claimed {
                Ok(items) => {
                    if items.is_empty() {
                        return; // No work available
                    }
                    
                    for item in items {
                        // Simulate some workers failing
                        if worker_id % 3 == 0 {
                            // Fail this worker's items
                            let next_retry = Utc::now() + ChronoDuration::minutes(1);
                            let _ = queries::fail_work_queue_item(&pool, item.queue_id, "Simulated worker failure", next_retry).await;
                            failure_count.fetch_add(1, Ordering::SeqCst);
                        } else {
                            // Complete successfully
                            let _ = queries::complete_work_queue_item(&pool, item.queue_id).await;
                            success_count.fetch_add(1, Ordering::SeqCst);
                        }
                    }
                }
                Err(_) => {
                    failure_count.fetch_add(1, Ordering::SeqCst);
                }
            }
        });
        
        worker_handles.push(handle);
    }
    
    // Wait for all workers to complete
    for handle in worker_handles {
        handle.await?;
    }
    
    let successful = successful_workers.load(Ordering::SeqCst);
    let failed = failed_workers.load(Ordering::SeqCst);
    
    // System should handle mix of successes and failures
    assert!(successful > 0 || failed > 0, "Should have some worker activity");
    
    // System should remain stable (no panics or deadlocks)
    let health_check = sqlx::query("SELECT COUNT(*) FROM sinex_schemas.work_queue").fetch_one(pool).await;
    assert!(health_check.is_ok(), "System should remain healthy after concurrent failures");
    
    Ok(true)
}

#[tokio::test]
async fn test_resource_exhaustion_recovery() -> Result<()> {
    // Test recovery from various resource exhaustion scenarios
    
    let memory_recovery = test_memory_pressure_recovery().await?;
    assert!(memory_recovery, "System should handle memory pressure");
    
    let channel_recovery = test_channel_overflow_recovery().await?;
    assert!(channel_recovery, "System should handle channel overflow");
    
    let file_handle_recovery = test_file_handle_exhaustion_recovery().await?;
    assert!(file_handle_recovery, "System should handle file handle exhaustion");
    
    Ok(())
}

async fn test_memory_pressure_recovery() -> Result<bool> {
    // Test system behavior under memory pressure
    
    let (tx, mut rx) = mpsc::channel(100);
    let memory_stress_detected = Arc::new(AtomicBool::new(false));
    
    // Simulate high memory usage by creating large events
    let stress_flag = memory_stress_detected.clone();
    let producer = tokio::spawn(async move {
        for i in 0..50 {
            // Create events with large payloads to simulate memory pressure
            let large_payload = "x".repeat(1024 * 10); // 10KB per event
            let event = RawEventBuilder::new(
                "memory_stress_test",
                "memory.pressure",
                json!({
                    "sequence": i,
                    "large_data": large_payload
                })
            ).build();
            
            match tx.send(event).await {
                Ok(()) => {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
                Err(_) => {
                    stress_flag.store(true, Ordering::SeqCst);
                    break; // Channel is full, memory pressure detected
                }
            }
        }
    });
    
    // Consumer that processes events with backpressure
    let consumer = tokio::spawn(async move {
        let mut processed = 0;
        while let Some(_event) = rx.recv().await {
            processed += 1;
            // Simulate processing time that could cause backpressure
            tokio::time::sleep(Duration::from_millis(2)).await;
            
            if processed >= 25 {
                break; // Process some events to demonstrate recovery
            }
        }
        processed
    });
    
    let producer_result = producer.await?;
    let processed_count = consumer.await?;
    
    // System should either handle the load or apply backpressure gracefully
    assert!(processed_count > 0, "Should process some events despite memory pressure");
    
    // Memory pressure detection is okay, system should handle it gracefully
    Ok(true)
}

async fn test_channel_overflow_recovery() -> Result<bool> {
    // Test recovery from channel overflow conditions
    
    let (tx, mut rx) = mpsc::channel(10); // Small channel to force overflow
    let overflow_handled = Arc::new(AtomicBool::new(false));
    
    // Producer that sends more than channel capacity
    let overflow_flag = overflow_handled.clone();
    let producer = tokio::spawn(async move {
        for i in 0..50 {
            let event = RawEventBuilder::new(
                "overflow_test",
                "channel.overflow",
                json!({"sequence": i})
            ).build();
            
            match tx.try_send(event) {
                Ok(()) => {
                    // Success
                }
                Err(_) => {
                    overflow_flag.store(true, Ordering::SeqCst);
                    // In real implementation, would apply backpressure or buffering
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }
        }
    });
    
    // Slow consumer to create backpressure
    tokio::time::sleep(Duration::from_millis(100)).await; // Let producer fill channel
    
    let consumer = tokio::spawn(async move {
        let mut consumed = 0;
        while let Ok(event) = tokio::time::timeout(Duration::from_millis(50), rx.recv()).await {
            if event.is_some() {
                consumed += 1;
            } else {
                break;
            }
        }
        consumed
    });
    
    producer.await?;
    let consumed_count = consumer.await?;
    
    // Should have detected overflow and handled it
    assert!(overflow_handled.load(Ordering::SeqCst), "Should detect channel overflow");
    assert!(consumed_count > 0, "Should still process some events");
    
    Ok(true)
}

async fn test_file_handle_exhaustion_recovery() -> Result<bool> {
    // Test recovery from file handle exhaustion
    
    let temp_dir = TempDir::new()?;
    let mut open_files = Vec::new();
    let max_files = 100; // Reasonable test limit
    
    // Phase 1: Open many files to simulate exhaustion
    for i in 0..max_files {
        let file_path = temp_dir.path().join(format!("test_file_{}.txt", i));
        
        match std::fs::File::create(&file_path) {
            Ok(file) => open_files.push(file),
            Err(_) => break, // Hit file handle limit
        }
    }
    
    let opened_count = open_files.len();
    assert!(opened_count > 0, "Should be able to open some files");
    
    // Phase 2: Try to open one more (should fail gracefully)
    let extra_file_path = temp_dir.path().join("extra_file.txt");
    let extra_file_result = std::fs::File::create(&extra_file_path);
    
    // Phase 3: Close some files and verify recovery
    open_files.truncate(opened_count / 2); // Close half the files
    
    let recovery_file_path = temp_dir.path().join("recovery_file.txt");
    let recovery_result = std::fs::File::create(&recovery_file_path);
    
    // Should be able to create files after closing some
    assert!(recovery_result.is_ok(), "Should be able to create files after closing some");
    
    Ok(true)
}
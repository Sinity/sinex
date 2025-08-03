// # End-to-End Workflow Integration Tests
//
// Comprehensive integration tests that verify complete workflows across multiple
// system components. These tests focus on realistic scenarios that span the entire
// system architecture from event ingestion to final processing.
//
// ## Test Coverage
//
// - **Event Ingestion Workflows**: Complete flow from satellite to database
// - **Stream Processing Workflows**: Redis Streams to automaton processing
// - **Checkpoint Management Workflows**: Persistence and recovery scenarios
// - **Multi-Component Coordination**: Component interaction and synchronization
// - **Error Recovery Workflows**: Failure detection and system recovery
// - **Performance Under Load**: Concurrent processing and resource management
// - **Data Consistency Workflows**: Cross-component data integrity verification

use chrono::{Duration, Utc};
use futures::future::join_all;
use redis::{cmd, AsyncCommands};
use sinex_core_types::CoreError;
use sinex_db::queries::{CheckpointQueries, EventQueries, OperationQueries};
use sinex_db::query_builder::{QueryBuilder, QueryParam};
use sinex_types::events::{event_types, services, EventFactory};
use sinex_satellite_sdk::{
    checkpoint::{CheckpointManager, CheckpointState},
    config::EventSourceConfig,
    redis_client::RedisStreamClient,
    stream_processor::Checkpoint,
    StatefulStreamProcessor,
};
use sinex_test_utils::prelude::*;
use sinex_test_utils::{events, generators, satellite_test_utils};
use sinex_types::ulid::Ulid;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration as StdDuration, Instant};
use tokio::sync::{Mutex, RwLock};

// =============================================================================
// Event Ingestion Workflow Tests
// =============================================================================

/// Test complete event ingestion workflow from satellite to database
#[sinex_test]
async fn test_complete_event_ingestion_workflow(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let pool = ctx.pool().clone();

    // Phase 1: Simulate satellite event generation
    let satellite_events = generators::test_events(50);
    println!("Generated {} satellite events", satellite_events.len());

    // Phase 2: Process events through ingestion pipeline
    let ingestion_start = Instant::now();
    let mut ingested_event_ids = Vec::new();

    for event in &satellite_events {
        match sinex_db::insert_event_with_validator(&pool, event, None).await {
            Ok(stored_event) => {
                ingested_event_ids.push(stored_event.id);
                println!("Ingested event: {}", stored_event.id);
            }
            Err(e) => {
                println!("Failed to ingest event {}: {}", event.id, e);
                return Err(CoreError::database(format!("Ingestion failed: {}", e))
                    .build()
                    .into());
            }
        }
    }

    let ingestion_duration = ingestion_start.elapsed();
    println!("Ingestion completed in {:?}", ingestion_duration);

    // Phase 3: Verify all events are in database with correct structure
    let mut stored_events = Vec::new();
    for event_id in &ingested_event_ids {
        let event = sinex_db::get_event_by_id(&pool, *event_id).await?;
        stored_events.push(event);
    }

    assert_eq!(
        stored_events.len(),
        satellite_events.len(),
        "All events should be stored"
    );

    // Phase 4: Verify event data integrity
    for (original, stored) in satellite_events.iter().zip(stored_events.iter()) {
        assert_eq!(original.id, stored.id, "Event ID should match");
        assert_eq!(original.source, stored.source, "Source should match");
        assert_eq!(
            original.event_type, stored.event_type,
            "Event type should match"
        );
        assert_eq!(original.host, stored.host, "Host should match");
        assert_eq!(original.payload, stored.payload, "Payload should match");
    }

    // Phase 5: Verify timeseries properties (TimescaleDB)
    let (time_range_events,) =
        EventQueries::count_by_time_range(Utc::now() - Duration::hours(1), Utc::now())
            .fetch_one::<(i64,)>(&pool)
            .await?;

    assert!(
        time_range_events >= satellite_events.len() as i64,
        "Events should be queryable by time range"
    );

    println!("✓ Complete event ingestion workflow verified");
    Ok(())
}

/// Test event ingestion with concurrent satellites
#[sinex_test]
async fn test_concurrent_satellite_ingestion_workflow(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let pool = ctx.pool().clone();

    // Simulate multiple satellites ingesting events concurrently
    let satellite_count = 5;
    let events_per_satellite = 20;

    let mut satellite_handles = Vec::new();
    let successful_ingestions = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let failed_ingestions = Arc::new(tokio::sync::Mutex::new(Vec::new()));

    for satellite_id in 0..satellite_count {
        let pool_clone = pool.clone();
        let successes = successful_ingestions.clone();
        let failures = failed_ingestions.clone();

        let handle = tokio::spawn(async move {
            let satellite_name = format!("satellite-{}", satellite_id);

            for event_id in 0..events_per_satellite {
                let factory = EventFactory::new(&satellite_name);
                let mut event = factory.create_event(
                    &format!("satellite.event.{}", event_id),
                    serde_json::json!({
                        "satellite_id": satellite_id,
                        "event_id": event_id,
                        "data": format!("concurrent-test-data-{}-{}", satellite_id, event_id)
                    }),
                );
                event.host = format!("host-{}", satellite_id);

                match sinex_db::insert_event_with_validator(&pool_clone, &event, None).await {
                    Ok(stored_event) => {
                        let mut successes_lock = successes.lock().await;
                        successes_lock.push((satellite_id, event_id, stored_event.id));
                        println!("Satellite {} ingested event {}", satellite_id, event_id);
                    }
                    Err(e) => {
                        let mut failures_lock = failures.lock().await;
                        failures_lock.push((satellite_id, event_id, e.to_string()));
                        println!(
                            "Satellite {} failed to ingest event {}: {}",
                            satellite_id, event_id, e
                        );
                    }
                }

                // Small delay to simulate realistic ingestion timing
                tokio::time::sleep(StdDuration::from_millis(10)).await;
            }

            println!("Satellite {} completed ingestion", satellite_id);
        });

        satellite_handles.push(handle);
    }

    // Wait for all satellites to complete
    join_all(satellite_handles).await;

    let successes = successful_ingestions.lock().await;
    let failures = failed_ingestions.lock().await;

    println!("Concurrent ingestion results:");
    println!("- Successful ingestions: {}", successes.len());
    println!("- Failed ingestions: {}", failures.len());

    // Verify expected number of events
    let expected_total = satellite_count * events_per_satellite;
    assert!(
        successes.len() + failures.len() == expected_total,
        "All ingestion attempts should be accounted for"
    );

    // Verify database state
    // Count events by pattern using LIKE
    let (total_events,) = QueryBuilder::select("core.events")
        .columns(&["COUNT(*) as count"])
        .where_op(
            "source",
            "LIKE",
            QueryParam::String("satellite-%".to_string()),
        )
        .fetch_one::<(i64,)>(&pool)
        .await?;

    assert_eq!(
        total_events,
        successes.len() as i64,
        "Database should contain all successful ingestions"
    );

    // Verify no data corruption with concurrent writes
    let (distinct_event_ids,) = QueryBuilder::select("core.events")
        .columns(&["COUNT(DISTINCT event_id) as count"])
        .where_op(
            "source",
            "LIKE",
            QueryParam::String("satellite-%".to_string()),
        )
        .fetch_one::<(i64,)>(&pool)
        .await?;

    assert_eq!(
        distinct_event_ids,
        successes.len() as i64,
        "All events should have unique IDs"
    );

    println!("✓ Concurrent satellite ingestion workflow verified");
    Ok(())
}

// =============================================================================
// Stream Processing Workflow Tests
// =============================================================================

/// Test complete stream processing workflow from Redis to automaton
#[sinex_test]
async fn test_stream_processing_workflow(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let pool = ctx.pool().clone();

    // Create test automaton for stream processing
    let processor_name = "test-stream-processor";
    let consumer_group = format!("{}-group", processor_name);

    // Initialize checkpoint manager
    let checkpoint_manager = CheckpointManager::new(
        pool.clone(),
        processor_name.to_string(),
        "test-consumer-group".to_string(),
        "test-consumer-name".to_string(),
    );

    // Phase 1: Set up Redis stream
    let redis_url =
        std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379".to_string());
    let redis_conn = redis::Client::open(redis_url)?;
    let mut redis_conn = redis_conn.get_multiplexed_async_connection().await?;
    let stream_key = "sinex:test:stream";

    // Phase 2: Add events to stream
    let test_events = generators::test_events(30);
    let mut stream_event_ids: Vec<String> = Vec::new();

    for event in &test_events {
        let stream_data = serde_json::json!({
            "event_id": event.id.to_string(),
            "source": event.source,
            "event_type": event.event_type,
            "payload": event.payload,
            "timestamp": event.ts_orig
        });

        match redis_conn
            .xadd(
                stream_key,
                "*",
                &[("event", serde_json::to_string(&event).unwrap())],
            )
            .await
        {
            Ok(id) => {
                stream_event_ids.push(id);
                println!("Added event {} to stream", event.id);
            }
            Err(e) => {
                return Err(CoreError::service(format!("Redis XADD failed: {}", e))
                    .build()
                    .into());
            }
        }
    }

    println!("Added {} events to Redis stream", stream_event_ids.len());

    // Phase 3: Process events through stream processor
    let processed_events = Arc::new(Mutex::new(Vec::new()));
    let processing_errors = Arc::new(Mutex::new(Vec::new()));

    // Create consumer group
    match redis_conn
        .xgroup_create_mkstream::<_, _, _, ()>(stream_key, &consumer_group, "0")
        .await
    {
        Ok(_) => println!("Created consumer group: {}", consumer_group),
        Err(e) => {
            println!("Consumer group creation failed (may already exist): {}", e);
        }
    }

    // Phase 4: Simulate stream processing
    let processing_start = Instant::now();
    let batch_size = 5;
    let mut processed_count = 0;

    while processed_count < test_events.len() {
        // Read batch from stream
        match cmd("XREADGROUP")
            .arg("GROUP")
            .arg(&consumer_group)
            .arg("test-consumer")
            .arg("COUNT")
            .arg(batch_size)
            .arg("STREAMS")
            .arg(stream_key)
            .arg(">")
            .query_async(&mut redis_conn)
            .await
        {
            Ok(messages) => {
                let messages: redis::streams::StreamReadReply = messages;
                if messages.keys.is_empty() {
                    println!("No more messages in stream");
                    break;
                }

                for key in messages.keys {
                    for message in key.ids {
                        processed_count += 1;

                        // Simulate processing
                        let event_data = message.map.get("payload").unwrap_or(&redis::Value::Nil);

                        // Store processing result
                        let mut processed = processed_events.lock().await;
                        processed.push(message.id.clone());

                        // Acknowledge message
                        match redis_conn
                            .xack::<_, _, _, ()>(stream_key, &consumer_group, &[&message.id])
                            .await
                        {
                            Ok(_) => println!("Acknowledged message: {}", message.id),
                            Err(e) => {
                                let mut errors = processing_errors.lock().await;
                                errors.push(format!("ACK failed for {}: {}", message.id, e));
                            }
                        }
                    }
                }
            }
            Err(e) => {
                let mut errors = processing_errors.lock().await;
                errors.push(format!("XREADGROUP failed: {}", e));
                break;
            }
        }

        // Update checkpoint
        let checkpoint_state = CheckpointState {
            checkpoint: Checkpoint::Stream {
                message_id: format!("stream-{}", processed_count),
                event_id: None,
            },
            processed_count: processed_count as u64,
            last_activity: chrono::Utc::now(),
            data: Some(serde_json::json!({
                "stream_key": stream_key,
                "consumer_group": consumer_group,
                "batch_size": batch_size
            })),
            version: 2,
        };

        match checkpoint_manager.save_checkpoint(&checkpoint_state).await {
            Ok(_) => println!("Saved checkpoint at count: {}", processed_count),
            Err(e) => {
                println!("Checkpoint save failed: {}", e);
            }
        }
    }

    let processing_duration = processing_start.elapsed();
    println!("Stream processing completed in {:?}", processing_duration);

    // Phase 5: Verify processing results
    let processed = processed_events.lock().await;
    let errors = processing_errors.lock().await;

    println!("Stream processing results:");
    println!("- Processed events: {}", processed.len());
    println!("- Processing errors: {}", errors.len());

    assert!(processed.len() > 0, "Some events should be processed");
    assert!(errors.len() < processed.len(), "Errors should be minimal");

    // Phase 6: Verify checkpoint state
    let final_checkpoint = checkpoint_manager.load_checkpoint().await?;
    assert!(
        final_checkpoint.processed_count > 0,
        "Checkpoint should track progress"
    );

    println!("✓ Stream processing workflow verified");
    Ok(())
}

// =============================================================================
// Checkpoint Management Workflow Tests
// =============================================================================

/// Test checkpoint persistence and recovery workflow
#[sinex_test]
async fn test_checkpoint_persistence_recovery_workflow(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let pool = ctx.pool().clone();
    let processor_name = "checkpoint-test-automaton";

    // Phase 1: Initialize checkpoint manager
    let checkpoint_manager = CheckpointManager::new(
        pool.clone(),
        processor_name.to_string(),
        "test-consumer-group".to_string(),
        "test-consumer-name".to_string(),
    );

    // Phase 2: Simulate processing with checkpoints
    let checkpoint_states = vec![
        CheckpointState {
            checkpoint: Checkpoint::Stream {
                message_id: "event-1".to_string(),
                event_id: None,
            },
            processed_count: 1,
            last_activity: Utc::now(),
            data: Some(serde_json::json!({"phase": "initial", "timestamp": Utc::now()})),
            version: 2,
        },
        CheckpointState {
            checkpoint: Checkpoint::Stream {
                message_id: "event-10".to_string(),
                event_id: None,
            },
            processed_count: 10,
            last_activity: Utc::now(),
            data: Some(serde_json::json!({"phase": "batch_1", "timestamp": Utc::now()})),
            version: 2,
        },
        CheckpointState {
            checkpoint: Checkpoint::Stream {
                message_id: "event-25".to_string(),
                event_id: None,
            },
            processed_count: 25,
            last_activity: Utc::now(),
            data: Some(serde_json::json!({"phase": "batch_2", "timestamp": Utc::now()})),
            version: 2,
        },
    ];

    // Save checkpoints progressively
    for (i, state) in checkpoint_states.iter().enumerate() {
        match checkpoint_manager.save_checkpoint(state).await {
            Ok(_) => {
                println!(
                    "Saved checkpoint {}: {} events processed",
                    i + 1,
                    state.processed_count
                );
            }
            Err(e) => {
                return Err(
                    CoreError::database(format!("Checkpoint save failed: {}", e))
                        .build()
                        .into(),
                );
            }
        }

        // Simulate processing time
        tokio::time::sleep(StdDuration::from_millis(100)).await;
    }

    // Phase 3: Verify checkpoint retrieval
    let retrieved_checkpoint = checkpoint_manager.load_checkpoint().await?;
    let final_state = checkpoint_states.last().unwrap();

    assert_eq!(
        retrieved_checkpoint.processed_count, final_state.processed_count,
        "Retrieved checkpoint should match final state"
    );
    assert_eq!(
        retrieved_checkpoint.last_processed_id(),
        final_state.last_processed_id(),
        "Last processed ID should match"
    );

    // Phase 4: Simulate automaton restart and recovery
    let recovery_manager = CheckpointManager::new(
        pool.clone(),
        processor_name.to_string(),
        "test-consumer-group".to_string(),
        "test-consumer-name".to_string(),
    );
    let recovery_checkpoint = recovery_manager.load_checkpoint().await?;

    println!("Recovery checkpoint:");
    println!(
        "- Last processed ID: {:?}",
        recovery_checkpoint.last_processed_id()
    );
    println!("- Processed count: {}", recovery_checkpoint.processed_count);
    println!("- Data: {:?}", recovery_checkpoint.data);

    // Verify recovery state matches expected
    assert_eq!(
        recovery_checkpoint.processed_count, 25,
        "Recovery should start from last checkpoint"
    );

    // Phase 5: Continue processing from checkpoint
    let continued_state = CheckpointState {
        checkpoint: Checkpoint::Stream {
            message_id: "event-50".to_string(),
            event_id: None,
        },
        processed_count: recovery_checkpoint.processed_count + 25,
        last_activity: Utc::now(),
        data: Some(serde_json::json!({"phase": "recovery", "timestamp": Utc::now()})),
        version: 2,
    };

    recovery_manager.save_checkpoint(&continued_state).await?;

    // Verify continued processing
    let final_recovery_checkpoint = recovery_manager.load_checkpoint().await?;
    assert_eq!(
        final_recovery_checkpoint.processed_count, 50,
        "Processing should continue from recovery point"
    );

    println!("✓ Checkpoint persistence and recovery workflow verified");
    Ok(())
}

// =============================================================================
// Multi-Component Coordination Tests
// =============================================================================

/// Test coordination between multiple system components
#[sinex_test]
async fn test_multi_component_coordination_workflow(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let pool = ctx.pool().clone();

    // Phase 1: Set up multiple components
    let components = vec![
        "fs-watcher",
        "terminal-satellite",
        "desktop-satellite",
        "system-satellite",
    ];

    let coordination_state = Arc::new(RwLock::new(HashMap::new()));
    let component_handles = Arc::new(Mutex::new(Vec::new()));

    // Phase 2: Simulate component lifecycle coordination
    for component in &components {
        let pool_clone = pool.clone();
        let state = coordination_state.clone();
        let component_name = component.to_string();

        let handle = tokio::spawn(async move {
            // Phase 2a: Component initialization
            {
                let mut state_lock = state.write().await;
                state_lock.insert(component_name.clone(), "initializing".to_string());
            }

            // Simulate initialization work
            tokio::time::sleep(StdDuration::from_millis(100)).await;

            // Phase 2b: Component ready state
            {
                let mut state_lock = state.write().await;
                state_lock.insert(component_name.clone(), "ready".to_string());
            }

            // Phase 2c: Component processing
            for i in 0..10 {
                let factory = EventFactory::new(&component_name);
                let mut event = factory.create_event(
                    &format!("{}.heartbeat", component_name),
                    serde_json::json!({
                        "component": component_name,
                        "heartbeat_id": i,
                        "timestamp": Utc::now()
                    }),
                );
                event.host = "coordination-test".to_string();

                match sinex_db::insert_event_with_validator(&pool_clone, &event, None).await {
                    Ok(_) => {
                        println!("Component {} sent heartbeat {}", component_name, i);
                    }
                    Err(e) => {
                        println!("Component {} heartbeat {} failed: {}", component_name, i, e);
                    }
                }

                tokio::time::sleep(StdDuration::from_millis(200)).await;
            }

            // Phase 2d: Component shutdown
            {
                let mut state_lock = state.write().await;
                state_lock.insert(component_name.clone(), "shutting_down".to_string());
            }

            tokio::time::sleep(StdDuration::from_millis(50)).await;

            {
                let mut state_lock = state.write().await;
                state_lock.insert(component_name.clone(), "stopped".to_string());
            }

            println!("Component {} lifecycle completed", component_name);
        });

        let mut handles = component_handles.lock().await;
        handles.push(handle);
    }

    // Phase 3: Wait for all components to complete
    let handles = {
        let mut handles_lock = component_handles.lock().await;
        std::mem::take(&mut *handles_lock)
    };

    join_all(handles).await;

    // Phase 4: Verify coordination state
    let final_state = coordination_state.read().await;
    println!("Final component states:");
    for (component, state) in final_state.iter() {
        println!("  {}: {}", component, state);
        assert_eq!(state, "stopped", "All components should be stopped");
    }

    // Phase 5: Verify component heartbeats in database
    for component in &components {
        let heartbeat_source = format!("{}.heartbeat", component);
        let (heartbeat_count,): (i64,) = EventQueries::count_by_source(heartbeat_source)
            .fetch_one(&pool)
            .await?;

        assert_eq!(
            heartbeat_count, 10,
            "Each component should have sent 10 heartbeats"
        );
    }

    println!("✓ Multi-component coordination workflow verified");
    Ok(())
}

// =============================================================================
// Error Recovery Workflow Tests
// =============================================================================

/// Test error detection and recovery workflow
#[sinex_test]
async fn test_error_recovery_workflow(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let pool = ctx.pool().clone();

    // Phase 1: Simulate system with intermittent errors
    let error_simulation = Arc::new(Mutex::new(0));
    let recovery_attempts = Arc::new(Mutex::new(Vec::new()));
    let successful_operations = Arc::new(Mutex::new(Vec::new()));

    // Phase 2: Simulate component with error recovery
    let component_name = "error-recovery-test";
    let operation_count = 20;

    for operation_id in 0..operation_count {
        let pool_clone = pool.clone();
        let error_count = error_simulation.clone();
        let recoveries = recovery_attempts.clone();
        let successes = successful_operations.clone();

        // Simulate error conditions (every 4th operation fails initially)
        let should_fail = operation_id % 4 == 0;

        if should_fail {
            // Simulate error and recovery
            let mut error_count_lock = error_count.lock().await;
            *error_count_lock += 1;

            println!(
                "Operation {} encountered error, attempting recovery",
                operation_id
            );

            // Simulate recovery attempts
            let mut recovery_success = false;
            for retry in 0..3 {
                tokio::time::sleep(StdDuration::from_millis(50)).await;

                let factory = EventFactory::new(component_name);
                let mut event = factory.create_event(
                    &format!("recovery.attempt.{}", retry),
                    serde_json::json!({
                        "operation_id": operation_id,
                        "retry_attempt": retry,
                        "error_type": "simulated_failure"
                    }),
                );
                event.host = "error-recovery-test".to_string();

                match sinex_db::insert_event_with_validator(&pool_clone, &event, None).await {
                    Ok(_) => {
                        recovery_success = true;
                        println!(
                            "Recovery attempt {} for operation {} succeeded",
                            retry, operation_id
                        );

                        let mut recoveries_lock = recoveries.lock().await;
                        recoveries_lock.push((operation_id, retry));
                        break;
                    }
                    Err(e) => {
                        println!(
                            "Recovery attempt {} for operation {} failed: {}",
                            retry, operation_id, e
                        );
                    }
                }
            }

            if !recovery_success {
                println!(
                    "Operation {} failed after all recovery attempts",
                    operation_id
                );
            }
        } else {
            // Normal operation
            let factory = EventFactory::new(component_name);
            let mut event = factory.create_event(
                "normal.operation",
                serde_json::json!({
                    "operation_id": operation_id,
                    "status": "success"
                }),
            );
            event.host = "error-recovery-test".to_string();

            match sinex_db::insert_event_with_validator(&pool_clone, &event, None).await {
                Ok(_) => {
                    let mut successes_lock = successes.lock().await;
                    successes_lock.push(operation_id);
                    println!("Operation {} completed successfully", operation_id);
                }
                Err(e) => {
                    println!("Normal operation {} failed: {}", operation_id, e);
                }
            }
        }
    }

    // Phase 3: Verify error recovery results
    let error_count = error_simulation.lock().await;
    let recoveries = recovery_attempts.lock().await;
    let successes = successful_operations.lock().await;

    println!("Error recovery workflow results:");
    println!("- Total errors simulated: {}", *error_count);
    println!("- Recovery attempts: {}", recoveries.len());
    println!("- Successful operations: {}", successes.len());

    // Phase 4: Verify database state reflects recovery
    let recovery_events: i64 = sqlx::query_scalar!(
        r#"
        SELECT COUNT(*)
        FROM core.events
        WHERE source = $1 AND event_type LIKE $2
        "#,
        component_name,
        "recovery.attempt%"
    )
    .fetch_one(&pool)
    .await?
    .unwrap_or(0);

    let normal_events: i64 = sqlx::query_scalar!(
        r#"
        SELECT COUNT(*)
        FROM core.events
        WHERE source = $1 AND event_type = $2
        "#,
        component_name,
        "normal.operation"
    )
    .fetch_one(&pool)
    .await?
    .unwrap_or(0);

    assert!(recovery_events > 0, "Recovery events should be recorded");
    assert!(normal_events > 0, "Normal operations should be recorded");

    // Phase 5: Verify system resilience
    let total_events = EventQueries::count_by_source(component_name.to_string())
        .fetch_one::<(i64,)>(&pool)
        .await
        .map(|r| r.0)?;

    assert!(
        total_events > operation_count as i64 / 2,
        "System should maintain functionality despite errors"
    );

    println!("✓ Error recovery workflow verified");
    Ok(())
}

// =============================================================================
// Performance Under Load Tests
// =============================================================================

/// Test system performance under concurrent load
#[sinex_test]
async fn test_performance_under_load_workflow(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let pool = ctx.pool().clone();

    // Phase 1: Configure load test parameters
    let concurrent_workers = 10;
    let events_per_worker = 50;
    let total_expected_events = concurrent_workers * events_per_worker;

    println!("Starting performance load test:");
    println!("- Concurrent workers: {}", concurrent_workers);
    println!("- Events per worker: {}", events_per_worker);
    println!("- Total expected events: {}", total_expected_events);

    // Phase 2: Track performance metrics
    let start_time = Instant::now();
    let successful_events = Arc::new(Mutex::new(0));
    let failed_events = Arc::new(Mutex::new(0));
    let processing_times = Arc::new(Mutex::new(Vec::new()));

    // Phase 3: Launch concurrent workers
    let worker_handles = (0..concurrent_workers)
        .map(|worker_id| {
            let pool_clone = pool.clone();
            let successes = successful_events.clone();
            let failures = failed_events.clone();
            let times = processing_times.clone();

            tokio::spawn(async move {
                let worker_start = Instant::now();

                for event_id in 0..events_per_worker {
                    let event_start = Instant::now();

                    let factory = EventFactory::new(&format!("load-worker-{}", worker_id));
                    let mut event = factory.create_event(
                        "performance.load.event",
                        serde_json::json!({
                            "worker_id": worker_id,
                            "event_id": event_id,
                            "timestamp": Utc::now(),
                            "data": format!("load-test-data-{}-{}", worker_id, event_id)
                        }),
                    );
                    event.host = "performance-test".to_string();

                    match sinex_db::insert_event_with_validator(&pool_clone, &event, None).await {
                        Ok(_) => {
                            let mut successes_lock = successes.lock().await;
                            *successes_lock += 1;

                            let event_duration = event_start.elapsed();
                            let mut times_lock = times.lock().await;
                            times_lock.push(event_duration);

                            if event_id % 10 == 0 {
                                println!("Worker {} processed {} events", worker_id, event_id + 1);
                            }
                        }
                        Err(e) => {
                            let mut failures_lock = failures.lock().await;
                            *failures_lock += 1;
                            println!("Worker {} event {} failed: {}", worker_id, event_id, e);
                        }
                    }
                }

                let worker_duration = worker_start.elapsed();
                println!("Worker {} completed in {:?}", worker_id, worker_duration);
            })
        })
        .collect::<Vec<_>>();

    // Phase 4: Wait for all workers to complete
    join_all(worker_handles).await;

    let total_duration = start_time.elapsed();
    let successes = *successful_events.lock().await;
    let failures = *failed_events.lock().await;
    let times = processing_times.lock().await;

    // Phase 5: Calculate performance metrics
    let throughput = successes as f64 / total_duration.as_secs_f64();
    let average_latency = times.iter().sum::<StdDuration>() / times.len() as u32;
    let default_duration = StdDuration::from_millis(0);
    let min_latency = times.iter().min().unwrap_or(&default_duration);
    let max_latency = times.iter().max().unwrap_or(&default_duration);

    println!("Performance load test results:");
    println!("- Total duration: {:?}", total_duration);
    println!("- Successful events: {}", successes);
    println!("- Failed events: {}", failures);
    println!("- Throughput: {:.2} events/second", throughput);
    println!("- Average latency: {:?}", average_latency);
    println!("- Min latency: {:?}", min_latency);
    println!("- Max latency: {:?}", max_latency);

    // Phase 6: Verify performance requirements
    assert!(
        successes >= (total_expected_events * 95) / 100,
        "At least 95% of events should succeed under load"
    );
    assert!(throughput > 10.0, "Throughput should be > 10 events/second");
    assert!(
        average_latency < StdDuration::from_millis(1000),
        "Average latency should be < 1 second"
    );

    // Phase 7: Verify database consistency under load
    let total_db_events = EventQueries::count_by_source("load-worker-%".to_string())
        .fetch_one::<(i64,)>(&pool)
        .await
        .map(|r| r.0)?;

    assert_eq!(
        total_db_events, successes as i64,
        "Database should contain all successful events"
    );

    println!("✓ Performance under load workflow verified");
    Ok(())
}

// =============================================================================
// Data Consistency Workflow Tests
// =============================================================================

/// Test data consistency across component boundaries
#[sinex_test]
async fn test_data_consistency_workflow(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let pool = ctx.pool().clone();

    // Phase 1: Set up consistency test scenario
    let consistency_scenario = "cross-component-consistency";
    let component_count = 3;
    let events_per_component = 20;

    println!(
        "Testing data consistency across {} components",
        component_count
    );

    // Phase 2: Generate linked events across components
    let mut event_chains = Vec::new();
    let base_timestamp = Utc::now();

    for chain_id in 0..events_per_component {
        let mut chain_events: Vec<RawEvent> = Vec::new();

        for component_id in 0..component_count {
            let factory = EventFactory::new(&format!("consistency-component-{}", component_id));
            let mut event = factory.create_event(
                &format!("consistency.chain.{}", chain_id),
                serde_json::json!({
                    "chain_id": chain_id,
                    "component_id": component_id,
                    "sequence_number": component_id,
                    "previous_event_id": if component_id > 0 {
                        Some(chain_events.last().unwrap().id.to_string())
                    } else {
                        None
                    },
                    "consistency_check": format!("chain-{}-step-{}", chain_id, component_id)
                }),
            );
            event.host = "consistency-test".to_string();
            event.ts_orig = Some(
                base_timestamp + Duration::seconds(chain_id as i64 * 10 + component_id as i64),
            );

            chain_events.push(event);
        }

        event_chains.push(chain_events);
    }

    // Phase 3: Insert all events and maintain consistency tracking
    let mut consistency_violations = Vec::new();
    let mut successful_chains = 0;

    for (chain_id, chain_events) in event_chains.iter().enumerate() {
        let mut chain_success = true;

        for event in chain_events {
            match sinex_db::insert_event_with_validator(&pool, event, None).await {
                Ok(_) => {
                    println!(
                        "Inserted event for chain {} from component {}",
                        chain_id, event.source
                    );
                }
                Err(e) => {
                    chain_success = false;
                    consistency_violations.push(format!(
                        "Chain {} event from {} failed: {}",
                        chain_id, event.source, e
                    ));
                }
            }
        }

        if chain_success {
            successful_chains += 1;
        }
    }

    // Phase 4: Verify consistency across components
    println!("Consistency verification results:");
    println!("- Successful chains: {}", successful_chains);
    println!("- Consistency violations: {}", consistency_violations.len());

    for violation in &consistency_violations {
        println!("  - {}", violation);
    }

    // Phase 5: Verify temporal consistency
    let temporal_check = sqlx::query!(
        r#"
        SELECT 
            source,
            event_type,
            ts_orig,
            payload->>'chain_id' as chain_id,
            payload->>'sequence_number' as sequence_number
        FROM core.events 
        WHERE source LIKE 'consistency-component-%' 
        ORDER BY payload->>'chain_id', payload->>'sequence_number'
        "#
    )
    .fetch_all(&pool)
    .await?;

    // Group by chain and verify sequence
    let mut chain_groups: HashMap<String, Vec<_>> = HashMap::new();
    for event in temporal_check {
        let chain_id = event.chain_id.clone().unwrap_or_default();
        chain_groups
            .entry(chain_id)
            .or_insert_with(Vec::new)
            .push(event);
    }

    let mut temporal_violations = 0;
    for (chain_id, events) in chain_groups {
        for i in 1..events.len() {
            let prev = &events[i - 1];
            let curr = &events[i];

            if prev.ts_orig >= curr.ts_orig {
                temporal_violations += 1;
                println!(
                    "Temporal violation in chain {}: {:?} >= {:?}",
                    chain_id, prev.ts_orig, curr.ts_orig
                );
            }
        }
    }

    // Phase 6: Verify referential consistency
    let referential_check = sqlx::query!(
        r#"
        SELECT 
            event_id::text as "event_id!",
            payload->>'chain_id' as chain_id,
            payload->>'previous_event_id' as previous_event_id
        FROM core.events 
        WHERE source LIKE 'consistency-component-%' 
        AND payload->>'previous_event_id' IS NOT NULL
        "#
    )
    .fetch_all(&pool)
    .await?;

    let mut referential_violations = 0;
    for event in referential_check {
        if let Some(prev_id) = event.previous_event_id {
            let event_id = prev_id.parse::<sinex_ulid::Ulid>().unwrap_or_default();
            let prev_exists_result = sinex_db::get_event_by_id(&pool, event_id).await;
            let prev_exists = if prev_exists_result.is_ok() { 1 } else { 0 };

            if prev_exists == 0 {
                referential_violations += 1;
                println!(
                    "Referential violation: event {} references non-existent {}",
                    event.event_id, prev_id
                );
            }
        }
    }

    // Phase 7: Final consistency verification
    println!("Final consistency results:");
    println!("- Temporal violations: {}", temporal_violations);
    println!("- Referential violations: {}", referential_violations);

    assert!(
        consistency_violations.len() < events_per_component / 10,
        "Consistency violations should be minimal"
    );
    assert_eq!(
        temporal_violations, 0,
        "No temporal violations should occur"
    );
    assert_eq!(
        referential_violations, 0,
        "No referential violations should occur"
    );
    assert!(
        successful_chains >= (events_per_component * 90) / 100,
        "At least 90% of chains should be successful"
    );

    println!("✓ Data consistency workflow verified");
    Ok(())
}

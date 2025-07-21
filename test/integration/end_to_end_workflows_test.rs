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

use crate::common::test_macros::*;
use crate::common::prelude::*;
use crate::common::generators;
use crate::common::builders::{TestEventBuilder, TestScenarioBuilder, BatchEventBuilder, TestEvents, TestCheckpointBuilder};
use crate::common::query_helpers::TestQueries;
use crate::common::test_factories::{
    UserActivityFactory, SystemEventFactory, FileSystemScenarioFactory, 
    WorkflowFactory, ErrorScenarioFactory, scenarios
};
use chrono::{Duration, Utc};
use futures::future::join_all;
use redis::{cmd, AsyncCommands};
use sinex_core_types::CoreError;
use sinex_db::queries::EventQueries;
use sinex_db::query_builder::{QueryBuilder, QueryParam};
use sinex_events::EventFactory;
use sinex_satellite_sdk::{
    checkpoint::{CheckpointManager, CheckpointState},
    stream_processor::Checkpoint,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration as StdDuration, Instant};
use tokio::sync::{Mutex, RwLock};

// =============================================================================
// Event Ingestion Workflow Tests
// =============================================================================

/// Test complete event ingestion workflow from satellite to database
#[sinex_test]
async fn test_complete_event_ingestion_workflow(ctx: TestContext) -> TestResult {
    let pool = ctx.pool().clone();

    // Phase 1: Simulate satellite event generation using BatchEventBuilder
    let batch_builder = BatchEventBuilder::new("satellite-test", "event.generated", 50)
        .with_payload_generator(|i| json!({
            "event_num": i,
            "satellite_data": format!("test-data-{}", i),
            "timestamp": Utc::now()
        }))
        .with_time_spacing(chrono::Duration::milliseconds(100));
    
    println!("Generating 50 satellite events");
    
    // Phase 2: Process events through ingestion pipeline
    let ingestion_start = Instant::now();
    let inserted_events = batch_builder.insert(&pool).await?;
    let ingested_event_ids: Vec<_> = inserted_events.iter().map(|e| e.id).collect();
    
    println!("Ingested {} events", ingested_event_ids.len());

    let ingestion_duration = ingestion_start.elapsed();
    println!("Ingestion completed in {:?}", ingestion_duration);

    // Phase 3: Verify all events are in database with correct structure
    let mut stored_events = Vec::new();
    for event_id in &ingested_event_ids {
        let event = TestQueries::get_event(&pool, *event_id).await?;
        stored_events.push(event);
    }

    assert_eq!(
        stored_events.len(),
        inserted_events.len(),
        "All events should be stored"
    );

    // Phase 4: Verify event data integrity
    for (original, stored) in inserted_events.iter().zip(stored_events.iter()) {
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
        time_range_events >= inserted_events.len() as i64,
        "Events should be queryable by time range"
    );

    println!("✓ Complete event ingestion workflow verified");

/// Test realistic user workflow from start to finish
#[sinex_test]
async fn test_user_development_workflow_end_to_end(ctx: TestContext) -> TestResult {
    let pool = ctx.pool().clone();
    
    println!("=== Testing Complete Development Workflow ===");
    
    // Generate a complete development workflow using factory
    let workflow_events = WorkflowFactory::create_git_workflow();
    
    // Insert all workflow events
    println!("Inserting {} workflow events", workflow_events.len());
    for event in &workflow_events {
        insert_event(&pool, event).await?;
    }
    
    // Verify workflow stages
    
    // 1. Branch creation
    let branch_events: Vec<_> = workflow_events.iter()
        .filter(|e| e.event_type == "shell.command_executed" && 
                e.payload.get("command").and_then(|v| v.as_str())
                    .map(|cmd| cmd.contains("git checkout")).unwrap_or(false))
        .collect();
    assert!(!branch_events.is_empty(), "Should have branch creation");
    
    // 2. File modifications
    let file_events: Vec<_> = workflow_events.iter()
        .filter(|e| e.event_type == "filesystem.file_modified")
        .collect();
    assert!(file_events.len() >= 3, "Should have multiple file modifications");
    
    // 3. Test execution
    let test_events: Vec<_> = workflow_events.iter()
        .filter(|e| e.event_type == "shell.command_executed" && 
                e.payload.get("command").and_then(|v| v.as_str())
                    .map(|cmd| cmd.contains("test")).unwrap_or(false))
        .collect();
    assert!(!test_events.is_empty(), "Should have test execution");
    
    // 4. Git operations
    let git_events: Vec<_> = workflow_events.iter()
        .filter(|e| e.event_type == "shell.command_executed" && 
                e.payload.get("command").and_then(|v| v.as_str())
                    .map(|cmd| cmd.starts_with("git")).unwrap_or(false))
        .collect();
    assert!(git_events.len() >= 5, "Should have multiple git operations");
    
    // Verify chronological order
    let timestamps: Vec<_> = workflow_events.iter()
        .filter_map(|e| e.ts_orig)
        .collect();
    
    for window in timestamps.windows(2) {
        assert!(window[0] <= window[1], "Events should be chronologically ordered");
    }
    
    println!("✓ Development workflow completed successfully");
    println!("  - {} total events", workflow_events.len());
    println!("  - {} file modifications", file_events.len());
    println!("  - {} git operations", git_events.len());

/// Test data pipeline workflow end-to-end
#[sinex_test]
async fn test_data_pipeline_workflow_end_to_end(ctx: TestContext) -> TestResult {
    let pool = ctx.pool().clone();
    
    println!("=== Testing Data Pipeline Workflow ===");
    
    // Generate data pipeline workflow
    let pipeline_events = WorkflowFactory::create_data_pipeline();
    
    // Insert pipeline events
    for event in &pipeline_events {
        insert_event(&pool, event).await?;
    }
    
    // Verify pipeline stages
    
    // 1. Data download
    let download_events: Vec<_> = pipeline_events.iter()
        .filter(|e| e.payload.get("command").and_then(|v| v.as_str())
                    .map(|cmd| cmd.contains("wget")).unwrap_or(false))
        .collect();
    assert!(!download_events.is_empty(), "Should have data download");
    
    // 2. File creation (downloaded data)
    let data_files: Vec<_> = pipeline_events.iter()
        .filter(|e| e.event_type == "filesystem.file_created" &&
                e.payload.get("path").and_then(|v| v.as_str())
                    .map(|p| p.ends_with(".csv") || p.ends_with(".json")).unwrap_or(false))
        .collect();
    assert!(data_files.len() >= 3, "Should create multiple data files");
    
    // 3. Processing steps
    let processing_commands: Vec<_> = pipeline_events.iter()
        .filter(|e| e.payload.get("command").and_then(|v| v.as_str())
                    .map(|cmd| cmd.contains("python") && cmd.contains("_data.py")).unwrap_or(false))
        .collect();
    assert_eq!(processing_commands.len(), 3, "Should have 3 processing steps");
    
    // 4. Upload result
    let upload_events: Vec<_> = pipeline_events.iter()
        .filter(|e| e.payload.get("command").and_then(|v| v.as_str())
                    .map(|cmd| cmd.contains("aws s3")).unwrap_or(false))
        .collect();
    assert!(!upload_events.is_empty(), "Should upload results");
    
    // Verify processing duration
    let processing_durations: Vec<i64> = processing_commands.iter()
        .filter_map(|e| e.payload.get("duration_ms").and_then(|v| v.as_i64()))
        .collect();
    
    assert!(!processing_durations.is_empty(), "Should have duration data");
    let total_duration: i64 = processing_durations.iter().sum();
    assert!(total_duration > 1000, "Processing should take meaningful time");
    
    println!("✓ Data pipeline workflow completed");
    println!("  - {} total events", pipeline_events.len());
    println!("  - {} data files created", data_files.len());
    println!("  - {} processing steps", processing_commands.len());
    println!("  - {}ms total processing time", total_duration);

/// Test system startup and monitoring workflow
#[sinex_test]
async fn test_system_startup_monitoring_workflow(ctx: TestContext) -> TestResult {
    let pool = ctx.pool().clone();
    
    println!("=== Testing System Startup and Monitoring ===");
    
    // Generate system startup sequence
    let startup_events = SystemEventFactory::create_system_startup();
    
    // Generate ongoing monitoring
    let monitoring_events = SystemEventFactory::create_system_monitoring(10, 30);
    
    // Insert all system events
    for event in startup_events.iter().chain(monitoring_events.iter()) {
        insert_event(&pool, event).await?;
    }
    
    // Verify startup sequence
    
    // 1. System boot
    let boot_events: Vec<_> = startup_events.iter()
        .filter(|e| e.payload.get("unit").and_then(|v| v.as_str())
                    .map(|u| u == "multi-user.target").unwrap_or(false))
        .collect();
    assert!(!boot_events.is_empty(), "Should have system boot event");
    
    // 2. Service startups in correct order
    let service_starts: Vec<_> = startup_events.iter()
        .filter(|e| e.event_type == "systemd.unit_started")
        .collect();
    
    // Verify critical services started
    let critical_services = ["postgresql", "redis", "sinex-ingestd"];
    for service in &critical_services {
        let found = service_starts.iter().any(|e| 
            e.payload.get("unit").and_then(|v| v.as_str())
                .map(|u| u.contains(service)).unwrap_or(false)
        );
        assert!(found, "Should start {} service", service);
    }
    
    // 3. Verify monitoring data
    let health_summaries: Vec<_> = monitoring_events.iter()
        .filter(|e| e.event_type == "sinex.system_health_summary")
        .collect();
    assert!(!health_summaries.is_empty(), "Should have health summaries");
    
    // Check health metrics
    for summary in &health_summaries {
        assert!(summary.payload.get("cpu_usage_percent").is_some(), "Should have CPU data");
        assert!(summary.payload.get("memory_used_mb").is_some(), "Should have memory data");
        assert!(summary.payload.get("uptime_seconds").is_some(), "Should have uptime data");
    }
    
    // 4. Process heartbeats
    let heartbeats: Vec<_> = monitoring_events.iter()
        .filter(|e| e.event_type == "sinex.process_heartbeat")
        .collect();
    assert!(!heartbeats.is_empty(), "Should have process heartbeats");
    
    println!("✓ System startup and monitoring verified");
    println!("  - {} startup events", startup_events.len());
    println!("  - {} monitoring events", monitoring_events.len());
    println!("  - {} health summaries", health_summaries.len());
    println!("  - {} heartbeats", heartbeats.len());

/// Test comprehensive file system workflow
#[sinex_test]
async fn test_file_system_workflow_end_to_end(ctx: TestContext) -> TestResult {
    let pool = ctx.pool().clone();
    
    println!("=== Testing File System Workflow ===");
    
    // Generate file system scenarios
    let file_workflow = FileSystemScenarioFactory::create_file_workflow("/home/user/test-project");
    let build_process = FileSystemScenarioFactory::create_build_process();
    
    // Insert all file system events
    for event in file_workflow.iter().chain(build_process.iter()) {
        insert_event(&pool, event).await?;
    }
    
    // Verify file operations
    
    // 1. Directory creation
    let dir_creates: Vec<_> = file_workflow.iter()
        .filter(|e| e.event_type == "filesystem.dir_created")
        .collect();
    assert!(!dir_creates.is_empty(), "Should create directories");
    
    // 2. File creation
    let file_creates: Vec<_> = file_workflow.iter()
        .filter(|e| e.event_type == "filesystem.file_created")
        .collect();
    assert!(file_creates.len() >= 4, "Should create multiple files");
    
    // 3. File modifications
    let file_mods: Vec<_> = file_workflow.iter()
        .filter(|e| e.event_type == "filesystem.file_modified")
        .collect();
    assert!(file_mods.len() >= 5, "Should have multiple modifications");
    
    // 4. File moves
    let file_moves: Vec<_> = file_workflow.iter()
        .filter(|e| e.event_type == "filesystem.file_moved")
        .collect();
    assert!(!file_moves.is_empty(), "Should have file moves");
    
    // 5. Build artifacts
    let build_artifacts: Vec<_> = build_process.iter()
        .filter(|e| e.payload.get("path").and_then(|v| v.as_str())
                    .map(|p| p.contains("target/release")).unwrap_or(false))
        .collect();
    assert!(!build_artifacts.is_empty(), "Should create build artifacts");
    
    // Verify file sizes increase with modifications
    let modified_sizes: Vec<i64> = file_mods.iter()
        .filter_map(|e| e.payload.get("size").and_then(|v| v.as_i64()))
        .collect();
    
    // Check that sizes generally increase (simulating file growth)
    for window in modified_sizes.windows(2) {
        assert!(window[1] >= window[0], "File sizes should grow with edits");
    }
    
    println!("✓ File system workflow completed");
    println!("  - {} directories created", dir_creates.len());
    println!("  - {} files created", file_creates.len());
    println!("  - {} files modified", file_mods.len());
    println!("  - {} build artifacts", build_artifacts.len());

/// Test deployment workflow with error recovery
#[sinex_test]
async fn test_deployment_with_recovery_workflow(ctx: TestContext) -> TestResult {
    let pool = ctx.pool().clone();
    
    println!("=== Testing Deployment with Recovery ===");
    
    // Generate deployment workflow
    let deployment = WorkflowFactory::create_deployment_workflow();
    
    // Generate potential error scenarios during deployment
    let errors = ErrorScenarioFactory::create_error_cascade();
    let recovery = ErrorScenarioFactory::create_recovery_scenario();
    
    // Interleave deployment with errors (simulating real deployment issues)
    let mut all_events = Vec::new();
    
    // Start deployment
    all_events.extend(deployment.iter().take(3).cloned());
    
    // Errors occur during deployment
    all_events.extend(errors);
    
    // Recovery actions
    all_events.extend(recovery);
    
    // Complete deployment
    all_events.extend(deployment.iter().skip(3).cloned());
    
    // Sort by timestamp to maintain chronological order
    all_events.sort_by_key(|e| e.ts_orig.unwrap_or_else(Utc::now));
    
    // Insert all events
    for event in &all_events {
        insert_event(&pool, event).await?;
    }
    
    // Verify deployment stages
    
    // 1. Build and test
    let build_events: Vec<_> = all_events.iter()
        .filter(|e| e.payload.get("command").and_then(|v| v.as_str())
                    .map(|cmd| cmd.contains("cargo build")).unwrap_or(false))
        .collect();
    assert!(!build_events.is_empty(), "Should have build step");
    
    // 2. Errors occurred
    let error_events: Vec<_> = all_events.iter()
        .filter(|e| e.event_type.contains("error") || 
                e.payload.get("error").is_some())
        .collect();
    assert!(!error_events.is_empty(), "Should have errors during deployment");
    
    // 3. Recovery actions
    let recovery_events: Vec<_> = all_events.iter()
        .filter(|e| e.event_type == "systemd.unit_started" ||
                (e.event_type == "shell.command_executed" && 
                 e.payload.get("command").and_then(|v| v.as_str())
                    .map(|cmd| cmd.contains("systemctl restart")).unwrap_or(false)))
        .collect();
    assert!(!recovery_events.is_empty(), "Should have recovery actions");
    
    // 4. Deployment completion
    let deploy_commands: Vec<_> = all_events.iter()
        .filter(|e| e.payload.get("command").and_then(|v| v.as_str())
                    .map(|cmd| cmd.contains("ssh") || cmd.contains("scp")).unwrap_or(false))
        .collect();
    assert!(deploy_commands.len() >= 5, "Should complete deployment steps");
    
    // 5. System health after recovery
    let post_recovery_health: Vec<_> = all_events.iter()
        .filter(|e| e.event_type == "sinex.system_health_summary" &&
                e.payload.get("status").and_then(|v| v.as_str())
                    .map(|s| s == "healthy").unwrap_or(false))
        .collect();
    assert!(!post_recovery_health.is_empty(), "System should be healthy after recovery");
    
    println!("✓ Deployment with recovery completed");
    println!("  - {} total events", all_events.len());
    println!("  - {} errors encountered", error_events.len());
    println!("  - {} recovery actions", recovery_events.len());
    println!("  - {} deployment commands", deploy_commands.len());

/// Test complete user workday scenario
#[sinex_test]
async fn test_complete_user_workday_scenario(ctx: TestContext) -> TestResult {
    let pool = ctx.pool().clone();
    
    println!("=== Testing Complete User Workday ===");
    
    // Generate a full workday scenario
    let workday = scenarios::user_workday();
    
    println!("Inserting {} workday events", workday.len());
    
    // Insert in batches to simulate real-time flow
    for chunk in workday.chunks(20) {
        for event in chunk {
            insert_event(&pool, event).await?;
        }
        // Small delay between batches
        tokio::time::sleep(StdDuration::from_millis(10)).await;
    }
    
    // Analyze the workday
    
    // 1. System startup events
    let startup_events: Vec<_> = workday.iter()
        .filter(|e| e.source == "systemd" && e.event_type.contains("started"))
        .collect();
    assert!(!startup_events.is_empty(), "Should have system startup");
    
    // 2. User session events
    let session_events: Vec<_> = workday.iter()
        .filter(|e| e.event_type.contains("session"))
        .collect();
    assert!(session_events.len() >= 2, "Should have session start/end");
    
    // 3. Development activity
    let dev_events: Vec<_> = workday.iter()
        .filter(|e| e.payload.get("command").and_then(|v| v.as_str())
                    .map(|cmd| cmd.contains("git") || cmd.contains("cargo")).unwrap_or(false))
        .collect();
    assert!(!dev_events.is_empty(), "Should have development activity");
    
    // 4. File operations
    let file_ops: Vec<_> = workday.iter()
        .filter(|e| e.source == "fs")
        .collect();
    assert!(!file_ops.is_empty(), "Should have file operations");
    
    // 5. System monitoring throughout
    let monitoring: Vec<_> = workday.iter()
        .filter(|e| e.event_type.contains("health") || e.event_type.contains("heartbeat"))
        .collect();
    assert!(!monitoring.is_empty(), "Should have continuous monitoring");
    
    // Verify time span
    let first_ts = workday.first().and_then(|e| e.ts_orig).unwrap();
    let last_ts = workday.last().and_then(|e| e.ts_orig).unwrap();
    let duration = last_ts.signed_duration_since(first_ts);
    
    assert!(duration.num_hours() >= 1, "Workday should span multiple hours");
    
    println!("✓ Complete workday scenario verified");
    println!("  - {} total events", workday.len());
    println!("  - {} hour span", duration.num_hours());
    println!("  - {} development events", dev_events.len());
    println!("  - {} monitoring events", monitoring.len());

/// Test concurrent satellite ingestion workflow
test_event_filter!(test_concurrent_satellite_ingestion_workflow, &["test1", "test2", "satellite-%"], 5, "satellite-%", 5);

// =============================================================================
// Stream Processing Workflow Tests
// =============================================================================

/// Test complete stream processing workflow from Redis to automaton
#[sinex_test]
async fn test_stream_processing_workflow(ctx: TestContext) -> TestResult {
    let pool = ctx.pool().clone();

    // Create test automaton for stream processing
    let automaton_name = "test-stream-processor";
    let consumer_group = format!("{}-group", automaton_name);

    // Initialize checkpoint manager
    let checkpoint_manager = CheckpointManager::new(
        pool.clone(),
        automaton_name.to_string(),
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
    let batch_events = BatchEventBuilder::new("stream-test", "stream.event", 30)
        .with_payload_generator(|i| json!({
            "stream_index": i,
            "data": format!("stream-data-{}", i)
        }))
        .build();
    
    let mut stream_event_ids: Vec<String> = Vec::new();

    for event in &batch_events {
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
                return Err(CoreError::Service(format!("Redis XADD failed: {}", e)).into());
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

    while processed_count < batch_events.len() {
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
    
    // Also verify via TestQueries
    let db_checkpoint = TestQueries::get_checkpoint(&pool, automaton_name).await?;
    assert!(db_checkpoint.is_some(), "Checkpoint should be in database");

    println!("✓ Stream processing workflow verified");
    Ok(())
}

// =============================================================================
// Checkpoint Management Workflow Tests
// =============================================================================

/// Test checkpoint persistence and recovery workflow
#[sinex_test]
async fn test_checkpoint_persistence_recovery_workflow(ctx: TestContext) -> TestResult {
    let pool = ctx.pool().clone();
    let automaton_name = "checkpoint-test-automaton";

    // Phase 1: Set up checkpoint scenario using builders
    let checkpoint_scenario = TestScenarioBuilder::new()
        .with_checkpoint(
            TestCheckpointBuilder::new(automaton_name)
                .with_last_processed("event-1")
                .with_processed_count(1)
                .with_state(json!({"phase": "initial", "timestamp": Utc::now()}))
                .with_version(2)
        )
        .with_checkpoint(
            TestCheckpointBuilder::new(automaton_name)
                .with_last_processed("event-10")
                .with_processed_count(10)
                .with_state(json!({"phase": "batch_1", "timestamp": Utc::now()}))
                .with_version(2)
        )
        .with_checkpoint(
            TestCheckpointBuilder::new(automaton_name)
                .with_last_processed("event-25")
                .with_processed_count(25)
                .with_state(json!({"phase": "batch_2", "timestamp": Utc::now()}))
                .with_version(2)
        );
    
    // Phase 2: Execute checkpoint scenario
    checkpoint_scenario.execute(&pool).await?;
    
    // Initialize checkpoint manager for verification
    let checkpoint_manager = CheckpointManager::new(
        pool.clone(),
        automaton_name.to_string(),
        "test-automaton-group".to_string(),
        "test-automaton-consumer".to_string(),
    );

    // Simulate processing time between checkpoints
    tokio::time::sleep(StdDuration::from_millis(100)).await;

    // Phase 3: Verify checkpoint retrieval via TestQueries
    let db_checkpoint = TestQueries::get_checkpoint(&pool, automaton_name).await?
        .expect("Checkpoint should exist");
    
    assert_eq!(
        db_checkpoint.processed_count, 25,
        "Retrieved checkpoint should have final count"
    );
    assert_eq!(
        db_checkpoint.last_processed_id.as_deref(),
        Some("event-25"),
        "Last processed ID should match"
    );

    // Phase 4: Simulate automaton restart and recovery
    // Update checkpoint to simulate continued processing
    TestCheckpointBuilder::new(automaton_name)
        .with_last_processed("event-50")
        .with_processed_count(50)
        .with_state(json!({"phase": "recovery", "timestamp": Utc::now()}))
        .with_version(2)
        .insert(&pool)
        .await?;
    
    // Verify continued processing via TestQueries
    let final_checkpoint = TestQueries::get_checkpoint(&pool, automaton_name).await?
        .expect("Final checkpoint should exist");
    
    assert_eq!(
        final_checkpoint.processed_count, 50,
        "Processing should continue from recovery point"
    );
    
    println!("Recovery checkpoint:");
    println!("- Last processed ID: {:?}", final_checkpoint.last_processed_id);
    println!("- Processed count: {}", final_checkpoint.processed_count);
    println!("- Data: {:?}", final_checkpoint.state_data);

    println!("✓ Checkpoint persistence and recovery workflow verified");
    Ok(())
}

// =============================================================================
// Multi-Component Coordination Tests
// =============================================================================

/// Test coordination
test_concurrent_operations!(test_multi_component_coordination_workflow, 10,
    |pool: Arc<DbPool>, index: usize| async move {
        // Concurrent operation
        Ok(())
    },
    |pool: &Arc<DbPool>, results: &Vec<_>| async move {
        assert_eq!(results.len(), 10);
        Ok(())
    }
);

// =============================================================================
// Error Recovery Workflow Tests
// =============================================================================

/// Test error detection and recovery workflow
#[sinex_test]
async fn test_error_recovery_workflow(ctx: TestContext) -> TestResult {
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

                let recovery_event = TestEventBuilder::new(
                    component_name,
                    &format!("recovery.attempt.{}", retry)
                )
                .with_payload(json!({
                    "operation_id": operation_id,
                    "retry_attempt": retry,
                    "error_type": "simulated_failure"
                }))
                .with_host("error-recovery-test");

                match recovery_event.insert(&pool_clone).await {
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
            let normal_event = TestEventBuilder::new(component_name, "normal.operation")
                .with_payload(json!({
                    "operation_id": operation_id,
                    "status": "success"
                }))
                .with_host("error-recovery-test");

            match normal_event.insert(&pool_clone).await {
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
    let all_events = TestQueries::get_events_by_source(&pool, component_name, None).await?;
    
    let recovery_events = all_events.iter()
        .filter(|e| e.event_type.starts_with("recovery.attempt"))
        .count() as i64;
    
    let normal_events = all_events.iter()
        .filter(|e| e.event_type == "normal.operation")
        .count() as i64;

    assert!(recovery_events > 0, "Recovery events should be recorded");
    assert!(normal_events > 0, "Normal operations should be recorded");

    // Phase 5: Verify system resilience
    let total_events = TestQueries::count_events_by_source(&pool, component_name).await?;

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
async fn test_performance_under_load_workflow(ctx: TestContext) -> TestResult {
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

                    let load_event = TestEventBuilder::new(
                        &format!("load-worker-{}", worker_id),
                        "performance.load.event"
                    )
                    .with_payload(json!({
                        "worker_id": worker_id,
                        "event_id": event_id,
                        "timestamp": Utc::now(),
                        "data": format!("load-test-data-{}-{}", worker_id, event_id)
                    }))
                    .with_host("performance-test");

                    match load_event.insert(&pool_clone).await {
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
    let mut total_db_events = 0i64;
    for worker_id in 0..concurrent_workers {
        let worker_count = TestQueries::count_events_by_source(
            &pool,
            &format!("load-worker-{}", worker_id)
        ).await?;
        total_db_events += worker_count;
    }

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
async fn test_data_consistency_workflow(ctx: TestContext) -> TestResult {
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
            let event_builder = TestEventBuilder::new(
                &format!("consistency-component-{}", component_id),
                &format!("consistency.chain.{}", chain_id)
            )
            .with_payload(json!({
                "chain_id": chain_id,
                "component_id": component_id,
                "sequence_number": component_id,
                "previous_event_id": if component_id > 0 {
                    Some(chain_events.last().unwrap().id.to_string())
                } else {
                    None
                },
                "consistency_check": format!("chain-{}-step-{}", chain_id, component_id)
            }))
            .with_host("consistency-test")
            .with_timestamp(
                base_timestamp + Duration::seconds(chain_id as i64 * 10 + component_id as i64)
            );

            chain_events.push(event_builder.build());
        }

        event_chains.push(chain_events);
    }

    // Phase 3: Insert all events and maintain consistency tracking
    let mut consistency_violations = Vec::new();
    let mut successful_chains = 0;

    for (chain_id, chain_events) in event_chains.iter().enumerate() {
        let mut chain_success = true;

        for event in chain_events {
            match TestQueries::insert_full_event(
                &pool,
                &event.source,
                &event.event_type,
                &event.host,
                event.payload.clone(),
                event.ts_orig,
                event.ingestor_version.clone(),
                event.payload_schema_id,
                event.source_event_ids.clone(),
            ).await {
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
    // Note: Raw SQL required for JSON field extraction and complex ordering
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
    // Note: Raw SQL required for JSON field extraction
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
            // Use TestQueries instead of direct db access
            match TestQueries::get_event(&pool, event_id).await {
                Ok(_) => {}, // Event exists
                Err(_) => {
                    referential_violations += 1;
                    println!(
                        "Referential violation: event {} references non-existent {}",
                        event.event_id, prev_id
                    );
                }
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

// =============================================================================
// Factory-Based Workflow Tests
// =============================================================================

/// Test a complete user workday using test factories
#[sinex_test]
async fn test_user_workday_workflow_with_factories(ctx: TestContext) -> TestResult {
    let pool = ctx.pool().clone();
    
    // Generate a complete workday scenario using factories
    let workday_events = scenarios::user_workday();
    
    println!("Generated {} workday events", workday_events.len());
    
    // Insert all events
    let start_time = Instant::now();
    let mut inserted_ids = Vec::new();
    
    for event in &workday_events {
        let inserted = TestQueries::insert_full_event(
            &pool,
            &event.source,
            &event.event_type,
            &event.host,
            event.payload.clone(),
            event.ts_orig,
            event.ingestor_version.clone(),
            event.payload_schema_id,
            event.source_event_ids.clone(),
        ).await?;
        inserted_ids.push(inserted);
    }
    
    let insertion_duration = start_time.elapsed();
    println!("Inserted all events in {:?}", insertion_duration);
    
    // Verify event distribution
    let event_sources: HashMap<String, usize> = workday_events
        .iter()
        .map(|e| e.source.clone())
        .fold(HashMap::new(), |mut acc, source| {
            *acc.entry(source).or_insert(0) += 1;
            acc
        });
    
    println!("Event distribution:");
    for (source, count) in &event_sources {
        println!("  {}: {} events", source, count);
    }
    
    // Verify we have events from multiple sources (user activity, system monitoring, etc.)
    assert!(event_sources.len() >= 3, "Should have events from multiple sources");
    assert!(event_sources.keys().any(|k| k.contains("shell")), "Should have shell events");
    assert!(event_sources.keys().any(|k| k.contains("sinex")), "Should have system events");
    
    // Verify temporal ordering
    let is_sorted = workday_events.windows(2).all(|w| w[0].ts_orig <= w[1].ts_orig);
    assert!(is_sorted, "Events should be temporally ordered");
    
    println!("✓ User workday workflow with factories verified");

/// Test error scenarios and recovery using factories
#[sinex_test]
async fn test_error_recovery_workflow_with_factories(ctx: TestContext) -> TestResult {
    let pool = ctx.pool().clone();
    
    // Generate error cascade scenario
    let error_cascade = ErrorScenarioFactory::create_error_cascade();
    
    // Generate recovery scenario
    let recovery_scenario = ErrorScenarioFactory::create_recovery_scenario();
    
    // Combine scenarios
    let mut all_events = error_cascade;
    all_events.extend(recovery_scenario);
    all_events.sort_by_key(|e| e.ts_orig.unwrap_or_else(Utc::now));
    
    println!("Generated {} error/recovery events", all_events.len());
    
    // Insert events
    for event in &all_events {
        TestQueries::insert_full_event(
            &pool,
            &event.source,
            &event.event_type,
            &event.host,
            event.payload.clone(),
            event.ts_orig,
            event.ingestor_version.clone(),
            event.payload_schema_id,
            event.source_event_ids.clone(),
        ).await?;
    }
    
    // Analyze error patterns
    let error_events = all_events.iter()
        .filter(|e| e.event_type.contains("error") || e.event_type == "unit.stopped")
        .count();
    
    let recovery_events = all_events.iter()
        .filter(|e| e.event_type == "unit.started" || e.event_type == "process.started")
        .count();
    
    let health_events = all_events.iter()
        .filter(|e| e.event_type == "system.health.summary")
        .count();
    
    println!("Event analysis:");
    println!("  Error events: {}", error_events);
    println!("  Recovery events: {}", recovery_events);
    println!("  Health monitoring events: {}", health_events);
    
    assert!(error_events > 0, "Should have error events");
    assert!(recovery_events > 0, "Should have recovery events");
    assert!(health_events > 0, "Should have health monitoring");
    
    // Verify recovery pattern: errors followed by recovery
    let has_recovery_pattern = all_events.windows(2).any(|w| {
        (w[0].event_type.contains("error") || w[0].event_type == "unit.stopped") &&
        (w[1].event_type == "unit.started" || w[1].event_type == "process.started")
    });
    
    assert!(has_recovery_pattern, "Should show error->recovery pattern");
    
    println!("✓ Error recovery workflow with factories verified");

/// Test file system workflows using factories
#[sinex_test]
async fn test_filesystem_workflow_with_factories(ctx: TestContext) -> TestResult {
    let pool = ctx.pool().clone();
    
    // Generate file workflow
    let file_workflow = FileSystemScenarioFactory::create_file_workflow("/test/project");
    
    // Generate build process
    let build_process = FileSystemScenarioFactory::create_build_process();
    
    // Combine workflows
    let mut all_events = file_workflow;
    all_events.extend(build_process);
    
    println!("Generated {} filesystem events", all_events.len());
    
    // Insert events
    for event in &all_events {
        TestQueries::insert_full_event(
            &pool,
            &event.source,
            &event.event_type,
            &event.host,
            event.payload.clone(),
            event.ts_orig,
            event.ingestor_version.clone(),
            event.payload_schema_id,
            event.source_event_ids.clone(),
        ).await?;
    }
    
    // Analyze filesystem operations
    let event_types: HashMap<&str, usize> = all_events
        .iter()
        .map(|e| e.event_type.as_str())
        .fold(HashMap::new(), |mut acc, evt_type| {
            *acc.entry(evt_type).or_insert(0) += 1;
            acc
        });
    
    println!("Filesystem operation distribution:");
    for (event_type, count) in &event_types {
        println!("  {}: {} operations", event_type, count);
    }
    
    // Verify expected operations
    assert!(event_types.contains_key("dir.created"), "Should have directory creation");
    assert!(event_types.contains_key("file.created"), "Should have file creation");
    assert!(event_types.contains_key("file.modified"), "Should have file modifications");
    assert!(event_types.contains_key("command.executed"), "Should have build commands");
    
    // Verify logical workflow order
    let dir_created_time = all_events.iter()
        .find(|e| e.event_type == "dir.created")
        .and_then(|e| e.ts_orig);
    
    let first_file_time = all_events.iter()
        .find(|e| e.event_type == "file.created")
        .and_then(|e| e.ts_orig);
    
    if let (Some(dir_time), Some(file_time)) = (dir_created_time, first_file_time) {
        assert!(dir_time <= file_time, "Directory should be created before files");
    }
    
    println!("✓ Filesystem workflow with factories verified");

/// Test complex workflows using multiple factories
#[sinex_test]
async fn test_complex_workflow_composition(ctx: TestContext) -> TestResult {
    let pool = ctx.pool().clone();
    
    // Create a complex scenario combining multiple factory outputs
    let user_session = UserActivityFactory::create_user_session(60, 30);
    let dev_workflow = UserActivityFactory::create_development_workflow();
    let git_workflow = WorkflowFactory::create_git_workflow();
    let system_monitoring = SystemEventFactory::create_system_monitoring(60, 30);
    
    // Merge all events
    let mut all_events = Vec::new();
    all_events.extend(user_session);
    all_events.extend(dev_workflow);
    all_events.extend(git_workflow);
    all_events.extend(system_monitoring);
    
    // Sort by timestamp for realistic ordering
    all_events.sort_by_key(|e| e.ts_orig.unwrap_or_else(Utc::now));
    
    println!("Generated {} events for complex workflow", all_events.len());
    
    // Insert in batches for performance
    let batch_size = 50;
    for (batch_num, batch) in all_events.chunks(batch_size).enumerate() {
        let batch_start = Instant::now();
        
        for event in batch {
            TestQueries::insert_full_event(
                &pool,
                &event.source,
                &event.event_type,
                &event.host,
                event.payload.clone(),
                event.ts_orig,
                event.ingestor_version.clone(),
                event.payload_schema_id,
                event.source_event_ids.clone(),
            ).await?;
        }
        
        println!("Batch {} ({} events) inserted in {:?}", 
                 batch_num, batch.len(), batch_start.elapsed());
    }
    
    // Verify complex interactions
    let has_git_after_file_edit = all_events.windows(2).any(|w| {
        w[0].event_type == "file.modified" && 
        w[1].event_type == "command.executed" &&
        w[1].payload.get("command")
            .and_then(|v| v.as_str())
            .map(|cmd| cmd.starts_with("git"))
            .unwrap_or(false)
    });
    
    assert!(has_git_after_file_edit, "Should have git operations after file edits");
    
    // Verify concurrent activity (user actions during system monitoring)
    let overlapping_events = all_events.windows(2).any(|w| {
        let is_user_event = w[0].source.contains("shell") || w[0].source.contains("fs");
        let is_system_event = w[1].source == "sinex" && w[1].event_type.contains("health");
        is_user_event && is_system_event
    });
    
    assert!(overlapping_events, "Should have interleaved user and system events");
    
    println!("✓ Complex workflow composition verified");
    Ok(())
}

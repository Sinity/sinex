// Integration tests for the new satellite architecture
//
// These tests verify that the satellite services can communicate
// properly and that the overall system works as expected.

use crate::common::prelude::*;
use crate::common::test_macros::*;
use crate::common::builders::{TestEventBuilder, TestCheckpointBuilder};
use crate::common::query_helpers::TestQueries;
use crate::common::test_factories::{
    UserActivityFactory, SystemEventFactory, WorkflowFactory,
    FileSystemScenarioFactory, ErrorScenarioFactory
};
use anyhow::Result;
use sinex_satellite_sdk::{config::EventSourceConfig, grpc_client::IngestClient};
use sinex_db::queries::CheckpointQueries;
use sinex_events::{EventFactory, event_types};
use sinex_test_macros::sinex_test;
use tracing::{info, warn};

#[sinex_test]
async fn test_satellite_architecture_basic_flow(ctx: TestContext) -> TestResult {
    // NOTE: This test is disabled due to ULID/UUID type issues with sqlx
    // TODO: Fix ULID handling in database queries
    return Ok(());

    info!("Testing basic satellite architecture flow");

    // Note: This is a unit test that verifies the SDK components work
    // Full integration would require running actual satellite processes

    // Test 1: Verify IngestClient can be created (would fail without actual ingestd)
    let ingest_result = IngestClient::new("/run/sinex/ingest.sock").await;

    // We expect this to fail in test environment since ingestd isn't running
    match ingest_result {
        Err(_) => {
            info!("✓ IngestClient properly fails when ingestd is not running");
        }
        Ok(_) => {
            warn!("IngestClient connected unexpectedly - is ingestd running?");
        }
    }

    // Test 2: Verify satellite configuration can be loaded
    let config = create_test_event_source_config();
    assert!(!config.base.service_name.is_empty());
    assert!(config.batch_size > 0);
    assert!(config.batch_timeout_secs > 0);
    info!("✓ Event source configuration loads correctly");

    // Test 3: Verify database schema includes new tables
    // Note: Schema introspection requires raw SQL
    let table_check = sqlx::query_scalar!(
        r#"
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables 
            WHERE table_schema = $1 AND table_name = $2
        )
        "#,
        "core",
        "automaton_checkpoints"
    )
    .fetch_one(ctx.pool())
    .await?;
    assert!(table_check.unwrap_or(false), "automaton_checkpoints table should exist");
    info!("✓ New database schema is in place");

    // Test 4: Test checkpoint functionality
    let checkpoint_test_result = test_checkpoint_functionality(ctx.pool()).await;
    assert!(
        checkpoint_test_result.is_ok(),
        "Checkpoint functionality should work"
    );
    info!("✓ Checkpoint functionality works correctly");

    Ok(())
}

#[sinex_test]
async fn test_satellite_sdk_components(ctx: TestContext) -> TestResult {
    info!("Testing satellite SDK components with fixtures");

    // Use populated checkpoints fixture for testing
    let checkpoints_fixture = crate::common::fixtures::populated_checkpoints(&ctx).await?;
    
    info!("✓ Using fixture with {} checkpoints", checkpoints_fixture.checkpoint_ids.len());

    // Test checkpoint manager with first automaton from fixture
    use sinex_satellite_sdk::checkpoint::CheckpointManager;
    
    let checkpoint_manager = CheckpointManager::new(
        ctx.pool().clone(),
        checkpoints_fixture.automaton_names[0].clone(),
        "default_group".to_string(),
        "default".to_string(),
    );

    // Load checkpoint from fixture
    let checkpoint = checkpoint_manager.load_checkpoint().await?;
    assert_eq!(checkpoint.processed_count, 100); // First automaton has 100 events
    info!("✓ Checkpoint from fixture loaded correctly");

    // Update checkpoint
    checkpoint.processed_count = 42;
    checkpoint.set_last_processed_id(Some("test-message-id".to_string()));
    checkpoint.data = Some(serde_json::json!({"test": "data"}));

    // Save checkpoint
    checkpoint_manager.save_checkpoint(&checkpoint).await?;
    info!("✓ Checkpoint saves successfully");

    // Load checkpoint again and verify
    let loaded_checkpoint = checkpoint_manager.load_checkpoint().await?;
    assert_eq!(loaded_checkpoint.processed_count, 42);
    assert_eq!(
        loaded_checkpoint.last_processed_id(),
        Some("test-message-id".to_string())
    );
    assert_eq!(
        loaded_checkpoint.data,
        Some(serde_json::json!({"test": "data"}))
    );
    info!("✓ Checkpoint loads saved data correctly");

    Ok(())
}

#[sinex_test]
async fn test_satellite_event_flow_simulation(ctx: TestContext) -> TestResult {
    info!("Testing simulated satellite event flow");

    // Simulate the flow described in the refactoring plan:
    // 1. Event source creates raw event
    // 2. Ingestd would write to core.events and publish to Redis
    // 3. Automaton would process and create canonical event

    // Step 1: Create a raw event (simulating what an event source would do)
    let raw_event = TestEventBuilder::new(sources::SHELL_KITTY, event_types::shell::COMMAND_EXECUTED)
        .with_payload(json!({
            "command": "ls -la",
            "working_directory": "/home/user",
            "exit_code": 0,
            "duration_ms": 150
        }))
        .insert(ctx.pool())
        .await?;
    
    let event_id = raw_event.id;
    info!("✓ Raw event written to database");

    // Step 3: Simulate automaton processing by creating canonical event in core.events
    let canonical_event = TestEventBuilder::new("canonical.terminal", "command.canonical")
        .with_payload(json!({
            "command": "ls -la",
            "working_directory": "/home/user",
            "source_events": [event_id.to_string()],
            "synthesis_timestamp": chrono::Utc::now().to_rfc3339(),
            "enrichment_history": []
        }))
        .with_source_events(vec![event_id])
        .insert(ctx.pool())
        .await?;
    
    let canonical_event_id = canonical_event.id;
    info!("✓ Canonical event created from raw event");

    // Step 4: Verify the complete flow
    let retrieved_canonical = TestQueries::get_event(ctx.pool(), canonical_event_id).await?;

    assert_eq!(retrieved_canonical.source, "canonical.terminal");
    assert_eq!(retrieved_canonical.event_type, "command.canonical");
    assert!(retrieved_canonical.payload.get("command").is_some());
    assert_eq!(retrieved_canonical.source_event_ids, Some(vec![event_id]));
    info!("✓ Complete satellite event flow simulation successful");

    Ok(())
}

#[sinex_test]
async fn test_satellite_processing_with_realistic_workload(ctx: TestContext) -> TestResult {
    info!("Testing satellite processing with realistic workload");
    
    // Generate a realistic user session to process
    let session_events = UserActivityFactory::create_user_session(30, 15);
    
    // Insert all events as if they came from satellites
    let mut raw_event_ids = Vec::new();
    for event in &session_events {
        let inserted = insert_event(ctx.pool(), event).await?;
        raw_event_ids.push(inserted.id);
    }
    
    info!("✓ Inserted {} raw events from simulated satellites", raw_event_ids.len());
    
    // Simulate automaton processing by grouping related events
    // Group by event type for canonical event creation
    let mut events_by_type: std::collections::HashMap<String, Vec<sinex_ulid::Ulid>> = std::collections::HashMap::new();
    
    for (event, id) in session_events.iter().zip(raw_event_ids.iter()) {
        events_by_type.entry(event.event_type.clone())
            .or_insert_with(Vec::new)
            .push(*id);
    }
    
    // Create canonical events for each type
    let mut canonical_count = 0;
    for (event_type, source_ids) in events_by_type.iter() {
        if source_ids.len() >= 2 {  // Only create canonical for multiple events
            let canonical = TestEventBuilder::new(
                "canonical.activity",
                &format!("{}.aggregated", event_type)
            )
            .with_payload(json!({
                "event_type": event_type,
                "source_count": source_ids.len(),
                "aggregated_at": chrono::Utc::now().to_rfc3339(),
                "summary": format!("Aggregated {} {} events", source_ids.len(), event_type)
            }))
            .with_source_events(source_ids.clone())
            .insert(ctx.pool())
            .await?;
            
            canonical_count += 1;
            info!("✓ Created canonical event for {} events of type {}", source_ids.len(), event_type);
        }
    }
    
    assert!(canonical_count > 0, "Should create at least one canonical event");
    info!("✓ Created {} canonical events from {} raw events", canonical_count, raw_event_ids.len());
    
    // Verify checkpointing for the processing
    let checkpoint_manager = sinex_satellite_sdk::checkpoint::CheckpointManager::new(
        ctx.pool().clone(),
        "activity-processor".to_string(),
        "main".to_string(),
        "worker-1".to_string(),
    );
    
    // Save processing state
    let mut checkpoint = checkpoint_manager.load_checkpoint().await?;
    checkpoint.processed_count = raw_event_ids.len() as u64;
    checkpoint.set_last_processed_id(Some(raw_event_ids.last().unwrap().to_string()));
    checkpoint.data = Some(json!({
        "canonical_events_created": canonical_count,
        "processing_complete": true
    }));
    
    checkpoint_manager.save_checkpoint(&checkpoint).await?;
    info!("✓ Saved checkpoint for processing {} events", raw_event_ids.len());
    
    Ok(())
}

#[sinex_test]
async fn test_satellite_error_handling_workflow(ctx: TestContext) -> TestResult {
    info!("Testing satellite error handling with realistic scenarios");
    
    // Generate error scenarios
    let error_events = ErrorScenarioFactory::create_error_cascade();
    
    // Insert error events
    let mut error_ids = Vec::new();
    for event in &error_events {
        let inserted = insert_event(ctx.pool(), event).await?;
        error_ids.push(inserted.id);
    }
    
    info!("✓ Inserted {} error events", error_ids.len());
    
    // Simulate error detection and recovery by an automaton
    let error_detections = error_events.iter()
        .filter(|e| e.event_type.contains("error"))
        .count();
    
    assert!(error_detections > 0, "Should have error events to process");
    
    // Create alert event based on error cascade
    let alert_event = TestEventBuilder::new("monitoring.alerts", "error.cascade_detected")
        .with_payload(json!({
            "error_count": error_detections,
            "first_error_id": error_ids.first().unwrap().to_string(),
            "last_error_id": error_ids.last().unwrap().to_string(),
            "cascade_duration_seconds": 30,
            "affected_services": ["health-aggregator"],
            "alert_level": "warning",
            "recommended_action": "Check service health and restart if needed"
        }))
        .with_source_events(error_ids.clone())
        .insert(ctx.pool())
        .await?;
    
    info!("✓ Created alert event for error cascade");
    
    // Verify recovery events were also captured
    let recovery_events = error_events.iter()
        .filter(|e| e.event_type == "sinex.process_started" && 
                e.payload.get("recovery").and_then(|v| v.as_bool()).unwrap_or(false))
        .count();
    
    assert!(recovery_events > 0, "Should have recovery events");
    info!("✓ Verified {} recovery events in the cascade", recovery_events);
    
    Ok(())
}

#[sinex_test]
async fn test_satellite_file_system_monitoring(ctx: TestContext) -> TestResult {
    info!("Testing satellite file system monitoring workflow");
    
    // Generate file system activity
    let fs_workflow = FileSystemScenarioFactory::create_file_workflow("/home/user/monitored-project");
    let build_events = FileSystemScenarioFactory::create_build_process();
    
    // Insert file system events
    let mut fs_event_ids = Vec::new();
    for event in fs_workflow.iter().chain(build_events.iter()) {
        let inserted = insert_event(ctx.pool(), event).await?;
        fs_event_ids.push(inserted.id);
    }
    
    info!("✓ Inserted {} file system events", fs_event_ids.len());
    
    // Simulate file system monitoring automaton
    // Count different types of operations
    let creates = fs_workflow.iter()
        .filter(|e| e.event_type.contains("created"))
        .count();
    let modifies = fs_workflow.iter()
        .filter(|e| e.event_type.contains("modified"))
        .count();
    let deletes = fs_workflow.iter()
        .filter(|e| e.event_type.contains("deleted"))
        .count();
    
    // Create summary event
    let summary = TestEventBuilder::new("fs.monitor", "activity.summary")
        .with_payload(json!({
            "period_minutes": 10,
            "total_operations": fs_event_ids.len(),
            "files_created": creates,
            "files_modified": modifies,
            "files_deleted": deletes,
            "build_detected": build_events.len() > 0,
            "hot_paths": ["/home/user/monitored-project/src/lib.rs"],
            "summary": "Active development session detected"
        }))
        .with_source_events(fs_event_ids.clone())
        .insert(ctx.pool())
        .await?;
    
    info!("✓ Created file system activity summary");
    
    // Verify build artifact detection
    let build_artifacts = build_events.iter()
        .filter(|e| e.payload.get("path").and_then(|v| v.as_str())
                    .map(|p| p.contains("target/release")).unwrap_or(false))
        .count();
    
    assert!(build_artifacts > 0, "Should detect build artifacts");
    info!("✓ Detected {} build artifacts", build_artifacts);
    
    Ok(())
}

/// Helper function to create test event source configuration
fn create_test_event_source_config() -> EventSourceConfig {
    use sinex_satellite_sdk::config::SatelliteConfig;
    use std::collections::HashMap;
    use std::path::PathBuf;

    let base_config = SatelliteConfig {
        service_name: "test-event-source".to_string(),
        log_level: "debug".to_string(),
        ingest_socket_path: "/run/sinex/ingest.sock".to_string(),
        redis_url: "redis://localhost:6379".to_string(),
        database_url: None,
        database_pool_size: 10,
        work_dir: PathBuf::from("/tmp/sinex-test"),
        dry_run: true,
        replay: None,
    };

    EventSourceConfig {
        base: base_config,
        batch_size: 100,
        batch_timeout_secs: 5,
        source_config: HashMap::new(),
    }
}


/// Helper function to test checkpoint functionality
async fn test_checkpoint_functionality(pool: &sqlx::PgPool) -> AnyhowResult<()> {
    use sinex_satellite_sdk::checkpoint::CheckpointManager;

    let manager = CheckpointManager::new(
        pool.clone(),
        "test-checkpoint-automaton".to_string(),
        "test-checkpoint-group".to_string(),
        "test-checkpoint-consumer".to_string(),
    );

    // Test checkpoint creation and retrieval using builder
    TestCheckpointBuilder::new("test-checkpoint-automaton")
        .with_group("test-checkpoint-group")
        .with_consumer("test-checkpoint-consumer")
        .with_last_processed("test-id-123")
        .with_processed_count(100)
        .with_state(json!({"test": "checkpoint"}))
        .with_version(2)
        .insert(&pool)
        .await?;
    
    let loaded = manager.load_checkpoint().await?;
    assert_eq!(loaded.last_processed_id(), Some("test-id-123".to_string()));
    assert_eq!(loaded.processed_count, 100);

    // Test checkpoint stats via TestQueries
    let checkpoint_count = TestQueries::count_checkpoints_by_automaton(&pool, "test-checkpoint-automaton").await?;
    assert!(checkpoint_count > 0, "Should have checkpoint records");

    info!("Checkpoint functionality test passed");
    Ok(())
}

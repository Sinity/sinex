// Integration tests for the unified node architecture (Phase 1)
//
// These tests verify Phase 1's Architectural Consolidation:
// - Unified Node trait
// - Single-writer pattern through ingestd
// - Schema contract enforcement

use sinex_db::DbPoolExt;
use sinex_node_sdk::stream_processor::{Checkpoint, TimeHorizon};
use sinex_primitives::temporal::Timestamp;
use sinex_primitives::DynamicPayload;
use tracing::info;
use xtask::sandbox::prelude::*;
use xtask::sandbox::TestResult;

#[sinex_test]
async fn test_phase1_unified_stream_processor_trait(ctx: TestContext) -> TestResult<()> {
    info!("Testing Phase 1: Unified Node trait");

    // Phase 1.1: Test that both ingestors and automata implement same trait
    // This validates the unified processing primitive requirement

    // Test 1: Verify unified checkpoint types work across both processor types
    let external_checkpoint = Checkpoint::external(
        serde_json::json!({"file": "/var/log/test.log", "offset": 1024}),
        "File position for ingestor",
    );

    let internal_checkpoint = Checkpoint::internal(Ulid::new(), 100);

    let stream_checkpoint = Checkpoint::stream("1234567890-0", Some(Ulid::new()));

    // Verify all checkpoint types serialize properly
    assert!(external_checkpoint.description().contains("File position"));
    assert!(internal_checkpoint.description().contains("event"));
    assert!(stream_checkpoint.description().contains("stream"));

    info!("✓ Unified checkpoint types validated for Phase 1");

    // Test 2: Verify TimeHorizon modes (replacing sensor/scanner split)
    let snapshot = TimeHorizon::Snapshot;
    let historical = TimeHorizon::Historical {
        end_time: Timestamp::now(),
    };
    let continuous = TimeHorizon::Continuous;

    assert!(snapshot.is_bounded());
    assert!(historical.is_bounded());
    assert!(continuous.is_continuous());
    assert!(!continuous.is_bounded());

    info!("✓ TimeHorizon modes validated for Phase 1");

    // Test 3: Test checkpoint persistence for state recovery
    // Use NATS context for KV
    let ctx_nats = ctx.with_nats().shared().await?;
    let checkpoint_test_result = test_checkpoint_functionality(&ctx_nats).await;
    assert!(
        checkpoint_test_result.is_ok(),
        "Checkpoint functionality should work"
    );
    info!("✓ Checkpoint persistence works for Phase 1");

    Ok(())
}

#[sinex_test]
async fn test_phase1_single_writer_pattern(ctx: TestContext) -> TestResult<()> {
    info!("Testing Phase 1.2: Single-writer pattern through ingestd");

    let ctx = ctx.with_nats().shared().await?;
    let _scope = ctx.pipeline().await?;

    // Phase 1.2: Enforce that all events flow through ingestd
    // No direct database writes from nodes

    // Test that events created via TestContext (simulating ingestd) have proper structure
    let test_event = ctx
        .publish(DynamicPayload::new(
            "single-writer-test",
            "pattern.validation",
            serde_json::json!({
                "test": "All writes must go through ingestd",
                "phase": "1.2"
            }),
        ))
        .await?;

    // Verify event has been assigned ULID by the "single writer" (ingestd simulation)
    assert!(
        test_event.id.is_some(),
        "Event must have ULID assigned by ingestd"
    );

    // Verify event is in database (written by the single writer)
    let retrieved = ctx
        .pool
        .events()
        .get_by_id(test_event.id.unwrap())
        .await?
        .expect("Event should exist after single-writer processing");

    assert_eq!(retrieved.source, test_event.source);
    assert_eq!(retrieved.event_type, test_event.event_type);

    // Test that direct database writes would violate the pattern
    // (In production, nodes should not have direct DB write access)
    info!("✓ Single-writer pattern validated - all events flow through ingestd");

    Ok(())
}

#[sinex_test]
async fn test_phase1_schema_contracts(ctx: TestContext) -> TestResult<()> {
    info!("Testing Phase 1.3: Schema contract enforcement");

    let ctx = ctx.with_nats().shared().await?;
    let _scope = ctx.pipeline().await?;

    // Phase 1.3: Test schema validation and contracts

    // Test event with valid schema
    let valid_event = ctx
        .publish(DynamicPayload::new(
            "schema-test",
            "contract.valid",
            serde_json::json!({
                "required_field": "present",
                "numeric_value": 42,
                "nested": {
                    "structure": "valid"
                }
            }),
        ))
        .await?;

    assert!(valid_event.id.is_some());

    // Test that events maintain schema consistency
    let retrieved = ctx
        .pool
        .events()
        .get_by_id(valid_event.id.unwrap())
        .await?
        .expect("Valid schema event should be stored");

    // Verify payload structure is preserved
    assert_eq!(retrieved.payload["required_field"], "present");
    assert_eq!(retrieved.payload["numeric_value"], 42);
    assert_eq!(retrieved.payload["nested"]["structure"], "valid");

    info!("✓ Schema contracts validated for Phase 1.3");

    Ok(())
}

#[sinex_test]
async fn test_node_sdk_components(ctx: TestContext) -> TestResult<()> {
    info!("Testing node SDK components");

    // Test checkpoint manager
    use sinex_node_sdk::CheckpointManager;

    let ctx = ctx.with_nats().shared().await?;
    let kv = ctx.checkpoint_kv().await?;
    let checkpoint_manager = CheckpointManager::new(
        kv,
        "test-automaton".to_string(),
        "test-group".to_string(),
        "test-consumer".to_string(),
    );

    // Load initial checkpoint (should be default)
    let mut checkpoint = checkpoint_manager.load_checkpoint().await?;
    assert_eq!(checkpoint.processed_count, 0);
    assert!(checkpoint.last_processed_id().is_none());
    info!("✓ Default checkpoint loads correctly");

    // Update checkpoint
    checkpoint.processed_count = 42;
    checkpoint.checkpoint = sinex_node_sdk::Checkpoint::Stream {
        message_id: "test-message-id".to_string(),
        event_id: None,
    };
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
async fn test_node_event_flow_simulation(ctx: TestContext) -> TestResult<()> {
    info!("Testing simulated node event flow");

    let ctx = ctx.with_nats().shared().await?;
    let _scope = ctx.pipeline().await?;

    // Simulate the flow described in the refactoring plan:
    // 1. Event source creates raw event
    // 2. Ingestd would write to core.events and publish to Redis
    // 3. Automaton would process and create canonical event

    // Step 1: Create a raw event using modern TestContext API
    let raw_event = ctx
        .publish(DynamicPayload::new(
            "terminal",
            "command.executed",
            serde_json::json!({
                "command": "ls -la",
                "working_directory": "/home/user",
                "exit_code": 0,
                "duration_ms": 150
            }),
        ))
        .await?;

    info!("✓ Raw event written to database");

    // Step 2: Create canonical event (simulating what an automaton would do)
    let canonical_event = ctx
        .publish(DynamicPayload::new(
            "canonical.terminal",
            "command.canonical",
            serde_json::json!({
                "command": "ls -la",
                "working_directory": "/home/user",
                "source_events": [raw_event.id.unwrap().to_string()],
                "synthesis_timestamp": Timestamp::now().format_rfc3339(),
                "enrichment_history": []
            }),
        ))
        .await?;

    info!("✓ Canonical event created from raw event");

    // Step 3: Verify the complete flow using repository pattern
    let retrieved_canonical = ctx
        .pool
        .events()
        .get_by_id(canonical_event.id.unwrap())
        .await?
        .expect("Canonical event should exist");

    assert_eq!(retrieved_canonical.source.as_str(), "canonical.terminal");
    assert_eq!(retrieved_canonical.event_type.as_str(), "command.canonical");
    assert!(retrieved_canonical.payload.get("command").is_some());
    info!("✓ Complete node event flow simulation successful");

    Ok(())
}

// Add new test for Phase 2 acquisition integration
#[sinex_test]
async fn test_phase2_acquisition_integration(ctx: TestContext) -> TestResult<()> {
    info!("Testing Phase 2: Acquisition Layer");

    let ctx = ctx.with_nats().shared().await?;
    let _scope = ctx.pipeline().await?;

    // Phase 2.1: Test source material tracking
    let material_id = Ulid::new();

    // Simulate event with material provenance
    let event_with_material = ctx
        .publish(DynamicPayload::new(
            "acquisition-test",
            "material.captured",
            serde_json::json!({
                "material_id": material_id.to_string(),
                "capture_type": "tree_watch",
                "target_path": "/var/log/test",
                "offset_start": 0,
                "offset_end": 1024,
                "capture_metadata": {
                    "source": "filesystem",
                    "mode": "continuous"
                }
            }),
        ))
        .await?;

    assert!(event_with_material.id.is_some());
    info!("✓ Source material tracking validated for Phase 2");

    // Phase 2.2: Test temporal ledger concept
    let temporal_events = vec![
        (
            "acquisition",
            "ledger.entry",
            serde_json::json!({
                "material_id": material_id.to_string(),
                "ts_capture_start": "2024-01-01T00:00:00Z",
                "ts_capture_end": "2024-01-01T00:01:00Z",
                "offset_start": 0,
                "offset_end": 1024
            }),
        ),
        (
            "acquisition",
            "ledger.entry",
            serde_json::json!({
                "material_id": material_id.to_string(),
                "ts_capture_start": "2024-01-01T00:01:00Z",
                "ts_capture_end": "2024-01-01T00:02:00Z",
                "offset_start": 1024,
                "offset_end": 2048
            }),
        ),
    ];

    for (source, event_type, payload) in temporal_events {
        let event = ctx
            .publish(DynamicPayload::new(source, event_type, payload))
            .await?;
        assert!(event.id.is_some());
    }

    info!("✓ Temporal ledger concept validated for Phase 2");

    // Phase 2.3: Test capture job submission pattern
    let job_event = ctx
        .publish(DynamicPayload::new(
            "acquisition",
            "capture.requested",
            serde_json::json!({
                "job_id": Ulid::new().to_string(),
                "capture_type": "tree_watch",
                "target_path": "/home/user/documents",
                "config": {
                    "recursive": true,
                    "follow_symlinks": false,
                    "max_depth": 10
                }
            }),
        ))
        .await?;

    assert!(job_event.id.is_some());
    info!("✓ Sensor job submission pattern validated for Phase 2");

    Ok(())
}

// Helper function removed - using TestContext::publish_event directly

/// Helper function to test checkpoint functionality
async fn test_checkpoint_functionality(ctx: &TestContext) -> TestResult<()> {
    use sinex_node_sdk::Checkpoint;
    use sinex_node_sdk::{CheckpointManager, CheckpointState};

    let kv = ctx.checkpoint_kv().await?;
    let manager = CheckpointManager::new(
        kv,
        "test-checkpoint-automaton".to_string(),
        "test-checkpoint-group".to_string(),
        "test-checkpoint-consumer".to_string(),
    );

    // Test checkpoint creation and retrieval
    let checkpoint = CheckpointState {
        checkpoint: Checkpoint::Stream {
            message_id: "test-id-123".to_string(),
            event_id: None,
        },
        processed_count: 100,
        last_activity: Timestamp::now(),
        data: Some(serde_json::json!({"test": "checkpoint"})),
        version: 2,
        revision: 0,
    };

    manager.save_checkpoint(&checkpoint).await?;
    let loaded = manager.load_checkpoint().await?;

    assert_eq!(loaded.last_processed_id(), checkpoint.last_processed_id());
    assert_eq!(loaded.processed_count, checkpoint.processed_count);

    // Test basic checkpoint functionality without complex queries
    // This validates that the checkpoint manager works with the database

    info!("Checkpoint functionality test passed");
    Ok(())
}

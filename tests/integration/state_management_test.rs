//! State management integration tests
//! 
//! Tests the StateRepository integration with the database, focusing on:
//! - Cross-functional state management (checkpoints + operations)
//! - Transaction consistency across state operations
//! - System health monitoring
//! - Processor lifecycle management

use color_eyre::eyre::Result;
use chrono::{DateTime, Duration, Utc};
use serde_json::json;
use sinex_core::db::repositories::{DbPoolExt, checkpoints::CheckpointInput, state::Operation};
use sinex_test_utils::prelude::*;
use sinex_core::types::domain::ProcessorName;
use sinex_core::types::Id;

#[sinex_test]
async fn test_checkpoint_operation_consistency(ctx: TestContext) -> Result<()> {
    let state_repo = ctx.pool.state();
    
    let processor_name: ProcessorName = "test-processor".into();
    let event_id = Id::new();
    
    // Create initial checkpoint
    let checkpoint = CheckpointInput::new(processor_name.clone())
        .with_last_processed_id(event_id.clone())
        .with_last_processed_ts(Utc::now())
        .with_checkpoint_data(json!({ "batch_size": 100 }));
        
    let saved_checkpoint = state_repo.save_checkpoint(checkpoint).await?;
    
    // Log operation for checkpoint creation
    let operation = Operation {
        id: None,
        actor: "state-manager".to_string(),
        scope: json!({
            "operation_type": "CheckpointCreated",
            "target_type": "checkpoints",
            "target_id": saved_checkpoint.id.to_string(),
            "processor_name": processor_name.as_ref()
        }),
        state: Some("completed".to_string()),
        preview_summary: Some(json!({ 
            "description": format!("Created checkpoint for processor {}", processor_name.as_ref()),
            "checkpoint_version": saved_checkpoint.checkpoint_version,
            "duration_ms": 25
        })),
        checkpoint: None,
        approved_by: None,
        approved_at: None,
        executor_node: None,
        started_at: Some(Utc::now()),
        finished_at: Some(Utc::now()),
        outcome: Some("success".to_string()),
        error_details: None,
        created_at: Utc::now(),
    };
    
    let logged_operation = state_repo.log_operation(operation).await?;
    
    // Verify checkpoint and operation are consistent
    let retrieved_checkpoint = state_repo
        .get_checkpoint(processor_name.as_ref())
        .await?
        .expect("Checkpoint should exist");
        
    assert_eq!(retrieved_checkpoint.id, saved_checkpoint.id);
    assert_eq!(retrieved_checkpoint.last_processed_id, Some(event_id));
    
    // Verify operation was logged correctly
    let retrieved_operation = state_repo
        .get_operation(&logged_operation.id)
        .await?
        .expect("Operation should exist");
        
    assert_eq!(retrieved_operation.operation_type, "checkpoint_created");
    assert_eq!(retrieved_operation.result_status, "success");
    
    Ok(())
}

#[sinex_test]
async fn test_processor_lifecycle_management(ctx: TestContext) -> Result<()> {
    let state_repo = ctx.pool.state();
    
    let processor_name: ProcessorName = "lifecycle-processor".into();
    let hostname = "test-host";
    
    // Register processor
    let manifest = state_repo
        .register_processor(&processor_name, "automaton", "1.0.0", hostname)
        .await?;
        
    // Log startup operation
    let startup_op = NewOperation {
        operation_type: OperationType::ServiceStarted,
        performed_by: "systemd".to_string(),
        target_type: Some("processors".to_string()),
        target_id: Some(processor_name.to_string()),
        description: format!("Started processor {} on {}", processor_name.as_ref(), hostname),
        metadata: Some(json!({ 
            "processor_type": "automaton",
            "version": "1.0.0",
            "hostname": hostname 
        })),
        result: OperationResult::Success,
        error_message: None,
        duration_ms: Some(1500),
    };
    
    state_repo.log_operation(startup_op).await?;
    
    // Verify processor is active
    let active_processors = state_repo.get_active_processors().await?;
    assert!(!active_processors.is_empty());
    
    let our_processor = active_processors
        .iter()
        .find(|p| p.processor_name == processor_name.as_ref())
        .expect("Our processor should be in active list");
        
    assert_eq!(our_processor.processor_type, "automaton");
    assert_eq!(our_processor.hostname, hostname);
    assert!(our_processor.end_time.is_none());
    
    // Simulate heartbeat update
    let heartbeat_updated = state_repo
        .update_processor_heartbeat(&processor_name, hostname)
        .await?;
        
    assert!(heartbeat_updated);
    
    // Log shutdown operation  
    let shutdown_op = NewOperation {
        operation_type: OperationType::ServiceStopped,
        performed_by: "systemd".to_string(),
        target_type: Some("processors".to_string()),
        target_id: Some(processor_name.to_string()),
        description: format!("Stopped processor {}", processor_name.as_ref()),
        metadata: Some(json!({ "graceful_shutdown": true })),
        result: OperationResult::Success,
        error_message: None,
        duration_ms: Some(500),
    };
    
    state_repo.log_operation(shutdown_op).await?;
    
    // Verify operations were logged
    let processor_operations = state_repo
        .get_operations_for_target("processors", processor_name.as_ref(), Some(10))
        .await?;
        
    assert_eq!(processor_operations.len(), 2);
    assert!(processor_operations.iter().any(|op| op.operation_type == "service_started"));
    assert!(processor_operations.iter().any(|op| op.operation_type == "service_stopped"));
    
    Ok(())
}

#[sinex_test]
async fn test_system_health_monitoring(ctx: TestContext) -> Result<()> {
    let state_repo = ctx.pool.state();
    
    // Run comprehensive health check
    let health_report = state_repo.run_system_health_checks().await?;
    
    // Verify core database functionality
    assert!(health_report.db_connected, "Database should be connected");
    assert!(health_report.ulid_extension_works, "ULID extension should work");
    assert!(health_report.json_schema_extension_works, "JSON Schema extension should work");
    assert!(health_report.events_table_exists, "Events table should exist");
    assert!(health_report.checkpoints_table_exists, "Checkpoints table should exist");
    
    // TimescaleDB may not be installed in test environment
    if let Some(version) = &health_report.timescaledb_version {
        assert!(!version.is_empty(), "TimescaleDB version should not be empty if present");
    }
    
    // Test individual system verification methods
    let uuid = state_repo.test_uuid_generation().await?;
    assert!(!uuid.to_string().is_empty());
    
    let ulid = state_repo.test_ulid_generation().await?;
    assert!(!ulid.is_empty());
    assert_eq!(ulid.len(), 26); // ULID is 26 characters
    
    let json_validation = state_repo.test_json_schema_validation().await?;
    assert!(json_validation);
    
    // Test table existence checks
    assert!(state_repo.table_exists("core", "events").await?);
    assert!(state_repo.table_exists("core", "processor_checkpoints").await?);
    assert!(state_repo.table_exists("core", "operations_log").await?);
    assert!(!state_repo.table_exists("nonexistent", "table").await?);
    
    Ok(())
}

#[sinex_test] 
async fn test_cross_service_state_tracking(ctx: TestContext) -> Result<()> {
    let state_repo = ctx.pool.state();
    
    // Simulate multi-service scenario: ingestd + fs-watcher + canonicalizer
    let services = vec![
        ("ingestd", "service", "2.0.0"),
        ("fs-watcher", "satellite", "1.5.0"), 
        ("canonicalizer", "automaton", "3.1.0"),
    ];
    
    let hostname = "integration-test-host";
    
    // Register all services and create checkpoints
    for (service_name, service_type, version) in services {
        let processor_name: ProcessorName = service_name.into();
        
        // Register processor
        state_repo
            .register_processor(&processor_name, service_type, version, hostname)
            .await?;
            
        // Create checkpoint with some state
        let checkpoint = CheckpointInput::new(processor_name.clone())
            .with_last_processed_ts(Utc::now())
            .with_state_data(json!({
                "service_type": service_type,
                "last_health_check": Utc::now(),
                "processed_events": 0
            }));
            
        state_repo.save_checkpoint(checkpoint).await?;
        
        // Log service operations
        let operations = vec![
            (OperationType::ServiceStarted, "Started successfully"),
            (OperationType::ServiceHealthCheck, "Health check passed"),
            (OperationType::EventProcessed, "Processed batch of events"),
        ];
        
        for (op_type, description) in operations {
            let operation = NewOperation {
                operation_type: op_type,
                performed_by: service_name.to_string(),
                target_type: Some("events".to_string()),
                target_id: None,
                description: description.to_string(),
                metadata: Some(json!({ "service_type": service_type })),
                result: OperationResult::Success,
                error_message: None,
                duration_ms: Some(10),
            };
            
            state_repo.log_operation(operation).await?;
        }
    }
    
    // Verify all services are tracked
    let all_checkpoints = state_repo.get_all_checkpoints().await?;
    assert_eq!(all_checkpoints.len(), 3);
    
    let active_processors = state_repo.get_active_processors().await?;
    assert_eq!(active_processors.len(), 3);
    
    // Verify operations across all services
    let all_operations = state_repo.get_recent_operations(20).await?;
    assert_eq!(all_operations.len(), 9); // 3 services × 3 operations each
    
    // Check operations by type
    let health_checks = state_repo
        .get_operations_by_type(OperationType::ServiceHealthCheck, None)
        .await?;
    assert_eq!(health_checks.len(), 3);
    
    let service_starts = state_repo
        .get_operations_by_type(OperationType::ServiceStarted, None)
        .await?;
    assert_eq!(service_starts.len(), 3);
    
    // Verify processor health summary
    let health_summary = state_repo.get_processor_health().await?;
    assert_eq!(health_summary.active_count, 3);
    assert_eq!(health_summary.unique_processors, 3);
    
    Ok(())
}

#[sinex_test]
async fn test_failure_scenario_tracking(ctx: TestContext) -> Result<()> {
    let state_repo = ctx.pool.state();
    
    let processor_name: ProcessorName = "failing-processor".into();
    
    // Log various failure scenarios
    let failure_operations = vec![
        (
            OperationType::EventProcessed,
            OperationResult::Failure,
            "Database connection timeout",
            Some(5000),
        ),
        (
            OperationType::SchemaRegistered, 
            OperationResult::Failure,
            "Invalid schema format",
            Some(100),
        ),
        (
            OperationType::BulkImport,
            OperationResult::Partial,
            "Imported 500/1000 records before error",
            Some(30000),
        ),
        (
            OperationType::ServiceHealthCheck,
            OperationResult::Failure, 
            "Service unresponsive",
            Some(10000),
        ),
    ];
    
    for (op_type, result, error_msg, duration) in failure_operations {
        let operation = NewOperation {
            operation_type: op_type,
            performed_by: processor_name.to_string(),
            target_type: Some("events".to_string()),
            target_id: None,
            description: "Failed operation".to_string(),
            metadata: Some(json!({ "retry_count": 3 })),
            result,
            error_message: Some(error_msg.to_string()),
            duration_ms: duration,
        };
        
        state_repo.log_operation(operation).await?;
    }
    
    // Get failed operations
    let failed_ops = state_repo.get_failed_operations(None, None).await?;
    assert_eq!(failed_ops.len(), 3); // Only actual failures, not partial
    
    // Verify all failed operations have error messages
    for op in &failed_ops {
        assert!(op.result_message.is_some());
        assert_eq!(op.result_status, "failure");
    }
    
    // Get operations by this specific processor
    let processor_ops = state_repo
        .get_operations_by_operator(processor_name.as_ref(), None)
        .await?;
    assert_eq!(processor_ops.len(), 4);
    
    // Get operation statistics
    let stats = state_repo.get_operation_statistics(None).await?;
    assert_eq!(stats.failed, 3);
    assert_eq!(stats.partial, 1);
    assert_eq!(stats.successful, 0);
    assert_eq!(stats.total, 4);
    
    // Average duration should be reasonable
    if let Some(avg_duration) = stats.avg_duration_ms {
        assert!(avg_duration > 0);
        assert!(avg_duration < 50000); // Less than 50 seconds average
    }
    
    Ok(())
}

#[sinex_test]
async fn test_checkpoint_processor_correlation(ctx: TestContext) -> Result<()> {
    let state_repo = ctx.pool.state();
    
    let processor_name: ProcessorName = "correlation-processor".into();
    let hostname = "correlation-host";
    
    // Register processor
    let manifest = state_repo
        .register_processor(&processor_name, "automaton", "2.0.0", hostname)
        .await?;
    
    // Create multiple checkpoints over time to simulate processing
    let base_time = Utc::now() - Duration::minutes(10);
    let checkpoints_data = vec![
        (base_time, 100, json!({ "status": "starting" })),
        (base_time + Duration::minutes(2), 250, json!({ "status": "processing" })),  
        (base_time + Duration::minutes(5), 400, json!({ "status": "processing" })),
        (base_time + Duration::minutes(8), 500, json!({ "status": "complete" })),
    ];
    
    for (timestamp, processed_count, state) in checkpoints_data {
        let checkpoint = CheckpointInput::new(processor_name.clone())
            .with_last_processed_ts(timestamp)
            .with_state_data(state.clone());
            
        let saved_checkpoint = state_repo.save_checkpoint(checkpoint).await?;
        
        // Log corresponding operation
        let operation = NewOperation {
            operation_type: OperationType::CheckpointUpdated,
            performed_by: processor_name.to_string(),
            target_type: Some("checkpoints".to_string()),
            target_id: Some(saved_checkpoint.id.to_string()),
            description: format!("Updated checkpoint - processed {}", processed_count),
            metadata: Some(json!({ 
                "processed_count": processed_count,
                "state": state,
                "checkpoint_version": saved_checkpoint.checkpoint_version
            })),
            result: OperationResult::Success,
            error_message: None,
            duration_ms: Some(50),
        };
        
        state_repo.log_operation(operation).await?;
    }
    
    // Verify final checkpoint state
    let final_checkpoint = state_repo
        .get_checkpoint(processor_name.as_ref())
        .await?
        .expect("Checkpoint should exist");
        
    assert_eq!(final_checkpoint.checkpoint_version, 4); // Updated 4 times
    
    if let Some(state_data) = &final_checkpoint.state_data {
        assert_eq!(state_data["status"], "complete");
    }
    
    // Verify checkpoint update operations were logged
    let checkpoint_ops = state_repo
        .get_operations_by_type(OperationType::CheckpointUpdated, None)
        .await?;
    assert_eq!(checkpoint_ops.len(), 4);
    
    // Verify operations have proper metadata correlation
    for op in checkpoint_ops {
        assert!(op.metadata.is_some());
        if let Some(metadata) = &op.metadata {
            assert!(metadata["checkpoint_version"].is_number());
            assert!(metadata["processed_count"].is_number());
        }
    }
    
    Ok(())
}
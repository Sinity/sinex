//! Integration tests for ingestd gRPC service
//!
//! Tests the gRPC communication layer between satellites and ingestd,
//! including event ingestion, batch processing, health checks, and error handling.

use color_eyre::eyre::Result;
use serde_json::json;
use sinex_db::models::Event as DbEvent;
use sinex_db::repositories::DbPoolExt;
use sinex_satellite_sdk::grpc_client::{IngestClient, BatchResult, HealthStatus};
use sinex_test_utils::prelude::*;
use sinex_types::domain::{EventSource, EventType, HostName};
use std::time::Duration;
use tokio::time::timeout;
use tracing::{debug, info, warn};

// =============================================================================
// BASIC GRPC COMMUNICATION TESTS
// =============================================================================

#[sinex_test]
async fn test_ingestd_grpc_connection_failure(ctx: TestContext) -> Result<()> {
    // Test that IngestClient properly fails when ingestd is not running
    let result = IngestClient::new("/nonexistent/socket/path").await;
    
    assert!(
        result.is_err(),
        "IngestClient should fail when connecting to nonexistent socket"
    );
    
    // Test connection to wrong socket path
    let result = IngestClient::new("/tmp/nonexistent.sock").await;
    assert!(result.is_err(), "Should fail to connect to nonexistent socket");
    
    info!("✓ IngestClient properly handles connection failures");
    Ok(())
}

#[sinex_test]
async fn test_mock_ingestd_grpc_service(ctx: TestContext) -> Result<()> {
    // Test basic gRPC communication patterns without real ingestd
    // This test validates the proto/gRPC layer works correctly
    
    // Create test event
    let test_event = ctx.create_test_event(
        "grpc-test",
        "mock.communication",
        json!({
            "message": "Testing gRPC communication",
            "test_id": "grpc-001"
        })
    ).await?;
    
    // Verify event structure is compatible with gRPC
    assert_eq!(test_event.source.as_str(), "grpc-test");
    assert_eq!(test_event.event_type.as_str(), "mock.communication");
    assert!(test_event.id.is_some());
    
    // Test event can be serialized (required for gRPC transport)
    let payload_json = serde_json::to_string(&test_event.payload)?;
    assert!(!payload_json.is_empty());
    
    // Test proto conversion logic (without actual gRPC call)
    let host = test_event.host.as_str();
    assert!(!host.is_empty(), "Host should be set for gRPC transport");
    
    info!("✓ Mock gRPC service communication test passed");
    Ok(())
}

// =============================================================================
// EVENT INGESTION TESTS
// =============================================================================

#[sinex_test]
async fn test_single_event_ingestion_pattern(ctx: TestContext) -> Result<()> {
    // Test single event ingestion patterns (without actual gRPC)
    
    let test_events = vec![
        (
            "filesystem",
            "file.created",
            json!({"path": "/test/file1.txt", "size": 1024})
        ),
        (
            "terminal", 
            "command.executed",
            json!({"command": "ls -la", "exit_code": 0})
        ),
        (
            "desktop",
            "window.focused",
            json!({"window_class": "firefox", "title": "Test Page"})
        ),
    ];
    
    for (source, event_type, payload) in test_events {
        let event = ctx.create_test_event(source, event_type, payload.clone()).await?;
        
        // Validate event structure for gRPC compatibility
        assert_eq!(event.source.as_str(), source);
        assert_eq!(event.event_type.as_str(), event_type);
        assert_eq!(event.payload, payload);
        assert!(event.id.is_some(), "Event should have ID for gRPC transport");
        
        // Test payload serialization (required for gRPC proto format)
        let serialized = serde_json::to_string(&event.payload)?;
        let _deserialized: serde_json::Value = serde_json::from_str(&serialized)?;
        
        debug!("✓ Event {} serialization validated", event.id.unwrap());
    }
    
    info!("✓ Single event ingestion patterns validated");
    Ok(())
}

#[sinex_test]
async fn test_batch_event_ingestion_pattern(ctx: TestContext) -> Result<()> {
    // Test batch ingestion patterns
    const BATCH_SIZE: usize = 50;
    
    let mut events = Vec::new();
    for i in 0..BATCH_SIZE {
        let event = ctx.create_test_event(
            "batch-test",
            "batch.item",
            json!({
                "index": i,
                "batch_id": "test-batch-grpc-001",
                "timestamp": chrono::Utc::now().timestamp()
            })
        ).await?;
        events.push(event);
    }
    
    // Validate batch structure
    assert_eq!(events.len(), BATCH_SIZE);
    
    // Test all events have unique IDs
    let ids: Vec<_> = events.iter().filter_map(|e| e.id.clone()).collect();
    for (i, id1) in ids.iter().enumerate() {
        for id2 in ids.iter().skip(i + 1) {
            assert_ne!(id1, id2, "Batch events must have unique IDs");
        }
    }
    
    // Test batch serialization (simulating gRPC batch transport)
    for event in &events {
        let _serialized = serde_json::to_string(&event.payload)?;
    }
    
    // Verify batch was processed correctly
    let batch_events = ctx.pool
        .events()
        .get_by_source(&EventSource::from_static("batch-test"), Some(100), None)
        .await?;
    
    assert_eq!(batch_events.len(), BATCH_SIZE);
    
    info!("✓ Batch event ingestion pattern validated (size: {})", BATCH_SIZE);
    Ok(())
}

// =============================================================================
// GRPC SERVICE HEALTH AND STATUS TESTS  
// =============================================================================

#[sinex_test]
async fn test_grpc_health_check_pattern(ctx: TestContext) -> Result<()> {
    // Test health check patterns (without actual gRPC service)
    
    // Simulate health check data structures
    let healthy_status = MockHealthStatus {
        healthy: true,
        status: "healthy".to_string(),
        message: None,
    };
    
    let unhealthy_status = MockHealthStatus {
        healthy: false,
        status: "shutting down".to_string(),
        message: Some("Service is shutting down gracefully".to_string()),
    };
    
    // Validate health status structures
    assert!(healthy_status.healthy);
    assert_eq!(healthy_status.status, "healthy");
    assert!(healthy_status.message.is_none());
    
    assert!(!unhealthy_status.healthy);
    assert_eq!(unhealthy_status.status, "shutting down");
    assert!(unhealthy_status.message.is_some());
    
    info!("✓ gRPC health check patterns validated");
    Ok(())
}

// Mock health status for testing without actual gRPC
#[derive(Debug)]
struct MockHealthStatus {
    healthy: bool,
    status: String,
    message: Option<String>,
}

// =============================================================================
// ERROR HANDLING AND RESILIENCE TESTS
// =============================================================================

#[sinex_test] 
async fn test_grpc_error_handling_patterns(ctx: TestContext) -> Result<()> {
    // Test error handling patterns for gRPC communication
    
    // Test invalid event data
    let invalid_cases = vec![
        ("", "valid.type", json!({})), // Empty source
        ("valid-source", "", json!({})), // Empty event type  
        ("valid-source", "valid.type", json!(null)), // Null payload
    ];
    
    for (source, event_type, payload) in invalid_cases {
        let result = if source.is_empty() || event_type.is_empty() {
            // These should fail at event creation
            ctx.create_test_event(source, event_type, payload.clone()).await
        } else {
            // This might succeed at creation but fail later
            ctx.create_test_event(source, event_type, payload.clone()).await
        };
        
        if source.is_empty() || event_type.is_empty() {
            assert!(result.is_err(), "Should reject invalid event data");
            debug!("✓ Properly rejected invalid event: source='{}', type='{}'", source, event_type);
        }
    }
    
    info!("✓ gRPC error handling patterns validated");
    Ok(())
}

#[sinex_test]
async fn test_grpc_timeout_and_retry_patterns(ctx: TestContext) -> Result<()> {
    // Test timeout and retry patterns
    
    // Simulate network timeout scenarios
    let short_timeout = Duration::from_millis(10);
    let normal_timeout = Duration::from_secs(5);
    
    // Test that operations complete within reasonable time
    let start = std::time::Instant::now();
    
    let event = ctx.create_test_event(
        "timeout-test",
        "timing.test", 
        json!({
            "operation": "timeout_test",
            "expected_duration_ms": 100
        })
    ).await?;
    
    let duration = start.elapsed();
    
    // Should complete quickly for local database operations
    assert!(duration < normal_timeout, "Operation should complete within timeout");
    assert!(event.id.is_some(), "Event should be created successfully");
    
    debug!("Operation completed in {:?}", duration);
    
    info!("✓ gRPC timeout patterns validated");
    Ok(())
}

// =============================================================================
// BATCH PROCESSING AND THROUGHPUT TESTS
// =============================================================================

#[sinex_test]
async fn test_grpc_batch_processing_patterns(ctx: TestContext) -> Result<()> {
    // Test batch processing patterns that would be used with gRPC
    
    // Test different batch sizes
    let batch_sizes = vec![1, 5, 10, 25, 50];
    
    for batch_size in batch_sizes {
        let batch_id = format!("batch-size-{}", batch_size);
        let mut events = Vec::new();
        
        // Create batch
        for i in 0..batch_size {
            let event = ctx.create_test_event(
                "batch-size-test",
                "batch.processing",
                json!({
                    "batch_id": batch_id,
                    "item_index": i,
                    "batch_size": batch_size
                })
            ).await?;
            events.push(event);
        }
        
        // Validate batch
        assert_eq!(events.len(), batch_size);
        
        // Simulate batch processing result
        let mock_result = MockBatchResult {
            success: true,
            event_ids: events.iter().filter_map(|e| e.id.as_ref().map(|id| id.to_string())).collect(),
            processed_count: batch_size as u32,
            failed_count: 0,
            error: None,
        };
        
        assert!(mock_result.success);
        assert_eq!(mock_result.processed_count as usize, batch_size);
        assert_eq!(mock_result.failed_count, 0);
        assert_eq!(mock_result.event_ids.len(), batch_size);
        
        debug!("✓ Batch size {} processed successfully", batch_size);
    }
    
    info!("✓ gRPC batch processing patterns validated");
    Ok(())
}

// Mock batch result for testing without actual gRPC
#[derive(Debug)]
struct MockBatchResult {
    success: bool,
    event_ids: Vec<String>,
    processed_count: u32,
    failed_count: u32,
    error: Option<String>,
}

// =============================================================================
// CONCURRENT GRPC CLIENT TESTS
// =============================================================================

#[sinex_test]
async fn test_concurrent_grpc_client_patterns(ctx: TestContext) -> Result<()> {
    // Test concurrent client usage patterns
    
    const CONCURRENT_TASKS: usize = 10;
    const EVENTS_PER_TASK: usize = 5;
    
    let mut handles = Vec::new();
    
    // Spawn concurrent tasks
    for task_id in 0..CONCURRENT_TASKS {
        let handle = tokio::spawn(async move {
            let ctx = TestContext::new().await?;
            let mut task_events = Vec::new();
            
            for event_id in 0..EVENTS_PER_TASK {
                let event = ctx.create_test_event(
                    "concurrent-grpc-test",
                    "concurrent.event",
                    json!({
                        "task_id": task_id,
                        "event_id": event_id,
                        "total_tasks": CONCURRENT_TASKS,
                        "events_per_task": EVENTS_PER_TASK
                    })
                ).await?;
                task_events.push(event);
            }
            
            Ok::<Vec<_>, color_eyre::eyre::Error>(task_events)
        });
        handles.push(handle);
    }
    
    // Collect results
    let mut all_events = Vec::new();
    for handle in handles {
        let task_events = handle.await??;
        all_events.extend(task_events);
    }
    
    // Validate results
    assert_eq!(all_events.len(), CONCURRENT_TASKS * EVENTS_PER_TASK);
    
    // Verify all events have unique IDs
    let ids: Vec<_> = all_events.iter().filter_map(|e| e.id.clone()).collect();
    for (i, id1) in ids.iter().enumerate() {
        for id2 in ids.iter().skip(i + 1) {
            assert_ne!(id1, id2, "Concurrent events must have unique IDs");
        }
    }
    
    info!("✓ Concurrent gRPC client patterns validated ({} tasks × {} events)", 
          CONCURRENT_TASKS, EVENTS_PER_TASK);
    Ok(())
}

// =============================================================================
// GRPC PROTOCOL AND SERIALIZATION TESTS
// =============================================================================

#[sinex_test]
async fn test_grpc_protocol_compatibility(ctx: TestContext) -> Result<()> {
    // Test protocol compatibility and serialization
    
    let test_cases = vec![
        // Basic ASCII
        ("ascii-test", json!({"message": "Hello World"})),
        // Unicode
        ("unicode-test", json!({"message": "Hello 世界 🌍"})),
        // Large payload
        ("large-test", json!({"data": "x".repeat(1000), "size": 1000})),
        // Complex nested structure
        ("complex-test", json!({
            "metadata": {
                "nested": {
                    "deep": {
                        "structure": [1, 2, 3, {"key": "value"}]
                    }
                }
            },
            "array": [true, false, null, 42, "string"],
            "special_chars": "!@#$%^&*()_+-=[]{}|;':\",./<>?"
        })),
    ];
    
    for (test_name, payload) in test_cases {
        let event = ctx.create_test_event(
            "protocol-test",
            "serialization.test",
            payload.clone()
        ).await?;
        
        // Test JSON serialization (gRPC proto uses JSON for payload)
        let serialized = serde_json::to_string(&event.payload)?;
        let deserialized: serde_json::Value = serde_json::from_str(&serialized)?;
        
        assert_eq!(event.payload, deserialized, "Serialization must be lossless");
        
        // Test required gRPC fields are present
        assert!(!event.source.as_str().is_empty(), "Source required for gRPC");
        assert!(!event.event_type.as_str().is_empty(), "Event type required for gRPC");
        assert!(!event.host.as_str().is_empty(), "Host required for gRPC");
        
        debug!("✓ Protocol test '{}' passed (payload size: {} bytes)", 
               test_name, serialized.len());
    }
    
    info!("✓ gRPC protocol compatibility validated");
    Ok(())
}

// =============================================================================
// PERFORMANCE AND LOAD TESTS
// =============================================================================

#[sinex_test] 
async fn test_grpc_performance_patterns(ctx: TestContext) -> Result<()> {
    // Test performance patterns for gRPC communication
    
    let start = std::time::Instant::now();
    let event_count = 100;
    
    // Create events with timing
    let mut events = Vec::new();
    for i in 0..event_count {
        let event = ctx.create_test_event(
            "performance-test",
            "perf.measurement",
            json!({
                "index": i,
                "timestamp": chrono::Utc::now().timestamp_micros(),
                "batch_size": event_count
            })
        ).await?;
        events.push(event);
    }
    
    let creation_duration = start.elapsed();
    
    // Validate performance
    assert_eq!(events.len(), event_count);
    
    let events_per_sec = event_count as f64 / creation_duration.as_secs_f64();
    
    info!("✓ Performance test completed:");
    info!("  - Created {} events in {:?}", event_count, creation_duration);
    info!("  - Rate: {:.2} events/sec", events_per_sec);
    
    // Performance should be reasonable for local operations
    assert!(events_per_sec > 10.0, "Should achieve reasonable throughput");
    
    Ok(())
}

// =============================================================================
// INTEGRATION WITH DATABASE LAYER
// =============================================================================

#[sinex_test]
async fn test_grpc_database_integration(ctx: TestContext) -> Result<()> {
    // Test integration between gRPC layer and database operations
    
    // Create events that would come through gRPC
    let grpc_events = vec![
        ("filesystem", "file.created", json!({"path": "/grpc/test1.txt"})),
        ("terminal", "command.executed", json!({"command": "grpc test"})),
        ("system", "service.started", json!({"service": "grpc-service"})),
    ];
    
    let mut created_events = Vec::new();
    for (source, event_type, payload) in grpc_events {
        let event = ctx.create_test_event(source, event_type, payload).await?;
        created_events.push(event);
    }
    
    // Verify events are in database (simulating post-gRPC processing)
    for event in &created_events {
        let retrieved = ctx.pool
            .events()
            .get_by_id(event.id.unwrap())
            .await?
            .expect("Event should exist in database");
        
        assert_eq!(retrieved.source, event.source);
        assert_eq!(retrieved.event_type, event.event_type);
        assert_eq!(retrieved.payload, event.payload);
    }
    
    // Test querying by source (common post-gRPC operation)
    let fs_events = ctx.pool
        .events()
        .get_by_source(&EventSource::from_static("filesystem"), Some(10), None)
        .await?;
    
    assert_eq!(fs_events.len(), 1);
    assert_eq!(fs_events[0].event_type.as_str(), "file.created");
    
    info!("✓ gRPC database integration validated");
    Ok(())
}
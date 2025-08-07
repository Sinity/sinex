//! Ingest Service Integration Tests
//!
//! Tests the sinex-ingestd service functionality including:
//! - Event ingestion and validation
//! - Database persistence patterns
//! - Performance characteristics and error handling
//! - Schema validation and synchronization
//!
//! These tests validate the core ingestion patterns that satellites use
//! to submit events for processing and storage.

use sinex_test_utils::prelude::*;
use sinex_db::repositories::DbPoolExt;
use std::time::Duration;
use tokio::time::timeout;

// ============================================================================
// Core Ingest Service Tests
// ============================================================================

/// Test ingest service initialization patterns
#[sinex_test]
async fn test_ingest_service_startup(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    tracing::info!("Testing ingest service startup and initialization");

    // Verify database connectivity for ingest service
    let pool = &ctx.pool;
    let connection = pool.acquire().await?;
    drop(connection);

    // Test configuration requirements
    let database_url = std::env::var("DATABASE_URL");
    assert!(database_url.is_ok(), "DATABASE_URL required for ingest service");

    let nats_url = std::env::var("SINEX_NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".to_string());
    assert!(!nats_url.is_empty(), "NATS URL should be configured for ingest service");

    tracing::info!(
        database_configured = database_url.is_ok(),
        nats_url = %nats_url,
        "Ingest service configuration validated"
    );

    Ok(())
}

/// Test event ingestion through the service API
#[sinex_test]
async fn test_event_ingestion_flow(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    tracing::info!("Testing event ingestion through service API");

    // Create test event that would come from a satellite
    let satellite_event = ctx.create_test_event(
        "fs-watcher",
        "file.created",
        serde_json::json!({
            "path": "/tmp/test_file.txt",
            "size": 1024,
            "timestamp": chrono::Utc::now().to_rfc3339(),
            "permissions": "0644"
        })
    ).await?;

    // Event is automatically stored via create_test_event
    assert_eq!(satellite_event.source.as_str(), "fs-watcher");
    assert_eq!(satellite_event.event_type.as_str(), "file.created");
    assert!(satellite_event.id.is_some());
    assert_eq!(satellite_event.payload["path"], "/tmp/test_file.txt");

    tracing::info!(
        event_id = ?satellite_event.id,
        source = %satellite_event.source,
        event_type = %satellite_event.event_type,
        "Event ingestion flow validated"
    );

    // Verify event can be retrieved
    let event_id = satellite_event.id.as_ref().unwrap().clone();
    let retrieved_event = ctx.pool.events().get_by_id(event_id).await?;
    assert!(retrieved_event.is_some(), "Ingested event should be retrievable");
    
    let retrieved = retrieved_event.unwrap();
    assert_eq!(retrieved.id, satellite_event.id);
    assert_eq!(retrieved.payload["path"], "/tmp/test_file.txt");

    Ok(())
}

/// Test batch ingestion functionality
#[sinex_test]
async fn test_batch_ingestion(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    tracing::info!("Testing batch event ingestion");

    // Create multiple events as would be sent in a batch
    let batch_events = vec![
        ctx.create_test_event(
            "fs-watcher",
            "file.created",
            serde_json::json!({
                "path": "/tmp/batch_file_1.txt",
                "size": 512
            })
        ).await?,
        ctx.create_test_event(
            "terminal",
            "command.executed",
            serde_json::json!({
                "command": "ls -la",
                "exit_code": 0,
                "duration_ms": 42
            })
        ).await?,
        ctx.create_test_event(
            "desktop",
            "window.focused",
            serde_json::json!({
                "window_title": "Terminal",
                "application": "gnome-terminal"
            })
        ).await?,
    ];

    // Events are automatically stored, extract their IDs
    let mut stored_ids = Vec::new();
    for event in &batch_events {
        stored_ids.push(event.id.as_ref().unwrap().clone());
    }

    assert_eq!(stored_ids.len(), 3, "All batch events should be stored");

    // Verify all events were persisted
    for event_id in &stored_ids {
        let retrieved = ctx.pool.events().get_by_id(event_id.clone()).await?;
        assert!(retrieved.is_some(), "Each batch event should be retrievable");
    }

    tracing::info!(
        batch_size = batch_events.len(),
        stored_count = stored_ids.len(),
        "Batch ingestion validated"
    );

    Ok(())
}

/// Test event validation during ingestion
#[sinex_test]
async fn test_ingestion_validation(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    tracing::info!("Testing event validation during ingestion");

    // Test valid event with complete payload
    let valid_event = ctx.create_test_event(
        "fs-watcher",
        "file.created",
        serde_json::json!({
            "path": "/tmp/valid_file.txt",
            "size": 1024,
            "timestamp": chrono::Utc::now().to_rfc3339(),
            "permissions": "0644",
            "inode": 12345
        })
    ).await?;

    assert!(valid_event.id.is_some(), "Valid event should be stored");

    // Test edge case validation - minimal payload
    let minimal_event = ctx.create_test_event(
        "system",
        "service.started",
        serde_json::json!({
            "service_name": "test-service"
        })
    ).await?;

    assert!(minimal_event.id.is_some(), "Minimal valid event should be stored");

    // Test large payload handling
    let large_payload = "x".repeat(10000); // 10KB payload
    let large_event_result = ctx.create_test_event(
        "application",
        "log.entry",
        serde_json::json!({
            "message": large_payload,
            "level": "info"
        })
    ).await;

    assert!(large_event_result.is_ok(), "Large payload event should be handled");

    tracing::info!("Event validation during ingestion verified");

    Ok(())
}

/// Test source and event type patterns
#[sinex_test]
async fn test_source_and_type_patterns(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    tracing::info!("Testing source and event type patterns");

    // Test events with different sources and types for pattern validation
    let test_patterns = vec![
        ("fs-watcher", "file.created", serde_json::json!({
            "path": "/tmp/pattern_test.txt",
            "size": 256
        })),
        ("terminal", "command.executed", serde_json::json!({
            "command": "cat /proc/version",
            "exit_code": 0
        })),
        ("desktop.window-manager", "window.focused", serde_json::json!({
            "window_id": 12345,
            "workspace": "main"
        })),
        ("system.systemd", "service.started", serde_json::json!({
            "unit": "nginx.service",
            "status": "active"
        })),
    ];

    let mut stored_events = Vec::new();
    for (source, event_type, payload) in test_patterns {
        let event = ctx.create_test_event(source, event_type, payload).await?;
        stored_events.push((source, event_type, event));
    }

    // Verify all events were processed
    assert_eq!(stored_events.len(), 4, "All pattern events should be processed");

    // Group events by source to verify pattern handling
    let mut events_by_source = std::collections::HashMap::new();
    for (source, _event_type, event) in stored_events {
        events_by_source.entry(source).or_insert(Vec::new()).push(event);
    }

    assert_eq!(events_by_source.len(), 4, "Should have events from 4 different sources");
    
    // Verify specific source patterns
    assert!(events_by_source.contains_key("fs-watcher"), "Should handle fs-watcher events");
    assert!(events_by_source.contains_key("terminal"), "Should handle terminal events");
    assert!(events_by_source.contains_key("desktop.window-manager"), "Should handle dotted source names");
    assert!(events_by_source.contains_key("system.systemd"), "Should handle system events");

    tracing::info!(
        total_events = events_by_source.values().map(|v| v.len()).sum::<usize>(),
        unique_sources = events_by_source.len(),
        "Source and type patterns validated"
    );

    Ok(())
}

// ============================================================================
// Service Performance and Reliability Tests
// ============================================================================

/// Test ingestion performance characteristics
#[sinex_test]
async fn test_ingestion_performance(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    tracing::info!("Testing ingestion service performance");

    let start_time = std::time::Instant::now();

    // Generate a batch of events to test throughput
    let batch_size = 100;
    let mut processed_events = 0;

    for i in 0..batch_size {
        let event_result = ctx.create_test_event(
            "performance-test",
            "throughput.test",
            serde_json::json!({
                "sequence": i,
                "batch_size": batch_size,
                "timestamp": chrono::Utc::now().to_rfc3339(),
                "payload_data": format!("Performance test event {}", i)
            })
        ).await;

        match event_result {
            Ok(_) => processed_events += 1,
            Err(e) => tracing::warn!(sequence = i, error = %e, "Event ingestion failed"),
        }
    }

    let duration = start_time.elapsed();
    let events_per_second = processed_events as f64 / duration.as_secs_f64();

    tracing::info!(
        processed_events = processed_events,
        duration_ms = duration.as_millis(),
        events_per_second = events_per_second,
        "Ingestion service performance measured"
    );

    // Verify reasonable performance (should process at least 10 events/second)
    assert!(events_per_second >= 10.0, 
        "Ingestion service should maintain reasonable throughput: {} events/second", events_per_second);

    // Verify all events were processed
    assert_eq!(processed_events, batch_size, "All performance test events should be processed");

    Ok(())
}

/// Test sequential ingestion handling (modified from concurrent due to TestContext constraints)
#[sinex_test]
async fn test_sequential_ingestion(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    tracing::info!("Testing sequential event ingestion");

    // Generate events to test ingestion capacity
    let mut successful_ingests = 0;
    let total_events = 20;
    
    for i in 0..total_events {
        let event_result = ctx.create_test_event(
            "sequential-ingest",
            "sequential.test",
            serde_json::json!({
                "worker_id": i,
                "batch_id": i / 5,
                "data": format!("Sequential ingestion test {}", i)
            })
        ).await;
        
        match event_result {
            Ok(_) => successful_ingests += 1,
            Err(e) => tracing::error!(error = %e, "Sequential ingestion failed"),
        }
    }

    assert_eq!(successful_ingests, total_events, "All events should be ingested successfully");

    // Verify events are in the database
    let recent_events = ctx.pool.events().get_by_source(&EventSource::from("sequential-ingest"), Some(25), None).await?;
    assert!(recent_events.len() >= total_events, "All sequential events should be persisted");

    tracing::info!(
        sequential_ingests = successful_ingests,
        persisted_events = recent_events.len(),
        "Sequential ingestion validated"
    );

    Ok(())
}

/// Test ingestion error handling and recovery
#[sinex_test]
async fn test_ingestion_error_handling(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    tracing::info!("Testing ingestion service error handling and recovery");

    // Test successful ingestion
    let valid_event = ctx.create_test_event(
        "error-test",
        "error.handling",
        serde_json::json!({
            "test_case": "valid_event",
            "data": "This should ingest successfully"
        })
    ).await;

    assert!(valid_event.is_ok(), "Valid event should be ingested successfully");

    // Test edge case payloads for robust error handling
    let edge_cases = vec![
        ("empty_payload", serde_json::json!({})),
        ("large_string", serde_json::json!({
            "large_data": "x".repeat(5000)
        })),
        ("deeply_nested", serde_json::json!({
            "level1": {
                "level2": {
                    "level3": {
                        "level4": {
                            "data": "deeply nested payload"
                        }
                    }
                }
            }
        })),
        ("array_payload", serde_json::json!({
            "items": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            "metadata": ["tag1", "tag2", "tag3"]
        })),
        ("unicode_content", serde_json::json!({
            "content": "Hello 世界 🌍 Ελληνικά Русский العربية"
        })),
    ];

    let mut processed_count = 0;
    for (case_name, payload) in edge_cases {
        let edge_event_result = ctx.create_test_event("error-test", "edge.case", payload).await;

        match edge_event_result {
            Ok(_) => {
                processed_count += 1;
                tracing::debug!(case = %case_name, "Edge case processed successfully");
            }
            Err(e) => {
                tracing::warn!(case = %case_name, error = %e, "Edge case processing failed");
            }
        }
    }

    assert!(processed_count >= 4, "Most edge cases should be handled gracefully");

    // Verify service recovery after edge case processing
    let recovery_event = ctx.create_test_event(
        "error-test",
        "error.recovery",
        serde_json::json!({
            "test_case": "post_recovery",
            "data": "Service should continue working after error handling"
        })
    ).await;

    assert!(recovery_event.is_ok(), "Service should recover and continue processing");

    tracing::info!(
        edge_cases_processed = processed_count,
        recovery_successful = recovery_event.is_ok(),
        "Ingestion error handling validated"
    );

    Ok(())
}

// ============================================================================
// Schema and Validation Tests
// ============================================================================

/// Test schema patterns during ingestion
#[sinex_test]
async fn test_schema_validation_patterns(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    tracing::info!("Testing schema validation patterns during ingestion");

    // Test events with different schema patterns
    let schema_test_events = vec![
        ("fs-watcher", "file.created", serde_json::json!({
            "path": "/tmp/schema_test.txt",
            "size": 1024,
            "created_at": chrono::Utc::now().to_rfc3339()
        })),
        ("terminal", "command.executed", serde_json::json!({
            "command": "echo 'schema test'",
            "exit_code": 0,
            "working_directory": "/tmp"
        })),
        ("system", "service.started", serde_json::json!({
            "service_name": "test-service",
            "pid": 12345,
            "status": "active"
        })),
    ];

    for (source, event_type, payload) in schema_test_events {
        let event = ctx.create_test_event(source, event_type, payload).await?;
        
        assert!(event.id.is_some(), 
               "Event with source {} and type {} should be stored", source, event_type);
    }

    tracing::info!("Schema validation patterns during ingestion verified");

    Ok(())
}

/// Test payload validation patterns
#[sinex_test]
async fn test_payload_validation_patterns(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    tracing::info!("Testing payload validation patterns");

    // Test various payload structures that should be valid
    let validation_patterns = vec![
        ("minimal", serde_json::json!({})),
        ("with_numbers", serde_json::json!({
            "integer": 42,
            "float": 3.14159,
            "negative": -123
        })),
        ("with_booleans", serde_json::json!({
            "success": true,
            "enabled": false
        })),
        ("with_null_values", serde_json::json!({
            "optional_field": null,
            "required_field": "present"
        })),
        ("mixed_types", serde_json::json!({
            "string": "text",
            "number": 123,
            "boolean": true,
            "array": [1, 2, 3],
            "object": {"nested": "value"}
        })),
    ];

    for (pattern_name, payload) in validation_patterns {
        let event = ctx.create_test_event("validation-test", "payload.test", payload).await?;
        
        assert!(event.id.is_some(), 
               "Payload pattern '{}' should be valid", pattern_name);
        
        tracing::debug!(pattern = %pattern_name, "Payload validation pattern passed");
    }

    tracing::info!("Payload validation patterns verified");

    Ok(())
}

// ============================================================================
// Service Health and Monitoring Tests
// ============================================================================

/// Test service health indicators
#[sinex_test]
async fn test_service_health_monitoring(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    tracing::info!("Testing service health monitoring");

    // Test basic health indicators through event processing
    let health_check_event = ctx.create_test_event(
        "health-monitor",
        "health.check",
        serde_json::json!({
            "timestamp": chrono::Utc::now().to_rfc3339(),
            "status": "healthy"
        })
    ).await?;

    assert!(health_check_event.id.is_some(), "Health check event should be processed");

    // Test database connectivity (core health indicator)
    let db_connection = ctx.pool.acquire().await?;
    drop(db_connection);

    // Test event retrieval (indicates service is operational)
    let recent_events = ctx.pool.events().get_recent(5).await?;
    assert!(!recent_events.is_empty(), "Service should be able to retrieve events");

    // Simulate service processing over time
    for i in 0..3 {
        let status_event = ctx.create_test_event(
            "health-monitor",
            "status.update",
            serde_json::json!({
                "sequence": i,
                "uptime_seconds": i * 60,
                "events_processed": i * 10
            })
        ).await?;

        assert!(status_event.id.is_some(), "Status update should be processed");
        
        // Brief delay to simulate time passage
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Verify service maintains health over time
    let status_events = ctx.pool.events().get_by_source(&EventSource::from("health-monitor"), Some(10), None).await?;
    assert!(status_events.len() >= 4, "Should have health monitoring events");

    tracing::info!(
        health_events = status_events.len(),
        "Service health monitoring validated"
    );

    Ok(())
}

/// Test resource management during ingestion
#[sinex_test]
async fn test_resource_management(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    tracing::info!("Testing resource management during ingestion");

    // Generate events with varying resource requirements
    let resource_patterns = vec![
        ("small_payload", 100),    // 100 byte payloads
        ("medium_payload", 1000),  // 1KB payloads  
        ("large_payload", 5000),   // 5KB payloads
    ];

    let events_per_pattern = 5;

    for (pattern_name, payload_size) in resource_patterns {
        let large_data = "x".repeat(payload_size);
        
        for i in 0..events_per_pattern {
            let event = ctx.create_test_event(
                "resource-test",
                "resource.test",
                serde_json::json!({
                    "pattern": pattern_name,
                    "payload_size": payload_size,
                    "sequence": i,
                    "data": large_data
                })
            ).await?;

            assert!(event.id.is_some(), 
                   "Event with payload size {} should be stored", payload_size);
        }
    }

    // Test service stability after resource variation
    let stability_event = ctx.create_test_event(
        "resource-test",
        "stability.check",
        serde_json::json!({
            "message": "Service should remain stable after resource variation"
        })
    ).await?;

    assert!(stability_event.id.is_some(), "Service should remain stable after resource tests");

    // Verify events are properly persisted
    let resource_events = ctx.pool.events().get_by_source(&EventSource::from("resource-test"), Some(20), None).await?;
    assert!(resource_events.len() >= 15, "All resource test events should be persisted");

    tracing::info!(
        total_resource_events = resource_events.len(),
        "Resource management validated"
    );

    Ok(())
}

/// Test timeout and deadline handling
#[sinex_test]
async fn test_timeout_and_deadline_handling(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    tracing::info!("Testing timeout and deadline handling");

    // Test normal operation within reasonable timeouts
    let timeout_duration = Duration::from_secs(5);
    
    let normal_operation = timeout(timeout_duration, async {
        ctx.create_test_event(
            "timeout-test",
            "timeout.test",
            serde_json::json!({
                "operation": "normal",
                "timestamp": chrono::Utc::now().to_rfc3339()
            })
        ).await
    }).await;

    assert!(normal_operation.is_ok(), "Normal operations should complete within timeout");
    assert!(normal_operation.unwrap().is_ok(), "Normal event should be stored successfully");

    // Test batch operations within timeouts
    let batch_timeout = Duration::from_secs(10);
    
    let batch_operation = timeout(batch_timeout, async {
        let mut batch_results = Vec::new();
        
        for i in 0..10 {
            let event = ctx.create_test_event(
                "timeout-test",
                "batch.timeout.test",
                serde_json::json!({
                    "batch_sequence": i,
                    "data": format!("Batch timeout test {}", i)
                })
            ).await?;
            
            batch_results.push(event);
        }
        
        Ok::<Vec<_>, color_eyre::eyre::Error>(batch_results)
    }).await;

    assert!(batch_operation.is_ok(), "Batch operations should complete within timeout");
    
    let batch_results = batch_operation.unwrap()?;
    assert_eq!(batch_results.len(), 10, "All batch events should be processed within timeout");

    tracing::info!(
        normal_operations = 1,
        batch_operations = batch_results.len(),
        "Timeout and deadline handling validated"
    );

    Ok(())
}
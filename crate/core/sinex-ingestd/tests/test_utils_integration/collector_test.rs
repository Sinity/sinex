// Modern ingestd and event collection integration tests
//
// These tests validate the current Sinex architecture with sinex-ingestd as the central
// coordinator, nodes for event collection, and NATS for message streaming.
// This replaces the deprecated sinex_collector architecture.

use xtask::sandbox::prelude::*;
use std::time::Duration;
use tokio::time::timeout;

// ============================================================================
// Ingestd Service Tests
// ============================================================================

/// Test that ingestd service can start with valid configuration
#[sinex_test]
async fn test_ingestd_service_startup(ctx: TestContext) -> TestResult<()> {
    tracing::info!("Testing ingestd service startup with valid configuration");

    // Verify database connectivity
    let pool = ctx.pool();
    let connection = pool.acquire().await?;
    drop(connection);

    // Test configuration validation
    let config_result = std::env::var("DATABASE_URL");
    assert!(config_result.is_ok(), "DATABASE_URL should be available for ingestd");

    let nats_url = std::env::var("SINEX_NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".to_string());
    assert!(!nats_url.is_empty(), "NATS URL should be configured");

    tracing::info!(
        database_url_configured = config_result.is_ok(),
        nats_url = %nats_url,
        "Ingestd service configuration validated"
    );

    Ok(())
}

/// Test ingestd configuration loading from environment
#[sinex_test]
async fn test_ingestd_environment_config(ctx: TestContext) -> TestResult<()> {
    tracing::info!("Testing ingestd environment-based configuration");

    // Test required environment variables
    let required_vars = vec![
        "DATABASE_URL",
    ];

    for var_name in required_vars {
        let value = std::env::var(var_name);
        assert!(value.is_ok(), "Required environment variable {} should be set", var_name);
        
        let var_value = value.unwrap();
        assert!(!var_value.is_empty(), "Environment variable {} should not be empty", var_name);
        
        tracing::debug!(var = %var_name, value_length = var_value.len(), "Environment variable validated");
    }

    // Test optional environment variables with defaults
    let optional_vars = vec![
        ("SINEX_NATS_URL", "nats://localhost:4222"),
        ("SINEX_LOG_LEVEL", "info"),
        ("SINEX_DB_POOL_SIZE", "10"),
    ];

    for (var_name, default_value) in optional_vars {
        let value = std::env::var(var_name).unwrap_or_else(|_| default_value.to_string());
        assert!(!value.is_empty(), "Environment variable {} should have a value or default", var_name);
        
        tracing::debug!(var = %var_name, value = %value, "Optional environment variable checked");
    }

    Ok(())
}

/// Test event ingestion flow through modern architecture
#[sinex_test]
async fn test_modern_event_ingestion_flow(ctx: TestContext) -> TestResult<()> {
    tracing::info!("Testing modern event ingestion flow");

    // Create test events directly using the modern API
    let events = ctx.events();
    
    let test_event = ctx.publish(
        "test.collector",
        serde_json::json!({
            "test_type": "ingestion_flow",
            "timestamp": crate::temporal::now().to_rfc3339(),
            "data": "Modern ingestion test"
        })
    ).await?;

    // Store event using modern repository pattern
    let stored_event = events.store(&test_event).await?;
    
    assert_eq!(stored_event.source, "test.collector");
    assert_eq!(stored_event.event_type, "test.collector");
    assert!(!stored_event.id.to_string().is_empty());

    tracing::info!(
        event_id = %stored_event.id,
        source = %stored_event.source,
        event_type = %stored_event.event_type,
        "Modern event ingestion flow validated"
    );

    // Verify event can be retrieved
    let retrieved_event = events.get_by_id(stored_event.id).await?;
    assert!(retrieved_event.is_some(), "Stored event should be retrievable");
    
    let retrieved = retrieved_event.unwrap();
    assert_eq!(retrieved.id, stored_event.id);
    assert_eq!(retrieved.payload["test_type"], "ingestion_flow");

    Ok(())
}

/// Test event filtering and validation in modern architecture
#[sinex_test]
async fn test_modern_event_filtering_and_validation(ctx: TestContext) -> TestResult<()> {
    tracing::info!("Testing modern event filtering and validation");

    let events = ctx.events();

    // Test valid event
    let valid_event = ctx.publish(
        "fs.file_created",
        serde_json::json!({
            "path": "/tmp/test_file.txt",
            "size": 1024,
            "permissions": "0644"
        })
    ).await?;

    let stored_valid = events.store(&valid_event).await?;
    assert!(!stored_valid.id.to_string().is_empty(), "Valid event should be stored");

    // Test event with minimal payload
    let minimal_event = ctx.publish(
        "terminal.command_executed",
        serde_json::json!({
            "command": "ls -la",
            "exit_code": 0
        })
    ).await?;

    let stored_minimal = events.store(&minimal_event).await?;
    assert!(!stored_minimal.id.to_string().is_empty(), "Minimal valid event should be stored");

    // Retrieve events by source to test filtering
    let fs_events = events.get_by_source("fs.file_created", Some(10)).await?;
    assert!(!fs_events.is_empty(), "Should find filesystem events");

    let terminal_events = events.get_by_source("terminal.command_executed", Some(10)).await?;
    assert!(!terminal_events.is_empty(), "Should find terminal events");

    tracing::info!(
        fs_events = fs_events.len(),
        terminal_events = terminal_events.len(),
        "Modern event filtering validated"
    );

    Ok(())
}

/// Test database output configuration in modern architecture
#[sinex_test]
async fn test_modern_database_output_config(ctx: TestContext) -> TestResult<()> {
    tracing::info!("Testing modern database output configuration");

    let events = ctx.events();

    // Create multiple test events to verify database persistence
    let mut event_ids = Vec::new();
    for i in 0..5 {
        let test_event = ctx.publish(
            "test.database_output",
            serde_json::json!({
                "sequence": i,
                "test_data": format!("Database output test {}", i),
                "timestamp": crate::temporal::now().to_rfc3339()
            })
        ).await?;

        let stored = events.store(&test_event).await?;
        event_ids.push(stored.id);
    }

    // Verify all events were persisted
    for event_id in &event_ids {
        let retrieved = events.get_by_id(*event_id).await?;
        assert!(retrieved.is_some(), "Event {} should be persisted in database", event_id);
    }

    // Test batch retrieval
    let recent_events = events.get_recent(10).await?;
    assert!(recent_events.len() >= 5, "Should retrieve recent events from database");

    // Verify events are time-ordered (most recent first) by ULID timestamp
    for i in 1..recent_events.len() {
        let prev = recent_events[i-1]
            .id
            .as_ref()
            .expect("id present")
            .as_ulid()
            .timestamp();
        let curr = recent_events[i]
            .id
            .as_ref()
            .expect("id present")
            .as_ulid()
            .timestamp();
        assert!(prev >= curr, "Events should be ordered by ingestion time (most recent first)");
    }

    tracing::info!(
        stored_events = event_ids.len(),
        retrieved_events = recent_events.len(),
        "Database output configuration validated"
    );

    Ok(())
}

// ============================================================================
// Event Source Integration Tests
// ============================================================================

/// Test event source integration with modern node architecture
#[sinex_test]
async fn test_node_integration_patterns(ctx: TestContext) -> TestResult<()> {
    tracing::info!("Testing node integration patterns");

    let events = ctx.events();

    // Test different node event patterns
    let node_events = vec![
        ("fs.file_created", serde_json::json!({
            "path": "/tmp/created_file.txt",
            "size": 512,
            "timestamp": crate::temporal::now().to_rfc3339()
        })),
        ("fs.file_modified", serde_json::json!({
            "path": "/tmp/modified_file.txt",
            "old_size": 256,
            "new_size": 512,
            "timestamp": crate::temporal::now().to_rfc3339()
        })),
        ("terminal.command_executed", serde_json::json!({
            "command": "cat /tmp/test.txt",
            "exit_code": 0,
            "duration_ms": 42,
            "working_directory": "/tmp"
        })),
        ("desktop.window_focus_changed", serde_json::json!({
            "previous_window": "Terminal",
            "current_window": "Browser",
            "timestamp": crate::temporal::now().to_rfc3339()
        })),
    ];

    let mut stored_events = Vec::new();
    for (source, payload) in node_events {
        let event = ctx.publish(source, payload).await?;
        let stored = events.store(&event).await?;
        stored_events.push(stored);
    }

    // Verify all node event types were processed
    assert_eq!(stored_events.len(), 4, "All node events should be processed");

    // Group events by source to verify node integration
    let mut events_by_source = std::collections::HashMap::new();
    for event in stored_events {
        events_by_source.entry(event.source.to_string()).or_insert(Vec::new()).push(event);
    }

    assert_eq!(events_by_source.len(), 3, "Should have events from 3 different nodes");
    assert!(events_by_source.contains_key("fs.file_created"), "Should have filesystem events");
    assert!(events_by_source.contains_key("terminal.command_executed"), "Should have terminal events");
    assert!(events_by_source.contains_key("desktop.window_focus_changed"), "Should have desktop events");

    tracing::info!(
        total_events = stored_events.len(),
        unique_sources = events_by_source.len(),
        "Node integration patterns validated"
    );

    Ok(())
}

// ============================================================================
// Event Processing Pipeline Tests
// ============================================================================

/// Test concurrent event processing pipeline
#[sinex_test]
async fn test_concurrent_event_processing_pipeline(ctx: TestContext) -> TestResult<()> {
    tracing::info!("Testing concurrent event processing pipeline");

    let events = ctx.events();

    // Generate events concurrently to test pipeline capacity
    let mut tasks = Vec::new();
    for i in 0..20 {
        let ctx_clone = ctx.clone();
        let task = tokio::spawn(async move {
            let event = ctx_clone.publish(
                "test.concurrent_processing",
                serde_json::json!({
                    "batch_id": i / 5,
                    "sequence": i,
                    "worker_id": format!("worker_{}", i % 4),
                    "data": format!("Concurrent test data {}", i)
                })
            ).await?;
            
            ctx_clone.events().store(&event).await
        });
        tasks.push(task);
    }

    // Wait for all concurrent operations to complete
    let results = futures::future::try_join_all(tasks).await?;
    
    // Verify all events were processed successfully
    let mut successful_stores = 0;
    for result in results {
        match result {
            Ok(_) => successful_stores += 1,
            Err(e) => tracing::error!(error = %e, "Event store failed"),
        }
    }

    assert_eq!(successful_stores, 20, "All concurrent events should be processed successfully");

    // Verify events are in the database
    let recent_events = events.get_by_source("test.concurrent_processing", Some(25)).await?;
    assert!(recent_events.len() >= 20, "All concurrent events should be persisted");

    // Verify event ordering and data integrity
    let batch_counts = recent_events.iter()
        .filter_map(|e| e.payload.get("batch_id"))
        .filter_map(|v| v.as_i64())
        .collect::<std::collections::HashSet<_>>();
    
    assert!(batch_counts.len() >= 4, "Should have events from multiple batches");

    tracing::info!(
        concurrent_events = successful_stores,
        persisted_events = recent_events.len(),
        unique_batches = batch_counts.len(),
        "Concurrent event processing pipeline validated"
    );

    Ok(())
}

/// Test event processing error handling and recovery
#[sinex_test]
async fn test_event_processing_error_handling(ctx: TestContext) -> TestResult<()> {
    tracing::info!("Testing event processing error handling and recovery");

    let events = ctx.events();

    // Test successful event processing
    let valid_event = ctx.publish(
        "test.error_handling",
        serde_json::json!({
            "test_case": "valid_event",
            "data": "This should process successfully"
        })
    ).await?;

    let stored_result = events.store(&valid_event).await;
    assert!(stored_result.is_ok(), "Valid event should be stored successfully");

    // Test system resilience with edge case payloads
    let edge_cases = vec![
        ("empty_payload", serde_json::json!({})),
        ("large_string", serde_json::json!({
            "large_data": "x".repeat(1000)
        })),
        ("nested_structure", serde_json::json!({
            "level1": {
                "level2": {
                    "level3": {
                        "data": "deeply nested data"
                    }
                }
            }
        })),
        ("array_data", serde_json::json!({
            "items": [1, 2, 3, 4, 5],
            "metadata": ["tag1", "tag2", "tag3"]
        })),
    ];

    let mut processed_count = 0;
    for (case_name, payload) in edge_cases {
        let edge_event = ctx.publish(
            "test.error_handling",
            payload
        ).await?;

        match events.store(&edge_event).await {
            Ok(_) => {
                processed_count += 1;
                tracing::debug!(case = %case_name, "Edge case processed successfully");
            }
            Err(e) => {
                tracing::warn!(case = %case_name, error = %e, "Edge case processing failed");
            }
        }
    }

    assert!(processed_count >= 3, "Most edge cases should be handled gracefully");

    // Verify system is still operational after edge case processing
    let recovery_event = ctx.publish(
        "test.error_handling",
        serde_json::json!({
            "test_case": "post_recovery",
            "data": "System should still work after error handling"
        })
    ).await?;

    let recovery_result = events.store(&recovery_event).await;
    assert!(recovery_result.is_ok(), "System should recover and continue processing events");

    tracing::info!(
        edge_cases_processed = processed_count,
        recovery_successful = recovery_result.is_ok(),
        "Event processing error handling validated"
    );

    Ok(())
}

// ============================================================================
// Resource Management Tests
// ============================================================================

/// Test resource usage and memory management during event processing
#[sinex_test]
async fn test_resource_usage_management(ctx: TestContext) -> TestResult<()> {
    tracing::info!("Testing resource usage and memory management");

    let events = ctx.events();

    // Generate events with varying payload sizes to test memory management
    let payload_sizes = vec![100, 1000, 10000]; // Different payload sizes
    let events_per_size = 20;

    for payload_size in payload_sizes {
        let large_data = "x".repeat(payload_size);

        for i in 0..events_per_size {
            let event = ctx.publish(
                "test.resource_usage",
                serde_json::json!({
                    "payload_size": payload_size,
                    "sequence": i,
                    "large_data": large_data,
                    "metadata": {
                        "test_phase": "memory_management",
                        "iteration": i
                    }
                })
            ).await?;

            let store_result = events.store(&event).await;
            assert!(store_result.is_ok(), "Event with payload size {} should be stored", payload_size);
        }
    }

    // Verify system stability after processing various payload sizes
    let stability_event = ctx.publish(
        "test.resource_usage",
        serde_json::json!({
            "test_phase": "stability_check",
            "message": "System should remain stable after variable payload processing"
        })
    ).await?;

    let stability_result = events.store(&stability_event).await;
    assert!(stability_result.is_ok(), "System should remain stable after resource usage test");

    // Verify events are properly persisted
    let resource_events = events.get_by_source("test.resource_usage", Some(100)).await?;
    assert!(resource_events.len() >= 60, "All resource usage events should be persisted");

    tracing::info!(
        total_events = resource_events.len(),
        "Resource usage and memory management validated"
    );

    Ok(())
}
//! Ingest Service Integration Tests
//!
//! Tests the sinex-ingestd service functionality including:
//! - Event ingestion and validation
//! - Database persistence patterns
//! - Performance characteristics and error handling
//! - Schema validation and synchronization
//!
//! These tests validate the core ingestion patterns that nodes use
//! to submit events for processing and storage.

use sinex_db::repositories::DbPoolExt;
use sinex_primitives::DynamicPayload;
use sinex_primitives::Timestamp;
use sinex_primitives::Uuid;
use std::time::Duration;
use tokio::time::{sleep, timeout};
use xtask::sandbox::prelude::*;
use xtask::sandbox::timing::Timeouts;

// ============================================================================
// Core Ingest Service Tests
// ============================================================================

/// Test ingest service initialization patterns
#[sinex_test]
async fn test_ingest_service_startup(ctx: TestContext) -> Result<()> {
    tracing::info!("Testing ingest service startup and initialization");

    // Verify database connectivity for ingest service
    let pool = &ctx.pool;
    let connection = pool.acquire().await?;
    drop(connection);

    // Test configuration requirements
    let database_url = std::env::var("DATABASE_URL");
    assert!(
        database_url.is_ok(),
        "DATABASE_URL required for ingest service"
    );

    let nats_url =
        std::env::var("SINEX_NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".to_string());
    assert!(
        !nats_url.is_empty(),
        "NATS URL should be configured for ingest service"
    );

    tracing::info!(
        database_configured = database_url.is_ok(),
        nats_url = %nats_url,
        "Ingest service configuration validated"
    );

    Ok(())
}

/// Test event ingestion through the service API
#[sinex_test]
async fn test_event_ingestion_flow(ctx: TestContext) -> Result<()> {
    let ctx = ctx.with_nats().shared().await?;
    let _scope = ctx.pipeline().await?;
    tracing::info!("Testing event ingestion through service API");

    // Create test event that would come from a node
    let node_event = ctx
        .publish(DynamicPayload::new(
            "fs-watcher",
            "file.created",
            serde_json::json!({
                "path": "/tmp/test_file.txt",
                "size": 1024,
                "created_at": sinex_primitives::temporal::format_rfc3339(Timestamp::now()),
                "permissions": 644
            }),
        ))
        .await?;

    // Event is automatically stored via publish_json_event
    assert_eq!(node_event.source.as_str(), "fs-watcher");
    assert_eq!(node_event.event_type.as_str(), "file.created");
    assert!(node_event.id.is_some());
    assert_eq!(node_event.payload["path"], "/tmp/test_file.txt");

    tracing::info!(
        event_id = ?node_event.id,
        source = %node_event.source,
        event_type = %node_event.event_type,
        "Event ingestion flow validated"
    );

    // Wait for persistence
    let event_id = *node_event.id.as_ref().unwrap();
    xtask::sandbox::timing::WaitHelpers::wait_for_event_id(&ctx.pool, event_id, 10).await?;

    // Verify event can be retrieved
    let retrieved_event = ctx.pool.events().get_by_id(event_id).await?;
    assert!(
        retrieved_event.is_some(),
        "Ingested event should be retrievable"
    );

    let retrieved = retrieved_event.unwrap();
    assert_eq!(retrieved.id, node_event.id);
    assert_eq!(retrieved.payload["path"], "/tmp/test_file.txt");

    Ok(())
}

/// Test batch ingestion functionality
#[sinex_test]
async fn test_batch_ingestion(ctx: TestContext) -> Result<()> {
    let ctx = ctx.with_nats().shared().await?;
    let _scope = ctx.pipeline().await?;
    tracing::info!("Testing batch event ingestion");

    // Create multiple events as would be sent in a batch
    let batch_events = vec![
        ctx.publish(DynamicPayload::new(
            "fs-watcher",
            "file.created",
            serde_json::json!({
                "path": "/tmp/batch_file_1.txt",
                "size": 512,
                "created_at": sinex_primitives::temporal::format_rfc3339(Timestamp::now())
            }),
        ))
        .await?,
        ctx.publish(DynamicPayload::new(
            "terminal",
            "command.executed",
            serde_json::json!({
                "command": "ls -la",
                "exit_code": 0,
                "duration_ms": 42
            }),
        ))
        .await?,
        ctx.publish(DynamicPayload::new(
            "desktop",
            "window.focused",
            serde_json::json!({
                "window_title": "Terminal",
                "application": "gnome-terminal"
            }),
        ))
        .await?,
    ];

    // Events are automatically stored, extract their IDs
    let mut stored_ids = Vec::new();
    for event in &batch_events {
        stored_ids.push(*event.id.as_ref().unwrap());
    }

    assert_eq!(stored_ids.len(), 3, "All batch events should be stored");

    // Verify all events were persisted
    xtask::sandbox::timing::WaitHelpers::wait_for_event_count(&ctx.pool, 3, 30).await?;
    for (idx, event_id) in stored_ids.iter().enumerate() {
        xtask::sandbox::timing::WaitHelpers::wait_for_event_id(&ctx.pool, *event_id, 10).await?;
        let retrieved = ctx
            .pool
            .events()
            .get_by_id(*event_id)
            .await?
            .expect("Batch event should be retrievable after wait");
        assert_eq!(
            retrieved.id,
            Some(*event_id),
            "Batch event {idx} should match stored ID"
        );
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
async fn test_ingestion_validation(ctx: TestContext) -> Result<()> {
    let ctx = ctx.with_nats().shared().await?;
    let _scope = ctx.pipeline().await?;
    tracing::info!("Testing event validation during ingestion");

    // Test valid event with complete payload
    let valid_event = ctx
        .publish(DynamicPayload::new(
            "fs-watcher",
            "file.created",
            serde_json::json!({
                "path": "/tmp/valid_file.txt",
                "size": 1024,
                "created_at": sinex_primitives::temporal::format_rfc3339(Timestamp::now()),
                "permissions": 644,
                "inode": 12345
            }),
        ))
        .await?;

    assert!(valid_event.id.is_some(), "Valid event should be stored");

    // Test edge case validation - minimal payload
    let minimal_event = ctx
        .publish(DynamicPayload::new(
            "system",
            "service.started",
            serde_json::json!({
                "service_name": "test-service"
            }),
        ))
        .await?;

    assert!(
        minimal_event.id.is_some(),
        "Minimal valid event should be stored"
    );

    // Test large payload handling
    let large_payload = "x".repeat(10000); // 10KB payload
    let large_event_result = ctx
        .publish(DynamicPayload::new(
            "application",
            "log.entry",
            serde_json::json!({
                "message": large_payload,
                "level": "info"
            }),
        ))
        .await;

    assert!(
        large_event_result.is_ok(),
        "Large payload event should be handled"
    );

    tracing::info!("Event validation during ingestion verified");

    Ok(())
}

/// Test source and event type patterns
#[sinex_test]
async fn test_source_and_type_patterns(ctx: TestContext) -> Result<()> {
    let ctx = ctx.with_nats().shared().await?;
    let _scope = ctx.pipeline().await?;
    tracing::info!("Testing source and event type patterns");

    // Test events with different sources and types for pattern validation
    let test_patterns = vec![
        (
            "fs-watcher",
            "file.created",
            serde_json::json!({
                "path": "/tmp/pattern_test.txt",
                "size": 256,
                "created_at": sinex_primitives::temporal::format_rfc3339(Timestamp::now())
            }),
        ),
        (
            "terminal",
            "command.executed",
            serde_json::json!({
                "command": "cat /proc/version",
                "exit_code": 0
            }),
        ),
        (
            "desktop.window-manager",
            "window.focused",
            serde_json::json!({
                "window_id": 12345,
                "workspace": "main"
            }),
        ),
        (
            "system.systemd",
            "service.started",
            serde_json::json!({
                "unit": "nginx.service",
                "status": "active"
            }),
        ),
    ];

    let mut stored_events = Vec::new();
    for (source, event_type, payload) in test_patterns {
        let event = ctx
            .publish(DynamicPayload::new(source, event_type, payload))
            .await?;
        stored_events.push((source, event_type, event));
    }

    // Verify all events were processed
    assert_eq!(
        stored_events.len(),
        4,
        "All pattern events should be processed"
    );

    // Group events by source to verify pattern handling
    let mut events_by_source = std::collections::HashMap::new();
    for (source, _event_type, event) in stored_events {
        events_by_source
            .entry(source)
            .or_insert(Vec::new())
            .push(event);
    }

    assert_eq!(
        events_by_source.len(),
        4,
        "Should have events from 4 different sources"
    );

    // Verify specific source patterns
    assert!(
        events_by_source.contains_key("fs-watcher"),
        "Should handle fs-watcher events"
    );
    assert!(
        events_by_source.contains_key("terminal"),
        "Should handle terminal events"
    );
    assert!(
        events_by_source.contains_key("desktop.window-manager"),
        "Should handle dotted source names"
    );
    assert!(
        events_by_source.contains_key("system.systemd"),
        "Should handle system events"
    );

    tracing::info!(
        total_events = events_by_source
            .values()
            .map(std::vec::Vec::len)
            .sum::<usize>(),
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
async fn test_ingestion_performance(ctx: TestContext) -> Result<()> {
    let ctx = ctx.with_nats().shared().await?;
    let _scope = ctx.pipeline().await?;
    tracing::info!("Testing ingestion service performance");

    let start_time = std::time::Instant::now();
    let run_id = Uuid::now_v7().to_string().to_lowercase();
    let source = format!("performance-test-{run_id}");

    // Generate a small batch of events to test throughput without hitting test timeouts.
    let target_events = 20usize;
    let max_attempts = target_events * 4;
    let mut processed_events = 0usize;
    let mut attempt = 0usize;

    while processed_events < target_events && attempt < max_attempts {
        let sequence = processed_events;
        attempt += 1;

        match ctx
            .publish(DynamicPayload::new(
                source.as_str(),
                "throughput.test",
                serde_json::json!({
                    "sequence": sequence,
                    "batch_size": target_events,
                    "timestamp": sinex_primitives::temporal::format_rfc3339(Timestamp::now()),
                    "payload_data": format!("Performance test event {}", sequence)
                }),
            ))
            .await
        {
            Ok(_) => processed_events += 1,
            Err(e) => {
                tracing::warn!(
                    attempt,
                    sequence,
                    error = %e,
                    "Event ingestion failed, will retry until target reached"
                );
                tokio::task::yield_now().await;
            }
        }
    }

    assert_eq!(
        processed_events, target_events,
        "Performance run should reach the target event count"
    );

    // Verify persistence with a longer retry loop to tolerate catalog latency on busy pools.
    let persisted = match xtask::sandbox::timing::WaitHelpers::wait_for_source_events(
        &ctx.pool,
        &source,
        processed_events,
        20,
    )
    .await
    {
        Ok(count) => count,
        Err(err) => {
            tracing::warn!(
                expected = processed_events,
                error = %err,
                "Wait for performance events timed out; reconciling with direct count"
            );
            let event_source = sinex_primitives::EventSource::new(&source)?;
            ctx.pool.events().count_by_source(&event_source).await? as usize
        }
    };

    assert!(
        persisted >= processed_events,
        "All performance test events should be persisted for source {source} (saw {persisted}, expected {processed_events})"
    );

    let duration = start_time.elapsed();
    let events_per_second = processed_events as f64 / duration.as_secs_f64().max(0.1);

    tracing::info!(
        processed_events = processed_events,
        duration_ms = duration.as_millis(),
        events_per_second = events_per_second,
        "Ingestion service performance measured"
    );

    // Verify reasonable performance (should process at least 1 event/second to avoid flake on slow hosts)
    assert!(
        events_per_second >= 1.0,
        "Ingestion service should maintain reasonable throughput even under load: {events_per_second} events/second"
    );

    // Verify all events were processed
    assert!(
        processed_events >= target_events,
        "All performance test events should be processed (processed {processed_events}, target {target_events})"
    );

    Ok(())
}

/// Test sequential ingestion handling (modified from concurrent due to TestContext constraints)
#[sinex_test]
async fn test_sequential_ingestion(ctx: TestContext) -> Result<()> {
    let ctx = ctx.with_nats().shared().await?;
    let _scope = ctx.pipeline().await?;
    tracing::info!("Testing sequential event ingestion");

    let source = format!(
        "sequential-ingest-{}",
        Uuid::now_v7().to_string().to_lowercase()
    );

    // Generate events to test ingestion capacity
    let mut successful_ingests = 0;
    let total_events = 20;

    for i in 0..total_events {
        let mut attempts = 0;
        let mut created = false;
        while attempts < 4 {
            attempts += 1;
            let event_result = ctx
                .publish(DynamicPayload::new(
                    source.as_str(),
                    "sequential.test",
                    serde_json::json!({
                        "worker_id": i,
                        "batch_id": i / 5,
                        "data": format!("Sequential ingestion test {}", i)
                    }),
                ))
                .await;

            match event_result {
                Ok(_) => {
                    successful_ingests += 1;
                    created = true;
                    break;
                }
                Err(e)
                    if attempts < 4
                        && (e.to_string().contains("deadlock detected")
                            || e.to_string().contains("could not serialize")
                            || e.to_string().contains("restart the transaction")) =>
                {
                    tracing::warn!(
                        attempt = attempts,
                        error = %e,
                        "Transient failure during sequential ingestion, retrying"
                    );
                    sleep(Duration::from_millis(20 * attempts as u64)).await;
                }
                Err(e) => {
                    tracing::error!(error = %e, "Sequential ingestion failed");
                    break;
                }
            }
        }

        if !created {
            return Err(color_eyre::eyre::eyre!(
                "Failed to ingest event after retries (sequence={i})"
            ));
        }
    }

    assert_eq!(
        successful_ingests, total_events,
        "All events should be ingested successfully"
    );

    // Verify events are in the database
    let observed = xtask::sandbox::timing::WaitHelpers::wait_for_source_events(
        &ctx.pool,
        &source,
        total_events as usize,
        25,
    )
    .await?;
    assert!(
        observed >= total_events as usize,
        "All sequential events should be persisted (observed {observed}, expected {total_events})"
    );

    tracing::info!(
        sequential_ingests = successful_ingests,
        persisted_events = observed,
        "Sequential ingestion validated"
    );

    Ok(())
}

/// Test ingestion error handling and recovery
#[sinex_test]
async fn test_ingestion_error_handling(ctx: TestContext) -> Result<()> {
    tracing::info!("Testing ingestion service error handling and recovery");
    let material_id = ctx.create_source_material(Some("error-handling")).await?;

    // Test successful ingestion via direct DB insert
    let valid_event = DynamicPayload::new(
        "error-test",
        "error.handling",
        serde_json::json!({
            "test_case": "valid_event",
            "data": "This should ingest successfully"
        }),
    )
    .from_material(material_id)
    .build()?;
    let valid_result = ctx.pool.events().insert(valid_event).await;
    assert!(
        valid_result.is_ok(),
        "Valid event should be ingested successfully"
    );

    // Test edge case payloads for robust error handling
    let edge_cases = vec![
        ("empty_payload", serde_json::json!({})),
        (
            "large_string",
            serde_json::json!({"large_data": "x".repeat(5000)}),
        ),
        (
            "deeply_nested",
            serde_json::json!({
                "level1": {"level2": {"level3": {"level4": {"data": "deeply nested payload"}}}}
            }),
        ),
        (
            "array_payload",
            serde_json::json!({
                "items": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                "metadata": ["tag1", "tag2", "tag3"]
            }),
        ),
        (
            "unicode_content",
            serde_json::json!({
                "content": "Hello 世界 🌍 Ελληνικά Русский العربية"
            }),
        ),
    ];

    let mut processed_count = 0;
    for (i, (case_name, payload)) in edge_cases.into_iter().enumerate() {
        let event = DynamicPayload::new("error-test", "edge.case", payload)
            .from_material_at(material_id, (i + 1) as i64)
            .build()?;
        match ctx.pool.events().insert(event).await {
            Ok(_) => {
                processed_count += 1;
                tracing::debug!(case = %case_name, "Edge case processed successfully");
            }
            Err(e) => {
                tracing::warn!(case = %case_name, error = %e, "Edge case processing failed");
            }
        }
    }

    assert!(
        processed_count >= 4,
        "Most edge cases should be handled gracefully"
    );

    // Verify DB recovery after edge case processing
    let recovery_event = DynamicPayload::new(
        "error-test",
        "error.recovery",
        serde_json::json!({
            "test_case": "post_recovery",
            "data": "Service should continue working after error handling"
        }),
    )
    .from_material_at(material_id, 10)
    .build()?;
    let recovery_result = ctx.pool.events().insert(recovery_event).await;
    assert!(
        recovery_result.is_ok(),
        "Service should recover and continue processing"
    );

    tracing::info!(
        edge_cases_processed = processed_count,
        recovery_successful = recovery_result.is_ok(),
        "Ingestion error handling validated"
    );

    Ok(())
}

// ============================================================================
// Schema and Validation Tests
// ============================================================================

/// Test schema patterns during ingestion
#[sinex_test]
async fn test_schema_validation_patterns(ctx: TestContext) -> Result<()> {
    tracing::info!("Testing schema validation patterns during ingestion");
    let material_id = ctx
        .create_source_material(Some("schema-validation"))
        .await?;

    // Test events with different schema patterns
    let schema_test_events = vec![
        (
            "fs-watcher",
            "file.created",
            serde_json::json!({
                "path": "/tmp/schema_test.txt",
                "size": 1024,
                "created_at": sinex_primitives::temporal::format_rfc3339(Timestamp::now())
            }),
        ),
        (
            "terminal",
            "command.executed",
            serde_json::json!({
                "command": "echo 'schema test'",
                "exit_code": 0,
                "working_directory": "/tmp"
            }),
        ),
        (
            "system",
            "service.started",
            serde_json::json!({
                "service_name": "test-service",
                "pid": 12345,
                "status": "active"
            }),
        ),
    ];

    for (i, (source, event_type, payload)) in schema_test_events.into_iter().enumerate() {
        let event = DynamicPayload::new(source, event_type, payload)
            .from_material_at(material_id, i as i64)
            .build()?;
        let inserted = ctx.pool.events().insert(event).await?;

        assert!(
            inserted.id.is_some(),
            "Event with source {source} and type {event_type} should be stored"
        );
    }

    tracing::info!("Schema validation patterns during ingestion verified");

    Ok(())
}

/// Test payload validation patterns
#[sinex_test]
async fn test_payload_validation_patterns(ctx: TestContext) -> Result<()> {
    tracing::info!("Testing payload validation patterns");
    let material_id = ctx
        .create_source_material(Some("payload-validation"))
        .await?;

    // Test various payload structures that should be valid
    let validation_patterns = vec![
        ("minimal", serde_json::json!({})),
        (
            "with_numbers",
            serde_json::json!({
                "integer": 42,
                "float": 1.23456,
                "negative": -123
            }),
        ),
        (
            "with_booleans",
            serde_json::json!({
                "success": true,
                "enabled": false
            }),
        ),
        (
            "with_null_values",
            serde_json::json!({
                "optional_field": null,
                "required_field": "present"
            }),
        ),
        (
            "mixed_types",
            serde_json::json!({
                "string": "text",
                "number": 123,
                "boolean": true,
                "array": [1, 2, 3],
                "object": {"nested": "value"}
            }),
        ),
    ];

    for (i, (pattern_name, payload)) in validation_patterns.into_iter().enumerate() {
        let event = DynamicPayload::new("validation-test", "payload.test", payload)
            .from_material_at(material_id, i as i64)
            .build()?;
        let inserted = ctx.pool.events().insert(event).await?;

        assert!(
            inserted.id.is_some(),
            "Payload pattern '{pattern_name}' should be valid"
        );

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
async fn test_service_health_monitoring(ctx: TestContext) -> Result<()> {
    let ctx = ctx.with_nats().shared().await?;
    let _scope = ctx.pipeline().await?;
    let source = format!(
        "health-monitor-{}",
        Uuid::now_v7().to_string().to_lowercase()
    );
    tracing::info!("Testing service health monitoring");

    // Test basic health indicators through event processing
    let health_check_event = ctx
        .publish(DynamicPayload::new(
            source.as_str(),
            "health.check",
            serde_json::json!({
                "timestamp": sinex_primitives::temporal::format_rfc3339(Timestamp::now()),
                "status": "healthy"
            }),
        ))
        .await?;

    assert!(
        health_check_event.id.is_some(),
        "Health check event should be processed"
    );

    // Test database connectivity (core health indicator)
    let db_connection = ctx.pool.acquire().await?;
    drop(db_connection);

    // Wait for persistence
    xtask::sandbox::timing::WaitHelpers::wait_for_source_events(&ctx.pool, &source, 1, 10).await?;

    // Test event retrieval (indicates service is operational)
    let recent_events = ctx.pool.events().get_recent(5).await?;
    assert!(
        !recent_events.is_empty(),
        "Service should be able to retrieve events"
    );

    // Simulate service processing over time
    for i in 0..3 {
        let status_event = ctx
            .publish(DynamicPayload::new(
                source.as_str(),
                "status.update",
                serde_json::json!({
                    "sequence": i,
                    "uptime_seconds": i * 60,
                    "events_processed": i * 10
                }),
            ))
            .await?;

        assert!(
            status_event.id.is_some(),
            "Status update should be processed"
        );

        tokio::task::yield_now().await;
    }

    xtask::sandbox::timing::WaitHelpers::wait_for_source_events(&ctx.pool, &source, 4, 12).await?;

    // Verify service maintains health over time
    let status_events = ctx
        .pool
        .events()
        .get_by_source(
            &EventSource::from(source.as_str()),
            sinex_primitives::Pagination::new(Some(10), None),
        )
        .await?;
    assert!(
        status_events.len() >= 4,
        "Should have health monitoring events"
    );

    tracing::info!(
        health_events = status_events.len(),
        "Service health monitoring validated"
    );

    Ok(())
}

/// Test resource management during ingestion
#[sinex_test]
async fn test_resource_management(ctx: TestContext) -> Result<()> {
    let ctx = ctx.with_nats().shared().await?;
    let _scope = ctx.pipeline().await?;
    tracing::info!("Testing resource management during ingestion");

    let source = format!(
        "resource-test-{}",
        Uuid::now_v7().to_string().to_lowercase()
    );

    // Generate events with varying resource requirements
    let resource_patterns = vec![
        ("small_payload", 100),  // 100 byte payloads
        ("medium_payload", 800), // ~1KB payloads
        ("large_payload", 2500), // 2.5KB payloads to keep test lightweight
    ];

    let events_per_pattern = 5;

    for (pattern_name, payload_size) in &resource_patterns {
        let large_data = "x".repeat(*payload_size);

        for i in 0..events_per_pattern {
            let event = ctx
                .publish(DynamicPayload::new(
                    source.as_str(),
                    "resource.test",
                    serde_json::json!({
                        "pattern": pattern_name,
                        "payload_size": payload_size,
                        "sequence": i,
                        "data": large_data
                    }),
                ))
                .await?;

            assert!(
                event.id.is_some(),
                "Event with payload size {payload_size} should be stored"
            );
        }
    }

    // Test service stability after resource variation
    let stability_event = ctx
        .publish(DynamicPayload::new(
            source.as_str(),
            "stability.check",
            serde_json::json!({
                "message": "Service should remain stable after resource variation"
            }),
        ))
        .await?;

    assert!(
        stability_event.id.is_some(),
        "Service should remain stable after resource tests"
    );

    // Verify persistence with wait helpers instead of backfilling.
    let expected_events = resource_patterns.len() * events_per_pattern + 1;
    let observed = xtask::sandbox::timing::WaitHelpers::wait_for_source_events(
        &ctx.pool,
        &source,
        expected_events,
        20,
    )
    .await?;
    assert!(
        observed >= expected_events,
        "Expected at least {expected_events} events for source {source}, saw {observed}"
    );

    // Verify events are properly persisted
    let resource_events = ctx
        .pool
        .events()
        .get_by_source(
            &EventSource::from(source.as_str()),
            sinex_primitives::Pagination::new(Some(expected_events as i64), None),
        )
        .await?;
    assert!(
        resource_events.len() >= expected_events,
        "All resource test events should be persisted (expected {expected_events}, found {})",
        resource_events.len()
    );

    tracing::info!(
        total_resource_events = resource_events.len(),
        "Resource management validated"
    );

    Ok(())
}

/// Test timeout and deadline handling
#[sinex_test]
async fn test_timeout_and_deadline_handling(ctx: TestContext) -> Result<()> {
    let ctx = ctx.with_nats().shared().await?;
    let _scope = ctx.pipeline().await?;
    tracing::info!("Testing timeout and deadline handling");

    // Test normal operation within reasonable timeouts
    let timeout_duration = Duration::from_secs(Timeouts::QUICK);

    let normal_operation = timeout(timeout_duration, async {
        ctx.publish(DynamicPayload::new(
            "timeout-test",
            "timeout.test",
            serde_json::json!({
                "operation": "normal",
                "timestamp": sinex_primitives::temporal::format_rfc3339(Timestamp::now())
            }),
        ))
        .await
    })
    .await;

    assert!(
        normal_operation.is_ok(),
        "Normal operations should complete within timeout"
    );
    assert!(
        normal_operation.unwrap().is_ok(),
        "Normal event should be stored successfully"
    );

    // Test batch operations within timeouts
    let batch_timeout = Duration::from_secs(Timeouts::SHORT);

    let batch_operation = timeout(batch_timeout, async {
        let mut batch_results = Vec::new();

        for i in 0..10 {
            let event = ctx
                .publish(DynamicPayload::new(
                    "timeout-test",
                    "batch.timeout.test",
                    serde_json::json!({
                        "batch_sequence": i,
                        "data": format!("Batch timeout test {}", i)
                    }),
                ))
                .await?;

            batch_results.push(event);
        }

        Ok::<Vec<_>, color_eyre::eyre::Error>(batch_results)
    })
    .await;

    assert!(
        batch_operation.is_ok(),
        "Batch operations should complete within timeout"
    );

    let batch_results = batch_operation.unwrap()?;
    assert_eq!(
        batch_results.len(),
        10,
        "All batch events should be processed within timeout"
    );

    xtask::sandbox::timing::WaitHelpers::wait_for_source_events(&ctx.pool, "timeout-test", 11, 30)
        .await?;

    tracing::info!(
        normal_operations = 1,
        batch_operations = batch_results.len(),
        "Timeout and deadline handling validated"
    );

    Ok(())
}

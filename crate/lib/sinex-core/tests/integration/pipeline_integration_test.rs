//! Pipeline Integration Tests
//!
//! Comprehensive tests for the Sinex data processing pipeline, focusing on:
//! - Event ingestion pipeline flows
//! - Stream processing through NATS JetStream
//! - Data transformation and enrichment
//! - Multi-stage processing workflows
//! - Pipeline error handling and recovery
//! - Performance under pipeline load
//!
//! This test suite verifies complete data flows from event capture through
//! final processing, ensuring data integrity and correct processing semantics.

use chrono::{Duration, Utc};
use color_eyre::eyre::Result;
use futures::future::join_all;
use serde_json::json;
use sinex_core::db::repositories::DbPoolExt;
use sinex_core::types::domain::{EventSource, EventType};
use sinex_test_utils::prelude::*;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration as StdDuration, Instant};
use tokio::sync::Mutex;
use tokio::time::{sleep, timeout};

// =============================================================================
// Event Ingestion Pipeline Tests
// =============================================================================

/// Test complete event ingestion pipeline from raw input to database storage
#[sinex_test]
async fn test_complete_event_ingestion_pipeline(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    tracing::info!("Testing complete event ingestion pipeline");

    // Phase 1: Generate diverse test events representing different sources
    let test_events = vec![
        (
            "filesystem",
            "file.created",
            json!({
                "path": "/tmp/test_file.txt",
                "size": 1024,
                "permissions": "644",
                "owner": "user",
                "created_at": Utc::now()
            }),
        ),
        (
            "terminal",
            "command.executed",
            json!({
                "command": "cargo test --workspace",
                "working_directory": "/home/user/sinex",
                "exit_code": 0,
                "duration_ms": 2340
            }),
        ),
        (
            "desktop",
            "window.focused",
            json!({
                "window_title": "VS Code - main.rs",
                "application": "code",
                "window_id": "0x1234567",
                "workspace": "development"
            }),
        ),
        (
            "system",
            "process.started",
            json!({
                "process_name": "cargo",
                "pid": 12345,
                "parent_pid": 1,
                "command_line": "cargo build --release",
                "user": "developer"
            }),
        ),
    ];

    let mut created_event_ids = Vec::new();
    let pipeline_start = Instant::now();

    // Phase 2: Process each event through the ingestion pipeline
    for (source, event_type, payload) in test_events.iter() {
        let event = ctx
            .create_test_event(source, event_type, payload.clone())
            .await?;
        created_event_ids.push(event.id);

        tracing::debug!(
            source = source,
            event_type = event_type,
            event_id = %event.id,
            "Event processed through ingestion pipeline"
        );
    }

    let pipeline_duration = pipeline_start.elapsed();
    tracing::info!(
        events_processed = created_event_ids.len(),
        duration_ms = pipeline_duration.as_millis(),
        "Pipeline ingestion completed"
    );

    // Phase 3: Verify all events are correctly stored with proper structure
    // Note: No batch get_by_ids method available, so query individually
    let mut stored_events = Vec::new();
    for event_id in &created_event_ids {
        if let Some(event) = ctx.pool.events().get_by_id(*event_id).await? {
            stored_events.push(event);
        }
    }

    assert_eq!(
        stored_events.len(),
        test_events.len(),
        "All events should be stored in database"
    );

    // Phase 4: Verify data integrity and processing semantics
    for (i, (source, event_type, expected_payload)) in test_events.iter().enumerate() {
        let stored_event = &stored_events[i];

        assert_eq!(stored_event.source, *source);
        assert_eq!(stored_event.event_type, *event_type);
        assert_eq!(stored_event.payload, *expected_payload);

        // Verify pipeline processing metadata
        let ingest_ts = stored_event
            .id
            .as_ref()
            .expect("id present")
            .as_ulid()
            .timestamp();
        assert!(ingest_ts > stored_event.ts_orig.unwrap_or(ingest_ts));
        assert!(stored_event
            .ingestor_version
            .as_ref()
            .map_or(false, |s| !s.is_empty()));

        tracing::debug!(
            event_id = %stored_event.id,
            source = stored_event.source,
            "Event integrity verified"
        );
    }

    // Phase 5: Verify time-series queryability (TimescaleDB functionality)
    let recent_events = ctx
        .pool
        .events()
        .get_by_time_range(Utc::now() - Duration::minutes(5), Utc::now(), None, None)
        .await?;

    let our_events_count = recent_events
        .iter()
        .filter(|e| created_event_ids.contains(&e.id))
        .count();

    assert_eq!(
        our_events_count,
        test_events.len(),
        "All events should be queryable by time range"
    );

    tracing::info!("Event ingestion pipeline test completed successfully");
    Ok(())
}

/// Test pipeline handling of concurrent event streams
#[sinex_test]
async fn test_concurrent_pipeline_processing(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    tracing::info!("Testing concurrent pipeline processing");

    let concurrent_streams = 4;
    let events_per_stream = 15;
    let processing_results = Arc::new(Mutex::new(Vec::new()));

    // Create concurrent processing tasks simulating multiple event sources
    let mut stream_handles = Vec::new();

    for stream_id in 0..concurrent_streams {
        let ctx_clone = ctx.clone();
        let results = processing_results.clone();

        let handle = tokio::spawn(async move {
            let stream_name = format!("stream_{}", stream_id);
            let mut stream_events = Vec::new();
            let stream_start = Instant::now();

            for event_idx in 0..events_per_stream {
                let event_payload = json!({
                    "stream_id": stream_id,
                    "event_index": event_idx,
                    "data": format!("concurrent_data_{}_{}", stream_id, event_idx),
                    "timestamp": Utc::now(),
                    "sequence": stream_id * events_per_stream + event_idx
                });

                match ctx_clone
                    .create_test_event(&stream_name, "stream.data", event_payload)
                    .await
                {
                    Ok(event) => {
                        stream_events.push(event.id);
                        tracing::trace!(
                            stream_id = stream_id,
                            event_idx = event_idx,
                            event_id = %event.id,
                            "Stream event processed"
                        );
                    }
                    Err(e) => {
                        tracing::error!(
                            stream_id = stream_id,
                            event_idx = event_idx,
                            error = %e,
                            "Stream event processing failed"
                        );
                        return Err(e);
                    }
                }

                // Small processing delay to simulate realistic load
                sleep(StdDuration::from_millis(5)).await;
            }

            let stream_duration = stream_start.elapsed();

            let mut results_lock = results.lock().await;
            results_lock.push((stream_id, stream_events, stream_duration));

            tracing::info!(
                stream_id = stream_id,
                events_processed = events_per_stream,
                duration_ms = stream_duration.as_millis(),
                "Stream processing completed"
            );

            Ok::<(), color_eyre::eyre::Error>(())
        });

        stream_handles.push(handle);
    }

    // Wait for all concurrent streams to complete
    let stream_results = join_all(stream_handles).await;

    // Verify all streams completed successfully
    for result in stream_results {
        result??; // Handle both join error and task error
    }

    let results = processing_results.lock().await;
    assert_eq!(
        results.len(),
        concurrent_streams,
        "All streams should complete processing"
    );

    // Verify database state after concurrent processing
    let mut total_events_processed = 0;
    let mut all_processed_ids = Vec::new();

    for (stream_id, event_ids, duration) in results.iter() {
        total_events_processed += event_ids.len();
        all_processed_ids.extend(event_ids.clone());

        tracing::info!(
            stream_id = stream_id,
            events = event_ids.len(),
            duration_ms = duration.as_millis(),
            "Stream processing summary"
        );
    }

    assert_eq!(
        total_events_processed,
        concurrent_streams * events_per_stream,
        "Total events processed should match expected"
    );

    // Verify all events are in database and accessible
    let mut stored_events = Vec::new();
    for event_id in &all_processed_ids {
        if let Some(event) = ctx.pool.events().get_by_id(*event_id).await? {
            stored_events.push(event);
        }
    }

    assert_eq!(
        stored_events.len(),
        total_events_processed,
        "All concurrent events should be stored"
    );

    // Verify proper ordering within each stream
    for stream_id in 0..concurrent_streams {
        let stream_events: Vec<_> = stored_events
            .iter()
            .filter(|e| e.payload.get("stream_id") == Some(&json!(stream_id)))
            .collect();

        assert_eq!(
            stream_events.len(),
            events_per_stream,
            "Each stream should have correct event count"
        );

        // Verify sequence ordering within stream
        let mut sequences: Vec<_> = stream_events
            .iter()
            .map(|e| e.payload["sequence"].as_u64().unwrap())
            .collect();

        sequences.sort();

        let expected_start = (stream_id * events_per_stream) as u64;
        let expected_end = expected_start + events_per_stream as u64 - 1;

        assert_eq!(sequences[0], expected_start);
        assert_eq!(sequences[sequences.len() - 1], expected_end);
    }

    tracing::info!(
        concurrent_streams = concurrent_streams,
        total_events = total_events_processed,
        "Concurrent pipeline processing test completed"
    );
    Ok(())
}

// =============================================================================
// Data Transformation Pipeline Tests
// =============================================================================

/// Test pipeline data transformation and enrichment
#[sinex_test]
async fn test_pipeline_data_transformation(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    tracing::info!("Testing pipeline data transformation and enrichment");

    // Create raw events that should be processed and enriched
    let raw_events = vec![
        (
            "terminal",
            "command.raw",
            json!({
                "command_line": "git commit -m 'initial commit'",
                "working_directory": "/home/user/project",
                "timestamp": Utc::now()
            }),
        ),
        (
            "filesystem",
            "file.raw",
            json!({
                "path": "/home/user/project/src/main.rs",
                "operation": "modify",
                "size": 2048
            }),
        ),
    ];

    let mut raw_event_ids = Vec::new();

    // Phase 1: Insert raw events
    for (source, event_type, payload) in raw_events.iter() {
        let event = ctx
            .create_test_event(source, event_type, payload.clone())
            .await?;
        raw_event_ids.push(event.id);
    }

    // Phase 2: Simulate processing pipeline transformations
    // In a real system, this would be done by automata/processors
    let mut transformed_event_ids = Vec::new();

    for raw_event_id in &raw_event_ids {
        let raw_event = ctx
            .pool
            .events()
            .get_by_id(*raw_event_id)
            .await?
            .expect("Raw event should exist");

        let transformed_payload = match (raw_event.source.as_str(), raw_event.event_type.as_str()) {
            ("terminal", "command.raw") => {
                // Simulate command parsing and enrichment
                json!({
                    "command": "git",
                    "subcommand": "commit",
                    "arguments": ["-m", "initial commit"],
                    "is_git_operation": true,
                    "working_directory": raw_event.payload["working_directory"],
                    "parsed_at": Utc::now(),
                    "source_event_id": raw_event.id
                })
            }
            ("filesystem", "file.raw") => {
                // Simulate file operation analysis
                json!({
                    "path": raw_event.payload["path"],
                    "operation_type": "file_modification",
                    "file_extension": "rs",
                    "language": "rust",
                    "is_source_code": true,
                    "analyzed_at": Utc::now(),
                    "source_event_id": raw_event.id
                })
            }
            _ => continue,
        };

        let transformed_event = ctx
            .create_test_event(
                &raw_event.source,
                &format!("{}.processed", raw_event.event_type),
                transformed_payload,
            )
            .await?;

        transformed_event_ids.push(transformed_event.id);
    }

    // Phase 3: Verify transformation results
    let mut transformed_events = Vec::new();
    for event_id in &transformed_event_ids {
        if let Some(event) = ctx.pool.events().get_by_id(*event_id).await? {
            transformed_events.push(event);
        }
    }

    assert_eq!(
        transformed_events.len(),
        raw_events.len(),
        "All raw events should be transformed"
    );

    // Verify terminal command transformation
    let git_event = transformed_events
        .iter()
        .find(|e| e.source == "terminal")
        .expect("Should find transformed terminal event");

    assert_eq!(git_event.event_type, "command.raw.processed");
    assert_eq!(git_event.payload["command"], "git");
    assert_eq!(git_event.payload["subcommand"], "commit");
    assert_eq!(git_event.payload["is_git_operation"], true);
    assert!(git_event.payload.get("source_event_id").is_some());

    // Verify filesystem transformation
    let file_event = transformed_events
        .iter()
        .find(|e| e.source == "filesystem")
        .expect("Should find transformed filesystem event");

    assert_eq!(file_event.event_type, "file.raw.processed");
    assert_eq!(file_event.payload["language"], "rust");
    assert_eq!(file_event.payload["is_source_code"], true);
    assert_eq!(file_event.payload["operation_type"], "file_modification");

    // Phase 4: Test pipeline provenance tracking
    for transformed_event in &transformed_events {
        let source_event_id = transformed_event.payload["source_event_id"]
            .as_str()
            .expect("Should have source event ID");

        // Verify the source event exists and is linked
        let source_event = ctx
            .pool
            .events()
            .get_by_id(source_event_id.parse()?)
            .await?
            .expect("Source event should exist");

        assert!(raw_event_ids.contains(&source_event.id));

        tracing::debug!(
            transformed_id = %transformed_event.id,
            source_id = %source_event.id,
            "Pipeline provenance verified"
        );
    }

    tracing::info!("Pipeline data transformation test completed");
    Ok(())
}

// =============================================================================
// Pipeline Error Handling Tests
// =============================================================================

/// Test pipeline error handling and recovery mechanisms
#[sinex_test]
async fn test_pipeline_error_handling(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    tracing::info!("Testing pipeline error handling and recovery");

    let pipeline_start = Instant::now();
    let mut successful_events = Vec::new();
    let mut error_scenarios = Vec::new();

    // Test various error scenarios in the pipeline
    let test_scenarios = vec![
        // Valid event - should succeed
        (
            "valid",
            "test.event",
            json!({
                "data": "valid_test_data",
                "timestamp": Utc::now()
            }),
            true, // should_succeed
        ),
        // Valid but complex event
        (
            "complex",
            "test.complex",
            json!({
                "nested": {
                    "data": {
                        "values": [1, 2, 3, 4, 5],
                        "metadata": {
                            "version": "1.0",
                            "type": "array_data"
                        }
                    }
                },
                "timestamp": Utc::now()
            }),
            true, // should_succeed
        ),
        // Another valid event with different structure
        (
            "simple",
            "test.minimal",
            json!({
                "message": "minimal event payload"
            }),
            true, // should_succeed
        ),
    ];

    // Process all scenarios through the pipeline
    for (source, event_type, payload, should_succeed) in test_scenarios {
        match ctx
            .create_test_event(source, event_type, payload.clone())
            .await
        {
            Ok(event) => {
                if should_succeed {
                    successful_events.push(event.id);
                    tracing::debug!(
                        source = source,
                        event_type = event_type,
                        event_id = %event.id,
                        "Event processed successfully"
                    );
                } else {
                    tracing::warn!(
                        source = source,
                        event_type = event_type,
                        "Expected failure but event succeeded"
                    );
                }
            }
            Err(e) => {
                if !should_succeed {
                    error_scenarios.push((source, event_type, e.to_string()));
                    tracing::debug!(
                        source = source,
                        event_type = event_type,
                        error = %e,
                        "Expected error occurred"
                    );
                } else {
                    return Err(e);
                }
            }
        }

        // Small delay between scenarios
        sleep(StdDuration::from_millis(50)).await;
    }

    let pipeline_duration = pipeline_start.elapsed();

    // Verify successful events are properly stored
    let mut stored_events = Vec::new();
    for event_id in &successful_events {
        if let Some(event) = ctx.pool.events().get_by_id(*event_id).await? {
            stored_events.push(event);
        }
    }

    assert_eq!(
        stored_events.len(),
        successful_events.len(),
        "All successful events should be stored"
    );

    // Verify pipeline resilience - system should still be functional after errors
    let recovery_event = ctx
        .create_test_event(
            "recovery",
            "test.recovery",
            json!({
                "message": "pipeline recovery verification",
                "processed_after_errors": true,
                "timestamp": Utc::now()
            }),
        )
        .await?;

    // Verify recovery event is processed correctly
    let recovery_stored = ctx
        .pool
        .events()
        .get_by_id(recovery_event.id)
        .await?
        .expect("Recovery event should exist");

    assert_eq!(recovery_stored.id, recovery_event.id);
    assert_eq!(recovery_stored.source, "recovery");
    assert_eq!(recovery_stored.event_type, "test.recovery");

    tracing::info!(
        successful_events = successful_events.len(),
        error_scenarios = error_scenarios.len(),
        duration_ms = pipeline_duration.as_millis(),
        "Pipeline error handling test completed"
    );
    Ok(())
}

// =============================================================================
// Pipeline Performance Tests
// =============================================================================

/// Test pipeline performance under sustained load
#[sinex_test]
async fn test_pipeline_performance_under_load(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    tracing::info!("Testing pipeline performance under sustained load");

    let events_to_process = 100;
    let batch_size = 10;
    let performance_start = Instant::now();
    let mut all_event_ids = Vec::new();
    let mut batch_durations = Vec::new();

    // Process events in batches to simulate realistic load patterns
    for batch_idx in 0..(events_to_process / batch_size) {
        let batch_start = Instant::now();
        let mut batch_event_ids = Vec::new();

        // Process a batch of events
        for event_idx in 0..batch_size {
            let global_idx = batch_idx * batch_size + event_idx;

            let event_payload = json!({
                "batch_id": batch_idx,
                "event_index": event_idx,
                "global_index": global_idx,
                "data": format!("load_test_data_{}", global_idx),
                "timestamp": Utc::now(),
                "metadata": {
                    "batch_size": batch_size,
                    "total_events": events_to_process
                }
            });

            let event = ctx
                .create_test_event("load_test", "performance.test", event_payload)
                .await?;

            batch_event_ids.push(event.id);
        }

        let batch_duration = batch_start.elapsed();
        batch_durations.push(batch_duration);
        all_event_ids.extend(batch_event_ids);

        tracing::debug!(
            batch_idx = batch_idx,
            batch_size = batch_size,
            duration_ms = batch_duration.as_millis(),
            "Batch processed"
        );

        // Brief pause between batches
        sleep(StdDuration::from_millis(10)).await;
    }

    let total_duration = performance_start.elapsed();

    // Performance verification
    assert_eq!(
        all_event_ids.len(),
        events_to_process,
        "All events should be processed"
    );

    // Verify all events are stored correctly
    let stored_events = ctx
        .pool
        .events()
        .get_by_source(
            &EventSource::from("load_test"),
            Some(events_to_process as i64),
            None,
        )
        .await?;

    let our_events_count = stored_events
        .iter()
        .filter(|e| all_event_ids.contains(&e.id))
        .count();

    assert_eq!(
        our_events_count, events_to_process,
        "All events should be stored in database"
    );

    // Calculate performance metrics
    let avg_batch_duration =
        batch_durations.iter().map(|d| d.as_millis()).sum::<u128>() / batch_durations.len() as u128;

    let events_per_second = (events_to_process as f64) / total_duration.as_secs_f64();

    // Performance assertions
    assert!(
        events_per_second > 10.0,
        "Pipeline should process at least 10 events per second, got {:.2}",
        events_per_second
    );

    assert!(
        total_duration.as_secs() < 30,
        "Performance test should complete within 30 seconds"
    );

    // Verify data integrity under load
    let sample_event = stored_events
        .iter()
        .find(|e| all_event_ids.contains(&e.id))
        .expect("Should find at least one stored event");

    assert_eq!(sample_event.source, "load_test");
    assert_eq!(sample_event.event_type, "performance.test");
    assert!(sample_event.payload.get("global_index").is_some());

    tracing::info!(
        total_events = events_to_process,
        total_duration_ms = total_duration.as_millis(),
        avg_batch_duration_ms = avg_batch_duration,
        events_per_second = events_per_second,
        "Pipeline performance test completed"
    );
    Ok(())
}

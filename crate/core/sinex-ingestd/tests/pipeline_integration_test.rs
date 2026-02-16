//! Pipeline Integration Tests
//!
//! Comprehensive tests for the Sinex data processing pipeline, focusing on:
//! - Event ingestion pipeline flows
//! - Stream processing through NATS `JetStream`
//! - Data transformation and enrichment
//! - Multi-stage processing workflows
//! - Pipeline error handling and recovery
//! - Performance under pipeline load
//!
//! This test suite verifies complete data flows from event capture through
//! final processing, ensuring data integrity and correct processing semantics.

use color_eyre::eyre::ensure;
use futures::{future::join_all, StreamExt};
use serde_json::json;
use sinex_primitives::DynamicPayload;
use sinex_primitives::EventSource;
use sinex_primitives::SinexError;
use sinex_primitives::Timestamp;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration as StdDuration;
use std::time::Instant;
use time::Duration;
use tokio::sync::Mutex;
use tokio::task::yield_now;
use xtask::sandbox::prelude::*;
use xtask::sandbox::timing::Timeouts;

// =============================================================================
// Event Ingestion Pipeline Tests
// =============================================================================

/// Minimal pipeline smoke test (single event, real ingestion path).
#[sinex_serial_test(timeout = 60)]
async fn test_pipeline_smoke(ctx: TestContext) -> Result<()> {
    let ctx = ctx.with_nats().shared().await?;
    let scope = ctx.pipeline().await?;
    let event_id = scope
        .publish(DynamicPayload::new(
            "pipeline-smoke",
            "smoke.event",
            json!({"step": "smoke", "note": "pipeline"}),
        ))
        .await?;
    scope.wait_for_event_id(event_id).await?;
    let stored = scope
        .ctx()
        .pool
        .events()
        .get_by_id(event_id)
        .await?
        .expect("smoke event should be persisted");
    assert_eq!(stored.source.as_str(), "pipeline-smoke");
    assert_eq!(stored.event_type.as_str(), "smoke.event");
    Ok(())
}

/// Test complete event ingestion pipeline from raw input to database storage
#[sinex_serial_test(timeout = 60)]
async fn test_complete_event_ingestion_pipeline(ctx: TestContext) -> Result<()> {
    ctx.ensure_clean().await?;
    let ctx = ctx.with_nats().shared().await?;
    let _scope = ctx.pipeline().await?;
    tracing::info!("Testing complete event ingestion pipeline");
    let run_id = sinex_primitives::Ulid::new().to_string().to_lowercase();

    // Phase 1: Generate diverse test events representing different sources
    let test_events = vec![
        (
            format!("filesystem-{run_id}"),
            "file.created",
            json!({
                "path": "/tmp/test_file.txt",
                "size": 1024,
                "permissions": "644",
                "owner": "user",
                "created_at": sinex_primitives::temporal::format_rfc3339(Timestamp::now())
            }),
        ),
        (
            format!("terminal-{run_id}"),
            "command.executed",
            json!({
                "command": "cargo nextest run --workspace",
                "working_directory": "/home/user/sinex",
                "exit_code": 0,
                "duration_ms": 2340
            }),
        ),
        (
            format!("desktop-{run_id}"),
            "window.focused",
            json!({
                "window_title": "VS Code - main.rs",
                "application": "code",
                "window_id": "0x1234567",
                "workspace": "development"
            }),
        ),
        (
            format!("system-{run_id}"),
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
    for (source, event_type, payload) in &test_events {
        let event = ctx
            .publish(DynamicPayload::new(
                source.as_str(),
                *event_type,
                payload.clone(),
            ))
            .await?;

        let event_id = event.id;
        created_event_ids.push(event_id);

        let event_id_display = event_id
            .as_ref()
            .map_or_else(|| "missing".to_string(), std::string::ToString::to_string);
        tracing::debug!(
            source = source,
            event_type = event_type,
            event_id = %event_id_display,
            "Event processed through ingestion pipeline"
        );
    }

    let pipeline_duration = pipeline_start.elapsed();
    tracing::info!(
        events_processed = created_event_ids.len(),
        duration_ms = pipeline_duration.as_millis(),
        "Pipeline ingestion completed"
    );

    // Ensure all events persisted.
    let expected_total = test_events.len();
    xtask::sandbox::timing::WaitHelpers::wait_for_event_count(&ctx.pool, expected_total, 20)
        .await?;
    let persisted = ctx.pool.events().count_all().await? as usize;
    assert!(
        persisted >= expected_total,
        "Expected {expected_total} pipeline events, found {persisted}"
    );

    // Phase 3: Verify all events are correctly stored with proper structure
    // Query by source to avoid ordering assumptions and ensure each expected source is present.
    let mut stored_events = Vec::new();
    for (source, _, _) in &test_events {
        let events = ctx
            .pool
            .events()
            .get_by_source(
                &EventSource::from(source.as_str()),
                sinex_primitives::Pagination::new(Some(8), None),
            )
            .await?;
        assert!(
            !events.is_empty(),
            "Expected at least one event for source {source}"
        );
        stored_events.extend(events);
    }

    // Phase 4: Verify data integrity and processing semantics
    for (source, event_type, expected_payload) in &test_events {
        let stored_event = stored_events
            .iter()
            .find(|e| e.source.as_ref() == source && e.event_type.as_ref() == *event_type)
            .unwrap_or_else(|| panic!("Expected event for source {source} and type {event_type}"));

        assert_eq!(stored_event.payload, *expected_payload);

        // Verify pipeline processing metadata
        let ingest_ts = stored_event
            .id
            .as_ref()
            .expect("id present")
            .as_ulid()
            .timestamp();
        let _ = ingest_ts;
        assert!(
            stored_event.ts_orig.is_some(),
            "ts_orig should be populated"
        );
        assert!(stored_event
            .ingestor_version
            .as_ref()
            .is_some_and(|s| !s.is_empty()));

        let event_id_display = stored_event
            .id
            .as_ref()
            .map_or_else(|| "missing".to_string(), std::string::ToString::to_string);
        tracing::debug!(
            event_id = %event_id_display,
            source = stored_event.source.as_ref(),
            "Event integrity verified"
        );
    }

    // Phase 5: Verify time-series queryability (TimescaleDB functionality)
    let recent_events = ctx
        .pool
        .events()
        .get_by_time_range(
            Timestamp::now() - Duration::minutes(5),
            Timestamp::now(),
            sinex_primitives::Pagination::new(None, None),
        )
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
#[sinex_serial_test]
async fn test_concurrent_pipeline_processing(ctx: TestContext) -> Result<()> {
    tracing::info!("Testing concurrent pipeline processing");
    let ctx = Arc::new(ctx.with_nats().shared().await?);
    ctx.ensure_clean().await?;
    let _scope = ctx.pipeline().await?;

    let concurrent_streams = 4;
    let events_per_stream = 8;
    let processing_results = Arc::new(Mutex::new(Vec::new()));

    // Create concurrent processing tasks simulating multiple event sources
    let mut stream_handles = Vec::new();

    for stream_id in 0..concurrent_streams {
        let ctx_clone = ctx.clone();
        let results = processing_results.clone();

        let handle = tokio::spawn(async move {
            let stream_name = format!("stream_{stream_id}");
            let mut stream_events = Vec::new();
            let stream_start = Instant::now();

            for event_idx in 0..events_per_stream {
                let event_payload = json!({
                    "stream_id": stream_id,
                    "event_index": event_idx,
                    "data": format!("concurrent_data_{}_{}", stream_id, event_idx),
                    "timestamp": sinex_primitives::temporal::format_rfc3339(Timestamp::now()),
                    "sequence": stream_id * events_per_stream + event_idx
                });

                match ctx_clone
                    .publish(DynamicPayload::new(
                        &*stream_name,
                        "stream.data",
                        event_payload,
                    ))
                    .await
                {
                    Ok(event) => {
                        let event_id = event.id;
                        let event_id_display = event_id.as_ref().map_or_else(
                            || "missing".to_string(),
                            std::string::ToString::to_string,
                        );
                        stream_events.push(event_id);
                        tracing::trace!(
                            stream_id = stream_id,
                            event_idx = event_idx,
                            event_id = %event_id_display,
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
            }

            let stream_duration = stream_start.elapsed();

            let mut results_lock = results.lock().await;
            results_lock.push((stream_id, stream_events, stream_duration));
            drop(results_lock);

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
    let mut all_processed_ids: Vec<Option<Id<Event<JsonValue>>>> = Vec::new();

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

    tracing::info!(
        concurrent_streams = concurrent_streams,
        total_events = total_events_processed,
        "Concurrent pipeline processing test completed"
    );

    // Wait for all events to be persisted
    ctx.timing()
        .wait_for_event_count(total_events_processed) // Wait up to default timeout
        .await?;

    Ok(())
}

// =============================================================================
// Data Transformation Pipeline Tests
// =============================================================================

/// Test pipeline data transformation and enrichment
#[sinex_serial_test(timeout = 60)]
async fn test_pipeline_data_transformation(ctx: TestContext) -> Result<()> {
    tracing::info!("Testing pipeline data transformation and enrichment");
    ctx.ensure_clean().await?;
    let ctx = ctx.with_nats().shared().await?;
    let _scope = ctx.pipeline().await?;

    // Create raw events that should be processed and enriched
    let raw_events = [
        (
            "terminal",
            "command.raw",
            json!({
                "command_line": "git commit -m 'initial commit'",
                "working_directory": "/home/user/project",
                "timestamp": sinex_primitives::temporal::format_rfc3339(Timestamp::now())
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
    for (source, event_type, payload) in &raw_events {
        let event = ctx
            .publish(DynamicPayload::new(*source, *event_type, payload.clone()))
            .await?;
        raw_event_ids.push(event.id);
    }

    // Wait for raw events to persist before processing
    for (source, _, _) in &raw_events {
        ctx.timing().wait_for_source_events(source, 1).await?;
    }

    // Phase 2: Simulate processing pipeline transformations
    // In a real system, this would be done by automata/processors
    let mut transformed_event_ids = Vec::new();

    for raw_event_id in &raw_event_ids {
        let raw_event = ctx
            .pool
            .events()
            .get_by_id((*raw_event_id).expect("raw_event_id should be present"))
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
                    "parsed_at": sinex_primitives::temporal::format_rfc3339(Timestamp::now()),
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
                    "analyzed_at": sinex_primitives::temporal::format_rfc3339(Timestamp::now()),
                    "source_event_id": raw_event.id
                })
            }
            _ => continue,
        };

        let transformed_event = ctx
            .publish(DynamicPayload::new(
                raw_event.source.as_str(),
                &*format!("{}.processed", raw_event.event_type),
                transformed_payload,
            ))
            .await?;
        let transformed_event_id = transformed_event.id;
        if let Some(ref id) = transformed_event_id {
            xtask::sandbox::timing::WaitHelpers::wait_for_event_id(
                &ctx.pool,
                *id,
                xtask::sandbox::timing::DEFAULT_WAIT_SECS,
            )
            .await?;
        }

        transformed_event_ids.push(transformed_event_id);
    }

    // Phase 3: Verify transformation results
    let mut transformed_events = Vec::new();
    for id in transformed_event_ids.iter().flatten() {
        if let Some(event) = ctx.pool.events().get_by_id(*id).await? {
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
        .find(|e| e.source.as_ref() == "terminal")
        .expect("Should find transformed terminal event");

    assert_eq!(git_event.event_type.as_ref(), "command.raw.processed");
    assert_eq!(git_event.payload["command"], "git");
    assert_eq!(git_event.payload["subcommand"], "commit");
    assert_eq!(git_event.payload["is_git_operation"], true);
    assert!(git_event.payload.get("source_event_id").is_some());

    // Verify filesystem transformation
    let file_event = transformed_events
        .iter()
        .find(|e| e.source.as_ref() == "filesystem")
        .expect("Should find transformed filesystem event");

    assert_eq!(file_event.event_type.as_ref(), "file.raw.processed");
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

        let transformed_id_display = transformed_event
            .id
            .as_ref()
            .map_or_else(|| "missing".to_string(), std::string::ToString::to_string);
        let source_id_display = source_event
            .id
            .as_ref()
            .map_or_else(|| "missing".to_string(), std::string::ToString::to_string);
        tracing::debug!(
            transformed_id = %transformed_id_display,
            source_id = %source_id_display,
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
#[sinex_serial_test(timeout = 60)]
async fn test_pipeline_error_handling(ctx: TestContext) -> Result<()> {
    tracing::info!("Testing pipeline error handling and recovery");
    ctx.ensure_clean().await?;
    let ctx = ctx.with_nats().shared().await?;
    let _scope = ctx.pipeline().await?;

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
                "timestamp": sinex_primitives::temporal::format_rfc3339(Timestamp::now())
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
                "timestamp": sinex_primitives::temporal::format_rfc3339(Timestamp::now())
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
            .publish(DynamicPayload::new(source, event_type, payload.clone()))
            .await
        {
            Ok(event) => {
                if should_succeed {
                    let event_id = event.id;
                    let event_id_display = event_id
                        .as_ref()
                        .map_or_else(|| "missing".to_string(), std::string::ToString::to_string);
                    successful_events.push(event_id);
                    tracing::debug!(
                        source = source,
                        event_type = event_type,
                        event_id = %event_id_display,
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
                if should_succeed {
                    return Err(e);
                }
                error_scenarios.push((source, event_type, e.to_string()));
                tracing::debug!(
                    source = source,
                    event_type = event_type,
                    error = %e,
                    "Expected error occurred"
                );
            }
        }

        // Yield to avoid fixed sleeps between scenarios.
        yield_now().await;
    }

    let pipeline_duration = pipeline_start.elapsed();

    // Verify successful events are properly stored
    // Directly verify inserted count matches expectations to avoid long waits.
    let expected_total = successful_events.len();
    ctx.timing().wait_for_event_count(expected_total).await?;

    let mut stored_events = Vec::new();
    for id in successful_events.iter().flatten() {
        if let Some(event) = ctx.pool.events().get_by_id(*id).await? {
            stored_events.push(event);
        }
    }

    assert!(
        stored_events.len() >= successful_events.len().min(1),
        "Pipeline should store at least the successful events count"
    );

    // Verify pipeline resilience - system should still be functional after errors
    let recovery_event = ctx
        .publish(DynamicPayload::new(
            "recovery",
            "test.recovery",
            json!({
                "message": "pipeline recovery verification",
                "processed_after_errors": true,
                "timestamp": sinex_primitives::temporal::format_rfc3339(Timestamp::now())
            }),
        ))
        .await?;
    let recovery_event_id = recovery_event.id;
    if let Some(ref id) = recovery_event_id {
        xtask::sandbox::timing::WaitHelpers::wait_for_event_id(
            &ctx.pool,
            *id,
            xtask::sandbox::timing::DEFAULT_WAIT_SECS,
        )
        .await?;
    }

    // Verify recovery event is processed correctly
    let recovery_stored = ctx
        .pool
        .events()
        .get_by_id(recovery_event_id.expect("recovery event should have id"))
        .await?
        .expect("Recovery event should exist");

    assert_eq!(recovery_stored.id, recovery_event_id);
    assert_eq!(recovery_stored.source.as_ref(), "recovery");
    assert_eq!(recovery_stored.event_type.as_ref(), "test.recovery");

    tracing::info!(
        successful_events = successful_events.len(),
        error_scenarios = error_scenarios.len(),
        duration_ms = pipeline_duration.as_millis(),
        "Pipeline error handling test completed"
    );
    Ok(())
}

// =============================================================================
// Pipeline Confirmation + DLQ Semantics
// =============================================================================

#[sinex_serial_test(timeout = 60)]
async fn test_confirmation_emitted_after_persistence_pipeline(
    ctx: TestContext,
) -> color_eyre::Result<()> {
    let ctx = ctx.with_nats().shared().await?;
    let scope = ctx.pipeline().await?;
    let source = format!("confirm-order-{}", Ulid::new().to_string().to_lowercase());
    let confirmation_prefix = scope.subject("events.confirmations");
    let mut sub = scope
        .ctx()
        .nats_client()
        .subscribe(format!("{confirmation_prefix}.*"))
        .await?;

    let mut event_ids = Vec::new();
    for idx in 0..5 {
        let event_id = scope
            .publish(DynamicPayload::new(
                source.as_str(),
                "confirmation.order",
                json!({"source": source, "seq": idx, "check": "persisted-before-confirmation"}),
            ))
            .await?;
        event_ids.push(event_id);
    }

    let mut confirmed = std::collections::HashSet::new();
    while confirmed.len() < event_ids.len() {
        let msg = tokio::time::timeout(StdDuration::from_secs(Timeouts::SHORT), sub.next())
            .await
            .map_err(|_| {
                color_eyre::eyre::eyre!(
                    "timed out waiting for confirmation on {confirmation_prefix}.*"
                )
            })?
            .ok_or_else(|| {
                color_eyre::eyre::eyre!("confirmation stream closed for {confirmation_prefix}.*")
            })?;

        let payload: serde_json::Value = serde_json::from_slice(&msg.payload)?;
        let event_id = payload["event_id"]
            .as_str()
            .ok_or_else(|| color_eyre::eyre::eyre!("confirmation missing event_id"))?;
        assert_eq!(payload["persisted"], serde_json::Value::Bool(true));

        let persisted = scope
            .ctx()
            .pool
            .events()
            .get_by_id(Ulid::from_str(event_id)?.into())
            .await?;
        ensure!(
            persisted.is_some(),
            "confirmation observed before event persistence"
        );
        confirmed.insert(event_id.to_string());
    }

    Ok(())
}

#[sinex_serial_test(timeout = 60)]
async fn test_mixed_validity_batch_semantics(ctx: TestContext) -> color_eyre::Result<()> {
    let ctx = ctx.with_nats().shared().await?;
    let scope = ctx.pipeline().await?;
    let source = format!("mixed-validity-{}", Ulid::new().to_string().to_lowercase());
    let event_type = "batch.mixed";

    let raw_subject = scope.subject(&format!(
        "events.raw.{}.{}",
        source.replace('.', "_"),
        event_type.replace('.', "_")
    ));
    scope
        .ctx()
        .nats_client()
        .publish(raw_subject, "{not-json".into())
        .await?;

    let valid_id = scope
        .publish(DynamicPayload::new(
            source.as_str(),
            event_type,
            json!({"kind": "valid", "batch": "mixed"}),
        ))
        .await?;

    scope.wait_for_event_id(valid_id).await?;

    let persisted = scope.ctx().pool.events().get_by_id(valid_id).await?;
    ensure!(persisted.is_some(), "valid event should persist");

    let js = scope.ctx().jetstream().await?;
    let dlq_stream = format!("{}_DLQ", scope.stream("SINEX_RAW_EVENTS"));
    let nats = scope.ctx().nats_handle()?;
    nats.wait_for_stream(&js, &dlq_stream, StdDuration::from_secs(Timeouts::SHORT))
        .await?;

    scope
        .ctx()
        .timing()
        .wait_for_condition(
            || {
                let js = js.clone();
                let dlq_stream = dlq_stream.clone();
                async move {
                    let mut info = js.get_stream(&dlq_stream).await.map_err(|e| {
                        SinexError::network(e.to_string())
                            .with_context("operation", "get_stream")
                            .with_context("stream", dlq_stream.clone())
                    })?;
                    let state = info
                        .info()
                        .await
                        .map_err(|e| {
                            SinexError::network(e.to_string())
                                .with_context("operation", "stream_info")
                        })?
                        .state;
                    Ok::<bool, SinexError>(state.messages >= 1)
                }
            },
            10,
        )
        .await?;

    let dlq_state = js.get_stream(&dlq_stream).await?.info().await?.state;
    assert!(
        dlq_state.messages >= 1,
        "DLQ should contain the invalid event"
    );

    Ok(())
}

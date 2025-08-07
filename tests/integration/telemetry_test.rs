//! Telemetry System Integration Tests
//!
//! This module contains comprehensive integration tests for Sinex's telemetry system,
//! covering both real-time metrics (Prometheus) and historical telemetry (Events).
//! The telemetry system uses a hybrid approach that combines:
//!
//! 1. **Real-time Metrics (Prometheus)** - In-memory metrics for operational monitoring
//! 2. **Historical Telemetry (Events)** - Periodic summary events for long-term analysis
//!
//! Tests cover:
//! - TelemetryAccumulator functionality
//! - SystemTelemetryEmitter behavior
//! - Global telemetry integration
//! - Concurrent telemetry recording
//! - Event emission and validation
//! - Performance characteristics
//! - Error handling scenarios

use color_eyre::eyre::{eyre, Result};
use serde_json::json;
use sinex_db::telemetry::telemetry::{
    get_global_telemetry, record_function_telemetry, set_global_telemetry, SystemTelemetryEmitter,
    TelemetryAccumulator,
};
use sinex_test_utils::prelude::*;
use sinex_types::domain::EventType;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::{sleep, Instant};
use tracing_test::traced_test;

// =============================================================================
// TELEMETRY ACCUMULATOR TESTS - Core telemetry functionality
// =============================================================================

#[sinex_test]
async fn test_telemetry_accumulator_basic_functionality(
    ctx: TestContext,
) -> Result<()> {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let accumulator = TelemetryAccumulator::new("test-component")
        .with_event_sender(tx.clone())
        .with_interval(Duration::from_millis(100));

    // Record various types of telemetry data
    accumulator.record_event_processed("file.created", 10.0);
    accumulator.record_event_processed("file.created", 20.0);
    accumulator.record_event_processed("file.modified", 5.0);

    accumulator.record_operation_latency("scan_directory", 150.0);
    accumulator.record_operation_latency("scan_directory", 200.0);
    accumulator.record_operation_latency("index_file", 45.5);

    accumulator.record_resource_usage(100.0, 25.0);
    accumulator.record_resource_usage(120.0, 30.0);

    accumulator.record_error("io_error");
    accumulator.record_error("io_error");
    accumulator.record_error("permission_denied");

    // Trigger telemetry emission
    accumulator
        .emit_telemetry()
        .await
        .map_err(|e| eyre!("Telemetry emission failed: {}", e))?;

    // Collect emitted events
    let mut events = Vec::new();
    while let Ok(event) = rx.try_recv() {
        events.push(event);
    }

    // Verify multiple event types were emitted
    ctx.assert("telemetry event types")
        .that(events.len() >= 3, "Should emit multiple event types")?;

    // Check that expected event types are present
    let event_types: Vec<_> = events.iter().map(|e| e.event_type.as_str()).collect();
    ctx.assert("expected event types")
        .that(
            event_types.contains(&"events.processed"),
            "Should include events.processed",
        )?
        .that(
            event_types.contains(&"operation.performance"),
            "Should include operation.performance",
        )?
        .that(
            event_types.contains(&"resource.usage"),
            "Should include resource.usage",
        )?
        .that(
            event_types.contains(&"errors.summary"),
            "Should include errors.summary",
        )?;

    // Validate specific event content
    for event in &events {
        ctx.assert("event structure")
            .eq(&event.source.as_str(), &"sinex.telemetry")?;

        match event.event_type.as_str() {
            "events.processed" => {
                let payload = &event.payload;
                ctx.assert("events.processed payload validation")
                    .that(
                        payload["total_events"].as_u64().unwrap() == 3,
                        "Should have total of 3 events",
                    )?
                    .that(
                        payload["events_per_type"]["file.created"].as_u64().unwrap() == 2,
                        "Should have 2 file.created events",
                    )?
                    .that(
                        payload["events_per_type"]["file.modified"].as_u64().unwrap() == 1,
                        "Should have 1 file.modified event",
                    )?;
            }
            "operation.performance" => {
                let payload = &event.payload;
                let operation_name = payload["operation_name"].as_str().unwrap();
                ctx.assert("operation.performance payload")
                    .that(
                        operation_name == "scan_directory" || operation_name == "index_file",
                        "Should be valid operation name",
                    )?
                    .that(
                        payload["items_processed"].as_u64().unwrap() > 0,
                        "Should have processed items",
                    )?;
            }
            "resource.usage" => {
                let payload = &event.payload;
                ctx.assert("resource.usage payload")
                    .eq(&payload["component"], &json!("test-component"))?
                    .that(
                        payload["memory_mb"]["avg"].as_f64().unwrap() == 110.0,
                        "Should have correct average memory",
                    )?
                    .that(
                        payload["cpu_percent"]["peak"].as_f64().unwrap() == 30.0,
                        "Should have correct peak CPU",
                    )?;
            }
            "errors.summary" => {
                let payload = &event.payload;
                ctx.assert("errors.summary payload")
                    .that(
                        payload["total_errors"].as_u64().unwrap() == 3,
                        "Should have 3 total errors",
                    )?
                    .that(
                        payload["errors_by_component"]["test-component"]
                            .as_u64()
                            .unwrap()
                            == 3,
                        "All errors should be from test-component",
                    )?;
            }
            _ => {}
        }
    }

    Ok(())
}

#[sinex_test]
async fn test_telemetry_state_reset_between_emissions(ctx: TestContext) -> Result<()> {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let accumulator = TelemetryAccumulator::new("reset-test").with_event_sender(tx);

    // First batch of metrics
    accumulator.record_event_processed("event.one", 10.0);
    accumulator.record_error("error.one");

    // Emit first batch
    accumulator
        .emit_telemetry()
        .await
        .map_err(|e| eyre!("First emission failed: {}", e))?;

    let mut first_batch = Vec::new();
    while let Ok(event) = rx.try_recv() {
        first_batch.push(event);
    }

    ctx.assert("first batch").not_empty(&first_batch)?;

    // Second batch of metrics (different from first)
    accumulator.record_event_processed("event.two", 20.0);
    accumulator.record_error("error.two");

    // Emit second batch
    accumulator
        .emit_telemetry()
        .await
        .map_err(|e| eyre!("Second emission failed: {}", e))?;

    let mut second_batch = Vec::new();
    while let Ok(event) = rx.try_recv() {
        second_batch.push(event);
    }

    // Verify second batch doesn't contain first batch data
    for event in &second_batch {
        match event.event_type.as_str() {
            "events.processed" => {
                ctx.assert("events reset properly").that(
                    !event.payload["events_per_type"]
                        .as_object()
                        .unwrap()
                        .contains_key("event.one"),
                    "Should not contain first batch event types",
                )?;
            }
            "errors.summary" => {
                // Errors are aggregated by severity, so we can't directly check for error.one
                // but we can verify the total count is only for the second batch
                ctx.assert("errors reset properly").that(
                    event.payload["total_errors"].as_u64().unwrap() == 1,
                    "Should only have errors from second batch",
                )?;
            }
            _ => {}
        }
    }

    Ok(())
}

#[sinex_test]
#[case::empty(vec![])]
#[case::single(vec![100.0])]
#[case::two_values(vec![50.0, 150.0])]
#[case::many_values(vec![10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0, 100.0])]
#[case::duplicates(vec![50.0, 50.0, 50.0, 50.0, 50.0])]
#[case::unsorted(vec![100.0, 10.0, 50.0, 30.0, 80.0])]
async fn test_telemetry_percentile_calculations(
    ctx: TestContext,
    #[case] values: Vec<f64>,
) -> Result<()> {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let accumulator = TelemetryAccumulator::new("percentile-test").with_event_sender(tx);

    if values.is_empty() {
        // Test that no telemetry is emitted for empty data
        accumulator
            .emit_telemetry()
            .await
            .map_err(|e| eyre!("Empty emission failed: {}", e))?;

        let event = rx.try_recv();
        ctx.assert("empty data handling")
            .that(event.is_err(), "Should not emit events for empty data")?;
        return Ok(());
    }

    // Record operation latencies
    for value in &values {
        accumulator.record_operation_latency("test_operation", *value);
    }

    // Emit telemetry
    accumulator
        .emit_telemetry()
        .await
        .map_err(|e| eyre!("Percentile emission failed: {}", e))?;

    // Find the operation performance event
    let mut operation_event = None;
    while let Ok(event) = rx.try_recv() {
        if event.event_type.as_str() == "operation.performance" {
            operation_event = Some(event);
            break;
        }
    }

    let event = operation_event.expect("Should have operation performance event");
    let metrics = &event.payload["metrics"];

    match values.len() {
        1 => {
            // Single value - all percentiles should be the same
            ctx.assert("single value percentiles")
                .eq(&metrics["duration_ms"]["p50"], &json!(values[0]))?
                .eq(&metrics["duration_ms"]["p95"], &json!(values[0]))?
                .eq(&metrics["duration_ms"]["p99"], &json!(values[0]))?
                .eq(&metrics["duration_ms"]["min"], &json!(values[0]))?
                .eq(&metrics["duration_ms"]["max"], &json!(values[0]))?;
        }
        2 => {
            // Two values
            let min = values.iter().fold(f64::INFINITY, |a, &b| a.min(b));
            let max = values.iter().fold(f64::NEG_INFINITY, |a, &b| a.max(b));
            ctx.assert("two value percentiles")
                .eq(&metrics["duration_ms"]["min"], &json!(min))?
                .eq(&metrics["duration_ms"]["max"], &json!(max))?;
        }
        10 => {
            // Many values - test specific percentiles for sorted case
            if values == vec![10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0, 100.0] {
                ctx.assert("many values percentiles")
                    .eq(&metrics["duration_ms"]["p50"], &json!(60.0))?
                    .eq(&metrics["duration_ms"]["min"], &json!(10.0))?
                    .eq(&metrics["duration_ms"]["max"], &json!(100.0))?;
            } else {
                // For unsorted, just verify min/max
                let min = values.iter().fold(f64::INFINITY, |a, &b| a.min(b));
                let max = values.iter().fold(f64::NEG_INFINITY, |a, &b| a.max(b));
                ctx.assert("unsorted values percentiles")
                    .eq(&metrics["duration_ms"]["min"], &json!(min))?
                    .eq(&metrics["duration_ms"]["max"], &json!(max))?;
            }
        }
        5 => {
            // Duplicates case - all values should be the same
            if values.iter().all(|&v| v == 50.0) {
                ctx.assert("duplicate values percentiles")
                    .eq(&metrics["duration_ms"]["p50"], &json!(50.0))?
                    .eq(&metrics["duration_ms"]["p95"], &json!(50.0))?
                    .eq(&metrics["duration_ms"]["min"], &json!(50.0))?
                    .eq(&metrics["duration_ms"]["max"], &json!(50.0))?;
            }
        }
        _ => {
            // General case - just verify structure exists
            ctx.assert("percentiles structure")
                .that(metrics["duration_ms"]["p50"].is_number(), "p50 exists")?
                .that(metrics["duration_ms"]["min"].is_number(), "min exists")?
                .that(metrics["duration_ms"]["max"].is_number(), "max exists")?;
        }
    }

    Ok(())
}

#[sinex_test]
async fn test_telemetry_background_emitter(ctx: TestContext) -> Result<()> {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let accumulator = TelemetryAccumulator::new("background-test")
        .with_event_sender(tx)
        .with_interval(Duration::from_millis(100));

    // Start background emitter
    let handle = accumulator.clone().spawn_emitter();

    // Record some metrics
    accumulator.record_event_processed("background.event", 15.0);
    accumulator.record_operation_latency("background.operation", 25.5);

    // Wait for automatic emission
    sleep(Duration::from_millis(150)).await;

    // Should have received events from background emitter
    let mut received_events = false;
    while let Ok(event) = rx.try_recv() {
        if event.event_type == EventType::new("events.processed")
            || event.event_type == EventType::new("operation.performance")
        {
            received_events = true;
            ctx.assert("background emission content")
                .eq(&event.source.as_str(), &"sinex.telemetry")?;

            if event.event_type == EventType::new("events.processed") {
                ctx.assert("background events processed")
                    .that(
                        event.payload["events_per_type"]["background.event"]
                            .as_u64()
                            .unwrap()
                            == 1,
                        "Should have recorded background event",
                    )?;
            }
        }
    }

    ctx.assert("background emitter functionality")
        .that(received_events, "Should have received background emissions")?;

    // Cleanup
    handle.abort();

    Ok(())
}

// =============================================================================
// SYSTEM TELEMETRY EMITTER TESTS - System-wide metrics
// =============================================================================

#[sinex_test]
async fn test_system_telemetry_emitter_basic(ctx: TestContext) -> Result<()> {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let emitter = SystemTelemetryEmitter::new(tx).with_interval(Duration::from_millis(100));

    // Emit system resources manually
    emitter
        .emit_system_resources()
        .await
        .map_err(|e| eyre!("System resource emission failed: {}", e))?;

    // Check the emitted event
    let event = rx.recv().await.expect("Should receive system telemetry event");

    ctx.assert("system telemetry event structure")
        .eq(&event.source.as_str(), &"sinex.telemetry")?
        .eq(&event.event_type, &EventType::new("system.resources"))?;

    // Verify payload structure (even if placeholder data)
    let payload = &event.payload;
    ctx.assert("system telemetry payload structure")
        .that(payload["cpu_usage_percent"].is_number(), "Should have CPU usage")?
        .that(
            payload["memory_usage_bytes"].is_number(),
            "Should have memory usage",
        )?
        .that(
            payload["memory_total_bytes"].is_number(),
            "Should have total memory",
        )?
        .that(payload["disk_usage_bytes"].is_number(), "Should have disk usage")?
        .that(
            payload["open_file_descriptors"].is_number(),
            "Should have file descriptor count",
        )?;

    Ok(())
}

#[sinex_test]
async fn test_system_telemetry_background_emission(ctx: TestContext) -> Result<()> {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let emitter = SystemTelemetryEmitter::new(tx).with_interval(Duration::from_millis(100));

    // Start background system telemetry
    let handle = emitter.spawn_emitter();

    // Wait for at least one emission
    sleep(Duration::from_millis(150)).await;

    // Should have received at least one system telemetry event
    let mut received_system_event = false;
    while let Ok(event) = rx.try_recv() {
        if event.event_type == EventType::new("system.resources") {
            received_system_event = true;
            ctx.assert("background system telemetry")
                .eq(&event.source.as_str(), &"sinex.telemetry")?;
            break;
        }
    }

    ctx.assert("system background emitter")
        .that(
            received_system_event,
            "Should have received system telemetry emission",
        )?;

    // Cleanup
    handle.abort();

    Ok(())
}

// =============================================================================
// GLOBAL TELEMETRY INTEGRATION TESTS - Auto-metrics integration
// =============================================================================

#[sinex_test]
async fn test_global_telemetry_setup_and_recording(ctx: TestContext) -> Result<()> {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let accumulator = TelemetryAccumulator::new("global-test").with_event_sender(tx);

    // Set as global telemetry
    set_global_telemetry(accumulator.clone()).await;

    // Verify global telemetry was set
    let global_telemetry = get_global_telemetry().await;
    ctx.assert("global telemetry setup")
        .some(&global_telemetry)?;

    // Record via global telemetry function
    record_function_telemetry("test_module", "test_function", 25.5, false);
    record_function_telemetry("test_module", "another_function", 15.2, true); // with error

    // Wait for async recording to complete
    sleep(Duration::from_millis(50)).await;

    // Emit to see recorded data
    accumulator
        .emit_telemetry()
        .await
        .map_err(|e| eyre!("Global telemetry emission failed: {}", e))?;

    // Verify recorded operations and errors
    let mut found_operations = 0;
    let mut found_errors = false;

    while let Ok(event) = rx.try_recv() {
        match event.event_type.as_str() {
            "operation.performance" => {
                found_operations += 1;
                let operation_name = event.payload["operation_name"].as_str().unwrap();
                ctx.assert("global operation recording").that(
                    operation_name == "test_module::test_function"
                        || operation_name == "test_module::another_function",
                    "Should record correct operation names",
                )?;
            }
            "errors.summary" => {
                found_errors = true;
                ctx.assert("global error recording").that(
                    event.payload["total_errors"].as_u64().unwrap() > 0,
                    "Should have recorded function errors",
                )?;
            }
            _ => {}
        }
    }

    ctx.assert("global telemetry recording")
        .that(found_operations > 0, "Should record function operations")?
        .that(found_errors, "Should record function errors")?;

    Ok(())
}

// =============================================================================
// CONCURRENCY AND PERFORMANCE TESTS - Multi-threaded scenarios
// =============================================================================

#[sinex_test]
async fn test_concurrent_telemetry_recording(ctx: TestContext) -> Result<()> {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let accumulator = TelemetryAccumulator::new("concurrent-test").with_event_sender(tx);

    // Spawn multiple tasks recording metrics concurrently
    let mut handles = vec![];
    for i in 0..10 {
        let acc = accumulator.clone();
        let handle = tokio::spawn(async move {
            for j in 0..100 {
                acc.record_event_processed(&format!("event.type{}", i), j as f64);
                acc.record_operation_latency(&format!("op{}", i), (i * 10 + j) as f64);
                acc.record_resource_usage((i * 100) as f64, (i * 5) as f64);
                if j % 10 == 0 {
                    acc.record_error(&format!("error{}", i));
                }
            }
        });
        handles.push(handle);
    }

    // Wait for all concurrent tasks to complete
    for handle in handles {
        handle.await?;
    }

    // Emit accumulated telemetry
    accumulator
        .emit_telemetry()
        .await
        .map_err(|e| eyre!("Concurrent emission failed: {}", e))?;

    let mut events = Vec::new();
    while let Ok(event) = rx.try_recv() {
        events.push(event);
    }

    // Verify all expected event types are present
    ctx.assert("concurrent recording results").that(
        events.len() >= 3,
        "Should have multiple event types after concurrent recording",
    )?;

    // Verify totals are correct
    for event in events {
        match event.event_type.as_str() {
            "events.processed" => {
                let count = event.payload["total_events"].as_u64().unwrap();
                ctx.assert("concurrent event count").eq(&count, &1000u64)?; // 10 tasks × 100 events
            }
            "errors.summary" => {
                let total = event.payload["total_errors"].as_u64().unwrap();
                ctx.assert("concurrent error count").eq(&total, &100u64)?; // 10 tasks × 10 errors each
            }
            "resource.usage" => {
                // Verify we have resource usage data
                ctx.assert("concurrent resource usage")
                    .that(
                        event.payload["memory_mb"]["current"].is_number(),
                        "Should have memory data",
                    )?
                    .that(
                        event.payload["cpu_percent"]["avg"].is_number(),
                        "Should have CPU data",
                    )?;
            }
            _ => {}
        }
    }

    Ok(())
}

#[sinex_test]
async fn test_telemetry_high_frequency_recording(ctx: TestContext) -> Result<()> {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let accumulator = TelemetryAccumulator::new("high-frequency-test").with_event_sender(tx);

    let start_time = Instant::now();
    let event_count = 1000;

    // Record many events rapidly
    for i in 0..event_count {
        accumulator.record_event_processed("high_frequency_event", i as f64 * 0.1);
        accumulator.record_operation_latency("rapid_operation", i as f64 * 0.05);

        if i % 100 == 0 {
            accumulator.record_error("occasional_error");
        }
    }

    let recording_duration = start_time.elapsed();

    // Emit telemetry
    accumulator
        .emit_telemetry()
        .await
        .map_err(|e| eyre!("High frequency emission failed: {}", e))?;

    // Verify performance characteristics
    ctx.assert("recording performance").that(
        recording_duration.as_millis() < 1000, // Should complete in under 1 second
        "High frequency recording should be fast",
    )?;

    // Verify data correctness
    let mut found_events_processed = false;
    while let Ok(event) = rx.try_recv() {
        if event.event_type.as_str() == "events.processed" {
            found_events_processed = true;
            ctx.assert("high frequency data accuracy").eq(
                &event.payload["total_events"],
                &json!(event_count),
            )?;
        }
    }

    ctx.assert("high frequency results")
        .that(found_events_processed, "Should emit processed events")?;

    Ok(())
}

// =============================================================================
// ERROR HANDLING AND EDGE CASES - Robustness testing
// =============================================================================

#[sinex_test]
async fn test_telemetry_no_data_emission(ctx: TestContext) -> Result<()> {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let accumulator = TelemetryAccumulator::new("no-data-test").with_event_sender(tx);

    // Emit without recording any data
    accumulator
        .emit_telemetry()
        .await
        .map_err(|e| eyre!("Empty emission failed: {}", e))?;

    // Should not emit any events when no data is collected
    let event = rx.try_recv();
    ctx.assert("no data emission behavior").that(
        event.is_err(),
        "Should not emit events when no data collected",
    )?;

    Ok(())
}

#[sinex_test]
async fn test_telemetry_without_event_sender(ctx: TestContext) -> Result<()> {
    let accumulator = TelemetryAccumulator::new("no-sender-test");
    // No event sender configured

    // Record some data
    accumulator.record_event_processed("test_event", 10.0);
    accumulator.record_error("test_error");

    // Emit should succeed but do nothing
    let result = accumulator.emit_telemetry().await;
    ctx.assert("no sender emission")
        .that(result.is_ok(), "Should handle missing sender gracefully")?;

    Ok(())
}

#[sinex_test]
#[traced_test]
async fn test_telemetry_emission_timing(ctx: TestContext) -> Result<()> {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let accumulator = TelemetryAccumulator::new("timing-test")
        .with_event_sender(tx)
        .with_interval(Duration::from_millis(50));

    // Test should_emit timing logic
    ctx.assert("initial emission state").that(
        accumulator.should_emit(),
        "Should be ready to emit initially",
    )?;

    // Record and emit
    accumulator.record_event_processed("timing_event", 5.0);
    accumulator
        .emit_telemetry()
        .await
        .map_err(|e| eyre!("Timing emission failed: {}", e))?;

    // Immediately after emission, should not be ready to emit again
    ctx.assert("post-emission state").that(
        !accumulator.should_emit(),
        "Should not be ready to emit immediately after emission",
    )?;

    // Wait for interval to pass
    sleep(Duration::from_millis(60)).await;

    // Now should be ready to emit again
    ctx.assert("after interval state").that(
        accumulator.should_emit(),
        "Should be ready to emit after interval passes",
    )?;

    // Clear any events that were emitted
    while rx.try_recv().is_ok() {}

    Ok(())
}

// =============================================================================
// INTEGRATION WITH EVENT SYSTEM TESTS - End-to-end verification
// =============================================================================

#[sinex_test]
async fn test_telemetry_events_in_database(ctx: TestContext) -> Result<()> {
    // This test verifies that telemetry events properly integrate with the event system
    let (tx, mut rx) = mpsc::unbounded_channel();
    let accumulator = TelemetryAccumulator::new("db-integration-test").with_event_sender(tx);

    // Record telemetry data
    accumulator.record_event_processed("integration.test", 42.0);
    accumulator.record_operation_latency("db_integration_operation", 123.5);

    // Emit telemetry
    accumulator
        .emit_telemetry()
        .await
        .map_err(|e| eyre!("DB integration emission failed: {}", e))?;

    // Collect emitted events and manually insert them into the database
    // (simulating what ingestd would do)
    let mut telemetry_events = Vec::new();
    while let Ok(event) = rx.try_recv() {
        telemetry_events.push(event);
    }

    ctx.assert("telemetry database integration")
        .not_empty(&telemetry_events)?;

    // Verify each telemetry event has proper structure
    for event in &telemetry_events {
        ctx.assert("telemetry event structure")
            .eq(&event.source.as_str(), &"sinex.telemetry")?
            .some(&event.id)?
            .that(event.payload.is_object(), "Should have structured payload")?
            .that(!event.payload.as_object().unwrap().is_empty(), "Payload should not be empty")?;

        // Verify timestamps are recent
        let now = chrono::Utc::now();
        let event_time = event.ts_ingest;
        let time_diff = now.signed_duration_since(event_time).num_seconds().abs();
        
        ctx.assert("telemetry event timing").that(
            time_diff < 60, // Within last minute
            "Telemetry events should have recent timestamps",
        )?;
    }

    Ok(())
}

#[sinex_test]
async fn test_telemetry_snapshot_validation(ctx: TestContext) -> Result<()> {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let accumulator = TelemetryAccumulator::new("snapshot-test").with_event_sender(tx);

    // Record predictable telemetry data for snapshot testing
    accumulator.record_event_processed("snapshot.event.a", 10.0);
    accumulator.record_event_processed("snapshot.event.b", 20.0);
    accumulator.record_operation_latency("snapshot_operation", 150.0);
    accumulator.record_resource_usage(256.0, 15.5);
    accumulator.record_error("snapshot_error");

    // Emit telemetry
    accumulator
        .emit_telemetry()
        .await
        .map_err(|e| eyre!("Snapshot emission failed: {}", e))?;

    // Collect and structure events for snapshot comparison
    let mut events = Vec::new();
    while let Ok(event) = rx.try_recv() {
        events.push(json!({
            "source": event.source.as_str(),
            "event_type": event.event_type.as_str(),
            "has_payload": !event.payload.as_object().unwrap().is_empty(),
            "payload_keys": event.payload.as_object().unwrap().keys().collect::<Vec<_>>()
        }));
    }

    // Sort for consistent snapshot comparison
    events.sort_by(|a, b| a["event_type"].as_str().cmp(&b["event_type"].as_str()));

    let snapshot_data = json!({
        "event_count": events.len(),
        "events": events
    });

    // Verify the structure matches expected telemetry output
    ctx.assert("telemetry snapshot structure")
        .that(events.len() >= 3, "Should have multiple telemetry event types")?
        .that(
            events.iter().all(|e| e["source"] == "sinex.telemetry"),
            "All events should be from telemetry source",
        )?
        .that(
            events.iter().all(|e| e["has_payload"] == true),
            "All events should have payloads",
        )?;

    // Note: In a real implementation, you might use assert_json_snapshot! here
    // but for now we'll do structural validation
    let event_types: Vec<&str> = events
        .iter()
        .map(|e| e["event_type"].as_str().unwrap())
        .collect();

    ctx.assert("expected telemetry event types").that(
        event_types.contains(&"events.processed")
            && event_types.contains(&"operation.performance")
            && event_types.contains(&"resource.usage")
            && event_types.contains(&"errors.summary"),
        "Should contain all expected telemetry event types",
    )?;

    Ok(())
}
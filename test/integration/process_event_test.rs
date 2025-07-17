// Process Event Integration Tests
//
// This module tests the event-based process lifecycle system, including:
// - ProcessHeartbeatEmitter functionality
// - Process lifecycle events (started, heartbeat, shutdown)
// - Health monitoring through events instead of database tables
// - Process metrics collection and reporting
// - Integration with the health aggregator

use crate::common::prelude::*;
use chrono::{Duration, Utc};
use sinex_core_runtime::{MetricsProvider, ProcessHeartbeatEmitter};
use sinex_db::queries::{EventQueries, CheckpointQueries};
use sinex_db::query_builder::{QueryBuilder, QueryParam};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::time::{sleep, timeout};

// =============================================================================
// Mock Metrics Provider for Testing
// =============================================================================

#[derive(Clone)]
struct MockMetricsProvider {
    uptime: Arc<Mutex<u64>>,
    memory_mb: Arc<Mutex<u32>>,
    events_processed: Arc<Mutex<u64>>,
    custom_metrics: Arc<Mutex<HashMap<String, serde_json::Value>>>,
}

impl MockMetricsProvider {
    fn new() -> Self {
        Self {
            uptime: Arc::new(Mutex::new(0)),
            memory_mb: Arc::new(Mutex::new(128)),
            events_processed: Arc::new(Mutex::new(0)),
            custom_metrics: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn set_uptime(&self, seconds: u64) {
        *self.uptime.lock().unwrap() = seconds;
    }

    fn set_memory(&self, mb: u32) {
        *self.memory_mb.lock().unwrap() = mb;
    }

    fn increment_events_processed(&self, count: u64) {
        *self.events_processed.lock().unwrap() += count;
    }

    fn add_custom_metric(&self, key: &str, value: serde_json::Value) {
        self.custom_metrics
            .lock()
            .unwrap()
            .insert(key.to_string(), value);
    }
}

impl MetricsProvider for MockMetricsProvider {
    fn get_metrics(&self) -> sinex_core_types::Result<serde_json::Value> {
        Ok(serde_json::json!({
            "uptime_seconds": *self.uptime.lock().unwrap(),
            "memory_mb": *self.memory_mb.lock().unwrap(),
            "events_processed": *self.events_processed.lock().unwrap(),
            "events_processed_last_minute": (*self.events_processed.lock().unwrap() % 60) as u32,
            "errors_last_hour": 0,
            "last_error_message": null
        }))
    }
}

// =============================================================================
// Process Heartbeat Emitter Tests
// =============================================================================

#[sinex_test]
async fn test_process_heartbeat_emitter_basic_functionality(ctx: TestContext) -> TestResult {
    let metrics_provider = MockMetricsProvider::new();
    metrics_provider.set_uptime(3600); // 1 hour
    metrics_provider.set_memory(256); // 256 MB
    metrics_provider.increment_events_processed(42);

    let emitter = ProcessHeartbeatEmitter::with_metrics_provider(
        ctx.pool().clone(),
        "test_process".to_string(),
        "1.0.0".to_string(),
        1, // 1 second interval for testing
        metrics_provider.clone(),
    );

    // Emit a single heartbeat
    emitter.emit_heartbeat().await?;

    // Verify heartbeat event was created
    let heartbeat_count: i64 = EventQueries::count_by_source_and_type_and_field(
        "sinex.process",
        "process.heartbeat",
        "process_name",
        "test_process",
    )
    .fetch_one(ctx.pool())
    .await?
    .unwrap_or(0);

    assert_eq!(heartbeat_count, 1, "Should have one heartbeat event");

    // Get the actual event to check payload
    let event_payload: serde_json::Value = EventQueries::get_payload_by_source_and_type_and_field(
        "sinex.process",
        "process.heartbeat",
        "process_name",
        "test_process",
    )
    .fetch_one(ctx.pool())
    .await?;

    assert_eq!(event_payload["process_name"], "test_process");
    assert_eq!(event_payload["version"], "1.0.0");
    assert_eq!(event_payload["health_status"], "healthy");
    assert_eq!(event_payload["uptime_seconds"], 3600);
    assert_eq!(event_payload["memory_mb"], 256);
    assert_eq!(event_payload["events_processed"], 42);

    Ok(())
}

#[sinex_test]
async fn test_process_lifecycle_events(ctx: TestContext) -> TestResult {
    let metrics_provider = MockMetricsProvider::new();

    let emitter = ProcessHeartbeatEmitter::with_metrics_provider(
        ctx.pool().clone(),
        "lifecycle_test".to_string(),
        "2.0.0".to_string(),
        5, // 5 second interval
        metrics_provider.clone(),
    );

    // Emit process started event
    emitter.emit_process_started().await?;

    // Update metrics over time
    metrics_provider.set_uptime(10);
    metrics_provider.increment_events_processed(15);

    // Emit heartbeat
    emitter.emit_heartbeat().await?;

    // Update metrics again
    metrics_provider.set_uptime(20);
    metrics_provider.increment_events_processed(25);

    // Emit another heartbeat
    emitter.emit_heartbeat().await?;

    // Emit process shutdown event
    emitter
        .emit_process_shutdown("Graceful shutdown requested")
        .await?;

    // Verify all events were created in correct order
    let all_events = EventQueries::get_by_source_and_field_ordered(
        "sinex.process",
        "process_name",
        "lifecycle_test",
        "ts_orig",
        "ASC",
    )
    .fetch_all(ctx.pool())
    .await?;

    assert_eq!(all_events.len(), 4, "Should have 4 lifecycle events");

    // Check process.started event
    assert_eq!(all_events[0].event_type, "process.started");
    let started_payload: serde_json::Value = all_events[0].payload.clone();
    assert_eq!(started_payload["process_name"], "lifecycle_test");
    assert_eq!(started_payload["version"], "2.0.0");

    // Check first heartbeat
    assert_eq!(all_events[1].event_type, "process.heartbeat");
    let heartbeat1_payload: serde_json::Value = all_events[1].payload.clone();
    assert_eq!(heartbeat1_payload["uptime_seconds"], 10);
    assert_eq!(heartbeat1_payload["events_processed"], 15);

    // Check second heartbeat
    assert_eq!(all_events[2].event_type, "process.heartbeat");
    let heartbeat2_payload: serde_json::Value = all_events[2].payload.clone();
    assert_eq!(heartbeat2_payload["uptime_seconds"], 20);
    assert_eq!(heartbeat2_payload["events_processed"], 40); // Cumulative

    // Check process.shutdown event
    assert_eq!(all_events[3].event_type, "process.shutdown");
    let shutdown_payload: serde_json::Value = all_events[3].payload.clone();
    assert_eq!(shutdown_payload["process_name"], "lifecycle_test");
    assert_eq!(shutdown_payload["reason"], "Graceful shutdown requested");

    Ok(())
}

#[sinex_test]
async fn test_process_heartbeat_with_custom_metrics(ctx: TestContext) -> TestResult {
    let metrics_provider = MockMetricsProvider::new();

    // Add custom metrics
    metrics_provider.add_custom_metric("queue_size", serde_json::json!(25));
    metrics_provider.add_custom_metric("active_connections", serde_json::json!(8));
    metrics_provider.add_custom_metric("cache_hit_rate", serde_json::json!(0.85));
    metrics_provider.add_custom_metric("last_error", serde_json::json!("Connection timeout"));

    let emitter = ProcessHeartbeatEmitter::with_metrics_provider(
        ctx.pool().clone(),
        "custom_metrics_test".to_string(),
        "1.5.0".to_string(),
        1,
        metrics_provider,
    );

    emitter.emit_heartbeat().await?;

    // Verify custom metrics are included
    let events = EventQueries::get_payloads_by_source_and_type_and_field(
        "sinex.process",
        "process.heartbeat",
        "process_name",
        "custom_metrics_test",
    )
    .fetch_all(ctx.pool())
    .await?;

    assert_eq!(events.len(), 1);

    let payload: serde_json::Value = events[0].payload.clone();
    assert_eq!(payload["queue_size"], 25);
    assert_eq!(payload["active_connections"], 8);
    assert_eq!(payload["cache_hit_rate"], 0.85);
    assert_eq!(payload["last_error"], "Connection timeout");

    Ok(())
}

#[sinex_test]
async fn test_process_heartbeat_continuous_emission(ctx: TestContext) -> TestResult {
    let metrics_provider = MockMetricsProvider::new();

    let emitter = ProcessHeartbeatEmitter::with_metrics_provider(
        ctx.pool().clone(),
        "continuous_test".to_string(),
        "1.0.0".to_string(),
        1, // 1 second interval for fast testing
        metrics_provider.clone(),
    );

    // Start continuous heartbeat emission in background
    let emitter_clone = emitter.clone();
    let heartbeat_handle = tokio::spawn(async move { emitter_clone.start_heartbeat_loop().await });

    // Let it run for a few seconds
    sleep(tokio::time::Duration::from_millis(3500)).await;

    // Update metrics during the run
    metrics_provider.set_uptime(100);
    metrics_provider.increment_events_processed(50);

    sleep(tokio::time::Duration::from_millis(1500)).await;

    // Stop the heartbeat loop
    emitter.stop_heartbeat_loop().await?;

    // Wait a bit for final operations
    sleep(tokio::time::Duration::from_millis(500)).await;

    heartbeat_handle.abort();

    // Verify multiple heartbeat events were emitted
    let heartbeat_count = EventQueries::count_by_source_and_type_and_field(
        "sinex.process",
        "process.heartbeat",
        "process_name",
        "continuous_test",
    )
    .fetch_one(ctx.pool())
    .await?
    .unwrap_or(0);

    assert!(
        heartbeat_count >= 3,
        "Should have at least 3 heartbeat events, got {}",
        heartbeat_count
    );
    assert!(
        heartbeat_count <= 8,
        "Should not have excessive heartbeats, got {}",
        heartbeat_count
    );

    // Verify latest heartbeat has updated metrics
    let latest_event = EventQueries::get_latest_payload_by_source_and_type_and_field(
        "sinex.process",
        "process.heartbeat",
        "process_name",
        "continuous_test",
    )
    .fetch_one(ctx.pool())
    .await?;

    let payload: serde_json::Value = latest_event.payload;
    assert_eq!(payload["uptime_seconds"], 100);
    assert!(payload["events_processed"].as_u64().unwrap() >= 50);

    Ok(())
}

// =============================================================================
// Health Aggregator Integration Tests
// =============================================================================

#[sinex_test]
async fn test_health_aggregator_process_discovery(ctx: TestContext) -> TestResult {
    // Create multiple processes with different states
    let processes = vec![
        ("web_server", "healthy", 3600, 512, 1000),
        ("worker_1", "healthy", 1800, 256, 500),
        ("worker_2", "degraded", 900, 128, 250),
        ("monitor", "healthy", 7200, 64, 100),
    ];

    for (name, status, uptime, memory, events) in processes {
        let metrics_provider = MockMetricsProvider::new();
        metrics_provider.set_uptime(uptime);
        metrics_provider.set_memory(memory);
        metrics_provider.increment_events_processed(events);

        // Add health status as custom metric
        metrics_provider.add_custom_metric("health_status", serde_json::json!(status));

        let emitter = ProcessHeartbeatEmitter::with_metrics_provider(
            ctx.pool().clone(),
            name.to_string(),
            "1.0.0".to_string(),
            60, // 1 minute interval
            metrics_provider,
        );

        emitter.emit_process_started().await?;
        emitter.emit_heartbeat().await?;
    }

    // Simulate health aggregator query (from the updated health aggregator)
    let health_data = sqlx::query!(
        r#"
        SELECT DISTINCT ON (payload->>'process_name')
            payload->>'process_name' as component_name,
            ts_orig as timestamp,
            payload->>'health_status' as status,
            (payload->>'uptime_seconds')::bigint as uptime_seconds,
            (payload->>'memory_mb')::integer as memory_usage_mb,
            (payload->>'events_processed')::integer as events_processed_last_minute,
            payload->>'version' as binary_version
        FROM core.events
        WHERE source = 'sinex.process'
          AND event_type = 'process.heartbeat'
        ORDER BY payload->>'process_name', ts_orig DESC
        "#
    )
    .fetch_all(ctx.pool())
    .await?;

    assert_eq!(health_data.len(), 4, "Should find all 4 processes");

    // Verify each process has correct data
    let mut found_processes = std::collections::HashMap::new();
    for row in health_data {
        let name = row.component_name.unwrap();
        found_processes.insert(
            name.clone(),
            (
                row.status.unwrap_or_else(|| "unknown".to_string()),
                row.uptime_seconds.unwrap_or(0),
                row.memory_usage_mb.unwrap_or(0),
                row.events_processed_last_minute.unwrap_or(0),
            ),
        );
    }

    assert_eq!(
        found_processes["web_server"],
        ("healthy".to_string(), 3600, 512, 1000)
    );
    assert_eq!(
        found_processes["worker_1"],
        ("healthy".to_string(), 1800, 256, 500)
    );
    assert_eq!(
        found_processes["worker_2"],
        ("degraded".to_string(), 900, 128, 250)
    );
    assert_eq!(
        found_processes["monitor"],
        ("healthy".to_string(), 7200, 64, 100)
    );

    Ok(())
}

#[sinex_test]
async fn test_process_failure_detection(ctx: TestContext) -> TestResult {
    let metrics_provider = MockMetricsProvider::new();

    let emitter = ProcessHeartbeatEmitter::with_metrics_provider(
        ctx.pool().clone(),
        "failing_process".to_string(),
        "1.0.0".to_string(),
        5,
        metrics_provider.clone(),
    );

    // Start normally
    emitter.emit_process_started().await?;
    emitter.emit_heartbeat().await?;

    // Simulate degraded state
    metrics_provider.add_custom_metric("health_status", serde_json::json!("degraded"));
    metrics_provider.add_custom_metric("error_count", serde_json::json!(5));
    emitter.emit_heartbeat().await?;

    // Simulate critical state
    metrics_provider.add_custom_metric("health_status", serde_json::json!("critical"));
    metrics_provider.add_custom_metric("error_count", serde_json::json!(15));
    metrics_provider.add_custom_metric(
        "last_error",
        serde_json::json!("Database connection failed"),
    );
    emitter.emit_heartbeat().await?;

    // Simulate shutdown due to errors
    emitter
        .emit_process_shutdown("Process terminated due to critical errors")
        .await?;

    // Verify the failure progression is recorded
    let events = EventQueries::get_by_source_and_field_ordered(
        "sinex.process",
        "process_name",
        "failing_process",
        "ts_orig",
        "ASC",
    )
    .fetch_all(ctx.pool())
    .await?;

    assert_eq!(events.len(), 5); // started + 3 heartbeats + shutdown

    // Check progression through health states
    let heartbeat_events: Vec<_> = events
        .iter()
        .filter(|e| e.event_type == "process.heartbeat")
        .collect();

    assert_eq!(heartbeat_events.len(), 3);

    let payload1: serde_json::Value = heartbeat_events[0].payload.clone();
    let payload2: serde_json::Value = heartbeat_events[1].payload.clone();
    let payload3: serde_json::Value = heartbeat_events[2].payload.clone();

    // First heartbeat should be healthy (default)
    assert_eq!(
        payload1
            .get("health_status")
            .unwrap_or(&serde_json::json!("healthy")),
        "healthy"
    );

    // Second heartbeat should be degraded
    assert_eq!(payload2["health_status"], "degraded");
    assert_eq!(payload2["error_count"], 5);

    // Third heartbeat should be critical
    assert_eq!(payload3["health_status"], "critical");
    assert_eq!(payload3["error_count"], 15);
    assert_eq!(payload3["last_error"], "Database connection failed");

    // Shutdown event should contain error reason
    let shutdown_event = events
        .iter()
        .find(|e| e.event_type == "process.shutdown")
        .unwrap();
    let shutdown_payload: serde_json::Value = shutdown_event.payload.clone();
    assert_eq!(
        shutdown_payload["reason"],
        "Process terminated due to critical errors"
    );

    Ok(())
}

#[sinex_test]
async fn test_process_restart_detection(ctx: TestContext) -> TestResult {
    let process_name = "restartable_process";

    // First process instance
    let metrics1 = MockMetricsProvider::new();
    let emitter1 = ProcessHeartbeatEmitter::with_metrics_provider(
        ctx.pool().clone(),
        process_name.to_string(),
        "1.0.0".to_string(),
        1,
        metrics1.clone(),
    );

    emitter1.emit_process_started().await?;
    metrics1.set_uptime(10);
    emitter1.emit_heartbeat().await?;
    emitter1.emit_process_shutdown("Planned restart").await?;

    // Small delay to ensure different timestamps
    sleep(tokio::time::Duration::from_millis(100)).await;

    // Second process instance (restart)
    let metrics2 = MockMetricsProvider::new();
    let emitter2 = ProcessHeartbeatEmitter::with_metrics_provider(
        ctx.pool().clone(),
        process_name.to_string(),
        "1.0.1".to_string(), // New version
        1,
        metrics2.clone(),
    );

    emitter2.emit_process_started().await?;
    metrics2.set_uptime(5); // Fresh start
    emitter2.emit_heartbeat().await?;

    // Verify restart is detectable by analyzing events
    let all_events = EventQueries::get_by_source_and_field_ordered(
        "sinex.process",
        "process_name",
        process_name,
        "ts_orig",
        "ASC",
    )
    .fetch_all(ctx.pool())
    .await?;

    assert_eq!(all_events.len(), 5); // start1, heartbeat1, shutdown1, start2, heartbeat2

    // Find the two start events
    let start_events: Vec<_> = all_events
        .iter()
        .filter(|e| e.event_type == "process.started")
        .collect();

    assert_eq!(
        start_events.len(),
        2,
        "Should have two process.started events"
    );

    let start1_payload: serde_json::Value = start_events[0].payload.clone();
    let start2_payload: serde_json::Value = start_events[1].payload.clone();

    assert_eq!(start1_payload["version"], "1.0.0");
    assert_eq!(start2_payload["version"], "1.0.1");

    // Verify restart is detectable by uptime reset
    let heartbeat_events: Vec<_> = all_events
        .iter()
        .filter(|e| e.event_type == "process.heartbeat")
        .collect();

    let heartbeat1_payload: serde_json::Value = heartbeat_events[0].payload.clone();
    let heartbeat2_payload: serde_json::Value = heartbeat_events[1].payload.clone();

    assert_eq!(heartbeat1_payload["uptime_seconds"], 10);
    assert_eq!(heartbeat2_payload["uptime_seconds"], 5); // Reset after restart

    Ok(())
}

// =============================================================================
// Performance and Stress Tests
// =============================================================================

#[sinex_test]
async fn test_high_frequency_heartbeats(ctx: TestContext) -> TestResult {
    let metrics_provider = MockMetricsProvider::new();

    let emitter = ProcessHeartbeatEmitter::with_metrics_provider(
        ctx.pool().clone(),
        "high_freq_test".to_string(),
        "1.0.0".to_string(),
        1, // Very frequent for testing
        metrics_provider.clone(),
    );

    // Emit many heartbeats rapidly
    let start_time = std::time::Instant::now();
    for i in 0..20 {
        metrics_provider.set_uptime(i * 5);
        metrics_provider.increment_events_processed(10);
        emitter.emit_heartbeat().await?;

        // Small delay to avoid overwhelming the database
        sleep(tokio::time::Duration::from_millis(10)).await;
    }
    let duration = start_time.elapsed();

    // Verify all heartbeats were recorded
    let count = EventQueries::count_by_source_and_type_and_field(
        "sinex.process",
        "process.heartbeat",
        "process_name",
        "high_freq_test",
    )
    .fetch_one(ctx.pool())
    .await?
    .unwrap_or(0);

    assert_eq!(count, 20, "Should have recorded all 20 heartbeats");

    // Verify performance (should complete quickly)
    assert!(
        duration.as_millis() < 5000,
        "High frequency heartbeats should complete within 5 seconds"
    );

    // Verify chronological ordering
    let timestamps = EventQueries::get_timestamps_by_source_and_type_and_field(
        "sinex.process",
        "process.heartbeat",
        "process_name",
        "high_freq_test",
        "ts_orig",
        "ASC",
    )
    .fetch_all(ctx.pool())
    .await?;

    // Verify timestamps are in order
    for i in 1..timestamps.len() {
        assert!(
            timestamps[i].ts_orig >= timestamps[i - 1].ts_orig,
            "Heartbeats should be chronologically ordered"
        );
    }

    Ok(())
}

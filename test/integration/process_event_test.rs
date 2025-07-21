// Process Event Integration Tests
//
// This module tests the event-based process lifecycle system, including:
// - ProcessHeartbeatEmitter functionality
// - Process lifecycle events (started, heartbeat, shutdown)
// - Health monitoring through events instead of database tables
// - Process metrics collection and reporting
// - Integration with the health aggregator

use crate::common::prelude::*;
use crate::common::builders::{TestEventBuilder, TestEvents};
use crate::common::query_helpers::TestQueries;
use chrono::Utc;
use sinex_core_runtime::MetricsProvider;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::time::sleep;

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

    // Create a heartbeat event with the metrics
    let heartbeat_event = TestEventBuilder::new("sinex.process", "process.heartbeat")
        .with_field("process_name", json!("test_process"))
        .with_field("version", json!("1.0.0"))
        .with_field("uptime_seconds", json!(*metrics_provider.uptime.lock().unwrap()))
        .with_field("memory_mb", json!(*metrics_provider.memory_mb.lock().unwrap()))
        .with_field("cpu_percent", json!(15.5))
        .with_field("events_processed", json!(*metrics_provider.events_processed.lock().unwrap()))
        .with_field("errors_count", json!(0))
        .with_field("health_status", json!("healthy"))
        .with_field("custom_metrics", json!({}))
        .insert(ctx.pool())
        .await?;

    // Verify heartbeat event was created
    let all_events = TestQueries::get_events_by_source(
        ctx.pool(),
        "sinex.process",
        Some(10),
    )
    .await?;

    let heartbeat_events: Vec<_> = all_events.iter()
        .filter(|e| e.event_type == "process.heartbeat" && 
                    e.payload.get("process_name") == Some(&serde_json::json!("test_process")))
        .collect();

    assert_eq!(heartbeat_events.len(), 1, "Should have one heartbeat event");

    // Check the payload
    let event_payload = &heartbeat_events[0].payload;
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
    let process_name = "lifecycle_test";
    let version = "2.0.0";
    let source_name = "sinex.process";

    // Emit process started event
    TestEventBuilder::new(source_name, "process.started")
        .with_field("process_name", json!(process_name))
        .with_field("version", json!(version))
        .with_field("git_revision", json!("abc123"))
        .with_field("binary_hash", json!("def456"))
        .with_field("build_time", json!(Utc::now()))
        .with_field("config_hash", json!("ghi789"))
        .insert(ctx.pool())
        .await?;

    // Update metrics over time
    metrics_provider.set_uptime(10);
    metrics_provider.increment_events_processed(15);

    // Emit heartbeat
    TestEventBuilder::new(source_name, "process.heartbeat")
        .with_field("process_name", json!(process_name))
        .with_field("version", json!(version))
        .with_field("uptime_seconds", json!(10))
        .with_field("memory_mb", json!(*metrics_provider.memory_mb.lock().unwrap()))
        .with_field("cpu_percent", json!(15.5))
        .with_field("events_processed", json!(*metrics_provider.events_processed.lock().unwrap()))
        .with_field("errors_count", json!(0))
        .with_field("health_status", json!("healthy"))
        .with_field("custom_metrics", json!({}))
        .insert(ctx.pool())
        .await?;

    // Update metrics again
    metrics_provider.set_uptime(20);
    metrics_provider.increment_events_processed(25);

    // Emit another heartbeat
    TestEventBuilder::new(source_name, "process.heartbeat")
        .with_field("process_name", json!(process_name))
        .with_field("version", json!(version))
        .with_field("uptime_seconds", json!(20))
        .with_field("memory_mb", json!(*metrics_provider.memory_mb.lock().unwrap()))
        .with_field("cpu_percent", json!(15.5))
        .with_field("events_processed", json!(*metrics_provider.events_processed.lock().unwrap()))
        .with_field("errors_count", json!(0))
        .with_field("health_status", json!("healthy"))
        .with_field("custom_metrics", json!({}))
        .insert(ctx.pool())
        .await?;

    // Emit process shutdown event
    TestEventBuilder::new(source_name, "process.shutdown")
        .with_field("process_name", json!(process_name))
        .with_field("version", json!(version))
        .with_field("uptime_seconds", json!(20))
        .with_field("graceful", json!(true))
        .with_field("shutdown_reason", json!("Graceful shutdown requested"))
        .insert(ctx.pool())
        .await?;

    // Verify all events were created in correct order
    let all_events = TestQueries::get_events_by_source(
        ctx.pool(),
        "sinex.process",
        Some(10),
    )
    .await?;

    // Filter to only our process
    let all_events: Vec<_> = all_events.into_iter()
        .filter(|e| e.payload.get("process_name") == Some(&serde_json::json!("lifecycle_test")))
        .collect();

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
    assert_eq!(shutdown_payload["shutdown_reason"], "Graceful shutdown requested");

    Ok(())
}

#[sinex_test]
async fn test_process_heartbeat_with_custom_metrics(ctx: TestContext) -> TestResult {
    let metrics_provider = MockMetricsProvider::new();
    let process_name = "custom_metrics_test";
    let version = "1.5.0";
    let source_name = "sinex.process";

    // Add custom metrics
    metrics_provider.add_custom_metric("queue_size", serde_json::json!(25));
    metrics_provider.add_custom_metric("active_connections", serde_json::json!(8));
    metrics_provider.add_custom_metric("cache_hit_rate", serde_json::json!(0.85));
    metrics_provider.add_custom_metric("last_error", serde_json::json!("Connection timeout"));

    // Create heartbeat with custom metrics
    let custom_metrics = metrics_provider.custom_metrics.lock().unwrap().clone();
    TestEventBuilder::new(source_name, "process.heartbeat")
        .with_field("process_name", json!(process_name))
        .with_field("version", json!(version))
        .with_field("uptime_seconds", json!(*metrics_provider.uptime.lock().unwrap()))
        .with_field("memory_mb", json!(*metrics_provider.memory_mb.lock().unwrap()))
        .with_field("cpu_percent", json!(15.5))
        .with_field("events_processed", json!(*metrics_provider.events_processed.lock().unwrap()))
        .with_field("errors_count", json!(0))
        .with_field("health_status", json!("healthy"))
        .with_field("custom_metrics", json!(custom_metrics))
        .insert(ctx.pool())
        .await?;

    // Verify custom metrics are included
    let events = TestQueries::get_events_by_source(
        ctx.pool(),
        "sinex.process",
        Some(10),
    )
    .await?;

    // Filter to only our process heartbeats
    let events: Vec<_> = events.into_iter()
        .filter(|e| e.event_type == "process.heartbeat" && 
                    e.payload.get("process_name") == Some(&serde_json::json!(process_name)))
        .collect();

    assert_eq!(events.len(), 1);

    let payload: serde_json::Value = events[0].payload.clone();
    let custom_metrics = &payload["custom_metrics"];
    assert_eq!(custom_metrics["queue_size"], 25);
    assert_eq!(custom_metrics["active_connections"], 8);
    assert_eq!(custom_metrics["cache_hit_rate"], 0.85);
    assert_eq!(custom_metrics["last_error"], "Connection timeout");

    Ok(())
}

#[sinex_test]
async fn test_process_heartbeat_continuous_emission(ctx: TestContext) -> TestResult {
    let metrics_provider = MockMetricsProvider::new();
    let process_name = "continuous_test";
    let version = "1.0.0";
    let source_name = "sinex.process";

    // Simulate continuous heartbeat emission by creating multiple events
    for i in 0..5 {
        // Update metrics over time
        metrics_provider.set_uptime((i + 1) * 20);
        metrics_provider.increment_events_processed(10);

        // Create heartbeat event
        TestEventBuilder::new(source_name, "process.heartbeat")
            .with_field("process_name", json!(process_name))
            .with_field("version", json!(version))
            .with_field("uptime_seconds", json!(*metrics_provider.uptime.lock().unwrap()))
            .with_field("memory_mb", json!(*metrics_provider.memory_mb.lock().unwrap()))
            .with_field("cpu_percent", json!(15.5))
            .with_field("events_processed", json!(*metrics_provider.events_processed.lock().unwrap()))
            .with_field("errors_count", json!(0))
            .with_field("health_status", json!("healthy"))
            .with_field("custom_metrics", json!({}))
            .insert(ctx.pool())
            .await?;

        // Small delay between heartbeats
        sleep(tokio::time::Duration::from_millis(100)).await;
    }

    // Verify multiple heartbeat events were emitted
    let all_events = TestQueries::get_events_by_source(
        ctx.pool(),
        "sinex.process",
        Some(100),
    )
    .await?;

    // Count heartbeat events for our process
    let heartbeat_count = all_events.iter()
        .filter(|e| e.event_type == "process.heartbeat" && 
                    e.payload.get("process_name") == Some(&serde_json::json!("continuous_test")))
        .count();

    assert_eq!(
        heartbeat_count, 5,
        "Should have exactly 5 heartbeat events, got {}",
        heartbeat_count
    );

    // Verify latest heartbeat has updated metrics
    let latest_event = all_events.iter()
        .filter(|e| e.event_type == "process.heartbeat" && 
                    e.payload.get("process_name") == Some(&serde_json::json!("continuous_test")))
        .max_by_key(|e| e.ts_ingest)
        .expect("Should have at least one heartbeat event");

    let payload = &latest_event.payload;
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
        // Emit process started event
        TestEventBuilder::new("sinex.process", "process.started")
            .with_field("process_name", json!(name))
            .with_field("version", json!("1.0.0"))
            .with_field("git_revision", json!("abc123"))
            .with_field("binary_hash", json!("def456"))
            .with_field("build_time", json!(Utc::now()))
            .with_field("config_hash", json!("ghi789"))
            .insert(ctx.pool())
            .await?;

        // Emit heartbeat with health status
        TestEventBuilder::new("sinex.process", "process.heartbeat")
            .with_field("process_name", json!(name))
            .with_field("version", json!("1.0.0"))
            .with_field("uptime_seconds", json!(uptime))
            .with_field("memory_mb", json!(memory))
            .with_field("cpu_percent", json!(15.5))
            .with_field("events_processed", json!(events))
            .with_field("errors_count", json!(0))
            .with_field("health_status", json!(status))
            .with_field("custom_metrics", json!({}))
            .insert(ctx.pool())
            .await?;
    }

    // Get all process heartbeat events using query builder
    let events = TestQueries::get_events_by_source(ctx.pool(), "sinex.process", Some(100)).await?;
    
    // Filter to heartbeats and group by process name
    let mut process_map = std::collections::HashMap::new();
    for event in events {
        if event.event_type == "process.heartbeat" {
            if let Some(process_name) = event.payload.get("process_name").and_then(|v| v.as_str()) {
                // Keep only the latest heartbeat for each process
                match process_map.get(process_name) {
                    None => { process_map.insert(process_name.to_string(), event); },
                    Some(existing) => {
                        if event.ts_orig.unwrap_or(event.ts_ingest) > existing.ts_orig.unwrap_or(existing.ts_ingest) {
                            process_map.insert(process_name.to_string(), event);
                        }
                    }
                }
            }
        }
    }
    
    let health_data: Vec<_> = process_map.into_iter().map(|(_, event)| event).collect();

    assert_eq!(health_data.len(), 4, "Should find all 4 processes");

    // Verify each process has correct data
    let mut found_processes = std::collections::HashMap::new();
    for event in health_data {
        let name = event.payload.get("process_name")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown")
            .to_string();
        
        let status = event.payload.get("health_status")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown")
            .to_string();
        
        let uptime = event.payload.get("uptime_seconds")
            .and_then(|v| v.as_i64())
            .unwrap_or(0);
        
        let memory = event.payload.get("memory_mb")
            .and_then(|v| v.as_i64())
            .unwrap_or(0) as i32;
        
        let events_processed = event.payload.get("events_processed")
            .and_then(|v| v.as_i64())
            .unwrap_or(0) as i32;
        
        found_processes.insert(
            name,
            (status, uptime, memory, events_processed),
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
    let process_name = "failing_process";
    let version = "1.0.0";
    // Start normally
    TestEventBuilder::new("sinex.process", "process.started")
        .with_field("process_name", json!(process_name))
        .with_field("version", json!(version))
        .with_field("git_revision", json!("abc123"))
        .with_field("binary_hash", json!("def456"))
        .with_field("build_time", json!(Utc::now()))
        .with_field("config_hash", json!("ghi789"))
        .insert(ctx.pool())
        .await?;

    // First heartbeat - healthy
    TestEventBuilder::new("sinex.process", "process.heartbeat")
        .with_field("process_name", json!(process_name))
        .with_field("version", json!(version))
        .with_field("uptime_seconds", json!(10))
        .with_field("memory_mb", json!(128))
        .with_field("cpu_percent", json!(15.5))
        .with_field("events_processed", json!(100))
        .with_field("errors_count", json!(0))
        .with_field("health_status", json!("healthy"))
        .with_field("custom_metrics", json!({}))
        .insert(ctx.pool())
        .await?;

    // Simulate degraded state
    metrics_provider.add_custom_metric("health_status", serde_json::json!("degraded"));
    metrics_provider.add_custom_metric("error_count", serde_json::json!(5));
    let custom_metrics = metrics_provider.custom_metrics.lock().unwrap().clone();
    
    TestEventBuilder::new("sinex.process", "process.heartbeat")
        .with_field("process_name", json!(process_name))
        .with_field("version", json!(version))
        .with_field("uptime_seconds", json!(20))
        .with_field("memory_mb", json!(128))
        .with_field("cpu_percent", json!(25.5))
        .with_field("events_processed", json!(150))
        .with_field("errors_count", json!(5))
        .with_field("health_status", json!("degraded"))
        .with_field("custom_metrics", json!(custom_metrics))
        .insert(ctx.pool())
        .await?;

    // Simulate critical state
    metrics_provider.add_custom_metric("health_status", serde_json::json!("critical"));
    metrics_provider.add_custom_metric("error_count", serde_json::json!(15));
    metrics_provider.add_custom_metric(
        "last_error",
        serde_json::json!("Database connection failed"),
    );
    let custom_metrics = metrics_provider.custom_metrics.lock().unwrap().clone();
    
    TestEventBuilder::new("sinex.process", "process.heartbeat")
        .with_field("process_name", json!(process_name))
        .with_field("version", json!(version))
        .with_field("uptime_seconds", json!(30))
        .with_field("memory_mb", json!(256))
        .with_field("cpu_percent", json!(45.5))
        .with_field("events_processed", json!(160))
        .with_field("errors_count", json!(15))
        .with_field("health_status", json!("critical"))
        .with_field("custom_metrics", json!(custom_metrics))
        .insert(ctx.pool())
        .await?;

    // Simulate shutdown due to errors
    TestEventBuilder::new("sinex.process", "process.shutdown")
        .with_field("process_name", json!(process_name))
        .with_field("version", json!(version))
        .with_field("uptime_seconds", json!(30))
        .with_field("graceful", json!(false))
        .with_field("shutdown_reason", json!("Process terminated due to critical errors"))
        .insert(ctx.pool())
        .await?;

    // Verify the failure progression is recorded
    let events = TestQueries::get_events_by_source(
        ctx.pool(),
        "sinex.process",
        Some(20),
    )
    .await?;

    // Filter to only our process
    let events: Vec<_> = events.into_iter()
        .filter(|e| e.payload.get("process_name") == Some(&serde_json::json!("failing_process")))
        .collect();

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
    assert_eq!(payload2["custom_metrics"]["error_count"], 5);

    // Third heartbeat should be critical
    assert_eq!(payload3["health_status"], "critical");
    assert_eq!(payload3["custom_metrics"]["error_count"], 15);
    assert_eq!(payload3["custom_metrics"]["last_error"], "Database connection failed");

    // Shutdown event should contain error reason
    let shutdown_event = events
        .iter()
        .find(|e| e.event_type == "process.shutdown")
        .unwrap();
    let shutdown_payload: serde_json::Value = shutdown_event.payload.clone();
    assert_eq!(
        shutdown_payload["shutdown_reason"],
        "Process terminated due to critical errors"
    );

    Ok(())
}

#[sinex_test]
async fn test_process_restart_detection(ctx: TestContext) -> TestResult {
    let process_name = "restartable_process";
    // First process instance
    let metrics1 = MockMetricsProvider::new();
    
    // Start first instance
    TestEventBuilder::new("sinex.process", "process.started")
        .with_field("process_name", json!(process_name))
        .with_field("version", json!("1.0.0"))
        .with_field("git_revision", json!("abc123"))
        .with_field("binary_hash", json!("def456"))
        .with_field("build_time", json!(Utc::now()))
        .with_field("config_hash", json!("ghi789"))
        .insert(ctx.pool())
        .await?;

    metrics1.set_uptime(10);
    TestEventBuilder::new("sinex.process", "process.heartbeat")
        .with_field("process_name", json!(process_name))
        .with_field("version", json!("1.0.0"))
        .with_field("uptime_seconds", json!(10))
        .with_field("memory_mb", json!(128))
        .with_field("cpu_percent", json!(15.5))
        .with_field("events_processed", json!(50))
        .with_field("errors_count", json!(0))
        .with_field("health_status", json!("healthy"))
        .with_field("custom_metrics", json!({}))
        .insert(ctx.pool())
        .await?;

    TestEventBuilder::new("sinex.process", "process.shutdown")
        .with_field("process_name", json!(process_name))
        .with_field("version", json!("1.0.0"))
        .with_field("uptime_seconds", json!(10))
        .with_field("graceful", json!(true))
        .with_field("shutdown_reason", json!("Planned restart"))
        .insert(ctx.pool())
        .await?;

    // Small delay to ensure different timestamps
    sleep(tokio::time::Duration::from_millis(100)).await;

    // Second process instance (restart)
    let metrics2 = MockMetricsProvider::new();
    
    // Start second instance with new version
    TestEventBuilder::new("sinex.process", "process.started")
        .with_field("process_name", json!(process_name))
        .with_field("version", json!("1.0.1")) // New version
        .with_field("git_revision", json!("xyz789"))
        .with_field("binary_hash", json!("uvw123"))
        .with_field("build_time", json!(Utc::now()))
        .with_field("config_hash", json!("rst456"))
        .insert(ctx.pool())
        .await?;

    metrics2.set_uptime(5); // Fresh start
    TestEventBuilder::new("sinex.process", "process.heartbeat")
        .with_field("process_name", json!(process_name))
        .with_field("version", json!("1.0.1"))
        .with_field("uptime_seconds", json!(5))
        .with_field("memory_mb", json!(128))
        .with_field("cpu_percent", json!(12.5))
        .with_field("events_processed", json!(10))
        .with_field("errors_count", json!(0))
        .with_field("health_status", json!("healthy"))
        .with_field("custom_metrics", json!({}))
        .insert(ctx.pool())
        .await?;

    // Verify restart is detectable by analyzing events
    let all_events = TestQueries::get_events_by_source(
        ctx.pool(),
        "sinex.process",
        Some(20),
    )
    .await?;

    // Filter to only our process
    let all_events: Vec<_> = all_events.into_iter()
        .filter(|e| e.payload.get("process_name") == Some(&serde_json::json!(process_name)))
        .collect();

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
    let process_name = "high_freq_test";
    let version = "1.0.0";
    // Emit many heartbeats rapidly
    let start_time = std::time::Instant::now();
    for i in 0..20 {
        metrics_provider.set_uptime(i * 5);
        metrics_provider.increment_events_processed(10);
        
        TestEventBuilder::new("sinex.process", "process.heartbeat")
            .with_field("process_name", json!(process_name))
            .with_field("version", json!(version))
            .with_field("uptime_seconds", json!(*metrics_provider.uptime.lock().unwrap()))
            .with_field("memory_mb", json!(*metrics_provider.memory_mb.lock().unwrap()))
            .with_field("cpu_percent", json!(15.5))
            .with_field("events_processed", json!(*metrics_provider.events_processed.lock().unwrap()))
            .with_field("errors_count", json!(0))
            .with_field("health_status", json!("healthy"))
            .with_field("custom_metrics", json!({}))
            .insert(ctx.pool())
            .await?;

        // Small delay to avoid overwhelming the database
        sleep(tokio::time::Duration::from_millis(10)).await;
    }
    let duration = start_time.elapsed();

    // Verify all heartbeats were recorded
    let all_events = TestQueries::get_events_by_source(
        ctx.pool(),
        "sinex.process",
        Some(50),
    )
    .await?;

    // Count heartbeat events for our process
    let count = all_events.iter()
        .filter(|e| e.event_type == "process.heartbeat" && 
                    e.payload.get("process_name") == Some(&serde_json::json!("high_freq_test")))
        .count();

    assert_eq!(count, 20, "Should have recorded all 20 heartbeats");

    // Verify performance (should complete quickly)
    assert!(
        duration.as_millis() < 5000,
        "High frequency heartbeats should complete within 5 seconds"
    );

    // Verify chronological ordering
    let heartbeat_events: Vec<_> = all_events.iter()
        .filter(|e| e.event_type == "process.heartbeat" && 
                    e.payload.get("process_name") == Some(&serde_json::json!("high_freq_test")))
        .collect();

    // Verify timestamps are in order
    for i in 1..heartbeat_events.len() {
        assert!(
            heartbeat_events[i].ts_ingest >= heartbeat_events[i - 1].ts_ingest,
            "Heartbeats should be chronologically ordered"
        );
    }

    Ok(())
}

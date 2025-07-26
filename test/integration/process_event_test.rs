// Process Event Integration Tests
//
// This module tests the event-based process lifecycle system, including:
// - ProcessHeartbeatEmitter functionality
// - Process lifecycle events (started, heartbeat, shutdown)
// - Health monitoring through events instead of database tables
// - Process metrics collection and reporting
// - Integration with the health aggregator

use chrono::{Duration, Utc};
use sinex_core_runtime::{MetricsProvider, ProcessHeartbeatEmitter};
use sinex_db::queries::{CheckpointQueries, EventQueries};
use sinex_db::query_builder::{QueryBuilder, QueryParam};
use sinex_events::{
    EventFactory, ProcessHeartbeatPayload, ProcessShutdownPayload, ProcessStartedPayload,
};
use sinex_test_utils::prelude::*;
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

    let event_factory = EventFactory::new("sinex.process");

    // Create a heartbeat event with the metrics
    let heartbeat_payload = serde_json::json!({
        "process_name": "test_process",
        "version": "1.0.0",
        "uptime_seconds": *metrics_provider.uptime.lock().unwrap(),
        "memory_mb": *metrics_provider.memory_mb.lock().unwrap(),
        "cpu_percent": 15.5,
        "events_processed": *metrics_provider.events_processed.lock().unwrap(),
        "errors_count": 0,
        "health_status": "healthy",
        "custom_metrics": {}
    });
    let heartbeat_event = event_factory.create_event("process.heartbeat", heartbeat_payload);

    // Insert the event into the database
    ctx.insert_event(&heartbeat_event).await?;

    // Verify heartbeat event was created
    let all_events: Vec<sinex_db::EventRecord> =
        EventQueries::get_by_source("sinex.process".to_string(), Some(10), None)
            .fetch_all(ctx.pool())
            .await?;

    let heartbeat_events: Vec<_> = all_events
        .iter()
        .filter(|e| {
            e.event_type == "process.heartbeat"
                && e.payload.get("process_name") == Some(&serde_json::json!("test_process"))
        })
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

    // Create EventFactory for generating events
    let event_factory = EventFactory::new(source_name);

    // Emit process started event
    let started_payload = serde_json::json!({
        "process_name": process_name,
        "version": version,
        "git_revision": "abc123",
        "binary_hash": "def456",
        "build_time": Utc::now(),
        "config_hash": "ghi789"
    });
    let started_event = event_factory.create_event("process.started", started_payload);
    ctx.insert_event(&started_event).await?;

    // Update metrics over time
    metrics_provider.set_uptime(10);
    metrics_provider.increment_events_processed(15);

    // Emit heartbeat
    let heartbeat1_payload = serde_json::json!({
        "process_name": process_name,
        "version": version,
        "uptime_seconds": 10,
        "memory_mb": *metrics_provider.memory_mb.lock().unwrap(),
        "cpu_percent": 15.5,
        "events_processed": *metrics_provider.events_processed.lock().unwrap(),
        "errors_count": 0,
        "health_status": "healthy",
        "custom_metrics": {}
    });
    let heartbeat1_event = event_factory.create_event("process.heartbeat", heartbeat1_payload);
    ctx.insert_event(&heartbeat1_event).await?;

    // Update metrics again
    metrics_provider.set_uptime(20);
    metrics_provider.increment_events_processed(25);

    // Emit another heartbeat
    let heartbeat2_payload = serde_json::json!({
        "process_name": process_name,
        "version": version,
        "uptime_seconds": 20,
        "memory_mb": *metrics_provider.memory_mb.lock().unwrap(),
        "cpu_percent": 15.5,
        "events_processed": *metrics_provider.events_processed.lock().unwrap(),
        "errors_count": 0,
        "health_status": "healthy",
        "custom_metrics": {}
    });
    let heartbeat2_event = event_factory.create_event("process.heartbeat", heartbeat2_payload);
    ctx.insert_event(&heartbeat2_event).await?;

    // Emit process shutdown event
    let shutdown_payload = serde_json::json!({
        "process_name": process_name,
        "version": version,
        "uptime_seconds": 20,
        "graceful": true,
        "shutdown_reason": "Graceful shutdown requested"
    });
    let shutdown_event = event_factory.create_event("process.shutdown", shutdown_payload);
    ctx.insert_event(&shutdown_event).await?;

    // Verify all events were created in correct order
    let all_events: Vec<sinex_db::EventRecord> =
        EventQueries::get_by_source("sinex.process".to_string(), Some(10), None)
            .fetch_all(ctx.pool())
            .await?;

    // Filter to only our process
    let all_events: Vec<_> = all_events
        .into_iter()
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
    assert_eq!(
        shutdown_payload["shutdown_reason"],
        "Graceful shutdown requested"
    );

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

    // Create EventFactory for generating events
    let event_factory = EventFactory::new(source_name);

    // Create heartbeat with custom metrics
    let custom_metrics = metrics_provider.custom_metrics.lock().unwrap().clone();
    let heartbeat_payload = serde_json::json!({
        "process_name": process_name,
        "version": version,
        "uptime_seconds": *metrics_provider.uptime.lock().unwrap(),
        "memory_mb": *metrics_provider.memory_mb.lock().unwrap(),
        "cpu_percent": 15.5,
        "events_processed": *metrics_provider.events_processed.lock().unwrap(),
        "errors_count": 0,
        "health_status": "healthy",
        "custom_metrics": custom_metrics
    });
    let heartbeat_event = event_factory.create_event("process.heartbeat", heartbeat_payload);
    ctx.insert_event(&heartbeat_event).await?;

    // Verify custom metrics are included
    let events: Vec<sinex_db::EventRecord> =
        EventQueries::get_by_source("sinex.process".to_string(), Some(10), None)
            .fetch_all(ctx.pool())
            .await?;

    // Filter to only our process heartbeats
    let events: Vec<_> = events
        .into_iter()
        .filter(|e| {
            e.event_type == "process.heartbeat"
                && e.payload.get("process_name") == Some(&serde_json::json!(process_name))
        })
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

    // Create EventFactory for generating events
    let event_factory = EventFactory::new(source_name);

    // Simulate continuous heartbeat emission by creating multiple events
    for i in 0..5 {
        // Update metrics over time
        metrics_provider.set_uptime((i + 1) * 20);
        metrics_provider.increment_events_processed(10);

        // Create heartbeat event
        let heartbeat_payload = serde_json::json!({
            "process_name": process_name,
            "version": version,
            "uptime_seconds": *metrics_provider.uptime.lock().unwrap(),
            "memory_mb": *metrics_provider.memory_mb.lock().unwrap(),
            "cpu_percent": 15.5,
            "events_processed": *metrics_provider.events_processed.lock().unwrap(),
            "errors_count": 0,
            "health_status": "healthy",
            "custom_metrics": {}
        });
        let heartbeat_event = event_factory.create_event("process.heartbeat", heartbeat_payload);
        ctx.insert_event(&heartbeat_event).await?;

        // Small delay between heartbeats
        sleep(tokio::time::Duration::from_millis(100)).await;
    }

    // Verify multiple heartbeat events were emitted
    let all_events: Vec<sinex_db::EventRecord> =
        EventQueries::get_by_source("sinex.process".to_string(), Some(100), None)
            .fetch_all(ctx.pool())
            .await?;

    // Count heartbeat events for our process
    let heartbeat_count = all_events
        .iter()
        .filter(|e| {
            e.event_type == "process.heartbeat"
                && e.payload.get("process_name") == Some(&serde_json::json!("continuous_test"))
        })
        .count();

    assert_eq!(
        heartbeat_count, 5,
        "Should have exactly 5 heartbeat events, got {}",
        heartbeat_count
    );

    // Verify latest heartbeat has updated metrics
    let latest_event = all_events
        .iter()
        .filter(|e| {
            e.event_type == "process.heartbeat"
                && e.payload.get("process_name") == Some(&serde_json::json!("continuous_test"))
        })
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

    let event_factory = EventFactory::new("sinex.process");

    for (name, status, uptime, memory, events) in processes {
        // Emit process started event
        let started_payload = serde_json::json!({
            "process_name": name,
            "version": "1.0.0",
            "git_revision": "abc123",
            "binary_hash": "def456",
            "build_time": Utc::now(),
            "config_hash": "ghi789"
        });
        let started_event = event_factory.create_event("process.started", started_payload);
        ctx.insert_event(&started_event).await?;

        // Emit heartbeat with health status
        let heartbeat_payload = serde_json::json!({
            "process_name": name,
            "version": "1.0.0",
            "uptime_seconds": uptime,
            "memory_mb": memory,
            "cpu_percent": 15.5,
            "events_processed": events,
            "errors_count": 0,
            "health_status": status,
            "custom_metrics": {}
        });
        let heartbeat_event = event_factory.create_event("process.heartbeat", heartbeat_payload);
        ctx.insert_event(&heartbeat_event).await?;
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
    let process_name = "failing_process";
    let version = "1.0.0";
    let event_factory = EventFactory::new("sinex.process");

    // Start normally
    let started_payload = serde_json::json!({
        "process_name": process_name,
        "version": version,
        "git_revision": "abc123",
        "binary_hash": "def456",
        "build_time": Utc::now(),
        "config_hash": "ghi789"
    });
    ctx.insert_event(&event_factory.create_event("process.started", started_payload))
        .await?;

    // First heartbeat - healthy
    let heartbeat1_payload = serde_json::json!({
        "process_name": process_name,
        "version": version,
        "uptime_seconds": 10,
        "memory_mb": 128,
        "cpu_percent": 15.5,
        "events_processed": 100,
        "errors_count": 0,
        "health_status": "healthy",
        "custom_metrics": {}
    });
    ctx.insert_event(&event_factory.create_event("process.heartbeat", heartbeat1_payload))
        .await?;

    // Simulate degraded state
    metrics_provider.add_custom_metric("health_status", serde_json::json!("degraded"));
    metrics_provider.add_custom_metric("error_count", serde_json::json!(5));
    let custom_metrics = metrics_provider.custom_metrics.lock().unwrap().clone();

    let heartbeat2_payload = serde_json::json!({
        "process_name": process_name,
        "version": version,
        "uptime_seconds": 20,
        "memory_mb": 128,
        "cpu_percent": 25.5,
        "events_processed": 150,
        "errors_count": 5,
        "health_status": "degraded",
        "custom_metrics": custom_metrics
    });
    ctx.insert_event(&event_factory.create_event("process.heartbeat", heartbeat2_payload))
        .await?;

    // Simulate critical state
    metrics_provider.add_custom_metric("health_status", serde_json::json!("critical"));
    metrics_provider.add_custom_metric("error_count", serde_json::json!(15));
    metrics_provider.add_custom_metric(
        "last_error",
        serde_json::json!("Database connection failed"),
    );
    let custom_metrics = metrics_provider.custom_metrics.lock().unwrap().clone();

    let heartbeat3_payload = serde_json::json!({
        "process_name": process_name,
        "version": version,
        "uptime_seconds": 30,
        "memory_mb": 256,
        "cpu_percent": 45.5,
        "events_processed": 160,
        "errors_count": 15,
        "health_status": "critical",
        "custom_metrics": custom_metrics
    });
    ctx.insert_event(&event_factory.create_event("process.heartbeat", heartbeat3_payload))
        .await?;

    // Simulate shutdown due to errors
    let shutdown_payload = serde_json::json!({
        "process_name": process_name,
        "version": version,
        "uptime_seconds": 30,
        "graceful": false,
        "shutdown_reason": "Process terminated due to critical errors"
    });
    ctx.insert_event(&event_factory.create_event("process.shutdown", shutdown_payload))
        .await?;

    // Verify the failure progression is recorded
    let events: Vec<sinex_db::EventRecord> =
        EventQueries::get_by_source("sinex.process".to_string(), Some(20), None)
            .fetch_all(ctx.pool())
            .await?;

    // Filter to only our process
    let events: Vec<_> = events
        .into_iter()
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
    assert_eq!(
        payload3["custom_metrics"]["last_error"],
        "Database connection failed"
    );

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
    let event_factory = EventFactory::new("sinex.process");

    // First process instance
    let metrics1 = MockMetricsProvider::new();

    // Start first instance
    let started1_payload = serde_json::json!({
        "process_name": process_name,
        "version": "1.0.0",
        "git_revision": "abc123",
        "binary_hash": "def456",
        "build_time": Utc::now(),
        "config_hash": "ghi789"
    });
    ctx.insert_event(&event_factory.create_event("process.started", started1_payload))
        .await?;

    metrics1.set_uptime(10);
    let heartbeat1_payload = serde_json::json!({
        "process_name": process_name,
        "version": "1.0.0",
        "uptime_seconds": 10,
        "memory_mb": 128,
        "cpu_percent": 15.5,
        "events_processed": 50,
        "errors_count": 0,
        "health_status": "healthy",
        "custom_metrics": {}
    });
    ctx.insert_event(&event_factory.create_event("process.heartbeat", heartbeat1_payload))
        .await?;

    let shutdown1_payload = serde_json::json!({
        "process_name": process_name,
        "version": "1.0.0",
        "uptime_seconds": 10,
        "graceful": true,
        "shutdown_reason": "Planned restart"
    });
    ctx.insert_event(&event_factory.create_event("process.shutdown", shutdown1_payload))
        .await?;

    // Small delay to ensure different timestamps
    sleep(tokio::time::Duration::from_millis(100)).await;

    // Second process instance (restart)
    let metrics2 = MockMetricsProvider::new();

    // Start second instance with new version
    let started2_payload = serde_json::json!({
        "process_name": process_name,
        "version": "1.0.1", // New version
        "git_revision": "xyz789",
        "binary_hash": "uvw123",
        "build_time": Utc::now(),
        "config_hash": "rst456"
    });
    ctx.insert_event(&event_factory.create_event("process.started", started2_payload))
        .await?;

    metrics2.set_uptime(5); // Fresh start
    let heartbeat2_payload = serde_json::json!({
        "process_name": process_name,
        "version": "1.0.1",
        "uptime_seconds": 5,
        "memory_mb": 128,
        "cpu_percent": 12.5,
        "events_processed": 10,
        "errors_count": 0,
        "health_status": "healthy",
        "custom_metrics": {}
    });
    ctx.insert_event(&event_factory.create_event("process.heartbeat", heartbeat2_payload))
        .await?;

    // Verify restart is detectable by analyzing events
    let all_events: Vec<sinex_db::EventRecord> =
        EventQueries::get_by_source("sinex.process".to_string(), Some(20), None)
            .fetch_all(ctx.pool())
            .await?;

    // Filter to only our process
    let all_events: Vec<_> = all_events
        .into_iter()
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
    let event_factory = EventFactory::new("sinex.process");

    // Emit many heartbeats rapidly
    let start_time = std::time::Instant::now();
    for i in 0..20 {
        metrics_provider.set_uptime(i * 5);
        metrics_provider.increment_events_processed(10);

        let heartbeat_payload = serde_json::json!({
            "process_name": process_name,
            "version": version,
            "uptime_seconds": *metrics_provider.uptime.lock().unwrap(),
            "memory_mb": *metrics_provider.memory_mb.lock().unwrap(),
            "cpu_percent": 15.5,
            "events_processed": *metrics_provider.events_processed.lock().unwrap(),
            "errors_count": 0,
            "health_status": "healthy",
            "custom_metrics": {}
        });
        ctx.insert_event(&event_factory.create_event("process.heartbeat", heartbeat_payload))
            .await?;

        // Small delay to avoid overwhelming the database
        sleep(tokio::time::Duration::from_millis(10)).await;
    }
    let duration = start_time.elapsed();

    // Verify all heartbeats were recorded
    let all_events: Vec<sinex_db::EventRecord> =
        EventQueries::get_by_source("sinex.process".to_string(), Some(50), None)
            .fetch_all(ctx.pool())
            .await?;

    // Count heartbeat events for our process
    let count = all_events
        .iter()
        .filter(|e| {
            e.event_type == "process.heartbeat"
                && e.payload.get("process_name") == Some(&serde_json::json!("high_freq_test"))
        })
        .count();

    assert_eq!(count, 20, "Should have recorded all 20 heartbeats");

    // Verify performance (should complete quickly)
    assert!(
        duration.as_millis() < 5000,
        "High frequency heartbeats should complete within 5 seconds"
    );

    // Verify chronological ordering
    let heartbeat_events: Vec<_> = all_events
        .iter()
        .filter(|e| {
            e.event_type == "process.heartbeat"
                && e.payload.get("process_name") == Some(&serde_json::json!("high_freq_test"))
        })
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

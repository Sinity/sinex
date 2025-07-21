// Process Event Integration Tests - Refactored with Test Macros
//
// This module tests the event-based process lifecycle system using test macros
// to reduce repetition and improve maintainability.

use crate::common::prelude::*;
use crate::common::builders::{TestEventBuilder, TestEvents};
use crate::common::query_helpers::TestQueries;
use chrono::Utc;
use sinex_core_runtime::MetricsProvider;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::time::sleep;

// Import test macros
use crate::test_event_insertion;
use crate::test_batch_events;
use crate::test_event_flow;
use crate::parameterized_test;

// =============================================================================
// Mock Metrics Provider for Testing (unchanged)
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
// PROCESS EVENT TESTS - Using Macros
// =============================================================================

// Simple process event insertion tests
test_event_insertion!(
    test_process_started_event,
    "sinex.process",
    "process.started",
    json!({
        "process_name": "test_process",
        "version": "1.0.0",
        "git_revision": "abc123",
        "binary_hash": "def456",
        "build_time": Utc::now(),
        "config_hash": "ghi789"
    })
);

test_event_insertion!(
    test_process_shutdown_event,
    "sinex.process",
    "process.shutdown",
    json!({
        "process_name": "test_process",
        "version": "1.0.0",
        "uptime_seconds": 3600,
        "graceful": true,
        "shutdown_reason": "User requested shutdown"
    })
);

// Event flow tests for process monitoring
test_event_flow!(
    test_process_to_health_aggregator_flow,
    "sinex.process",
    "process.heartbeat",
    "health-aggregator"
);

test_event_flow!(
    test_process_to_metrics_collector_flow,
    "sinex.process",
    "process.metrics",
    "metrics-collector"
);

// Parameterized tests for different process states
parameterized_test!(
    test_process_health_states,
    vec![
        ("Healthy", json!({
            "process_name": "healthy_process",
            "health_status": "healthy",
            "uptime_seconds": 3600,
            "memory_mb": 256,
            "errors_count": 0
        })),
        ("Degraded", json!({
            "process_name": "degraded_process",
            "health_status": "degraded",
            "uptime_seconds": 1800,
            "memory_mb": 512,
            "errors_count": 5
        })),
        ("Critical", json!({
            "process_name": "critical_process",
            "health_status": "critical",
            "uptime_seconds": 300,
            "memory_mb": 1024,
            "errors_count": 50
        })),
    ],
    |pool, payload| async move {
        let event = TestEventBuilder::new("sinex.process", "process.heartbeat")
            .with_payload(payload.clone())
            .insert(pool)
            .await?;
        
        let retrieved = TestQueries::get_event(pool, event.id).await?;
        assert_eq!(retrieved.payload["health_status"], payload["health_status"]);
        assert_eq!(retrieved.payload["process_name"], payload["process_name"]);
        
        Ok(())
    }
);

// Batch heartbeat events test
test_batch_events!(
    test_multiple_heartbeats,
    "sinex.process",
    "process.heartbeat",
    10,
    |pool, events| async move {
        // Verify heartbeats are time-ordered
        for i in 1..events.len() {
            assert!(events[i].id > events[i-1].id, "Heartbeats should be time-ordered");
        }
        
        // Verify all have required fields
        for event in &events {
            assert!(event.payload.get("process_name").is_some());
            assert!(event.payload.get("health_status").is_some());
        }
        
        Ok(())
    }
);

// =============================================================================
// COMPLEX TESTS - Still using direct implementation
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

    // This test is complex enough that using macros would reduce clarity
    // It tests a full lifecycle with state changes between events
    
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

    // Verify event sequence and state progression
    assert_eq!(all_events[0].event_type, "process.started");
    assert_eq!(all_events[1].event_type, "process.heartbeat");
    assert_eq!(all_events[1].payload["uptime_seconds"], 10);
    assert_eq!(all_events[2].event_type, "process.heartbeat");
    assert_eq!(all_events[2].payload["uptime_seconds"], 20);
    assert_eq!(all_events[3].event_type, "process.shutdown");

    Ok(())
}
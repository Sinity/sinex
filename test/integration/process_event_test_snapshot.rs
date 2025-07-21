// Process Event Integration Tests with Snapshot Testing
//
// Demonstrates snapshot testing for process lifecycle events,
// heartbeat monitoring, and metrics collection.

use crate::common::prelude::*;
use crate::common::builders::{TestEventBuilder, TestEvents};
use crate::common::query_helpers::TestQueries;
use crate::common::snapshot_testing::{assert_snapshot, snapshot, Redaction};
use chrono::Utc;
use sinex_core_runtime::MetricsProvider;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::time::sleep;

// =============================================================================
// Mock Metrics Provider (same as original)
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
        self.custom_metrics.lock().unwrap().insert(key.to_string(), value);
    }
}

// =============================================================================
// SNAPSHOT TESTS FOR PROCESS LIFECYCLE
// =============================================================================

#[sinex_test]
async fn test_process_lifecycle_snapshot(ctx: TestContext) -> TestResult {
    let process_name = "snapshot_lifecycle_test";
    let version = "2.0.0";
    let source_name = "sinex.process";

    // Emit complete process lifecycle
    TestEventBuilder::new(source_name, "process.started")
        .with_field("process_name", json!(process_name))
        .with_field("version", json!(version))
        .with_field("git_revision", json!("abc123"))
        .with_field("binary_hash", json!("def456"))
        .with_field("build_time", json!(Utc::now()))
        .with_field("config_hash", json!("ghi789"))
        .insert(ctx.pool())
        .await?;

    // Emit heartbeats with increasing metrics
    for i in 1..=3 {
        TestEventBuilder::new(source_name, "process.heartbeat")
            .with_field("process_name", json!(process_name))
            .with_field("version", json!(version))
            .with_field("uptime_seconds", json!(i * 10))
            .with_field("memory_mb", json!(128 + i * 10))
            .with_field("cpu_percent", json!(15.5 + i as f64))
            .with_field("events_processed", json!(i * 100))
            .with_field("errors_count", json!(0))
            .with_field("health_status", json!("healthy"))
            .with_field("custom_metrics", json!({}))
            .insert(ctx.pool())
            .await?;
    }

    // Emit shutdown
    TestEventBuilder::new(source_name, "process.shutdown")
        .with_field("process_name", json!(process_name))
        .with_field("version", json!(version))
        .with_field("uptime_seconds", json!(30))
        .with_field("graceful", json!(true))
        .with_field("shutdown_reason", json!("Graceful shutdown requested"))
        .insert(ctx.pool())
        .await?;

    // Retrieve all events for this process
    let all_events = TestQueries::get_events_by_source(
        ctx.pool(),
        "sinex.process",
        Some(20),
    )
    .await?;

    let process_events: Vec<_> = all_events.into_iter()
        .filter(|e| e.payload.get("process_name") == Some(&json!(process_name)))
        .collect();

    // Create lifecycle snapshot
    let lifecycle_snapshot = json!({
        "process_name": process_name,
        "version": version,
        "event_sequence": process_events.iter().map(|e| json!({
            "event_type": e.event_type,
            "key_metrics": match e.event_type.as_str() {
                "process.started" => json!({
                    "git_revision": e.payload["git_revision"],
                    "binary_hash": e.payload["binary_hash"],
                    "config_hash": e.payload["config_hash"],
                }),
                "process.heartbeat" => json!({
                    "uptime_seconds": e.payload["uptime_seconds"],
                    "memory_mb": e.payload["memory_mb"],
                    "cpu_percent": e.payload["cpu_percent"],
                    "events_processed": e.payload["events_processed"],
                    "health_status": e.payload["health_status"],
                }),
                "process.shutdown" => json!({
                    "uptime_seconds": e.payload["uptime_seconds"],
                    "graceful": e.payload["graceful"],
                    "shutdown_reason": e.payload["shutdown_reason"],
                }),
                _ => json!({}),
            }
        })).collect::<Vec<_>>(),
        "total_events": process_events.len(),
    });

    // Assert with timestamp redaction
    assert_snapshot!(
        lifecycle_snapshot,
        "process_lifecycle_complete",
        Redaction::timestamps(),
        Redaction::field("event_sequence.*.key_metrics.build_time", json!("[BUILD_TIME]"))
    );

    Ok(())
}

#[sinex_test]
async fn test_process_metrics_evolution_snapshot(ctx: TestContext) -> TestResult {
    let metrics_provider = MockMetricsProvider::new();
    let process_name = "metrics_evolution_test";
    let version = "1.5.0";
    let source_name = "sinex.process";

    // Simulate metrics evolution over time
    let mut metrics_history = Vec::new();

    for hour in 0..5 {
        // Update metrics
        metrics_provider.set_uptime(hour * 3600);
        metrics_provider.set_memory(128 + hour * 50);
        metrics_provider.increment_events_processed(1000);
        
        // Add custom metrics that change over time
        metrics_provider.add_custom_metric("queue_size", json!(25 + hour * 5));
        metrics_provider.add_custom_metric("active_connections", json!(8 + hour * 2));
        metrics_provider.add_custom_metric("cache_hit_rate", json!(0.85 - (hour as f64 * 0.05)));
        
        let custom_metrics = metrics_provider.custom_metrics.lock().unwrap().clone();
        
        // Emit heartbeat
        TestEventBuilder::new(source_name, "process.heartbeat")
            .with_field("process_name", json!(process_name))
            .with_field("version", json!(version))
            .with_field("uptime_seconds", json!(*metrics_provider.uptime.lock().unwrap()))
            .with_field("memory_mb", json!(*metrics_provider.memory_mb.lock().unwrap()))
            .with_field("cpu_percent", json!(15.5 + hour as f64 * 2.5))
            .with_field("events_processed", json!(*metrics_provider.events_processed.lock().unwrap()))
            .with_field("errors_count", json!(hour * 2)) // Simulating increasing errors
            .with_field("health_status", json!(if hour < 3 { "healthy" } else { "degraded" }))
            .with_field("custom_metrics", json!(custom_metrics.clone()))
            .insert(ctx.pool())
            .await?;

        // Capture metrics state
        metrics_history.push(json!({
            "hour": hour,
            "uptime_seconds": hour * 3600,
            "memory_mb": 128 + hour * 50,
            "events_processed": (hour + 1) * 1000,
            "errors_count": hour * 2,
            "health_status": if hour < 3 { "healthy" } else { "degraded" },
            "custom_metrics": custom_metrics,
        }));
    }

    // Create metrics evolution snapshot
    let evolution_snapshot = json!({
        "process_info": {
            "name": process_name,
            "version": version,
        },
        "metrics_evolution": metrics_history,
        "analysis": {
            "memory_growth_mb": 200, // 50 MB per hour * 4 hours
            "total_events_processed": 5000,
            "health_degradation_at_hour": 3,
            "cache_hit_rate_decline": 0.20, // From 0.85 to 0.65
        },
    });

    // Use snapshot builder for more control
    snapshot(evolution_snapshot)
        .name("process_metrics_evolution")
        .redact_timestamps()
        .assert();

    Ok(())
}

#[sinex_test]
async fn test_multi_process_health_snapshot(ctx: TestContext) -> TestResult {
    let source_name = "sinex.process";
    
    // Define multiple processes with different health states
    let processes = vec![
        ("api_server", "1.0.0", "healthy", 128, 1000),
        ("worker_1", "1.0.0", "healthy", 256, 2000),
        ("worker_2", "1.0.0", "degraded", 512, 3000),
        ("analytics", "2.0.0", "unhealthy", 1024, 100),
    ];

    // Emit heartbeats for all processes
    for (name, version, health, memory, events) in &processes {
        TestEventBuilder::new(source_name, "process.heartbeat")
            .with_field("process_name", json!(name))
            .with_field("version", json!(version))
            .with_field("uptime_seconds", json!(3600))
            .with_field("memory_mb", json!(memory))
            .with_field("cpu_percent", json!(15.5))
            .with_field("events_processed", json!(events))
            .with_field("errors_count", json!(if health == &"unhealthy" { 50 } else { 0 }))
            .with_field("health_status", json!(health))
            .with_field("custom_metrics", json!({
                "last_health_check": "2024-01-01T00:00:00Z",
                "restart_count": if health == &"unhealthy" { 3 } else { 0 },
            }))
            .insert(ctx.pool())
            .await?;
    }

    // Retrieve all heartbeats
    let all_events = TestQueries::get_events_by_source(
        ctx.pool(),
        "sinex.process",
        Some(10),
    )
    .await?;

    let heartbeats: Vec<_> = all_events.into_iter()
        .filter(|e| e.event_type == "process.heartbeat")
        .collect();

    // Build health dashboard snapshot
    let health_snapshot = json!({
        "timestamp": "[TIMESTAMP]",
        "processes": heartbeats.iter().map(|e| json!({
            "name": e.payload["process_name"],
            "version": e.payload["version"],
            "health_status": e.payload["health_status"],
            "memory_mb": e.payload["memory_mb"],
            "events_processed": e.payload["events_processed"],
            "errors_count": e.payload["errors_count"],
            "custom_metrics": e.payload["custom_metrics"],
        })).collect::<Vec<_>>(),
        "summary": {
            "total_processes": heartbeats.len(),
            "healthy_count": heartbeats.iter().filter(|e| e.payload["health_status"] == "healthy").count(),
            "degraded_count": heartbeats.iter().filter(|e| e.payload["health_status"] == "degraded").count(),
            "unhealthy_count": heartbeats.iter().filter(|e| e.payload["health_status"] == "unhealthy").count(),
            "total_memory_mb": heartbeats.iter()
                .filter_map(|e| e.payload["memory_mb"].as_u64())
                .sum::<u64>(),
            "total_events_processed": heartbeats.iter()
                .filter_map(|e| e.payload["events_processed"].as_u64())
                .sum::<u64>(),
        },
    });

    assert_snapshot!(
        health_snapshot,
        "multi_process_health_dashboard",
        Redaction::field("processes.*.custom_metrics.last_health_check", json!("[TIMESTAMP]"))
    );

    Ok(())
}

#[sinex_test]
async fn test_process_error_tracking_snapshot(ctx: TestContext) -> TestResult {
    let process_name = "error_tracking_test";
    let version = "1.0.0";
    let source_name = "sinex.process";

    // Simulate a process with increasing errors
    let error_scenarios = vec![
        (0, "healthy", json!({})),
        (5, "healthy", json!({"recent_errors": ["Connection timeout", "Retry succeeded"]})),
        (15, "degraded", json!({"recent_errors": ["Database connection lost", "Queue overflow"]})),
        (50, "unhealthy", json!({"recent_errors": ["Out of memory", "Service unavailable", "Critical failure"]})),
    ];

    for (i, (error_count, health_status, error_details)) in error_scenarios.iter().enumerate() {
        TestEventBuilder::new(source_name, "process.heartbeat")
            .with_field("process_name", json!(process_name))
            .with_field("version", json!(version))
            .with_field("uptime_seconds", json!((i + 1) * 60))
            .with_field("memory_mb", json!(128))
            .with_field("cpu_percent", json!(15.5))
            .with_field("events_processed", json!(100))
            .with_field("errors_count", json!(error_count))
            .with_field("health_status", json!(health_status))
            .with_field("error_details", error_details.clone())
            .insert(ctx.pool())
            .await?;
    }

    // Retrieve events
    let all_events = TestQueries::get_events_by_source(
        ctx.pool(),
        "sinex.process",
        Some(10),
    )
    .await?;

    let error_events: Vec<_> = all_events.into_iter()
        .filter(|e| e.payload.get("process_name") == Some(&json!(process_name)))
        .collect();

    // Create error tracking snapshot
    let error_snapshot = json!({
        "process": process_name,
        "error_progression": error_events.iter().map(|e| json!({
            "uptime_seconds": e.payload["uptime_seconds"],
            "errors_count": e.payload["errors_count"],
            "health_status": e.payload["health_status"],
            "error_details": e.payload.get("error_details").unwrap_or(&json!({})),
        })).collect::<Vec<_>>(),
        "analysis": {
            "initial_health": "healthy",
            "final_health": "unhealthy",
            "max_errors": 50,
            "degradation_threshold": 15,
        },
    });

    assert_snapshot!(error_snapshot, "process_error_progression");

    Ok(())
}
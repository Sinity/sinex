//! Event Processing Integration Tests
//!
//! This module tests the event-based process lifecycle system, including:
//! - Process lifecycle events (started, heartbeat, shutdown)
//! - Health monitoring through events instead of database tables
//! - Process metrics collection and reporting
//! - Integration with the health aggregator
//!
//! Migrated to modern infrastructure: #[sinex_test], TestContext, repository pattern.

use xtask::sandbox::prelude::*;

// Additional specific imports

#[sinex_test]
async fn test_process_heartbeat_emitter_basic_functionality(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    tracing::info!("Testing basic process heartbeat emitter functionality");

    // Create heartbeat event using modern TestContext
    let heartbeat_payload = json!({
        "process_name": "test_process",
        "version": "1.0.0",
        "uptime_seconds": 3600,
        "memory_mb": 256,
        "cpu_percent": 15.5,
        "events_processed": 42,
        "errors_count": 0,
        "health_status": "healthy",
        "custom_metrics": {}
    });

    let _heartbeat_event = ctx
        .publish(DynamicPayload::new("sinex.process", "process.heartbeat", heartbeat_payload))
        .await?;

    // Verify heartbeat event was created using repository pattern
    let events = ctx
        .pool
        .events()
        .get_by_source(
            &EventSource::from("sinex.process"),
            sinex_core::types::Pagination::new(Some(10), None),
        )
        .await?;

    let heartbeat_events: Vec<_> = events
        .iter()
        .filter(|e| e.event_type == EventType::from("process.heartbeat"))
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

    tracing::info!("Process heartbeat emitter basic functionality test completed");
    Ok(())
}

#[sinex_test]
async fn test_process_lifecycle_events(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    tracing::info!("Testing complete process lifecycle events");

    let process_name = "lifecycle_test";
    let version = "2.0.0";

    // Emit process started event
    let started_payload = json!({
        "process_name": process_name,
        "version": version,
        "git_revision": "abc123",
        "binary_hash": "def456",
        "build_time": Utc::now(),
        "config_hash": "ghi789"
    });
    let _started_event = ctx
        .publish(DynamicPayload::new("sinex.process", "process.started", started_payload))
        .await?;

    // Emit heartbeat
    let heartbeat1_payload = json!({
        "process_name": process_name,
        "version": version,
        "uptime_seconds": 10,
        "memory_mb": 128,
        "cpu_percent": 15.5,
        "events_processed": 15,
        "errors_count": 0,
        "health_status": "healthy",
        "custom_metrics": {}
    });
    let _heartbeat1_event = ctx
        .publish(DynamicPayload::new("sinex.process", "process.heartbeat", heartbeat1_payload))
        .await?;

    // Update metrics again
    // Emit another heartbeat
    let heartbeat2_payload = json!({
        "process_name": process_name,
        "version": version,
        "uptime_seconds": 20,
        "memory_mb": 128,
        "cpu_percent": 15.5,
        "events_processed": 40,
        "errors_count": 0,
        "health_status": "healthy",
        "custom_metrics": {}
    });
    let _heartbeat2_event = ctx
        .publish(DynamicPayload::new("sinex.process", "process.heartbeat", heartbeat2_payload))
        .await?;

    // Emit process shutdown event
    let shutdown_payload = json!({
        "process_name": process_name,
        "version": version,
        "uptime_seconds": 20,
        "graceful": true,
        "shutdown_reason": "Graceful shutdown requested"
    });
    let _shutdown_event = ctx
        .publish(DynamicPayload::new("sinex.process", "process.shutdown", shutdown_payload))
        .await?;

    // Verify all events were created in correct order using repository pattern
    let all_events = ctx
        .pool
        .events()
        .get_by_source(
            &EventSource::from("sinex.process"),
            sinex_core::types::Pagination::new(Some(10), None),
        )
        .await?;

    // Filter to only our process
    let all_events: Vec<_> = all_events
        .into_iter()
        .filter(|e| e.payload.get("process_name") == Some(&json!("lifecycle_test")))
        .collect();

    assert_eq!(all_events.len(), 4, "Should have 4 lifecycle events");

    // Check process.started event
    assert_eq!(all_events[0].event_type, EventType::from("process.started"));
    let started_payload: serde_json::Value = all_events[0].payload.clone();
    assert_eq!(started_payload["process_name"], "lifecycle_test");
    assert_eq!(started_payload["version"], "2.0.0");

    // Check first heartbeat
    assert_eq!(
        all_events[1].event_type,
        EventType::from("process.heartbeat")
    );
    let heartbeat1_payload: serde_json::Value = all_events[1].payload.clone();
    assert_eq!(heartbeat1_payload["uptime_seconds"], 10);
    assert_eq!(heartbeat1_payload["events_processed"], 15);

    // Check second heartbeat
    assert_eq!(
        all_events[2].event_type,
        EventType::from("process.heartbeat")
    );
    let heartbeat2_payload: serde_json::Value = all_events[2].payload.clone();
    assert_eq!(heartbeat2_payload["uptime_seconds"], 20);
    assert_eq!(heartbeat2_payload["events_processed"], 40);

    // Check process.shutdown event
    assert_eq!(
        all_events[3].event_type,
        EventType::from("process.shutdown")
    );
    let shutdown_payload: serde_json::Value = all_events[3].payload.clone();
    assert_eq!(shutdown_payload["process_name"], "lifecycle_test");
    assert_eq!(
        shutdown_payload["shutdown_reason"],
        "Graceful shutdown requested"
    );

    tracing::info!("Process lifecycle events test completed");
    Ok(())
}

#[sinex_test]
async fn test_process_heartbeat_with_custom_metrics(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    tracing::info!("Testing process heartbeat with custom metrics");

    let process_name = "custom_metrics_test";
    let version = "1.5.0";

    // Create heartbeat with custom metrics
    let custom_metrics = {
        let mut metrics = HashMap::new();
        metrics.insert("queue_size".to_string(), json!(25));
        metrics.insert("active_connections".to_string(), json!(8));
        metrics.insert("cache_hit_rate".to_string(), json!(0.85));
        metrics.insert("last_error".to_string(), json!("Connection timeout"));
        metrics
    };

    let heartbeat_payload = json!({
        "process_name": process_name,
        "version": version,
        "uptime_seconds": 100,
        "memory_mb": 128,
        "cpu_percent": 15.5,
        "events_processed": 0,
        "errors_count": 0,
        "health_status": "healthy",
        "custom_metrics": custom_metrics
    });

    let _heartbeat_event = ctx
        .publish(DynamicPayload::new("sinex.process", "process.heartbeat", heartbeat_payload))
        .await?;

    // Verify custom metrics are included using repository pattern
    let events = ctx
        .pool
        .events()
        .get_by_source(
            &EventSource::from("sinex.process"),
            sinex_core::types::Pagination::new(Some(10), None),
        )
        .await?;

    // Filter to only our process heartbeats
    let events: Vec<_> = events
        .into_iter()
        .filter(|e| e.payload.get("process_name") == Some(&json!(process_name)))
        .filter(|e| e.event_type == EventType::from("process.heartbeat"))
        .collect();

    assert_eq!(events.len(), 1);

    let payload: serde_json::Value = events[0].payload.clone();
    let custom_metrics = &payload["custom_metrics"];
    assert_eq!(custom_metrics["queue_size"], 25);
    assert_eq!(custom_metrics["active_connections"], 8);
    assert_eq!(custom_metrics["cache_hit_rate"], 0.85);
    assert_eq!(custom_metrics["last_error"], "Connection timeout");

    tracing::info!("Process heartbeat with custom metrics test completed");
    Ok(())
}

#[sinex_test]
async fn test_health_aggregator_process_discovery(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    tracing::info!("Testing health aggregator process discovery");

    // Create multiple processes with different states
    let processes = vec![
        ("web_server", "healthy", 3600, 512, 1000),
        ("worker_1", "healthy", 1800, 256, 500),
        ("worker_2", "degraded", 900, 128, 250),
        ("monitor", "healthy", 7200, 64, 100),
    ];

    for (name, status, uptime, memory, events) in processes {
        // Emit process started event
        let started_payload = json!({
            "process_name": name,
            "version": "1.0.0",
            "git_revision": "abc123",
            "binary_hash": "def456",
            "build_time": Utc::now(),
            "config_hash": "ghi789"
        });

        let _started_event = ctx
            .publish(DynamicPayload::new("sinex.process", "process.started", started_payload))
            .await?;

        // Emit heartbeat with health status
        let heartbeat_payload = json!({
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

        let _heartbeat_event = ctx
            .publish(DynamicPayload::new("sinex.process", "process.heartbeat", heartbeat_payload))
            .await?;
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
    .fetch_all(ctx.pool.as_ref())
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

    tracing::info!(
        processes_discovered = found_processes.len(),
        "Health aggregator discovery test completed"
    );
    Ok(())
}

#[sinex_test]
async fn test_process_failure_detection(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    tracing::info!("Testing process failure detection through events");

    let process_name = "failing_process";
    let version = "1.0.0";

    // Start normally
    let started_payload = json!({
        "process_name": process_name,
        "version": version,
        "git_revision": "abc123",
        "binary_hash": "def456",
        "build_time": Utc::now(),
        "config_hash": "ghi789"
    });
    let _started_event = ctx
        .publish(DynamicPayload::new("sinex.process", "process.started", started_payload))
        .await?;

    // First heartbeat - healthy
    let heartbeat1_payload = json!({
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
    let _heartbeat1_event = ctx
        .publish(DynamicPayload::new("sinex.process", "process.heartbeat", heartbeat1_payload))
        .await?;

    // Simulate degraded state
    let degraded_custom_metrics = {
        let mut metrics = HashMap::new();
        metrics.insert("health_status".to_string(), json!("degraded"));
        metrics.insert("error_count".to_string(), json!(5));
        metrics
    };

    let heartbeat2_payload = json!({
        "process_name": process_name,
        "version": version,
        "uptime_seconds": 20,
        "memory_mb": 128,
        "cpu_percent": 25.5,
        "events_processed": 150,
        "errors_count": 5,
        "health_status": "degraded",
        "custom_metrics": degraded_custom_metrics
    });
    let _heartbeat2_event = ctx
        .publish(DynamicPayload::new("sinex.process", "process.heartbeat", heartbeat2_payload))
        .await?;

    // Simulate critical state
    let critical_custom_metrics = {
        let mut metrics = HashMap::new();
        metrics.insert("health_status".to_string(), json!("critical"));
        metrics.insert("error_count".to_string(), json!(15));
        metrics.insert(
            "last_error".to_string(),
            json!("Database connection failed"),
        );
        metrics
    };

    let heartbeat3_payload = json!({
        "process_name": process_name,
        "version": version,
        "uptime_seconds": 30,
        "memory_mb": 256,
        "cpu_percent": 45.5,
        "events_processed": 160,
        "errors_count": 15,
        "health_status": "critical",
        "custom_metrics": critical_custom_metrics
    });
    let _heartbeat3_event = ctx
        .publish(DynamicPayload::new("sinex.process", "process.heartbeat", heartbeat3_payload))
        .await?;

    // Simulate shutdown due to errors
    let shutdown_payload = json!({
        "process_name": process_name,
        "version": version,
        "uptime_seconds": 30,
        "graceful": false,
        "shutdown_reason": "Process terminated due to critical errors"
    });
    let _shutdown_event = ctx
        .publish(DynamicPayload::new("sinex.process", "process.shutdown", shutdown_payload))
        .await?;

    // Verify the failure progression is recorded using repository pattern
    let events = ctx
        .pool
        .events()
        .get_by_source(
            &EventSource::from("sinex.process"),
            sinex_core::types::Pagination::new(Some(10), None),
        )
        .await?;

    // Filter to only our process
    let events: Vec<_> = events
        .into_iter()
        .filter(|e| e.payload.get("process_name") == Some(&json!("failing_process")))
        .collect();

    assert_eq!(events.len(), 5); // started + 3 heartbeats + shutdown

    // Check progression through health states
    let heartbeat_events: Vec<_> = events
        .iter()
        .filter(|e| e.event_type == EventType::from("process.heartbeat"))
        .collect();

    assert_eq!(heartbeat_events.len(), 3);

    let payload1: serde_json::Value = heartbeat_events[0].payload.clone();
    let payload2: serde_json::Value = heartbeat_events[1].payload.clone();
    let payload3: serde_json::Value = heartbeat_events[2].payload.clone();

    // First heartbeat should be healthy
    assert_eq!(payload1["health_status"], "healthy");

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
        .find(|e| e.event_type == EventType::from("process.shutdown"))
        .unwrap();
    let shutdown_payload: serde_json::Value = shutdown_event.payload.clone();
    assert_eq!(
        shutdown_payload["shutdown_reason"],
        "Process terminated due to critical errors"
    );

    tracing::info!("Process failure detection test completed");
    Ok(())
}

#[sinex_test]
async fn test_high_frequency_heartbeats(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    tracing::info!("Testing high frequency heartbeat processing");

    let process_name = "high_freq_test";
    let version = "1.0.0";

    // Emit many heartbeats rapidly
    let start_time = std::time::Instant::now();
    for i in 0..20 {
        let heartbeat_payload = json!({
            "process_name": process_name,
            "version": version,
            "uptime_seconds": i * 5,
            "memory_mb": 128,
            "cpu_percent": 15.5,
            "events_processed": (i + 1) * 10,
            "errors_count": 0,
            "health_status": "healthy",
            "custom_metrics": {}
        });

        let _heartbeat_event = ctx
            .publish(DynamicPayload::new("sinex.process", "process.heartbeat", heartbeat_payload))
            .await?;

        tokio::task::yield_now().await;
    }
    let duration = start_time.elapsed();

    // Verify all heartbeats were recorded using repository pattern
    let all_events = ctx
        .pool
        .events()
        .get_by_source(
            &EventSource::from("sinex.process"),
            sinex_core::types::Pagination::new(Some(30), None),
        )
        .await?;

    // Count heartbeat events for our process
    let count = all_events
        .iter()
        .filter(|e| e.payload.get("process_name") == Some(&json!("high_freq_test")))
        .filter(|e| e.event_type == EventType::from("process.heartbeat"))
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
        .filter(|e| e.payload.get("process_name") == Some(&json!("high_freq_test")))
        .filter(|e| e.event_type == EventType::from("process.heartbeat"))
        .collect();

    // Verify ingestion (ULID) timestamps are in order
    for i in 1..heartbeat_events.len() {
        let prev_ts = heartbeat_events[i - 1]
            .id
            .as_ref()
            .expect("id present")
            .as_ulid()
            .timestamp();
        let curr_ts = heartbeat_events[i]
            .id
            .as_ref()
            .expect("id present")
            .as_ulid()
            .timestamp();
        assert!(
            curr_ts >= prev_ts,
            "Heartbeats should be chronologically ordered"
        );
    }

    tracing::info!(
        heartbeats_processed = count,
        duration_ms = duration.as_millis(),
        "High frequency heartbeats test completed"
    );
    Ok(())
}

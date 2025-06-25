use sqlx::postgres::PgPoolOptions;
use crate::common::prelude::*;
use crate::common::timing_optimization::replacements::{wait_for_filtered_event_count};
use sinex_test_macros::sinex_test;

#[sinex_test]
async fn test_agent_heartbeat_generation() {
    let database_url = std::env::var("TEST_DATABASE_URL")
        .unwrap_or_else(|_| "postgres://sinex_test:testpass@localhost:5433/sinex_test".to_string());
    
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .expect("Failed to connect to test database");
    
    // Create agent
    sqlx::query(
        "INSERT INTO sinex_schemas.agent_manifests (agent_name, version, status) 
         VALUES ($1, $2, $3)"
    )
    .bind("heartbeat_test_agent")
    .bind("1.0.0")
    .bind("running")
    .execute(&pool)
    .await
    .unwrap();
    
    // Simulate heartbeat event insertion
    let heartbeat_payload = json!({
        "agent_name": "heartbeat_test_agent",
        "version": "1.0.0",
        "status": "healthy",
        "uptime_seconds": 3600,
        "metrics": {
            "events_processed": 1523,
            "error_count": 2,
            "memory_usage_mb": 156.5,
            "cpu_usage_percent": 23.4
        },
        "last_processed_event_id": Ulid::new().to_string(),
        "queue_size": 45
    });
    
    let event_id = Ulid::new();
    sqlx::query(
        "INSERT INTO raw.events (id, source, event_type, host, payload) 
         VALUES ($1::ulid, $2, $3, $4, $5::jsonb)"
    )
    .bind(&event_id.to_string())
    .bind("sinex.agent.heartbeat_test_agent")
    .bind("agent.heartbeat")
    .bind("test_host")
    .bind(&heartbeat_payload)
    .execute(&pool)
    .await
    .unwrap();
    
    // Update agent's last_heartbeat_ts
    sqlx::query(
        "UPDATE sinex_schemas.agent_manifests 
         SET last_heartbeat_ts = now() 
         WHERE agent_name = $1"
    )
    .bind("heartbeat_test_agent")
    .execute(&pool)
    .await
    .unwrap();
    
    // Verify heartbeat was recorded
    let last_heartbeat: Option<chrono::DateTime<chrono::Utc>> = sqlx::query_scalar(
        "SELECT last_heartbeat_ts FROM sinex_schemas.agent_manifests WHERE agent_name = $1"
    )
    .bind("heartbeat_test_agent")
    .fetch_one(&pool)
    .await
    .unwrap();
    
    assert!(last_heartbeat.is_some(), "Heartbeat timestamp should be set");
    
    // Verify heartbeat event exists using timing utility
    let heartbeat_count = wait_for_filtered_event_count(
        &pool,
        "source = $1 AND event_type = $2",
        &["sinex.agent.heartbeat_test_agent", "agent.heartbeat"],
        1,
        5
    ).await.unwrap();
    
    pretty_assertions::assert_eq!(heartbeat_count, 1, "Heartbeat event should exist");
}

#[sinex_test]
async fn test_stale_heartbeat_detection() {
    let database_url = std::env::var("TEST_DATABASE_URL")
        .unwrap_or_else(|_| "postgres://sinex_test:testpass@localhost:5433/sinex_test".to_string());
    
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .expect("Failed to connect to test database");
    
    // Create agents with different heartbeat times
    let agents = vec![
        ("stale_agent_1", Some("1 hour ago")),
        ("stale_agent_2", Some("5 minutes ago")),
        ("healthy_agent", Some("30 seconds ago")),
        ("new_agent", None), // No heartbeat yet
    ];
    
    for (name, heartbeat_offset) in agents {
        sqlx::query(
            "INSERT INTO sinex_schemas.agent_manifests (agent_name, version, status) 
             VALUES ($1, $2, $3)"
        )
        .bind(name)
        .bind("1.0.0")
        .bind("running")
        .execute(&pool)
        .await
        .unwrap();
        
        if let Some(offset) = heartbeat_offset {
            sqlx::query(&format!(
                "UPDATE sinex_schemas.agent_manifests 
                 SET last_heartbeat_ts = now() - interval '{}' 
                 WHERE agent_name = $1",
                offset
            ))
            .bind(name)
            .execute(&pool)
            .await
            .unwrap();
        }
    }
    
    // Query for stale agents (no heartbeat in last 10 minutes)
    let stale_agents: Vec<String> = sqlx::query_scalar(
        "SELECT agent_name FROM sinex_schemas.agent_manifests 
         WHERE status = 'running' 
         AND (last_heartbeat_ts IS NULL OR last_heartbeat_ts < now() - interval '10 minutes')
         ORDER BY agent_name"
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    
    pretty_assertions::assert_eq!(stale_agents.len(), 2);
    assert!(stale_agents.contains(&"new_agent".to_string()));
    assert!(stale_agents.contains(&"stale_agent_1".to_string()));
    
    // Query for healthy agents
    let healthy_agents: Vec<String> = sqlx::query_scalar(
        "SELECT agent_name FROM sinex_schemas.agent_manifests 
         WHERE status = 'running' 
         AND last_heartbeat_ts IS NOT NULL 
         AND last_heartbeat_ts >= now() - interval '10 minutes'
         ORDER BY agent_name"
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    
    pretty_assertions::assert_eq!(healthy_agents.len(), 2);
    assert!(healthy_agents.contains(&"healthy_agent".to_string()));
    assert!(healthy_agents.contains(&"stale_agent_2".to_string()));
}

#[sinex_test]
async fn test_heartbeat_metrics_tracking() {
    let database_url = std::env::var("TEST_DATABASE_URL")
        .unwrap_or_else(|_| "postgres://sinex_test:testpass@localhost:5433/sinex_test".to_string());
    
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .expect("Failed to connect to test database");
    
    // Create agent
    sqlx::query(
        "INSERT INTO sinex_schemas.agent_manifests (agent_name, version) 
         VALUES ($1, $2)"
    )
    .bind("metrics_test_agent")
    .bind("1.0.0")
    .execute(&pool)
    .await
    .unwrap();
    
    // Insert multiple heartbeats with different metrics
    for i in 0..5 {
        let heartbeat = json!({
            "agent_name": "metrics_test_agent",
            "version": "1.0.0",
            "status": if i == 3 { "degraded" } else { "healthy" },
            "uptime_seconds": 3600 + (i * 60),
            "metrics": {
                "events_processed": 1000 + (i * 100),
                "error_count": i,
                "memory_usage_mb": 100.0 + (i as f64 * 10.5),
                "cpu_usage_percent": 20.0 + (i as f64 * 2.5)
            }
        });
        
        sqlx::query(
            "INSERT INTO raw.events (source, event_type, host, payload, ts_orig) 
             VALUES ($1, $2, $3, $4::jsonb, now() - interval '1 minute' * $5)"
        )
        .bind("sinex.agent.metrics_test_agent")
        .bind("agent.heartbeat")
        .bind("test_host")
        .bind(&heartbeat)
        .bind(5 - i) // Older events first
        .execute(&pool)
        .await
        .unwrap();
        
        // Small delay to ensure distinct timestamps
        tokio::task::yield_now().await;
    }
    
    // Query latest heartbeat metrics
    let latest_metrics: serde_json::Value = sqlx::query_scalar(
        "SELECT payload->'metrics' 
         FROM raw.events 
         WHERE source = 'sinex.agent.metrics_test_agent' 
         AND event_type = 'agent.heartbeat'
         ORDER BY ts_orig DESC 
         LIMIT 1"
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    
    pretty_assertions::assert_eq!(latest_metrics["events_processed"], 1400);
    pretty_assertions::assert_eq!(latest_metrics["error_count"], 4);
    
    // Query average metrics over time window
    let avg_cpu: Option<f64> = sqlx::query_scalar(
        "SELECT AVG((payload->'metrics'->>'cpu_usage_percent')::float) 
         FROM raw.events 
         WHERE source = 'sinex.agent.metrics_test_agent' 
         AND event_type = 'agent.heartbeat'
         AND ts_orig >= now() - interval '10 minutes'"
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    
    assert!(avg_cpu.is_some());
    assert!((avg_cpu.unwrap() - 25.0).abs() < 0.1); // Average should be ~25%
    
    // Count degraded status occurrences using timing utility
    let degraded_count = wait_for_filtered_event_count(
        &pool,
        "source = $1 AND event_type = $2 AND payload->>'status' = $3",
        &["sinex.agent.metrics_test_agent", "agent.heartbeat", "degraded"],
        1,
        5
    ).await.unwrap();
    
    pretty_assertions::assert_eq!(degraded_count, 1);
}

#[sinex_test]
async fn test_heartbeat_based_status_updates() {
    let database_url = std::env::var("TEST_DATABASE_URL")
        .unwrap_or_else(|_| "postgres://sinex_test:testpass@localhost:5433/sinex_test".to_string());
    
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .expect("Failed to connect to test database");
    
    // Create agent
    sqlx::query(
        "INSERT INTO sinex_schemas.agent_manifests (agent_name, version, status) 
         VALUES ($1, $2, $3)"
    )
    .bind("status_update_agent")
    .bind("1.0.0")
    .bind("running")
    .execute(&pool)
    .await
    .unwrap();
    
    // Simulate error in heartbeat
    let error_heartbeat = json!({
        "agent_name": "status_update_agent",
        "version": "1.0.0",
        "status": "error",
        "error": {
            "type": "connection_failed",
            "message": "Unable to connect to data source",
            "timestamp": chrono::Utc::now()
        }
    });
    
    sqlx::query(
        "INSERT INTO raw.events (source, event_type, host, payload) 
         VALUES ($1, $2, $3, $4::jsonb)"
    )
    .bind("sinex.agent.status_update_agent")
    .bind("agent.heartbeat")
    .bind("test_host")
    .bind(&error_heartbeat)
    .execute(&pool)
    .await
    .unwrap();
    
    // Update agent status based on heartbeat
    sqlx::query(
        "UPDATE sinex_schemas.agent_manifests 
         SET status = 'error_state',
             last_heartbeat_ts = now(),
             last_error_ts = now(),
             last_error_summary = $1
         WHERE agent_name = $2"
    )
    .bind("Unable to connect to data source")
    .bind("status_update_agent")
    .execute(&pool)
    .await
    .unwrap();
    
    // Verify status was updated
    let (status, error_summary): (String, Option<String>) = sqlx::query_as(
        "SELECT status, last_error_summary 
         FROM sinex_schemas.agent_manifests 
         WHERE agent_name = $1"
    )
    .bind("status_update_agent")
    .fetch_one(&pool)
    .await
    .unwrap();
    
    pretty_assertions::assert_eq!(status, "error_state");
    pretty_assertions::assert_eq!(error_summary.unwrap(), "Unable to connect to data source");
}

#[sinex_test]
async fn test_heartbeat_frequency_monitoring() {
    let database_url = std::env::var("TEST_DATABASE_URL")
        .unwrap_or_else(|_| "postgres://sinex_test:testpass@localhost:5433/sinex_test".to_string());
    
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .expect("Failed to connect to test database");
    
    // Create agent
    sqlx::query(
        "INSERT INTO sinex_schemas.agent_manifests (agent_name, version) 
         VALUES ($1, $2)"
    )
    .bind("frequency_test_agent")
    .bind("1.0.0")
    .execute(&pool)
    .await
    .unwrap();
    
    // Insert heartbeats at irregular intervals
    let intervals = vec![60, 65, 55, 120, 58, 62]; // seconds
    let mut cumulative_time = 0;
    
    for (i, interval) in intervals.iter().enumerate() {
        cumulative_time += interval;
        
        let heartbeat = json!({
            "agent_name": "frequency_test_agent",
            "version": "1.0.0",
            "status": "healthy",
            "sequence": i
        });
        
        sqlx::query(
            "INSERT INTO raw.events (source, event_type, host, payload, ts_orig) 
             VALUES ($1, $2, $3, $4::jsonb, now() - interval '1 second' * $5)"
        )
        .bind("sinex.agent.frequency_test_agent")
        .bind("agent.heartbeat")
        .bind("test_host")
        .bind(&heartbeat)
        .bind(cumulative_time)
        .execute(&pool)
        .await
        .unwrap();
    }
    
    // Calculate heartbeat intervals using window functions
    let intervals: Vec<(i32,)> = sqlx::query_as(
        "WITH heartbeat_intervals AS (
            SELECT 
                EXTRACT(EPOCH FROM (ts_orig - LAG(ts_orig) OVER (ORDER BY ts_orig)))::int as interval_seconds
            FROM raw.events 
            WHERE source = 'sinex.agent.frequency_test_agent' 
            AND event_type = 'agent.heartbeat'
            ORDER BY ts_orig
        )
        SELECT interval_seconds 
        FROM heartbeat_intervals 
        WHERE interval_seconds IS NOT NULL"
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    
    // Verify we got the expected intervals (with some tolerance for timestamp precision)
    pretty_assertions::assert_eq!(intervals.len(), 5); // One less than total heartbeats
    
    // Check for irregular interval (the 120 second gap)
    let max_interval = intervals.iter().map(|(i,)| *i).max().unwrap();
    assert!(max_interval >= 119 && max_interval <= 121, "Should detect the 120 second gap");
    
    // Calculate average interval
    let avg_interval: Option<f64> = sqlx::query_scalar(
        "WITH heartbeat_intervals AS (
            SELECT 
                EXTRACT(EPOCH FROM (ts_orig - LAG(ts_orig) OVER (ORDER BY ts_orig))) as interval_seconds
            FROM raw.events 
            WHERE source = 'sinex.agent.frequency_test_agent' 
            AND event_type = 'agent.heartbeat'
            ORDER BY ts_orig
        )
        SELECT AVG(interval_seconds) 
        FROM heartbeat_intervals 
        WHERE interval_seconds IS NOT NULL"
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    
    assert!(avg_interval.is_some());
    let avg = avg_interval.unwrap();
    assert!(avg > 60.0 && avg < 80.0, "Average interval should be around 70 seconds");
}
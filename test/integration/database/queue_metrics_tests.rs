// Queue metrics tests - should fail until metrics implementation is complete
// Tests for queue_depth, dequeue_latency_ms, and per_agent_lag metrics

use crate::common::prelude::*;
use chrono::{Utc, Duration};

#[sinex_test]
async fn test_queue_depth_metric_calculation(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();
    // Test that queue_depth metric correctly counts pending items per agent
    
    // Create test agents
    let agent1 = "metrics-agent-1";
    let agent2 = "metrics-agent-2";
    
    create_test_agent(pool, agent1).await?;
    create_test_agent(pool, agent2).await?;
    
    // Create test events
    let event1_id = insert_test_event(pool, "metrics_test", "event1").await?;
    let event2_id = insert_test_event(pool, "metrics_test", "event2").await?;
    let event3_id = insert_test_event(pool, "metrics_test", "event3").await?;
    
    // Add to work queue with different statuses
    add_to_work_queue(pool, event1_id, agent1, 3).await?; // pending
    add_to_work_queue(pool, event2_id, agent1, 3).await?; // pending
    add_to_work_queue(pool, event3_id, agent2, 3).await?; // pending
    
    // Mark one as processing to exclude from queue depth
    sqlx::query!(
        "UPDATE sinex_schemas.work_queue SET status = 'processing' WHERE raw_event_id = $1::uuid::ulid",
        event3_id.to_uuid()
    )
    .execute(pool)
    .await?;
    
    // Calculate queue depth metrics
    let queue_metrics = calculate_queue_depth_metrics(pool).await?;
    
    
    // First, let's check what the function returns and verify against what we expect
    // After adding 2 items for agent1 and 1 for agent2 (marked as processing)
    // Expected: agent1=2 pending, agent2=0 pending (but both should be in results)
    
    // Verify that we get both agents in the results
    pretty_assertions::assert_eq!(queue_metrics.len(), 2, "Should have metrics for 2 agents (even if one has 0 pending)");
    
    // Sort results by agent name for consistent checking
    let mut sorted_metrics = queue_metrics.clone();
    sorted_metrics.sort_by(|a, b| a.agent_name.cmp(&b.agent_name));
    
    // Check agent1 (metrics-agent-1)
    pretty_assertions::assert_eq!(sorted_metrics[0].agent_name, agent1);
    pretty_assertions::assert_eq!(sorted_metrics[0].queue_depth, 2, "Agent1 should have 2 pending items");
    
    // Check agent2 (metrics-agent-2)  
    pretty_assertions::assert_eq!(sorted_metrics[1].agent_name, agent2);
    pretty_assertions::assert_eq!(sorted_metrics[1].queue_depth, 0, "Agent2 should have 0 pending items (1 processing)");
    
    Ok(())
}

#[sinex_test]
async fn test_dequeue_latency_metric_calculation(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();
    // Test that dequeue_latency_ms measures time from creation to processing
    
    let agent_name = "latency-test-agent";
    create_test_agent(pool, agent_name).await?;
    
    // Create event and add to queue
    let event_id = insert_test_event(pool, "latency_test", "event").await?;
    let work_item = add_to_work_queue(pool, event_id, agent_name, 3).await?;
    
    // Wait for any pending work queue operations
    ctx.wait_for_work_queue(0).await?;
    
    // Mark as processing (simulate worker claim)
    let processing_start = Utc::now();
    sqlx::query!(
        r#"
        UPDATE sinex_schemas.work_queue 
        SET status = 'processing', last_attempt_ts = $2 
        WHERE queue_id = $1::uuid::ulid
        "#,
        work_item.queue_id.to_uuid(),
        processing_start
    )
    .execute(pool)
    .await?;
    
    // Calculate dequeue latency (this function should exist after implementation)
    let latency_metrics = calculate_dequeue_latency_metrics(pool).await?;
    
    // Verify latency is measured
    let agent_metric = latency_metrics.iter().find(|m| m.agent_name == agent_name).unwrap();
    assert!(agent_metric.avg_dequeue_latency_ms > 90.0, "Should measure at least 90ms latency");
    assert!(agent_metric.avg_dequeue_latency_ms < 200.0, "Should be under 200ms latency");
    
    Ok(())
}

#[sinex_test]
async fn test_per_agent_lag_metric_calculation(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();
    // Test that per_agent_lag measures how far behind each agent is
    
    let fast_agent = "fast-agent";
    let slow_agent = "slow-agent"; 
    
    create_test_agent(pool, fast_agent).await?;
    create_test_agent(pool, slow_agent).await?;
    
    // Create events at different times
    let old_event = insert_test_event(pool, "lag_test", "old_event").await?;
    let recent_event = insert_test_event(pool, "lag_test", "recent_event").await?;
    
    // Assign to different agents
    add_to_work_queue(pool, old_event, slow_agent, 3).await?;
    add_to_work_queue(pool, recent_event, fast_agent, 3).await?;
    
    // Age the old event artificially by updating the work queue created_at time
    sqlx::query!(
        r#"
        UPDATE sinex_schemas.work_queue 
        SET created_at = $2 
        WHERE raw_event_id = $1::uuid::ulid
        "#,
        old_event.to_uuid(),
        Utc::now() - Duration::minutes(10)
    )
    .execute(pool)
    .await?;
    
    // Calculate lag metrics (this function should exist after implementation)
    let lag_metrics = calculate_per_agent_lag_metrics(pool).await?;
    
    // Verify lag measurements
    let slow_agent_metric = lag_metrics.iter().find(|m| m.agent_name == slow_agent).unwrap();
    let fast_agent_metric = lag_metrics.iter().find(|m| m.agent_name == fast_agent).unwrap();
    
    assert!(slow_agent_metric.max_lag_seconds > 500.0, "Slow agent should have significant lag");
    assert!(fast_agent_metric.max_lag_seconds < 60.0, "Fast agent should have minimal lag");
    
    Ok(())
}

#[sinex_test]
async fn test_prometheus_metrics_exposition(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();
    // Test that metrics are properly exposed in Prometheus format
    
    let agent_name = "prometheus-test-agent";
    create_test_agent(pool, agent_name).await?;
    
    // Create some test data
    let event_id = insert_test_event(pool, "prometheus_test", "event").await?;
    add_to_work_queue(pool, event_id, agent_name, 3).await?;
    
    // Generate Prometheus metrics output (this function should exist after implementation)
    let prometheus_output = generate_prometheus_metrics(pool).await?;
    
    // Verify Prometheus format
    assert!(prometheus_output.contains("sinex_queue_depth"), "Should contain queue depth metric");
    assert!(prometheus_output.contains("sinex_dequeue_latency_ms"), "Should contain latency metric");
    assert!(prometheus_output.contains("sinex_agent_lag_seconds"), "Should contain lag metric");
    
    // Verify agent labels
    assert!(prometheus_output.contains(&format!("agent_name=\"{}\"", agent_name)), 
           "Should include agent name labels");
    
    // Verify metric values are numeric
    let lines: Vec<&str> = prometheus_output.lines().collect();
    let metric_lines: Vec<&str> = lines.iter()
        .filter(|line| !line.starts_with('#') && !line.is_empty())
        .copied()
        .collect();
    
    assert!(!metric_lines.is_empty(), "Should have actual metric values");
    
    for line in metric_lines {
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() >= 2 {
            let value = parts[1];
            assert!(value.parse::<f64>().is_ok(), "Metric value should be numeric: {}", value);
        }
    }
    
    Ok(())
}

#[sinex_test]
async fn test_metrics_update_frequency(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();
    // Test that metrics are updated efficiently without excessive database queries
    
    let agent_name = "frequency-test-agent";
    create_test_agent(pool, agent_name).await?;
    
    // Create multiple events
    for i in 0..5 {
        let event_id = insert_test_event(pool, "frequency_test", &format!("event_{}", i)).await?;
        add_to_work_queue(pool, event_id, agent_name, 3).await?;
    }
    
    // Measure time for metrics calculation
    let start_time = std::time::Instant::now();
    
    // Calculate all metrics (should be efficient)
    let _queue_metrics = calculate_queue_depth_metrics(pool).await?;
    let _latency_metrics = calculate_dequeue_latency_metrics(pool).await?;
    let _lag_metrics = calculate_per_agent_lag_metrics(pool).await?;
    
    let duration = start_time.elapsed();
    
    // Should complete quickly (under 100ms for small dataset)
    assert!(duration.as_millis() < 100, "Metrics calculation should be efficient: {:?}", duration);
    
    Ok(())
}

// Helper functions and types

#[derive(Debug)]
pub struct DequeueLatencyMetric {
    pub agent_name: String,
    pub avg_dequeue_latency_ms: f64,
}

#[derive(Debug)]
pub struct AgentLagMetric {
    pub agent_name: String,
    pub max_lag_seconds: f64,
}

// Functions that should exist after implementation

// Removed local implementation - using the one from sinex_db::queries now

async fn calculate_dequeue_latency_metrics(pool: &PgPool) -> Result<Vec<DequeueLatencyMetric>> {
    // This should calculate average and max dequeue latency per agent
    let records = sqlx::query!(
        r#"
        SELECT 
            target_agent_name as agent_name,
            AVG(EXTRACT(EPOCH FROM (last_attempt_ts - created_at)) * 1000)::float8 as "avg_latency_ms!"
        FROM sinex_schemas.work_queue 
        WHERE last_attempt_ts IS NOT NULL
        AND created_at IS NOT NULL
        GROUP BY target_agent_name
        "#
    )
    .fetch_all(pool)
    .await?;
    
    let metrics = records.into_iter()
        .map(|r| DequeueLatencyMetric {
            agent_name: r.agent_name,
            avg_dequeue_latency_ms: r.avg_latency_ms,
        })
        .collect();
    
    Ok(metrics)
}

async fn calculate_per_agent_lag_metrics(pool: &PgPool) -> Result<Vec<AgentLagMetric>> {
    // This should calculate how far behind each agent is based on oldest pending event  
    // Use work queue created_at instead of event ts_ingest for lag calculation
    let records = sqlx::query!(
        r#"
        SELECT 
            target_agent_name as agent_name,
            MAX(EXTRACT(EPOCH FROM (now() - created_at)))::float8 as "max_lag_seconds!"
        FROM sinex_schemas.work_queue
        WHERE status IN ('pending', 'failed_retryable')
        GROUP BY target_agent_name
        "#
    )
    .fetch_all(pool)
    .await?;
    
    let metrics = records.into_iter()
        .map(|r| AgentLagMetric {
            agent_name: r.agent_name,
            max_lag_seconds: r.max_lag_seconds,
        })
        .collect();
    
    Ok(metrics)
}

async fn generate_prometheus_metrics(pool: &PgPool) -> Result<String> {
    // This should generate Prometheus-formatted metrics
    let queue_metrics = calculate_queue_depth_metrics(pool).await?;
    let latency_metrics = calculate_dequeue_latency_metrics(pool).await?;
    let lag_metrics = calculate_per_agent_lag_metrics(pool).await?;
    
    let mut output = String::new();
    
    // Queue depth metrics
    output.push_str("# HELP sinex_queue_depth Number of pending items in work queue per agent\n");
    output.push_str("# TYPE sinex_queue_depth gauge\n");
    for metric in queue_metrics {
        output.push_str(&format!(
            "sinex_queue_depth{{agent_name=\"{}\"}} {}\n",
            metric.agent_name, metric.queue_depth
        ));
    }
    
    // Dequeue latency metrics
    output.push_str("# HELP sinex_dequeue_latency_ms Average time from queue insertion to processing start\n");
    output.push_str("# TYPE sinex_dequeue_latency_ms gauge\n");
    for metric in latency_metrics {
        output.push_str(&format!(
            "sinex_dequeue_latency_ms{{agent_name=\"{}\"}} {:.2}\n",
            metric.agent_name, metric.avg_dequeue_latency_ms
        ));
    }
    
    // Agent lag metrics
    output.push_str("# HELP sinex_agent_lag_seconds Maximum age of oldest pending event per agent\n");
    output.push_str("# TYPE sinex_agent_lag_seconds gauge\n");
    for metric in lag_metrics {
        output.push_str(&format!(
            "sinex_agent_lag_seconds{{agent_name=\"{}\"}} {:.2}\n",
            metric.agent_name, metric.max_lag_seconds
        ));
    }
    
    Ok(output)
}

// Test helper functions

async fn create_test_agent(pool: &PgPool, agent_name: &str) -> TestResult {
    sqlx::query!(
        r#"
        INSERT INTO sinex_schemas.agent_manifests 
        (agent_name, version, status, agent_type, registered_at, updated_at)
        VALUES ($1, '1.0.0', 'running', 'test', now(), now())
        ON CONFLICT (agent_name) DO NOTHING
        "#,
        agent_name
    )
    .execute(pool)
    .await?;
    Ok(())
}

async fn insert_test_event(pool: &PgPool, source: &str, test_data: &str) -> Result<Ulid> {
    let payload = json!({
        "test": test_data,
        "source": source
    });
    
    let event = insert_raw_event(pool,
        source,
        "test_event",
        "test_host",
        payload,
        None,
        Some("1.0.0"),
        None,
    ).await?;
    
    Ok(event.id)
}
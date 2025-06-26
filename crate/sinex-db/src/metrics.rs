// Queue metrics implementation for Prometheus exposition
use anyhow::Result;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

/// Queue depth metric per agent
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueDepthMetric {
    pub agent_name: String,
    pub queue_depth: i64,
}

/// Dequeue latency metrics per agent
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DequeueLatencyMetric {
    pub agent_name: String,
    pub avg_dequeue_latency_ms: f64,
    pub max_dequeue_latency_ms: f64,
    pub p50_dequeue_latency_ms: f64,
    pub p95_dequeue_latency_ms: f64,
}

/// Agent lag metrics (how far behind processing)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentLagMetric {
    pub agent_name: String,
    pub max_lag_seconds: f64,
    pub avg_lag_seconds: f64,
    pub oldest_pending_seconds: f64,
}

/// Combined metrics for Prometheus exposition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueMetrics {
    pub queue_depth: Vec<QueueDepthMetric>,
    pub dequeue_latency: Vec<DequeueLatencyMetric>,
    pub agent_lag: Vec<AgentLagMetric>,
    pub total_pending_items: i64,
    pub total_processing_items: i64,
    pub total_failed_items: i64,
}

/// Calculate queue depth metrics per agent
/// Counts pending and retryable items
pub async fn calculate_queue_depth_metrics(pool: DbPoolRef) -> Result<Vec<QueueDepthMetric>> {
    let records = sqlx::query!(
        r#"
        SELECT 
            target_agent_name as agent_name,
            COUNT(*) as queue_depth
        FROM sinex_schemas.work_queue 
        WHERE status IN ('pending', 'failed_retryable')
        GROUP BY target_agent_name
        ORDER BY target_agent_name
        "#
    )
    .fetch_all(pool)
    .await?;
    
    let metrics = records.into_iter()
        .map(|r| QueueDepthMetric {
            agent_name: r.agent_name,
            queue_depth: r.queue_depth.unwrap_or(0),
        })
        .collect();
    
    Ok(metrics)
}

/// Calculate dequeue latency metrics per agent  
/// Measures time from queue insertion to processing start
pub async fn calculate_dequeue_latency_metrics(pool: DbPoolRef) -> Result<Vec<DequeueLatencyMetric>> {
    let records = sqlx::query!(
        r#"
        SELECT 
            target_agent_name as agent_name,
            AVG(EXTRACT(EPOCH FROM (last_attempt_ts - created_at)) * 1000)::float8 as "avg_latency_ms!",
            MAX(EXTRACT(EPOCH FROM (last_attempt_ts - created_at)) * 1000)::float8 as "max_latency_ms!",
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (last_attempt_ts - created_at)) * 1000)::float8 as "p50_latency_ms!",
            PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (last_attempt_ts - created_at)) * 1000)::float8 as "p95_latency_ms!"
        FROM sinex_schemas.work_queue 
        WHERE last_attempt_ts IS NOT NULL
        AND created_at IS NOT NULL
        AND last_attempt_ts >= created_at
        GROUP BY target_agent_name
        ORDER BY target_agent_name
        "#
    )
    .fetch_all(pool)
    .await?;
    
    let metrics = records.into_iter()
        .map(|r| DequeueLatencyMetric {
            agent_name: r.agent_name,
            avg_dequeue_latency_ms: r.avg_latency_ms.max(0.0),
            max_dequeue_latency_ms: r.max_latency_ms.max(0.0),
            p50_dequeue_latency_ms: r.p50_latency_ms.max(0.0),
            p95_dequeue_latency_ms: r.p95_latency_ms.max(0.0),
        })
        .collect();
    
    Ok(metrics)
}

/// Calculate per-agent lag metrics
/// Shows how far behind each agent is in processing
pub async fn calculate_per_agent_lag_metrics(pool: DbPoolRef) -> Result<Vec<AgentLagMetric>> {
    let records = sqlx::query!(
        r#"
        SELECT 
            target_agent_name as agent_name,
            MAX(EXTRACT(EPOCH FROM (now() - created_at)))::float8 as "max_lag_seconds!",
            AVG(EXTRACT(EPOCH FROM (now() - created_at)))::float8 as "avg_lag_seconds!",
            MAX(EXTRACT(EPOCH FROM (now() - created_at))) FILTER (WHERE status IN ('pending', 'failed_retryable'))::float8 as "oldest_pending_seconds!"
        FROM sinex_schemas.work_queue
        WHERE status IN ('pending', 'failed_retryable', 'processing')
        GROUP BY target_agent_name
        ORDER BY target_agent_name
        "#
    )
    .fetch_all(pool)
    .await?;
    
    let metrics = records.into_iter()
        .map(|r| AgentLagMetric {
            agent_name: r.agent_name,
            max_lag_seconds: r.max_lag_seconds.max(0.0),
            avg_lag_seconds: r.avg_lag_seconds.max(0.0),
            oldest_pending_seconds: r.oldest_pending_seconds.max(0.0),
        })
        .collect();
    
    Ok(metrics)
}

/// Calculate overall queue statistics
pub async fn calculate_overall_queue_stats(pool: DbPoolRef) -> Result<(i64, i64, i64)> {
    let stats = sqlx::query!(
        r#"
        SELECT
            COUNT(*) FILTER (WHERE status IN ('pending', 'failed_retryable')) as pending_count,
            COUNT(*) FILTER (WHERE status = 'processing') as processing_count,
            COUNT(*) FILTER (WHERE status = 'failed') as failed_count
        FROM sinex_schemas.work_queue
        "#
    )
    .fetch_one(pool)
    .await?;
    
    Ok((
        stats.pending_count.unwrap_or(0),
        stats.processing_count.unwrap_or(0),
        stats.failed_count.unwrap_or(0),
    ))
}

/// Calculate all queue metrics
pub async fn calculate_all_queue_metrics(pool: DbPoolRef) -> Result<QueueMetrics> {
    let (queue_depth, dequeue_latency, agent_lag, (total_pending, total_processing, total_failed)) = tokio::try_join!(
        calculate_queue_depth_metrics(pool),
        calculate_dequeue_latency_metrics(pool),
        calculate_per_agent_lag_metrics(pool),
        calculate_overall_queue_stats(pool)
    )?;
    
    Ok(QueueMetrics {
        queue_depth,
        dequeue_latency,
        agent_lag,
        total_pending_items: total_pending,
        total_processing_items: total_processing,
        total_failed_items: total_failed,
    })
}

/// Generate Prometheus-formatted metrics string
pub fn format_prometheus_metrics(metrics: &QueueMetrics) -> String {
    let mut output = String::new();
    
    // Queue depth metrics
    output.push_str("# HELP sinex_queue_depth Number of pending items in work queue per agent\n");
    output.push_str("# TYPE sinex_queue_depth gauge\n");
    for metric in &metrics.queue_depth {
        output.push_str(&format!(
            "sinex_queue_depth{{agent_name=\"{}\"}} {}\n",
            metric.agent_name, metric.queue_depth
        ));
    }
    
    // Overall queue stats
    output.push_str("# HELP sinex_total_pending_items Total number of pending work queue items\n");
    output.push_str("# TYPE sinex_total_pending_items gauge\n");
    output.push_str(&format!("sinex_total_pending_items {}\n", metrics.total_pending_items));
    
    output.push_str("# HELP sinex_total_processing_items Total number of processing work queue items\n");
    output.push_str("# TYPE sinex_total_processing_items gauge\n");
    output.push_str(&format!("sinex_total_processing_items {}\n", metrics.total_processing_items));
    
    output.push_str("# HELP sinex_total_failed_items Total number of failed work queue items\n");
    output.push_str("# TYPE sinex_total_failed_items gauge\n");
    output.push_str(&format!("sinex_total_failed_items {}\n", metrics.total_failed_items));
    
    // Dequeue latency metrics
    output.push_str("# HELP sinex_dequeue_latency_ms Average time from queue insertion to processing start\n");
    output.push_str("# TYPE sinex_dequeue_latency_ms gauge\n");
    for metric in &metrics.dequeue_latency {
        output.push_str(&format!(
            "sinex_dequeue_latency_ms{{agent_name=\"{}\",quantile=\"avg\"}} {:.2}\n",
            metric.agent_name, metric.avg_dequeue_latency_ms
        ));
        output.push_str(&format!(
            "sinex_dequeue_latency_ms{{agent_name=\"{}\",quantile=\"max\"}} {:.2}\n",
            metric.agent_name, metric.max_dequeue_latency_ms
        ));
        output.push_str(&format!(
            "sinex_dequeue_latency_ms{{agent_name=\"{}\",quantile=\"0.5\"}} {:.2}\n",
            metric.agent_name, metric.p50_dequeue_latency_ms
        ));
        output.push_str(&format!(
            "sinex_dequeue_latency_ms{{agent_name=\"{}\",quantile=\"0.95\"}} {:.2}\n",
            metric.agent_name, metric.p95_dequeue_latency_ms
        ));
    }
    
    // Agent lag metrics
    output.push_str("# HELP sinex_agent_lag_seconds How far behind each agent is in processing\n");
    output.push_str("# TYPE sinex_agent_lag_seconds gauge\n");
    for metric in &metrics.agent_lag {
        output.push_str(&format!(
            "sinex_agent_lag_seconds{{agent_name=\"{}\",stat=\"max\"}} {:.2}\n",
            metric.agent_name, metric.max_lag_seconds
        ));
        output.push_str(&format!(
            "sinex_agent_lag_seconds{{agent_name=\"{}\",stat=\"avg\"}} {:.2}\n",
            metric.agent_name, metric.avg_lag_seconds
        ));
        output.push_str(&format!(
            "sinex_agent_lag_seconds{{agent_name=\"{}\",stat=\"oldest_pending\"}} {:.2}\n",
            metric.agent_name, metric.oldest_pending_seconds
        ));
    }
    
    output
}

/// Generate Prometheus metrics and return formatted string
pub async fn generate_prometheus_metrics(pool: DbPoolRef) -> Result<String> {
    let metrics = calculate_all_queue_metrics(pool).await?;
    Ok(format_prometheus_metrics(&metrics))
}
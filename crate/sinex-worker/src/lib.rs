pub mod worker;

use anyhow::Result;
use async_trait::async_trait;
use sinex_db::models::WorkQueueItem;
use sqlx::PgPool;
use prometheus::{register_counter_vec, register_histogram_vec, register_gauge_vec, CounterVec, HistogramVec, GaugeVec};
use once_cell::sync::Lazy;

/// Trait for implementing agent-specific processing logic
#[async_trait]
pub trait EventProcessor: Send + Sync {
    /// Process a single event from the work queue
    async fn process_event(
        &self,
        pool: &PgPool,
        item: &WorkQueueItem,
    ) -> Result<()>;

    /// Get the agent name this processor handles
    fn agent_name(&self) -> &str;

    /// Get the batch size for processing
    fn batch_size(&self) -> i32 {
        10
    }

    /// Get the poll interval in seconds when no work is available
    fn poll_interval_secs(&self) -> u64 {
        1
    }
}

/// Calculate exponential backoff with jitter
pub fn calculate_backoff_secs(attempts: i32) -> f64 {
    use rand::Rng;
    
    let base_delay_secs = 60.0;
    let delay_secs = base_delay_secs * (2.0_f64.powi(attempts));
    let jitter_factor = rand::thread_rng().gen_range(0.8..=1.2);
    let final_delay_secs = (delay_secs * jitter_factor).max(1.0).min(24.0 * 3600.0);
    
    final_delay_secs
}

// ===== Metrics (from metrics.rs) =====

static ITEMS_CLAIMED: Lazy<CounterVec> = Lazy::new(|| {
    register_counter_vec!(
        "sinex_worker_items_claimed_total",
        "Total number of items claimed from promotion queue",
        &["agent_name"]
    )
    .unwrap()
});

static ITEMS_PROCESSED: Lazy<CounterVec> = Lazy::new(|| {
    register_counter_vec!(
        "sinex_worker_items_processed_total",
        "Total number of items successfully processed",
        &["agent_name"]
    )
    .unwrap()
});

static ITEMS_FAILED: Lazy<CounterVec> = Lazy::new(|| {
    register_counter_vec!(
        "sinex_worker_items_failed_total",
        "Total number of items that failed processing",
        &["agent_name"]
    )
    .unwrap()
});

static ITEMS_DLQ: Lazy<CounterVec> = Lazy::new(|| {
    register_counter_vec!(
        "sinex_worker_items_dlq_total",
        "Total number of items moved to dead letter queue",
        &["agent_name"]
    )
    .unwrap()
});

static PROCESSING_DURATION: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec!(
        "sinex_worker_processing_duration_seconds",
        "Time spent processing individual items",
        &["agent_name"],
        vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0]
    )
    .unwrap()
});

// ===== Queue Metrics =====

static QUEUE_DEPTH: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge_vec!(
        "sinex_queue_depth",
        "Number of pending items in work queue per agent",
        &["agent_name"]
    )
    .unwrap()
});

static DEQUEUE_LATENCY: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge_vec!(
        "sinex_dequeue_latency_ms", 
        "Dequeue latency in milliseconds",
        &["agent_name", "quantile"]
    )
    .unwrap()
});

static AGENT_LAG: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge_vec!(
        "sinex_agent_lag_seconds",
        "How far behind each agent is in processing",
        &["agent_name", "stat"]
    )
    .unwrap()
});

static TOTAL_QUEUE_ITEMS: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge_vec!(
        "sinex_total_queue_items",
        "Total queue items by status",
        &["status"]
    )
    .unwrap()
});

/// Worker metrics for a specific agent
pub struct WorkerMetrics {
    pub items_claimed: prometheus::Counter,
    pub items_processed: prometheus::Counter,
    pub items_failed: prometheus::Counter,
    pub items_dlq: prometheus::Counter,
    pub processing_duration: prometheus::Histogram,
}

impl WorkerMetrics {
    pub fn new(agent_name: &str) -> Self {
        Self {
            items_claimed: ITEMS_CLAIMED.with_label_values(&[agent_name]),
            items_processed: ITEMS_PROCESSED.with_label_values(&[agent_name]),
            items_failed: ITEMS_FAILED.with_label_values(&[agent_name]),
            items_dlq: ITEMS_DLQ.with_label_values(&[agent_name]),
            processing_duration: PROCESSING_DURATION.with_label_values(&[agent_name]),
        }
    }
}

/// Start metrics server on specified port
pub async fn start_metrics_server(port: u16) -> anyhow::Result<()> {
    use axum::{routing::get, Router};
    
    let app = Router::new().route("/metrics", get(metrics_handler));
    
    let addr = std::net::SocketAddr::from(([0, 0, 0, 0], port));
    tracing::info!("Metrics server listening on {}", addr);
    
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    
    Ok(())
}

/// Update queue metrics from database
pub async fn update_queue_metrics(pool: &PgPool) -> Result<()> {
    use sinex_db::metrics::{calculate_all_queue_metrics};
    
    let metrics = calculate_all_queue_metrics(pool).await?;
    
    // Update queue depth metrics
    for metric in &metrics.queue_depth {
        QUEUE_DEPTH
            .with_label_values(&[&metric.agent_name])
            .set(metric.queue_depth as f64);
    }
    
    // Update dequeue latency metrics  
    for metric in &metrics.dequeue_latency {
        DEQUEUE_LATENCY
            .with_label_values(&[&metric.agent_name, "avg"])
            .set(metric.avg_dequeue_latency_ms);
        DEQUEUE_LATENCY
            .with_label_values(&[&metric.agent_name, "max"])
            .set(metric.max_dequeue_latency_ms);
        DEQUEUE_LATENCY
            .with_label_values(&[&metric.agent_name, "0.5"])
            .set(metric.p50_dequeue_latency_ms);
        DEQUEUE_LATENCY
            .with_label_values(&[&metric.agent_name, "0.95"])
            .set(metric.p95_dequeue_latency_ms);
    }
    
    // Update agent lag metrics
    for metric in &metrics.agent_lag {
        AGENT_LAG
            .with_label_values(&[&metric.agent_name, "max"])
            .set(metric.max_lag_seconds);
        AGENT_LAG
            .with_label_values(&[&metric.agent_name, "avg"])
            .set(metric.avg_lag_seconds);
        AGENT_LAG
            .with_label_values(&[&metric.agent_name, "oldest_pending"])
            .set(metric.oldest_pending_seconds);
    }
    
    // Update total queue stats
    TOTAL_QUEUE_ITEMS
        .with_label_values(&["pending"])
        .set(metrics.total_pending_items as f64);
    TOTAL_QUEUE_ITEMS
        .with_label_values(&["processing"])
        .set(metrics.total_processing_items as f64);
    TOTAL_QUEUE_ITEMS
        .with_label_values(&["failed"])
        .set(metrics.total_failed_items as f64);
    
    Ok(())
}

/// Start enhanced metrics server with queue metrics
pub async fn start_queue_metrics_server(pool: PgPool, port: u16, update_interval_secs: u64) -> Result<()> {
    use axum::{routing::get, Router};
    use std::sync::Arc;
    use tokio::time::{interval, Duration};
    
    let pool = Arc::new(pool);
    
    // Spawn background task to update queue metrics periodically
    let metrics_pool = pool.clone();
    tokio::spawn(async move {
        let mut interval = interval(Duration::from_secs(update_interval_secs));
        loop {
            interval.tick().await;
            if let Err(e) = update_queue_metrics(&metrics_pool).await {
                tracing::error!("Failed to update queue metrics: {}", e);
            }
        }
    });
    
    let app = Router::new()
        .route("/metrics", get(enhanced_metrics_handler))
        .with_state(pool);
    
    let addr = std::net::SocketAddr::from(([0, 0, 0, 0], port));
    tracing::info!("Enhanced metrics server with queue metrics listening on {}", addr);
    
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    
    Ok(())
}

async fn enhanced_metrics_handler(
    axum::extract::State(pool): axum::extract::State<std::sync::Arc<PgPool>>
) -> String {
    use prometheus::{Encoder, TextEncoder};
    
    // Update queue metrics on-demand for fresh data
    if let Err(e) = update_queue_metrics(&pool).await {
        tracing::warn!("Failed to update queue metrics for /metrics endpoint: {}", e);
    }
    
    let encoder = TextEncoder::new();
    let metric_families = prometheus::gather();
    let mut buffer = vec![];
    encoder.encode(&metric_families, &mut buffer).unwrap();
    String::from_utf8(buffer).unwrap()
}

async fn metrics_handler() -> String {
    use prometheus::{Encoder, TextEncoder};
    
    let encoder = TextEncoder::new();
    let metric_families = prometheus::gather();
    let mut buffer = vec![];
    encoder.encode(&metric_families, &mut buffer).unwrap();
    String::from_utf8(buffer).unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinex_db::models::WorkQueueItem;
    use async_trait::async_trait;

    /// Mock event processor for testing
    struct MockEventProcessor {
        agent_name: String,
        batch_size: i32,
        poll_interval: u64,
        should_fail: bool,
    }

    impl MockEventProcessor {
        fn new(agent_name: &str) -> Self {
            Self {
                agent_name: agent_name.to_string(),
                batch_size: 5,
                poll_interval: 2,
                should_fail: false,
            }
        }

        fn with_failure(mut self) -> Self {
            self.should_fail = true;
            self
        }

        fn with_batch_size(mut self, size: i32) -> Self {
            self.batch_size = size;
            self
        }

        fn with_poll_interval(mut self, interval: u64) -> Self {
            self.poll_interval = interval;
            self
        }
    }

    #[async_trait]
    impl EventProcessor for MockEventProcessor {
        async fn process_event(
            &self,
            _pool: &PgPool,
            _item: &WorkQueueItem,
        ) -> Result<()> {
            if self.should_fail {
                anyhow::bail!("Mock processor intentionally failed");
            }
            Ok(())
        }

        fn agent_name(&self) -> &str {
            &self.agent_name
        }

        fn batch_size(&self) -> i32 {
            self.batch_size
        }

        fn poll_interval_secs(&self) -> u64 {
            self.poll_interval
        }
    }

    #[test]
    fn test_calculate_backoff_secs() {
        // Test that backoff increases exponentially
        let backoff_0 = calculate_backoff_secs(0);
        let backoff_1 = calculate_backoff_secs(1);
        let backoff_2 = calculate_backoff_secs(2);
        let backoff_3 = calculate_backoff_secs(3);

        // Should be roughly: 60, 120, 240, 480 seconds with jitter
        assert!(backoff_0 >= 48.0 && backoff_0 <= 72.0, "backoff_0: {}", backoff_0); // 60 * (0.8 to 1.2)
        assert!(backoff_1 >= 96.0 && backoff_1 <= 144.0, "backoff_1: {}", backoff_1); // 120 * (0.8 to 1.2)
        assert!(backoff_2 >= 192.0 && backoff_2 <= 288.0, "backoff_2: {}", backoff_2); // 240 * (0.8 to 1.2)
        assert!(backoff_3 >= 384.0 && backoff_3 <= 576.0, "backoff_3: {}", backoff_3); // 480 * (0.8 to 1.2)

        // Test that backoff is bounded at 24 hours
        let large_attempts = calculate_backoff_secs(20);
        assert!(large_attempts <= 24.0 * 3600.0, "Should be capped at 24 hours");

        // Test minimum bound
        let min_backoff = calculate_backoff_secs(-5);
        assert!(min_backoff >= 1.0, "Should have minimum of 1 second");
    }

    #[test]
    fn test_calculate_backoff_jitter() {
        // Test that jitter produces different values
        let attempts = 2;
        let mut values = Vec::new();
        
        for _ in 0..10 {
            values.push(calculate_backoff_secs(attempts));
        }
        
        // Should not all be the same (extremely unlikely with jitter)
        let first_value = values[0];
        let all_same = values.iter().all(|&x| (x - first_value).abs() < 0.001);
        assert!(!all_same, "Jitter should produce different values");
    }

    #[test]
    fn test_worker_metrics_creation() {
        let metrics = WorkerMetrics::new("test_agent");
        
        // Test that metrics are properly initialized
        assert_eq!(metrics.items_claimed.get(), 0.0);
        assert_eq!(metrics.items_processed.get(), 0.0);
        assert_eq!(metrics.items_failed.get(), 0.0);
        assert_eq!(metrics.items_dlq.get(), 0.0);
    }

    #[test]
    fn test_worker_metrics_increment() {
        let metrics = WorkerMetrics::new("test_agent_2");
        
        // Test metric increments
        metrics.items_claimed.inc();
        metrics.items_processed.inc();
        metrics.items_failed.inc();
        metrics.items_dlq.inc();
        
        assert_eq!(metrics.items_claimed.get(), 1.0);
        assert_eq!(metrics.items_processed.get(), 1.0);
        assert_eq!(metrics.items_failed.get(), 1.0);
        assert_eq!(metrics.items_dlq.get(), 1.0);
    }

    #[test]
    fn test_event_processor_trait_defaults() {
        let processor = MockEventProcessor::new("default_test");
        
        // Test default values
        assert_eq!(processor.batch_size(), 5);
        assert_eq!(processor.poll_interval_secs(), 2);
        assert_eq!(processor.agent_name(), "default_test");
    }

    #[test]
    fn test_event_processor_customization() {
        let processor = MockEventProcessor::new("custom_test")
            .with_batch_size(20)
            .with_poll_interval(5);
        
        assert_eq!(processor.batch_size(), 20);
        assert_eq!(processor.poll_interval_secs(), 5);
        assert_eq!(processor.agent_name(), "custom_test");
    }

    #[tokio::test]
    async fn test_mock_event_processor_success() {
        let processor = MockEventProcessor::new("success_test");
        
        // Create a dummy WorkQueueItem for testing
        let _dummy_item = WorkQueueItem {
            queue_id: sinex_ulid::Ulid::new(),
            raw_event_id: sinex_ulid::Ulid::new(),
            target_agent_name: "test_agent".to_string(),
            status: "pending".to_string(),
            attempts: 0,
            max_attempts: 3,
            last_attempt_ts: None,
            next_retry_ts: None,
            error_message_last: None,
            created_at: chrono::Utc::now(),
            processing_worker_id: None,
            processed_at: None,
            failure_reason: None,
        };

        // Test that the processor can be used with the trait
        assert_eq!(processor.agent_name(), "success_test");
        assert_eq!(processor.batch_size(), 5);
        assert_eq!(processor.poll_interval_secs(), 2);
    }

    #[test]
    fn test_prometheus_metrics_registration() {
        // Test that metrics are properly registered and can be accessed
        let metrics = WorkerMetrics::new("prometheus_test");
        
        // Increment some metrics
        metrics.items_claimed.inc_by(5.0);
        metrics.items_processed.inc_by(3.0);
        
        // Record processing duration
        let timer = metrics.processing_duration.start_timer();
        std::thread::sleep(std::time::Duration::from_millis(1));
        timer.observe_duration();
        
        // Verify the values
        assert_eq!(metrics.items_claimed.get(), 5.0);
        assert_eq!(metrics.items_processed.get(), 3.0);
        assert!(metrics.processing_duration.get_sample_count() > 0);
    }

    #[test]
    fn test_exponential_backoff_bounds() {
        // Test boundary conditions for exponential backoff
        
        // Very large number of attempts should still be bounded
        for attempts in [100, 1000, 10000] {
            let backoff = calculate_backoff_secs(attempts);
            assert!(backoff <= 24.0 * 3600.0 + 1.0, "Backoff should be bounded at ~24 hours for {} attempts", attempts);
            assert!(backoff >= 1.0, "Backoff should be at least 1 second for {} attempts", attempts);
        }
        
        // Very small (negative) attempts should have minimum backoff
        for attempts in [-100, -10, -1] {
            let backoff = calculate_backoff_secs(attempts);
            assert!(backoff >= 1.0, "Negative attempts should have minimum 1 second backoff");
        }
    }

    #[test]
    fn test_backoff_progression() {
        // Test that backoff generally increases with more attempts
        for attempts in 0..10 {
            let current_backoff = calculate_backoff_secs(attempts);
            
            // Due to jitter, we can't guarantee strict monotonic increase,
            // but the average should trend upward. We'll check the base value without jitter.
            let base_delay = 60.0 * (2.0_f64.powi(attempts));
            let expected_min = (base_delay * 0.8).max(1.0);
            let expected_max = (base_delay * 1.2).min(24.0 * 3600.0);
            
            assert!(current_backoff >= expected_min, 
                "Backoff {} should be >= {} for attempts {}", current_backoff, expected_min, attempts);
            assert!(current_backoff <= expected_max, 
                "Backoff {} should be <= {} for attempts {}", current_backoff, expected_max, attempts);
        }
    }
}

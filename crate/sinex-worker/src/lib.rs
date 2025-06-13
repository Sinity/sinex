pub mod worker;

use anyhow::Result;
use async_trait::async_trait;
use sinex_db::models::PromotionQueueItem;
use sqlx::PgPool;
use prometheus::{register_counter_vec, register_histogram_vec, CounterVec, HistogramVec};
use once_cell::sync::Lazy;

/// Trait for implementing agent-specific processing logic
#[async_trait]
pub trait EventProcessor: Send + Sync {
    /// Process a single event from the promotion queue
    async fn process_event(
        &self,
        pool: &PgPool,
        item: &PromotionQueueItem,
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

async fn metrics_handler() -> String {
    use prometheus::{Encoder, TextEncoder};
    
    let encoder = TextEncoder::new();
    let metric_families = prometheus::gather();
    let mut buffer = vec![];
    encoder.encode(&metric_families, &mut buffer).unwrap();
    String::from_utf8(buffer).unwrap()
}

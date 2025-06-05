use prometheus::{register_counter_vec, register_histogram_vec, CounterVec, HistogramVec};
use once_cell::sync::Lazy;

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
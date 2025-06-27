use prometheus::{CounterVec, GaugeVec, HistogramOpts, HistogramVec, Registry};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::info;

use crate::recovery::CollectorError;

/// Metrics for collector operations
pub struct CollectorMetrics {
    pub events_processed: CounterVec,
    pub events_failed: CounterVec,
    pub dlq_writes: CounterVec,
    pub processing_duration: HistogramVec,
    pub active_connections: GaugeVec,
    pub memory_usage: GaugeVec,
    pub event_lag: HistogramVec,
    pub sources_active: GaugeVec,
    pub config_reloads: CounterVec,
}

impl CollectorMetrics {
    pub fn new() -> Result<Self, prometheus::Error> {
        Ok(Self {
            events_processed: CounterVec::new(
                prometheus::Opts::new(
                    "sinex_events_processed_total",
                    "Total number of events processed",
                ),
                &["collector", "source", "event_type"],
            )?,

            events_failed: CounterVec::new(
                prometheus::Opts::new("sinex_events_failed_total", "Total number of failed events"),
                &["collector", "source", "error_type", "error_category"],
            )?,

            dlq_writes: CounterVec::new(
                prometheus::Opts::new(
                    "sinex_dlq_writes_total",
                    "Total number of events written to DLQ",
                ),
                &["collector", "reason"],
            )?,

            processing_duration: HistogramVec::new(
                HistogramOpts::new(
                    "sinex_event_processing_duration_seconds",
                    "Time taken to process events",
                )
                .buckets(vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0]),
                &["collector", "operation"],
            )?,

            active_connections: GaugeVec::new(
                prometheus::Opts::new("sinex_active_connections", "Number of active connections"),
                &["collector", "connection_type"],
            )?,

            memory_usage: GaugeVec::new(
                prometheus::Opts::new("sinex_memory_usage_bytes", "Memory usage in bytes"),
                &["collector", "component"],
            )?,

            event_lag: HistogramVec::new(
                HistogramOpts::new(
                    "sinex_event_lag_seconds",
                    "Time between event occurrence and processing",
                )
                .buckets(vec![0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0, 300.0]),
                &["collector", "source"],
            )?,

            sources_active: GaugeVec::new(
                prometheus::Opts::new("sinex_sources_active", "Number of active event sources"),
                &["collector", "source_type"],
            )?,

            config_reloads: CounterVec::new(
                prometheus::Opts::new(
                    "sinex_config_reloads_total",
                    "Number of configuration reloads",
                ),
                &["collector", "status"],
            )?,
        })
    }

    pub fn register_with(self, registry: &Registry) -> Result<Arc<Self>, prometheus::Error> {
        registry.register(Box::new(self.events_processed.clone()))?;
        registry.register(Box::new(self.events_failed.clone()))?;
        registry.register(Box::new(self.dlq_writes.clone()))?;
        registry.register(Box::new(self.processing_duration.clone()))?;
        registry.register(Box::new(self.active_connections.clone()))?;
        registry.register(Box::new(self.memory_usage.clone()))?;
        registry.register(Box::new(self.event_lag.clone()))?;
        registry.register(Box::new(self.sources_active.clone()))?;
        registry.register(Box::new(self.config_reloads.clone()))?;

        Ok(Arc::new(self))
    }
}

/// Event processing instrumentation
pub struct EventInstrumentation {
    pub collector: String,
    pub metrics: Arc<CollectorMetrics>,
}

impl EventInstrumentation {
    pub fn new(collector: String, metrics: Arc<CollectorMetrics>) -> Self {
        Self { collector, metrics }
    }

    pub fn record_event_processed(&self, source: &str, event_type: &str) {
        self.metrics
            .events_processed
            .with_label_values(&[&self.collector, source, event_type])
            .inc();
    }

    pub fn record_event_failed(&self, source: &str, error: &CollectorError) {
        let error_type = match error {
            CollectorError::Configuration { .. } => "configuration",
            CollectorError::Connection { .. } => "connection",
            CollectorError::EventProcessing { .. } => "processing",
            CollectorError::ResourceExhausted { .. } => "resource",
            CollectorError::Validation { .. } => "validation",
            CollectorError::Temporary { .. } => "temporary",
        };

        let category = match error.category() {
            crate::recovery::ErrorCategory::Retryable => "retryable",
            crate::recovery::ErrorCategory::Permanent => "permanent",
            crate::recovery::ErrorCategory::System => "system",
            crate::recovery::ErrorCategory::User => "user",
        };

        self.metrics
            .events_failed
            .with_label_values(&[&self.collector, source, error_type, category])
            .inc();
    }

    pub fn record_dlq_write(&self, reason: &str) {
        self.metrics
            .dlq_writes
            .with_label_values(&[&self.collector, reason])
            .inc();
    }

    pub fn record_processing_time(&self, operation: &str, duration: Duration) {
        self.metrics
            .processing_duration
            .with_label_values(&[&self.collector, operation])
            .observe(duration.as_secs_f64());
    }

    pub fn record_event_lag(&self, source: &str, lag: Duration) {
        self.metrics
            .event_lag
            .with_label_values(&[&self.collector, source])
            .observe(lag.as_secs_f64());
    }

    pub fn set_active_connections(&self, conn_type: &str, count: i64) {
        self.metrics
            .active_connections
            .with_label_values(&[&self.collector, conn_type])
            .set(count as f64);
    }

    pub fn set_sources_active(&self, source_type: &str, count: i64) {
        self.metrics
            .sources_active
            .with_label_values(&[&self.collector, source_type])
            .set(count as f64);
    }

    pub fn record_config_reload(&self, success: bool) {
        let status = if success { "success" } else { "failure" };
        self.metrics
            .config_reloads
            .with_label_values(&[&self.collector, status])
            .inc();
    }
}

/// Background metrics collector for system stats
pub async fn start_system_metrics_collector(
    metrics: Arc<CollectorMetrics>,
    collector_name: String,
) {
    let mut interval = tokio::time::interval(Duration::from_secs(10));

    loop {
        interval.tick().await;

        // Collect memory stats (if available)
        if let Ok(info) = sys_info::mem_info() {
            let total_kb = info.total * 1024; // Convert to bytes
            let avail_kb = info.avail * 1024;
            let used = total_kb - avail_kb;

            metrics
                .memory_usage
                .with_label_values(&[&collector_name, "used"])
                .set(used as f64);

            metrics
                .memory_usage
                .with_label_values(&[&collector_name, "total"])
                .set(total_kb as f64);
        }

        // Could add more system metrics here like CPU, disk, etc.
    }
}

/// Initialize observability with structured logging
pub fn init_observability(
    service_name: &str,
    log_level: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(log_level));

    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_target(true)
        .with_thread_ids(true)
        .with_line_number(true); // JSON formatting can be added later if needed

    use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

    tracing_subscriber::registry()
        .with(env_filter)
        .with(fmt_layer)
        .init();

    info!(service = %service_name, "Observability initialized");
    Ok(())
}

/// Timer for measuring operation durations
pub struct OperationTimer {
    start: Instant,
    operation: String,
    instrumentation: Arc<EventInstrumentation>,
}

impl OperationTimer {
    pub fn new(operation: String, instrumentation: Arc<EventInstrumentation>) -> Self {
        Self {
            start: Instant::now(),
            operation,
            instrumentation,
        }
    }
}

impl Drop for OperationTimer {
    fn drop(&mut self) {
        let duration = self.start.elapsed();
        self.instrumentation
            .record_processing_time(&self.operation, duration);
    }
}

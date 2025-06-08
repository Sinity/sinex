use prometheus::{
    CounterVec, GaugeVec, HistogramVec, Registry, HistogramOpts,
};
use std::sync::Arc;

/// Metrics for ingestor operations
pub struct IngestorMetrics {
    pub events_processed: CounterVec,
    pub events_failed: CounterVec,
    pub dlq_writes: CounterVec,
    pub processing_duration: HistogramVec,
    pub active_connections: GaugeVec,
    pub memory_usage: GaugeVec,
    pub event_lag: HistogramVec,
}

impl IngestorMetrics {
    pub fn new(_registry: &Registry) -> Result<Self, prometheus::Error> {
        Ok(Self {
            events_processed: CounterVec::new(
                prometheus::Opts::new(
                    "sinex_events_processed_total",
                    "Total number of events processed"
                ),
                &["ingestor", "source", "event_type"]
            )?,
            
            events_failed: CounterVec::new(
                prometheus::Opts::new(
                    "sinex_events_failed_total",
                    "Total number of failed events"
                ),
                &["ingestor", "source", "error_type", "error_category"]
            )?,
            
            dlq_writes: CounterVec::new(
                prometheus::Opts::new(
                    "sinex_dlq_writes_total",
                    "Total number of events written to DLQ"
                ),
                &["ingestor", "reason"]
            )?,
            
            processing_duration: HistogramVec::new(
                HistogramOpts::new(
                    "sinex_event_processing_duration_seconds",
                    "Time taken to process events"
                ).buckets(vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0]),
                &["ingestor", "operation"]
            )?,
            
            active_connections: GaugeVec::new(
                prometheus::Opts::new(
                    "sinex_active_connections",
                    "Number of active connections"
                ),
                &["ingestor", "connection_type"]
            )?,
            
            memory_usage: GaugeVec::new(
                prometheus::Opts::new(
                    "sinex_memory_usage_bytes",
                    "Memory usage in bytes"
                ),
                &["ingestor", "component"]
            )?,
            
            event_lag: HistogramVec::new(
                HistogramOpts::new(
                    "sinex_event_lag_seconds",
                    "Time between event occurrence and processing"
                ).buckets(vec![0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0, 300.0]),
                &["ingestor", "source"]
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
        Ok(Arc::new(self))
    }
}

/// Tracing context for operations
pub struct TraceContext {
    pub trace_id: String,
    pub span_id: String,
    pub parent_span_id: Option<String>,
}

impl TraceContext {
    pub fn new() -> Self {
        Self {
            trace_id: uuid::Uuid::new_v4().to_string(),
            span_id: uuid::Uuid::new_v4().to_string(),
            parent_span_id: None,
        }
    }
    
    pub fn child(&self) -> Self {
        Self {
            trace_id: self.trace_id.clone(),
            span_id: uuid::Uuid::new_v4().to_string(),
            parent_span_id: Some(self.span_id.clone()),
        }
    }
}

/// Structured logging setup - simplified without complex OpenTelemetry
pub fn init_observability(
    _service_name: &str,
    log_level: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    // Just use standard tracing setup
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(log_level));
    
    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_target(true)
        .with_thread_ids(true)
        .with_line_number(true);
    
    use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
    
    tracing_subscriber::registry()
        .with(env_filter)
        .with(fmt_layer)
        .init();
    
    Ok(())
}

/// Event processing instrumentation
pub struct EventInstrumentation {
    pub ingestor: String,
    pub metrics: Arc<IngestorMetrics>,
}

impl EventInstrumentation {
    pub fn new(ingestor: String, metrics: Arc<IngestorMetrics>) -> Self {
        Self { ingestor, metrics }
    }
    
    pub fn record_event_processed(&self, source: &str, event_type: &str) {
        self.metrics
            .events_processed
            .with_label_values(&[&self.ingestor, source, event_type])
            .inc();
    }
    
    pub fn record_event_failed(&self, source: &str, error: &crate::error_handling::IngestorError) {
        let error_type = match error {
            crate::error_handling::IngestorError::Configuration { .. } => "configuration",
            crate::error_handling::IngestorError::Connection { .. } => "connection",
            crate::error_handling::IngestorError::EventProcessing { .. } => "processing",
            crate::error_handling::IngestorError::ResourceExhausted { .. } => "resource",
            crate::error_handling::IngestorError::Validation { .. } => "validation",
            crate::error_handling::IngestorError::Temporary { .. } => "temporary",
        };
        
        let category = match error.category() {
            crate::error_handling::ErrorCategory::Retryable => "retryable",
            crate::error_handling::ErrorCategory::Permanent => "permanent",
            crate::error_handling::ErrorCategory::System => "system",
            crate::error_handling::ErrorCategory::User => "user",
        };
        
        self.metrics
            .events_failed
            .with_label_values(&[&self.ingestor, source, error_type, category])
            .inc();
    }
    
    pub fn record_processing_time(&self, operation: &str, duration: std::time::Duration) {
        self.metrics
            .processing_duration
            .with_label_values(&[&self.ingestor, operation])
            .observe(duration.as_secs_f64());
    }
    
    pub fn record_event_lag(&self, source: &str, lag: std::time::Duration) {
        self.metrics
            .event_lag
            .with_label_values(&[&self.ingestor, source])
            .observe(lag.as_secs_f64());
    }
}

/// Health check endpoint data
#[derive(Debug, serde::Serialize)]
pub struct HealthStatus {
    pub status: HealthState,
    pub version: String,
    pub uptime_seconds: u64,
    pub components: Vec<ComponentHealth>,
}

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum HealthState {
    Healthy,
    Degraded,
    Unhealthy,
}

#[derive(Debug, serde::Serialize)]
pub struct ComponentHealth {
    pub name: String,
    pub status: HealthState,
    pub message: Option<String>,
    pub last_check: chrono::DateTime<chrono::Utc>,
}

/// Background metrics collector
pub async fn start_metrics_collector(
    metrics: Arc<IngestorMetrics>,
    ingestor: String,
) {
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(10));
    
    loop {
        interval.tick().await;
        
        // Collect memory stats
        if let Some(usage) = memory_stats::memory_stats() {
            metrics
                .memory_usage
                .with_label_values(&[&ingestor, "physical"])
                .set(usage.physical_mem as f64);
                
            metrics
                .memory_usage
                .with_label_values(&[&ingestor, "virtual"])
                .set(usage.virtual_mem as f64);
        }
        
        // Could add more system metrics here
    }
}
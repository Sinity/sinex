//! Event Processing Metrics
//!
//! This module provides metrics for tracking event processing operations.

use once_cell::sync::Lazy;
use parking_lot::RwLock;
use prometheus::{Counter, Gauge, Histogram, IntGauge};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use crate::telemetry::metrics::registry::GlobalMetrics;

/// Event processing metrics collector
#[derive(Debug, Clone)]
pub struct EventMetrics {
    pub event_type: String,
    pub events_processed: Counter,
    pub processing_duration: Histogram,
    pub processing_errors: Counter,
    pub queue_depth: IntGauge,
    pub throughput: Gauge,
    pub labels: HashMap<String, String>,
}

impl EventMetrics {
    pub fn new(event_type: &str, labels: HashMap<String, String>) -> Self {
        let events_processed = Counter::with_opts(
            prometheus::Opts::new(
                "sinex_events_processed_total",
                "Total number of events processed",
            )
            .namespace("sinex")
            .subsystem("events")
            .const_labels(labels.clone()),
        )
        .unwrap();

        let processing_duration = Histogram::with_opts(
            prometheus::HistogramOpts::new(
                "sinex_event_processing_duration_seconds",
                "Event processing duration in seconds",
            )
            .namespace("sinex")
            .subsystem("events")
            .const_labels(labels.clone())
            .buckets(vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0]),
        )
        .unwrap();

        let processing_errors = Counter::with_opts(
            prometheus::Opts::new(
                "sinex_event_processing_errors_total",
                "Total number of event processing errors",
            )
            .namespace("sinex")
            .subsystem("events")
            .const_labels(labels.clone()),
        )
        .unwrap();

        let queue_depth = IntGauge::with_opts(
            prometheus::Opts::new("sinex_event_queue_depth", "Current depth of event queue")
                .namespace("sinex")
                .subsystem("events")
                .const_labels(labels.clone()),
        )
        .unwrap();

        let throughput = Gauge::with_opts(
            prometheus::Opts::new(
                "sinex_event_throughput_per_second",
                "Event processing throughput per second",
            )
            .namespace("sinex")
            .subsystem("events")
            .const_labels(labels.clone()),
        )
        .unwrap();

        // Register with global metrics
        GlobalMetrics::register_counter(&events_processed);
        GlobalMetrics::register_histogram(&processing_duration);
        GlobalMetrics::register_counter(&processing_errors);
        GlobalMetrics::register_gauge(&queue_depth);
        GlobalMetrics::register_gauge(&throughput);

        Self {
            event_type: event_type.to_string(),
            events_processed,
            processing_duration,
            processing_errors,
            queue_depth,
            throughput,
            labels,
        }
    }

    pub fn record_event_processed(&self, duration: std::time::Duration) {
        self.events_processed.inc();
        self.processing_duration.observe(duration.as_secs_f64());
    }

    pub fn record_error(&self, _error_type: &str) {
        self.processing_errors.inc();
    }

    pub fn update_queue_depth(&self, depth: i64) {
        self.queue_depth.set(depth);
    }

    pub fn update_throughput(&self, events_per_second: f64) {
        self.throughput.set(events_per_second);
    }
}

/// Event processing guard
pub struct EventProcessingGuard {
    metrics: Arc<EventMetrics>,
    start_time: Instant,
}

impl EventProcessingGuard {
    pub fn new(metrics: Arc<EventMetrics>) -> Self {
        Self {
            metrics,
            start_time: Instant::now(),
        }
    }

    pub fn record_error(self, error_type: &str) {
        let duration = self.start_time.elapsed();
        self.metrics
            .processing_duration
            .observe(duration.as_secs_f64());
        self.metrics.record_error(error_type);
    }
}

impl Drop for EventProcessingGuard {
    fn drop(&mut self) {
        let duration = self.start_time.elapsed();
        self.metrics.record_event_processed(duration);
    }
}

/// Global event metrics
static EVENT_METRICS: Lazy<Arc<RwLock<HashMap<String, Arc<EventMetrics>>>>> =
    Lazy::new(|| Arc::new(RwLock::new(HashMap::new())));

/// Get or create event metrics
pub fn get_event_metrics(event_type: &str, labels: HashMap<String, String>) -> Arc<EventMetrics> {
    let key = format!("event_{}", event_type);

    // Try to get existing metrics
    if let Some(metrics) = EVENT_METRICS.read().get(&key) {
        return metrics.clone();
    }

    // Create new metrics
    let metrics = Arc::new(EventMetrics::new(event_type, labels));
    EVENT_METRICS.write().insert(key, metrics.clone());

    metrics
}

/// Create an event processing guard for automatic metrics
pub fn track_event_processing(event_type: &str) -> EventProcessingGuard {
    let metrics = get_event_metrics(event_type, HashMap::new());
    EventProcessingGuard::new(metrics)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_event_metrics() {
        let metrics = get_event_metrics("test_event", HashMap::new());

        let guard = EventProcessingGuard::new(metrics.clone());
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        drop(guard);

        // Verify metrics were recorded
        assert!(metrics.events_processed.get() > 0.0);
    }
}

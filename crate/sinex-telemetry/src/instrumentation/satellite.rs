//! Satellite-Specific Metrics
//!
//! This module provides metrics for StatefulStreamProcessor implementations
//! with satellite-specific metrics including scan operations, checkpoint management, and error recovery.

use once_cell::sync::Lazy;
use parking_lot::RwLock;
use prometheus::{Counter, Gauge, Histogram, IntGauge};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use crate::registry::GlobalMetrics;

/// Satellite-specific metrics collector
#[derive(Debug, Clone)]
pub struct SatelliteMetrics {
    pub processor_name: String,
    pub processor_type: String,
    pub scans_completed: Counter,
    pub scan_duration: Histogram,
    pub scan_errors: Counter,
    pub checkpoints_saved: Counter,
    pub checkpoint_errors: Counter,
    pub events_discovered: Counter,
    pub events_processed: Counter,
    pub processing_lag: Gauge,
    pub active_scans: IntGauge,
    pub source_health: Gauge,
    pub labels: HashMap<String, String>,
}

impl SatelliteMetrics {
    pub fn new(
        processor_name: &str,
        processor_type: &str,
        labels: HashMap<String, String>,
    ) -> Self {
        let scans_completed = Counter::with_opts(
            prometheus::Opts::new(
                "sinex_satellite_scans_completed_total",
                "Total number of scans completed",
            )
            .namespace("sinex")
            .subsystem("satellite")
            .const_labels(labels.clone()),
        )
        .unwrap();

        let scan_duration = Histogram::with_opts(
            prometheus::HistogramOpts::new(
                "sinex_satellite_scan_duration_seconds",
                "Satellite scan duration in seconds",
            )
            .namespace("sinex")
            .subsystem("satellite")
            .const_labels(labels.clone())
            .buckets(vec![0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0, 300.0]),
        )
        .unwrap();

        let scan_errors = Counter::with_opts(
            prometheus::Opts::new(
                "sinex_satellite_scan_errors_total",
                "Total number of scan errors",
            )
            .namespace("sinex")
            .subsystem("satellite")
            .const_labels(labels.clone()),
        )
        .unwrap();

        let checkpoints_saved = Counter::with_opts(
            prometheus::Opts::new(
                "sinex_satellite_checkpoints_saved_total",
                "Total number of checkpoints saved",
            )
            .namespace("sinex")
            .subsystem("satellite")
            .const_labels(labels.clone()),
        )
        .unwrap();

        let checkpoint_errors = Counter::with_opts(
            prometheus::Opts::new(
                "sinex_satellite_checkpoint_errors_total",
                "Total number of checkpoint errors",
            )
            .namespace("sinex")
            .subsystem("satellite")
            .const_labels(labels.clone()),
        )
        .unwrap();

        let events_discovered = Counter::with_opts(
            prometheus::Opts::new(
                "sinex_satellite_events_discovered_total",
                "Total number of events discovered",
            )
            .namespace("sinex")
            .subsystem("satellite")
            .const_labels(labels.clone()),
        )
        .unwrap();

        let events_processed = Counter::with_opts(
            prometheus::Opts::new(
                "sinex_satellite_events_processed_total",
                "Total number of events processed",
            )
            .namespace("sinex")
            .subsystem("satellite")
            .const_labels(labels.clone()),
        )
        .unwrap();

        let processing_lag = Gauge::with_opts(
            prometheus::Opts::new(
                "sinex_satellite_processing_lag_seconds",
                "Processing lag in seconds",
            )
            .namespace("sinex")
            .subsystem("satellite")
            .const_labels(labels.clone()),
        )
        .unwrap();

        let active_scans = IntGauge::with_opts(
            prometheus::Opts::new(
                "sinex_satellite_active_scans",
                "Number of currently active scans",
            )
            .namespace("sinex")
            .subsystem("satellite")
            .const_labels(labels.clone()),
        )
        .unwrap();

        let source_health = Gauge::with_opts(
            prometheus::Opts::new(
                "sinex_satellite_source_health_score",
                "Health score of the satellite source (0-1)",
            )
            .namespace("sinex")
            .subsystem("satellite")
            .const_labels(labels.clone()),
        )
        .unwrap();

        // Register with global metrics
        GlobalMetrics::register_counter(&scans_completed);
        GlobalMetrics::register_histogram(&scan_duration);
        GlobalMetrics::register_counter(&scan_errors);
        GlobalMetrics::register_counter(&checkpoints_saved);
        GlobalMetrics::register_counter(&checkpoint_errors);
        GlobalMetrics::register_counter(&events_discovered);
        GlobalMetrics::register_counter(&events_processed);
        GlobalMetrics::register_gauge(&processing_lag);
        GlobalMetrics::register_gauge(&active_scans);
        GlobalMetrics::register_gauge(&source_health);

        Self {
            processor_name: processor_name.to_string(),
            processor_type: processor_type.to_string(),
            scans_completed,
            scan_duration,
            scan_errors,
            checkpoints_saved,
            checkpoint_errors,
            events_discovered,
            events_processed,
            processing_lag,
            active_scans,
            source_health,
            labels,
        }
    }

    pub fn record_scan_start(&self) {
        self.active_scans.inc();
    }

    pub fn record_scan_complete(&self, duration: std::time::Duration, events_found: u64) {
        self.scans_completed.inc();
        self.scan_duration.observe(duration.as_secs_f64());
        self.events_discovered.inc_by(events_found as f64);
        self.active_scans.dec();
    }

    pub fn record_scan_error(&self, _error_type: &str) {
        self.scan_errors.inc();
        self.active_scans.dec();
    }

    pub fn record_checkpoint_saved(&self) {
        self.checkpoints_saved.inc();
    }

    pub fn record_checkpoint_error(&self, _error_type: &str) {
        self.checkpoint_errors.inc();
    }

    pub fn record_event_processed(&self) {
        self.events_processed.inc();
    }

    pub fn update_processing_lag(&self, lag_seconds: f64) {
        self.processing_lag.set(lag_seconds);
    }

    pub fn update_source_health(&self, health_score: f64) {
        self.source_health.set(health_score.max(0.0).min(1.0));
    }
}

/// Satellite scan guard that automatically records metrics
pub struct SatelliteScanGuard {
    metrics: Arc<SatelliteMetrics>,
    start_time: Instant,
}

impl SatelliteScanGuard {
    pub fn new(metrics: Arc<SatelliteMetrics>) -> Self {
        metrics.record_scan_start();
        Self {
            metrics,
            start_time: Instant::now(),
        }
    }

    pub fn record_error(self, error_type: &str) {
        let duration = self.start_time.elapsed();
        self.metrics.scan_duration.observe(duration.as_secs_f64());
        self.metrics.record_scan_error(error_type);
    }

    pub fn complete_with_events(self, events_found: u64) {
        let duration = self.start_time.elapsed();
        self.metrics.record_scan_complete(duration, events_found);
    }
}

impl Drop for SatelliteScanGuard {
    fn drop(&mut self) {
        let duration = self.start_time.elapsed();
        self.metrics.record_scan_complete(duration, 0);
    }
}

/// Satellite health monitor
#[derive(Clone)]
pub struct SatelliteHealthMonitor {
    metrics: Arc<SatelliteMetrics>,
    last_scan_time: Option<Instant>,
    error_count: u64,
    recovery_count: u64,
}

impl SatelliteHealthMonitor {
    pub fn new(metrics: Arc<SatelliteMetrics>) -> Self {
        Self {
            metrics,
            last_scan_time: None,
            error_count: 0,
            recovery_count: 0,
        }
    }

    pub fn record_scan_completed(&mut self) {
        self.last_scan_time = Some(Instant::now());
        self.update_health_score();
    }

    pub fn record_error(&mut self) {
        self.error_count += 1;
        self.update_health_score();
    }

    pub fn record_recovery(&mut self) {
        self.recovery_count += 1;
        self.update_health_score();
    }

    fn update_health_score(&self) {
        let mut health_score = 1.0;

        // Reduce health based on recent errors
        if self.error_count > 0 {
            health_score -= (self.error_count as f64 * 0.1).min(0.8);
        }

        // Reduce health if no recent scans
        if let Some(last_scan) = self.last_scan_time {
            let time_since_last_scan = last_scan.elapsed().as_secs() as f64;
            if time_since_last_scan > 300.0 {
                // 5 minutes
                health_score -= 0.2;
            }
        } else {
            health_score -= 0.5; // No scans yet
        }

        // Increase health based on recovery success rate
        if self.recovery_count > 0 {
            let success_rate = self.recovery_count as f64 / self.recovery_count as f64;
            health_score += success_rate * 0.1;
        }

        // Clamp health score between 0 and 1
        health_score = health_score.max(0.0).min(1.0);

        self.metrics.update_source_health(health_score);
    }
}

/// Global satellite health monitors
static SATELLITE_HEALTH_MONITORS: Lazy<Arc<RwLock<HashMap<String, SatelliteHealthMonitor>>>> =
    Lazy::new(|| Arc::new(RwLock::new(HashMap::new())));

/// Get or create satellite health monitor
pub fn get_satellite_health_monitor(
    processor_name: &str,
    processor_type: &str,
) -> SatelliteHealthMonitor {
    let key = format!("{}::{}", processor_name, processor_type);

    // Try to get existing monitor
    if let Some(monitor) = SATELLITE_HEALTH_MONITORS.read().get(&key) {
        return monitor.clone();
    }

    // Create new monitor
    let metrics = get_satellite_metrics(processor_name, processor_type, HashMap::new());
    let monitor = SatelliteHealthMonitor::new(metrics);
    SATELLITE_HEALTH_MONITORS
        .write()
        .insert(key, monitor.clone());

    monitor
}

/// Global satellite metrics
static SATELLITE_METRICS: Lazy<Arc<RwLock<HashMap<String, Arc<SatelliteMetrics>>>>> =
    Lazy::new(|| Arc::new(RwLock::new(HashMap::new())));

/// Get or create satellite metrics
pub fn get_satellite_metrics(
    processor_name: &str,
    processor_type: &str,
    labels: HashMap<String, String>,
) -> Arc<SatelliteMetrics> {
    let key = format!("{}::{}", processor_name, processor_type);

    // Try to get existing metrics
    if let Some(metrics) = SATELLITE_METRICS.read().get(&key) {
        return metrics.clone();
    }

    // Create new metrics
    let metrics = Arc::new(SatelliteMetrics::new(
        processor_name,
        processor_type,
        labels,
    ));
    SATELLITE_METRICS.write().insert(key, metrics.clone());

    metrics
}

/// Create a satellite scan guard for automatic metrics
pub fn track_satellite_scan(processor_name: &str, processor_type: &str) -> SatelliteScanGuard {
    let metrics = get_satellite_metrics(processor_name, processor_type, HashMap::new());
    SatelliteScanGuard::new(metrics)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_satellite_metrics() {
        let metrics = get_satellite_metrics("test_satellite", "stream_processor", HashMap::new());

        let guard = SatelliteScanGuard::new(metrics.clone());
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        guard.complete_with_events(5);

        // Verify metrics were recorded
        assert!(metrics.scans_completed.get() > 0.0);
        assert!(metrics.events_discovered.get() >= 5.0);
    }

    #[tokio::test]
    async fn test_satellite_health_monitor() {
        let metrics = get_satellite_metrics("health_test", "stream_processor", HashMap::new());
        let mut monitor = SatelliteHealthMonitor::new(metrics);

        monitor.record_scan_completed();
        monitor.record_error();
        monitor.record_recovery();

        // Health score should be calculated
        // Just verify it doesn't panic
    }
}

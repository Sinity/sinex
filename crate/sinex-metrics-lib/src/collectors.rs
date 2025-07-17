//! Metrics Collection Infrastructure
//!
//! This module provides the core infrastructure for collecting and managing metrics
//! across the Sinex system.

use once_cell::sync::Lazy;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::interval;

/// Metric value types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MetricValue {
    Counter(f64),
    Gauge(f64),
    Histogram(Vec<f64>),
    Summary(SummaryValue),
}

/// Summary metric value
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SummaryValue {
    pub count: u64,
    pub sum: f64,
    // Use String keys for quantiles to avoid f64 Hash/Eq issues
    pub quantiles: HashMap<String, f64>, // quantile (as string) -> value
}

/// Metric types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum MetricType {
    Counter,
    Gauge,
    Histogram,
    Summary,
}

/// Individual metric entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricEntry {
    pub name: String,
    pub help: String,
    pub metric_type: MetricType,
    pub value: MetricValue,
    pub labels: HashMap<String, String>,
    pub timestamp: u64,
}

/// Metrics collector interface
pub trait MetricsCollector: Send + Sync {
    fn collect(&self) -> Vec<MetricEntry>;
    fn name(&self) -> &str;
}

/// System metrics collector
pub struct SystemMetricsCollector {
    name: String,
    #[allow(dead_code)] // TODO: Use collect_interval for scheduled collection
    collect_interval: Duration,
}

impl SystemMetricsCollector {
    pub fn new(name: String, collect_interval: Duration) -> Self {
        Self {
            name,
            collect_interval,
        }
    }

    fn collect_memory_metrics(&self) -> Vec<MetricEntry> {
        // TODO: Implement actual memory metrics collection
        vec![
            MetricEntry {
                name: "sinex_system_memory_usage_bytes".to_string(),
                help: "System memory usage in bytes".to_string(),
                metric_type: MetricType::Gauge,
                value: MetricValue::Gauge(get_system_memory_usage() as f64),
                labels: HashMap::new(),
                timestamp: current_timestamp(),
            },
            MetricEntry {
                name: "sinex_system_memory_available_bytes".to_string(),
                help: "System memory available in bytes".to_string(),
                metric_type: MetricType::Gauge,
                value: MetricValue::Gauge(get_system_memory_available() as f64),
                labels: HashMap::new(),
                timestamp: current_timestamp(),
            },
        ]
    }

    fn collect_cpu_metrics(&self) -> Vec<MetricEntry> {
        // TODO: Implement actual CPU metrics collection
        vec![
            MetricEntry {
                name: "sinex_system_cpu_usage_percent".to_string(),
                help: "System CPU usage percentage".to_string(),
                metric_type: MetricType::Gauge,
                value: MetricValue::Gauge(get_system_cpu_usage()),
                labels: HashMap::new(),
                timestamp: current_timestamp(),
            },
            MetricEntry {
                name: "sinex_system_load_average_1m".to_string(),
                help: "System load average over 1 minute".to_string(),
                metric_type: MetricType::Gauge,
                value: MetricValue::Gauge(get_system_load_average()),
                labels: HashMap::new(),
                timestamp: current_timestamp(),
            },
        ]
    }

    fn collect_disk_metrics(&self) -> Vec<MetricEntry> {
        // TODO: Implement actual disk metrics collection
        vec![
            MetricEntry {
                name: "sinex_system_disk_usage_bytes".to_string(),
                help: "System disk usage in bytes".to_string(),
                metric_type: MetricType::Gauge,
                value: MetricValue::Gauge(get_system_disk_usage() as f64),
                labels: HashMap::new(),
                timestamp: current_timestamp(),
            },
            MetricEntry {
                name: "sinex_system_disk_io_read_bytes_total".to_string(),
                help: "Total disk bytes read".to_string(),
                metric_type: MetricType::Counter,
                value: MetricValue::Counter(get_system_disk_read_bytes() as f64),
                labels: HashMap::new(),
                timestamp: current_timestamp(),
            },
            MetricEntry {
                name: "sinex_system_disk_io_write_bytes_total".to_string(),
                help: "Total disk bytes written".to_string(),
                metric_type: MetricType::Counter,
                value: MetricValue::Counter(get_system_disk_write_bytes() as f64),
                labels: HashMap::new(),
                timestamp: current_timestamp(),
            },
        ]
    }

    fn collect_network_metrics(&self) -> Vec<MetricEntry> {
        // TODO: Implement actual network metrics collection
        vec![
            MetricEntry {
                name: "sinex_system_network_bytes_received_total".to_string(),
                help: "Total network bytes received".to_string(),
                metric_type: MetricType::Counter,
                value: MetricValue::Counter(get_system_network_bytes_received() as f64),
                labels: HashMap::new(),
                timestamp: current_timestamp(),
            },
            MetricEntry {
                name: "sinex_system_network_bytes_sent_total".to_string(),
                help: "Total network bytes sent".to_string(),
                metric_type: MetricType::Counter,
                value: MetricValue::Counter(get_system_network_bytes_sent() as f64),
                labels: HashMap::new(),
                timestamp: current_timestamp(),
            },
        ]
    }
}

impl MetricsCollector for SystemMetricsCollector {
    fn collect(&self) -> Vec<MetricEntry> {
        let mut metrics = Vec::new();

        metrics.extend(self.collect_memory_metrics());
        metrics.extend(self.collect_cpu_metrics());
        metrics.extend(self.collect_disk_metrics());
        metrics.extend(self.collect_network_metrics());

        metrics
    }

    fn name(&self) -> &str {
        &self.name
    }
}

/// Process metrics collector
pub struct ProcessMetricsCollector {
    name: String,
    pid: u32,
}

impl ProcessMetricsCollector {
    pub fn new(name: String) -> Self {
        Self {
            name,
            pid: std::process::id(),
        }
    }

    fn collect_process_metrics(&self) -> Vec<MetricEntry> {
        // TODO: Implement actual process metrics collection
        vec![
            MetricEntry {
                name: "sinex_process_memory_usage_bytes".to_string(),
                help: "Process memory usage in bytes".to_string(),
                metric_type: MetricType::Gauge,
                value: MetricValue::Gauge(get_process_memory_usage(self.pid) as f64),
                labels: HashMap::from([("pid".to_string(), self.pid.to_string())]),
                timestamp: current_timestamp(),
            },
            MetricEntry {
                name: "sinex_process_cpu_usage_percent".to_string(),
                help: "Process CPU usage percentage".to_string(),
                metric_type: MetricType::Gauge,
                value: MetricValue::Gauge(get_process_cpu_usage(self.pid)),
                labels: HashMap::from([("pid".to_string(), self.pid.to_string())]),
                timestamp: current_timestamp(),
            },
            MetricEntry {
                name: "sinex_process_file_descriptors".to_string(),
                help: "Number of open file descriptors".to_string(),
                metric_type: MetricType::Gauge,
                value: MetricValue::Gauge(get_process_file_descriptors(self.pid) as f64),
                labels: HashMap::from([("pid".to_string(), self.pid.to_string())]),
                timestamp: current_timestamp(),
            },
        ]
    }
}

impl MetricsCollector for ProcessMetricsCollector {
    fn collect(&self) -> Vec<MetricEntry> {
        self.collect_process_metrics()
    }

    fn name(&self) -> &str {
        &self.name
    }
}

/// Global metrics collectors registry
static METRICS_COLLECTORS: Lazy<Arc<RwLock<Vec<Box<dyn MetricsCollector>>>>> =
    Lazy::new(|| Arc::new(RwLock::new(Vec::new())));

/// Register a metrics collector
pub fn register_collector(collector: Box<dyn MetricsCollector>) {
    METRICS_COLLECTORS.write().push(collector);
}

/// Collect all metrics from registered collectors
pub fn collect_all_metrics() -> Vec<MetricEntry> {
    let collectors = METRICS_COLLECTORS.read();
    let mut all_metrics = Vec::new();

    for collector in collectors.iter() {
        all_metrics.extend(collector.collect());
    }

    all_metrics
}

/// Background metrics collection task
pub struct BackgroundCollector {
    interval: Duration,
    collectors: Vec<Box<dyn MetricsCollector>>,
}

impl BackgroundCollector {
    pub fn new(interval: Duration) -> Self {
        Self {
            interval,
            collectors: Vec::new(),
        }
    }

    pub fn add_collector(&mut self, collector: Box<dyn MetricsCollector>) {
        self.collectors.push(collector);
    }

    pub async fn run(&self) {
        let mut interval_timer = interval(self.interval);

        loop {
            interval_timer.tick().await;

            // Collect metrics from all collectors
            for collector in &self.collectors {
                let metrics = collector.collect();

                // Store metrics in the global registry
                for metric in metrics {
                    store_metric_entry(metric);
                }
            }
        }
    }
}

/// Metrics storage
static METRICS_STORAGE: Lazy<Arc<RwLock<HashMap<String, MetricEntry>>>> =
    Lazy::new(|| Arc::new(RwLock::new(HashMap::new())));

/// Store a metric entry
fn store_metric_entry(metric: MetricEntry) {
    let key = format!(
        "{}::{}",
        metric.name,
        metric
            .labels
            .iter()
            .map(|(k, v)| format!("{}={}", k, v))
            .collect::<Vec<_>>()
            .join(",")
    );

    METRICS_STORAGE.write().insert(key, metric);
}

/// Get all stored metrics
pub fn get_all_stored_metrics() -> Vec<MetricEntry> {
    METRICS_STORAGE.read().values().cloned().collect()
}

/// Start background metrics collection
pub async fn start_background_collectors() {
    let system_collector =
        SystemMetricsCollector::new("system".to_string(), Duration::from_secs(30));

    let process_collector = ProcessMetricsCollector::new("process".to_string());

    let mut background_collector = BackgroundCollector::new(Duration::from_secs(10));
    background_collector.add_collector(Box::new(system_collector));
    background_collector.add_collector(Box::new(process_collector));

    tokio::spawn(async move {
        background_collector.run().await;
    });
}

/// Get current timestamp in seconds since epoch
fn current_timestamp() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

// System metrics functions - These would be implemented using system APIs
fn get_system_memory_usage() -> u64 {
    // TODO: Implement actual system memory usage
    1024 * 1024 * 1024 // 1GB placeholder
}

fn get_system_memory_available() -> u64 {
    // TODO: Implement actual system memory available
    4 * 1024 * 1024 * 1024 // 4GB placeholder
}

fn get_system_cpu_usage() -> f64 {
    // TODO: Implement actual system CPU usage
    25.0 // 25% placeholder
}

fn get_system_load_average() -> f64 {
    // TODO: Implement actual system load average
    1.5 // 1.5 placeholder
}

fn get_system_disk_usage() -> u64 {
    // TODO: Implement actual system disk usage
    50 * 1024 * 1024 * 1024 // 50GB placeholder
}

fn get_system_disk_read_bytes() -> u64 {
    // TODO: Implement actual system disk read bytes
    1024 * 1024 * 1024 // 1GB placeholder
}

fn get_system_disk_write_bytes() -> u64 {
    // TODO: Implement actual system disk write bytes
    512 * 1024 * 1024 // 512MB placeholder
}

fn get_system_network_bytes_received() -> u64 {
    // TODO: Implement actual system network bytes received
    100 * 1024 * 1024 // 100MB placeholder
}

fn get_system_network_bytes_sent() -> u64 {
    // TODO: Implement actual system network bytes sent
    50 * 1024 * 1024 // 50MB placeholder
}

fn get_process_memory_usage(pid: u32) -> u64 {
    // TODO: Implement actual process memory usage
    let _ = pid;
    100 * 1024 * 1024 // 100MB placeholder
}

fn get_process_cpu_usage(pid: u32) -> f64 {
    // TODO: Implement actual process CPU usage
    let _ = pid;
    10.0 // 10% placeholder
}

fn get_process_file_descriptors(pid: u32) -> u64 {
    // TODO: Implement actual process file descriptor count
    let _ = pid;
    50 // 50 FDs placeholder
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_system_metrics_collector() {
        let collector =
            SystemMetricsCollector::new("test_system".to_string(), Duration::from_secs(10));

        let metrics = collector.collect();
        assert!(!metrics.is_empty());
        assert_eq!(collector.name(), "test_system");
    }

    #[tokio::test]
    async fn test_process_metrics_collector() {
        let collector = ProcessMetricsCollector::new("test_process".to_string());

        let metrics = collector.collect();
        assert!(!metrics.is_empty());
        assert_eq!(collector.name(), "test_process");
    }

    #[tokio::test]
    async fn test_metrics_storage() {
        let metric = MetricEntry {
            name: "test_metric".to_string(),
            help: "Test metric".to_string(),
            metric_type: MetricType::Counter,
            value: MetricValue::Counter(42.0),
            labels: HashMap::new(),
            timestamp: current_timestamp(),
        };

        store_metric_entry(metric);

        let stored_metrics = get_all_stored_metrics();
        assert_eq!(stored_metrics.len(), 1);
        assert_eq!(stored_metrics[0].name, "test_metric");
    }

    #[tokio::test]
    async fn test_background_collector() {
        let collector =
            SystemMetricsCollector::new("test_system".to_string(), Duration::from_secs(1));

        let mut background_collector = BackgroundCollector::new(Duration::from_millis(100));
        background_collector.add_collector(Box::new(collector));

        // Run for a short time
        let handle = tokio::spawn(async move {
            background_collector.run().await;
        });

        tokio::time::sleep(Duration::from_millis(200)).await;
        handle.abort();

        // Check that metrics were collected
        let stored_metrics = get_all_stored_metrics();
        assert!(!stored_metrics.is_empty());
    }
}

//! # Metrics Collection Infrastructure
//!
//! This module provides the core infrastructure for collecting and managing metrics
//! across the Sinex system.
//!
//! ## Overview
//!
//! The collectors module implements a flexible framework for gathering metrics from
//! various sources including system resources, process statistics, and custom metrics.
//! It supports both pull-based collection (where collectors are queried) and push-based
//! collection (where metrics are actively reported).
//!
//! ## Components
//!
//! - [`MetricsCollector`] - Trait for implementing custom metric collectors
//! - [`SystemMetricsCollector`] - Collects system-wide metrics (CPU, memory, disk, network)
//! - [`ProcessMetricsCollector`] - Collects process-specific metrics
//! - [`BackgroundCollector`] - Runs collectors periodically in the background
//! - [`MetricEntry`] - Represents a single metric observation
//! - [`MetricType`] and [`MetricValue`] - Type system for different metric kinds
//!
//! ## Usage
//!
//! ```rust,ignore
//! use sinex_telemetry::metrics::collectors::{
//!     SystemMetricsCollector, register_collector, start_background_collectors
//! };
//! use std::time::Duration;
//!
//! // Register a system metrics collector
//! let collector = Box::new(SystemMetricsCollector::new(
//!     "system".to_string(),
//!     Duration::from_secs(30),
//! ));
//! register_collector(collector);
//!
//! // Start background collection
//! start_background_collectors().await;
//! ```
//!
//! ## Custom Collectors
//!
//! Implement the `MetricsCollector` trait to create custom collectors:
//!
//! ```rust,ignore
//! struct MyCollector;
//!
//! impl MetricsCollector for MyCollector {
//!     fn collect(&self) -> Vec<MetricEntry> {
//!         vec![MetricEntry {
//!             name: "my_metric".to_string(),
//!             help: "My custom metric".to_string(),
//!             metric_type: MetricType::Gauge,
//!             value: MetricValue::Gauge(42.0),
//!             labels: HashMap::new(),
//!             timestamp: current_timestamp(),
//!         }]
//!     }
//!     
//!     fn name(&self) -> &str {
//!         "my_collector"
//!     }
//! }
//! ```

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
    use sinex_test_utils::prelude::*;

    #[sinex_test]
    async fn test_system_metrics_collector(_ctx: TestContext) -> color_eyre::eyre::Result<()> {
        let collector =
            SystemMetricsCollector::new("test_system".to_string(), Duration::from_secs(10));

        let metrics = collector.collect();
        assert!(!metrics.is_empty());
        assert_eq!(collector.name(), "test_system");

        // Verify all expected metric types
        let metric_names: Vec<String> = metrics.iter().map(|m| m.name.clone()).collect();
        assert!(metric_names.contains(&"sinex_system_memory_usage_bytes".to_string()));
        assert!(metric_names.contains(&"sinex_system_memory_available_bytes".to_string()));
        assert!(metric_names.contains(&"sinex_system_cpu_usage_percent".to_string()));
        assert!(metric_names.contains(&"sinex_system_load_average_1m".to_string()));
        assert!(metric_names.contains(&"sinex_system_disk_usage_bytes".to_string()));
        assert!(metric_names.contains(&"sinex_system_disk_io_read_bytes_total".to_string()));
        assert!(metric_names.contains(&"sinex_system_disk_io_write_bytes_total".to_string()));
        assert!(metric_names.contains(&"sinex_system_network_bytes_received_total".to_string()));
        assert!(metric_names.contains(&"sinex_system_network_bytes_sent_total".to_string()));

        // Verify metric types
        for metric in metrics {
            match metric.name.as_str() {
                name if name.ends_with("_total") => {
                    assert_eq!(metric.metric_type, MetricType::Counter);
                    assert!(matches!(metric.value, MetricValue::Counter(_)));
                }
                _ => {
                    assert_eq!(metric.metric_type, MetricType::Gauge);
                    assert!(matches!(metric.value, MetricValue::Gauge(_)));
                }
            }
        }

        Ok(())
    }

    #[sinex_test]
    async fn test_process_metrics_collector(_ctx: TestContext) -> color_eyre::eyre::Result<()> {
        let collector = ProcessMetricsCollector::new("test_process".to_string());

        let metrics = collector.collect();
        assert!(!metrics.is_empty());
        assert_eq!(collector.name(), "test_process");

        // Verify process-specific metrics
        assert_eq!(metrics.len(), 3);

        for metric in &metrics {
            // All process metrics should have pid label
            assert!(metric.labels.contains_key("pid"));
            assert_eq!(metric.labels["pid"], std::process::id().to_string());

            match metric.name.as_str() {
                "sinex_process_memory_usage_bytes" => {
                    assert_eq!(metric.metric_type, MetricType::Gauge);
                    assert!(matches!(metric.value, MetricValue::Gauge(_)));
                }
                "sinex_process_cpu_usage_percent" => {
                    assert_eq!(metric.metric_type, MetricType::Gauge);
                    assert!(matches!(metric.value, MetricValue::Gauge(_)));
                }
                "sinex_process_file_descriptors" => {
                    assert_eq!(metric.metric_type, MetricType::Gauge);
                    assert!(matches!(metric.value, MetricValue::Gauge(_)));
                }
                _ => panic!("Unexpected metric: {}", metric.name),
            }
        }

        Ok(())
    }

    #[sinex_test]
    async fn test_metrics_storage(_ctx: TestContext) -> color_eyre::eyre::Result<()> {
        // Clear any existing metrics
        METRICS_STORAGE.write().clear();

        let metric = MetricEntry {
            name: "test_metric".to_string(),
            help: "Test metric".to_string(),
            metric_type: MetricType::Counter,
            value: MetricValue::Counter(42.0),
            labels: HashMap::new(),
            timestamp: current_timestamp(),
        };

        store_metric_entry(metric.clone());

        let stored_metrics = get_all_stored_metrics();
        assert_eq!(stored_metrics.len(), 1);
        assert_eq!(stored_metrics[0].name, "test_metric");
        assert_eq!(stored_metrics[0].help, "Test metric");

        // Test with labels
        let labeled_metric = MetricEntry {
            name: "test_metric".to_string(),
            help: "Test metric with labels".to_string(),
            metric_type: MetricType::Counter,
            value: MetricValue::Counter(100.0),
            labels: HashMap::from([
                ("method".to_string(), "GET".to_string()),
                ("status".to_string(), "200".to_string()),
            ]),
            timestamp: current_timestamp(),
        };

        store_metric_entry(labeled_metric);

        let stored_metrics = get_all_stored_metrics();
        assert_eq!(stored_metrics.len(), 2); // Original + labeled

        // Clear storage
        METRICS_STORAGE.write().clear();

        Ok(())
    }

    #[sinex_test]
    async fn test_metric_value_types(_ctx: TestContext) -> color_eyre::eyre::Result<()> {
        // Test Summary value
        let summary = SummaryValue {
            count: 100,
            sum: 5000.0,
            quantiles: HashMap::from([
                ("0.5".to_string(), 45.0),
                ("0.9".to_string(), 85.0),
                ("0.99".to_string(), 98.0),
            ]),
        };

        let summary_metric = MetricEntry {
            name: "test_summary".to_string(),
            help: "Test summary metric".to_string(),
            metric_type: MetricType::Summary,
            value: MetricValue::Summary(summary.clone()),
            labels: HashMap::new(),
            timestamp: current_timestamp(),
        };

        store_metric_entry(summary_metric);

        let stored = get_all_stored_metrics();
        if let MetricValue::Summary(stored_summary) = &stored[0].value {
            assert_eq!(stored_summary.count, 100);
            assert_eq!(stored_summary.sum, 5000.0);
            assert_eq!(stored_summary.quantiles.get("0.5"), Some(&45.0));
        } else {
            panic!("Expected Summary metric value");
        }

        // Clear storage
        METRICS_STORAGE.write().clear();

        Ok(())
    }

    #[sinex_test]
    async fn test_global_collectors_registry(_ctx: TestContext) -> color_eyre::eyre::Result<()> {
        // Clear any existing collectors
        METRICS_COLLECTORS.write().clear();

        let collector1 = Box::new(SystemMetricsCollector::new(
            "sys1".to_string(),
            Duration::from_secs(1),
        ));
        let collector2 = Box::new(ProcessMetricsCollector::new("proc1".to_string()));

        register_collector(collector1);
        register_collector(collector2);

        let all_metrics = collect_all_metrics();

        // Should have metrics from both collectors
        let sources: Vec<String> = all_metrics
            .iter()
            .filter_map(|m| {
                if m.name.starts_with("sinex_system") {
                    Some("system".to_string())
                } else if m.name.starts_with("sinex_process") {
                    Some("process".to_string())
                } else {
                    None
                }
            })
            .collect();

        assert!(sources.contains(&"system".to_string()));
        assert!(sources.contains(&"process".to_string()));

        // Clear collectors
        METRICS_COLLECTORS.write().clear();

        Ok(())
    }

    #[sinex_test(timeout = 1)]
    async fn test_background_collector(ctx: TestContext) -> color_eyre::eyre::Result<()> {
        // Clear storage
        METRICS_STORAGE.write().clear();

        let collector =
            SystemMetricsCollector::new("test_system".to_string(), Duration::from_secs(1));

        let mut background_collector = BackgroundCollector::new(Duration::from_millis(100));
        background_collector.add_collector(Box::new(collector));

        // Run for a short time
        let handle = tokio::spawn(async move {
            background_collector.run().await;
        });

        // Wait for at least one collection cycle
        tokio::time::sleep(Duration::from_millis(150)).await;

        handle.abort();

        // Check that metrics were collected
        let stored_metrics = get_all_stored_metrics();
        assert!(!stored_metrics.is_empty());

        // Verify metrics were actually stored with proper keys
        let storage = METRICS_STORAGE.read();
        for (key, metric) in storage.iter() {
            assert!(key.starts_with(&metric.name));

            // If metric has labels, key should contain them
            if !metric.labels.is_empty() {
                for (label_key, label_value) in &metric.labels {
                    assert!(key.contains(&format!("{}={}", label_key, label_value)));
                }
            }
        }

        // Clear storage
        drop(storage);
        METRICS_STORAGE.write().clear();

        Ok(())
    }

    #[sinex_test]
    async fn test_concurrent_metric_storage(ctx: TestContext) -> color_eyre::eyre::Result<()> {
        use tokio::task::JoinSet;

        // Clear storage
        METRICS_STORAGE.write().clear();

        let mut tasks = JoinSet::new();

        // Spawn multiple tasks writing metrics concurrently
        for i in 0..10 {
            tasks.spawn(async move {
                for j in 0..10 {
                    let metric = MetricEntry {
                        name: format!("concurrent_metric_{}", i),
                        help: "Concurrent test metric".to_string(),
                        metric_type: MetricType::Counter,
                        value: MetricValue::Counter((i * 10 + j) as f64),
                        labels: HashMap::from([("worker".to_string(), i.to_string())]),
                        timestamp: current_timestamp(),
                    };
                    store_metric_entry(metric);
                    tokio::time::sleep(Duration::from_micros(100)).await;
                }
            });
        }

        // Wait for all tasks
        while let Some(result) = tasks.join_next().await {
            result?;
        }

        // Verify all metrics were stored correctly
        let stored = get_all_stored_metrics();
        assert_eq!(stored.len(), 10); // 10 unique metric names (latest value for each)

        // Clear storage
        METRICS_STORAGE.write().clear();

        Ok(())
    }
}

#[cfg(all(test, feature = "bench"))]
mod benches {
    use super::*;
    use sinex_test_utils::prelude::*;

    #[sinex_bench]
    async fn bench_system_metrics_collection(ctx: &mut BenchContext) -> color_eyre::eyre::Result<()> {
        let collector =
            SystemMetricsCollector::new("bench_system".to_string(), Duration::from_secs(1));

        ctx.bench("system_metrics_collection", || {
            collector.collect();
        });

        Ok(())
    }

    #[sinex_bench]
    async fn bench_process_metrics_collection(ctx: &mut BenchContext) -> color_eyre::eyre::Result<()> {
        let collector = ProcessMetricsCollector::new("bench_process".to_string());

        ctx.bench("process_metrics_collection", || {
            collector.collect();
        });

        Ok(())
    }

    #[sinex_bench]
    async fn bench_metric_storage(ctx: &mut BenchContext) -> color_eyre::eyre::Result<()> {
        let metric = MetricEntry {
            name: "bench_metric".to_string(),
            help: "Benchmark metric".to_string(),
            metric_type: MetricType::Counter,
            value: MetricValue::Counter(42.0),
            labels: HashMap::from([
                ("method".to_string(), "GET".to_string()),
                ("endpoint".to_string(), "/api/v1/events".to_string()),
            ]),
            timestamp: current_timestamp(),
        };

        ctx.bench("metric_storage", || {
            store_metric_entry(metric.clone());
        });

        Ok(())
    }

    #[sinex_bench]
    async fn bench_collect_all_metrics(ctx: &mut BenchContext) -> color_eyre::eyre::Result<()> {
        // Register some collectors
        METRICS_COLLECTORS.write().clear();
        register_collector(Box::new(SystemMetricsCollector::new(
            "bench1".to_string(),
            Duration::from_secs(1),
        )));
        register_collector(Box::new(ProcessMetricsCollector::new("bench2".to_string())));

        ctx.bench("collect_all_metrics", || {
            collect_all_metrics();
        });

        Ok(())
    }

    #[sinex_bench]
    async fn bench_get_all_stored_metrics(ctx: &mut BenchContext) -> color_eyre::eyre::Result<()> {
        // Pre-populate storage
        METRICS_STORAGE.write().clear();
        for i in 0..100 {
            let metric = MetricEntry {
                name: format!("bench_stored_{}", i),
                help: "Benchmark stored metric".to_string(),
                metric_type: MetricType::Gauge,
                value: MetricValue::Gauge(i as f64),
                labels: HashMap::new(),
                timestamp: current_timestamp(),
            };
            store_metric_entry(metric);
        }

        ctx.bench("get_all_stored_metrics", || {
            get_all_stored_metrics();
        });

        Ok(())
    }
}

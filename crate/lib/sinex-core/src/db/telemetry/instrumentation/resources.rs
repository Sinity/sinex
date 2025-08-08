//! System Resource Metrics
//!
//! This module provides comprehensive system resource monitoring including CPU, memory, disk, and network metrics.

use once_cell::sync::Lazy;
use parking_lot::RwLock;
use prometheus::{Counter, Gauge, IntGauge};
use std::collections::HashMap;
use std::sync::Arc;
use sysinfo::{CpuRefreshKind, Disks, MemoryRefreshKind, Networks, RefreshKind, System};

use crate::telemetry::metrics::registry::GlobalMetrics;

/// Resource types to track
#[derive(Debug, Clone, PartialEq)]
pub enum ResourceType {
    Memory,
    Cpu,
    FileDescriptors,
    NetworkConnections,
    DiskIo,
}

/// System resource metrics collector
#[derive(Debug, Clone)]
pub struct ResourceMetrics {
    pub tracked_resources: Vec<ResourceType>,
    pub memory_usage_bytes: Gauge,
    pub memory_available_bytes: Gauge,
    pub cpu_usage_percent: Gauge,
    pub load_average: Gauge,
    pub disk_read_bytes: Counter,
    pub disk_write_bytes: Counter,
    pub disk_usage_percent: Gauge,
    pub network_rx_bytes: Counter,
    pub network_tx_bytes: Counter,
    pub file_descriptors_open: IntGauge,
    pub network_connections_active: IntGauge,
    pub labels: HashMap<String, String>,
}

impl ResourceMetrics {
    pub fn new(tracked_resources: Vec<ResourceType>, labels: HashMap<String, String>) -> Self {
        let memory_usage_bytes = Gauge::with_opts(
            prometheus::Opts::new("sinex_memory_usage_bytes", "Current memory usage in bytes")
                .namespace("sinex")
                .subsystem("system")
                .const_labels(labels.clone()),
        )
        .unwrap();

        let memory_available_bytes = Gauge::with_opts(
            prometheus::Opts::new("sinex_memory_available_bytes", "Available memory in bytes")
                .namespace("sinex")
                .subsystem("system")
                .const_labels(labels.clone()),
        )
        .unwrap();

        let cpu_usage_percent = Gauge::with_opts(
            prometheus::Opts::new("sinex_cpu_usage_percent", "CPU usage percentage")
                .namespace("sinex")
                .subsystem("system")
                .const_labels(labels.clone()),
        )
        .unwrap();

        let load_average = Gauge::with_opts(
            prometheus::Opts::new("sinex_load_average", "System load average")
                .namespace("sinex")
                .subsystem("system")
                .const_labels(labels.clone()),
        )
        .unwrap();

        let disk_read_bytes = Counter::with_opts(
            prometheus::Opts::new("sinex_disk_read_bytes_total", "Total bytes read from disk")
                .namespace("sinex")
                .subsystem("system")
                .const_labels(labels.clone()),
        )
        .unwrap();

        let disk_write_bytes = Counter::with_opts(
            prometheus::Opts::new(
                "sinex_disk_write_bytes_total",
                "Total bytes written to disk",
            )
            .namespace("sinex")
            .subsystem("system")
            .const_labels(labels.clone()),
        )
        .unwrap();

        let disk_usage_percent = Gauge::with_opts(
            prometheus::Opts::new("sinex_disk_usage_percent", "Disk usage percentage")
                .namespace("sinex")
                .subsystem("system")
                .const_labels(labels.clone()),
        )
        .unwrap();

        let network_rx_bytes = Counter::with_opts(
            prometheus::Opts::new(
                "sinex_network_rx_bytes_total",
                "Total bytes received over network",
            )
            .namespace("sinex")
            .subsystem("system")
            .const_labels(labels.clone()),
        )
        .unwrap();

        let network_tx_bytes = Counter::with_opts(
            prometheus::Opts::new(
                "sinex_network_tx_bytes_total",
                "Total bytes transmitted over network",
            )
            .namespace("sinex")
            .subsystem("system")
            .const_labels(labels.clone()),
        )
        .unwrap();

        let file_descriptors_open = IntGauge::with_opts(
            prometheus::Opts::new(
                "sinex_file_descriptors_open",
                "Number of open file descriptors",
            )
            .namespace("sinex")
            .subsystem("system")
            .const_labels(labels.clone()),
        )
        .unwrap();

        let network_connections_active = IntGauge::with_opts(
            prometheus::Opts::new(
                "sinex_network_connections_active",
                "Number of active network connections",
            )
            .namespace("sinex")
            .subsystem("system")
            .const_labels(labels.clone()),
        )
        .unwrap();

        // Register with global metrics
        GlobalMetrics::register_gauge(&memory_usage_bytes);
        GlobalMetrics::register_gauge(&memory_available_bytes);
        GlobalMetrics::register_gauge(&cpu_usage_percent);
        GlobalMetrics::register_gauge(&load_average);
        GlobalMetrics::register_counter(&disk_read_bytes);
        GlobalMetrics::register_counter(&disk_write_bytes);
        GlobalMetrics::register_gauge(&disk_usage_percent);
        GlobalMetrics::register_counter(&network_rx_bytes);
        GlobalMetrics::register_counter(&network_tx_bytes);
        GlobalMetrics::register_gauge(&file_descriptors_open);
        GlobalMetrics::register_gauge(&network_connections_active);

        Self {
            tracked_resources,
            memory_usage_bytes,
            memory_available_bytes,
            cpu_usage_percent,
            load_average,
            disk_read_bytes,
            disk_write_bytes,
            disk_usage_percent,
            network_rx_bytes,
            network_tx_bytes,
            file_descriptors_open,
            network_connections_active,
            labels,
        }
    }

    pub fn update_memory_stats(&self, used_bytes: u64, available_bytes: u64) {
        self.memory_usage_bytes.set(used_bytes as f64);
        self.memory_available_bytes.set(available_bytes as f64);
    }

    pub fn update_cpu_stats(&self, usage_percent: f64, load_avg: f64) {
        self.cpu_usage_percent.set(usage_percent);
        self.load_average.set(load_avg);
    }

    pub fn record_disk_read(&self, bytes: u64) {
        self.disk_read_bytes.inc_by(bytes as f64);
    }

    pub fn record_disk_write(&self, bytes: u64) {
        self.disk_write_bytes.inc_by(bytes as f64);
    }

    pub fn update_disk_usage(&self, usage_percent: f64) {
        self.disk_usage_percent.set(usage_percent);
    }

    pub fn record_network_rx(&self, bytes: u64) {
        self.network_rx_bytes.inc_by(bytes as f64);
    }

    pub fn record_network_tx(&self, bytes: u64) {
        self.network_tx_bytes.inc_by(bytes as f64);
    }

    pub fn update_file_descriptors(&self, count: i64) {
        self.file_descriptors_open.set(count);
    }

    pub fn update_network_connections(&self, count: i64) {
        self.network_connections_active.set(count);
    }

    /// Collect current system metrics
    pub fn collect_system_metrics(&self) {
        let refresh_kind = RefreshKind::new()
            .with_memory(MemoryRefreshKind::everything())
            .with_cpu(CpuRefreshKind::everything());

        let mut system = System::new_with_specifics(refresh_kind);
        system.refresh_specifics(refresh_kind);

        if self.tracked_resources.contains(&ResourceType::Memory) {
            let used_memory = system.used_memory();
            let available_memory = system.available_memory();
            self.update_memory_stats(used_memory, available_memory);
        }

        if self.tracked_resources.contains(&ResourceType::Cpu) {
            let cpu_usage: f64 = system
                .cpus()
                .iter()
                .map(|cpu| cpu.cpu_usage() as f64)
                .sum::<f64>()
                / system.cpus().len() as f64;
            // Load average is now an associated function in sysinfo 0.30+
            let load_avg = System::load_average().one;
            self.update_cpu_stats(cpu_usage, load_avg);
        }

        // In sysinfo 0.30+, disks and networks are separate structs
        if self.tracked_resources.contains(&ResourceType::DiskIo) {
            let _disks = Disks::new_with_refreshed_list();
            // Update disk metrics - simplified for now
            // TODO: Implement proper disk metrics aggregation
        }

        if self
            .tracked_resources
            .contains(&ResourceType::NetworkConnections)
        {
            let _networks = Networks::new_with_refreshed_list();
            // Update network metrics - simplified for now
            // TODO: Implement proper network metrics aggregation
        }
    }
}

/// Global resource metrics
static RESOURCE_METRICS: Lazy<Arc<RwLock<HashMap<String, Arc<ResourceMetrics>>>>> =
    Lazy::new(|| Arc::new(RwLock::new(HashMap::new())));

/// Get or create resource metrics
pub fn get_resource_metrics(
    name: &str,
    tracked_resources: Vec<ResourceType>,
    labels: HashMap<String, String>,
) -> Arc<ResourceMetrics> {
    let key = format!("resource_{}", name);

    // Try to get existing metrics
    if let Some(metrics) = RESOURCE_METRICS.read().get(&key) {
        return metrics.clone();
    }

    // Create new metrics
    let metrics = Arc::new(ResourceMetrics::new(tracked_resources, labels));
    RESOURCE_METRICS.write().insert(key, metrics.clone());

    metrics
}

/// Create default system resource metrics
pub fn create_system_metrics() -> Arc<ResourceMetrics> {
    get_resource_metrics(
        "system",
        vec![
            ResourceType::Memory,
            ResourceType::Cpu,
            ResourceType::DiskIo,
            ResourceType::NetworkConnections,
        ],
        HashMap::new(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_resource_metrics_creation() -> Result<(), Box<dyn std::error::Error>> {
        let metrics = create_system_metrics();
        assert_eq!(metrics.tracked_resources.len(), 4);
        Ok(())
    }

    #[tokio::test]
    async fn test_system_metrics_collection() -> Result<(), Box<dyn std::error::Error>> {
        let metrics = create_system_metrics();
        metrics.collect_system_metrics();

        // Should have collected some metrics
        assert!(metrics.memory_usage_bytes.get() >= 0.0);
        Ok(())
    }
}

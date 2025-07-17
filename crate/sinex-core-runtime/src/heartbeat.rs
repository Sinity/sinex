//! Heartbeat and health monitoring utilities
//!
//! This module provides utilities for health monitoring and heartbeat emission,
//! including system health checks and process monitoring.

use serde::{Deserialize, Serialize};
use sinex_core_types::{RawEvent, Result};
use sinex_events::EventFactory;
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::{interval, Instant};
use tracing::{debug, warn};

/// Health status levels
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum HealthStatus {
    Healthy,
    Warning,
    Critical,
    Unknown,
}

/// System health information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemHealth {
    pub status: HealthStatus,
    pub message: String,
    pub details: HashMap<String, serde_json::Value>,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

impl SystemHealth {
    /// Create a new healthy system health
    pub fn healthy(message: impl Into<String>) -> Self {
        Self {
            status: HealthStatus::Healthy,
            message: message.into(),
            details: HashMap::new(),
            timestamp: chrono::Utc::now(),
        }
    }

    /// Create a new warning system health
    pub fn warning(message: impl Into<String>) -> Self {
        Self {
            status: HealthStatus::Warning,
            message: message.into(),
            details: HashMap::new(),
            timestamp: chrono::Utc::now(),
        }
    }

    /// Create a new critical system health
    pub fn critical(message: impl Into<String>) -> Self {
        Self {
            status: HealthStatus::Critical,
            message: message.into(),
            details: HashMap::new(),
            timestamp: chrono::Utc::now(),
        }
    }

    /// Add a detail to the health information
    pub fn with_detail(mut self, key: impl Into<String>, value: impl Serialize) -> Self {
        if let Ok(json_value) = serde_json::to_value(value) {
            self.details.insert(key.into(), json_value);
        }
        self
    }
}

/// Trait for providing metrics information
pub trait MetricsProvider {
    /// Get current metrics as a JSON value
    fn get_metrics(&self) -> Result<serde_json::Value>;
}

/// Process heartbeat emitter
pub struct ProcessHeartbeatEmitter {
    source_name: String,
    interval_duration: Duration,
    metrics_providers: Vec<Box<dyn MetricsProvider + Send + Sync>>,
}

impl ProcessHeartbeatEmitter {
    /// Create a new heartbeat emitter
    pub fn new(source_name: impl Into<String>, interval_duration: Duration) -> Self {
        Self {
            source_name: source_name.into(),
            interval_duration,
            metrics_providers: Vec::new(),
        }
    }

    /// Add a metrics provider
    pub fn add_metrics_provider<P: MetricsProvider + Send + Sync + 'static>(
        mut self,
        provider: P,
    ) -> Self {
        self.metrics_providers.push(Box::new(provider));
        self
    }

    /// Start emitting heartbeats
    pub async fn start_emitting<F>(&self, mut emit_fn: F) -> Result<()>
    where
        F: FnMut(RawEvent) -> Result<()>,
    {
        let mut interval = interval(self.interval_duration);
        let mut sequence = 0u64;

        loop {
            interval.tick().await;
            sequence += 1;

            let heartbeat_event = self.create_heartbeat_event(sequence)?;

            if let Err(e) = emit_fn(heartbeat_event) {
                warn!("Failed to emit heartbeat: {}", e);
            } else {
                debug!("Emitted heartbeat #{} for {}", sequence, self.source_name);
            }
        }
    }

    /// Create a single heartbeat event
    fn create_heartbeat_event(&self, sequence: u64) -> Result<RawEvent> {
        let mut payload = serde_json::json!({
            "source": self.source_name,
            "sequence": sequence,
            "timestamp": chrono::Utc::now(),
            "status": "healthy"
        });

        // Collect metrics from all providers
        let mut metrics = HashMap::new();
        for provider in &self.metrics_providers {
            match provider.get_metrics() {
                Ok(provider_metrics) => {
                    if let serde_json::Value::Object(map) = provider_metrics {
                        for (key, value) in map {
                            metrics.insert(key, value);
                        }
                    }
                }
                Err(e) => {
                    warn!("Failed to get metrics from provider: {}", e);
                }
            }
        }

        if !metrics.is_empty() {
            payload["metrics"] = serde_json::Value::Object(metrics.into_iter().collect());
        }

        // Create RawEvent
        let factory = EventFactory::new(&self.source_name);
        let event = factory.create_event("process.heartbeat", payload);

        Ok(event)
    }
}

/// Determine health status based on multiple health checks
pub fn determine_health_status(healths: &[SystemHealth]) -> HealthStatus {
    if healths.is_empty() {
        return HealthStatus::Unknown;
    }

    let mut has_critical = false;
    let mut has_warning = false;

    for health in healths {
        match health.status {
            HealthStatus::Critical => has_critical = true,
            HealthStatus::Warning => has_warning = true,
            HealthStatus::Healthy => {}
            HealthStatus::Unknown => {}
        }
    }

    if has_critical {
        HealthStatus::Critical
    } else if has_warning {
        HealthStatus::Warning
    } else {
        HealthStatus::Healthy
    }
}

/// Simple metrics provider for basic system information
pub struct BasicMetricsProvider {
    name: String,
    start_time: Instant,
}

impl BasicMetricsProvider {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            start_time: Instant::now(),
        }
    }
}

impl MetricsProvider for BasicMetricsProvider {
    fn get_metrics(&self) -> Result<serde_json::Value> {
        let uptime_seconds = self.start_time.elapsed().as_secs();

        Ok(serde_json::json!({
            "name": self.name,
            "uptime_seconds": uptime_seconds,
            "memory_usage": get_memory_usage(),
            "thread_count": get_thread_count(),
        }))
    }
}

/// Get basic memory usage information
fn get_memory_usage() -> serde_json::Value {
    // This is a simplified implementation
    // In a real system, you might use a crate like `sysinfo` for detailed metrics
    serde_json::json!({
        "rss_bytes": 0,  // Placeholder
        "heap_bytes": 0, // Placeholder
    })
}

/// Get thread count information
fn get_thread_count() -> u64 {
    // This is a simplified implementation
    // In a real system, you might use system APIs or process information
    1 // Placeholder
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_system_health_creation() {
        let health = SystemHealth::healthy("System is running well")
            .with_detail("cpu_usage", 15.5)
            .with_detail("memory_usage", 75);

        assert_eq!(health.status, HealthStatus::Healthy);
        assert_eq!(health.message, "System is running well");
        assert!(health.details.contains_key("cpu_usage"));
        assert!(health.details.contains_key("memory_usage"));
    }

    #[test]
    fn test_determine_health_status() {
        let healths = vec![
            SystemHealth::healthy("Service A is healthy"),
            SystemHealth::warning("Service B has warnings"),
            SystemHealth::healthy("Service C is healthy"),
        ];

        assert_eq!(determine_health_status(&healths), HealthStatus::Warning);

        let healths = vec![
            SystemHealth::healthy("Service A is healthy"),
            SystemHealth::critical("Service B is critical"),
            SystemHealth::warning("Service C has warnings"),
        ];

        assert_eq!(determine_health_status(&healths), HealthStatus::Critical);

        let healths = vec![
            SystemHealth::healthy("Service A is healthy"),
            SystemHealth::healthy("Service B is healthy"),
        ];

        assert_eq!(determine_health_status(&healths), HealthStatus::Healthy);
    }

    #[test]
    fn test_basic_metrics_provider() {
        let provider = BasicMetricsProvider::new("test_service");
        let metrics = provider.get_metrics().unwrap();

        assert!(metrics.get("name").is_some());
        assert!(metrics.get("uptime_seconds").is_some());
        assert!(metrics.get("memory_usage").is_some());
        assert!(metrics.get("thread_count").is_some());
    }

    #[tokio::test]
    async fn test_heartbeat_emitter() {
        let emitter = ProcessHeartbeatEmitter::new("test_service", Duration::from_millis(100))
            .add_metrics_provider(BasicMetricsProvider::new("test"));

        let events: Vec<RawEvent> = Vec::new();
        let emit_count = 0;

        // We'll manually call create_heartbeat_event instead of start_emitting
        // to avoid the infinite loop in tests
        let event = emitter.create_heartbeat_event(1).unwrap();

        assert_eq!(event.source, "test_service");
        assert_eq!(event.event_type, "process.heartbeat");
        assert!(event.payload.get("sequence").is_some());
        assert!(event.payload.get("timestamp").is_some());
    }
}

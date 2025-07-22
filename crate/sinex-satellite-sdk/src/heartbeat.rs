//! Structured heartbeat logging for satellite services
//!
//! This module implements the Journald Heartbeat Idea from the design discussion:
//! Satellites emit structured JSON logs to stdout, which systemd captures in journald,
//! which gets picked up by the journald ingestor as regular events, and processed
//! by the health aggregator automaton.

use serde::{Deserialize, Serialize};
use sinex_core_utils::CoordinationPrimitive;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::SystemTime;
use tokio::time::{interval, Duration};
use tracing::info;

/// Heartbeat metrics and status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatMetrics {
    /// Service name (e.g., "sinex-fs-watcher")
    pub service_name: String,
    /// Current status: "healthy", "degraded", "failed"
    pub status: String,
    /// Number of events processed since last heartbeat
    pub events_processed: u64,
    /// Service uptime in seconds
    pub uptime_seconds: u64,
    /// Memory usage in MB (approximate)
    pub memory_usage_mb: u32,
    /// CPU usage percentage (approximate)
    pub cpu_usage_percent: f32,
    /// Number of errors in the last period
    pub errors_count: u32,
    /// Last error message (if any)
    pub last_error_message: Option<String>,
    /// Binary version
    pub version: String,
    /// Git commit hash
    pub git_hash: String,
    /// Timestamp of this heartbeat
    pub timestamp: String,
    /// Additional metadata specific to the service
    pub metadata: Option<serde_json::Value>,
}

/// Heartbeat emitter that logs structured JSON to stdout
#[derive(Clone)]
pub struct HeartbeatEmitter {
    service_name: String,
    start_time: SystemTime,
    events_processed: CoordinationPrimitive,
    errors_count: CoordinationPrimitive,
    last_error: Arc<parking_lot::Mutex<Option<String>>>,
    pub interval_seconds: u64,
    version: String,
    git_hash: String,
}

impl HeartbeatEmitter {
    /// Create a new heartbeat emitter
    pub fn new(service_name: String, interval_seconds: u64) -> Self {
        let version = env!("CARGO_PKG_VERSION").to_string();
        let git_hash = option_env!("GIT_HASH").unwrap_or("unknown").to_string();

        Self {
            service_name,
            start_time: SystemTime::now(),
            events_processed: CoordinationPrimitive::event_counter(0, "events_processed"),
            errors_count: CoordinationPrimitive::event_counter(0, "errors_count"),
            last_error: Arc::new(parking_lot::Mutex::new(None)),
            interval_seconds,
            version,
            git_hash,
        }
    }

    /// Increment the events processed counter
    pub fn increment_events_processed(&self, count: u64) {
        self.events_processed.add(count as usize);
    }

    /// Record an error
    pub fn record_error(&self, error_message: &str) {
        self.errors_count.add(1);
        *self.last_error.lock() = Some(error_message.to_string());
    }

    /// Get current status based on error rate and activity
    fn get_current_status(&self) -> String {
        let errors = self.errors_count.get();

        // Simple heuristic: if we have more than 10 errors, we're degraded
        // If more than 50, we're failed
        if errors > 50 {
            "failed".to_string()
        } else if errors > 10 {
            "degraded".to_string()
        } else {
            "healthy".to_string()
        }
    }

    /// Get approximate memory usage in MB
    fn get_memory_usage_mb(&self) -> u32 {
        // Basic implementation using /proc/self/status
        if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
            for line in status.lines() {
                if line.starts_with("VmRSS:") {
                    if let Some(kb_str) = line.split_whitespace().nth(1) {
                        if let Ok(kb) = kb_str.parse::<u32>() {
                            return kb / 1024; // Convert KB to MB
                        }
                    }
                }
            }
        }
        0 // Default if we can't read memory info
    }

    /// Get approximate CPU usage (placeholder implementation)
    fn get_cpu_usage_percent(&self) -> f32 {
        // This is a placeholder - proper CPU usage would require
        // tracking over time. For now, return 0.0
        0.0
    }

    /// Create heartbeat metrics
    fn create_heartbeat_metrics(&self, metadata: Option<serde_json::Value>) -> HeartbeatMetrics {
        let uptime = self.start_time.elapsed().unwrap_or_default().as_secs();

        let events_processed = self.events_processed.swap(0);
        let errors_count = self.errors_count.swap(0) as u32;
        let last_error = self.last_error.lock().take();

        HeartbeatMetrics {
            service_name: self.service_name.clone(),
            status: self.get_current_status(),
            events_processed,
            uptime_seconds: uptime,
            memory_usage_mb: self.get_memory_usage_mb(),
            cpu_usage_percent: self.get_cpu_usage_percent(),
            errors_count,
            last_error_message: last_error,
            version: self.version.clone(),
            git_hash: self.git_hash.clone(),
            timestamp: chrono::Utc::now().to_rfc3339(),
            metadata,
        }
    }

    /// Emit a single heartbeat to stdout
    pub fn emit_heartbeat(&self, metadata: Option<serde_json::Value>) {
        let metrics = self.create_heartbeat_metrics(metadata);

        // Create structured log message that journald will capture
        let log_entry = serde_json::json!({
            "level": "INFO",
            "message": "heartbeat",
            "target": "heartbeat",
            "module_path": "sinex_satellite_sdk::heartbeat",
            "file": "heartbeat.rs",
            "line": 1,
            "fields": {
                "service_name": metrics.service_name,
                "status": metrics.status,
                "events_processed": metrics.events_processed,
                "uptime_seconds": metrics.uptime_seconds,
                "memory_usage_mb": metrics.memory_usage_mb,
                "cpu_usage_percent": metrics.cpu_usage_percent,
                "errors_count": metrics.errors_count,
                "last_error_message": metrics.last_error_message,
                "version": metrics.version,
                "git_hash": metrics.git_hash,
                "timestamp": metrics.timestamp,
                "metadata": metrics.metadata
            }
        });

        // Print directly to stdout - systemd will capture this
        println!("{}", log_entry);

        // Also log via tracing for local debugging
        info!(
            service = %metrics.service_name,
            status = %metrics.status,
            events_processed = metrics.events_processed,
            uptime_seconds = metrics.uptime_seconds,
            memory_usage_mb = metrics.memory_usage_mb,
            errors_count = metrics.errors_count,
            "Satellite heartbeat emitted"
        );
    }

    /// Start periodic heartbeat emission
    pub async fn start_periodic_heartbeat(
        &self,
        mut metadata_provider: Option<Box<dyn Fn() -> Option<serde_json::Value> + Send>>,
    ) {
        let mut interval = interval(Duration::from_secs(self.interval_seconds));

        info!(
            service = %self.service_name,
            interval_seconds = self.interval_seconds,
            "Starting periodic heartbeat emission"
        );

        loop {
            interval.tick().await;

            let metadata = metadata_provider.as_mut().and_then(|provider| provider());

            self.emit_heartbeat(metadata);
        }
    }

    /// Get a handle for incrementing counters
    pub fn get_counter_handle(&self) -> HeartbeatCounterHandle {
        HeartbeatCounterHandle {
            events_processed: self.events_processed.clone(),
            errors_count: self.errors_count.clone(),
            last_error: self.last_error.clone(),
        }
    }
}

/// Handle for incrementing heartbeat counters from other parts of the code
#[derive(Clone)]
pub struct HeartbeatCounterHandle {
    events_processed: CoordinationPrimitive,
    errors_count: CoordinationPrimitive,
    last_error: Arc<parking_lot::Mutex<Option<String>>>,
}

impl HeartbeatCounterHandle {
    /// Increment events processed counter
    pub fn increment_events_processed(&self, count: u64) {
        self.events_processed.add(count as usize);
    }

    /// Record an error
    pub fn record_error(&self, error_message: &str) {
        self.errors_count.add(1);
        *self.last_error.lock() = Some(error_message.to_string());
    }

    /// Get current events processed count
    pub fn get_events_processed(&self) -> u64 {
        self.events_processed.get() as u64
    }

    /// Get current errors count
    pub fn get_errors_count(&self) -> u64 {
        self.errors_count.get() as u64
    }
}

/// Helper macro for creating heartbeat logs in satellite services
#[macro_export]
macro_rules! emit_heartbeat {
    ($service_name:expr) => {
        let log_entry = serde_json::json!({
            "level": "INFO",
            "message": "heartbeat",
            "target": "heartbeat",
            "fields": {
                "service_name": $service_name,
                "status": "healthy",
                "timestamp": chrono::Utc::now().to_rfc3339()
            }
        });
        println!("{}", log_entry);
    };

    ($service_name:expr, $($field:ident = $value:expr),+) => {
        let mut fields = serde_json::json!({
            "service_name": $service_name,
            "timestamp": chrono::Utc::now().to_rfc3339()
        });

        $(
            fields[stringify!($field)] = serde_json::json!($value);
        )+

        let log_entry = serde_json::json!({
            "level": "INFO",
            "message": "heartbeat",
            "target": "heartbeat",
            "fields": fields
        });
        println!("{}", log_entry);
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_heartbeat_emitter_creation() {
        let emitter = HeartbeatEmitter::new("test-service".to_string(), 30);
        assert_eq!(emitter.service_name, "test-service");
        assert_eq!(emitter.interval_seconds, 30);
    }

    #[tokio::test]
    async fn test_counter_handle() {
        let emitter = HeartbeatEmitter::new("test-service".to_string(), 30);
        let handle = emitter.get_counter_handle();

        handle.increment_events_processed(5);
        handle.record_error("test error");

        assert_eq!(handle.get_events_processed(), 5);
        assert_eq!(handle.get_errors_count(), 1);
    }

    #[test]
    fn test_heartbeat_metrics_creation() {
        let emitter = HeartbeatEmitter::new("test-service".to_string(), 30);
        emitter.increment_events_processed(10);
        emitter.record_error("test error");

        let metrics = emitter.create_heartbeat_metrics(None);
        assert_eq!(metrics.service_name, "test-service");
        assert_eq!(metrics.errors_count, 1);
        assert!(metrics.last_error_message.is_some());
    }

    #[test]
    fn test_emit_heartbeat_macro() {
        // This test just ensures the macro compiles
        emit_heartbeat!("test-service");
        emit_heartbeat!("test-service", events_processed = 5, status = "healthy");
    }
}

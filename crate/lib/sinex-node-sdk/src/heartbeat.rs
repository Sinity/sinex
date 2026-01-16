//! Structured heartbeat logging for satellite services
//!
//! This module implements the Journald Heartbeat Idea from the design discussion:
//! Satellites emit structured JSON logs to stdout, which systemd captures in journald,
//! which gets picked up by the journald ingestor as regular events, and processed
//! by the health aggregator automaton.

use crate::stream_processor::ProcessorRuntimeState;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sinex_core::types::events::payloads::process::{
    ProcessDegradedPayload, ProcessFailedPayload, ProcessStatus,
};
use sinex_core::types::{utils::CoordinationPrimitive, Seconds};
use std::mem::MaybeUninit;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use tokio::time::interval;
use tracing::{info, warn};

/// Heartbeat metrics and status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatMetrics {
    /// Service name (e.g., "sinex-fs-watcher")
    pub service_name: String,
    /// Current status: healthy, degraded, failed
    pub status: ProcessStatus,
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

/// Pluggable sink for heartbeat log emission
pub trait HeartbeatLogSink: Send + Sync + std::fmt::Debug {
    fn emit(&self, entry: &serde_json::Value);
}

#[derive(Debug, Default)]
struct StdoutHeartbeatSink;

impl HeartbeatLogSink for StdoutHeartbeatSink {
    fn emit(&self, entry: &serde_json::Value) {
        println!("{}", entry);
    }
}

/// Heartbeat emitter that logs structured JSON to stdout
#[derive(Debug, Clone)]
pub struct HeartbeatEmitter {
    service_name: String,
    start_time: SystemTime,
    events_processed: CoordinationPrimitive,
    errors_count: CoordinationPrimitive,
    last_error: Arc<parking_lot::Mutex<Option<String>>>,
    pub interval_seconds: Seconds,
    version: String,
    git_hash: String,
    cpu_sample: Arc<parking_lot::Mutex<Option<CpuSample>>>,
    cpu_cores: usize,
    log_sink: Arc<dyn HeartbeatLogSink>,
    last_emitted_status: Arc<parking_lot::Mutex<ProcessStatus>>,
}

#[derive(Debug, Clone, Copy)]
struct CpuSample {
    cpu_seconds: f64,
    timestamp: Instant,
}

impl HeartbeatEmitter {
    /// Create a new heartbeat emitter
    pub fn new(service_name: String, interval_seconds: Seconds) -> Self {
        let version = env!("CARGO_PKG_VERSION").to_string();
        let git_hash = option_env!("GIT_HASH").unwrap_or("unknown").to_string();
        let initial_cpu_sample = Self::read_process_cpu_seconds().map(|cpu_seconds| CpuSample {
            cpu_seconds,
            timestamp: Instant::now(),
        });
        let cpu_cores = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);

        Self {
            service_name,
            start_time: SystemTime::now(),
            events_processed: CoordinationPrimitive::event_counter(0, "events_processed"),
            errors_count: CoordinationPrimitive::event_counter(0, "errors_count"),
            last_error: Arc::new(parking_lot::Mutex::new(None)),
            interval_seconds,
            version,
            git_hash,
            cpu_sample: Arc::new(parking_lot::Mutex::new(initial_cpu_sample)),
            cpu_cores,
            log_sink: Arc::new(StdoutHeartbeatSink::default()),
            last_emitted_status: Arc::new(parking_lot::Mutex::new(ProcessStatus::Healthy)),
        }
    }

    /// Configure a custom log sink (primarily for tests)
    pub fn with_log_sink(mut self, sink: Arc<dyn HeartbeatLogSink>) -> Self {
        self.log_sink = sink;
        self
    }

    /// Construct a heartbeat emitter for a runtime with the provided interval
    pub fn from_runtime(runtime: &ProcessorRuntimeState, interval_seconds: Seconds) -> Self {
        Self::new(
            runtime.service_info().service_name().to_string(),
            interval_seconds,
        )
    }

    /// Expose configured service name for tests and diagnostics
    pub fn service_name(&self) -> &str {
        &self.service_name
    }

    /// Expose configured heartbeat interval
    pub fn interval_seconds(&self) -> Seconds {
        self.interval_seconds
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

    fn determine_status(recent_errors: usize) -> ProcessStatus {
        if recent_errors > 50 {
            ProcessStatus::Failed
        } else if recent_errors > 10 {
            ProcessStatus::Degraded
        } else {
            ProcessStatus::Healthy
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

    /// Get approximate CPU usage (recent delta across all available cores)
    fn get_cpu_usage_percent(&self) -> f32 {
        let current_cpu = match Self::read_process_cpu_seconds() {
            Some(value) => value,
            None => return 0.0,
        };
        let now = Instant::now();
        let mut sample = self.cpu_sample.lock();

        if let Some(previous) = *sample {
            let cpu_delta = current_cpu - previous.cpu_seconds;
            let wall_delta = (now - previous.timestamp).as_secs_f64();
            if wall_delta > 0.0 && cpu_delta >= 0.0 {
                let utilization = (cpu_delta / wall_delta) * 100.0;
                let normalized = utilization / self.cpu_cores as f64;
                *sample = Some(CpuSample {
                    cpu_seconds: current_cpu,
                    timestamp: now,
                });
                return normalized.clamp(0.0, 100.0).max(0.0) as f32;
            }
        }

        *sample = Some(CpuSample {
            cpu_seconds: current_cpu,
            timestamp: now,
        });
        0.0
    }

    /// Create heartbeat metrics
    pub fn create_heartbeat_metrics(
        &self,
        metadata: Option<serde_json::Value>,
    ) -> HeartbeatMetrics {
        let uptime = self.start_time.elapsed().unwrap_or_default().as_secs();
        let recent_errors = self.errors_count.get();

        let events_processed = {
            let old = self.events_processed.get();
            self.events_processed.reset();
            old
        };
        self.errors_count.reset();
        let last_error = self.last_error.lock().take();
        let status = Self::determine_status(recent_errors);

        HeartbeatMetrics {
            service_name: self.service_name.clone(),
            status,
            events_processed: events_processed as u64,
            uptime_seconds: uptime,
            memory_usage_mb: self.get_memory_usage_mb(),
            cpu_usage_percent: self.get_cpu_usage_percent(),
            errors_count: recent_errors as u32,
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
        let log_entry = json!({
            "level": "INFO",
            "message": "heartbeat",
            "target": "heartbeat",
            "module_path": "sinex_node_sdk::heartbeat",
            "file": "heartbeat.rs",
            "line": 1,
            "fields": {
                "event_type": "satellite.heartbeat",
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

        self.log_sink.emit(&log_entry);

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

        self.emit_status_alert_if_needed(&metrics);
    }

    /// Start periodic heartbeat emission
    pub async fn start_periodic_heartbeat(
        &self,
        mut metadata_provider: Option<Box<dyn Fn() -> Option<serde_json::Value> + Send>>,
    ) {
        let mut interval = interval(Duration::from_secs(self.interval_seconds.as_secs()));

        info!(
            service = %self.service_name,
            interval_seconds = self.interval_seconds.as_secs(),
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

    fn read_process_cpu_seconds() -> Option<f64> {
        let mut usage = MaybeUninit::<libc::rusage>::uninit();
        let result = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
        if result != 0 {
            return None;
        }
        let usage = unsafe { usage.assume_init() };
        Some(Self::timeval_to_seconds(&usage.ru_utime) + Self::timeval_to_seconds(&usage.ru_stime))
    }

    fn timeval_to_seconds(tv: &libc::timeval) -> f64 {
        tv.tv_sec as f64 + tv.tv_usec as f64 / 1_000_000_f64
    }
}

impl HeartbeatEmitter {
    fn emit_status_alert_if_needed(&self, metrics: &HeartbeatMetrics) {
        let mut last_status = self.last_emitted_status.lock();
        if metrics.status == *last_status {
            return;
        }

        let next_status = metrics.status;
        *last_status = next_status;

        match next_status {
            ProcessStatus::Healthy => {
                info!(
                    service = %metrics.service_name,
                    "Satellite recovered to healthy status"
                );
            }
            ProcessStatus::Degraded => {
                self.log_process_alert("process.degraded", metrics);
            }
            ProcessStatus::Failed => {
                self.log_process_alert("process.failed", metrics);
            }
        }
    }

    fn log_process_alert(&self, event_type: &str, metrics: &HeartbeatMetrics) {
        let payload = match event_type {
            "process.failed" => serde_json::to_value(ProcessFailedPayload {
                process_name: metrics.service_name.clone(),
                uptime_seconds: metrics.uptime_seconds,
                errors_in_window: metrics.errors_count,
                last_error_message: metrics.last_error_message.clone(),
                metadata: metrics.metadata.clone(),
            })
            .unwrap_or_else(|_| json!({})),
            _ => serde_json::to_value(ProcessDegradedPayload {
                process_name: metrics.service_name.clone(),
                uptime_seconds: metrics.uptime_seconds,
                errors_in_window: metrics.errors_count,
                last_error_message: metrics.last_error_message.clone(),
                metadata: metrics.metadata.clone(),
            })
            .unwrap_or_else(|_| json!({})),
        };

        let alert_entry = json!({
            "level": "WARN",
            "message": event_type,
            "target": "heartbeat",
            "module_path": "sinex_node_sdk::heartbeat",
            "file": "heartbeat.rs",
            "line": 1,
            "fields": {
                "event_type": event_type,
                "service_name": metrics.service_name,
                "status": metrics.status,
                "errors_count": metrics.errors_count,
                "last_error_message": metrics.last_error_message,
                "uptime_seconds": metrics.uptime_seconds,
                "metadata": metrics.metadata,
                "payload": payload,
            }
        });

        self.log_sink.emit(&alert_entry);

        warn!(
            service = %metrics.service_name,
            status = %metrics.status,
            errors = metrics.errors_count,
            "Satellite transitioned to {} state",
            event_type
        );
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

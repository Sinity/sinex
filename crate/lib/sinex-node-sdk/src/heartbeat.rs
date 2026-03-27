//! Structured heartbeat logging for node services
//!
//! This module implements the Journald Heartbeat Idea from the design discussion:
//! Nodes emit structured JSON logs to stdout, which systemd captures in journald,
//! which gets picked up by the journald ingestor as regular events, and processed
//! by the health aggregator automaton.

use crate::runtime::stream::NodeRuntimeState;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sinex_primitives::domain::NodeName;
use sinex_primitives::events::payloads::process::{
    ProcessDegradedPayload, ProcessFailedPayload, ProcessStatus,
};
use sinex_primitives::utils::CoordinationPrimitive;
use sinex_primitives::{Seconds, Uuid};
use std::mem::MaybeUninit;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use tokio::time::interval;
use tracing::{debug, info, warn};

/// Configurable health thresholds.
///
/// These can be overridden via environment variables:
/// - `SINEX_HEARTBEAT_DEGRADED_THRESHOLD`: Errors in 5min window to trigger degraded (default: 10)
/// - `SINEX_HEARTBEAT_FAILED_THRESHOLD`: Errors in 5min window to trigger failed (default: 50)
const DEFAULT_DEGRADED_THRESHOLD: usize = 10;
const DEFAULT_FAILED_THRESHOLD: usize = 50;

fn get_degraded_threshold() -> usize {
    std::env::var("SINEX_HEARTBEAT_DEGRADED_THRESHOLD")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_DEGRADED_THRESHOLD)
}

fn get_failed_threshold() -> usize {
    std::env::var("SINEX_HEARTBEAT_FAILED_THRESHOLD")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_FAILED_THRESHOLD)
}

/// Heartbeat metrics and status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatMetrics {
    /// Service name (e.g., "sinex-fs-ingestor")
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
        println!("{entry}");
    }
}

/// Heartbeat emitter that logs structured JSON to stdout
#[derive(Debug, Clone)]
pub struct HeartbeatEmitter {
    service_name: String,
    node_name: Option<NodeName>,
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
    /// Sliding window for error tracking (last 5 minutes).
    error_window: Arc<parking_lot::Mutex<Vec<Instant>>>,
    node_run_id: Option<Uuid>,
    /// Optional database pool for persisting heartbeat status to `core.node_manifests`.
    /// When set, each heartbeat emission also updates the `last_heartbeat_at` and `status`
    /// columns for this node, enabling efficient active-node queries.
    #[cfg(feature = "db")]
    db_pool: Option<sinex_db::DbPool>,
}

#[derive(Debug, Clone, Copy)]
struct CpuSample {
    cpu_seconds: f64,
    timestamp: Instant,
}

impl HeartbeatEmitter {
    /// Create a new heartbeat emitter
    #[must_use]
    pub fn new(service_name: String, interval_seconds: Seconds) -> Self {
        let version = env!("CARGO_PKG_VERSION").to_string();
        let git_hash = option_env!("GIT_HASH").unwrap_or("unknown").to_string();
        let initial_cpu_sample = Self::read_process_cpu_seconds().map(|cpu_seconds| CpuSample {
            cpu_seconds,
            timestamp: Instant::now(),
        });
        let cpu_cores = std::thread::available_parallelism().map_or(1, std::num::NonZero::get);

        Self {
            service_name,
            node_name: None,
            start_time: SystemTime::now(),
            events_processed: CoordinationPrimitive::event_counter(0, "events_processed"),
            errors_count: CoordinationPrimitive::event_counter(0, "errors_count"),
            last_error: Arc::new(parking_lot::Mutex::new(None)),
            interval_seconds,
            version,
            git_hash,
            cpu_sample: Arc::new(parking_lot::Mutex::new(initial_cpu_sample)),
            cpu_cores,
            log_sink: Arc::new(StdoutHeartbeatSink),
            last_emitted_status: Arc::new(parking_lot::Mutex::new(ProcessStatus::Healthy)),
            error_window: Arc::new(parking_lot::Mutex::new(Vec::new())),
            node_run_id: None,
            #[cfg(feature = "db")]
            db_pool: None,
        }
    }

    /// Configure a custom log sink (primarily for tests)
    pub fn with_log_sink(mut self, sink: Arc<dyn HeartbeatLogSink>) -> Self {
        self.log_sink = sink;
        self
    }

    #[must_use]
    pub fn with_node_name(mut self, node_name: NodeName) -> Self {
        self.node_name = Some(node_name);
        self
    }

    #[must_use]
    pub fn with_version(mut self, version: impl Into<String>) -> Self {
        self.version = version.into();
        self
    }

    #[must_use]
    pub fn with_node_run_id(mut self, node_run_id: Uuid) -> Self {
        self.node_run_id = Some(node_run_id);
        self
    }

    /// Configure a database pool for persisting heartbeat status.
    ///
    /// When set, each heartbeat emission will also update `last_heartbeat_at`
    /// and `status = 'active'` in `core.node_manifests` for this node.
    #[cfg(feature = "db")]
    #[must_use]
    pub fn with_db_pool(mut self, pool: sinex_db::DbPool) -> Self {
        self.db_pool = Some(pool);
        self
    }

    /// Construct a heartbeat emitter for a runtime with the provided interval
    #[must_use]
    pub fn from_runtime(runtime: &NodeRuntimeState, interval_seconds: Seconds) -> Self {
        let emitter = Self::new(
            runtime.service_info().service_name().to_string(),
            interval_seconds,
        )
        .with_node_name(NodeName::new(runtime.node_name()))
        .with_version(runtime.version().to_string());

        let emitter = if let Some(node_run_id) = runtime.node_run_id() {
            emitter.with_node_run_id(node_run_id)
        } else {
            emitter
        };

        #[cfg(feature = "db")]
        let emitter = if let Some(pool) = runtime.handles().db_pool().cloned() {
            emitter.with_db_pool(pool)
        } else {
            emitter
        };

        emitter
    }

    /// Expose configured service name for tests and diagnostics
    #[must_use]
    pub fn service_name(&self) -> &str {
        &self.service_name
    }

    /// Expose configured heartbeat interval
    #[must_use]
    pub fn interval_seconds(&self) -> Seconds {
        self.interval_seconds
    }

    /// Increment the events processed counter
    pub fn increment_events_processed(&self, count: u64) {
        let _ = self.events_processed.add(count as usize);
    }

    /// Record an error
    ///
    /// Record an error and update the 5-minute sliding error window.
    pub fn record_error(&self, error_message: &str) {
        let _ = self.errors_count.add(1);
        *self.last_error.lock() = Some(error_message.to_string());

        // Add to sliding window
        let mut window = self.error_window.lock();
        window.push(Instant::now());
    }

    /// Determine status based on the 5-minute sliding window and configured thresholds.
    fn determine_status(&self) -> ProcessStatus {
        const WINDOW_DURATION: Duration = Duration::from_mins(5); // 5 minutes
        let now = Instant::now();

        // Clean up old errors and count recent ones
        let mut window = self.error_window.lock();
        window.retain(|timestamp| now.duration_since(*timestamp) < WINDOW_DURATION);
        let recent_errors = window.len();

        let failed_threshold = get_failed_threshold();
        let degraded_threshold = get_degraded_threshold();

        if recent_errors > failed_threshold {
            ProcessStatus::Failed
        } else if recent_errors > degraded_threshold {
            ProcessStatus::Degraded
        } else {
            ProcessStatus::Healthy
        }
    }

    /// Get approximate memory usage in MB
    ///
    /// Logs parse failures and returns 0 as fallback.
    async fn get_memory_usage_mb(&self) -> u32 {
        // Basic implementation using /proc/self/status
        match tokio::fs::read_to_string("/proc/self/status").await {
            Ok(status) => {
                for line in status.lines() {
                    if line.starts_with("VmRSS:")
                        && let Some(kb_str) = line.split_whitespace().nth(1)
                    {
                        match kb_str.parse::<u32>() {
                            Ok(kb) => return kb / 1024, // Convert KB to MB
                            Err(e) => {
                                warn!(
                                    error = %e,
                                    raw_value = %kb_str,
                                    "Failed to parse VmRSS value from /proc/self/status"
                                );
                                return 0;
                            }
                        }
                    }
                }
                warn!("VmRSS line not found in /proc/self/status");
                0
            }
            Err(e) => {
                warn!(error = %e, "Failed to read /proc/self/status for memory usage");
                0 // Default if we can't read memory info
            }
        }
    }

    /// Get approximate CPU usage (recent delta across all available cores)
    ///
    /// Logs when CPU sampling fails.
    fn get_cpu_usage_percent(&self) -> f32 {
        let Some(current_cpu) = Self::read_process_cpu_seconds() else {
            warn!("Failed to read process CPU seconds via getrusage");
            return 0.0;
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
    ///
    /// Uses atomic counter reset for heartbeat interval accounting.
    /// Note: `CoordinationPrimitive::reset()` internally uses swap(0, `AcqRel`) which is atomic,
    /// but the pattern of get-then-reset is not. We read `errors_count` before resetting to
    /// include it in the current heartbeat metrics.
    ///
    /// KNOWN LIMITATION: There's a small window between `get()` and `reset()` where counter
    /// updates could be lost. For heartbeat metrics this is acceptable as it only affects
    /// the accuracy of per-interval counts, not cumulative totals. To fix this properly,
    /// `CoordinationPrimitive` would need a `fetch_and_reset()` method.
    pub async fn create_heartbeat_metrics(
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
        let status = self.determine_status();

        HeartbeatMetrics {
            service_name: self.service_name.clone(),
            status,
            events_processed: events_processed as u64,
            uptime_seconds: uptime,
            memory_usage_mb: self.get_memory_usage_mb().await,
            cpu_usage_percent: self.get_cpu_usage_percent(),
            errors_count: recent_errors as u32,
            last_error_message: last_error,
            version: self.version.clone(),
            git_hash: self.git_hash.clone(),
            timestamp: sinex_primitives::temporal::format_rfc3339(sinex_primitives::temporal::now()),
            metadata,
        }
    }

    /// Emit a single heartbeat to stdout
    pub async fn emit_heartbeat(&self, metadata: Option<serde_json::Value>) {
        let metrics = self.create_heartbeat_metrics(metadata).await;

        // Create structured log message that journald will capture
        let log_entry = json!({
            "level": "INFO",
            "message": "heartbeat",
            "target": "heartbeat",
            "module_path": "sinex_node_sdk::heartbeat",
            "file": "heartbeat.rs",
            "line": 1,
            "fields": {
                "event_type": "node.heartbeat",
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

        // Persist heartbeat to database if pool is configured
        #[cfg(feature = "db")]
        if let Some(ref pool) = self.db_pool {
            use sinex_db::DbPoolExt;
            if let Some(node_name) = &self.node_name {
                match pool
                    .state()
                    .update_node_heartbeat_for_version(node_name, &metrics.version)
                    .await
                {
                    Ok(true) => {}
                    Ok(false) => {
                        warn!(
                            node = %node_name,
                            service = %metrics.service_name,
                            version = %metrics.version,
                            "Heartbeat did not persist because the node manifest row is missing"
                        );
                    }
                    Err(e) => {
                        debug!(
                            node = %node_name,
                            service = %metrics.service_name,
                            error = %e,
                            "Failed to persist node manifest heartbeat to database (non-fatal)"
                        );
                    }
                }
            }

            if let Some(node_run_id) = self.node_run_id {
                match pool.state().update_node_run_heartbeat(node_run_id).await {
                    Ok(true) => {}
                    Ok(false) => {
                        warn!(
                            service = %metrics.service_name,
                            node_run_id = %node_run_id,
                            "Heartbeat did not persist because the node run row is missing"
                        );
                    }
                    Err(e) => {
                        debug!(
                            service = %metrics.service_name,
                            node_run_id = %node_run_id,
                            error = %e,
                            "Failed to persist node run heartbeat to database (non-fatal)"
                        );
                    }
                }
            }
        }

        // Also log via tracing for local debugging
        info!(
            service = %metrics.service_name,
            status = %metrics.status,
            events_processed = metrics.events_processed,
            uptime_seconds = metrics.uptime_seconds,
            memory_usage_mb = metrics.memory_usage_mb,
            errors_count = metrics.errors_count,
            "Node heartbeat emitted"
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

            self.emit_heartbeat(metadata).await;
        }
    }

    /// Get a handle for incrementing counters
    #[must_use]
    pub fn get_counter_handle(&self) -> HeartbeatCounterHandle {
        HeartbeatCounterHandle {
            events_processed: self.events_processed.clone(),
            errors_count: self.errors_count.clone(),
            last_error: self.last_error.clone(),
            error_window: self.error_window.clone(),
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
                    "Node recovered to healthy status"
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
        let payload = if event_type == "process.failed" {
            serde_json::to_value(ProcessFailedPayload {
                process_name: metrics.service_name.clone(),
                uptime_seconds: metrics.uptime_seconds,
                errors_in_window: metrics.errors_count,
                last_error_message: metrics.last_error_message.clone(),
                metadata: metrics.metadata.clone(),
            })
            .unwrap_or_else(|_| json!({}))
        } else {
            serde_json::to_value(ProcessDegradedPayload {
                process_name: metrics.service_name.clone(),
                uptime_seconds: metrics.uptime_seconds,
                errors_in_window: metrics.errors_count,
                last_error_message: metrics.last_error_message.clone(),
                metadata: metrics.metadata.clone(),
            })
            .unwrap_or_else(|_| json!({}))
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
            event_type = %event_type,
            "Node transitioned to state"
        );
    }
}

/// Handle for incrementing heartbeat counters from other parts of the code
#[derive(Clone)]
pub struct HeartbeatCounterHandle {
    events_processed: CoordinationPrimitive,
    errors_count: CoordinationPrimitive,
    last_error: Arc<parking_lot::Mutex<Option<String>>>,
    error_window: Arc<parking_lot::Mutex<Vec<Instant>>>,
}

impl HeartbeatCounterHandle {
    /// Increment events processed counter
    pub fn increment_events_processed(&self, count: u64) {
        let _ = self.events_processed.add(count as usize);
    }

    /// Record an error
    ///
    /// Adds the error to the sliding window used for status computation.
    pub fn record_error(&self, error_message: &str) {
        let _ = self.errors_count.add(1);
        *self.last_error.lock() = Some(error_message.to_string());

        // Add to sliding window
        let mut window = self.error_window.lock();
        window.push(Instant::now());
    }

    /// Get current events processed count
    #[must_use]
    pub fn get_events_processed(&self) -> u64 {
        self.events_processed.get() as u64
    }

    /// Get current errors count
    #[must_use]
    pub fn get_errors_count(&self) -> u64 {
        self.errors_count.get() as u64
    }
}

/// Helper macro for creating heartbeat logs in node services
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
                "timestamp": sinex_primitives::temporal::format_rfc3339(sinex_primitives::temporal::now())
            }
        });
        println!("{log_entry}");
    };

    ($service_name:expr, $($field:ident = $value:expr),+) => {
        let mut fields = serde_json::json!({
            "service_name": $service_name,
            "timestamp": sinex_primitives::temporal::format_rfc3339(sinex_primitives::temporal::now())
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
        println!("{log_entry}");
    };
}

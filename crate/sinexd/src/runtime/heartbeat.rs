//! Structured heartbeat logging for runtime modules
//!
//! This module implements the Journald Heartbeat Idea from the design discussion:
//! Runtime modules emit structured JSON logs to stdout, which systemd captures in journald,
//! which gets picked up by the journald source as regular events, and processed
//! by the health aggregator automaton.

use crate::runtime::error_helpers::elapsed_seconds_with_warning;
use crate::runtime::stream::RuntimeContext;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sinex_primitives::Seconds;
use sinex_primitives::domain::{HealthStatus, ServiceName};
use sinex_primitives::env as shared_env;
use sinex_primitives::events::payloads::process::{ProcessDegradedPayload, ProcessFailedPayload};
use sinex_primitives::utils::CoordinationPrimitive;
use std::mem::MaybeUninit;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime};
use tokio::time::interval;
use tracing::{debug, info, warn};

/// Configurable health thresholds and cadence.
///
/// These can be overridden via environment variables:
/// - `SINEX_HEARTBEAT_DEGRADED_THRESHOLD`: Errors in 5min window to trigger degraded (default: 10)
/// - `SINEX_HEARTBEAT_FAILED_THRESHOLD`: Errors in 5min window to trigger failed (default: 50)
/// - `SINEX_HEARTBEAT_SUMMARY_EVERY`: Emit a compact liveness summary every N beats (default: 30).
///   With the standard 30s heartbeat interval that is one compact summary line per 15 minutes per
///   module — enough for an operator to confirm liveness in journald without the full-metadata
///   volume of baseline records. Set to 0 to disable periodic summaries entirely.
const DEFAULT_DEGRADED_THRESHOLD: usize = 10;
const DEFAULT_FAILED_THRESHOLD: usize = 50;
/// Default: 30 beats × 30 s interval = 1 summary per 15 minutes per module.
const DEFAULT_SUMMARY_EVERY: u64 = 30;

fn get_degraded_threshold() -> usize {
    env_usize_with_default(
        "SINEX_HEARTBEAT_DEGRADED_THRESHOLD",
        DEFAULT_DEGRADED_THRESHOLD,
    )
}

fn get_failed_threshold() -> usize {
    env_usize_with_default("SINEX_HEARTBEAT_FAILED_THRESHOLD", DEFAULT_FAILED_THRESHOLD)
}

fn env_usize_with_default(var: &str, default: usize) -> usize {
    shared_env::parse_or(var, default, "heartbeat")
}

fn get_summary_every() -> u64 {
    shared_env::parse_or(
        "SINEX_HEARTBEAT_SUMMARY_EVERY",
        DEFAULT_SUMMARY_EVERY,
        "heartbeat",
    )
}

/// Heartbeat metrics and status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatMetrics {
    /// Service name (e.g., "sinexd")
    pub service_name: ServiceName,
    /// Current status: healthy, degraded, failed
    pub status: HealthStatus,
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

/// Heartbeat emitter that logs structured JSON to stdout.
///
#[derive(Debug, Clone)]
pub struct HeartbeatEmitter {
    service_name: ServiceName,
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
    last_emitted_status: Arc<parking_lot::Mutex<HealthStatus>>,
    /// Sliding window for error tracking (last 5 minutes).
    error_window: Arc<parking_lot::Mutex<Vec<Instant>>>,
    /// Whether the one-per-run baseline heartbeat record has been written to the log sink.
    logged_first_beat: Arc<AtomicBool>,
    /// Monotonic beat counter used to drive the periodic liveness summary cadence.
    beat_count: Arc<AtomicU64>,
    /// Emit a compact liveness summary every this many beats (0 = disabled).
    /// Set at construction from `SINEX_HEARTBEAT_SUMMARY_EVERY`; overridable via
    /// [`HeartbeatEmitter::with_summary_every`] for tests.
    summary_every: u64,
}

#[derive(Debug, Clone, Copy)]
struct CpuSample {
    cpu_seconds: f64,
    timestamp: Instant,
}

impl HeartbeatEmitter {
    /// Create a new heartbeat emitter
    #[must_use]
    pub fn new(service_name: ServiceName, interval_seconds: Seconds) -> Self {
        let version = env!("CARGO_PKG_VERSION").to_string();
        let git_hash = option_env!("GIT_HASH").unwrap_or("unknown").to_string();
        let initial_cpu_sample = Self::read_process_cpu_seconds().map(|cpu_seconds| CpuSample {
            cpu_seconds,
            timestamp: Instant::now(),
        });
        let cpu_cores = std::thread::available_parallelism().map_or(1, std::num::NonZero::get);

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
            log_sink: Arc::new(StdoutHeartbeatSink),
            last_emitted_status: Arc::new(parking_lot::Mutex::new(HealthStatus::Healthy)),
            error_window: Arc::new(parking_lot::Mutex::new(Vec::new())),
            logged_first_beat: Arc::new(AtomicBool::new(false)),
            beat_count: Arc::new(AtomicU64::new(0)),
            summary_every: get_summary_every(),
        }
    }

    /// Configure a custom log sink (primarily for tests)
    pub fn with_log_sink(mut self, sink: Arc<dyn HeartbeatLogSink>) -> Self {
        self.log_sink = sink;
        self
    }

    #[must_use]
    pub fn with_version(mut self, version: impl Into<String>) -> Self {
        self.version = version.into();
        self
    }

    /// Override the periodic-summary cadence (every N beats; 0 disables summaries).
    ///
    /// In production this is set via `SINEX_HEARTBEAT_SUMMARY_EVERY` (default 30).
    /// Use this builder when writing tests that need a shorter cadence to stay fast.
    #[must_use]
    pub fn with_summary_every(mut self, n: u64) -> Self {
        self.summary_every = n;
        self
    }

    /// Construct a heartbeat emitter for a runtime with the provided interval
    #[must_use]
    pub fn from_runtime(runtime: &RuntimeContext, interval_seconds: Seconds) -> Self {
        let emitter = Self::new(
            runtime.service_info().service_name().clone(),
            interval_seconds,
        )
        .with_version(runtime.version().to_string());

        emitter
    }

    /// Expose configured service name for tests and diagnostics
    #[must_use]
    pub fn service_name(&self) -> &ServiceName {
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
    fn determine_status(&self) -> HealthStatus {
        let recent_errors = self.recent_error_count();
        let failed_threshold = get_failed_threshold();
        let degraded_threshold = get_degraded_threshold();

        if recent_errors >= failed_threshold {
            HealthStatus::Unhealthy
        } else if recent_errors >= degraded_threshold {
            HealthStatus::Degraded
        } else {
            HealthStatus::Healthy
        }
    }

    fn recent_error_count(&self) -> usize {
        const WINDOW_DURATION: Duration = Duration::from_mins(5); // 5 minutes
        let now = Instant::now();

        // Clean up old errors and count recent ones
        let mut window = self.error_window.lock();
        window.retain(|timestamp| now.duration_since(*timestamp) < WINDOW_DURATION);
        window.len()
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
    /// Uses `CoordinationPrimitive::swap_reset()` (an `AtomicUsize::swap(0, AcqRel)`) to
    /// atomically read-and-zero each counter in a single operation. Increments arriving
    /// concurrently are never lost: they land either in the snapshot (if they raced before
    /// the swap) or in the next interval (if they raced after). No window where counts
    /// silently disappear.
    pub async fn create_heartbeat_metrics(
        &self,
        metadata: Option<serde_json::Value>,
    ) -> HeartbeatMetrics {
        let uptime = elapsed_seconds_with_warning(self.start_time, "heartbeat uptime");
        let recent_errors = self.errors_count.swap_reset();
        let events_processed = self.events_processed.swap_reset();
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
    ///
    /// Three emission paths:
    ///
    /// 1. **Full baseline record** — emitted once per run (first beat) via the log sink.
    ///    Contains all identity metadata (version, git_hash, metadata map, etc.).
    ///
    /// 2. **Signal-bearing record** — emitted via the log sink whenever the module is
    ///    non-healthy or the beat carried errors/error messages.  Same format as the
    ///    baseline.
    ///
    /// 3. **Compact liveness summary** — emitted every `summary_every` beats (default 30,
    ///    configurable via `SINEX_HEARTBEAT_SUMMARY_EVERY` or [`Self::with_summary_every`]).
    ///    Contains only dynamic non-identifying fields: `service_name`, `status`,
    ///    `uptime_seconds`, `events_processed`.  Gives operators a journald liveness
    ///    signal without the full-metadata volume (#1726).
    ///
    /// Journald persists every stdout byte regardless of the `level` field inside the JSON,
    /// so suppressing log-sink emission is the only lever for steady-state journal volume.
    /// Status *transitions* are logged separately by `emit_status_alert_if_needed`.
    pub async fn emit_heartbeat(&self, metadata: Option<serde_json::Value>) {
        let metrics = self.create_heartbeat_metrics(metadata).await;

        let first_beat = !self.logged_first_beat.swap(true, Ordering::Relaxed);
        let signal_bearing = metrics.status != HealthStatus::Healthy
            || metrics.errors_count > 0
            || metrics.last_error_message.is_some();

        if first_beat || signal_bearing {
            let log_entry = json!({
                "level": "INFO",
                "message": "heartbeat",
                "target": "heartbeat",
                "module_path": "sinexd::runtime::heartbeat",
                "file": "heartbeat.rs",
                "line": 1,
                "fields": {
                    "event_type": "runtime.heartbeat",
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
        }

        // Periodic liveness summary (#1726).
        //
        // Every `summary_every` beats, emit a compact JSON record via the log sink so
        // operators have a journald signal that the module is still alive — without
        // re-emitting the full baseline record (version, git_hash, metadata, etc.) on
        // every tick.  The summary carries only dynamic, non-identifying fields:
        // service_name, status, uptime_seconds, events_processed.  No potentially
        // path-bearing or context-carrying metadata is included.
        //
        // The beat counter increments on every call so the cadence is stable regardless
        // of whether the individual beat was signal-bearing.  Beat 0 is the first beat
        // (already emitted as a baseline above), so summaries start at beat `summary_every`.
        if self.summary_every > 0 {
            let beat = self.beat_count.fetch_add(1, Ordering::Relaxed);
            if beat > 0 && beat % self.summary_every == 0 {
                let summary_entry = json!({
                    "level": "INFO",
                    "message": "heartbeat.summary",
                    "target": "heartbeat",
                    "module_path": "sinexd::runtime::heartbeat",
                    "file": "heartbeat.rs",
                    "line": 1,
                    "fields": {
                        "service_name": metrics.service_name,
                        "status": metrics.status,
                        "uptime_seconds": metrics.uptime_seconds,
                        "events_processed": metrics.events_processed,
                    }
                });
                self.log_sink.emit(&summary_entry);
            }
        }

        // Also log via tracing for local debugging
        debug!(
            service = %metrics.service_name,
            status = %metrics.status,
            events_processed = metrics.events_processed,
            uptime_seconds = metrics.uptime_seconds,
            memory_usage_mb = metrics.memory_usage_mb,
            errors_count = metrics.errors_count,
            "runtime module heartbeat emitted"
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
        let recent_errors_in_window = self.recent_error_count() as u32;
        *last_status = next_status;

        match next_status {
            HealthStatus::Healthy | HealthStatus::Unknown => {
                info!(
                    service = %metrics.service_name,
                    "runtime module recovered to healthy status"
                );
            }
            HealthStatus::Degraded => {
                self.log_process_alert("process.degraded", metrics, recent_errors_in_window);
            }
            HealthStatus::Unhealthy => {
                self.log_process_alert("process.failed", metrics, recent_errors_in_window);
            }
        }
    }

    fn log_process_alert(
        &self,
        event_type: &str,
        metrics: &HeartbeatMetrics,
        recent_errors_in_window: u32,
    ) {
        let payload = if event_type == "process.failed" {
            serde_json::to_value(ProcessFailedPayload {
                process_name: metrics.service_name.to_string(),
                uptime_seconds: metrics.uptime_seconds,
                errors_in_window: recent_errors_in_window,
                last_error_message: metrics.last_error_message.clone(),
                metadata: metrics.metadata.clone(),
            })
            .unwrap_or_else(|e| {
                json!({
                    "_serialization_error": e.to_string(),
                    "process_name": metrics.service_name.to_string(),
                    "errors_in_window": recent_errors_in_window,
                })
            })
        } else {
            serde_json::to_value(ProcessDegradedPayload {
                process_name: metrics.service_name.to_string(),
                uptime_seconds: metrics.uptime_seconds,
                errors_in_window: recent_errors_in_window,
                last_error_message: metrics.last_error_message.clone(),
                metadata: metrics.metadata.clone(),
            })
            .unwrap_or_else(|e| {
                json!({
                    "_serialization_error": e.to_string(),
                    "process_name": metrics.service_name,
                    "errors_in_window": recent_errors_in_window,
                })
            })
        };

        let alert_entry = json!({
            "level": "WARN",
            "message": event_type,
            "target": "heartbeat",
            "module_path": "sinexd::runtime::heartbeat",
            "file": "heartbeat.rs",
            "line": 1,
            "fields": {
                "event_type": event_type,
                "service_name": metrics.service_name,
                "status": metrics.status,
                "errors_count": recent_errors_in_window,
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
            errors = recent_errors_in_window,
            event_type = %event_type,
            "runtime module transitioned to state"
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

#[cfg(test)]
mod tests {
    use super::*;
    use xtask::sandbox::sinex_test;

    #[derive(Debug, Default)]
    struct RecordingSink(parking_lot::Mutex<Vec<serde_json::Value>>);

    impl HeartbeatLogSink for RecordingSink {
        fn emit(&self, entry: &serde_json::Value) {
            self.0.lock().push(entry.clone());
        }
    }

    impl RecordingSink {
        fn heartbeat_records(&self) -> Vec<serde_json::Value> {
            self.0
                .lock()
                .iter()
                .filter(|entry| entry["message"] == "heartbeat")
                .cloned()
                .collect()
        }

        fn summary_records(&self) -> Vec<serde_json::Value> {
            self.0
                .lock()
                .iter()
                .filter(|entry| entry["message"] == "heartbeat.summary")
                .cloned()
                .collect()
        }
    }

    fn emitter_with_sink() -> (HeartbeatEmitter, Arc<RecordingSink>) {
        let sink = Arc::new(RecordingSink::default());
        // Disable periodic summaries so existing signal-bearing and suppression tests
        // are not affected by the summary cadence.
        let emitter = HeartbeatEmitter::new(
            ServiceName::new("heartbeat-test"),
            sinex_primitives::Seconds::from_secs(60),
        )
        .with_log_sink(sink.clone())
        .with_summary_every(0);
        (emitter, sink)
    }

    fn emitter_with_sink_and_summary_every(n: u64) -> (HeartbeatEmitter, Arc<RecordingSink>) {
        let sink = Arc::new(RecordingSink::default());
        let emitter = HeartbeatEmitter::new(
            ServiceName::new("heartbeat-test"),
            sinex_primitives::Seconds::from_secs(60),
        )
        .with_log_sink(sink.clone())
        .with_summary_every(n);
        (emitter, sink)
    }

    #[sinex_test]
    async fn first_beat_emits_baseline_then_routine_beats_are_suppressed() -> TestResult<()> {
        let (emitter, sink) = emitter_with_sink();

        emitter.emit_heartbeat(None).await;
        emitter.emit_heartbeat(None).await;
        emitter.emit_heartbeat(None).await;

        let records = sink.heartbeat_records();
        assert_eq!(
            records.len(),
            1,
            "only the baseline record should be emitted for healthy steady state"
        );
        assert_eq!(records[0]["fields"]["event_type"], "runtime.heartbeat");
        assert_eq!(records[0]["fields"]["status"], "healthy");
        Ok(())
    }

    #[sinex_test]
    async fn error_carrying_beat_is_emitted() -> TestResult<()> {
        let (emitter, sink) = emitter_with_sink();
        let handle = emitter.get_counter_handle();

        emitter.emit_heartbeat(None).await; // baseline
        emitter.emit_heartbeat(None).await; // suppressed
        handle.record_error("boom");
        emitter.emit_heartbeat(None).await; // signal-bearing

        let records = sink.heartbeat_records();
        assert_eq!(records.len(), 2, "baseline plus the error-carrying beat");
        let error_beat = &records[1];
        assert_eq!(error_beat["fields"]["errors_count"], 1);
        assert_eq!(error_beat["fields"]["last_error_message"], "boom");
        Ok(())
    }

    #[sinex_test]
    async fn recovery_returns_to_suppressed_steady_state() -> TestResult<()> {
        let (emitter, sink) = emitter_with_sink();
        let handle = emitter.get_counter_handle();

        emitter.emit_heartbeat(None).await; // baseline
        handle.record_error("transient");
        emitter.emit_heartbeat(None).await; // signal-bearing
        emitter.emit_heartbeat(None).await; // healthy and error-free again

        // A single error stays far below the degraded threshold, so the
        // post-recovery beat is healthy and must be suppressed.
        let records = sink.heartbeat_records();
        assert_eq!(
            records.len(),
            2,
            "healthy error-free beats after recovery must be suppressed"
        );
        assert_eq!(records.last().unwrap()["fields"]["errors_count"], 1);
        Ok(())
    }

    /// Verify the periodic liveness summary cadence by construction (#1726).
    ///
    /// With `summary_every = 3`, beats 0, 1, 2 produce no summary; beat 3 fires the first
    /// compact summary; beat 6 fires the second; and so on.  The full baseline record is
    /// emitted only on beat 0 (first beat).  This pins the AC: "steady-state sinexd journal
    /// volume drops by an order of magnitude with no loss of health-transition observability."
    #[sinex_test]
    async fn periodic_summary_emits_compact_record_at_configured_cadence() -> TestResult<()> {
        // summary_every = 3: first summary fires at beat counter = 3, second at 6.
        let (emitter, sink) = emitter_with_sink_and_summary_every(3);

        for _ in 0..7 {
            emitter.emit_heartbeat(None).await;
        }

        // Exactly one full baseline record (beat 0); no further full records since healthy.
        let full_records = sink.heartbeat_records();
        assert_eq!(
            full_records.len(),
            1,
            "only the first beat produces a full baseline JSON record"
        );

        // Exactly two compact summaries: at beats 3 and 6.
        let summaries = sink.summary_records();
        assert_eq!(
            summaries.len(),
            2,
            "summaries must fire at beat multiples of summary_every (3 and 6 out of 0..6)"
        );

        // Summary fields are compact: only service_name, status, uptime_seconds, events_processed.
        let summary = &summaries[0];
        assert_eq!(summary["message"], "heartbeat.summary");
        assert_eq!(summary["level"], "INFO");
        let fields = &summary["fields"];
        assert!(fields["service_name"].is_string(), "service_name present");
        assert!(fields["status"].is_string(), "status present");
        assert!(
            fields["uptime_seconds"].is_number(),
            "uptime_seconds present"
        );
        assert!(
            fields["events_processed"].is_number(),
            "events_processed present"
        );
        // No full-metadata fields in summary.
        assert!(fields["version"].is_null(), "version absent from summary");
        assert!(fields["git_hash"].is_null(), "git_hash absent from summary");
        assert!(fields["metadata"].is_null(), "metadata absent from summary");

        Ok(())
    }

    /// Verify that summaries are disabled when `summary_every = 0`.
    #[sinex_test]
    async fn periodic_summary_disabled_when_summary_every_is_zero() -> TestResult<()> {
        let (emitter, sink) = emitter_with_sink_and_summary_every(0);

        for _ in 0..100 {
            emitter.emit_heartbeat(None).await;
        }

        assert_eq!(
            sink.summary_records().len(),
            0,
            "no summaries emitted when summary_every = 0"
        );
        Ok(())
    }
}

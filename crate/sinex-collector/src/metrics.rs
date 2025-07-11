use anyhow::Result;
use chrono::Utc;
use serde::Serialize;
use serde_json::{json, Value as JsonValue};
use sinex_core::{CoreError, ErrorContext, EventSender, OptionalTimestamp, RawEvent, Timestamp};
use sinex_db::{DbPool, DbPoolRef};
use sinex_ulid::Ulid;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use sysinfo::System;
use tokio::sync::RwLock;
use tracing::{error, info, warn};

/// Single second of metric data
#[derive(Debug, Clone, Serialize)]
pub struct SecondMetrics {
    pub timestamp: Timestamp,
    pub cpu_percent: f32,
    pub memory_mb: u64,
    pub events_count: u64,
    pub errors_count: u64,
    pub queue_depth: usize,
    pub active_sources: usize,
    pub db_pool_size: u32,
    pub db_pool_idle: u32,
}

/// Ring buffer storing high-resolution metrics
pub struct MetricsRingBuffer {
    /// Fixed-size buffer of per-second metrics
    buffer: VecDeque<SecondMetrics>,
    /// Maximum entries to keep (5 minutes = 300 seconds)
    max_size: usize,
    /// Last time we emitted metrics
    last_emit: Instant,
    /// Emit interval (normally 30 seconds)
    emit_interval: Duration,
}

impl MetricsRingBuffer {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Default for MetricsRingBuffer {
    fn default() -> Self {
        Self {
            buffer: VecDeque::with_capacity(300),
            max_size: 300,
            last_emit: Instant::now(),
            emit_interval: Duration::from_secs(30),
        }
    }
}

impl MetricsRingBuffer {
    /// Add a new second of metrics
    pub fn push(&mut self, metrics: SecondMetrics) {
        // Remove oldest if at capacity
        if self.buffer.len() >= self.max_size {
            self.buffer.pop_front();
        }
        self.buffer.push_back(metrics);
    }

    /// Check if it's time to emit metrics
    pub fn should_emit(&self) -> bool {
        self.last_emit.elapsed() >= self.emit_interval
    }

    /// Get all metrics and reset emit timer
    pub fn take_metrics(&mut self) -> Vec<SecondMetrics> {
        self.last_emit = Instant::now();
        self.buffer.iter().cloned().collect()
    }

    /// Adjust emit interval for adaptive sampling
    pub fn set_emit_interval(&mut self, interval: Duration) {
        self.emit_interval = interval;
    }
}

/// Tracks metrics for a specific event source
#[derive(Debug, Clone, Serialize)]
pub struct SourceMetrics {
    pub events_total: u64,
    pub errors_total: u64,
    pub bytes_processed: u64,
    pub last_event_time: OptionalTimestamp,
    #[serde(flatten)]
    pub custom: HashMap<String, JsonValue>,
}

/// Main metrics collector for the unified collector
pub struct CollectorMetrics {
    /// System info provider
    system: Arc<RwLock<System>>,

    /// Process start time
    start_time: Instant,

    /// Ring buffer of high-res metrics
    ring_buffer: Arc<RwLock<MetricsRingBuffer>>,

    /// Atomic counters for lock-free updates
    events_processed: Arc<AtomicU64>,
    errors_total: Arc<AtomicU64>,

    /// Per-source metrics
    source_metrics: Arc<RwLock<HashMap<String, SourceMetrics>>>,

    /// Current state for adaptive sampling
    adaptive_state: Arc<RwLock<AdaptiveState>>,
}

/// State for adaptive sampling decisions
#[derive(Debug)]
struct AdaptiveState {
    high_load: bool,
    recent_errors: bool,
    cpu_spike: bool,
}

impl CollectorMetrics {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Default for CollectorMetrics {
    fn default() -> Self {
        Self {
            system: Arc::new(RwLock::new(System::new_all())),
            start_time: Instant::now(),
            ring_buffer: Arc::new(RwLock::new(MetricsRingBuffer::new())),
            events_processed: Arc::new(AtomicU64::new(0)),
            errors_total: Arc::new(AtomicU64::new(0)),
            source_metrics: Arc::new(RwLock::new(HashMap::new())),
            adaptive_state: Arc::new(RwLock::new(AdaptiveState {
                high_load: false,
                recent_errors: false,
                cpu_spike: false,
            })),
        }
    }
}

impl CollectorMetrics {
    /// Start the metrics collection loop
    pub async fn start(self: Arc<Self>, event_tx: EventSender, db_pool: Option<DbPool>) {
        info!("Starting high-resolution metrics collection");

        // Spawn 1-second sampling task
        let sampler = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            loop {
                interval.tick().await;
                if let Err(e) = sampler.sample_metrics(db_pool.as_ref()).await {
                    warn!("Failed to sample metrics: {}", e);
                }
            }
        });

        // Spawn emission task
        let emitter = self.clone();
        tokio::spawn(async move {
            let mut check_interval = tokio::time::interval(Duration::from_secs(5));
            loop {
                check_interval.tick().await;

                // Check if we should emit
                let should_emit = {
                    let buffer = emitter.ring_buffer.read().await;
                    buffer.should_emit()
                };

                if should_emit {
                    if let Ok(event) = emitter.create_metrics_event().await {
                        if let Err(e) = event_tx.send(event).await {
                            warn!("Failed to send metrics event: {}", e);
                        }
                    }

                    // Check adaptive sampling
                    emitter.update_adaptive_sampling().await;

                    // Check for silent event sources (every 5 minutes)
                    let silent_sources = emitter.check_silent_sources(5).await;
                    if !silent_sources.is_empty() {
                        // Create event for silent sources
                        let silent_event = RawEvent {
                            id: Ulid::new(),
                            source: "sinex.monitoring.sources".to_string(),
                            event_type: "sources_silent".to_string(),
                            ts_ingest: Utc::now(),
                            ts_orig: None,
                            host: gethostname::gethostname().to_string_lossy().to_string(),
                            ingestor_version: Some(env!("CARGO_PKG_VERSION").to_string()),
                            payload_schema_id: None,
                            payload: json!({
                                "silent_sources": silent_sources,
                                "threshold_minutes": 5,
                                "timestamp": Utc::now()
                            }),
                        };

                        if let Err(e) = event_tx.send(silent_event).await {
                            warn!("Failed to send silent sources event: {}", e);
                        }
                    }

                    // Check resource exhaustion
                    let resource_status = emitter.check_resource_exhaustion().await;
                    if resource_status["status"] != "ok" {
                        // Create event for resource exhaustion
                        let resource_event = RawEvent {
                            id: Ulid::new(),
                            source: "sinex.monitoring.resources".to_string(),
                            event_type: "resource_exhaustion".to_string(),
                            ts_ingest: Utc::now(),
                            ts_orig: None,
                            host: gethostname::gethostname().to_string_lossy().to_string(),
                            ingestor_version: Some(env!("CARGO_PKG_VERSION").to_string()),
                            payload_schema_id: None,
                            payload: resource_status,
                        };

                        if let Err(e) = event_tx.send(resource_event).await {
                            warn!("Failed to send resource exhaustion event: {}", e);
                        }
                    }
                }
            }
        });
    }

    /// Sample current metrics (called every second)

    async fn sample_metrics(&self, db_pool: Option<DbPoolRef<'_>>) -> Result<()> {
        // Update system info
        {
            let mut system = self.system.write().await;
            system.refresh_processes();
            system.refresh_memory();
            system.refresh_cpu_usage();
        }

        // Get current process info
        let (cpu_percent, memory_mb) = {
            let system = self.system.read().await;
            let pid = std::process::id() as usize;

            let process = system
                .process(sysinfo::Pid::from(pid))
                .ok_or_else(|| anyhow::anyhow!("Process not found"))?;

            let cpu = process.cpu_usage();
            let memory = process.memory() / 1024; // KB to MB

            (cpu, memory)
        };

        // Get database pool stats and queue depth
        let (db_pool_size, db_pool_idle, queue_depth) = if let Some(pool) = db_pool {
            let queue_metrics = sinex_db::metrics_queries::calculate_queue_depth_metrics(pool)
                .await
                .unwrap_or_default();
            let total_queue_depth: i64 = queue_metrics.iter().map(|m| m.queue_depth).sum();
            (
                pool.size(),
                pool.num_idle() as u32,
                total_queue_depth as u32,
            )
        } else {
            (0, 0, 0)
        };

        // Get source count
        let active_sources = {
            let sources = self.source_metrics.read().await;
            sources.len()
        };

        // Create metrics snapshot
        let metrics = SecondMetrics {
            timestamp: Utc::now(),
            cpu_percent,
            memory_mb,
            events_count: self.events_processed.load(Ordering::Relaxed),
            errors_count: self.errors_total.load(Ordering::Relaxed),
            queue_depth: queue_depth as usize,
            active_sources,
            db_pool_size,
            db_pool_idle,
        };

        // Add to ring buffer
        {
            let mut buffer = self.ring_buffer.write().await;
            buffer.push(metrics);
        }

        Ok(())
    }

    /// Create a metrics event from collected data

    async fn create_metrics_event(&self) -> Result<RawEvent> {
        let timeseries = {
            let mut buffer = self.ring_buffer.write().await;
            buffer.take_metrics()
        };

        let source_metrics = {
            let sources = self.source_metrics.read().await;
            sources.clone()
        };

        let uptime_seconds = self.start_time.elapsed().as_secs();

        // Calculate aggregates over the timeseries
        let (avg_cpu, max_memory, events_per_second) = if !timeseries.is_empty() {
            let sum_cpu: f32 = timeseries.iter().map(|m| m.cpu_percent).sum();
            let avg_cpu = sum_cpu / timeseries.len() as f32;

            let max_memory = timeseries.iter().map(|m| m.memory_mb).max().unwrap_or(0);

            let events_start = timeseries.first().map(|m| m.events_count).unwrap_or(0);
            let events_end = timeseries.last().map(|m| m.events_count).unwrap_or(0);
            let duration = timeseries.len() as f64;
            let events_per_second = if duration > 0.0 {
                (events_end - events_start) as f64 / duration
            } else {
                0.0
            };

            (avg_cpu, max_memory, events_per_second)
        } else {
            (0.0, 0, 0.0)
        };

        // Get adaptive state for inclusion in metrics
        let adaptive_state = {
            let state = self.adaptive_state.read().await;
            json!({
                "high_load": state.high_load,
                "recent_errors": state.recent_errors,
                "cpu_spike": state.cpu_spike,
            })
        };

        let event = RawEvent {
            id: Ulid::new(),
            source: "sinex.metrics.collector".to_string(),
            event_type: "metrics_timeseries".to_string(),
            ts_ingest: Utc::now(),
            ts_orig: None,
            host: gethostname::gethostname().to_string_lossy().to_string(),
            ingestor_version: Some(env!("CARGO_PKG_VERSION").to_string()),
            payload_schema_id: None,
            payload: json!({
                "interval_seconds": 30,
                "uptime_seconds": uptime_seconds,

                // Aggregated metrics for quick queries
                "summary": {
                    "avg_cpu_percent": avg_cpu,
                    "max_memory_mb": max_memory,
                    "events_per_second": events_per_second,
                    "total_events": self.events_processed.load(Ordering::Relaxed),
                    "total_errors": self.errors_total.load(Ordering::Relaxed),
                },

                // Per-source breakdown
                "sources": source_metrics,

                // Full resolution timeseries (1-second granularity)
                "timeseries": {
                    "resolution_seconds": 1,
                    "datapoints": timeseries.iter().map(|m| {
                        json!({
                            "ts": m.timestamp.to_rfc3339(),
                            "cpu": m.cpu_percent,
                            "mem": m.memory_mb,
                            "events": m.events_count,
                            "errors": m.errors_count,
                            "queue": m.queue_depth,
                            "sources": m.active_sources,
                            "db_pool": m.db_pool_size,
                            "db_idle": m.db_pool_idle,
                        })
                    }).collect::<Vec<_>>(),
                },

                // Context for debugging
                "context": {
                    "version": env!("CARGO_PKG_VERSION"),
                    "rustc_version": option_env!("RUSTC_VERSION").unwrap_or("unknown"),
                    "adaptive_sampling": adaptive_state
                }
            }),
        };

        Ok(event)
    }

    /// Update adaptive sampling based on current conditions
    async fn update_adaptive_sampling(&self) {
        let recent_metrics = {
            let buffer = self.ring_buffer.read().await;
            buffer
                .buffer
                .iter()
                .rev()
                .take(10)
                .cloned()
                .collect::<Vec<_>>()
        };

        if recent_metrics.is_empty() {
            return;
        }

        // Check for high load (>1000 events/sec)
        let high_load = recent_metrics.iter().any(|m| m.events_count > 1000);

        // Check for recent errors
        let recent_errors = recent_metrics.iter().any(|m| m.errors_count > 0);

        // Check for CPU spikes (>80%)
        let cpu_spike = recent_metrics.iter().any(|m| m.cpu_percent > 80.0);

        // Update state
        {
            let mut state = self.adaptive_state.write().await;
            state.high_load = high_load;
            state.recent_errors = recent_errors;
            state.cpu_spike = cpu_spike;
        }

        // Adjust emit interval based on conditions
        let new_interval = if recent_errors || cpu_spike {
            Duration::from_secs(5) // 5 seconds during incidents
        } else if high_load {
            Duration::from_secs(10) // 10 seconds during high load
        } else {
            Duration::from_secs(30) // 30 seconds normal
        };

        {
            let mut buffer = self.ring_buffer.write().await;
            if buffer.emit_interval != new_interval {
                info!("Adjusting metrics interval to {:?}", new_interval);
                buffer.set_emit_interval(new_interval);
            }
        }
    }

    /// Increment event counter
    /// Record successful event processing with enhanced context
    pub fn record_event(&self, source: &str) {
        self.record_event_with_metrics(source, None)
    }

    /// Record event with additional metrics context
    pub fn record_event_with_metrics(&self, source: &str, bytes_processed: Option<u64>) {
        // Increment atomic counter
        self.events_processed.fetch_add(1, Ordering::Relaxed);

        // Update source-specific metrics asynchronously
        let source_name = source.to_string();
        let source_metrics = self.source_metrics.clone();
        let timestamp = Utc::now();

        tokio::spawn(async move {
            if let Ok(mut sources) = source_metrics.try_write() {
                let metrics = sources
                    .entry(source_name.clone())
                    .or_insert_with(|| SourceMetrics {
                        events_total: 0,
                        errors_total: 0,
                        bytes_processed: 0,
                        last_event_time: None,
                        custom: HashMap::new(),
                    });

                metrics.events_total += 1;
                metrics.last_event_time = Some(timestamp);

                if let Some(bytes) = bytes_processed {
                    metrics.bytes_processed += bytes;
                }

                tracing::trace!(
                    source = source_name,
                    events_total = metrics.events_total,
                    bytes_processed = metrics.bytes_processed,
                    "Event recorded successfully"
                );
            }
        });
    }

    // Legacy method removed - use record_error_with_context instead

    /// Enhanced error recording with rich context and categorization
    pub fn record_error_with_context(
        &self,
        source: &str,
        error_context: Option<&ErrorContext>,
        operation: Option<&str>,
    ) {
        // Increment atomic counter
        self.errors_total.fetch_add(1, Ordering::Relaxed);

        // Extract rich context information for structured logging
        let error_details = if let Some(ctx) = error_context {
            let error_info = ctx.to_error_info();
            json!({
                "operation": operation.unwrap_or("unknown"),
                "source": source,
                "error_type": error_info.error_type,
                "error_message": error_info.message,
                "context_data": error_info.context,
                "source_chain": error_info.source_chain,
                "timestamp": Utc::now(),
                "severity": "error"
            })
        } else {
            json!({
                "operation": operation.unwrap_or("unknown"),
                "source": source,
                "error_message": "Unknown error (no context provided)",
                "timestamp": Utc::now(),
                "severity": "error"
            })
        };

        // Structured error logging with rich context
        error!(
            source = source,
            operation = operation.unwrap_or("unknown"),
            error_details = %serde_json::to_string(&error_details).unwrap_or_default(),
            "Error recorded in metrics system"
        );

        // Update source-specific error tracking (async spawn to avoid blocking)
        let source_name = source.to_string();
        let source_metrics = self.source_metrics.clone();
        tokio::spawn(async move {
            if let Ok(mut sources) = source_metrics.try_write() {
                let metrics = sources.entry(source_name).or_insert_with(|| SourceMetrics {
                    events_total: 0,
                    errors_total: 0,
                    bytes_processed: 0,
                    last_event_time: None,
                    custom: HashMap::new(),
                });
                metrics.errors_total += 1;
            }
        });
    }

    /// Record error from Core error type with automatic context extraction
    pub fn record_core_error(&self, source: &str, error: &CoreError, operation: &str) {
        // Create ErrorContext from CoreError if not already available
        let error_context = match error {
            CoreError::Validation(msg) => ErrorContext::new(error.clone())
                .with_operation(operation)
                .with_context("source", source)
                .with_context("validation_error", msg),
            CoreError::Database(msg) => ErrorContext::new(error.clone())
                .with_operation(operation)
                .with_context("source", source)
                .with_context("database_error", msg),
            CoreError::Io(msg) => ErrorContext::new(error.clone())
                .with_operation(operation)
                .with_context("source", source)
                .with_context("io_error", msg),
            CoreError::Configuration(msg) => ErrorContext::new(error.clone())
                .with_operation(operation)
                .with_context("source", source)
                .with_context("config_error", msg),
            CoreError::Serialization(msg) => ErrorContext::new(error.clone())
                .with_operation(operation)
                .with_context("source", source)
                .with_context("serialization_error", msg),
            CoreError::Other(msg) => ErrorContext::new(error.clone())
                .with_operation(operation)
                .with_context("source", source)
                .with_context("other_error", msg),
        };

        self.record_error_with_context(source, Some(&error_context), Some(operation));
    }

    /// Update source-specific metrics with enhanced error context handling
    pub async fn update_source_metrics(
        &self,
        source: &str,
        update: impl FnOnce(&mut SourceMetrics),
    ) {
        // Enhanced metrics update with error handling and context
        let mut sources = self.source_metrics.write().await;
        {
            let metrics = sources
                .entry(source.to_string())
                .or_insert_with(|| SourceMetrics {
                    events_total: 0,
                    errors_total: 0,
                    bytes_processed: 0,
                    last_event_time: None,
                    custom: HashMap::new(),
                });

            // Apply the update function
            update(metrics);

            // Log metrics update for observability
            tracing::debug!(
                source = source,
                events_total = metrics.events_total,
                errors_total = metrics.errors_total,
                bytes_processed = metrics.bytes_processed,
                "Source metrics updated"
            );
        }
    }

    /// Check for silent event sources (sources that haven't produced events recently)
    pub async fn check_silent_sources(&self, silence_threshold_minutes: u64) -> Vec<String> {
        let threshold = Utc::now() - chrono::Duration::minutes(silence_threshold_minutes as i64);
        let sources = self.source_metrics.read().await;

        let mut silent_sources = Vec::new();
        for (source, metrics) in sources.iter() {
            if let Some(last_event) = metrics.last_event_time {
                if last_event < threshold {
                    silent_sources.push(source.clone());
                }
            } else {
                // Source has never produced events
                silent_sources.push(source.clone());
            }
        }

        if !silent_sources.is_empty() {
            warn!(
                silent_sources = ?silent_sources,
                threshold_minutes = silence_threshold_minutes,
                "Silent event sources detected"
            );
        }

        silent_sources
    }

    /// Check for resource exhaustion conditions
    pub async fn check_resource_exhaustion(&self) -> JsonValue {
        let recent_metrics = {
            let buffer = self.ring_buffer.read().await;
            buffer
                .buffer
                .iter()
                .rev()
                .take(10)
                .cloned()
                .collect::<Vec<_>>()
        };

        if recent_metrics.is_empty() {
            return json!({"status": "no_data"});
        }

        let avg_memory =
            recent_metrics.iter().map(|m| m.memory_mb).sum::<u64>() / recent_metrics.len() as u64;
        let max_memory = recent_metrics
            .iter()
            .map(|m| m.memory_mb)
            .max()
            .unwrap_or(0);
        let avg_cpu =
            recent_metrics.iter().map(|m| m.cpu_percent).sum::<f32>() / recent_metrics.len() as f32;
        let max_cpu = recent_metrics
            .iter()
            .map(|m| m.cpu_percent)
            .fold(0.0f32, |acc, x| acc.max(x));
        let avg_queue_depth =
            recent_metrics.iter().map(|m| m.queue_depth).sum::<usize>() / recent_metrics.len();
        let max_queue_depth = recent_metrics
            .iter()
            .map(|m| m.queue_depth)
            .max()
            .unwrap_or(0);

        // Define warning thresholds
        let memory_warning_mb = 1024; // 1GB
        let memory_critical_mb = 2048; // 2GB
        let cpu_warning_percent = 70.0;
        let cpu_critical_percent = 90.0;
        let queue_warning_depth = 1000;
        let queue_critical_depth = 5000;

        let mut warnings = Vec::new();
        let mut criticals = Vec::new();

        // Memory checks
        if avg_memory > memory_critical_mb {
            criticals.push(format!("Memory usage critical: {}MB average", avg_memory));
        } else if avg_memory > memory_warning_mb {
            warnings.push(format!("Memory usage high: {}MB average", avg_memory));
        }

        // CPU checks
        if avg_cpu > cpu_critical_percent {
            criticals.push(format!("CPU usage critical: {:.1}% average", avg_cpu));
        } else if avg_cpu > cpu_warning_percent {
            warnings.push(format!("CPU usage high: {:.1}% average", avg_cpu));
        }

        // Queue depth checks
        if avg_queue_depth > queue_critical_depth {
            criticals.push(format!("Queue depth critical: {} average", avg_queue_depth));
        } else if avg_queue_depth > queue_warning_depth {
            warnings.push(format!("Queue depth high: {} average", avg_queue_depth));
        }

        let status = if !criticals.is_empty() {
            "critical"
        } else if !warnings.is_empty() {
            "warning"
        } else {
            "ok"
        };

        if !warnings.is_empty() || !criticals.is_empty() {
            if !criticals.is_empty() {
                error!(criticals = ?criticals, "Critical resource exhaustion conditions detected");
            }
            if !warnings.is_empty() {
                warn!(warnings = ?warnings, "Resource exhaustion warnings detected");
            }
        }

        json!({
            "status": status,
            "timestamp": Utc::now(),
            "metrics": {
                "memory_mb": {
                    "average": avg_memory,
                    "maximum": max_memory,
                    "warning_threshold": memory_warning_mb,
                    "critical_threshold": memory_critical_mb
                },
                "cpu_percent": {
                    "average": avg_cpu,
                    "maximum": max_cpu,
                    "warning_threshold": cpu_warning_percent,
                    "critical_threshold": cpu_critical_percent
                },
                "queue_depth": {
                    "average": avg_queue_depth,
                    "maximum": max_queue_depth,
                    "warning_threshold": queue_warning_depth,
                    "critical_threshold": queue_critical_depth
                }
            },
            "warnings": warnings,
            "criticals": criticals
        })
    }

    /// Get comprehensive error statistics with rich context
    pub async fn get_error_statistics(&self) -> JsonValue {
        let sources = self.source_metrics.read().await;
        let total_errors = self.errors_total.load(Ordering::Relaxed);
        let total_events = self.events_processed.load(Ordering::Relaxed);

        let error_rate = if total_events > 0 {
            (total_errors as f64 / total_events as f64) * 100.0
        } else {
            0.0
        };

        let mut source_error_breakdown = HashMap::new();
        for (source, metrics) in sources.iter() {
            let source_error_rate = if metrics.events_total > 0 {
                (metrics.errors_total as f64 / metrics.events_total as f64) * 100.0
            } else {
                0.0
            };

            source_error_breakdown.insert(
                source.clone(),
                json!({
                    "errors_total": metrics.errors_total,
                    "events_total": metrics.events_total,
                    "error_rate_percent": source_error_rate,
                    "last_event_time": metrics.last_event_time
                }),
            );
        }

        json!({
            "timestamp": Utc::now(),
            "total_errors": total_errors,
            "total_events": total_events,
            "overall_error_rate_percent": error_rate,
            "source_breakdown": source_error_breakdown,
            "monitoring_period": "session"
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ring_buffer() {
        let mut buffer = MetricsRingBuffer::new();

        // Add some metrics
        for i in 0..5 {
            let metrics = SecondMetrics {
                timestamp: Utc::now(),
                cpu_percent: i as f32,
                memory_mb: 100 + i,
                events_count: i * 10,
                errors_count: 0,
                queue_depth: 0,
                active_sources: 1,
                db_pool_size: 10,
                db_pool_idle: 5,
            };
            buffer.push(metrics);
        }

        assert_eq!(buffer.buffer.len(), 5);

        // Check emit timing
        assert!(!buffer.should_emit()); // Too soon

        // Take metrics
        let metrics = buffer.take_metrics();
        assert_eq!(metrics.len(), 5);
        assert_eq!(metrics[0].cpu_percent, 0.0);
        assert_eq!(metrics[4].cpu_percent, 4.0);
    }

    #[tokio::test]
    async fn test_adaptive_sampling() {
        let metrics = Arc::new(CollectorMetrics::new());

        // Simulate high CPU
        {
            let mut buffer = metrics.ring_buffer.write().await;
            for _ in 0..5 {
                buffer.push(SecondMetrics {
                    timestamp: Utc::now(),
                    cpu_percent: 85.0, // High CPU
                    memory_mb: 512,
                    events_count: 100,
                    errors_count: 0,
                    queue_depth: 0,
                    active_sources: 1,
                    db_pool_size: 10,
                    db_pool_idle: 5,
                });
            }
        }

        // Update adaptive sampling
        metrics.update_adaptive_sampling().await;

        // Check that interval was reduced
        let interval = {
            let buffer = metrics.ring_buffer.read().await;
            buffer.emit_interval
        };

        assert_eq!(interval, Duration::from_secs(5)); // Should be in incident mode
    }
}

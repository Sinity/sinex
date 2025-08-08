//! # Telemetry Event Emission System
//!
//! This module provides telemetry event emission for long-term metrics storage.
//! It works alongside Prometheus metrics to emit periodic summary events that
//! can be stored as regular Sinex events for historical analysis.
//!
//! ## Architecture Overview
//!
//! Sinex uses a hybrid approach for metrics and telemetry that combines:
//! 1. **Real-time metrics** via Prometheus for operational monitoring
//! 2. **Historical telemetry** stored as events for long-term analysis
//!
//! This design avoids the overhead of storing every metric update while preserving
//! both real-time visibility and historical data.
//!
//! ### Real-time Metrics (Prometheus)
//!
//! The metrics library provides automatic instrumentation through procedural macros:
//!
//! ```rust,ignore
//! #[auto_metrics]
//! async fn process_events(&self, events: Vec<Event>) -> Result<()> {
//!     // Automatically tracks:
//!     // - function_calls_total{status="success|error"}
//!     // - function_duration_seconds (histogram)
//!     // - function_errors_total
//!     // - function_active_calls (gauge)
//! }
//! ```
//!
//! These metrics are:
//! - Stored in memory using the Prometheus client library
//! - Exposed via `/metrics` endpoint for Prometheus scraping
//! - Visualized in Grafana dashboards
//! - Typically retained for 15-90 days in Prometheus
//!
//! ### Historical Telemetry (Events)
//!
//! Instead of storing every metric update in a database table, components emit
//! periodic summary events with fine-grained categorization:
//!
//! ```rust,ignore
//! // Event processing metrics (per component, every 5 minutes)
//! emit_event("sinex.telemetry", "events.processed", json!({
//!     "component": "fs-watcher",
//!     "period_seconds": 300,
//!     "count": 1523,
//!     "by_type": {
//!         "file.created": 234,
//!         "file.modified": 1289,
//!         "directory.created": 0
//!     }
//! })?);
//!
//! // Component performance metrics (every 5 minutes)
//! emit_event("sinex.telemetry", "operation.performance", json!({
//!     "component": "fs-watcher",
//!     "operation": "scan_directory",
//!     "period_seconds": 300,
//!     "count": 45,
//!     "duration_ms": {
//!         "p50": 120,
//!         "p95": 280,
//!         "p99": 450,
//!         "max": 823
//!     }
//! })?);
//! ```
//!
//! ## Design Decisions
//!
//! ### Per-Component vs System-Wide Metrics
//!
//! We emit telemetry events **per component** rather than aggregating across all
//! components because:
//!
//! 1. **Independent scaling**: Each component may have different performance characteristics
//! 2. **Debugging**: Easier to identify which component is having issues
//! 3. **Flexible aggregation**: Can still aggregate in queries when needed
//! 4. **Component lifecycle**: Components start/stop independently
//!
//! For system-wide metrics (CPU, memory, disk), we emit separate `system.resources` events.
//!
//! ### Event Granularity
//!
//! Events are categorized by metric type rather than bundling all metrics together:
//!
//! - `events.processed` - Event throughput metrics
//! - `operation.performance` - Operation latency metrics  
//! - `resource.usage` - Component resource consumption
//! - `system.resources` - System-wide resources
//! - `errors.summary` - Error occurrences (batched every 5 minutes if present)
//!
//! This enables:
//! - Type-specific queries without parsing large payloads
//! - Different retention policies per metric type
//! - Efficient aggregation queries
//! - Clear event stream semantics
//!
//! ## Benefits
//!
//! This hybrid approach provides:
//!
//! 1. **Low overhead**: ~2000 telemetry events/day vs 400k+ individual metric updates
//! 2. **Real-time monitoring**: Prometheus provides sub-minute visibility
//! 3. **Long-term storage**: Events can be retained indefinitely
//! 4. **Unified queries**: Telemetry can be correlated with other events
//! 5. **Flexible granularity**: Different metrics can have different summary periods
//! 6. **Component isolation**: Issues in one component are immediately visible
//!
//! ## Usage
//!
//! ### Basic Setup
//!
//! ```rust,ignore
//! use sinex_telemetry::telemetry::{TelemetryAccumulator, SystemTelemetryEmitter};
//! use tokio::sync::mpsc;
//! use std::time::Duration;
//!
//! // Create event channel
//! let (tx, rx) = mpsc::unbounded_channel();
//!
//! // Create telemetry accumulator
//! let telemetry = TelemetryAccumulator::new("my-component")
//!     .with_event_sender(tx.clone())
//!     .with_interval(Duration::from_secs(300)); // 5 minutes
//!
//! // Set as global telemetry
//! set_global_telemetry(telemetry.clone()).await;
//!
//! // Spawn background emitter
//! telemetry.spawn_emitter();
//!
//! // Also emit system-wide metrics
//! let system_emitter = SystemTelemetryEmitter::new(tx);
//! system_emitter.spawn_emitter();
//! ```
//!
//! ### Recording Metrics
//!
//! ```rust,ignore
//! // Record event processing
//! telemetry.record_event_processed("file.created", 12.5);
//!
//! // Record operation latency
//! telemetry.record_operation_latency("scan_directory", 145.2);
//!
//! // Record resource usage
//! telemetry.record_resource_usage(memory_mb, cpu_percent);
//!
//! // Record errors
//! telemetry.record_error("io_error");
//! ```
//!
//! ### Integration with Auto-Metrics
//!
//! When using the `#[auto_metrics]` macro, telemetry is automatically recorded:
//!
//! ```rust,ignore
//! #[auto_metrics]
//! async fn my_function() -> Result<()> {
//!     // Function execution time and errors are automatically
//!     // recorded in both Prometheus and telemetry
//! }
//! ```
//!
//! ## Querying Telemetry
//!
//! ### Recent Performance
//! ```sql
//! SELECT
//!     payload->>'component' as component,
//!     payload->>'operation' as operation,
//!     payload->'duration_ms'->>'p50' as p50_latency,
//!     payload->'duration_ms'->>'p95' as p95_latency
//! FROM core.events
//! WHERE source = 'sinex.telemetry'
//!   AND event_type = 'operation.performance'
//!   AND ts_ingest > NOW() - INTERVAL '1 hour'
//! ORDER BY ts_ingest DESC;
//! ```
//!
//! ### Resource Usage Trends
//! ```sql
//! SELECT
//!     date_trunc('hour', ts_ingest) as hour,
//!     payload->>'component' as component,
//!     AVG((payload->'cpu_percent'->>'avg')::float) as avg_cpu,
//!     MAX((payload->'memory_mb'->>'peak')::float) as max_memory
//! FROM core.events
//! WHERE source = 'sinex.telemetry'
//!   AND event_type = 'resource.usage'
//!   AND ts_ingest > NOW() - INTERVAL '24 hours'
//! GROUP BY hour, component
//! ORDER BY hour, component;
//! ```
//!
//! ## Summary Periods
//!
//! Recommended summary intervals:
//! - **System resources**: 1 minute (1440 events/day)
//! - **Component metrics**: 5 minutes (288 events/day per component)
//! - **Error summaries**: On occurrence or 5 minutes if errors present
//! - **Daily rollups**: For long-term trending

use crate::models::RawEvent;
use chrono::{DateTime, Utc};
use once_cell::sync::Lazy;
use parking_lot::RwLock;
use serde_json::{json, Value as JsonValue};
use crate::types::events::{
    ComponentResourceUsagePayload, ErrorsSummaryPayload, EventsProcessedPayload,
    OperationPerformancePayload, SystemResourcesPayload,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, RwLock as TokioRwLock};
use tracing::{debug, error, info};

/// Type alias for event sender
pub type EventSender = mpsc::UnboundedSender<Event>;

/// Telemetry accumulator that collects metrics and emits periodic summary events.
///
/// This struct accumulates various metrics over a time period and periodically
/// emits them as Sinex events for long-term storage and analysis.
///
/// # Example
///
/// ```rust,ignore
/// let accumulator = TelemetryAccumulator::new("my-service")
///     .with_event_sender(tx)
///     .with_interval(Duration::from_secs(300));
///
/// // Record metrics
/// accumulator.record_event_processed("user.created", 15.3);
/// accumulator.record_operation_latency("database_query", 45.7);
///
/// // Metrics are automatically emitted every 5 minutes
/// ```
#[derive(Clone)]
pub struct TelemetryAccumulator {
    component: String,
    event_sender: Option<EventSender>,
    state: Arc<RwLock<TelemetryState>>,
    emission_interval: Duration,
}

#[derive(Debug)]
struct TelemetryState {
    // Event processing metrics
    event_counts: HashMap<String, u64>,
    event_latencies: Vec<f64>,

    // Performance metrics
    operation_latencies: HashMap<String, Vec<f64>>,

    // Resource usage
    memory_samples: Vec<f64>,
    cpu_samples: Vec<f64>,

    // Error tracking
    error_counts: HashMap<String, u64>,

    // Timing
    last_emission: Instant,
    period_start: DateTime<Utc>,
}

impl TelemetryState {
    fn new() -> Self {
        Self {
            event_counts: HashMap::new(),
            event_latencies: Vec::new(),
            operation_latencies: HashMap::new(),
            memory_samples: Vec::new(),
            cpu_samples: Vec::new(),
            error_counts: HashMap::new(),
            last_emission: Instant::now(),
            period_start: Utc::now(),
        }
    }

    fn reset(&mut self) {
        self.event_counts.clear();
        self.event_latencies.clear();
        self.operation_latencies.clear();
        self.memory_samples.clear();
        self.cpu_samples.clear();
        self.error_counts.clear();
        self.last_emission = Instant::now();
        self.period_start = Utc::now();
    }
}

impl TelemetryAccumulator {
    /// Create a new telemetry accumulator for the specified component.
    ///
    /// # Arguments
    ///
    /// * `component` - Name of the component (e.g., "fs-watcher", "ingestd")
    pub fn new(component: impl Into<String>) -> Self {
        Self {
            component: component.into(),
            event_sender: None,
            state: Arc::new(RwLock::new(TelemetryState::new())),
            emission_interval: Duration::from_secs(300), // 5 minutes default
        }
    }

    /// Set the event sender for telemetry emission.
    ///
    /// Telemetry events will be sent through this channel.
    pub fn with_event_sender(mut self, sender: EventSender) -> Self {
        self.event_sender = Some(sender);
        self
    }

    /// Set custom emission interval.
    ///
    /// # Arguments
    ///
    /// * `interval` - How often to emit telemetry events
    pub fn with_interval(mut self, interval: Duration) -> Self {
        self.emission_interval = interval;
        self
    }

    /// Record an event being processed.
    ///
    /// # Arguments
    ///
    /// * `event_type` - Type of event (e.g., "file.created", "user.login")
    /// * `duration_ms` - Processing time in milliseconds
    pub fn record_event_processed(&self, event_type: &str, duration_ms: f64) {
        let mut state = self.state.write();
        *state
            .event_counts
            .entry(event_type.to_string())
            .or_insert(0) += 1;
        state.event_latencies.push(duration_ms);
    }

    /// Record an operation latency.
    ///
    /// # Arguments
    ///
    /// * `operation` - Name of the operation (e.g., "scan_directory", "db_query")
    /// * `duration_ms` - Operation duration in milliseconds
    pub fn record_operation_latency(&self, operation: &str, duration_ms: f64) {
        let mut state = self.state.write();
        state
            .operation_latencies
            .entry(operation.to_string())
            .or_insert_with(Vec::new)
            .push(duration_ms);
    }

    /// Record a resource usage sample.
    ///
    /// # Arguments
    ///
    /// * `memory_mb` - Current memory usage in megabytes
    /// * `cpu_percent` - Current CPU usage percentage (0-100)
    pub fn record_resource_usage(&self, memory_mb: f64, cpu_percent: f64) {
        let mut state = self.state.write();
        state.memory_samples.push(memory_mb);
        state.cpu_samples.push(cpu_percent);
    }

    /// Record an error occurrence.
    ///
    /// # Arguments
    ///
    /// * `error_type` - Type of error (e.g., "io_error", "validation_error")
    pub fn record_error(&self, error_type: &str) {
        let mut state = self.state.write();
        *state
            .error_counts
            .entry(error_type.to_string())
            .or_insert(0) += 1;
    }

    /// Check if it's time to emit telemetry.
    pub fn should_emit(&self) -> bool {
        let state = self.state.read();
        state.last_emission.elapsed() >= self.emission_interval
    }

    /// Emit all accumulated telemetry events.
    ///
    /// This method is typically called automatically by the background emitter,
    /// but can be called manually if needed.
    pub async fn emit_telemetry(&self) -> Result<(), Box<dyn std::error::Error>> {
        let Some(ref sender) = self.event_sender else {
            debug!("No event sender configured for telemetry");
            return Ok(());
        };

        // Collect data and reset state within lock scope
        let (_period_seconds, events_to_emit) = {
            let mut state = self.state.write();
            let period_seconds = state.last_emission.elapsed().as_secs();

            // Skip if no data collected
            if state.event_counts.is_empty()
                && state.operation_latencies.is_empty()
                && state.memory_samples.is_empty()
                && state.error_counts.is_empty()
            {
                return Ok(());
            }

            let mut events = Vec::new();
            // Will create events using schemaless builder

            // Emit events.processed telemetry
            if !state.event_counts.is_empty() {
                // Need to prepare data for the typed payload
                let total_events = state.event_counts.values().sum::<u64>();
                let processing_rate = if period_seconds > 0 {
                    total_events as f64 / period_seconds as f64
                } else {
                    0.0
                };

                // Build events_per_source map (we only have component name)
                let mut events_per_source = HashMap::new();
                events_per_source.insert(self.component.clone(), total_events);

                // The event_counts map has event types as keys
                let events_per_type = state.event_counts.clone();

                events.push(Event::from_payload(EventsProcessedPayload {
                    time_range_seconds: period_seconds,
                    total_events,
                    events_per_source,
                    events_per_type,
                    processing_rate,
                }));
            }

            // Emit operation performance telemetry
            for (operation, latencies) in &state.operation_latencies {
                if !latencies.is_empty() {
                    // Calculate average duration
                    let avg_duration = latencies.iter().sum::<f64>() / latencies.len() as f64;

                    // Create metrics map from percentiles
                    let percentiles = calculate_percentiles(latencies);
                    let mut metrics = HashMap::new();
                    metrics.insert("component".to_string(), json!(self.component));
                    metrics.insert("period_seconds".to_string(), json!(period_seconds));
                    metrics.insert("count".to_string(), json!(latencies.len()));
                    metrics.insert("duration_ms".to_string(), percentiles);

                    events.push(Event::from_payload(OperationPerformancePayload {
                        operation_name: operation.clone(),
                        duration_ms: avg_duration as u64,
                        items_processed: latencies.len() as u64,
                        success: true,
                        error: None,
                        metrics,
                    }));
                }
            }

            // Emit resource usage telemetry
            if !state.memory_samples.is_empty() {
                let memory_mb = json!({
                    "current": state.memory_samples.last().copied().unwrap_or(0.0),
                    "avg": calculate_average(&state.memory_samples),
                    "peak": state.memory_samples.iter().fold(0.0_f64, |a, &b| a.max(b)),
                });

                let cpu_percent = json!({
                    "avg": calculate_average(&state.cpu_samples),
                    "peak": state.cpu_samples.iter().fold(0.0_f64, |a, &b| a.max(b)),
                });

                events.push(Event::from_payload(ComponentResourceUsagePayload {
                    component: self.component.clone(),
                    period_seconds,
                    memory_mb,
                    cpu_percent,
                }));
            }

            // Emit error telemetry
            if !state.error_counts.is_empty() {
                let total_errors = state.error_counts.values().sum::<u64>();
                let error_rate = if period_seconds > 0 {
                    total_errors as f64 / period_seconds as f64
                } else {
                    0.0
                };

                // Build errors_by_severity map (we don't have severity info, so use "error")
                let mut errors_by_severity = HashMap::new();
                errors_by_severity.insert("error".to_string(), total_errors);

                // Build errors_by_component map
                let mut errors_by_component = HashMap::new();
                errors_by_component.insert(self.component.clone(), total_errors);

                events.push(Event::from_payload(ErrorsSummaryPayload {
                    time_range_seconds: period_seconds,
                    total_errors,
                    errors_by_severity,
                    errors_by_component,
                    error_rate,
                }));
            }

            // Reset state for next period
            state.reset();

            (period_seconds, events)
        }; // Lock is dropped here

        // Send all events after releasing the lock
        for event in events_to_emit {
            sender.send(event)?;
        }

        info!(
            component = %self.component,
            "Emitted telemetry events"
        );

        Ok(())
    }

    /// Start a background task that emits telemetry periodically.
    ///
    /// The returned handle can be used to cancel the background task.
    pub fn spawn_emitter(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(self.emission_interval);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                interval.tick().await;

                if let Err(e) = self.emit_telemetry().await {
                    error!(
                        component = %self.component,
                        error = %e,
                        "Failed to emit telemetry"
                    );
                }
            }
        })
    }
}

/// Calculate percentiles from a slice of values.
///
/// Returns a JSON object with p50, p95, p99, max, and min values.
fn calculate_percentiles(values: &[f64]) -> JsonValue {
    if values.is_empty() {
        return json!({});
    }

    let mut sorted = values.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

    let len = sorted.len();
    json!({
        "p50": sorted[len / 2],
        "p95": sorted[len * 95 / 100],
        "p99": sorted[len * 99 / 100],
        "max": sorted[len - 1],
        "min": sorted[0],
    })
}

/// Calculate average from a slice of values.
fn calculate_average(values: &[f64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    values.iter().sum::<f64>() / values.len() as f64
}

/// System-wide telemetry emitter for collecting OS-level metrics.
///
/// This emitter collects system-wide metrics like CPU usage, memory usage,
/// and disk I/O, emitting them as telemetry events.
///
/// # Example
///
/// ```rust,ignore
/// let system_emitter = SystemTelemetryEmitter::new(event_sender);
/// system_emitter.spawn_emitter();
/// ```
pub struct SystemTelemetryEmitter {
    event_sender: EventSender,
    emission_interval: Duration,
}

impl SystemTelemetryEmitter {
    /// Create a new system telemetry emitter.
    pub fn new(event_sender: EventSender) -> Self {
        Self {
            event_sender,
            emission_interval: Duration::from_secs(60), // 1 minute for system metrics
        }
    }

    /// Set custom emission interval.
    pub fn with_interval(mut self, interval: Duration) -> Self {
        self.emission_interval = interval;
        self
    }

    /// Emit system-wide resource telemetry.
    ///
    /// This collects current system metrics and emits them as a telemetry event.
    pub async fn emit_system_resources(&self) -> Result<(), Box<dyn std::error::Error>> {
        // TODO: Implement actual system resource collection using sysinfo crate
        // For now, emit placeholder data
        let event = Event::from_payload(SystemResourcesPayload {
            cpu_usage_percent: 0.0,
            memory_usage_bytes: 0,
            memory_total_bytes: 0,
            disk_usage_bytes: 0,
            disk_total_bytes: 0,
            open_file_descriptors: 0,
            network_bytes_sent: 0,
            network_bytes_received: 0,
        });

        self.event_sender.send(event)?;
        Ok(())
    }

    /// Start system telemetry emission in the background.
    ///
    /// The returned handle can be used to cancel the background task.
    pub fn spawn_emitter(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(self.emission_interval);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                interval.tick().await;

                if let Err(e) = self.emit_system_resources().await {
                    error!(
                        error = %e,
                        "Failed to emit system telemetry"
                    );
                }
            }
        })
    }
}

/// Global telemetry registry for accessing the current telemetry accumulator.
static GLOBAL_TELEMETRY: Lazy<Arc<TokioRwLock<Option<TelemetryAccumulator>>>> =
    Lazy::new(|| Arc::new(TokioRwLock::new(None)));

/// Set the global telemetry accumulator.
///
/// This is typically called during application initialization to make
/// telemetry available to the auto-metrics infrastructure.
pub async fn set_global_telemetry(accumulator: TelemetryAccumulator) {
    let mut global = GLOBAL_TELEMETRY.write().await;
    *global = Some(accumulator);
}

/// Get the global telemetry accumulator.
///
/// Returns None if telemetry has not been initialized.
pub async fn get_global_telemetry() -> Option<TelemetryAccumulator> {
    let global = GLOBAL_TELEMETRY.read().await;
    global.clone()
}

/// Record a function call in telemetry.
///
/// This is called automatically by the auto-metrics infrastructure and
/// should not typically be called directly.
///
/// # Arguments
///
/// * `module` - Module path of the function
/// * `function` - Function name
/// * `duration_ms` - Execution time in milliseconds
/// * `error` - Whether the function resulted in an error
pub fn record_function_telemetry(module: &str, function: &str, duration_ms: f64, error: bool) {
    // Use tokio::spawn to avoid blocking
    let module = module.to_string();
    let function = function.to_string();
    tokio::spawn(async move {
        if let Some(telemetry) = get_global_telemetry().await {
            let operation = format!("{}::{}", module, function);
            telemetry.record_operation_latency(&operation, duration_ms);
            if error {
                telemetry.record_error("function_error");
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use color_eyre::eyre::eyre;
    use serde_json::json;
    use sinex_test_utils::prelude::*;
    use crate::types::domain::EventType;
    use std::time::Duration;
    use tokio::sync::mpsc;

    #[sinex_test]
    async fn test_telemetry_accumulator_basic(ctx: TestContext) -> color_eyre::eyre::Result<()> {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let accumulator = TelemetryAccumulator::new("test-component")
            .with_event_sender(tx.clone())
            .with_interval(Duration::from_millis(100));

        // Record various metrics
        accumulator.record_event_processed("file.created", 10.0);
        accumulator.record_event_processed("file.created", 20.0);
        accumulator.record_event_processed("file.modified", 5.0);

        accumulator.record_operation_latency("scan_directory", 150.0);
        accumulator.record_operation_latency("scan_directory", 200.0);

        accumulator.record_resource_usage(100.0, 25.0);
        accumulator.record_resource_usage(120.0, 30.0);

        accumulator.record_error("io_error");
        accumulator.record_error("io_error");
        accumulator.record_error("permission_denied");

        // Emit telemetry
        accumulator
            .emit_telemetry()
            .await
            .map_err(|e| eyre!("Emit failed: {}", e))?;

        // Collect emitted events
        let mut events = Vec::new();
        while let Ok(event) = rx.try_recv() {
            events.push(event);
        }

        // Verify events were emitted
        ctx.assert("telemetry events emitted")
            .that(events.len() >= 3, "Should emit multiple event types")?;

        // Check event types
        let event_types: Vec<_> = events.iter().map(|e| e.event_type.as_str()).collect();
        ctx.assert("event types")
            .that(
                event_types.contains(&"events.processed"),
                "Should have events.processed",
            )?
            .that(
                event_types.contains(&"operation.performance"),
                "Should have operation.performance",
            )?
            .that(
                event_types.contains(&"resource.usage"),
                "Should have resource.usage",
            )?
            .that(
                event_types.contains(&"errors.summary"),
                "Should have errors.summary",
            )?;

        // Verify event content
        for event in &events {
            ctx.assert("event structure")
                .eq(&event.source.as_str(), &"sinex.telemetry")?;

            match event.event_type.as_str() {
                "events.processed" => {
                    let payload = &event.payload;
                    ctx.assert("events.processed payload")
                        .eq(&payload["component"], &json!("test-component"))?
                        .that(
                            payload["count"].as_u64().unwrap() == 3,
                            "Should have 3 events",
                        )?
                        .that(
                            payload["by_type"]["file.created"].as_u64().unwrap() == 2,
                            "Should have 2 file.created",
                        )?
                        .that(
                            payload["by_type"]["file.modified"].as_u64().unwrap() == 1,
                            "Should have 1 file.modified",
                        )?;
                }
                "operation.performance" => {
                    let payload = &event.payload;
                    ctx.assert("operation.performance payload")
                        .eq(&payload["operation"], &json!("scan_directory"))?
                        .that(
                            payload["count"].as_u64().unwrap() == 2,
                            "Should have 2 operations",
                        )?;
                }
                "resource.usage" => {
                    let payload = &event.payload;
                    ctx.assert("resource.usage payload")
                        .that(
                            payload["memory_mb"]["avg"].as_f64().unwrap() == 110.0,
                            "Should have correct avg memory",
                        )?
                        .that(
                            payload["cpu_percent"]["peak"].as_f64().unwrap() == 30.0,
                            "Should have correct peak CPU",
                        )?;
                }
                "errors.summary" => {
                    let payload = &event.payload;
                    ctx.assert("errors.summary payload")
                        .that(
                            payload["total_errors"].as_u64().unwrap() == 3,
                            "Should have 3 total errors",
                        )?
                        .that(
                            payload["by_type"]["io_error"].as_u64().unwrap() == 2,
                            "Should have 2 io_errors",
                        )?
                        .that(
                            payload["by_type"]["permission_denied"].as_u64().unwrap() == 1,
                            "Should have 1 permission_denied",
                        )?;
                }
                _ => {}
            }
        }

        Ok(())
    }

    #[sinex_test]
    async fn test_telemetry_state_reset(ctx: TestContext) -> Result<()> {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let accumulator = TelemetryAccumulator::new("reset-test").with_event_sender(tx);

        // Record first batch
        accumulator.record_event_processed("event.one", 10.0);
        accumulator.record_error("error.one");

        // Emit and check
        accumulator
            .emit_telemetry()
            .await
            .map_err(|e| eyre!("Emit failed: {}", e))?;

        let mut first_batch = Vec::new();
        while let Ok(event) = rx.try_recv() {
            first_batch.push(event);
        }

        ctx.assert("first batch").not_empty(&first_batch)?;

        // Record second batch
        accumulator.record_event_processed("event.two", 20.0);
        accumulator.record_error("error.two");

        // Emit again
        accumulator
            .emit_telemetry()
            .await
            .map_err(|e| eyre!("Emit failed: {}", e))?;

        let mut second_batch = Vec::new();
        while let Ok(event) = rx.try_recv() {
            second_batch.push(event);
        }

        // Verify second batch doesn't contain first batch data
        for event in &second_batch {
            if event.event_type.as_str() == "events.processed" {
                ctx.assert("events reset").that(
                    !event.payload["by_type"]
                        .as_object()
                        .unwrap()
                        .contains_key("event.one"),
                    "Should not contain first batch events",
                )?;
            } else if event.event_type.as_str() == "errors.summary" {
                ctx.assert("errors reset").that(
                    !event.payload["by_type"]
                        .as_object()
                        .unwrap()
                        .contains_key("error.one"),
                    "Should not contain first batch errors",
                )?;
            }
        }

        Ok(())
    }

    #[sinex_test]
    #[case("empty", vec![])]
    #[case("single", vec![100.0])]
    #[case("two", vec![50.0, 150.0])]
    #[case("many", vec![10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0, 100.0])]
    #[case("duplicates", vec![50.0, 50.0, 50.0, 50.0, 50.0])]
    #[case("unsorted", vec![100.0, 10.0, 50.0, 30.0, 80.0])]
    async fn test_telemetry_percentile_calculation(
        ctx: TestContext,
        #[case] name: &str,
        #[case] values: Vec<f64>,
    ) -> color_eyre::eyre::Result<()> {
        // Test percentile edge cases
        let result = calculate_percentiles(&values);

        match name {
            "empty" => {
                ctx.assert("empty percentiles").eq(&result, &json!({}))?;
            }
            "single" => {
                ctx.assert("single value percentiles")
                    .eq(&result["p50"], &json!(100.0))?
                    .eq(&result["p95"], &json!(100.0))?
                    .eq(&result["p99"], &json!(100.0))?
                    .eq(&result["min"], &json!(100.0))?
                    .eq(&result["max"], &json!(100.0))?;
            }
            "two" => {
                ctx.assert("two value percentiles")
                    .eq(&result["p50"], &json!(150.0))? // Index 1
                    .eq(&result["min"], &json!(50.0))?
                    .eq(&result["max"], &json!(150.0))?;
            }
            "many" => {
                ctx.assert("many value percentiles")
                    .eq(&result["p50"], &json!(60.0))? // Index 5
                    .eq(&result["p95"], &json!(100.0))? // Index 9
                    .eq(&result["p99"], &json!(100.0))? // Index 9
                    .eq(&result["min"], &json!(10.0))?
                    .eq(&result["max"], &json!(100.0))?;
            }
            "duplicates" => {
                ctx.assert("duplicate value percentiles")
                    .eq(&result["p50"], &json!(50.0))?
                    .eq(&result["p95"], &json!(50.0))?
                    .eq(&result["p99"], &json!(50.0))?;
            }
            "unsorted" => {
                // Should be sorted internally
                ctx.assert("unsorted value percentiles")
                    .eq(&result["min"], &json!(10.0))?
                    .eq(&result["max"], &json!(100.0))?;
            }
            _ => {}
        }

        Ok(())
    }

    #[sinex_test]
    async fn test_telemetry_background_emitter(ctx: TestContext) -> Result<()> {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let accumulator = TelemetryAccumulator::new("background-test")
            .with_event_sender(tx)
            .with_interval(Duration::from_millis(100));

        // Start background emitter
        let handle = accumulator.clone().spawn_emitter();

        // Record metrics
        accumulator.record_event_processed("background.event", 15.0);

        // Wait for emission
        tokio::time::sleep(Duration::from_millis(150)).await;

        // Should have received events
        let mut received = false;
        while let Ok(event) = rx.try_recv() {
            if event.event_type == EventType::new("events.processed") {
                received = true;
                ctx.assert("background emission")
                    .eq(&event.payload["component"], &json!("background-test"))?;
            }
        }

        ctx.assert("background emitter")
            .that(received, "Should have received background emission")?;

        // Cleanup
        handle.abort();

        Ok(())
    }

    #[sinex_test]
    async fn test_system_telemetry_emitter(ctx: TestContext) -> Result<()> {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let emitter = SystemTelemetryEmitter::new(tx).with_interval(Duration::from_millis(100));

        // Emit system resources
        emitter
            .emit_system_resources()
            .await
            .map_err(|e| eyre!("Emit failed: {}", e))?;

        // Check event
        let event = rx.recv().await.unwrap();

        ctx.assert("system telemetry")
            .eq(&event.source.as_str(), &"sinex.telemetry")?
            .eq(&event.event_type, &EventType::new("system.resources"))?;

        let payload = &event.payload;
        ctx.assert("system payload")
            .that(payload["cpu"].is_object(), "Should have CPU metrics")?
            .that(payload["memory"].is_object(), "Should have memory metrics")?
            .that(
                payload["disk_io"].is_object(),
                "Should have disk I/O metrics",
            )?;

        Ok(())
    }

    #[sinex_test]
    async fn test_global_telemetry_integration(ctx: TestContext) -> Result<()> {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let accumulator = TelemetryAccumulator::new("global-test").with_event_sender(tx);

        // Set as global
        set_global_telemetry(accumulator.clone()).await;

        // Record via global
        record_function_telemetry("test_module", "test_function", 25.5, false);

        // Wait for async recording
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Emit to see the recorded data
        accumulator
            .emit_telemetry()
            .await
            .map_err(|e| eyre!("Emit failed: {}", e))?;

        // Verify
        let mut found_operation = false;
        while let Ok(event) = rx.try_recv() {
            if event.event_type == EventType::new("operation.performance") {
                found_operation = true;
                ctx.assert("global telemetry recording").eq(
                    &event.payload["operation"],
                    &json!("test_module::test_function"),
                )?;
            }
        }

        ctx.assert("global telemetry").that(
            found_operation,
            "Should have recorded operation via global telemetry",
        )?;

        Ok(())
    }

    #[sinex_test]
    async fn test_telemetry_no_data_emission(ctx: TestContext) -> Result<()> {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let accumulator = TelemetryAccumulator::new("no-data-test").with_event_sender(tx);

        // Emit without recording any data
        accumulator
            .emit_telemetry()
            .await
            .map_err(|e| eyre!("Emit failed: {}", e))?;

        // Should not emit any events
        let event = rx.try_recv();
        ctx.assert("no data emission").that(
            event.is_err(),
            "Should not emit events when no data collected",
        )?;

        Ok(())
    }

    #[sinex_test]
    async fn test_telemetry_concurrent_recording(ctx: TestContext) -> Result<()> {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let accumulator = TelemetryAccumulator::new("concurrent-test").with_event_sender(tx);

        // Spawn multiple tasks recording metrics concurrently
        let mut handles = vec![];
        for i in 0..10 {
            let acc = accumulator.clone();
            let handle = tokio::spawn(async move {
                for j in 0..100 {
                    acc.record_event_processed(&format!("event.type{}", i), j as f64);
                    acc.record_operation_latency(&format!("op{}", i), (i * 10 + j) as f64);
                    if j % 10 == 0 {
                        acc.record_error(&format!("error{}", i));
                    }
                }
            });
            handles.push(handle);
        }

        // Wait for all tasks
        for handle in handles {
            handle.await?;
        }

        // Emit and verify
        accumulator
            .emit_telemetry()
            .await
            .map_err(|e| eyre!("Emit failed: {}", e))?;

        let mut events = Vec::new();
        while let Ok(event) = rx.try_recv() {
            events.push(event);
        }

        // Should have all event types
        ctx.assert("concurrent recording").that(
            events.len() >= 3,
            "Should have multiple event types after concurrent recording",
        )?;

        // Check totals
        for event in events {
            match event.event_type.as_str() {
                "events.processed" => {
                    let count = event.payload["count"].as_u64().unwrap();
                    ctx.assert("concurrent event count").eq(&count, &1000u64)?; // 10 tasks * 100 events
                }
                "errors.summary" => {
                    let total = event.payload["total_errors"].as_u64().unwrap();
                    ctx.assert("concurrent error count").eq(&total, &100u64)?; // 10 tasks * 10 errors each
                }
                _ => {}
            }
        }

        Ok(())
    }

    #[cfg(all(test, feature = "bench"))]
    mod benches {
        use super::*;
        use sinex_test_utils::prelude::*;

        #[sinex_bench]
        async fn bench_telemetry_record_event(
            ctx: &mut BenchContext,
        ) -> color_eyre::eyre::Result<()> {
            let accumulator = TelemetryAccumulator::new("bench");

            ctx.bench("record_event_processed", || {
                accumulator.record_event_processed("test.event", 1.5);
            });

            Ok(())
        }

        #[sinex_bench]
        async fn bench_telemetry_record_operation(
            ctx: &mut BenchContext,
        ) -> color_eyre::eyre::Result<()> {
            let accumulator = TelemetryAccumulator::new("bench");

            ctx.bench("record_operation_latency", || {
                accumulator.record_operation_latency("test_operation", 45.7);
            });

            Ok(())
        }

        #[sinex_bench]
        async fn bench_telemetry_record_error(
            ctx: &mut BenchContext,
        ) -> color_eyre::eyre::Result<()> {
            let accumulator = TelemetryAccumulator::new("bench");

            ctx.bench("record_error", || {
                accumulator.record_error("test_error");
            });

            Ok(())
        }

        #[sinex_bench]
        async fn bench_telemetry_record_resource_usage(
            ctx: &mut BenchContext,
        ) -> color_eyre::eyre::Result<()> {
            let accumulator = TelemetryAccumulator::new("bench");

            ctx.bench("record_resource_usage", || {
                accumulator.record_resource_usage(1024.0, 25.5);
            });

            Ok(())
        }

        #[sinex_bench]
        async fn bench_telemetry_emit(ctx: &mut BenchContext) -> color_eyre::eyre::Result<()> {
            let (tx, _rx) = mpsc::unbounded_channel();
            let accumulator = TelemetryAccumulator::new("bench").with_event_sender(tx);

            // Pre-populate with data
            for i in 0..100 {
                accumulator.record_event_processed(&format!("event.{}", i), i as f64);
                accumulator.record_operation_latency(&format!("op.{}", i), i as f64 * 10.0);
                if i % 10 == 0 {
                    accumulator.record_error(&format!("error.{}", i));
                }
            }

            ctx.bench("emit_telemetry", || {
                let rt = tokio::runtime::Runtime::new().unwrap();
                rt.block_on(async {
                    accumulator.emit_telemetry().await.unwrap();
                });
            });

            Ok(())
        }

        #[sinex_bench]
        async fn bench_concurrent_telemetry_recording(
            ctx: &mut BenchContext,
        ) -> color_eyre::eyre::Result<()> {
            use std::sync::Arc;
            use std::thread;

            let accumulator = Arc::new(TelemetryAccumulator::new("bench"));

            ctx.bench("concurrent_recording", || {
                let handles: Vec<_> = (0..4)
                    .map(|thread_id| {
                        let acc = accumulator.clone();
                        thread::spawn(move || {
                            for i in 0..25 {
                                acc.record_event_processed(
                                    &format!("event.{}.{}", thread_id, i),
                                    i as f64,
                                );
                                acc.record_operation_latency(
                                    &format!("op.{}.{}", thread_id, i),
                                    i as f64 * 2.0,
                                );
                            }
                        })
                    })
                    .collect();

                for handle in handles {
                    handle.join().unwrap();
                }
            });

            Ok(())
        }
    }
}

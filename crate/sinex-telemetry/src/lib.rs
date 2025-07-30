//! # Sinex Telemetry Library
//!
//! A comprehensive telemetry and metrics library for the Sinex event-driven data capture system.
//! This library implements a hybrid approach that combines real-time Prometheus metrics with
//! long-term telemetry event storage.
//!
//! ## Overview
//!
//! The library provides two complementary telemetry systems:
//!
//! 1. **Real-time Metrics (Prometheus)**
//!    - In-memory metrics for operational monitoring
//!    - Sub-minute visibility via `/metrics` endpoint
//!    - Automatic instrumentation with procedural macros
//!    - Typically retained for 15-90 days
//!
//! 2. **Historical Telemetry (Events)**
//!    - Periodic summary events stored as Sinex events
//!    - Long-term retention (indefinite)
//!    - Per-component granularity for debugging
//!    - ~2000 events/day vs 400k+ metric updates
//!
//! ## Quick Start
//!
//! ### Basic Setup
//!
//! ```rust,ignore
//! use sinex_telemetry::{init_metrics, TelemetryAccumulator, set_global_telemetry};
//! use tokio::sync::mpsc;
//!
//! #[tokio::main]
//! async fn main() {
//!     // Initialize Prometheus metrics
//!     init_metrics().await;
//!     
//!     // Set up telemetry
//!     let (tx, rx) = mpsc::channel(100);
//!     let telemetry = TelemetryAccumulator::new("my-service")
//!         .with_event_sender(tx)
//!         .with_interval(Duration::from_secs(300)); // 5 minutes
//!     
//!     // Set as global telemetry for auto-metrics integration
//!     set_global_telemetry(telemetry.clone()).await;
//!     
//!     // Spawn background emitter
//!     telemetry.spawn_emitter();
//!     
//!     // Your application code here
//! }
//! ```
//!
//! ### Automatic Metrics with Macros
//!
//! The [`auto_metrics`] procedural macro automatically instruments functions:
//!
//! ```rust,ignore
//! use sinex_telemetry::auto_metrics;
//!
//! #[auto_metrics]
//! async fn process_request(data: &str) -> Result<String, Error> {
//!     // Automatically tracks:
//!     // - function_calls_total{status="success|error"}
//!     // - function_duration_seconds (histogram)
//!     // - function_errors_total
//!     // - function_active_calls (gauge)
//!     Ok(data.to_uppercase())
//! }
//! ```
//!
//! ### Manual Telemetry Recording
//!
//! Record metrics manually when you need fine-grained control:
//!
//! ```rust,ignore
//! // Record event processing
//! telemetry.record_event_processed("user.created", 15.3);
//!
//! // Record operation performance
//! telemetry.record_operation_latency("database_query", 45.7);
//!
//! // Record resource usage
//! telemetry.record_resource_usage(memory_mb, cpu_percent);
//!
//! // Record errors
//! telemetry.record_error("validation_error");
//! ```
//!
//! ### Exporting Metrics
//!
//! Export metrics in various formats:
//!
//! ```rust,ignore
//! use sinex_telemetry::{export_prometheus, export_json, export_openmetrics};
//!
//! // Get Prometheus-formatted metrics
//! let prometheus_output = export_prometheus();
//!
//! // Get JSON-formatted metrics
//! let json_output = export_json();
//!
//! // Get OpenMetrics format
//! let openmetrics_output = export_openmetrics();
//! ```
//!
//! ## Architecture
//!
//! ### Module Organization
//!
//! - [`telemetry`] - Core telemetry accumulation and event emission
//! - [`metrics`] - Prometheus metrics infrastructure
//! - [`instrumentation`] - Automatic instrumentation utilities
//!
//! ### Event Types
//!
//! The library emits several types of telemetry events:
//!
//! - `events.processed` - Event throughput metrics per component
//! - `operation.performance` - Operation latency percentiles
//! - `resource.usage` - Component memory and CPU usage
//! - `system.resources` - System-wide resource metrics
//! - `errors.summary` - Error counts by type
//!
//! ## Performance Considerations
//!
//! - Metrics are stored in-memory with minimal overhead
//! - Telemetry events are batched and emitted periodically
//! - Lock-free data structures minimize contention
//! - Recommended emission intervals:
//!   - System resources: 1 minute
//!   - Component metrics: 5 minutes
//!   - Error summaries: On occurrence or 5 minutes
//!
//! ## Integration
//!
//! ### With Sinex Satellites
//!
//! ```rust,ignore
//! // In satellite initialization
//! let telemetry = TelemetryAccumulator::new("fs-watcher")
//!     .with_event_sender(event_sender.clone());
//! telemetry.spawn_emitter();
//! ```
//!
//! ### With ingestd
//!
//! ```rust,ignore
//! // Ingestd uses self-injection for telemetry
//! let telemetry_events = telemetry.collect_events();
//! for event in telemetry_events {
//!     self.inject_event(event).await?;
//! }
//! ```
//!
//! ### With Gateway
//!
//! ```rust,ignore
//! // Gateway batches telemetry before sending
//! self.telemetry_batch.push(telemetry_event);
//! if self.telemetry_batch.len() >= BATCH_SIZE {
//!     self.flush_telemetry().await?;
//! }
//! ```

pub mod instrumentation;
pub mod metrics;
pub mod telemetry;

// Re-export the main functionality
pub use instrumentation::*;
pub use metrics::*;
pub use telemetry::*;

/// Initialize the metrics system.
///
/// This sets up the global metrics registry and starts background collectors.
/// Should be called once at application startup.
///
/// # Example
///
/// ```rust,ignore
/// #[tokio::main]
/// async fn main() {
///     sinex_telemetry::init_metrics().await;
///     // Your application code
/// }
/// ```
///
/// # What This Does
///
/// 1. Initializes the global Prometheus registry
/// 2. Starts system metrics collectors (CPU, memory, disk)
/// 3. Starts process metrics collectors
/// 4. Sets up background collection intervals
///
/// # Panics
///
/// This function should not panic under normal circumstances.
pub async fn init_metrics() {
    metrics::registry::init_global_registry().await;
    metrics::collectors::start_background_collectors().await;
}

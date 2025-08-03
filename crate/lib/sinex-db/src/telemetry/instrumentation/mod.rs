//! # Automatic Instrumentation for Metrics and Telemetry
//!
//! This module provides procedural macros and instrumentation utilities
//! for automatically collecting metrics from various parts of the system.
//!
//! ## Overview
//!
//! The instrumentation module enables automatic metric collection without
//! manual instrumentation code. It provides both compile-time macros and
//! runtime utilities to capture performance metrics, resource usage, and
//! application behavior.
//!
//! ## Submodules
//!
//! - [`auto_metrics`] - Automatic function instrumentation with metrics
//! - [`database`] - Database operation metrics and query tracking
//! - [`events`] - Event processing metrics and throughput tracking
//! - [`resources`] - Resource usage monitoring (CPU, memory, I/O)
//! - [`satellite`] - Satellite-specific instrumentation utilities
//!
//! ## Usage
//!
//! ### Automatic Function Metrics
//!
//! ```rust,ignore
//! use sinex_telemetry::auto_metrics;
//!
//! #[auto_metrics]
//! async fn process_data(input: &str) -> Result<String, Error> {
//!     // Function automatically tracks:
//!     // - Call count
//!     // - Execution time
//!     // - Error rate
//!     // - Concurrent executions
//!     Ok(input.to_uppercase())
//! }
//! ```
//!
//! ### Database Metrics
//!
//! ```rust,ignore
//! use sinex_telemetry::instrumentation::DatabaseMetrics;
//!
//! let db_metrics = DatabaseMetrics::new("postgres");
//! db_metrics.record_query("SELECT", 25.3, true);
//! ```
//!
//! ### Event Processing Metrics
//!
//! ```rust,ignore
//! use sinex_telemetry::instrumentation::EventMetrics;
//!
//! let event_metrics = EventMetrics::new("fs-watcher");
//! event_metrics.record_event_processed("file.created", 15.2);
//! ```
//!
//! ## Integration with Telemetry
//!
//! All instrumentation automatically integrates with both:
//! - Prometheus metrics for real-time monitoring
//! - Telemetry events for long-term analysis
//!
//! This dual approach ensures comprehensive observability without
//! sacrificing performance or storage efficiency.

pub mod auto_metrics;
pub mod database;
pub mod events;
pub mod resources;
pub mod satellite;

pub use auto_metrics::*;
pub use database::*;
pub use events::*;
pub use resources::*;
pub use satellite::*;

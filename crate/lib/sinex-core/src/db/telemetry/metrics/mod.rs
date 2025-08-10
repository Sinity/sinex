//! # Real-time Metrics Collection Using Prometheus
//!
//! This module provides infrastructure for collecting and exposing real-time
//! metrics that can be scraped by Prometheus for operational monitoring.
//!
//! ## Overview
//!
//! The metrics module implements a comprehensive Prometheus-based monitoring
//! solution that provides:
//!
//! - Thread-safe metric registration and management
//! - Multiple metric types (counters, gauges, histograms)
//! - Various export formats (Prometheus, JSON, OpenMetrics, InfluxDB, StatsD)
//! - Background collectors for system and process metrics
//! - Integration with the telemetry system for long-term storage
//!
//! ## Submodules
//!
//! - [`collectors`] - Infrastructure for collecting metrics from various sources
//! - [`export`] - Export metrics in different formats
//! - [`registry`] - Central registry for all metrics
//!
//! ## Usage
//!
//! ```rust,ignore
//! use sinex_telemetry::metrics::{GlobalMetrics, export_prometheus};
//! use std::collections::HashMap;
//!
//! // Create metrics
//! let counter = GlobalMetrics::get_or_create_counter(
//!     "requests_total",
//!     "Total number of requests",
//!     HashMap::new(),
//! );
//! counter.inc();
//!
//! // Export for Prometheus
//! let prometheus_text = export_prometheus();
//! ```

pub mod collectors;
pub mod export;
pub mod registry;

pub use collectors::*;
pub use export::*;
pub use registry::*;

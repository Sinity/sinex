//! # Telemetry Event Emission Module
//!
//! This module provides telemetry accumulation and periodic emission of
//! summary events that are stored as regular Sinex events for historical analysis.
//!
//! ## Overview
//!
//! The telemetry system complements Prometheus metrics by providing:
//! - Long-term storage of metrics as events
//! - Per-component metric isolation
//! - Periodic summary aggregation
//! - Integration with Sinex event infrastructure
//!
//! ## Components
//!
//! - [`TelemetryAccumulator`] - Main telemetry collection and emission
//! - [`SystemTelemetryEmitter`] - System-wide resource telemetry
//! - [`set_global_telemetry`] - Global telemetry registration
//! - [`record_function_telemetry`] - Function-level telemetry recording
//!
//! ## Usage
//!
//! ```rust,ignore
//! use sinex_telemetry::telemetry::TelemetryAccumulator;
//! use std::time::Duration;
//!
//! let telemetry = TelemetryAccumulator::new("my-component")
//!     .with_event_sender(event_sender)
//!     .with_interval(Duration::from_secs(300));
//!
//! // Record metrics
//! telemetry.record_event_processed("user.created", 15.3);
//! telemetry.record_operation_latency("db_query", 45.7);
//!
//! // Spawn background emitter
//! telemetry.spawn_emitter();
//! ```

pub mod accumulator;

pub use accumulator::*;

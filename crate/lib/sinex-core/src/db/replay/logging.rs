//! Logging utilities for replay operations
//!
//! Provides consistent logging patterns with appropriate levels for different scenarios.

use crate::db::models::event::Event;
use crate::db::models::JsonValue;
use crate::types::Id;
use std::fmt::Display;
use tracing::{debug, error, info, trace, warn};

/// Log levels for different replay operations
pub struct ReplayLogger;

impl ReplayLogger {
    /// Debug-level logging for detailed internal state
    pub fn debug_state<T: Display>(component: &str, message: T) {
        debug!(component = component, "{}", message);
    }

    /// Info-level logging for significant events
    pub fn info_operation<T: Display>(operation: &str, message: T) {
        info!(operation = operation, "{}", message);
    }

    /// Warn-level logging for potential issues
    pub fn warn_violation<T: Display>(violation_type: &str, message: T) {
        warn!(violation = violation_type, "{}", message);
    }

    /// Error-level logging for failures
    pub fn error_failure<T: Display>(context: &str, error: T) {
        error!(context = context, error = %error, "Operation failed");
    }

    /// Trace-level logging for very detailed debugging
    pub fn trace_event(event_id: &Id<Event<JsonValue>>, action: &str) {
        trace!(event_id = %event_id, action = action, "Processing event");
    }

    /// Log the start of a replay operation
    pub fn start_replay(
        operation_id: &Id<crate::db::repositories::state::Operation>,
        scope: &serde_json::Value,
    ) {
        info!(
            operation_id = %operation_id,
            scope = %scope,
            "Starting replay operation"
        );
    }

    /// Log dry-run mode operation
    pub fn dry_run_operation(operation: &str, target: &str, details: &serde_json::Value) {
        info!(
            mode = "DRY_RUN",
            operation = operation,
            target = target,
            details = %details,
            "Would perform operation (dry-run mode)"
        );
    }

    /// Log dry-run summary
    pub fn dry_run_summary(
        total_operations: usize,
        events_affected: usize,
        estimated_duration_ms: u64,
    ) {
        info!(
            mode = "DRY_RUN",
            total_operations = total_operations,
            events_affected = events_affected,
            estimated_duration_ms = estimated_duration_ms,
            "Dry-run complete - no changes made"
        );
    }

    /// Log the completion of a replay operation
    pub fn complete_replay(
        operation_id: &Id<crate::db::repositories::state::Operation>,
        events_processed: usize,
        duration_ms: u64,
    ) {
        info!(
            operation_id = %operation_id,
            events_processed = events_processed,
            duration_ms = duration_ms,
            "Replay operation completed"
        );
    }

    /// Log a checkpoint save
    pub fn checkpoint_saved(
        operation_id: &Id<crate::db::repositories::state::Operation>,
        checkpoint: &serde_json::Value,
    ) {
        debug!(
            operation_id = %operation_id,
            checkpoint = %checkpoint,
            "Checkpoint saved"
        );
    }

    /// Log batch processing progress
    pub fn batch_progress(batch_num: usize, total_batches: usize, events_in_batch: usize) {
        let is_milestone = batch_num != 0 && batch_num % 10 == 0;
        if is_milestone || batch_num == total_batches {
            info!(
                batch = batch_num,
                total = total_batches,
                events = events_in_batch,
                "Processing batch"
            );
        } else {
            debug!(
                batch = batch_num,
                total = total_batches,
                events = events_in_batch,
                "Processing batch"
            );
        }
    }

    /// Log cascade analysis results
    pub fn cascade_analysis(
        root_event: &Id<Event<JsonValue>>,
        depth: usize,
        affected_events: usize,
    ) {
        info!(
            root_event = %root_event,
            depth = depth,
            affected_events = affected_events,
            "Cascade analysis complete"
        );
    }

    /// Log invariant violations
    pub fn invariant_violated(violation: &crate::db::replay::invariants::InvariantViolation) {
        let severity = violation.violation_type.severity();
        match severity {
            crate::db::replay::invariants::ViolationSeverity::Low => {
                debug!(
                    violation_type = ?violation.violation_type,
                    "Low severity invariant violation"
                );
            }
            crate::db::replay::invariants::ViolationSeverity::Medium => {
                info!(
                    violation_type = ?violation.violation_type,
                    "Medium severity invariant violation"
                );
            }
            crate::db::replay::invariants::ViolationSeverity::High => {
                warn!(
                    violation_type = ?violation.violation_type,
                    context = %violation.context,
                    "High severity invariant violation"
                );
            }
            crate::db::replay::invariants::ViolationSeverity::Critical => {
                error!(
                    violation_type = ?violation.violation_type,
                    context = %violation.context,
                    detected_at = %violation.detected_at,
                    "Critical invariant violation - replay blocked"
                );
            }
        }
    }

    /// Log retry attempts
    pub fn retry_attempt(operation: &str, attempt: u32, max_attempts: u32, error: &str) {
        if attempt == max_attempts {
            error!(
                operation = operation,
                attempt = attempt,
                max_attempts = max_attempts,
                error = error,
                "Final retry attempt failed"
            );
        } else {
            warn!(
                operation = operation,
                attempt = attempt,
                max_attempts = max_attempts,
                error = error,
                "Retry attempt"
            );
        }
    }

    /// Log performance metrics
    pub fn performance_metrics(
        operation: &str,
        events_per_second: f64,
        memory_used_mb: f64,
        cpu_usage_percent: f64,
    ) {
        debug!(
            operation = operation,
            events_per_second = events_per_second,
            memory_mb = memory_used_mb,
            cpu_percent = cpu_usage_percent,
            "Performance metrics"
        );
    }
}

/// Macro for consistent structured logging
#[macro_export]
macro_rules! replay_log {
    (debug, $($key:ident = $value:expr),* $(,)?) => {
        ::tracing::debug!($($key = ?$value,)* "Replay debug");
    };
    (info, $($key:ident = $value:expr),* $(,)?) => {
        ::tracing::info!($($key = ?$value,)* "Replay info");
    };
    (warn, $($key:ident = $value:expr),* $(,)?) => {
        ::tracing::warn!($($key = ?$value,)* "Replay warning");
    };
    (error, $($key:ident = $value:expr),* $(,)?) => {
        ::tracing::error!($($key = ?$value,)* "Replay error");
    };
}

//! Invariant enforcement for replay operations
//!
//! This module provides types and functions for validating system invariants
//! during replay operations to ensure correctness and consistency.

use crate::db::models::event::Event;
use crate::db::models::JsonValue;
use crate::types::Id;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Types of invariant violations that can occur during replay
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ViolationType {
    /// Event references non-existent parent in provenance chain
    BrokenProvenance {
        event_id: Id<Event<JsonValue>>,
        missing_parent_id: Id<Event<JsonValue>>,
    },

    /// Circular dependency detected in event graph
    CircularDependency {
        event_ids: Vec<Id<Event<JsonValue>>>,
    },

    /// Event timestamp is out of order with its dependencies
    OutOfOrderTimestamp {
        event_id: Id<Event<JsonValue>>,
        event_timestamp: DateTime<Utc>,
        dependency_id: Id<Event<JsonValue>>,
        dependency_timestamp: DateTime<Utc>,
    },

    /// Event has been tampered with (checksum mismatch)
    TamperedEvent {
        event_id: Id<Event<JsonValue>>,
        expected_checksum: String,
        actual_checksum: String,
    },

    /// Event references a source material anchor that doesn't exist
    OrphanedAnchor {
        event_id: Id<Event<JsonValue>>,
        material_id: Id<Event<JsonValue>>,
        anchor_byte: i64,
        material_size: i64,
    },

    /// Event payload doesn't match its declared schema
    SchemaMismatch {
        event_id: Id<Event<JsonValue>>,
        schema_id: Option<uuid::Uuid>,
        schema_name: Option<String>,
        validation_error: String,
    },

    /// Event claims to have occurred before its dependencies (time travel)
    TemporalParadox {
        event_id: Id<Event<JsonValue>>,
        claimed_time: DateTime<Utc>,
        earliest_possible_time: DateTime<Utc>,
        conflicting_dependency: Id<Event<JsonValue>>,
    },

    /// Gap detected in continuous material stream
    MaterialGap {
        material_id: Id<Event<JsonValue>>,
        gap_start: i64,
        gap_end: i64,
    },

    /// Overlapping material slices
    MaterialOverlap {
        material_id: Id<Event<JsonValue>>,
        overlap_start: i64,
        overlap_end: i64,
        conflicting_events: Vec<Id<Event<JsonValue>>>,
    },
}

impl ViolationType {
    /// Get a human-readable description of the violation
    pub fn description(&self) -> String {
        match self {
            ViolationType::BrokenProvenance {
                event_id,
                missing_parent_id,
            } => {
                format!("Event {event_id} references non-existent parent {missing_parent_id}")
            }
            ViolationType::CircularDependency { event_ids } => {
                format!(
                    "Circular dependency detected involving {} events",
                    event_ids.len()
                )
            }
            ViolationType::OutOfOrderTimestamp { event_id, .. } => {
                format!("Event {event_id} has timestamp out of order with its dependencies")
            }
            ViolationType::TamperedEvent { event_id, .. } => {
                format!("Event {event_id} has been tampered with (checksum mismatch)")
            }
            ViolationType::OrphanedAnchor {
                event_id,
                anchor_byte,
                ..
            } => {
                format!("Event {event_id} references non-existent anchor at byte {anchor_byte}")
            }
            ViolationType::SchemaMismatch {
                event_id,
                validation_error,
                ..
            } => {
                format!("Event {event_id} payload validation failed: {validation_error}")
            }
            ViolationType::TemporalParadox { event_id, .. } => {
                format!("Event {event_id} claims to have occurred before its dependencies")
            }
            ViolationType::MaterialGap {
                material_id,
                gap_start,
                gap_end,
            } => {
                format!("Gap detected in material {material_id} from byte {gap_start} to {gap_end}")
            }
            ViolationType::MaterialOverlap {
                material_id,
                overlap_start,
                overlap_end,
                ..
            } => {
                format!(
                    "Overlapping slices in material {material_id} from byte {overlap_start} to {overlap_end}"
                )
            }
        }
    }

    /// Get the severity level of the violation
    pub fn severity(&self) -> ViolationSeverity {
        match self {
            ViolationType::TamperedEvent { .. } => ViolationSeverity::Critical,
            ViolationType::CircularDependency { .. } => ViolationSeverity::Critical,
            ViolationType::TemporalParadox { .. } => ViolationSeverity::Critical,
            ViolationType::BrokenProvenance { .. } => ViolationSeverity::High,
            ViolationType::SchemaMismatch { .. } => ViolationSeverity::High,
            ViolationType::OrphanedAnchor { .. } => ViolationSeverity::Medium,
            ViolationType::OutOfOrderTimestamp { .. } => ViolationSeverity::Medium,
            ViolationType::MaterialGap { .. } => ViolationSeverity::Medium,
            ViolationType::MaterialOverlap { .. } => ViolationSeverity::Low,
        }
    }
}

/// Severity levels for violations
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum ViolationSeverity {
    /// Minor issue that might be acceptable in some contexts
    Low,
    /// Issue that should be investigated but might not block replay
    Medium,
    /// Serious issue that should block replay in most cases
    High,
    /// Critical issue that must always block replay
    Critical,
}

/// A detected invariant violation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InvariantViolation {
    pub violation_type: ViolationType,
    pub detected_at: DateTime<Utc>,
    pub operation_id: Option<Id<crate::db::repositories::state::Operation>>,
    pub context: serde_json::Value,
}

impl InvariantViolation {
    /// Create a new invariant violation
    pub fn new(
        violation_type: ViolationType,
        operation_id: Option<Id<crate::db::repositories::state::Operation>>,
    ) -> Self {
        Self {
            violation_type,
            detected_at: Utc::now(),
            operation_id,
            context: serde_json::json!({}),
        }
    }

    /// Add context information to the violation
    pub fn with_context(mut self, key: impl ToString, value: impl Serialize) -> Self {
        if let Some(obj) = self.context.as_object_mut() {
            obj.insert(
                key.to_string(),
                serde_json::to_value(value).unwrap_or(serde_json::Value::Null),
            );
        }
        self
    }
}

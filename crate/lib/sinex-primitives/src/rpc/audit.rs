//! Audit trail types

use crate::domain::{EventSource, EventType, OperationStatus};
use crate::events::Event;
use crate::ids::Id;
use crate::rpc::ops::Operation;
use crate::temporal::Timestamp;
use serde::{Deserialize, Serialize};
use serde_json::Value;

fn default_audit_limit() -> usize {
    100
}

/// Operation record from the operations log
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperationRecord {
    pub id: Id<Operation>,
    pub operation_type: String,
    pub operator: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<Value>,
    pub result_status: OperationStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result_message: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preview_summary: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<i32>,
}

/// Event summary for audit trail
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventSummary {
    pub id: Id<Event>,
    pub source: EventSource,
    pub event_type: EventType,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ts_orig: Option<Timestamp>,
    pub ts_coided: Timestamp,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provenance_operation_id: Option<Id<Operation>>,
}

/// Audit trail combining operation and affected events
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditTrail {
    pub operation: OperationRecord,
    pub affected_events: Vec<EventSummary>,
}

// ─────────────────────────────────────────────────────────────
// audit.get
// ─────────────────────────────────────────────────────────────

/// Request: audit.get
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditGetRequest {
    /// Operation ID to get audit trail for
    pub operation_id: Id<Operation>,
    /// Maximum number of affected events to return per page (default 100, max 1000)
    #[serde(default = "default_audit_limit")]
    pub limit: usize,
    /// Cursor for pagination: exclusive upper-bound event ID from the previous page.
    ///
    /// Events are returned in descending UUIDv7 (time) order. Pass the `next_cursor`
    /// from the previous response to retrieve the next page.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after_id: Option<Id<Event>>,
}

/// Response: audit.get
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditGetResponse {
    pub audit_trail: AuditTrail,
    pub event_count: usize,
    /// Cursor to pass as `after_id` to retrieve the next page, if more results exist.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<Id<Event>>,
    /// Whether additional pages of affected events are available.
    pub has_more: bool,
}

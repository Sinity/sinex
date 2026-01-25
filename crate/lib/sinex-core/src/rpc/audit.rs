//! Audit trail types

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Operation record from the operations log
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperationRecord {
    pub id: String,
    pub operation_type: String,
    pub operator: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<Value>,
    pub result_status: String,
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
    pub id: String,
    pub source: String,
    pub event_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ts_orig: Option<DateTime<Utc>>,
    pub ts_ingest: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provenance_operation_id: Option<String>,
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
    pub operation_id: String,
}

/// Response: audit.get
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditGetResponse {
    pub audit_trail: AuditTrail,
    pub event_count: usize,
}

//! Semantic epoch and shadow-lane audit payloads.
//!
//! These payloads are the event-native control trail for semantic
//! experimentation. Raw shadow outputs remain lane artifacts, not canonical
//! events; these events record operator-visible registry/diff/discard facts.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sinex_macros::EventPayload;

use crate::{
    EntityRelationDiffReport, SemanticEpochRecord, SemanticLaneRecord, SemanticLaneStatus,
    Timestamp, Uuid,
};

/// A semantic epoch was declared for a fixed scope and configuration.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(
    source = "semantics",
    event_type = "semantic.epoch_recorded",
    version = "1.0.0"
)]
pub struct SemanticEpochRecordedPayload {
    pub epoch: SemanticEpochRecord,
    pub created_by: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation_id: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub supersedes_epoch_id: Option<Uuid>,
}

/// A canonical, shadow, or experiment lane was created.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(
    source = "semantics",
    event_type = "semantic.lane_created",
    version = "1.0.0"
)]
pub struct SemanticLaneCreatedPayload {
    pub lane: SemanticLaneRecord,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation_id: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<Timestamp>,
}

/// A semantic lane lifecycle transition was recorded.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(
    source = "semantics",
    event_type = "semantic.lane_status_changed",
    version = "1.0.0"
)]
pub struct SemanticLaneStatusChangedPayload {
    pub lane_id: Uuid,
    pub previous_status: SemanticLaneStatus,
    pub new_status: SemanticLaneStatus,
    pub reason: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation_id: Option<Uuid>,
    pub changed_at: Timestamp,
}

/// A deterministic comparison report was recorded for two lanes.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(
    source = "semantics",
    event_type = "semantic.lane_diff_recorded",
    version = "1.0.0"
)]
pub struct SemanticLaneDiffRecordedPayload {
    pub diff_id: Uuid,
    pub baseline_lane_id: Uuid,
    pub candidate_lane_id: Uuid,
    pub diff_kind: String,
    pub report: EntityRelationDiffReport,
    pub report_hash: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation_id: Option<Uuid>,
    pub created_at: Timestamp,
}

/// Raw lane outputs were discarded through an explicit operation.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(
    source = "semantics",
    event_type = "semantic.lane_outputs_discarded",
    version = "1.0.0"
)]
pub struct SemanticLaneOutputsDiscardedPayload {
    pub lane_id: Uuid,
    pub output_kind: String,
    pub discarded_output_count: u64,
    pub reason: String,
    pub operation_id: Uuid,
    pub discarded_at: Timestamp,
}

#[cfg(any(test, feature = "testing"))]
impl SemanticLaneStatusChangedPayload {
    #[must_use]
    pub fn test_discarded(lane_id: Uuid) -> Self {
        Self {
            lane_id,
            previous_status: SemanticLaneStatus::Compared,
            new_status: SemanticLaneStatus::Discarded,
            reason: "candidate churn too high".to_string(),
            operation_id: Some(Uuid::from_u128(10)),
            changed_at: Timestamp::UNIX_EPOCH,
        }
    }
}

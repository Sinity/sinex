//! What the operator wrote down for themselves: quick notes (the raw log) and
//! their steering store's commitments, activities and reviews.
use crate::Timestamp;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// One entry of the operator's quick-note log. The entry is its place in the
/// log; an edit to it is a newer reading.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(version = 2, type = "operator_log.entry",
    key = location,
    replace = newer,
    time = written_at,
    quality = intrinsic
)]
pub struct LogEntry {
    /// Unknown for an entry headed by a label instead of a time
    /// (`**SPECIAL:**`).
    pub written_at: Option<Timestamp>,
    /// The bold heading when it is a label, not a time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    /// The entry's text, continuation lines included.
    pub text: String,
}

mod steering;
pub use steering::{
    Activity, ActivityKind, Commitment, CommitmentStatus, EnergyTier, Review, SteeringRow,
    SteeringValues,
};

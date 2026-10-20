//! What the operator wrote down for themselves: quick notes (the raw log) and
//! their steering store's commitments, activities and reviews.
use crate::Timestamp;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// One entry of the operator's quick-note log. The entry is its place in the
/// log; an edit to it is a newer reading.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(
    type = "operator.log.entry",
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

/// A row of the operator's steering store (a commitment with its forecast, an
/// activity, a review), as the store holds it.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(
    type = "steering.record",
    key = ["table", "record_id"],
    replace = newer,
    time = at,
    quality = intrinsic
)]
pub struct SteeringRecord {
    /// `commitments`, `activities` or `reviews`.
    pub table: String,
    /// The row's `id` in its table.
    pub record_id: String,
    /// The row's own time (`created_at`, a review's `ts`); activities have
    /// none.
    pub at: Option<Timestamp>,
    pub values: Map<String, Value>,
}

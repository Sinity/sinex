//! Metadata about intervals when a stream could not capture records.
use crate::Timestamp;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(version = 3, type = "capture.gap", key = location, replace = repeat, time = start, quality = captured)]
pub struct Gap {
    pub stream: String,
    pub start: Option<Timestamp>,
    pub end: Option<Timestamp>,
    pub reason: String,
    /// Present only when a numbered stream proves exact missing sequence bounds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sequence: Option<SequenceGap>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SequenceGap {
    pub first_missing: u64,
    pub last_missing: u64,
}
